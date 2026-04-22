import os
import re
import logging
import pyotp
import pytz
import requests
import pandas as pd
from dataclasses import dataclass
from datetime import datetime, time, timedelta
from SmartApi.smartConnect import SmartConnect
from telegram import Update
from telegram.ext import Application, ContextTypes, MessageHandler, filters

# --- LOGGING SETUP ---
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

IST = pytz.timezone("Asia/Kolkata")
LOT_SIZE = 30
PRICE_UPDATE_STEP = 2.0 
MAX_OPEN_TRADES = 1

# --- CONFIGURATION ---
TELEGRAM_BOT_TOKEN = os.getenv("TELE_TOKEN_MCX")
TRADE_CHANNEL_ID = os.getenv("CHAT_ID_MCX")
ANGEL_API_KEY = os.getenv("ANGEL_API_KEY")
ANGEL_CLIENT_ID = os.getenv("ANGEL_CLIENT_ID")
ANGEL_PASSWORD = os.getenv("ANGEL_PASSWORD")
ANGEL_TOTP_SECRET = os.getenv("ANGEL_TOTP_SECRET")

@dataclass
class PaperTrade:
    symbol: str
    strike: int
    option_type: str
    tradingsymbol: str
    token: str
    entry: float
    qty: int
    sl: float
    last_alert_price: float
    targets: list
    highest_target_hit: int = 0

class BankNiftyPaperBot:
    def __init__(self):
        self.smart = None
        self.instruments = None
        self.open_trade = None

    def login_angel(self):
        try:
            totp = pyotp.TOTP(ANGEL_TOTP_SECRET).now()
            self.smart = SmartConnect(api_key=ANGEL_API_KEY)
            self.smart.generateSession(ANGEL_CLIENT_ID, ANGEL_PASSWORD, totp)
            logger.info("✅ Angel One Login Successful")
        except Exception as e:
            logger.error(f"❌ Angel Login Failed: {e}")

    def load_instruments(self):
        try:
            url = "https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json"
            df = pd.read_json(url)
            df = df[(df["exch_seg"] == "NFO") & (df["name"] == "BANKNIFTY")].copy()
            df["expiry_dt"] = pd.to_datetime(df["expiry"], format="%d%b%Y", errors="coerce")
            today = datetime.now().replace(hour=0, minute=0, second=0)
            self.instruments = df[df["expiry_dt"] >= today].copy()
            logger.info(f"✅ Loaded {len(self.instruments)} BANKNIFTY contracts")
        except Exception as e:
            logger.error(f"❌ Failed to load instruments: {e}")

    def get_ltp(self, symbol, token):
        data = self.smart.ltpData("NFO", symbol, token)
        return float(data["data"]["ltp"]) if data.get("status") else 0.0

    def parse_signal(self, text):
        # Look for BANKNIFTY + Strike + PE/CE
        match = re.search(r"BANKNIFTY\s+(\d+)\s+(CE|PE)", text, re.I)
        if not match:
            return None
        return {"strike": int(match.group(1)), "type": match.group(2).upper()}

    def enter_trade(self, signal):
        if self.open_trade:
            logger.info("⚠️ Trade ignored: A trade is already active.")
            return None
        
        df = self.instruments
        df = df[df["symbol"].str.contains(f"{signal['strike']}{signal['type']}", na=False)].copy()
        if df.empty:
            logger.error(f"❌ No contract found for {signal['strike']} {signal['type']}")
            return None
            
        row = df[df["expiry_dt"] == df["expiry_dt"].min()].iloc[0]
        ltp = self.get_ltp(row["symbol"], row["token"])
        if ltp == 0: return None

        targets = [ltp + 30, ltp + 60, ltp + 90, ltp + 120]
        self.open_trade = PaperTrade(
            symbol="BANKNIFTY", strike=signal["strike"], option_type=signal["type"],
            tradingsymbol=row["symbol"], token=row["token"], entry=ltp, qty=LOT_SIZE,
            sl=ltp - 30, last_alert_price=ltp, targets=targets
        )
        return self.open_trade

    def process_update(self):
        if not self.open_trade: return None
        t = self.open_trade
        ltp = self.get_ltp(t.tradingsymbol, t.token)
        
        if ltp <= t.sl:
            self.open_trade = None
            return {"type": "EXIT_SL", "price": ltp}
            
        for i, target_price in enumerate(t.targets):
            level = i + 1
            if ltp >= target_price and t.highest_target_hit < level:
                t.highest_target_hit = level
                if level == 1: t.sl = t.entry
                elif level == 2: t.sl = t.targets[0]
                elif level == 3: t.sl = t.targets[1]
                
                if level == 4:
                    self.open_trade = None
                    return {"type": "T4_COMPLETE", "price": ltp}
                return {"type": "TARGET_HIT", "level": level, "price": ltp, "new_sl": t.sl}

        if ltp >= t.last_alert_price + PRICE_UPDATE_STEP:
            t.last_alert_price = ltp
            return {"type": "PRICE_MOVE", "price": ltp}
        return None

bot_engine = BankNiftyPaperBot()

async def handle_any_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # This captures Channel Posts AND Private Chat Messages
    msg = update.channel_post or update.message
    if not msg or not msg.text:
        return

    # LOG EVERYTHING TO RAILWAY
    logger.info(f"📩 MESSAGE RECEIVED: '{msg.text[:50]}...' from Chat ID: {msg.chat_id}")

    signal = bot_engine.parse_signal(msg.text)
    if not signal:
        logger.info("ℹ️ Message ignored: Does not match BANKNIFTY pattern.")
        return

    trade = bot_engine.enter_trade(signal)
    if trade:
        msg_text = (f"🔥 **BANKNIFTY {trade.strike} {trade.option_type}**\n\n"
                    f"📍 **Entry:** {trade.entry:.2f}\n"
                    f"🛡️ **SL:** {trade.sl:.2f}\n"
                    f"🎯 **T1:** {trade.targets[0]:.2f} | **T2:** {trade.targets[1]:.2f}\n"
                    f"🎯 **T3:** {trade.targets[2]:.2f} | **T4:** {trade.targets[3]:.2f}")
        await context.bot.send_message(chat_id=TRADE_CHANNEL_ID, text=msg_text, parse_mode='Markdown')
        logger.info(f"✅ Trade Entered & Alerted in Channel {TRADE_CHANNEL_ID}")

async def track_trade_job(context: ContextTypes.DEFAULT_TYPE):
    res = bot_engine.process_update()
    if not res: return
    
    if res["type"] == "PRICE_MOVE":
        await context.bot.send_message(chat_id=TRADE_CHANNEL_ID, text=f"📈 **Price Update:** {res['price']:.2f}")
    elif res["type"] == "TARGET_HIT":
        await context.bot.send_message(chat_id=TRADE_CHANNEL_ID, text=f"🎯 **T{res['level']} HIT!** @ {res['price']:.2f}\n✅ SL Shifted: **{res['new_sl']:.2f}**", parse_mode='Markdown')
    elif res["type"] == "T4_COMPLETE":
        await context.bot.send_message(chat_id=TRADE_CHANNEL_ID, text=f"💰 **T4 REACHED!** @ {res['price']:.2f}\n🏁 **BOOK FULL TRADE**", parse_mode='Markdown')
    elif res["type"] == "EXIT_SL":
        await context.bot.send_message(chat_id=TRADE_CHANNEL_ID, text=f"❌ **SL HIT @ {res['price']:.2f}**\nPosition Closed.")

def main():
    bot_engine.login_angel()
    bot_engine.load_instruments()
    
    app = Application.builder().token(TELEGRAM_BOT_TOKEN).build()
    
    # Listen to ALL text (Channel Posts and Private Messages)
    app.add_handler(MessageHandler(filters.TEXT & (~filters.COMMAND), handle_any_message))
    
    # Live tracker (Every 2 seconds)
    app.job_queue.run_repeating(track_trade_job, interval=2)
    
    logger.info("🚀 Bot is live and polling...")
    app.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == "__main__":
    main()
