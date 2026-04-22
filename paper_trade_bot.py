import os
import re
import logging
from dataclasses import dataclass
from datetime import datetime, time, timedelta

import pandas as pd
import pyotp
import pytz
import requests
from SmartApi.smartConnect import SmartConnect
from telegram import Update
from telegram.ext import Application, ContextTypes, MessageHandler, filters

# --- LOGGING CONFIGURATION ---
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

IST = pytz.timezone("Asia/Kolkata")
LOT_SIZE = 30
PRICE_UPDATE_STEP = 2.0  # Live update every 2 points increase
MAX_OPEN_TRADES = 1

# --- ENVIRONMENT VARIABLES ---
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
        logger.info("Connecting to Angel One...")
        totp = pyotp.TOTP(ANGEL_TOTP_SECRET).now()
        self.smart = SmartConnect(api_key=ANGEL_API_KEY)
        self.smart.generateSession(ANGEL_CLIENT_ID, ANGEL_PASSWORD, totp)
        logger.info("Angel One Login: OK")

    def load_instruments(self):
        logger.info("Fetching Scrip Master...")
        url = "https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json"
        df = pd.read_json(url)
        df = df[(df["exch_seg"] == "NFO") & (df["name"] == "BANKNIFTY")].copy()
        df["expiry_dt"] = pd.to_datetime(df["expiry"], format="%d%b%Y", errors="coerce")
        # Filter for current or future expiry
        today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        self.instruments = df[df["expiry_dt"] >= today].copy()
        logger.info(f"Instruments Loaded: {len(self.instruments)} rows")

    def get_ltp(self, symbol, token):
        try:
            data = self.smart.ltpData("NFO", symbol, token)
            if data.get("status"):
                return float(data["data"]["ltp"])
        except Exception as e:
            logger.error(f"LTP Error: {e}")
        return 0.0

    def parse_signal(self, text):
        # Improved regex for symbol identification
        match = re.search(r"BANKNIFTY\s+(\d+)\s+(CE|PE)", text, re.I)
        if not match: return None
        return {"strike": int(match.group(1)), "type": match.group(2).upper()}

    def enter_trade(self, signal):
        if self.open_trade:
            logger.info("Trade ignored: One trade already active.")
            return None
        
        # Find correct token for nearest expiry
        df = self.instruments
        df = df[df["symbol"].str.contains(f"{signal['strike']}{signal['type']}", na=False)].copy()
        if df.empty: return None
        
        row = df[df["expiry_dt"] == df["expiry_dt"].min()].iloc[0]
        ltp = self.get_ltp(row["symbol"], row["token"])
        if ltp == 0: return None

        # 4-Level Target Ladder
        targets = [ltp + 30, ltp + 60, ltp + 90, ltp + 120]
        
        self.open_trade = PaperTrade(
            symbol="BANKNIFTY", strike=signal["strike"], option_type=signal["type"],
            tradingsymbol=row["symbol"], token=row["token"], entry=ltp, qty=LOT_SIZE,
            sl=ltp - 30, last_alert_price=ltp, targets=targets
        )
        logger.info(f"ENTERED: {row['symbol']} at {ltp}")
        return self.open_trade

    def process_update(self):
        if not self.open_trade: return None
        t = self.open_trade
        ltp = self.get_ltp(t.tradingsymbol, t.token)
        if ltp == 0: return None

        # 1. Stop Loss Check
        if ltp <= t.sl:
            self.open_trade = None
            return {"type": "EXIT_SL", "price": ltp}

        # 2. Target Check & SL Shifting
        for i, target_price in enumerate(t.targets):
            level = i + 1
            if ltp >= target_price and t.highest_target_hit < level:
                t.highest_target_hit = level
                
                # Logic: SL shifts up every target reach
                if level == 1: t.sl = t.entry           # T1 hit -> SL to Cost
                elif level == 2: t.sl = t.targets[0]    # T2 hit -> SL to T1
                elif level == 3: t.sl = t.targets[1]    # T3 hit -> SL to T2
                
                if level == 4:
                    self.open_trade = None
                    return {"type": "T4_COMPLETE", "price": ltp}
                return {"type": "TARGET_HIT", "level": level, "price": ltp, "new_sl": t.sl}

        # 3. Live Price Update (Every 2 points)
        if ltp >= t.last_alert_price + PRICE_UPDATE_STEP:
            t.last_alert_price = ltp
            return {"type": "PRICE_MOVE", "price": ltp}
            
        return None

bot = BankNiftyPaperBot()

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg_obj = update.channel_post or update.message
    if not msg_obj or not msg_obj.text: return
    
    signal = bot.parse_signal(msg_obj.text)
    if not signal: return

    trade = bot.enter_trade(signal)
    if trade:
        # Beautiful clean identification as requested
        msg = (f"🔥 **BANKNIFTY {trade.strike} {trade.option_type}**\n\n"
               f"📍 **Entry:** {trade.entry:.2f}\n"
               f"🛡️ **SL:** {trade.sl:.2f}\n"
               f"🎯 **T1:** {trade.targets[0]:.2f}\n"
               f"🎯 **T2:** {trade.targets[1]:.2f}\n"
               f"🎯 **T3:** {trade.targets[2]:.2f}\n"
               f"🎯 **T4:** {trade.targets[3]:.2f}")
        await context.bot.send_message(chat_id=TRADE_CHANNEL_ID, text=msg, parse_mode='Markdown')

async def track_trade(context: ContextTypes.DEFAULT_TYPE):
    res = bot.process_update()
    if not res: return
    
    if res["type"] == "PRICE_MOVE":
        await context.bot.send_message(chat_id=TRADE_CHANNEL_ID, text=f"📈 **Price Update:** {res['price']:.2f}")
    
    elif res["type"] == "TARGET_HIT":
        msg = (f"🎯 **TARGET {res['level']} HIT!** @ {res['price']:.2f}\n"
               f"✅ SL Shifted up to: **{res['new_sl']:.2f}**")
        await context.bot.send_message(chat_id=TRADE_CHANNEL_ID, text=msg, parse_mode='Markdown')
    
    elif res["type"] == "T4_COMPLETE":
        msg = (f"💰 **T4 REACHED!** @ {res['price']:.2f}\n"
               f"🏁 **BOOK FULL TRADE NOW**")
        await context.bot.send_message(chat_id=TRADE_CHANNEL_ID, text=msg, parse_mode='Markdown')
    
    elif res["type"] == "EXIT_SL":
        await context.bot.send_message(chat_id=TRADE_CHANNEL_ID, text=f"❌ **SL HIT @ {res['price']:.2f}**\nPosition Closed.")

def main():
    bot.login_angel()
    bot.load_instruments()
    app = Application.builder().token(TELEGRAM_BOT_TOKEN).build()
    
    # Message handler for alerts
    app.add_handler(MessageHandler(filters.TEXT, handle_message))
    
    # Background job for live tracking (Interval 2 seconds)
    app.job_queue.run_repeating(track_trade, interval=2)
    
    logger.info("Bot Polling Started...")
    app.run_polling()

if __name__ == "__main__":
    main()
