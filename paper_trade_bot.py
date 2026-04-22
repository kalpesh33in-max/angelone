import json
import math
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

# --- FULL LOGGING ENABLED ---
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

IST = pytz.timezone("Asia/Kolkata")
LOT_SIZE = 30
STEP_POINTS = 30
MAX_OPEN_TRADES = 1
DUPLICATE_MINUTES = 10

BASE_DIR = os.path.dirname(__file__)
SCRIP_MASTER_FILE = os.path.join(BASE_DIR, "OpenAPIScripMaster.json")
SCRIP_MASTER_URL = "https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json"

def env(name, default=None):
    return os.getenv(name, default)

TELEGRAM_BOT_TOKEN = env("PAPER_TRADE_BOT_TOKEN") or env("TELE_TOKEN_MCX")
TRADE_CHANNEL_ID = env("PAPER_TRADE_CHANNEL_ID") or env("CHAT_ID_MCX")

ANGEL_API_KEY = env("ANGEL_API_KEY")
ANGEL_CLIENT_ID = env("ANGEL_CLIENT_ID")
ANGEL_PASSWORD = env("ANGEL_PASSWORD")
ANGEL_TOTP_SECRET = env("ANGEL_TOTP_SECRET")

@dataclass
class PaperTrade:
    symbol: str
    strike: int
    option_type: str
    tradingsymbol: str
    token: str
    entry: float
    qty: int
    opened_at: datetime
    sl: float
    last_alert_ltp: float
    targets: list
    highest_target_hit: int = 0

    def target(self, level):
        if level <= len(self.targets):
            return self.targets[level - 1]
        return self.entry + (STEP_POINTS * level)

class BankNiftyPaperBot:
    def __init__(self):
        self.smart = None
        self.instruments = None
        self.open_trade = None
        self.last_signal = {}

    def login_angel(self):
        logger.info("Attempting Angel One login...")
        totp = pyotp.TOTP(ANGEL_TOTP_SECRET).now()
        smart = SmartConnect(api_key=ANGEL_API_KEY)
        session = smart.generateSession(ANGEL_CLIENT_ID, ANGEL_PASSWORD, totp)
        if not session.get("status"):
            logger.error(f"Angel login failed: {session.get('message')}")
            raise RuntimeError(f"Angel login failed: {session.get('message')}")
        self.smart = smart
        logger.info("Angel login successful.")

    def load_instruments(self):
        logger.info("Loading BANKNIFTY instruments...")
        if not os.path.exists(SCRIP_MASTER_FILE):
            response = requests.get(SCRIP_MASTER_URL, timeout=45)
            response.raise_for_status()
            with open(SCRIP_MASTER_FILE, "wb") as f:
                f.write(response.content)
        df = pd.read_json(SCRIP_MASTER_FILE)
        df = df[(df["exch_seg"] == "NFO") & (df["name"] == "BANKNIFTY")].copy()
        df["expiry_dt"] = pd.to_datetime(df["expiry"], format="%d%b%Y", errors="coerce")
        today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        df = df[df["expiry_dt"] >= today].copy()
        self.instruments = df
        logger.info(f"Loaded {len(df)} instruments.")

    def resolve_option(self, strike, option_type):
        if self.instruments is None: self.load_instruments()
        df = self.instruments
        df = df[df["symbol"].str.contains(f"{strike}{option_type}", na=False)].copy()
        if df.empty: 
            logger.error(f"No BANKNIFTY token for {strike}{option_type}")
            raise RuntimeError(f"No BANKNIFTY token found for {strike}{option_type}")
        nearest_expiry = df["expiry_dt"].min()
        row = df[df["expiry_dt"] == nearest_expiry].iloc[0]
        return str(row["symbol"]), str(row["token"])

    def get_ltp(self, tradingsymbol, token):
        data = self.smart.ltpData("NFO", tradingsymbol, token)
        if not data.get("status"):
            logger.warning(f"LTP fetch failed for {tradingsymbol}")
            return 0.0
        return float(data["data"]["ltp"])

    def parse_signal(self, text):
        logger.info(f"Incoming message: {text[:60]}...")
        # Flexibly match 'BUY BANKNIFTY <Strike> <Type>'
        match = re.search(r"BUY\s+BANKNIFTY\s+(\d+)\s+(CE|PE)", text, re.I)
        if not match: 
            logger.info("Message does not contain a valid BUY signal.")
            return None
        
        strike = int(match.group(1))
        option_type = match.group(2).upper()
        
        # Parse points directly from the alert text
        sl_match = re.search(r"SL:\s*(\d+)", text, re.I)
        tg_match = re.search(r"TARGET:\s*(\d+)", text, re.I)
        
        logger.info(f"Valid Signal: {strike} {option_type}")
        return {
            "strike": strike,
            "option_type": option_type,
            "sl_points": float(sl_match.group(1)) if sl_match else STEP_POINTS,
            "target_points": float(tg_match.group(1)) if tg_match else 60.0
        }

    def is_duplicate(self, strike, option_type):
        key = f"{strike}{option_type}"
        now = datetime.now(IST)
        last = self.last_signal.get(key)
        if last and now - last < timedelta(minutes=DUPLICATE_MINUTES):
            logger.info(f"Duplicate alert for {key} ignored.")
            return True
        self.last_signal[key] = now
        return False

    def enter_trade(self, signal):
        if self.open_trade:
            logger.info("Signal ignored: One trade is already active.")
            return None, "OPEN_TRADE_EXISTS"
        if self.is_duplicate(signal["strike"], signal["option_type"]):
            return None, "DUPLICATE_SIGNAL"

        symbol, token = self.resolve_option(signal["strike"], signal["option_type"])
        entry = self.get_ltp(symbol, token)
        if entry == 0.0: return None, "LTP_ERROR"

        sl = entry - signal["sl_points"]
        # Ladder based on T1: 30pts, T2: 60pts (from Target), T3: 90pts
        targets = [entry + 30, entry + signal["target_points"], entry + signal["target_points"] + 30]
        
        trade = PaperTrade(
            symbol="BANKNIFTY", strike=signal["strike"], option_type=signal["option_type"],
            tradingsymbol=symbol, token=token, entry=entry, qty=LOT_SIZE,
            opened_at=datetime.now(IST), sl=sl, last_alert_ltp=entry, targets=targets
        )
        self.open_trade = trade
        logger.info(f"ENTRY: {symbol} @ {entry}")
        return trade, "ENTERED"

    def update_trade(self):
        if not self.open_trade: return None
        trade = self.open_trade
        ltp = self.get_ltp(trade.tradingsymbol, trade.token)

        if ltp <= trade.sl:
            logger.info(f"EXIT (SL): {trade.tradingsymbol} hit {ltp}")
            self.open_trade = None
            return {"type": "EXIT", "trade": trade, "ltp": ltp}

        next_tg = trade.target(trade.highest_target_hit + 1)
        if ltp >= next_tg:
            trade.highest_target_hit += 1
            logger.info(f"TARGET {trade.highest_target_hit}: Hit at {ltp}")
            if trade.highest_target_hit == 1: trade.sl = trade.entry
            elif trade.highest_target_hit == 2: trade.sl = trade.target(1)
            
            if trade.highest_target_hit >= 3:
                self.open_trade = None
                return {"type": "FINAL_TARGET", "trade": trade, "ltp": ltp}
            return {"type": "TARGET", "trade": trade, "ltp": ltp, "target_no": trade.highest_target_hit}

        return None

engine = BankNiftyPaperBot()

def market_open():
    now = datetime.now(IST).time()
    is_open = time(9, 15) <= now <= time(15, 30)
    if not is_open:
        logger.info(f"Signal ignored. Market is currently closed ({now}).")
    return is_open

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.channel_post or update.message
    if not msg or not msg.text: return
    
    signal = engine.parse_signal(msg.text)
    if not signal: return

    if not market_open():
        await context.bot.send_message(chat_id=TRADE_CHANNEL_ID, text="PAPER TRADE IGNORED: market closed")
        return

    trade, status = engine.enter_trade(signal)
    if trade:
        text = (f"✅ **PAPER TRADE ENTERED**\n"
                f"BANKNIFTY {trade.strike} {trade.option_type}\n"
                f"Entry: {trade.entry:.2f} | SL: {trade.sl:.2f}\n"
                f"T1: {trade.target(1):.2f} | T2: {trade.target(2):.2f}")
        await context.bot.send_message(chat_id=TRADE_CHANNEL_ID, text=text, parse_mode='Markdown')

async def monitor_trade(context: ContextTypes.DEFAULT_TYPE):
    if not market_open() or not engine.open_trade: return
    event = engine.update_trade()
    if not event: return
    
    t = event["trade"]
    if event["type"] == "TARGET":
        msg = f"🎯 **TARGET {event['target_no']} HIT**\n{t.strike} {t.option_type} @ {event['ltp']:.2f}\nSL Trailed: {t.sl:.2f}"
    elif event["type"] == "FINAL_TARGET":
        msg = f"💰 **FINAL TARGET HIT**\n{t.strike} {t.option_type} Closed @ {event['ltp']:.2f}"
    elif event["type"] == "EXIT":
        msg = f"❌ **SL HIT - EXIT**\n{t.strike} {t.option_type} @ {event['ltp']:.2f}"
    
    await context.bot.send_message(chat_id=TRADE_CHANNEL_ID, text=msg, parse_mode='Markdown')

def main():
    engine.login_angel()
    engine.load_instruments()
    app = Application.builder().token(TELEGRAM_BOT_TOKEN).build()
    app.add_handler(MessageHandler(filters.TEXT, handle_message))
    app.job_queue.run_repeating(monitor_trade, interval=3, first=3)
    logger.info("System fully operational.")
    app.run_polling(drop_pending_updates=True)

if __name__ == "__main__":
    main()
