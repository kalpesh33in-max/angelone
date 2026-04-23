import json
import math
import os
import re
from dataclasses import dataclass
from datetime import datetime, time, timedelta

import pandas as pd
import pyotp
import pytz
import requests
from SmartApi.smartConnect import SmartConnect
from telegram import Update
from telegram.ext import Application, ContextTypes, MessageHandler, filters

IST = pytz.timezone("Asia/Kolkata")

LOT_SIZE = 30
STEP_POINTS = 30
MAX_TARGET_LEVEL = 4
MAX_OPEN_TRADES = 5   # ✅ MULTI TRADE ENABLED
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
        return self.targets[level - 1]

    @property
    def next_target(self):
        if self.highest_target_hit >= len(self.targets):
            return None
        return self.target(self.highest_target_hit + 1)


class BankNiftyPaperBot:
    def __init__(self):
        self.smart = None
        self.instruments = None
        self.open_trades = []   # ✅ MULTI TRADE
        self.last_signal = {}

    def login_angel(self):
        totp = pyotp.TOTP(ANGEL_TOTP_SECRET).now()
        self.smart = SmartConnect(api_key=ANGEL_API_KEY)
        self.smart.generateSession(ANGEL_CLIENT_ID, ANGEL_PASSWORD, totp)

    def load_instruments(self):
        if not os.path.exists(SCRIP_MASTER_FILE):
            r = requests.get(SCRIP_MASTER_URL)
            with open(SCRIP_MASTER_FILE, "wb") as f:
                f.write(r.content)

        df = pd.read_json(SCRIP_MASTER_FILE)
        df = df[(df["exch_seg"] == "NFO") & (df["name"] == "BANKNIFTY")].copy()
        df["expiry_dt"] = pd.to_datetime(df["expiry"], format="%d%b%Y")
        df = df[df["expiry_dt"] >= datetime.now()]
        self.instruments = df

    def resolve_option(self, strike, option_type):
        df = self.instruments
        df = df[df["symbol"].str.contains(f"{strike}{option_type}")]
        row = df.sort_values("expiry_dt").iloc[0]
        return str(row["symbol"]), str(row["token"])

    def get_ltp(self, ts, token):
        return float(self.smart.ltpData("NFO", ts, token)["data"]["ltp"])

    def parse_signal(self, text):
        match = re.search(r"BANKNIFTY\s+(\d+)\s*(CE|PE)", text, re.I)
        if not match:
            return None
        return {
            "strike": int(match.group(1)),
            "option_type": match.group(2).upper(),
        }

    def is_duplicate(self, strike, option_type):
        key = f"{strike}{option_type}"
        now = datetime.now(IST)
        last = self.last_signal.get(key)
        if last and now - last < timedelta(minutes=DUPLICATE_MINUTES):
            return True
        self.last_signal[key] = now
        return False

    def enter_trade(self, signal):
        if len(self.open_trades) >= MAX_OPEN_TRADES:
            return None, "MAX_LIMIT"

        if self.is_duplicate(signal["strike"], signal["option_type"]):
            return None, "DUPLICATE"

        ts, token = self.resolve_option(signal["strike"], signal["option_type"])
        ltp = self.get_ltp(ts, token)

        targets = [ltp + STEP_POINTS * i for i in range(1, 5)]

        trade = PaperTrade(
            symbol="BANKNIFTY",
            strike=signal["strike"],
            option_type=signal["option_type"],
            tradingsymbol=ts,
            token=token,
            entry=ltp,
            qty=LOT_SIZE,
            opened_at=datetime.now(IST),
            sl=ltp - STEP_POINTS,
            last_alert_ltp=ltp,
            targets=targets,
        )

        self.open_trades.append(trade)
        return trade, "ENTERED"

    def update_trades(self):
        events = []

        for trade in self.open_trades[:]:
            ltp = self.get_ltp(trade.tradingsymbol, trade.token)

            if ltp <= trade.sl:
                self.open_trades.remove(trade)
                events.append(f"❌ SL HIT {trade.strike} {trade.option_type}")
                continue

            if trade.next_target and ltp >= trade.next_target:
                trade.highest_target_hit += 1
                trade.sl = trade.entry if trade.highest_target_hit == 1 else trade.targets[trade.highest_target_hit - 2]
                events.append(f"🎯 T{trade.highest_target_hit} HIT {ltp}")

        return events


engine = BankNiftyPaperBot()


async def send(context, text):
    await context.bot.send_message(chat_id=TRADE_CHANNEL_ID, text=text)


async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.channel_post or update.message
    if not msg or not msg.text:
        return

    # ✅ LOG EVERYTHING
    print(f"MSG [{msg.chat_id}]: {msg.text[:120]}", flush=True)

    signal = engine.parse_signal(msg.text)
    if not signal:
        return

    trade, status = engine.enter_trade(signal)

    if status == "MAX_LIMIT":
        await send(context, "⚠️ Max trades running")
        return
    if status == "DUPLICATE":
        return

    await send(context, f"🔥 ENTRY {trade.strike} {trade.option_type} @ {trade.entry}")


async def monitor(context: ContextTypes.DEFAULT_TYPE):
    events = engine.update_trades()
    for e in events:
        await send(context, e)


def main():
    print("Starting bot...")
    engine.login_angel()
    engine.load_instruments()

    app = Application.builder().token(TELEGRAM_BOT_TOKEN).build()
    app.add_handler(MessageHandler(filters.TEXT, handle_message))
    app.job_queue.run_repeating(monitor, interval=3)

    app.run_polling()


if __name__ == "__main__":
    main()
