import os
import re
from dataclasses import dataclass
from datetime import datetime, timedelta

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
MAX_OPEN_TRADES = 3
DUPLICATE_MINUTES = 10

SCRIP_MASTER_FILE = "OpenAPIScripMaster.json"
SCRIP_MASTER_URL = "https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json"

def env(n): return os.getenv(n)

TELEGRAM_BOT_TOKEN = env("TELE_TOKEN_MCX")
CHAT_ID = env("CHAT_ID_MCX")

ANGEL_API_KEY = env("ANGEL_API_KEY")
ANGEL_CLIENT_ID = env("ANGEL_CLIENT_ID")
ANGEL_PASSWORD = env("ANGEL_PASSWORD")
ANGEL_TOTP_SECRET = env("ANGEL_TOTP_SECRET")


@dataclass
class Trade:
    strike: int
    option_type: str
    ts: str
    token: str
    entry: float
    sl: float
    targets: list
    highest_target: int = 0
    last_price_alert: float = 0


class Engine:
    def __init__(self):
        self.smart = None
        self.df = None
        self.trades = []
        self.last_signal = {}

    def login(self):
        totp = pyotp.TOTP(ANGEL_TOTP_SECRET).now()
        self.smart = SmartConnect(api_key=ANGEL_API_KEY)
        self.smart.generateSession(ANGEL_CLIENT_ID, ANGEL_PASSWORD, totp)

    def load(self):
        if not os.path.exists(SCRIP_MASTER_FILE):
            r = requests.get(SCRIP_MASTER_URL)
            open(SCRIP_MASTER_FILE, "wb").write(r.content)

        df = pd.read_json(SCRIP_MASTER_FILE)
        df = df[(df.exch_seg == "NFO") & (df.name == "BANKNIFTY")]
        df["expiry"] = pd.to_datetime(df["expiry"], format="%d%b%Y")
        self.df = df[df.expiry >= datetime.now()]

    def resolve(self, strike, opt):
        df = self.df[self.df.symbol.str.contains(f"{strike}{opt}")]
        row = df.sort_values("expiry").iloc[0]
        return row.symbol, row.token

    def ltp(self, ts, token):
        return float(self.smart.ltpData("NFO", ts, token)["data"]["ltp"])

    def parse(self, txt):
        m = re.search(r"BANKNIFTY\s+(\d+)\s*(CE|PE)", txt)
        if not m: return None
        return int(m.group(1)), m.group(2)

    def duplicate(self, k):
        now = datetime.now(IST)
        if k in self.last_signal and now - self.last_signal[k] < timedelta(minutes=10):
            return True
        self.last_signal[k] = now
        return False

    def create_trade(self, strike, opt):
        ts, token = self.resolve(strike, opt)
        price = self.ltp(ts, token)

        tgs = [price + STEP_POINTS * i for i in range(1, 5)]

        return Trade(
            strike, opt, ts, token,
            price,
            price - STEP_POINTS,
            tgs,
            0,
            price
        )

    def process_signal(self, strike, opt):
        key = f"{strike}{opt}"
        if self.duplicate(key):
            return None, "DUP"

        # 🔁 reversal
        for t in self.trades[:]:
            if t.option_type != opt:
                exit_price = self.ltp(t.ts, t.token)
                self.trades.remove(t)
                new_trade = self.create_trade(strike, opt)
                self.trades.append(new_trade)
                return ("REV", t, exit_price, new_trade)

        if len(self.trades) >= MAX_OPEN_TRADES:
            return None, "MAX"

        t = self.create_trade(strike, opt)
        self.trades.append(t)
        return ("NEW", t)

    def update(self):
        msgs = []
        for t in self.trades[:]:
            price = self.ltp(t.ts, t.token)

            # ❌ SL
            if price <= t.sl:
                self.trades.remove(t)
                msgs.append(f"❌ BANKNIFTY {t.strike} {t.option_type} SL HIT")
                continue

            # 🎯 target
            if t.highest_target < 4 and price >= t.targets[t.highest_target]:
                t.highest_target += 1
                t.sl = t.entry if t.highest_target == 1 else t.targets[t.highest_target - 2]
                msgs.append(f"🎯 BANKNIFTY {t.strike} {t.option_type} T{t.highest_target} HIT @ {round(price)}")

            # 📈 ONLY UP MOVE ALERT
            if int(price) > int(t.last_price_alert):
                t.last_price_alert = price
                msgs.append(f"📈 BANKNIFTY {t.strike} {t.option_type} @ {round(price)}")

        return msgs


engine = Engine()


def format_trade(t):
    return "\n".join([
        f"🚀 BANKNIFTY {t.strike} {t.option_type} BUY @ {round(t.entry)}",
        f"🛡️ SL: {round(t.sl)}",
        f"🎯 T1: {round(t.targets[0])}",
        f"🎯 T2: {round(t.targets[1])}",
        f"🎯 T3: {round(t.targets[2])}",
        f"🎯 T4: {round(t.targets[3])}",
    ])


async def send(ctx, txt):
    await ctx.bot.send_message(chat_id=CHAT_ID, text=txt)


async def handle(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    msg = update.channel_post or update.message
    if not msg or not msg.text:
        return

    print("MSG:", msg.text[:100])

    parsed = engine.parse(msg.text)
    if not parsed:
        return

    strike, opt = parsed
    res = engine.process_signal(strike, opt)

    if not res:
        return

    if res[0] == "REV":
        _, old, exit_price, new = res
        await send(ctx, f"🔁 EXIT BANKNIFTY {old.strike} {old.option_type} @ {round(exit_price)}")
        await send(ctx, format_trade(new))

    elif res[0] == "NEW":
        _, t = res
        await send(ctx, format_trade(t))


async def monitor(ctx):
    msgs = engine.update()
    for m in msgs:
        await send(ctx, m)


def main():
    engine.login()
    engine.load()

    app = Application.builder().token(TELEGRAM_BOT_TOKEN).build()
    app.add_handler(MessageHandler(filters.TEXT, handle))
    app.job_queue.run_repeating(monitor, interval=3)

    app.run_polling()


if __name__ == "__main__":
    main()
