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

    @property
    def next_target(self):
        if self.highest_target_hit >= len(self.targets):
            return None
        return self.target(self.highest_target_hit + 1)


class BankNiftyPaperBot:
    def __init__(self):
        self.smart = None
        self.instruments = None
        self.open_trade = None
        self.last_signal = {}

    def login_angel(self):
        missing = [
            name for name, value in {
                "ANGEL_API_KEY": ANGEL_API_KEY,
                "ANGEL_CLIENT_ID": ANGEL_CLIENT_ID,
                "ANGEL_PASSWORD": ANGEL_PASSWORD,
                "ANGEL_TOTP_SECRET": ANGEL_TOTP_SECRET,
            }.items()
            if not value
        ]
        if missing:
            raise RuntimeError(f"Missing Angel env: {', '.join(missing)}")

        totp = pyotp.TOTP(ANGEL_TOTP_SECRET).now()
        smart = SmartConnect(api_key=ANGEL_API_KEY)
        session = smart.generateSession(ANGEL_CLIENT_ID, ANGEL_PASSWORD, totp)
        if not session.get("status"):
            raise RuntimeError(f"Angel login failed: {session.get('message')}")
        self.smart = smart

    def load_instruments(self):
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

    def resolve_option(self, strike, option_type):
        if self.instruments is None:
            self.load_instruments()

        df = self.instruments
        df = df[df["symbol"].str.contains(f"{strike}{option_type}", na=False)].copy()
        if df.empty:
            raise RuntimeError(f"No BANKNIFTY option token found for {strike}{option_type}")

        nearest_expiry = df["expiry_dt"].min()
        row = df[df["expiry_dt"] == nearest_expiry].iloc[0]
        return str(row["symbol"]), str(row["token"])

    def get_ltp(self, tradingsymbol, token):
        data = self.smart.ltpData("NFO", tradingsymbol, token)
        if not data.get("status"):
            raise RuntimeError(f"LTP failed: {data.get('message')}")
        return float(data["data"]["ltp"])

    def parse_price(self, text, label):
        match = re.search(
            rf"\b{label}\s*[:=]\s*(\d+(?:\.\d+)?)(?![\d.])(?!(?:\s*pts?\b))",
            text,
            re.I,
        )
        if not match:
            return None
        return float(match.group(1))

    def parse_points(self, text, label):
        match = re.search(rf"\b{label}\s*[:=]\s*(\d+(?:\.\d+)?)\s*pts?\b", text, re.I)
        if not match:
            return None
        return float(match.group(1))

    def parse_signal(self, text):
        patterns = [
            r"(?:ACTION:\s*)?BUY\s+BANKNIFTY\s+(\d+)\s*(CE|PE)\b",
            r"\bBANKNIFTY\s+BUY\s+(\d+)\s*(CE|PE)\b",
            r"\bBANKNIFTY\s+(\d+)\s*(CE|PE)\b",
        ]
        match = None
        for pattern in patterns:
            match = re.search(pattern, text, re.I)
            if match:
                break
        if not match:
            return None
        option_type = match.group(2).upper()
        return {
            "strike": int(match.group(1)),
            "option_type": option_type,
            "signal_name": "CALL" if option_type == "CE" else "PUT",
            "entry": self.parse_price(text, "Entry"),
            "sl": self.parse_price(text, "SL"),
            "sl_points": self.parse_points(text, "SL"),
            "target_points": self.parse_points(text, "TARGET"),
            "targets": [
                target
                for target in (
                    self.parse_price(text, "T1"),
                    self.parse_price(text, "T2"),
                    self.parse_price(text, "T3"),
                    self.parse_price(text, "T4"),
                )
                if target is not None
            ],
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
        strike = signal["strike"]
        option_type = signal["option_type"]
        if self.open_trade is not None:
            return None, "OPEN_TRADE_EXISTS"
        if self.is_duplicate(strike, option_type):
            return None, "DUPLICATE_SIGNAL"

        tradingsymbol, token = self.resolve_option(strike, option_type)
        entry = signal["entry"] or self.get_ltp(tradingsymbol, token)
        sl_points = signal["sl_points"] or STEP_POINTS
        target_points = signal["target_points"]
        sl = signal["sl"] if signal["sl"] is not None else entry - sl_points
        if signal["targets"]:
            targets = signal["targets"]
        elif target_points:
            targets = sorted(
                {
                    entry + STEP_POINTS,
                    entry + target_points,
                    entry + target_points + STEP_POINTS,
                    entry + target_points + (STEP_POINTS * 2),
                }
            )
        else:
            targets = [
                entry + STEP_POINTS,
                entry + (STEP_POINTS * 2),
                entry + (STEP_POINTS * 3),
                entry + (STEP_POINTS * 4),
            ]
        targets = targets[:MAX_TARGET_LEVEL]
        while len(targets) < MAX_TARGET_LEVEL:
            targets.append(entry + (STEP_POINTS * (len(targets) + 1)))
        trade = PaperTrade(
            symbol="BANKNIFTY",
            strike=strike,
            option_type=option_type,
            tradingsymbol=tradingsymbol,
            token=token,
            entry=entry,
            qty=LOT_SIZE,
            opened_at=datetime.now(IST),
            sl=sl,
            last_alert_ltp=entry,
            targets=targets,
        )
        self.open_trade = trade
        return trade, "ENTERED"

    def update_trade(self):
        trade = self.open_trade
        if trade is None:
            return None

        ltp = self.get_ltp(trade.tradingsymbol, trade.token)

        if ltp <= trade.sl:
            closed = trade
            exit_price = trade.sl
            pnl = (exit_price - trade.entry) * trade.qty
            self.open_trade = None
            return {
                "type": "EXIT",
                "trade": closed,
                "ltp": ltp,
                "exit_price": exit_price,
                "pnl": pnl,
                "reason": "SL HIT",
            }

        pnl = (ltp - trade.entry) * trade.qty
        next_target = trade.next_target
        if next_target is not None and ltp >= next_target:
            old_sl = trade.sl
            target_no = trade.highest_target_hit
            while target_no < len(trade.targets) and ltp >= trade.target(target_no + 1):
                target_no += 1
            trade.highest_target_hit = target_no
            if target_no == 1:
                trade.sl = trade.entry
            elif target_no > 1:
                trade.sl = trade.target(target_no - 1)
            trade.last_alert_ltp = max(trade.last_alert_ltp, ltp)

            if target_no >= len(trade.targets):
                closed = trade
                self.open_trade = None
                return {
                    "type": "FINAL_TARGET",
                    "trade": closed,
                    "ltp": ltp,
                    "pnl": pnl,
                    "target_no": target_no,
                    "old_sl": old_sl,
                }

            return {
                "type": "TARGET",
                "trade": trade,
                "ltp": ltp,
                "pnl": pnl,
                "target_no": target_no,
                "old_sl": old_sl,
            }

        if math.floor(ltp) > math.floor(trade.last_alert_ltp):
            trade.last_alert_ltp = ltp
            return {
                "type": "PROGRESS",
                "trade": trade,
                "ltp": ltp,
                "pnl": pnl,
            }

        return None


engine = BankNiftyPaperBot()


def market_open():
    now = datetime.now(IST).time()
    return time(9, 15) <= now <= time(15, 30)


async def send_channel(context, text):
    await context.bot.send_message(chat_id=TRADE_CHANNEL_ID, text=text)


def is_forwarded_message(msg):
    return bool(
        getattr(msg, "forward_origin", None)
        or getattr(msg, "forward_from", None)
        or getattr(msg, "forward_from_chat", None)
    )


def is_allowed_signal_source(msg):
    if TRADE_CHANNEL_ID and str(msg.chat_id) == str(TRADE_CHANNEL_ID):
        return True
    return is_forwarded_message(msg)


def target_ladder_text(trade):
    return "\n".join(
        f"🎯 T{level}: {trade.target(level):.2f}"
        for level in range(1, len(trade.targets) + 1)
    )


def target_action_text(target_no, trade):
    if target_no == 1:
        return f"T1 reached. Exit or move SL cost to cost: {trade.sl:.2f}"
    if target_no == 2:
        return f"T2 reached. Exit or move SL to T1: {trade.sl:.2f}"
    if target_no == 3:
        return f"T3 reached. Exit or move SL to T2: {trade.sl:.2f}"
    return f"T{target_no} reached. Exit or trail SL: {trade.sl:.2f}"


async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.channel_post or update.message
    if not msg or not msg.text:
        print("Telegram update ignored: no text message", flush=True)
        return

    preview = msg.text.replace("\n", " ")[:160]
    print(f"Telegram message received from {msg.chat_id}: {preview}", flush=True)

    if not is_allowed_signal_source(msg):
        print(
            (
                f"Telegram message ignored: chat_id {msg.chat_id} != configured "
                f"{TRADE_CHANNEL_ID} and message is not forwarded"
            ),
            flush=True,
        )
        return

    signal = engine.parse_signal(msg.text)
    if not signal:
        print("Telegram message ignored: no BANKNIFTY buy signal found", flush=True)
        return

    strike = signal["strike"]
    option_type = signal["option_type"]
    try:
        trade, status = engine.enter_trade(signal)
    except Exception as exc:
        print(f"Paper trade error: {exc}", flush=True)
        await send_channel(context, f"PAPER TRADE ERROR: {exc}")
        return

    if status == "OPEN_TRADE_EXISTS":
        print("Paper trade ignored: one trade already open", flush=True)
        await send_channel(context, "PAPER TRADE IGNORED: one trade already open")
        return
    if status == "DUPLICATE_SIGNAL":
        print(f"Paper trade ignored: duplicate {strike}{option_type}", flush=True)
        await send_channel(context, f"PAPER TRADE IGNORED: duplicate {strike}{option_type}")
        return

    print(f"Paper trade entered: BANKNIFTY {strike} {option_type}", flush=True)

    await send_channel(
        context,
        "\n".join(
            [
                f"🔥 BANKNIFTY {trade.strike} {trade.option_type}",
                "",
                f"📍 Entry: {trade.entry:.2f}",
                f"🛡️ SL: {trade.sl:.2f}",
                target_ladder_text(trade),
            ]
        ),
    )


async def monitor_trade(context: ContextTypes.DEFAULT_TYPE):
    if engine.open_trade is None:
        return
    try:
        event = engine.update_trade()
    except Exception as exc:
        await send_channel(context, f"PAPER TRADE MONITOR ERROR: {exc}")
        return

    if not event:
        return

    trade = event["trade"]
    if event["type"] == "PROGRESS":
        await send_channel(
            context,
            f"📈 Price Update: {event['ltp']:.2f}",
        )
    elif event["type"] == "TARGET":
        await send_channel(
            context,
            "\n".join(
                [
                    f"🎯 T{event['target_no']} HIT @ {event['ltp']:.2f} Book or ride",
                    f"✅ SL Shifted: {trade.sl:.2f}",
                ]
            ),
        )
    elif event["type"] == "FINAL_TARGET":
        await send_channel(
            context,
            "\n".join(
                [
                    f"💰 T{event['target_no']} REACHED @ {event['ltp']:.2f}",
                    "🏁 BOOK FULL TRADE",
                ]
            ),
        )
    elif event["type"] == "EXIT":
        await send_channel(
            context,
            "\n".join(
                [
                    f"❌ SL HIT @ {event['ltp']:.2f}",
                    "Position Closed.",
                ]
            ),
        )


def main():
    if not TELEGRAM_BOT_TOKEN or not TRADE_CHANNEL_ID:
        raise RuntimeError(
            "Set PAPER_TRADE_BOT_TOKEN/PAPER_TRADE_CHANNEL_ID or TELE_TOKEN_MCX/CHAT_ID_MCX"
        )

    print("Starting BANKNIFTY paper trade bot...")
    engine.login_angel()
    print("Angel login OK.")
    engine.load_instruments()
    print("BANKNIFTY instruments loaded.")

    app = Application.builder().token(TELEGRAM_BOT_TOKEN).build()
    app.add_handler(MessageHandler(filters.TEXT, handle_message))
    app.job_queue.run_repeating(monitor_trade, interval=3, first=3)
    print("Telegram polling started.")
    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
