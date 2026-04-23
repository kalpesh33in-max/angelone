import asyncio
import os
import re
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any

import pandas as pd
import pyotp
import pytz
import requests
from SmartApi.smartConnect import SmartConnect
from telethon import TelegramClient, events
from telethon.sessions import StringSession

IST = pytz.timezone("Asia/Kolkata")

STEP_POINTS = 30
MAX_TARGET_LEVEL = 4
DUPLICATE_MINUTES = 10
MONITOR_INTERVAL_SECONDS = 3

SCRIP_MASTER_FILE = "OpenAPIScripMaster.json"
SCRIP_MASTER_URL = "https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json"


def env(name: str, default: str | None = None) -> str | None:
    return os.getenv(name, default)


TG_API_ID = int(env("TG_API_ID"))
TG_API_HASH = env("TG_API_HASH")
TG_SESSION_STR = env("TG_SESSION_STR")

SOURCE_CHAT = env("SOURCE_CHAT", "Marketmenia_news")

OUTPUT_BOT_TOKEN = env("PAPER_TRADE_BOT_TOKEN") or env("TELE_TOKEN_MCX")
OUTPUT_CHAT_ID = env("PAPER_TRADE_CHANNEL_ID") or env("CHAT_ID_MCX")

ANGEL_API_KEY = env("ANGEL_API_KEY")
ANGEL_CLIENT_ID = env("ANGEL_CLIENT_ID")
ANGEL_PASSWORD = env("ANGEL_PASSWORD")
ANGEL_TOTP_SECRET = env("ANGEL_TOTP_SECRET")


@dataclass
class Trade:
    strike: int
    option_type: str
    symbol: str
    token: str
    entry: float
    sl: float
    targets: list[float]
    highest_target: int = 0
    last_price_alert: float = 0.0


class Engine:
    def __init__(self) -> None:
        self.smart = None
        self.df = None
        self.trade: Trade | None = None
        self.last_signal: dict[str, datetime] = {}

    def login(self) -> None:
        totp = pyotp.TOTP(ANGEL_TOTP_SECRET).now()
        self.smart = SmartConnect(api_key=ANGEL_API_KEY)
        self.smart.generateSession(ANGEL_CLIENT_ID, ANGEL_PASSWORD, totp)

    def load(self) -> None:
        if not os.path.exists(SCRIP_MASTER_FILE):
            response = requests.get(SCRIP_MASTER_URL, timeout=30)
            response.raise_for_status()
            with open(SCRIP_MASTER_FILE, "wb") as fp:
                fp.write(response.content)

        df = pd.read_json(SCRIP_MASTER_FILE)
        df = df[(df.exch_seg == "NFO") & (df.name == "BANKNIFTY")].copy()
        df["expiry"] = pd.to_datetime(df["expiry"], format="%d%b%Y")
        self.df = df[df.expiry >= datetime.now()].copy()

    def resolve(self, strike: int, option_type: str) -> tuple[str, str]:
        df = self.df[self.df.symbol.str.contains(f"{strike}{option_type}")]
        row = df.sort_values("expiry").iloc[0]
        return row.symbol, row.token

    def ltp(self, symbol: str, token: str) -> float:
        data = self.smart.ltpData("NFO", symbol, token)
        return float(data["data"]["ltp"])

    def parse_dual_match(self, text: str) -> tuple[int, str] | None:
        if "INSTITUTIONAL DUAL MATCH" not in text.upper():
            return None
        match = re.search(r"ACTION:\s*BUY\s+BANKNIFTY\s+(\d+)\s*(CE|PE)", text, re.IGNORECASE)
        if not match:
            match = re.search(r"BANKNIFTY\s+(\d+)\s*(CE|PE)", text, re.IGNORECASE)
        if not match:
            return None
        return int(match.group(1)), match.group(2).upper()

    def duplicate(self, key: str) -> bool:
        now = datetime.now(IST)
        if key in self.last_signal and now - self.last_signal[key] < timedelta(minutes=DUPLICATE_MINUTES):
            return True
        self.last_signal[key] = now
        return False

    def create_trade(self, strike: int, option_type: str) -> Trade:
        symbol, token = self.resolve(strike, option_type)
        price = self.ltp(symbol, token)
        targets = [price + STEP_POINTS * i for i in range(1, MAX_TARGET_LEVEL + 1)]
        return Trade(
            strike=strike,
            option_type=option_type,
            symbol=symbol,
            token=token,
            entry=price,
            sl=price - STEP_POINTS,
            targets=targets,
            highest_target=0,
            last_price_alert=price,
        )

    def process_signal(self, strike: int, option_type: str):
        key = f"{strike}{option_type}"
        if self.duplicate(key):
            return None, "DUP"

        if self.trade and self.trade.option_type != option_type:
            exit_price = self.ltp(self.trade.symbol, self.trade.token)
            old_trade = self.trade
            self.trade = self.create_trade(strike, option_type)
            return ("REV", old_trade, exit_price, self.trade)

        if self.trade:
            return None, "ACTIVE"

        self.trade = self.create_trade(strike, option_type)
        return ("NEW", self.trade)

    def update(self) -> list[str]:
        if not self.trade:
            return []

        trade = self.trade
        price = self.ltp(trade.symbol, trade.token)
        messages: list[str] = []

        if price <= trade.sl:
            messages.append(f"\u274c SL HIT @ {price:.2f}")
            self.trade = None
            return messages

        if trade.highest_target < MAX_TARGET_LEVEL and price >= trade.targets[trade.highest_target]:
            trade.highest_target += 1
            trade.sl = trade.entry if trade.highest_target == 1 else trade.targets[trade.highest_target - 2]
            messages.append(
                f"\U0001f3af BANKNIFTY {trade.strike} {trade.option_type} "
                f"T{trade.highest_target} HIT @ {price:.2f}"
            )

        if price > trade.last_price_alert:
            trade.last_price_alert = price
            messages.append(f"\U0001f4c8 Price Update: {price:.2f}")

        return messages


engine = Engine()


def format_trade(trade: Trade) -> str:
    return "\n".join(
        [
            f"\U0001f525 BANKNIFTY {trade.strike} {trade.option_type}",
            "",
            f"\U0001f4cd Entry: {trade.entry:.2f}",
            f"\U0001f6e1\ufe0f SL: {trade.sl:.2f}",
            f"\U0001f3af T1: {trade.targets[0]:.2f}",
            f"\U0001f3af T2: {trade.targets[1]:.2f}",
            f"\U0001f3af T3: {trade.targets[2]:.2f}",
            f"\U0001f3af T4: {trade.targets[3]:.2f}",
        ]
    )


def send_output(text: str) -> None:
    response = requests.post(
        f"https://api.telegram.org/bot{OUTPUT_BOT_TOKEN}/sendMessage",
        data={"chat_id": OUTPUT_CHAT_ID, "text": text},
        timeout=30,
    )
    response.raise_for_status()


async def monitor_loop() -> None:
    while True:
        for message in engine.update():
            try:
                send_output(message)
            except Exception as exc:
                print(f"Monitor send failed: {exc}")
        await asyncio.sleep(MONITOR_INTERVAL_SECONDS)


async def main() -> None:
    engine.login()
    engine.load()

    client = TelegramClient(StringSession(TG_SESSION_STR), TG_API_ID, TG_API_HASH)
    await client.start()
    print(f"Listening to source chat: {SOURCE_CHAT}")

    @client.on(events.NewMessage())
    async def handler(event):
        chat = await event.get_chat()
        text = event.raw_text or ""
        chat_id = getattr(event, "chat_id", None)
        title = getattr(chat, "title", None)
        username = getattr(chat, "username", None)
        first_name = getattr(chat, "first_name", None)
        print(
            "New message:",
            {
                "chat_id": chat_id,
                "title": title,
                "username": username,
                "first_name": first_name,
                "text": text[:200],
            },
        )

        source_match = False
        source_value = str(SOURCE_CHAT).strip().lower()
        candidates: list[str] = [str(chat_id).lower()]
        for value in (title, username, first_name):
            if value:
                candidates.append(str(value).strip().lower())
        if source_value in candidates:
            source_match = True

        if not source_match:
            print(f"Ignored message from non-source chat. Expected {SOURCE_CHAT}, got {candidates}")
            return

        parsed = engine.parse_dual_match(text)
        if not parsed:
            print("Source message received, but no dual-match pattern found.")
            return

        strike, option_type = parsed
        print(f"Dual match detected: strike={strike}, option_type={option_type}")
        result = engine.process_signal(strike, option_type)

        if not result:
            print("No action taken for signal.")
            return

        if result[0] == "REV":
            _, old_trade, exit_price, new_trade = result
            send_output(f"\U0001f501 EXIT BANKNIFTY {old_trade.strike} {old_trade.option_type} @ {exit_price:.2f}")
            send_output(format_trade(new_trade))
            print("Reversal processed and output sent.")
        elif result[0] == "NEW":
            _, trade = result
            send_output(format_trade(trade))
            print("New trade created and output sent.")

    await asyncio.gather(client.run_until_disconnected(), monitor_loop())


if __name__ == "__main__":
    asyncio.run(main())
