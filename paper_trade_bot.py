import asyncio
import logging
import os
import re
import threading
import time
import json
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any

import pandas as pd
import pyotp
import pytz
import requests
from logzero import loglevel
from SmartApi.smartConnect import SmartConnect
from SmartApi.smartWebSocketV2 import SmartWebSocketV2
from telethon import TelegramClient, events
from telethon.sessions import StringSession

IST = pytz.timezone("Asia/Kolkata")

# Silence noisy logs
loglevel(logging.CRITICAL)

DEFAULT_STEP_POINTS = 30
MAX_TARGET_LEVEL = 4
DUPLICATE_MINUTES = 10
MONITOR_INTERVAL_SECONDS = 3
ORDER_RETRY_COUNT = 3
ORDER_RETRY_DELAY = 1.5

SCRIP_MASTER_FILE = "OpenAPIScripMaster.json"
SCRIP_MASTER_URL = "https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json"

INDEX_UNDERLYINGS = {"BANKNIFTY", "NIFTY", "SENSEX", "MIDCPNIFTY"}
STOCK_UNDERLYINGS = {"HDFCBANK", "ICICIBANK", "RELIANCE"}
SUPPORTED_UNDERLYINGS = INDEX_UNDERLYINGS | STOCK_UNDERLYINGS


def env(name: str, default: str | None = None) -> str | None:
    return os.getenv(name, default)


def env_bool(name: str, default: str = "false") -> bool:
    return str(env(name, default)).lower() in ("true", "1", "yes")


def env_int(name: str, default: str) -> int:
    return int(str(env(name, default)).strip())


def parse_symbols(value: str | None, default: set[str]) -> set[str]:
    if not value:
        return set(default)
    symbols = {item.strip().upper() for item in value.split(",") if item.strip()}
    return symbols or set(default)


def parse_hhmm(value: str, default: str) -> datetime.time:
    try:
        return datetime.strptime(value, "%H:%M").time()
    except Exception:
        return datetime.strptime(default, "%H:%M").time()


USE_ANGEL_WS = env_bool("USE_ANGEL_WS", "false")
WS_MAX_AGE_SECONDS = float(env("WS_MAX_AGE_SECONDS", "5") or 5)

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

REAL_TRADE_ENABLED = env_bool("REAL_TRADE_ENABLED", "false")
REAL_PRODUCT_TYPE = str(env("REAL_PRODUCT_TYPE", "INTRADAY")).upper()
REAL_ORDER_TYPE = str(env("REAL_ORDER_TYPE", "MARKET")).upper()
REAL_ORDER_VARIETY = str(env("REAL_ORDER_VARIETY", "NORMAL")).upper()

MAX_TRADES_PER_DAY = env_int("MAX_TRADES_PER_DAY", "5")

ALLOW_REAL_TRADING_AFTER_RAW = str(env("ALLOW_REAL_TRADING_AFTER", "09:20"))
STOP_REAL_TRADING_AFTER_RAW = str(env("STOP_REAL_TRADING_AFTER", "15:10"))

ALLOW_REAL_TRADING_AFTER = parse_hhmm(ALLOW_REAL_TRADING_AFTER_RAW, "09:20")
STOP_REAL_TRADING_AFTER = parse_hhmm(STOP_REAL_TRADING_AFTER_RAW, "15:10")

TRADE_UNDERLYINGS = parse_symbols(
    env("TRADE_UNDERLYINGS", "NIFTY,BANKNIFTY"),
    {"NIFTY", "BANKNIFTY"},
)

REAL_ALLOWED_UNDERLYINGS = parse_symbols(
    env("REAL_ALLOWED_UNDERLYINGS", "NIFTY,BANKNIFTY"),
    {"NIFTY", "BANKNIFTY"},
)

KEEPALIVE_ENABLED = env_bool("KEEPALIVE_ENABLED", "true")
KEEPALIVE_INTERVAL_SECONDS = env_int("KEEPALIVE_INTERVAL_SECONDS", "300")

KEEPALIVE_START_RAW = str(env("KEEPALIVE_START", "09:00"))
KEEPALIVE_END_RAW = str(env("KEEPALIVE_END", "15:30"))

KEEPALIVE_START = parse_hhmm(KEEPALIVE_START_RAW, "09:00")
KEEPALIVE_END = parse_hhmm(KEEPALIVE_END_RAW, "15:30")

KEEPALIVE_LOG_ENABLED = env_bool("KEEPALIVE_LOG_ENABLED", "false")
STARTUP_CONFIRMATION_ENABLED = env_bool("STARTUP_CONFIRMATION_ENABLED", "true")

LOT_SIZES = {
    "NIFTY": env_int("NIFTY_LOT_SIZE", "65"),
    "BANKNIFTY": env_int("BANKNIFTY_LOT_SIZE", "30"),
}


@dataclass
class Trade:
    underlying: str
    strike: int
    option_type: str
    symbol: str
    token: str
    exchange: str
    entry: float
    sl: float
    targets: list[float]
    step_points: float
    qty: int
    entry_order_id: str | None = None
    exit_order_id: str | None = None
    highest_target: int = 0
    last_price_alert: float = 0.0


def safe_error_detail(exc: Exception) -> str:
    try:
        detail = str(exc) or type(exc).__name__
    except Exception:
        return type(exc).__name__

    secrets = [
        TG_API_HASH,
        TG_SESSION_STR,
        OUTPUT_BOT_TOKEN,
        ANGEL_API_KEY,
        ANGEL_CLIENT_ID,
        ANGEL_PASSWORD,
        ANGEL_TOTP_SECRET,
    ]

    for secret in secrets:
        if secret and secret in detail:
            detail = detail.replace(secret, "***")

    detail = re.sub(r"\b\d{6,12}:[A-Za-z0-9_-]{20,}\b", "***", detail)
    return detail


class Engine:
    def __init__(self) -> None:
        self.smart = None
        self.df = None
        self.trades: dict[str, Trade] = {}
        self.last_signal: dict[str, datetime] = {}

        self.real_trade_day = datetime.now(IST).date()
        self.real_trades_today = 0

    # ---------------------------------------------------------
    # LOGIN
    # ---------------------------------------------------------

    def login(self) -> None:
        try:
            totp = pyotp.TOTP(ANGEL_TOTP_SECRET).now()

            self.smart = SmartConnect(api_key=ANGEL_API_KEY)

            response = self.smart.generateSession(
                ANGEL_CLIENT_ID,
                ANGEL_PASSWORD,
                totp,
            )

            print("Angel login success")

            return response

        except Exception as exc:
            raise RuntimeError(
                f"Angel login failed: {safe_error_detail(exc)}"
            ) from None

    # ---------------------------------------------------------
    # LOAD MASTER
    # ---------------------------------------------------------

    def load(self) -> None:
        if not os.path.exists(SCRIP_MASTER_FILE):
            response = requests.get(SCRIP_MASTER_URL, timeout=30)
            response.raise_for_status()

            with open(SCRIP_MASTER_FILE, "wb") as fp:
                fp.write(response.content)

        df = pd.read_json(SCRIP_MASTER_FILE)

        df = df[
            (df.exch_seg.isin(["NFO", "BFO"]))
            & (df.name.isin(SUPPORTED_UNDERLYINGS))
        ].copy()

        df["expiry"] = pd.to_datetime(df["expiry"], format="%d%b%Y")

        self.df = df[df.expiry >= datetime.now()].copy()

        print(f"Scrip master loaded: {len(self.df)} rows")

    # ---------------------------------------------------------
    # VALIDATION
    # ---------------------------------------------------------

    def strike_valid(self, underlying: str, strike: int) -> bool:

        if underlying == "NIFTY":
            return 18000 <= strike <= 30000

        if underlying == "BANKNIFTY":
            return 35000 <= strike <= 70000

        if underlying == "SENSEX":
            return 40000 <= strike <= 100000

        if underlying == "MIDCPNIFTY":
            return 5000 <= strike <= 20000

        return True

    # ---------------------------------------------------------
    # PARSER FIX
    # ---------------------------------------------------------

    def parse_dual_match(
        self,
        text: str
    ) -> tuple[str, int, str] | None:

        upper = text.upper()

        if "INSTITUTIONAL DUAL MATCH" not in upper:
            return None

        symbol_pattern = "|".join(
            sorted(SUPPORTED_UNDERLYINGS, key=len, reverse=True)
        )

        matches = re.findall(
            rf"ACTION:\s*BUY\s+({symbol_pattern})\s+(\d{{4,6}})\s*(CE|PE)",
            upper,
            re.IGNORECASE,
        )

        if not matches:
            matches = re.findall(
                rf"({symbol_pattern})\s+(\d{{4,6}})\s*(CE|PE)",
                upper,
                re.IGNORECASE,
            )

        if not matches:
            return None

        valid_matches = []

        for symbol, strike, option_type in matches:

            strike_int = int(strike)

            if not self.strike_valid(symbol, strike_int):
                print(f"Rejected invalid strike: {symbol} {strike_int}")
                continue

            valid_matches.append(
                (
                    symbol.upper(),
                    strike_int,
                    option_type.upper(),
                )
            )

        if not valid_matches:
            return None

        # Use LAST valid match
        return valid_matches[-1]

    # ---------------------------------------------------------
    # RESOLVE
    # ---------------------------------------------------------

    def resolve(
        self,
        underlying: str,
        strike: int,
        option_type: str,
    ) -> tuple[str, str, str]:

        df = self.df[
            (self.df.name == underlying)
            & (
                self.df.symbol.str.contains(
                    f"{strike}{option_type}",
                    regex=False,
                )
            )
        ]

        if df.empty:
            raise RuntimeError(
                f"Scrip not found: {underlying} {strike} {option_type}"
            )

        row = df.sort_values("expiry").iloc[0]

        return row.symbol, row.token, row.exch_seg

    # ---------------------------------------------------------
    # LTP
    # ---------------------------------------------------------

    def ltp(
        self,
        exchange: str,
        symbol: str,
        token: str,
    ) -> float:

        try:
            response = self.smart.ltpData(exchange, symbol, token)

            print(f"LTP RAW RESPONSE: {response}")

            if isinstance(response, str):
                response = json.loads(response)

            if isinstance(response, dict):

                data_block = response.get("data")

                if isinstance(data_block, str):
                    data_block = json.loads(data_block)

                if isinstance(data_block, dict):

                    raw = (
                        data_block.get("ltp")
                        or data_block.get("LTP")
                        or data_block.get("last_traded_price")
                    )

                    if raw is not None:
                        return float(raw)

            raise RuntimeError("No LTP found")

        except Exception as exc:
            raise RuntimeError(
                f"LTP fetch failed for {symbol}: {safe_error_detail(exc)}"
            ) from None

    # ---------------------------------------------------------
    # ORDER PARSER FIX
    # ---------------------------------------------------------

    def extract_order_id(self, response: Any) -> str:

        print(f"ORDER RAW RESPONSE: {response}")

        if response is None:
            raise RuntimeError("Angel returned NONE response")

        if response == "":
            raise RuntimeError("Angel returned EMPTY response")

        if isinstance(response, dict):

            data = response.get("data")

            if isinstance(data, dict):

                order_id = (
                    data.get("orderid")
                    or data.get("order_id")
                )

                if order_id:
                    return str(order_id)

            order_id = (
                response.get("orderid")
                or response.get("order_id")
            )

            if order_id:
                return str(order_id)

            message = (
                response.get("message")
                or response.get("error")
                or response.get("status")
                or str(response)
            )

            raise RuntimeError(message)

        if response:
            return str(response)

        raise RuntimeError("Unknown order failure")

    # ---------------------------------------------------------
    # ORDER WITH RETRY FIX
    # ---------------------------------------------------------

    def place_real_order(
        self,
        trade: Trade,
        transaction_type: str,
    ) -> str:

        if not REAL_TRADE_ENABLED:
            return ""

        order_params = {
            "variety": REAL_ORDER_VARIETY,
            "tradingsymbol": trade.symbol,
            "symboltoken": trade.token,
            "transactiontype": transaction_type,
            "exchange": trade.exchange,
            "ordertype": REAL_ORDER_TYPE,
            "producttype": REAL_PRODUCT_TYPE,
            "duration": "DAY",
            "price": "0",
            "squareoff": "0",
            "stoploss": "0",
            "quantity": str(trade.qty),
        }

        last_error = None

        for attempt in range(1, ORDER_RETRY_COUNT + 1):

            try:

                print(
                    f"{transaction_type} ORDER ATTEMPT {attempt} "
                    f"FOR {trade.symbol}"
                )

                response = self.smart.placeOrder(order_params)

                order_id = self.extract_order_id(response)

                print(f"ORDER SUCCESS: {order_id}")

                return order_id

            except Exception as exc:

                last_error = exc

                print(
                    f"ORDER ATTEMPT FAILED: "
                    f"{safe_error_detail(exc)}"
                )

                try:
                    self.login()
                except Exception:
                    pass

                time.sleep(ORDER_RETRY_DELAY)

        raise RuntimeError(
            f"{transaction_type} order failed after retries: "
            f"{safe_error_detail(last_error)}"
        )

    # ---------------------------------------------------------
    # OTHER FUNCTIONS
    # ---------------------------------------------------------

    def duplicate(self, key: str) -> bool:

        now = datetime.now(IST)

        if (
            key in self.last_signal
            and now - self.last_signal[key]
            < timedelta(minutes=DUPLICATE_MINUTES)
        ):
            return True

        self.last_signal[key] = now

        return False

    def step_points_for(self, underlying: str) -> float:

        if underlying in STOCK_UNDERLYINGS:
            return 3.0

        return float(DEFAULT_STEP_POINTS)

    def qty_for(self, underlying: str) -> int:

        return LOT_SIZES.get(underlying, 1)

    def create_trade(
        self,
        underlying: str,
        strike: int,
        option_type: str,
    ) -> Trade:

        symbol, token, exchange = self.resolve(
            underlying,
            strike,
            option_type,
        )

        price = self.ltp(exchange, symbol, token)

        step_points = self.step_points_for(underlying)

        targets = [
            price + step_points * i
            for i in range(1, MAX_TARGET_LEVEL + 1)
        ]

        return Trade(
            underlying=underlying,
            strike=strike,
            option_type=option_type,
            symbol=symbol,
            token=token,
            exchange=exchange,
            entry=price,
            sl=price - step_points,
            targets=targets,
            step_points=step_points,
            qty=self.qty_for(underlying),
        )

    def process_signal(
        self,
        underlying: str,
        strike: int,
        option_type: str,
    ):

        key = f"{underlying}_{strike}_{option_type}"

        if self.duplicate(key):
            return None, "DUP"

        active = self.trades.get(underlying)

        if active:
            if active.option_type == option_type:
                return None, "ACTIVE"

            print("Reverse signal detected")

        trade = self.create_trade(
            underlying,
            strike,
            option_type,
        )

        if REAL_TRADE_ENABLED:

            order_id = self.place_real_order(
                trade,
                "BUY",
            )

            trade.entry_order_id = order_id

        self.trades[underlying] = trade

        return ("NEW", trade)

    def update(self):

        if not self.trades:
            return []

        messages = []

        for underlying, trade in list(self.trades.items()):

            try:

                price = self.ltp(
                    trade.exchange,
                    trade.symbol,
                    trade.token,
                )

                if price <= trade.sl:

                    messages.append(
                        f"❌ {underlying} SL HIT @ {price:.2f}"
                    )

                    del self.trades[underlying]

                    continue

                if (
                    trade.highest_target < MAX_TARGET_LEVEL
                    and price >= trade.targets[trade.highest_target]
                ):

                    trade.highest_target += 1

                    messages.append(
                        f"🎯 "
                        f"{trade.underlying} "
                        f"{trade.strike} "
                        f"{trade.option_type} "
                        f"T{trade.highest_target} "
                        f"HIT @ {price:.2f}"
                    )

                if price > trade.last_price_alert:

                    trade.last_price_alert = price

                    messages.append(
                        f"📈 "
                        f"{underlying} "
                        f"Price Update: {price:.2f}"
                    )

            except Exception as exc:

                print(
                    f"Monitor update failed: "
                    f"{safe_error_detail(exc)}"
                )

        return messages


engine = Engine()


def format_trade(trade: Trade) -> str:

    lines = [
        f"🔥 {trade.underlying} {trade.strike} {trade.option_type}",
        "",
        f"Qty: {trade.qty}",
    ]

    if trade.entry_order_id:
        lines.append(f"BUY Order: {trade.entry_order_id}")

    lines.extend(
        [
            f"📍 Entry: {trade.entry:.2f}",
            f"🛡️ SL: {trade.sl:.2f}",
            f"🎯 T1: {trade.targets[0]:.2f}",
            f"🎯 T2: {trade.targets[1]:.2f}",
            f"🎯 T3: {trade.targets[2]:.2f}",
            f"🎯 T4: {trade.targets[3]:.2f}",
        ]
    )

    return "\n".join(lines)


def send_output(text: str) -> None:

    response = requests.post(
        f"https://api.telegram.org/bot{OUTPUT_BOT_TOKEN}/sendMessage",
        data={
            "chat_id": OUTPUT_CHAT_ID,
            "text": text,
        },
        timeout=30,
    )

    print(
        f"Telegram send status: "
        f"{response.status_code}"
    )


async def monitor_loop():

    while True:

        try:

            messages = engine.update()

            for message in messages:

                try:
                    send_output(message)

                except Exception as exc:
                    print(
                        f"Send failed: "
                        f"{safe_error_detail(exc)}"
                    )

        except Exception as exc:

            print(
                f"Monitor loop failed: "
                f"{safe_error_detail(exc)}"
            )

        await asyncio.sleep(MONITOR_INTERVAL_SECONDS)


async def main():

    engine.login()
    engine.load()

    client = TelegramClient(
        StringSession(TG_SESSION_STR),
        TG_API_ID,
        TG_API_HASH,
    )

    await client.start()

    print(f"Listening to source chat: {SOURCE_CHAT}")

    @client.on(events.NewMessage())
    async def handler(event):

        chat = await event.get_chat()

        text = event.raw_text or ""

        print(f"NEW MESSAGE:\n{text}")

        parsed = engine.parse_dual_match(text)

        if not parsed:
            print("No valid signal")
            return

        underlying, strike, option_type = parsed

        print(
            f"SIGNAL DETECTED: "
            f"{underlying} "
            f"{strike} "
            f"{option_type}"
        )

        try:

            result = engine.process_signal(
                underlying,
                strike,
                option_type,
            )

            if not result:
                return

            if result[0] == "NEW":

                _, trade = result

                send_output(format_trade(trade))

        except Exception as exc:

            print(
                f"SIGNAL PROCESSING FAILED: "
                f"{safe_error_detail(exc)}"
            )

    await asyncio.gather(
        client.run_until_disconnected(),
        monitor_loop(),
    )


if __name__ == "__main__":
    asyncio.run(main())
