# =========================================================
# PAPER + REAL TRADE BOT
# FINAL STABLE VERSION
# =========================================================

import asyncio
import logging
import os
import re
import time
import json

from dataclasses import dataclass
from datetime import datetime, timedelta

import pandas as pd
import pyotp
import pytz
import requests

from logzero import loglevel

from SmartApi.smartConnect import SmartConnect

from telethon import TelegramClient, events
from telethon.sessions import StringSession

# =========================================================
# TIMEZONE
# =========================================================

IST = pytz.timezone("Asia/Kolkata")

# =========================================================
# SILENT LOGS
# =========================================================

loglevel(logging.CRITICAL)

# =========================================================
# CONFIG
# =========================================================

DEFAULT_STEP_POINTS = 30
MAX_TARGET_LEVEL = 4

DUPLICATE_MINUTES = 10

MONITOR_INTERVAL_SECONDS = 3

ORDER_RETRY_COUNT = 3
ORDER_RETRY_DELAY = 2

SCRIP_MASTER_FILE = "OpenAPIScripMaster.json"

SCRIP_MASTER_URL = (
    "https://margincalculator.angelbroking.com/"
    "OpenAPI_File/files/OpenAPIScripMaster.json"
)

INDEX_UNDERLYINGS = {
    "BANKNIFTY",
    "NIFTY",
    "SENSEX",
    "MIDCPNIFTY",
}

STOCK_UNDERLYINGS = {
    "HDFCBANK",
    "ICICIBANK",
    "RELIANCE",
}

SUPPORTED_UNDERLYINGS = (
    INDEX_UNDERLYINGS
    | STOCK_UNDERLYINGS
)

# =========================================================
# ENV HELPERS
# =========================================================


def env(name, default=None):

    return os.getenv(name, default)


def env_bool(name, default="false"):

    return str(
        env(name, default)
    ).lower() in (
        "true",
        "1",
        "yes",
    )


def env_int(name, default):

    return int(
        str(env(name, default)).strip()
    )

# =========================================================
# ENV
# =========================================================

TG_API_ID = int(env("TG_API_ID"))

TG_API_HASH = env("TG_API_HASH")

TG_SESSION_STR = env("TG_SESSION_STR")

SOURCE_CHAT = env(
    "SOURCE_CHAT",
    "Marketmenia_news",
)

OUTPUT_BOT_TOKEN = (
    env("PAPER_TRADE_BOT_TOKEN")
    or env("TELE_TOKEN_MCX")
)

OUTPUT_CHAT_ID = (
    env("PAPER_TRADE_CHANNEL_ID")
    or env("CHAT_ID_MCX")
)

ANGEL_API_KEY = env("ANGEL_API_KEY")

ANGEL_CLIENT_ID = env("ANGEL_CLIENT_ID")

ANGEL_PASSWORD = env("ANGEL_PASSWORD")

ANGEL_TOTP_SECRET = env("ANGEL_TOTP_SECRET")

REAL_TRADE_ENABLED = env_bool(
    "REAL_TRADE_ENABLED",
    "false",
)

LOT_SIZES = {
    "NIFTY": env_int(
        "NIFTY_LOT_SIZE",
        "65",
    ),
    "BANKNIFTY": env_int(
        "BANKNIFTY_LOT_SIZE",
        "30",
    ),
}

# =========================================================
# TRADE
# =========================================================


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

    targets: list

    step_points: float

    qty: int

    entry_order_id: str | None = None

    highest_target: int = 0

    last_price_alert: float = 0.0

# =========================================================
# SAFE ERROR
# =========================================================


def safe_error(exc):

    try:
        return str(exc)

    except Exception:
        return type(exc).__name__

# =========================================================
# TG OUTPUT
# =========================================================


def send_output(text):

    try:

        print(
            f"SENDING TG:\n{text}"
        )

        response = requests.post(
            f"https://api.telegram.org/bot"
            f"{OUTPUT_BOT_TOKEN}/sendMessage",
            data={
                "chat_id": OUTPUT_CHAT_ID,
                "text": text,
            },
            timeout=30,
        )

        print(
            f"TG STATUS: "
            f"{response.status_code}"
        )

    except Exception as exc:

        print(
            f"TG SEND ERROR: "
            f"{safe_error(exc)}"
        )

# =========================================================
# ENGINE
# =========================================================


class Engine:

    def __init__(self):

        self.smart = None

        self.df = None

        self.trades = {}

        self.last_signal = {}

    # =====================================================
    # LOGIN
    # =====================================================

    def login(self):

        print("ANGEL LOGIN START")

        totp = pyotp.TOTP(
            ANGEL_TOTP_SECRET
        ).now()

        self.smart = SmartConnect(
            api_key=ANGEL_API_KEY
        )

        response = self.smart.generateSession(
            ANGEL_CLIENT_ID,
            ANGEL_PASSWORD,
            totp,
        )

        print(
            f"ANGEL LOGIN SUCCESS: "
            f"{response}"
        )

        return response

    # =====================================================
    # LOAD MASTER
    # =====================================================

    def load(self):

        print("LOADING MASTER")

        if not os.path.exists(
            SCRIP_MASTER_FILE
        ):

            response = requests.get(
                SCRIP_MASTER_URL,
                timeout=30,
            )

            response.raise_for_status()

            with open(
                SCRIP_MASTER_FILE,
                "wb",
            ) as fp:

                fp.write(
                    response.content
                )

        df = pd.read_json(
            SCRIP_MASTER_FILE
        )

        df = df[
            (
                df.exch_seg.isin(
                    [
                        "NFO",
                        "BFO",
                    ]
                )
            )
            & (
                df.name.isin(
                    SUPPORTED_UNDERLYINGS
                )
            )
        ].copy()

        df["expiry"] = pd.to_datetime(
            df["expiry"],
            format="%d%b%Y",
        )

        self.df = df[
            df.expiry >= datetime.now()
        ].copy()

        print(
            f"MASTER LOADED: "
            f"{len(self.df)}"
        )

    # =====================================================
    # STRIKE VALIDATION
    # =====================================================

    def strike_valid(
        self,
        underlying,
        strike,
    ):

        if underlying == "NIFTY":
            return 18000 <= strike <= 30000

        if underlying == "BANKNIFTY":
            return 35000 <= strike <= 70000

        if underlying == "SENSEX":
            return 40000 <= strike <= 100000

        return True

    # =====================================================
    # PARSER
    # =====================================================

    def parse_dual_match(
        self,
        text,
    ):

        upper = text.upper()

        if (
            "INSTITUTIONAL DUAL MATCH"
            not in upper
        ):

            return None

        symbol_pattern = "|".join(
            sorted(
                SUPPORTED_UNDERLYINGS,
                key=len,
                reverse=True,
            )
        )

        matches = re.findall(
            rf"({symbol_pattern})\s+"
            rf"(\d{{4,6}})\s*"
            rf"(CE|PE)",
            upper,
            re.IGNORECASE,
        )

        if not matches:
            return None

        valid = []

        for (
            symbol,
            strike,
            option_type,
        ) in matches:

            strike_int = int(strike)

            if not self.strike_valid(
                symbol,
                strike_int,
            ):

                print(
                    f"INVALID STRIKE: "
                    f"{symbol} "
                    f"{strike_int}"
                )

                continue

            valid.append(
                (
                    symbol.upper(),
                    strike_int,
                    option_type.upper(),
                )
            )

        if not valid:
            return None

        return valid[-1]

    # =====================================================
    # DUPLICATE
    # =====================================================

    def duplicate(
        self,
        key,
    ):

        now = datetime.now(IST)

        if (
            key in self.last_signal
            and now
            - self.last_signal[key]
            < timedelta(
                minutes=DUPLICATE_MINUTES
            )
        ):

            return True

        self.last_signal[key] = now

        return False

    # =====================================================
    # RESOLVE
    # =====================================================

    def resolve(
        self,
        underlying,
        strike,
        option_type,
    ):

        df = self.df[
            (
                self.df.name
                == underlying
            )
            & (
                self.df.symbol.str.contains(
                    f"{strike}{option_type}",
                    regex=False,
                )
            )
        ]

        if df.empty:

            raise RuntimeError(
                f"SCRIP NOT FOUND: "
                f"{underlying} "
                f"{strike} "
                f"{option_type}"
            )

        row = df.sort_values(
            "expiry"
        ).iloc[0]

        return (
            row.symbol,
            row.token,
            row.exch_seg,
        )

    # =====================================================
    # LTP
    # =====================================================

    def ltp(
        self,
        exchange,
        symbol,
        token,
    ):

        response = self.smart.ltpData(
            exchange,
            symbol,
            token,
        )

        print(
            f"LTP RESPONSE: "
            f"{response}"
        )

        if isinstance(
            response,
            str,
        ):

            response = json.loads(
                response
            )

        data = response.get(
            "data",
            {},
        )

        return float(
            data.get("ltp")
        )

    # =====================================================
    # REAL ORDER
    # =====================================================

    def place_real_order(
        self,
        trade,
    ):

        order_params = {

            "variety":
                "NORMAL",

            "tradingsymbol":
                trade.symbol,

            "symboltoken":
                trade.token,

            "transactiontype":
                "BUY",

            "exchange":
                trade.exchange,

            "ordertype":
                "MARKET",

            "producttype":
                "INTRADAY",

            "duration":
                "DAY",

            "price":
                "0",

            "squareoff":
                "0",

            "stoploss":
                "0",

            "quantity":
                str(trade.qty),
        }

        last_error = None

        for attempt in range(
            1,
            ORDER_RETRY_COUNT + 1,
        ):

            try:

                print(
                    f"REAL ORDER "
                    f"ATTEMPT "
                    f"{attempt}"
                )

                response = (
                    self.smart
                    .placeOrderFullResponse(
                        order_params
                    )
                )

                print(
                    f"RAW ANGEL RESPONSE TYPE: "
                    f"{type(response)}"
                )

                print(
                    f"FULL ORDER RESPONSE: "
                    f"{repr(response)}"
                )

                if response is None:

                    raise RuntimeError(
                        "Angel returned None"
                    )

                if response == {}:

                    raise RuntimeError(
                        "Angel returned empty dict"
                    )

                if response == "":

                    raise RuntimeError(
                        "Angel returned empty string"
                    )

                status = str(
                    response.get(
                        "status",
                        ""
                    )
                ).lower()

                message = response.get(
                    "message",
                    ""
                )

                data = response.get(
                    "data",
                    {}
                )

                order_id = data.get(
                    "orderid"
                )

                # SUCCESS

                if (
                    status == "success"
                    and order_id
                ):

                    print(
                        f"ORDER SUCCESS: "
                        f"{order_id}"
                    )

                    return str(order_id)

                raise RuntimeError(
                    f"{message} | "
                    f"{response}"
                )

            except Exception as exc:

                last_error = exc

                print(
                    f"ORDER ERROR: "
                    f"{safe_error(exc)}"
                )

                # RELOGIN

                try:

                    self.login()

                except Exception as relogin_exc:

                    print(
                        f"RELOGIN FAILED: "
                        f"{safe_error(relogin_exc)}"
                    )

                time.sleep(
                    ORDER_RETRY_DELAY
                )

        raise RuntimeError(
            f"FINAL ORDER FAILURE: "
            f"{safe_error(last_error)}"
        )

    # =====================================================
    # STEP
    # =====================================================

    def step_points_for(
        self,
        underlying,
    ):

        if (
            underlying
            in STOCK_UNDERLYINGS
        ):

            return 3.0

        return float(
            DEFAULT_STEP_POINTS
        )

    # =====================================================
    # CREATE TRADE
    # =====================================================

    def create_trade(
        self,
        underlying,
        strike,
        option_type,
    ):

        symbol, token, exchange = (
            self.resolve(
                underlying,
                strike,
                option_type,
            )
        )

        price = self.ltp(
            exchange,
            symbol,
            token,
        )

        step_points = (
            self.step_points_for(
                underlying
            )
        )

        targets = [
            price + step_points * i
            for i in range(
                1,
                MAX_TARGET_LEVEL + 1,
            )
        ]

        return Trade(
            underlying=underlying,
            strike=strike,
            option_type=option_type,
            symbol=symbol,
            token=token,
            exchange=exchange,
            entry=price,
            sl=price
            - step_points,
            targets=targets,
            step_points=step_points,
            qty=LOT_SIZES.get(
                underlying,
                1,
            ),
        )

    # =====================================================
    # PROCESS SIGNAL
    # =====================================================

    def process_signal(
        self,
        underlying,
        strike,
        option_type,
    ):

        key = (
            f"{underlying}_"
            f"{strike}_"
            f"{option_type}"
        )

        if self.duplicate(key):

            print(
                "DUPLICATE SIGNAL"
            )

            return None

        active = self.trades.get(
            underlying
        )

        # REVERSE

        if active:

            if (
                active.option_type
                == option_type
            ):

                print(
                    "SAME SIDE ACTIVE"
                )

                return None

            print(
                "REVERSE SIGNAL"
            )

            del self.trades[
                underlying
            ]

        # PAPER TRADE FIRST

        trade = self.create_trade(
            underlying,
            strike,
            option_type,
        )

        self.trades[
            underlying
        ] = trade

        print(
            "PAPER TRADE CREATED"
        )

        # REAL TRADE SEPARATE

        if REAL_TRADE_ENABLED:

            try:

                order_id = (
                    self.place_real_order(
                        trade
                    )
                )

                trade.entry_order_id = (
                    order_id
                )

            except Exception as exc:

                print(
                    f"REAL TRADE FAILED: "
                    f"{safe_error(exc)}"
                )

        return trade

    # =====================================================
    # UPDATE
    # =====================================================

    def update(self):

        messages = []

        for (
            underlying,
            trade,
        ) in list(
            self.trades.items()
        ):

            try:

                price = self.ltp(
                    trade.exchange,
                    trade.symbol,
                    trade.token,
                )

                # SL

                if price <= trade.sl:

                    messages.append(
                        f"❌ "
                        f"{underlying} "
                        f"SL HIT @ "
                        f"{price:.2f}"
                    )

                    del self.trades[
                        underlying
                    ]

                    continue

                # TARGET

                if (
                    trade.highest_target
                    < MAX_TARGET_LEVEL
                    and price
                    >= trade.targets[
                        trade.highest_target
                    ]
                ):

                    trade.highest_target += 1

                    messages.append(
                        f"🎯 "
                        f"{trade.underlying} "
                        f"{trade.strike} "
                        f"{trade.option_type} "
                        f"T"
                        f"{trade.highest_target} "
                        f" HIT @ "
                        f"{price:.2f}"
                    )

                # PRICE UPDATE

                if (
                    price
                    > trade.last_price_alert
                ):

                    trade.last_price_alert = (
                        price
                    )

                    messages.append(
                        f"📈 "
                        f"{underlying} "
                        f" Price Update: "
                        f"{price:.2f}"
                    )

            except Exception as exc:

                print(
                    f"UPDATE ERROR: "
                    f"{safe_error(exc)}"
                )

        return messages

# =========================================================
# ENGINE
# =========================================================

engine = Engine()

# =========================================================
# FORMAT
# =========================================================


def format_trade(trade):

    lines = [
        f"🔥 "
        f"{trade.underlying} "
        f"{trade.strike} "
        f"{trade.option_type}",
        "",
        f"Qty: {trade.qty}",
    ]

    if trade.entry_order_id:

        lines.append(
            f"BUY Order: "
            f"{trade.entry_order_id}"
        )

    lines.extend(
        [
            f"📍 Entry: "
            f"{trade.entry:.2f}",

            f"🛡️ SL: "
            f"{trade.sl:.2f}",

            f"🎯 T1: "
            f"{trade.targets[0]:.2f}",

            f"🎯 T2: "
            f"{trade.targets[1]:.2f}",

            f"🎯 T3: "
            f"{trade.targets[2]:.2f}",

            f"🎯 T4: "
            f"{trade.targets[3]:.2f}",
        ]
    )

    return "\n".join(lines)

# =========================================================
# MONITOR
# =========================================================


async def monitor_loop():

    while True:

        try:

            messages = engine.update()

            for message in messages:

                send_output(message)

        except Exception as exc:

            print(
                f"MONITOR ERROR: "
                f"{safe_error(exc)}"
            )

        await asyncio.sleep(
            MONITOR_INTERVAL_SECONDS
        )

# =========================================================
# MAIN
# =========================================================


async def main():

    print("BOT STARTING")

    engine.login()

    engine.load()

    client = TelegramClient(
        StringSession(
            TG_SESSION_STR
        ),
        TG_API_ID,
        TG_API_HASH,
    )

    await client.start()

    print(
        f"LISTENING TO: "
        f"{SOURCE_CHAT}"
    )

    @client.on(events.NewMessage())
    async def handler(event):

        try:

            chat = await event.get_chat()

            text = (
                event.raw_text
                or ""
            )

            # =================================================
            # SOURCE PROTECTION
            # =================================================

            source_match = False

            source_value = str(
                SOURCE_CHAT
            ).strip().lower()

            candidates = [
                str(event.chat_id).lower()
            ]

            for value in (
                getattr(
                    chat,
                    "title",
                    None,
                ),
                getattr(
                    chat,
                    "username",
                    None,
                ),
                getattr(
                    chat,
                    "first_name",
                    None,
                ),
            ):

                if value:

                    candidates.append(
                        str(value)
                        .strip()
                        .lower()
                    )

            print(
                f"SOURCE CHECK: "
                f"{candidates}"
            )

            if (
                source_value
                in candidates
            ):

                source_match = True

            if not source_match:

                print(
                    "IGNORED "
                    "NON SOURCE CHAT"
                )

                return

            # =================================================
            # VALID SOURCE
            # =================================================

            print(
                "\n===================="
            )

            print(
                "VALID SOURCE MESSAGE"
            )

            print(
                f"MESSAGE:\n{text}"
            )

            print(
                "====================\n"
            )

            # =================================================
            # PARSE
            # =================================================

            parsed = (
                engine.parse_dual_match(
                    text
                )
            )

            if not parsed:

                print(
                    "NO VALID SIGNAL"
                )

                return

            (
                underlying,
                strike,
                option_type,
            ) = parsed

            print(
                f"SIGNAL: "
                f"{underlying} "
                f"{strike} "
                f"{option_type}"
            )

            trade = (
                engine.process_signal(
                    underlying,
                    strike,
                    option_type,
                )
            )

            if not trade:

                print(
                    "NO TRADE"
                )

                return

            # ALWAYS SEND PAPER ALERT

            send_output(
                format_trade(
                    trade
                )
            )

            print(
                "TRADE CREATED"
            )

        except Exception as exc:

            print(
                f"HANDLER ERROR: "
                f"{safe_error(exc)}"
            )

    await asyncio.gather(
        client.run_until_disconnected(),
        monitor_loop(),
    )

# =========================================================
# START
# =========================================================

if __name__ == "__main__":

    asyncio.run(main())
