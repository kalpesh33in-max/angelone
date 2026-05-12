import asyncio
import json
import os
import re
import time
from dataclasses import dataclass
from datetime import datetime, timedelta

import pandas as pd
import pyotp
import pytz
import requests

from SmartApi.smartConnect import SmartConnect
from telethon import TelegramClient, events
from telethon.sessions import StringSession

IST = pytz.timezone("Asia/Kolkata")

# =========================
# ENV
# =========================

def env(k, d=None):
    return os.getenv(k, d)

def env_bool(k, d="false"):
    return str(env(k, d)).lower() in ("true", "1", "yes")

TG_API_ID = int(env("TG_API_ID"))
TG_API_HASH = env("TG_API_HASH")
TG_SESSION_STR = env("TG_SESSION_STR")

SOURCE_CHAT = env(
    "SOURCE_CHAT",
    "Marketmenia_news",
)

BOT_TOKEN = (
    env("PAPER_TRADE_BOT_TOKEN")
    or env("TELE_TOKEN_MCX")
)

CHAT_ID = (
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
    "NIFTY": 65,
    "BANKNIFTY": 30,
}

# =========================
# CONFIG
# =========================

STEP = 30
MAX_TARGET = 4
MONITOR_DELAY = 3
DUP_MIN = 10

MASTER_FILE = "OpenAPIScripMaster.json"

MASTER_URL = (
    "https://margincalculator.angelbroking.com/"
    "OpenAPI_File/files/OpenAPIScripMaster.json"
)

SUPPORTED = {
    "NIFTY",
    "BANKNIFTY",
    "SENSEX",
    "MIDCPNIFTY",
    "HDFCBANK",
    "ICICIBANK",
    "RELIANCE",
}

# =========================
# HELPERS
# =========================

def safe(e):
    try:
        return str(e)
    except:
        return type(e).__name__

def tg(text):

    print(f"TG:\n{text}")

    try:

        r = requests.post(
            f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
            data={
                "chat_id": CHAT_ID,
                "text": text,
            },
            timeout=30,
        )

        print(f"TG STATUS: {r.status_code}")

    except Exception as e:

        print(f"TG ERROR: {safe(e)}")

# =========================
# TRADE
# =========================

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

    qty: int

    order_id: str | None = None

    target_hit: int = 0
    last_alert: float = 0

# =========================
# ENGINE
# =========================

class Engine:

    def __init__(self):

        self.smart = None
        self.df = None

        self.trades = {}
        self.last_signal = {}

    # =====================

    def login(self):

        print("ANGEL LOGIN")

        self.smart = SmartConnect(
            api_key=ANGEL_API_KEY
        )

        totp = pyotp.TOTP(
            ANGEL_TOTP_SECRET
        ).now()

        self.smart.generateSession(
            ANGEL_CLIENT_ID,
            ANGEL_PASSWORD,
            totp,
        )

    # =====================

    def load(self):

        print("LOAD MASTER")

        if not os.path.exists(MASTER_FILE):

            r = requests.get(
                MASTER_URL,
                timeout=30,
            )

            with open(MASTER_FILE, "wb") as f:
                f.write(r.content)

        df = pd.read_json(MASTER_FILE)

        df = df[
            (df.exch_seg.isin(["NFO", "BFO"]))
            & (df.name.isin(SUPPORTED))
        ].copy()

        df["expiry"] = pd.to_datetime(
            df["expiry"],
            format="%d%b%Y",
        )

        self.df = df[
            df.expiry >= datetime.now()
        ].copy()

    # =====================

    def strike_ok(self, u, s):

        if u == "NIFTY":
            return 18000 <= s <= 30000

        if u == "BANKNIFTY":
            return 35000 <= s <= 70000

        return True

    # =====================

    def parse(self, text):

        up = text.upper()

        if "INSTITUTIONAL DUAL MATCH" not in up:
            return None

        pat = "|".join(
            sorted(
                SUPPORTED,
                key=len,
                reverse=True,
            )
        )

        m = re.findall(
            rf"({pat})\s+(\d{{4,6}})\s*(CE|PE)",
            up,
        )

        if not m:
            return None

        valid = []

        for sym, strike, ot in m:

            strike = int(strike)

            if not self.strike_ok(sym, strike):
                continue

            valid.append(
                (
                    sym,
                    strike,
                    ot,
                )
            )

        return valid[-1] if valid else None

    # =====================

    def dup(self, key):

        now = datetime.now(IST)

        if (
            key in self.last_signal
            and now - self.last_signal[key]
            < timedelta(minutes=DUP_MIN)
        ):
            return True

        self.last_signal[key] = now

        return False

    # =====================

    def resolve(self, u, s, ot):

        df = self.df[
            (self.df.name == u)
            & (
                self.df.symbol.str.contains(
                    f"{s}{ot}",
                    regex=False,
                )
            )
        ]

        if df.empty:
            raise RuntimeError(
                f"SCRIP NOT FOUND: {u} {s} {ot}"
            )

        row = df.sort_values("expiry").iloc[0]

        return (
            row.symbol,
            row.token,
            row.exch_seg,
        )

    # =====================

    def ltp(self, ex, sym, token):

        r = self.smart.ltpData(
            ex,
            sym,
            token,
        )

        print(f"LTP: {r}")

        if isinstance(r, str):
            r = json.loads(r)

        return float(
            r.get("data", {}).get("ltp")
        )

    # =====================
    # REAL ORDER
    # =====================

    def real_order(self, trade):

        params = {

            "variety": "NORMAL",

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

            # IMPORTANT FIX
            "producttype":
                "INTRADAY",

            "duration":
                "DAY",

            "price":
                "0",

            "quantity":
                str(trade.qty),
        }

        err = None

        for i in range(1, 4):

            try:

                print(
                    f"REAL ORDER {i}"
                )

                r = (
                    self.smart
                    .placeOrderFullResponse(
                        params
                    )
                )

                print(
                    f"ORDER RESPONSE: {r}"
                )

                if not r:
                    raise RuntimeError(
                        "EMPTY RESPONSE"
                    )

                data = r.get("data", {})

                oid = data.get("orderid")

                if oid:

                    print(
                        f"ORDER SUCCESS: {oid}"
                    )

                    return str(oid)

                raise RuntimeError(str(r))

            except Exception as e:

                err = e

                print(
                    f"ORDER ERROR: {safe(e)}"
                )

                try:
                    self.login()
                except:
                    pass

                time.sleep(2)

        raise RuntimeError(
            f"FINAL ORDER FAIL: {safe(err)}"
        )

    # =====================

    def create_trade(self, u, s, ot):

        sym, token, ex = self.resolve(
            u,
            s,
            ot,
        )

        price = self.ltp(
            ex,
            sym,
            token,
        )

        targets = [
            price + STEP * i
            for i in range(1, 5)
        ]

        return Trade(
            underlying=u,
            strike=s,
            option_type=ot,

            symbol=sym,
            token=token,
            exchange=ex,

            entry=price,
            sl=price - STEP,
            targets=targets,

            qty=LOT_SIZES.get(u, 1),
        )

    # =====================

    def signal(self, u, s, ot):

        key = f"{u}_{s}_{ot}"

        if self.dup(key):

            print("DUP SIGNAL")

            return None

        active = self.trades.get(u)

        if active:

            if active.option_type == ot:
                return None

            del self.trades[u]

        trade = self.create_trade(
            u,
            s,
            ot,
        )

        # PAPER TRADE FIRST

        self.trades[u] = trade

        print("PAPER TRADE CREATED")

        # REAL TRADE

        if REAL_TRADE_ENABLED:

            try:

                oid = self.real_order(trade)

                trade.order_id = oid

            except Exception as e:

                print(
                    f"REAL FAIL: {safe(e)}"
                )

        return trade

    # =====================

    def update(self):

        msgs = []

        for u, t in list(self.trades.items()):

            try:

                p = self.ltp(
                    t.exchange,
                    t.symbol,
                    t.token,
                )

                # SL

                if p <= t.sl:

                    msgs.append(
                        f"❌ {u} SL HIT @ {p:.2f}"
                    )

                    del self.trades[u]

                    continue

                # TARGET

                if (
                    t.target_hit < MAX_TARGET
                    and p >= t.targets[t.target_hit]
                ):

                    t.target_hit += 1

                    msgs.append(
                        f"🎯 {u} "
                        f"{t.strike} "
                        f"{t.option_type} "
                        f"T{t.target_hit} "
                        f"HIT @ {p:.2f}"
                    )

                # PRICE

                if p > t.last_alert:

                    t.last_alert = p

                    msgs.append(
                        f"📈 {u} "
                        f"PRICE {p:.2f}"
                    )

            except Exception as e:

                print(
                    f"UPDATE ERROR: {safe(e)}"
                )

        return msgs

# =========================
# ENGINE
# =========================

engine = Engine()

# =========================
# FORMAT
# =========================

def fmt(t):

    x = [
        f"🔥 {t.underlying} "
        f"{t.strike} "
        f"{t.option_type}",
        "",
        f"Qty: {t.qty}",
    ]

    if t.order_id:
        x.append(
            f"BUY ORDER: {t.order_id}"
        )

    x.extend(
        [
            f"📍 ENTRY: {t.entry:.2f}",
            f"🛡️ SL: {t.sl:.2f}",
            f"🎯 T1: {t.targets[0]:.2f}",
            f"🎯 T2: {t.targets[1]:.2f}",
            f"🎯 T3: {t.targets[2]:.2f}",
            f"🎯 T4: {t.targets[3]:.2f}",
        ]
    )

    return "\n".join(x)

# =========================
# MONITOR
# =========================

async def monitor():

    while True:

        try:

            for m in engine.update():
                tg(m)

        except Exception as e:

            print(
                f"MONITOR ERROR: {safe(e)}"
            )

        await asyncio.sleep(MONITOR_DELAY)

# =========================
# MAIN
# =========================

async def main():

    print("BOT START")

    engine.login()
    engine.load()

    client = TelegramClient(
        StringSession(TG_SESSION_STR),
        TG_API_ID,
        TG_API_HASH,
    )

    await client.start()

    print(
        f"LISTENING: {SOURCE_CHAT}"
    )

    @client.on(events.NewMessage())
    async def handler(event):

        try:

            chat = await event.get_chat()

            text = event.raw_text or ""

            # =====================
            # SOURCE FILTER
            # =====================

            src = str(
                SOURCE_CHAT
            ).strip().lower()

            cands = [
                str(event.chat_id).lower()
            ]

            for v in (
                getattr(chat, "title", None),
                getattr(chat, "username", None),
                getattr(chat, "first_name", None),
            ):

                if v:
                    cands.append(
                        str(v).strip().lower()
                    )

            if src not in cands:

                print("IGNORE CHAT")

                return

            print(
                f"\nVALID MESSAGE:\n{text}\n"
            )

            # =====================
            # PARSE
            # =====================

            p = engine.parse(text)

            if not p:

                print("NO SIGNAL")

                return

            u, s, ot = p

            print(
                f"SIGNAL: "
                f"{u} {s} {ot}"
            )

            trade = engine.signal(
                u,
                s,
                ot,
            )

            if not trade:
                return

            tg(fmt(trade))

        except Exception as e:

            print(
                f"HANDLER ERROR: {safe(e)}"
            )

    await asyncio.gather(
        client.run_until_disconnected(),
        monitor(),
    )

# =========================
# START
# =========================

if __name__ == "__main__":

    asyncio.run(main())
