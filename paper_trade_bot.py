import asyncio
import json
import os
import re
import time
import uuid
from dataclasses import dataclass, field
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

def env_int(k, d):
    try:
        return int(env(k, d))
    except (TypeError, ValueError):
        return int(d)

def env_float(k, d):
    try:
        return float(env(k, d))
    except (TypeError, ValueError):
        return float(d)

def env_csv(k, d):
    return {
        x.strip().upper()
        for x in str(env(k, d)).split(",")
        if x.strip()
    }

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

REAL_PRODUCT_TYPE = env(
    "REAL_PRODUCT_TYPE",
    "INTRADAY",
).upper()

REAL_ORDER_TYPE = env(
    "REAL_ORDER_TYPE",
    "LIMIT",
).upper()

REAL_PRICE_BUFFER = env_float(
    "REAL_PRICE_BUFFER",
    "1.0",
)

MAX_REAL_TRADES_PER_DAY = env_int(
    "MAX_TRADES_PER_DAY",
    "5",
)

ALLOW_REAL_TRADING_AFTER = env(
    "ALLOW_REAL_TRADING_AFTER",
    "09:20",
)

STOP_REAL_TRADING_AFTER = env(
    "STOP_REAL_TRADING_AFTER",
    "15:10",
)

PAPER_TRADE_START_TIME = env(
    "PAPER_TRADE_START_TIME",
    "09:19",
)

PAPER_TRADE_STOP_TIME = env(
    "PAPER_TRADE_STOP_TIME",
    "15:00",
)

REAL_ALLOWED_UNDERLYINGS = env_csv(
    "REAL_ALLOWED_UNDERLYINGS",
    "NIFTY,BANKNIFTY",
)

# Matrix / Element X Credentials
MATRIX_HOMESERVER = env("MATRIX_HOMESERVER", "https://matrix.org").rstrip("/")
MATRIX_ACCESS_TOKEN = env("MATRIX_ACCESS_TOKEN", "")
MATRIX_USER = env("MATRIX_USER", "")
MATRIX_PASS = env("MATRIX_PASS", "")
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
MATRIX_TOKEN_FILE = os.path.join(BASE_DIR, "matrix_access_token.txt")
MATRIX_ROOM_ID = env("MATRIX_ROOM_ID", "")

LOT_SIZES = {
    "NIFTY": env_int("NIFTY_LOT_SIZE", "65"),
    "BANKNIFTY": env_int("BANKNIFTY_LOT_SIZE", "30"),
}

# =========================
# CONFIG
# =========================

STEP = 30
MAX_TARGET = 5
MONITOR_DELAY = 3
DUP_MIN = 10
REVERSE_WAIT_SECONDS = env_int("REVERSE_WAIT_SECONDS", "60")
OPTION_PRICE_ALERT_STEP = env_float("OPTION_PRICE_ALERT_STEP", "5")
STOCK_PRICE_ALERT_STEP = env_float("STOCK_PRICE_ALERT_STEP", "2")
STOCK_MIS_QTY = env_int("STOCK_MIS_QTY", "100")
STOCK_MIS_SL_POINTS = env_float("STOCK_MIS_SL_POINTS", "5")
STOCK_MIS_TARGET_POINTS = env_float("STOCK_MIS_TARGET_POINTS", "10")

REVERSE_PROTECT_POINTS = {
    "NIFTY": env_float("NIFTY_REVERSE_PROTECT_POINTS", "5"),
    "BANKNIFTY": env_float("BANKNIFTY_REVERSE_PROTECT_POINTS", "10"),
}

MASTER_FILE = "OpenAPIScripMaster.json"

MASTER_URL = (
    "https://margincalculator.angelbroking.com/"
    "OpenAPI_File/files/OpenAPIScripMaster.json"
)

INDEX_SYMBOLS = {
    "NIFTY",
    "BANKNIFTY",
    "SENSEX",
    "MIDCPNIFTY",
}

CROR_OPTION_WRITER_LOTS = env_int("CROR_OPTION_WRITER_LOTS", "500")
CROR_OPTION_SHORT_COVERING_LOTS = env_int("CROR_OPTION_SHORT_COVERING_LOTS", "1000")
CROR_OPTION_BUYER_LOTS = env_int("CROR_OPTION_BUYER_LOTS", "1000")
CROR_STOCK_FUT_LOTS = env_int("CROR_STOCK_FUT_LOTS", "1000")
CROR_INDEX_FUT_LOTS = env_int("CROR_INDEX_FUT_LOTS", "3000")
EXPLOSIVE_OPT_THRESHOLD = 15.0
LOCK_IN_POINTS = 150
TRAILING_GAP = 120

# =========================
# HELPERS
# =========================

def safe(e):
    try:
        return str(e)
    except:
        return type(e).__name__

def parse_api_response(r):
    if isinstance(r, str):
        r = json.loads(r)

    if not isinstance(r, dict):
        raise RuntimeError(
            f"BAD API RESPONSE: {r}"
        )

    return r

def api_error(r):
    msg = (
        r.get("message")
        or r.get("errorCode")
        or r.get("errorcode")
        or "unknown error"
    )
    code = (
        r.get("errorCode")
        or r.get("errorcode")
        or r.get("code")
    )

    if code and str(code) not in str(msg):
        return f"{msg} ({code})"

    return str(msg)

def hhmm(v):
    return datetime.strptime(v, "%H:%M").time()

def tick(v):
    return round(round(float(v) / 0.05) * 0.05, 2)

def trade_step(underlying):
    return 3 if underlying.upper() in {"HDFCBANK", "ICICIBANK", "RELIANCE"} else 30

def trade_option_type(option_type):
    return option_type

# =========================
# MATRIX UTILS
# =========================

def perform_matrix_login():
    if not MATRIX_USER or not MATRIX_PASS:
        return None
    
    login_url = f"{MATRIX_HOMESERVER}/_matrix/client/v3/login"
    payload = {
        "type": "m.login.password",
        "user": MATRIX_USER,
        "password": MATRIX_PASS,
        "initial_device_display_name": "PaperTradeBotAuto"
    }
    
    try:
        response = requests.post(login_url, json=payload, timeout=15)
        if response.status_code == 200:
            token = response.json().get("access_token")
            if token:
                with open(MATRIX_TOKEN_FILE, "w") as f:
                    f.write(token)
                print("✅ Matrix auto-login successful.")
                return token
        else:
            print(f"❌ Matrix auto-login failed: {response.status_code} - {response.text}")
    except Exception as e:
        print(f"❌ Matrix auto-login error: {safe(e)}")
    return None

def clear_matrix_token_file():
    try:
        if os.path.exists(MATRIX_TOKEN_FILE):
            os.remove(MATRIX_TOKEN_FILE)
    except Exception as e:
        print(f"❌ Error clearing {MATRIX_TOKEN_FILE}: {safe(e)}")

def get_matrix_token():
    # 1. Try to read from file first
    token = None
    if os.path.exists(MATRIX_TOKEN_FILE):
        try:
            with open(MATRIX_TOKEN_FILE, "r") as f:
                token = f.read().strip()
        except Exception as e:
            print(f"❌ Error reading {MATRIX_TOKEN_FILE}: {safe(e)}")
    
    # 2. Fallback to environment variable
    if not token:
        token = MATRIX_ACCESS_TOKEN
        
    # 3. Auto-login if still no token
    if not token:
        token = perform_matrix_login()
        
    return token

def refresh_matrix_token():
    clear_matrix_token_file()
    return perform_matrix_login()

def tg(text):

    print(f"ALERT:\n{text}")

    # --- Send to Telegram ---
    try:
        res = requests.post(
            f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
            data={
                "chat_id": CHAT_ID,
                "text": text,
            },
            timeout=30,
        )
        if res.status_code != 200:
            print(f"TG ERROR: {res.status_code} - {res.text}")
    except Exception as e:
        print(f"TG ERROR: {safe(e)}")

    # --- Send to Matrix / Element X ---
    token = get_matrix_token()
    if token and MATRIX_ROOM_ID:
        try:
            txn_id = str(uuid.uuid4())
            url = f"{MATRIX_HOMESERVER}/_matrix/client/v3/rooms/{MATRIX_ROOM_ID}/send/m.room.message/{txn_id}"
            headers = {
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json"
            }
            payload = {
                "msgtype": "m.text",
                "body": text
            }
            res = requests.put(url, headers=headers, data=json.dumps(payload), timeout=10)
            
            if res.status_code in (401, 403):
                print(f"⚠️ Matrix token rejected ({res.status_code}). Attempting auto-login...")
                new_token = refresh_matrix_token()
                if new_token:
                    headers["Authorization"] = f"Bearer {new_token}"
                    res = requests.put(url, headers=headers, data=json.dumps(payload), timeout=10)

            if res.status_code != 200:
                print(f"MATRIX ERROR: {res.status_code} - {res.text}")
        except Exception as e:
            print(f"MATRIX EXCEPTION: {safe(e)}")

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
    side: str = "BUY"
    instrument_kind: str = "OPTION"

    order_id: str | None = None
    exit_order_id: str | None = None
    real_open: bool = False
    real_error: str | None = None

    target_hit: int = 0
    last_alert: float = 0
    high_price: float = 0
    is_reverse: bool = False
    signal_source: str | None = None
    opened_at: datetime = field(
        default_factory=lambda: datetime.now(IST)
    )
    reverse_protected: bool = False

# =========================
# ENGINE
# =========================

class Engine:

    def __init__(self):

        self.smart = None
        self.df = None
        self.spot_df = None

        self.trades = {}
        self.last_signal = {}
        self.reverse_wait_until = {}
        self.real_trade_day = None
        self.real_entries_today = 0

    def get_atm(self, price, symbol, option_type):
        symbol = symbol.upper()
        option_type = option_type.upper()
        rows = self.df[
            (self.df.name == symbol)
            & self.df.symbol.astype(str).str.endswith(option_type)
        ].copy()
        if rows.empty:
            raise RuntimeError(
                f"NO OPTION CONTRACTS: {symbol} {option_type}"
            )

        nearest_expiry = rows["expiry"].min()
        rows = rows[rows["expiry"] == nearest_expiry].copy()
        strike_pattern = re.compile(
            rf"^{re.escape(symbol)}\d{{2}}[A-Z]{{3}}(\d+(?:\.\d+)?){option_type}$"
        )
        strikes = []
        for contract_symbol in rows.symbol.astype(str):
            match = strike_pattern.match(contract_symbol)
            if match:
                strikes.append(float(match.group(1)))

        if not strikes:
            raise RuntimeError(
                f"NO PARSABLE OPTION STRIKES: {symbol} {option_type}"
            )

        strike = min(strikes, key=lambda value: abs(value - float(price)))
        return int(strike) if strike.is_integer() else strike

    # =====================

    def login(self):

        print("ANGEL LOGIN")

        self.smart = SmartConnect(
            api_key=ANGEL_API_KEY
        )

        totp = pyotp.TOTP(
            ANGEL_TOTP_SECRET
        ).now()

        r = self.smart.generateSession(
            ANGEL_CLIENT_ID,
            ANGEL_PASSWORD,
            totp,
        )

        r = parse_api_response(r)

        if r.get("status") is False:
            raise RuntimeError(
                f"ANGEL LOGIN FAILED: {api_error(r)}"
            )

        print("ANGEL LOGIN OK")

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

        master = pd.read_json(MASTER_FILE)
        master["expiry"] = pd.to_datetime(
            master.get("expiry"),
            format="%d%b%Y",
            errors="coerce",
        )

        today = datetime.now(IST).date()
        derivatives = master[
            master.exch_seg.isin(["NFO", "BFO"])
            & master["expiry"].notna()
        ].copy()
        self.df = derivatives[
            derivatives["expiry"].dt.date >= today
        ].copy()

        self.spot_df = master[
            (master.exch_seg == "NSE")
            & master.symbol.astype(str).str.endswith("-EQ")
        ].copy()

    # =====================

    def strike_ok(self, u, s):

        if u == "NIFTY":
            return 18000 <= s <= 30000

        if u == "BANKNIFTY":
            return 35000 <= s <= 70000

        return True

    # =====================

    def _cror_value(self, pattern, text, cast=float, default=None):
        m = re.search(pattern, text, re.IGNORECASE)
        if not m:
            return default
        try:
            return cast(str(m.group(1)).replace(",", ""))
        except (TypeError, ValueError):
            return default

    def parse_cror_alerts(self, text):
        alerts = []
        blocks = re.split(r"\n\s*-{3,}\s*\n", text.strip())

        for block in blocks:
            up = block.upper()
            action = None
            for candidate in ("SHORT COVERING", "UNWINDING", "FUT BUY", "FUT SELL", "BUYER", "WRITER", "BUY", "SELL"):
                if re.search(rf"\b{candidate}\b", up):
                    action = candidate
                    break

            if not action:
                continue

            lots = self._cror_value(r"\bLots\s*:\s*([\d,]+)", block, int)
            price = self._cror_value(r"\bPrice\s*:\s*([\d.]+)", block, float)
            fut_price = self._cror_value(r"\bFut\s+Price\s*:\s*([\d.]+)", block, float)
            turnover = self._cror_value(r"\bTurnover\s*:\s*(?:₹|Rs\.?)?\s*([\d.]+)\s*Cr", block, float)

            opt = re.search(
                r"(?:NFO:)?([A-Z&]+?)(\d{2}[A-Z]{3})(\d{3,6})(CE|PE)\s*\(([^)]*ITM[^)]*)\)",
                up,
            )

            if opt:
                symbol = opt.group(1)
                strike = int(opt.group(3))
                option_type = opt.group(4)
                moneyness = opt.group(5)

                if lots is None:
                    continue

                threshold = None
                if action == "WRITER":
                    threshold = CROR_OPTION_WRITER_LOTS
                elif action == "SHORT COVERING":
                    threshold = CROR_OPTION_SHORT_COVERING_LOTS
                elif action == "BUYER":
                    threshold = CROR_OPTION_BUYER_LOTS

                if threshold is None or lots < threshold:
                    continue

                if action == "WRITER":
                    signal_ot = "PE" if option_type == "CE" else "CE"
                else:
                    signal_ot = option_type

                entry_allowed = "NEAR-ITM" not in moneyness

                alerts.append(
                    {
                        "kind": "OPTION",
                        "symbol": symbol,
                        "strike": strike,
                        "option_type": option_type,
                        "signal_ot": signal_ot,
                        "entry_allowed": entry_allowed,
                        "action": action,
                        "lots": lots,
                        "price": price,
                        "fut_price": fut_price,
                        "turnover": turnover,
                        "moneyness": moneyness,
                        "threshold": threshold,
                    }
                )
                continue

            fut = re.search(
                r"(?:NFO:)?([A-Z&]+?)(\d{2}[A-Z]{3})FUT\b",
                up,
            )

            if fut and lots is not None:
                symbol = fut.group(1)
                is_index = symbol in INDEX_SYMBOLS
                threshold = (
                    CROR_INDEX_FUT_LOTS
                    if is_index
                    else CROR_STOCK_FUT_LOTS
                )

                if lots >= threshold:
                    if (
                        not is_index
                        and action not in {"BUYER", "WRITER"}
                    ):
                        continue

                    signal_ot = None
                    if action in {"SHORT COVERING", "FUT BUY", "BUY", "BUYER"}:
                        signal_ot = "CE"
                    elif action in {"UNWINDING", "FUT SELL", "SELL", "WRITER"}:
                        signal_ot = "PE"

                    alerts.append(
                        {
                            "kind": "FUTURE",
                            "symbol": symbol,
                            "signal_ot": signal_ot,
                            "action": action,
                            "lots": lots,
                            "price": price,
                            "fut_price": fut_price,
                            "turnover": turnover,
                            "threshold": threshold,
                            "trade_mode": "INDEX_OPTION" if is_index else "STOCK_SPOT",
                        }
                    )

        return alerts

    def short_cror_source(self, a):
        action_code = {
            "SHORT COVERING": "S_C",
            "WRITER": "WR",
            "BUYER": "BUY",
        }.get(a["action"], a["action"].replace(" ", "_"))
        moneyness = re.sub(
            r"-[\d.]+-DIFF$",
            "",
            str(a.get("moneyness", "")),
            flags=re.IGNORECASE,
        ).lower()
        return (
            f"{action_code} {a['option_type']}-{moneyness} "
            f"{a['lots']}lots"
        )

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
                self.df.symbol.astype(str).str.match(
                    rf"^{re.escape(u)}\d{{2}}[A-Z]{{3}}{int(s)}{ot}$",
                    na=False,
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
            int(float(row.get("lotsize", LOT_SIZES.get(u, 1)))),
        )

    def resolve_spot(self, u):
        if self.spot_df is None:
            raise RuntimeError("SPOT MASTER NOT LOADED")

        rows = self.spot_df[
            (self.spot_df.name.astype(str) == u)
            | (self.spot_df.symbol.astype(str) == f"{u}-EQ")
        ]
        if rows.empty:
            raise RuntimeError(f"SPOT SCRIP NOT FOUND: {u}")

        row = rows.iloc[0]
        return row.symbol, row.token, row.exch_seg

    def resolve_future(self, u):
        rows = self.df[
            (self.df.name == u)
            & self.df.symbol.astype(str).str.match(
                rf"^{re.escape(u)}\d{{2}}[A-Z]{{3}}FUT$",
                na=False,
            )
        ]
        if rows.empty:
            raise RuntimeError(f"FUTURE SCRIP NOT FOUND: {u}")

        row = rows.sort_values("expiry").iloc[0]
        return row.symbol, row.token, row.exch_seg

    async def confirm_reverse_after_wait(
        self,
        u,
        direction,
        reference_future_price,
        signal_source,
        strike=None,
        stock_mode=False,
    ):
        await asyncio.sleep(REVERSE_WAIT_SECONDS)

        try:
            if u in self.trades:
                return

            symbol, token, exchange = self.resolve_future(u)
            current_future_price = self.ltp(exchange, symbol, token)
            aligned = (
                current_future_price > reference_future_price
                if direction in {"CE", "BUY"}
                else current_future_price < reference_future_price
            )
            if not aligned:
                return

            self.reverse_wait_until.pop(u, None)
            source = (
                f"{signal_source} | 1MIN FUT CONFIRMED "
                f"{reference_future_price:.2f}->{current_future_price:.2f}"
            )
            if stock_mode:
                trade, msgs = self.stock_signal(
                    u,
                    direction,
                    source,
                )
            else:
                trade, msgs = self.signal(
                    u,
                    strike,
                    direction,
                    False,
                    source,
                )

            for msg in msgs:
                tg(msg)
            if trade:
                tg(fmt(trade))
        except Exception as e:
            print(f"REVERSE CONFIRM ERROR {u}: {safe(e)}")

    # =====================

    def ltp(self, ex, sym, token):

        last = None

        for attempt in range(1, 3):

            r = self.smart.ltpData(
                ex,
                sym,
                token,
            )

            print(f"LTP: {r}")

            r = parse_api_response(r)
            last = r

            if r.get("success") is False or r.get("status") is False:
                err = api_error(r)

                if (
                    attempt == 1
                    and (
                        r.get("errorCode") == "AG8001"
                        or "invalid token" in err.lower()
                    )
                ):
                    print("LTP TOKEN ERROR - RELOGIN")
                    self.login()
                    continue

                raise RuntimeError(
                    f"LTP FAILED: {err}"
                )

            data = r.get("data")

            if not isinstance(data, dict):
                raise RuntimeError(
                    f"LTP FAILED: bad data: {data}"
                )

            ltp = data.get("ltp")

            if ltp is None:
                raise RuntimeError(
                    f"LTP FAILED: missing ltp: {r}"
                )

            return float(ltp)

        raise RuntimeError(
            f"LTP FAILED: {last}"
        )

    # =====================
    # REAL ORDER
    # =====================

    def reset_real_day(self):

        today = datetime.now(IST).date()

        if self.real_trade_day != today:
            self.real_trade_day = today
            self.real_entries_today = 0

    # =====================

    def real_entry_block_reason(self, u, instrument_kind="OPTION"):

        if not REAL_TRADE_ENABLED:
            return "REAL_TRADE_ENABLED=false"

        if (
            instrument_kind != "STOCK"
            and u not in REAL_ALLOWED_UNDERLYINGS
        ):
            return f"{u} not in REAL_ALLOWED_UNDERLYINGS"

        self.reset_real_day()

        now = datetime.now(IST).time()

        if now < hhmm(ALLOW_REAL_TRADING_AFTER):
            return f"before {ALLOW_REAL_TRADING_AFTER}"

        if now > hhmm(STOP_REAL_TRADING_AFTER):
            return f"after {STOP_REAL_TRADING_AFTER}"

        if self.real_entries_today >= MAX_REAL_TRADES_PER_DAY:
            return "max real trades reached"

        return None

    # =====================

    def paper_entry_block_reason(self):

        now = datetime.now(IST).time()

        if now < hhmm(PAPER_TRADE_START_TIME):
            return f"before {PAPER_TRADE_START_TIME}"

        if now >= hhmm(PAPER_TRADE_STOP_TIME):
            return f"after {PAPER_TRADE_STOP_TIME}"

        return None

    # =====================

    def real_price(self, side, ref_price):

        if REAL_ORDER_TYPE == "MARKET":
            return "0"

        if ref_price is None:
            raise RuntimeError(
                "LIMIT order needs reference price"
            )

        if side == "BUY":
            price = ref_price + REAL_PRICE_BUFFER
        else:
            price = max(
                0.05,
                ref_price - REAL_PRICE_BUFFER,
            )

        return f"{tick(price):.2f}"

    # =====================

    def order_id(self, r):

        if isinstance(r, str):

            text = r.strip()

            if not text:
                raise RuntimeError(
                    "EMPTY ORDER RESPONSE"
                )

            try:
                r = json.loads(text)
            except json.JSONDecodeError:
                return text

        if isinstance(r, (int, float)):
            return str(r)

        if not isinstance(r, dict):
            raise RuntimeError(
                f"BAD ORDER RESPONSE: {r}"
            )

        if r.get("status") is False:
            raise RuntimeError(str(r))

        data = r.get("data")

        oid = None

        if isinstance(data, dict):
            oid = (
                data.get("orderid")
                or data.get("order_id")
                or data.get("uniqueorderid")
            )
        elif isinstance(data, str):
            oid = data

        oid = (
            oid
            or r.get("orderid")
            or r.get("order_id")
            or r.get("uniqueorderid")
        )

        if not oid:
            raise RuntimeError(str(r))

        return str(oid)

    # =====================

    def real_order(self, trade, side, ref_price):

        params = {

            "variety": "NORMAL",

            "tradingsymbol":
                trade.symbol,

            "symboltoken":
                trade.token,

            "transactiontype":
                side,

            "exchange":
                trade.exchange,

            "ordertype":
                REAL_ORDER_TYPE,

            "producttype":
                REAL_PRODUCT_TYPE,

            "duration":
                "DAY",

            "price":
                self.real_price(
                    side,
                    ref_price,
                ),

            "quantity":
                str(trade.qty),
        }

        err = None

        for i in range(1, 4):

            try:

                print(
                    f"REAL {side} ORDER {i}: {params}"
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

                oid = self.order_id(r)

                print(
                    f"ORDER SUCCESS: {oid}"
                )

                return oid

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

    def create_trade(
        self,
        u,
        s,
        ot,
        is_reverse=False,
        signal_source=None,
    ):

        sym, token, ex, lot_size = self.resolve(
            u,
            s,
            ot,
        )

        price = self.ltp(
            ex,
            sym,
            token,
        )

        step = trade_step(u)
        # CE and PE are both long option-premium trades. Profit happens when
        # the bought option premium rises.
        sl = max(0.05, price - step)
        targets = [price + step * i for i in range(1, MAX_TARGET + 1)]

        return Trade(
            underlying=u,
            strike=s,
            option_type=ot,

            symbol=sym,
            token=token,
            exchange=ex,

            entry=price,
            sl=sl,
            targets=targets,

            qty=lot_size,
            side="BUY",
            instrument_kind="OPTION",
            high_price=price,
            is_reverse=is_reverse,
            signal_source=signal_source,
        )

    def create_stock_trade(self, u, side, signal_source=None):
        sym, token, ex = self.resolve_spot(u)
        price = self.ltp(ex, sym, token)

        if side == "BUY":
            sl = price - STOCK_MIS_SL_POINTS
            targets = [price + STOCK_MIS_TARGET_POINTS]
        else:
            sl = price + STOCK_MIS_SL_POINTS
            targets = [price - STOCK_MIS_TARGET_POINTS]

        return Trade(
            underlying=u,
            strike=0,
            option_type="MIS",
            symbol=sym,
            token=token,
            exchange=ex,
            entry=price,
            sl=sl,
            targets=targets,
            qty=STOCK_MIS_QTY,
            side=side,
            instrument_kind="STOCK",
            high_price=price,
            signal_source=signal_source,
        )

    # =====================

    def trail_sl(self, trade):

        if trade.target_hit <= 0:
            return None

        if trade.target_hit == 1:
            new_sl = trade.entry
        else:
            new_sl = trade.targets[
                trade.target_hit - 2
            ]

        old_sl = trade.sl
        if new_sl > trade.sl:
            trade.sl = new_sl
            return old_sl, new_sl

        return None

    def trade_label(self, trade):
        if trade.instrument_kind == "STOCK":
            return f"{trade.underlying} MIS {trade.side}"
        return (
            f"{trade.underlying} {trade.strike} "
            f"{trade.option_type}"
        )

    # =====================

    def close_trade(self, trade, reason, price):

        msgs = []

        if not trade.real_open:
            return True, msgs

        if price is None:
            price = self.ltp(
                trade.exchange,
                trade.symbol,
                trade.token,
            )

        try:

            exit_side = "SELL" if trade.side == "BUY" else "BUY"
            oid = self.real_order(
                trade,
                exit_side,
                price,
            )

            trade.exit_order_id = oid
            trade.real_open = False

            msgs.append(
                f"REAL {exit_side} {trade.underlying} "
                f"@ {price:.2f} "
                f"({reason}) "
                f"ORDER: {oid}"
            )

            return True, msgs

        except Exception as e:

            trade.real_error = safe(e)

            msgs.append(
                f"⚠️ REAL EXIT FAILED "
                f"{trade.underlying} "
                f"{trade.strike} "
                f"{trade.option_type}: "
                f"{safe(e)}"
            )

            return False, msgs

    # =====================

    def signal(
        self,
        u,
        s,
        ot,
        reverse_confirmed=False,
        signal_source=None,
        reference_future_price=None,
    ):
        msgs = []
        now = datetime.now(IST)

        reason = self.paper_entry_block_reason()
        if reason:
            print(f"OUTSIDE WINDOW SIGNAL ALLOWED {u} {s} {ot}: {reason}")
            msgs.append(f"OUTSIDE WINDOW: {reason}")

        if now < self.reverse_wait_until.get(u, now):
            return None, msgs

        active = self.trades.get(u)
        if active:
            if (
                active.instrument_kind == "OPTION"
                and active.option_type == ot
            ):
                return None, msgs

            exit_price = self.ltp(
                active.exchange,
                active.symbol,
                active.token,
            )
            ok, exit_msgs = self.close_trade(
                active,
                "REVERSE SIGNAL",
                exit_price,
            )
            msgs.extend(exit_msgs)
            if not ok:
                return None, msgs

            label = (
                f"{active.strike} {active.option_type}"
                if active.instrument_kind == "OPTION"
                else f"MIS {active.side}"
            )
            msgs.append(
                f"{u} {label} EXIT @ {exit_price:.2f} | "
                f"REVERSE {ot}, WAIT 1 MIN"
            )
            del self.trades[u]
            self.reverse_wait_until[u] = now + timedelta(
                seconds=REVERSE_WAIT_SECONDS
            )
            if reference_future_price is not None:
                asyncio.create_task(
                    self.confirm_reverse_after_wait(
                        u,
                        ot,
                        float(reference_future_price),
                        signal_source or "REVERSE",
                        strike=s,
                    )
                )
            return None, msgs

        key = f"{u}_{s}_{ot}"
        if self.dup(key):
            return None, msgs

        try:
            trade = self.create_trade(
                u,
                s,
                ot,
                False,
                signal_source,
            )
        except Exception as e:
            fallback = (
                f"{u} {s} {ot}\n\n"
                f"SIGNAL SOURCE: {signal_source or 'CROR'}\n"
                f"CONTRACT LOOKUP FAILED: {safe(e)}"
            )
            print(f"FALLBACK SIGNAL: {fallback}")
            msgs.append(fallback)
            return None, msgs
        self.trades[u] = trade
        print("PAPER OPTION TRADE CREATED")

        if REAL_TRADE_ENABLED:
            try:
                reason = self.real_entry_block_reason(
                    u,
                    trade.instrument_kind,
                )
                if reason:
                    raise RuntimeError(reason)
                oid = self.real_order(
                    trade,
                    trade.side,
                    trade.entry,
                )
                trade.order_id = oid
                trade.real_open = True
                self.real_entries_today += 1
            except Exception as e:
                trade.real_error = safe(e)
                print(f"REAL FAIL: {safe(e)}")

        return trade, msgs

    def stock_signal(
        self,
        u,
        side,
        signal_source=None,
        reference_future_price=None,
    ):
        msgs = []
        now = datetime.now(IST)

        if self.paper_entry_block_reason():
            return None, msgs

        if now < self.reverse_wait_until.get(u, now):
            return None, msgs

        active = self.trades.get(u)
        if active:
            if (
                active.instrument_kind == "STOCK"
                and active.side == side
            ):
                return None, msgs

            exit_price = self.ltp(
                active.exchange,
                active.symbol,
                active.token,
            )
            ok, exit_msgs = self.close_trade(
                active,
                "REVERSE SIGNAL",
                exit_price,
            )
            msgs.extend(exit_msgs)
            if not ok:
                return None, msgs

            label = (
                f"{active.strike} {active.option_type}"
                if active.instrument_kind == "OPTION"
                else f"MIS {active.side}"
            )
            msgs.append(
                f"{u} {label} EXIT @ {exit_price:.2f} | "
                f"REVERSE {side}, WAIT 1 MIN"
            )
            del self.trades[u]
            self.reverse_wait_until[u] = now + timedelta(
                seconds=REVERSE_WAIT_SECONDS
            )
            if reference_future_price is not None:
                asyncio.create_task(
                    self.confirm_reverse_after_wait(
                        u,
                        side,
                        float(reference_future_price),
                        signal_source or "REVERSE",
                        stock_mode=True,
                    )
                )
            return None, msgs

        if self.dup(f"STOCK_{u}_{side}"):
            return None, msgs

        trade = self.create_stock_trade(
            u,
            side,
            signal_source=signal_source,
        )
        self.trades[u] = trade
        print("PAPER STOCK MIS TRADE CREATED")

        if REAL_TRADE_ENABLED:
            try:
                reason = self.real_entry_block_reason(
                    u,
                    trade.instrument_kind,
                )
                if reason:
                    raise RuntimeError(reason)
                oid = self.real_order(
                    trade,
                    trade.side,
                    trade.entry,
                )
                trade.order_id = oid
                trade.real_open = True
                self.real_entries_today += 1
            except Exception as e:
                trade.real_error = safe(e)
                print(f"REAL FAIL: {safe(e)}")

        return trade, msgs

    # =====================

    def update(self):
        msgs = []

        if (
            datetime.now(IST).time() >= hhmm(PAPER_TRADE_STOP_TIME)
            and self.trades
        ):
            for u, trade in list(self.trades.items()):
                try:
                    price = self.ltp(
                        trade.exchange,
                        trade.symbol,
                        trade.token,
                    )
                    ok, exit_msgs = self.close_trade(
                        trade,
                        f"PAPER CUTOFF {PAPER_TRADE_STOP_TIME}",
                        price,
                    )
                    msgs.extend(exit_msgs)
                    if ok:
                        msgs.append(
                            f"{self.trade_label(trade)} "
                            f"EXIT @ {price:.2f} | "
                            f"CUTOFF {PAPER_TRADE_STOP_TIME}"
                        )
                        del self.trades[u]
                except Exception as e:
                    print(f"CUTOFF EXIT ERROR {u}: {safe(e)}")
            return msgs

        for u, trade in list(self.trades.items()):
            try:
                price = self.ltp(
                    trade.exchange,
                    trade.symbol,
                    trade.token,
                )

                sl_hit = (
                    price <= trade.sl
                    if trade.side == "BUY"
                    else price >= trade.sl
                )
                if sl_hit:
                    ok, exit_msgs = self.close_trade(
                        trade,
                        "SL HIT",
                        price,
                    )
                    msgs.extend(exit_msgs)
                    if ok:
                        msgs.append(
                            f"{self.trade_label(trade)} "
                            f"SL HIT @ {price:.2f} | "
                            f"ENTRY {trade.entry:.2f}"
                        )
                        del self.trades[u]
                    continue

                closed = False
                while trade.target_hit < len(trade.targets):
                    target = trade.targets[trade.target_hit]
                    target_hit = (
                        price >= target
                        if trade.side == "BUY"
                        else price <= target
                    )
                    if not target_hit:
                        break

                    trade.target_hit += 1
                    target_no = trade.target_hit
                    msgs.append(
                        f"{self.trade_label(trade)} "
                        f"T{target_no} HIT @ {price:.2f}"
                    )

                    if trade.instrument_kind == "OPTION":
                        trail_result = self.trail_sl(trade)
                        if trail_result:
                            old_sl, new_sl = trail_result
                            msgs.append(
                                f"{self.trade_label(trade)} "
                                f"SL {old_sl:.2f} -> {new_sl:.2f}"
                            )

                    if target_no >= len(trade.targets):
                        ok, exit_msgs = self.close_trade(
                            trade,
                            f"T{target_no} HIT",
                            price,
                        )
                        msgs.extend(exit_msgs)
                        if ok:
                            msgs.append(
                                f"{self.trade_label(trade)} "
                                f"EXIT @ {price:.2f} | TARGET"
                            )
                            del self.trades[u]
                            closed = True
                        break

                if closed:
                    continue

                alert_step = (
                    STOCK_PRICE_ALERT_STEP
                    if trade.instrument_kind == "STOCK"
                    else OPTION_PRICE_ALERT_STEP
                )
                favorable_move = (
                    price >= trade.high_price + alert_step
                    if trade.side == "BUY"
                    else price <= trade.high_price - alert_step
                )
                if favorable_move:
                    trade.high_price = price
                    trade.last_alert = price
                    msgs.append(
                        f"{self.trade_label(trade)} "
                        f"PRICE {price:.2f}"
                    )

            except Exception as e:
                print(f"UPDATE ERROR: {safe(e)}")

        return msgs

# =========================
# ENGINE
# =========================

engine = Engine()

# =========================
# FORMAT
# =========================

def fmt(t):
    if t.instrument_kind == "STOCK":
        return (
            f"{t.underlying} MIS {t.side} {t.qty} QTY\n"
            f"ENTRY: {t.entry:.2f}\n"
            f"SL: {t.sl:.2f}\n"
            f"TARGET: {t.targets[0]:.2f}"
        )

    lines = [
        f"{t.underlying} {t.strike} {t.option_type}",
        f"ENTRY: {t.entry:.2f}",
        f"SL: {t.sl:.2f}",
    ]
    lines.extend(
        f"T{index}: {target:.2f}"
        for index, target in enumerate(t.targets, 1)
    )
    if t.signal_source:
        lines.append(t.signal_source)
    return "\n".join(lines)

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

    print("PAPER BOT START")

    try:
        engine.login()
        engine.load()
    except Exception as e:
        print(
            f"STARTUP ERROR: {safe(e)}"
        )
        tg(
            f"BOT STARTUP ERROR: {safe(e)}"
        )
        raise

    client = TelegramClient(
        StringSession(TG_SESSION_STR),
        TG_API_ID,
        TG_API_HASH,
    )

    await client.start()

    print(
        f"LISTENING: {SOURCE_CHAT}"
    )
    tg(
        f"PAPER scanner started\n"
        f"Listening to: {SOURCE_CHAT}"
    )

    @client.on(events.NewMessage())
    async def handler(event):
        signal_desc = None
        try:
            chat = await event.get_chat()
            text = event.raw_text or ""

            # =====================
            # SOURCE FILTER
            # =====================
            src = str(SOURCE_CHAT).strip().lower().lstrip("@")
            cands = [str(event.chat_id).lower()]
            for v in (getattr(chat, "title", None), getattr(chat, "username", None), getattr(chat, "first_name", None)):
                if v: cands.append(str(v).strip().lower())
            if src not in cands:
                return

            print(f"\nVALID MESSAGE:\n{text}\n")
            now = datetime.now(IST)

            # =====================
            # CROR SCAN BOT DETECTION
            # =====================
            cror_alerts = engine.parse_cror_alerts(text)
            if cror_alerts:
                for a in cror_alerts:
                    if a["kind"] == "FUTURE":
                        symbol = a["symbol"]
                        if a.get("trade_mode") == "STOCK_SPOT":
                            side = "BUY" if a["action"] == "BUYER" else "SELL"
                            source = (
                                f"{a['action'].replace(' ', '_')} "
                                f"{a['lots']}lots"
                            )
                            trade, msgs = engine.stock_signal(
                                symbol,
                                side,
                                source,
                                reference_future_price=a.get("fut_price"),
                            )
                            for msg in msgs:
                                tg(msg)
                            if trade:
                                tg(fmt(trade))
                            continue

                        if not a.get("signal_ot"):
                            continue

                        if not a.get("fut_price"):
                            continue

                        trade_strike = engine.get_atm(
                            a["fut_price"],
                            symbol,
                            a["signal_ot"],
                        )
                        signal_desc = (
                            f"{symbol} ATM {trade_strike} {a['signal_ot']} "
                            f"(CROR FUT {a['action']} {a['lots']} lots)"
                        )
                        source = (
                            f"{a['action'].replace(' ', '_')} "
                            f"{a['lots']}lots"
                        )
                        trade, msgs = engine.signal(
                            symbol,
                            trade_strike,
                            a["signal_ot"],
                            False,
                            source,
                            reference_future_price=a["fut_price"],
                        )
                        for msg in msgs:
                            tg(msg)
                        if trade:
                            tg(fmt(trade))
                        continue

                    symbol = a["symbol"]
                    if not a.get("fut_price"):
                        continue

                    trade_strike = engine.get_atm(
                        a["fut_price"],
                        symbol,
                        a["signal_ot"],
                    )

                    if not engine.strike_ok(symbol, trade_strike):
                        continue

                    active = engine.trades.get(symbol)
                    if not a["entry_allowed"]:
                        if not active:
                            continue

                        if active.option_type == a["signal_ot"]:
                            continue

                    signal_desc = (
                        f"{symbol} ATM {trade_strike} {a['signal_ot']} "
                        f"(CROR {a['action']} {a['lots']} lots)"
                    )
                    source = engine.short_cror_source(a)
                    trade, msgs = engine.signal(
                        symbol,
                        trade_strike,
                        a["signal_ot"],
                        False,
                        source,
                        reference_future_price=a["fut_price"],
                    )
                    for msg in msgs:
                        tg(msg)
                    if trade:
                        tg(fmt(trade))

                return

        except Exception as e:
            print(f"HANDLER ERROR: {safe(e)}")
            if signal_desc: tg(f"BOT ERROR AFTER SIGNAL {signal_desc}: {safe(e)}")

    await asyncio.gather(
        client.run_until_disconnected(),
        monitor(),
    )

# =========================
# START
# =========================

if __name__ == "__main__":

    asyncio.run(main())
