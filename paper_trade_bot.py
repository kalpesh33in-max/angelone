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
NO_T1_EXIT_SECONDS = 180

REVERSE_PROTECT_POINTS = {
    "NIFTY": env_float("NIFTY_REVERSE_PROTECT_POINTS", "5"),
    "BANKNIFTY": env_float("BANKNIFTY_REVERSE_PROTECT_POINTS", "10"),
}

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

INDEX_SYMBOLS = {
    "NIFTY",
    "BANKNIFTY",
    "SENSEX",
    "MIDCPNIFTY",
}

STRIKE_STEPS = {
    "BANKNIFTY": env_int("BANKNIFTY_STRIKE_STEP", "100"),
    "NIFTY": env_int("NIFTY_STRIKE_STEP", "50"),
    "SENSEX": env_int("SENSEX_STRIKE_STEP", "100"),
    "MIDCPNIFTY": env_int("MIDCPNIFTY_STRIKE_STEP", "25"),
    "HDFCBANK": 5,
    "ICICIBANK": 10,
    "RELIANCE": 10,
}

FUT_LOT_THRESHOLD = env_int("FUT_LOT_THRESHOLD", "3000")
CROR_OPTION_WRITER_LOTS = env_int("CROR_OPTION_WRITER_LOTS", "500")
CROR_OPTION_SHORT_COVERING_LOTS = env_int("CROR_OPTION_SHORT_COVERING_LOTS", "1000")
CROR_OPTION_BUYER_LOTS = env_int("CROR_OPTION_BUYER_LOTS", "1000")
CROR_STOCK_FUT_LOTS = env_int("CROR_STOCK_FUT_LOTS", "1000")
CROR_INDEX_FUT_LOTS = env_int("CROR_INDEX_FUT_LOTS", "3000")
ITM_WRITER_THRESHOLD_CR = env_float("ITM_WRITER_THRESHOLD_CR", "11")
ITM_SC_THRESHOLD_CR = env_float("ITM_SC_THRESHOLD_CR", "20")
ITM_WRITER_CONFLICT_CR = env_float("ITM_WRITER_CONFLICT_CR", "10")
DUAL_MATCH_WINDOW_SECONDS = env_int("DUAL_MATCH_WINDOW_SECONDS", "60")
OTM_DUAL_2MIN_TURN_CR = env_float("OTM_DUAL_2MIN_TURN_CR", "15")
OTM_DUAL_2MIN_COMPONENT_CR = env_float("OTM_DUAL_2MIN_COMPONENT_CR", "10")
OTM_DUAL_5MIN_TURN_CR = env_float("OTM_DUAL_5MIN_TURN_CR", "2")
OTM_DUAL_5MIN_COMPONENT_CR = env_float("OTM_DUAL_5MIN_COMPONENT_CR", "1")

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
        requests.post(
            f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
            data={
                "chat_id": CHAT_ID,
                "text": text,
            },
            timeout=30,
        )
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

        self.trades = {}
        self.last_signal = {}
        self.real_trade_day = None
        self.real_entries_today = 0

        # State tracking for flow signals
        self.last_signals_by_symbol = {}
        self.last_otm_signals_by_symbol = {}
        self.instant_itm_alerts = {}
        self.price_history = {} # {symbol: [(price, timestamp), ...]}

    # =====================

    def update_price_history(self, symbol, price):
        if symbol not in self.price_history:
            self.price_history[symbol] = []
        self.price_history[symbol].append((price, time.time()))
        # Keep only last 5 entries (approx 10 minutes of data)
        if len(self.price_history[symbol]) > 5:
            self.price_history[symbol].pop(0)

    def check_momentum(self, symbol, side):
        history = self.price_history.get(symbol, [])
        if len(history) < 2:
            return True, "No History" # Not enough data yet

        current_p, _ = history[-1]
        prev_p, _ = history[-2] # Price from 2 mins ago

        if side == "CALL":
            res = current_p >= prev_p
            return res, f"Bullish (Curr:{current_p} >= Prev:{prev_p})" if res else f"Bearish (Curr:{current_p} < Prev:{prev_p})"
        else:
            res = current_p <= prev_p
            return res, f"Bearish (Curr:{current_p} <= Prev:{prev_p})" if res else f"Bullish (Curr:{current_p} > Prev:{prev_p})"

    async def delayed_momentum_signal(self, symbol, strike, ot, source, original_price, tg_func, fmt_func):
        """Wait 60s and re-check LTP for momentum alignment"""
        await asyncio.sleep(60)
        try:
            # Resolve instrument again to get fresh LTP
            sym, token, ex, lot_size = self.resolve(symbol, strike, ot)
            current_ltp = self.ltp(ex, sym, token)

            # Check if price has moved in direction compared to original signal
            aligned = False
            if ot == "CE" and current_ltp > original_price: aligned = True
            elif ot == "PE" and current_ltp < original_price: aligned = True

            if aligned:
                trade, msgs = self.signal(symbol, strike, ot, False, f"{source} (Momentum Confirmed 60s)")
                for msg in msgs: tg_func(msg)
                if trade: tg_func(fmt_func(trade))
            else:
                tg_func(f"⚠️ SIGNAL SKIPPED: {symbol} {strike} {ot}\nNo Momentum alignment after 60s wait.\nOrig: {original_price} | Now: {current_ltp}")
        except Exception as e:
            print(f"DELAYED SIGNAL ERROR: {e}")

    # =====================

    def get_atm(self, price, symbol):
        step = STRIKE_STEPS.get(symbol.upper(), 100)
        return int(round(float(price) / step) * step)

    def _normalize_cr(self, value, unit):
        try:
            val = float(value)
            return val if unit == "Cr" else (val / 100 if unit == "L" else 0.0)
        except: return 0.0

    def get_writing_values(self, label, text):
        pattern = rf"{label}\s+\d+\(([\d.]+)(Cr|L|)\)\s+\d+\(([\d.]+)(Cr|L|)\)"
        matches = re.findall(pattern, text, re.IGNORECASE)
        if not matches: return 0.0, 0.0
        itm_val, itm_unit, otm_val, otm_unit = matches[0]
        return self._normalize_cr(itm_val, itm_unit), self._normalize_cr(otm_val, otm_unit)

    def get_value(self, label, text):
        pattern = rf"{label}\s*:\s*([\d.]+)(Cr|L|)"
        matches = re.findall(pattern, text, re.IGNORECASE)
        if not matches: return 0.0
        val_str, unit = matches[-1]
        return self._normalize_cr(val_str, unit)

    def get_future_price(self, text, symbol):
        if not text: return None
        pattern = rf"(?<![A-Z0-9_]){re.escape(symbol)}\s*\(FUT:\s*([\d.]+)\)"
        match = re.search(pattern, text, re.IGNORECASE)
        return float(match.group(1)) if match else None

    def extract_instrument_section(self, text, symbol):
        sym_pat = rf"(?<![A-Z0-9_]){re.escape(symbol)}\s*\(FUT:"
        m = re.search(sym_pat, text, re.IGNORECASE)
        if not m: return None
        start = m.start()
        next_pos = [len(text)]
        for sym in SUPPORTED:
            if sym == symbol: continue
            m2 = re.search(rf"(?<![A-Z0-9_]){re.escape(sym)}\s*\(FUT:", text[m.end():], re.IGNORECASE)
            if m2: next_pos.append(m.end() + m2.start())
        return text[start:min(next_pos)]

    def parse_flow_metrics(self, section):
        if not section: return None
        opt_part = section.split("---- FUTURES FLOW ----")[0]
        c_itm, c_otm = self.get_writing_values("CALL_WR", opt_part)
        p_itm, p_otm = self.get_writing_values("PUT_WR", opt_part)
        cs_itm, cs_otm = self.get_writing_values("CALL_SC", opt_part)
        ps_itm, ps_otm = self.get_writing_values("PUT_SC", opt_part)
        return {
            "bull_t": self.get_value("Bullish Turn", opt_part),
            "bear_t": self.get_value("Bearish Turn", opt_part),
            "call_itm": c_itm, "call_otm": c_otm,
            "put_itm": p_itm, "put_otm": p_otm,
            "call_sc_itm": cs_itm, "call_sc_otm": cs_otm,
            "put_sc_itm": ps_itm, "put_sc_otm": ps_otm
        }

    def get_otm_dual_signal(self, metrics, short_lbl):
        if short_lbl == "2MIN":
            turn_min = OTM_DUAL_2MIN_TURN_CR
            component_min = OTM_DUAL_2MIN_COMPONENT_CR
        else:
            turn_min = OTM_DUAL_5MIN_TURN_CR
            component_min = OTM_DUAL_5MIN_COMPONENT_CR

        bullish_components = [
            ("PUT_WR OTM", metrics["put_otm"]),
            ("CALL_SC OTM", metrics["call_sc_otm"]),
        ]
        bearish_components = [
            ("CALL_WR OTM", metrics["call_otm"]),
            ("PUT_SC OTM", metrics["put_sc_otm"]),
        ]
        bull_label, bull_component = max(bullish_components, key=lambda item: item[1])
        bear_label, bear_component = max(bearish_components, key=lambda item: item[1])

        call_ok = metrics["bull_t"] >= turn_min and bull_component >= component_min
        put_ok = metrics["bear_t"] >= turn_min and bear_component >= component_min

        if call_ok and not put_ok:
            return {"type": "CALL", "turn": metrics["bull_t"], "component_label": bull_label, "component_value": bull_component}
        if put_ok and not call_ok:
            return {"type": "PUT", "turn": metrics["bear_t"], "component_label": bear_label, "component_value": bear_component}
        return None

    def get_dual_match_thresholds(self, symbol, short_lbl, now):
        if symbol == "BANKNIFTY" and 1 <= now.day <= 10:
            return (5.0, 5.0) if short_lbl == "2MIN" else (1.0, 1.0)
        return (10.0, 6.5) if short_lbl == "2MIN" else (2.0, 1.0)

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

        df = pd.read_json(MASTER_FILE)

        df = df[
            df.exch_seg.isin(["NFO", "BFO"])
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

        if "WATCH" in up:
            return None

        if not self.signal_source(text):
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
                    trade_option_type(ot),
                )
            )

        return valid[-1] if valid else None

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
                threshold = (
                    CROR_INDEX_FUT_LOTS
                    if symbol in INDEX_SYMBOLS
                    else CROR_STOCK_FUT_LOTS
                )

                if lots >= threshold:
                    signal_ot = None
                    if action in {"SHORT COVERING", "FUT BUY", "BUY", "BUYER"}:
                        signal_ot = "CE"
                    elif action in {"UNWINDING", "FUT SELL", "SELL"}:
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
                        }
                    )

        return alerts

    def cror_alert_text(self, a):
        if a["kind"] == "FUTURE":
            extra = ""
            if a.get("signal_ot"):
                side = "CALL" if a["signal_ot"] == "CE" else "PUT"
                extra = f"\nSIGNAL: ATM {side}"
            return (
                f"CROR FUTURE ALERT\n"
                f"{a['symbol']} FUT {a['action']}\n"
                f"LOTS: {a['lots']} >= {a['threshold']}\n"
                f"TURNOVER: {a['turnover']} Cr\n"
                f"PRICE: {a['price']}"
                f"{extra}"
            )

        side = "CALL" if a["signal_ot"] == "CE" else "PUT"
        return (
            f"{a['symbol']} {a['strike']} {a['option_type']} {a['moneyness']}\n"
            f"ACTION: {a['action']} LOTS: {a['lots']} >= {a['threshold']}"
        )

    # =====================

    def signal_source(self, text):

        up = text.upper()

        if "STANDARD BALANCED FLOW" in up:
            return "STANDARD BALANCED FLOW"

        if "DIRECT: AGGRESSIVE OTM WRITER" in up:
            return "DIRECT: AGGRESSIVE OTM WRITER"

        if "DIRECT: AGGRESSIVE OTM SHORT COVERING" in up:
            return "DIRECT: AGGRESSIVE OTM SHORT COVERING"

        if "DIRECT: AGGRESSIVE ITM WRITER" in up:
            return "DIRECT: AGGRESSIVE ITM WRITER"

        if "DIRECT: AGGRESSIVE ITM SHORT COVERING" in up:
            return "DIRECT: AGGRESSIVE ITM SHORT COVERING"

        if "FULL 2MIN FAST ITM WRITING" in up:
            return "FULL 2MIN FAST ITM WRITING"

        if "FULL 2MIN ITM WRITING" in up:
            return "FULL 2MIN ITM WRITING"

        if "FULL 2MIN OPTION+FUTURE" in up:
            return "FULL 2MIN OPTION+FUTURE"

        if "INSTITUTIONAL FULL" in up:
            return "INSTITUTIONAL FULL"

        if "INSTITUTIONAL DUAL MATCH" in up:
            return "INSTITUTIONAL DUAL MATCH"

        return None

    # =====================

    def parse_exit(self, text):

        up = text.upper()

        pat = "|".join(
            sorted(
                SUPPORTED,
                key=len,
                reverse=True,
            )
        )

        m = re.search(
            rf"EXIT\s+({pat})\b",
            up,
        )

        if not m:
            return None

        pending = None

        pm = re.search(
            rf"PENDING:\s*BUY\s+({pat})\s+(\d{{4,6}})\s*(CE|PE)",
            up,
        )

        if pm:
            pending = (
                pm.group(1),
                int(pm.group(2)),
                trade_option_type(pm.group(3)),
            )

        return m.group(1), pending

    # =====================

    def parse_cancel(self, text):

        up = text.upper()

        if "REVERSE CANCELLED" not in up:
            return None

        pat = "|".join(
            sorted(
                SUPPORTED,
                key=len,
                reverse=True,
            )
        )

        pm = re.search(
            rf"PENDING:\s*BUY\s+({pat})\s+(\d{{4,6}})\s*(CE|PE)",
            up,
        )

        if not pm:
            return "🚫 REVERSE CANCELLED"

        return (
            "🚫 REVERSE CANCELLED\n\n"
            f"PENDING: BUY {pm.group(1)} "
            f"{int(pm.group(2))} {trade_option_type(pm.group(3))}\n"
            "REASON: NEXT 2MIN FLOW NOT CONFIRMED"
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
            int(float(row.get("lotsize", LOT_SIZES.get(u, 1)))),
        )

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

    def real_entry_block_reason(self, u):

        if not REAL_TRADE_ENABLED:
            return "REAL_TRADE_ENABLED=false"

        if u not in REAL_ALLOWED_UNDERLYINGS:
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
        sl = price - step
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
            high_price=price,
            is_reverse=is_reverse,
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

            oid = self.real_order(
                trade,
                "SELL",
                price,
            )

            trade.exit_order_id = oid
            trade.real_open = False

            msgs.append(
                f"✅ REAL SELL {trade.underlying} "
                f"{trade.strike} "
                f"{trade.option_type} "
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

    def exit_only(self, u, pending):

        msgs = []
        active = self.trades.get(u)

        if active:

            ok, exit_msgs = self.close_trade(
                active,
                "REVERSE EXIT ONLY",
                None,
            )

            msgs.extend(exit_msgs)

            if not ok:
                return msgs

            msgs.append(
                f"🔄 {u} OLD "
                f"{active.strike} "
                f"{active.option_type} "
                f"EXITED\n"
                f"REASON: REVERSE EXIT ONLY"
            )

            del self.trades[u]

        else:

            msgs.append(
                f"⚠️ {u} REVERSE EXIT ONLY\n"
                f"NO ACTIVE TRADE"
            )

        if pending:

            pu, ps, pot = pending

            msgs.append(
                f"⏳ PENDING: BUY {pu} "
                f"{ps} {pot}\n"
                f"WAIT: NEXT 2MIN CONFIRMATION"
            )

        return msgs

    # =====================

    def signal(
        self,
        u,
        s,
        ot,
        reverse_confirmed=False,
        signal_source=None,
    ):

        msgs = []
        entry_block_reason = self.paper_entry_block_reason()

        active = self.trades.get(u)
        active_opposite = False

        if active:

            if active.option_type == ot:
                msgs.append(
                    f"🚀💥 {u} SAME DIRECTION SIGNAL AGAIN\n"
                    f"ACTIVE: {active.strike} {active.option_type} | "
                    f"NEW: {s} {ot}\n"
                    f"READY TO FLY / STRONG MOVEMENT"
                )
                return None, msgs

            active_opposite = True

            ok, exit_msgs = self.close_trade(
                active,
                (
                    "REVERSE CONFIRMED OLD EXIT"
                    if reverse_confirmed
                    else "REVERSE EXIT ONLY"
                ),
                None,
            )

            msgs.extend(exit_msgs)

            if not ok:
                return None, msgs

            msgs.append(
                f"🔄 {u} OLD "
                f"{active.strike} "
                f"{active.option_type} "
                f"EXITED ON "
                f"{'REVERSE CONFIRMED' if reverse_confirmed else 'REVERSE EXIT ONLY'}"
            )

            del self.trades[u]

            if not reverse_confirmed:
                msgs.append(
                    f"⏳ PENDING: BUY {u} {s} {ot}\n"
                    f"WAIT: NEXT 2MIN CONFIRMATION"
                )
                return None, msgs

            if entry_block_reason:
                msgs.append(
                    f"⏱️ PAPER ENTRY BLOCKED\n"
                    f"{u} {s} {ot}\n"
                    f"REASON: {entry_block_reason}"
                )
                return None, msgs

        key = f"{u}_{s}_{ot}"

        if entry_block_reason:
            msgs.append(
                f"⏱️ PAPER ENTRY BLOCKED\n"
                f"{u} {s} {ot}\n"
                f"REASON: {entry_block_reason}"
            )
            return None, msgs

        if not active_opposite and self.dup(key):

            print("DUP SIGNAL")

            msgs.append(
                f"🚀💥 {u} {s} {ot} "
                f"SAME DIRECTION REPEAT SIGNAL\n"
                f"READY TO FLY / STRONG MOVEMENT"
            )

            return None, msgs

        trade = self.create_trade(
            u,
            s,
            ot,
            reverse_confirmed,
            signal_source,
        )

        # PAPER TRADE FIRST

        self.trades[u] = trade

        print("PAPER TRADE CREATED")

        # REAL TRADE

        if REAL_TRADE_ENABLED:

            try:

                reason = self.real_entry_block_reason(u)

                if reason:
                    raise RuntimeError(reason)

                oid = self.real_order(
                    trade,
                    "BUY",
                    trade.entry,
                )

                trade.order_id = oid
                trade.real_open = True
                self.real_entries_today += 1

            except Exception as e:

                trade.real_error = safe(e)

                print(
                    f"REAL FAIL: {safe(e)}"
                )

        return trade, msgs

    # =====================

    def update(self):

        msgs = []

        if (
            datetime.now(IST).time()
            >= hhmm(PAPER_TRADE_STOP_TIME)
            and self.trades
        ):

            for u, t in list(self.trades.items()):

                try:
                    p = self.ltp(
                        t.exchange,
                        t.symbol,
                        t.token,
                    )
                    msgs.append(
                        f"⏰ {u} PAPER CUTOFF EXIT\n"
                        f"TIME: {PAPER_TRADE_STOP_TIME}\n"
                        f"EXIT @ {p:.2f}"
                    )
                except Exception as e:
                    p = None
                    msgs.append(
                        f"⏰ {u} PAPER CUTOFF EXIT\n"
                        f"TIME: {PAPER_TRADE_STOP_TIME}\n"
                        f"LTP ERROR: {safe(e)}"
                    )

                try:
                    ok, exit_msgs = self.close_trade(
                        t,
                        f"PAPER CUTOFF {PAPER_TRADE_STOP_TIME}",
                        p,
                    )

                    msgs.extend(exit_msgs)

                    if ok:
                        del self.trades[u]

                except Exception as e:
                    msgs.append(
                        f"⚠️ {u} PAPER CUTOFF EXIT FAILED: {safe(e)}"
                    )

            return msgs

        for u, t in list(self.trades.items()):

            try:

                p = self.ltp(
                    t.exchange,
                    t.symbol,
                    t.token,
                )

                # SL
                is_sl_hit = p <= t.sl

                if is_sl_hit:
                    msgs.append(f"❌ {u} TRAILING SL HIT @ {p:.2f}")
                    ok, exit_msgs = self.close_trade(t, "TRAILING SL HIT", p)
                    msgs.extend(exit_msgs)
                    if ok:
                        del self.trades[u]
                    continue
                
                # Check for Trailing SL update (if any momentum-based trailing exists, but here we use target-based)
                # trail = self.trail_sl(t) # Called inside target logic below

                # TARGET
                closed = False
                while (
                    t.target_hit < len(t.targets)
                    and p >= t.targets[t.target_hit]
                ):
                    t.target_hit += 1
                    target_no = t.target_hit
                    msgs.append(f"🎯 {u} T{target_no} HIT @ {p:.2f}")

                    # SL shift every target hit right
                    trail_res = self.trail_sl(t)
                    if trail_res:
                        old_sl, new_sl = trail_res
                        msgs.append(f"🛡️ {u} SL MOVED: {old_sl:.2f} -> {new_sl:.2f}")

                    if target_no >= MAX_TARGET:
                        msgs.append(f"🏁 {u} FINAL TARGET REACHED. EXITING.")
                        ok, exit_msgs = self.close_trade(t, f"T{target_no} HIT", p)
                        msgs.extend(exit_msgs)
                        if ok:
                            del self.trades[u]
                            closed = True
                        break

                if closed:
                    continue

                # TIME EXIT (3 MIN NO REACTION)
                # If price hasn't moved at least 30 pts (T1) in 3 minutes, cut the trade.
                if (
                    datetime.now(IST) - t.opened_at >= timedelta(seconds=NO_T1_EXIT_SECONDS)
                    and t.high_price < t.targets[0]
                ):
                    msgs.append(
                        f"⚠️ {u} NO REACTION EXIT\n\n"
                        f"Price failed to reach T1 within 3 minutes.\n"
                        f"EXIT @ {p:.2f}"
                    )
                    ok, exit_msgs = self.close_trade(t, "NO T1 3 MIN", p)
                    msgs.extend(exit_msgs)
                    if ok:
                        del self.trades[u]
                    continue
                    
                # PRICE
                new_fav = False
                if p > t.high_price:
                    t.high_price = p
                    new_fav = True

                if new_fav:
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
        f"📍 ENTRY: {t.entry:.2f}",
        f"🛡️ SL: {t.sl:.2f}",
        f"🎯 T1: {t.targets[0]:.2f}",
        f"🎯 T2: {t.targets[1]:.2f}",
        f"🎯 T3: {t.targets[2]:.2f}",
        f"🎯 T4: {t.targets[3]:.2f}",
        f"🎯 T5: {t.targets[4]:.2f}",
    ]

    if t.is_reverse:
        x.extend(
            [
                "",
                "✅ REVERSE CONFIRMED NEXT 2MIN",
            ]
        )

    if t.signal_source:
        x.extend(
            [
                "",
                f"SIGNAL SOURCE: {t.signal_source}",
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
                    tg(engine.cror_alert_text(a))

                    if a["kind"] == "FUTURE":
                        symbol = a["symbol"]
                        if not a.get("signal_ot"):
                            tg(
                                f"CROR FUTURE TRADE SKIPPED\n"
                                f"{symbol} FUT {a['action']} has no CALL/PUT direction"
                            )
                            continue

                        if not a.get("fut_price"):
                            tg(
                                f"CROR FUTURE TRADE SKIPPED\n"
                                f"{symbol} FUT missing Fut Price for ATM strike"
                            )
                            continue

                        trade_strike = engine.get_atm(a["fut_price"], symbol)
                        signal_desc = (
                            f"{symbol} ATM {trade_strike} {a['signal_ot']} "
                            f"(CROR FUT {a['action']} {a['lots']} lots)"
                        )
                        source = (
                            f"CROR FUT {a['action']} {a['lots']} lots "
                            f"ATM from Fut Price {a['fut_price']}"
                        )
                        trade, msgs = engine.signal(
                            symbol,
                            trade_strike,
                            a["signal_ot"],
                            False,
                            source,
                        )
                        for msg in msgs:
                            tg(msg)
                        if trade:
                            tg(fmt(trade))
                        continue

                    symbol = a["symbol"]
                    if not a.get("fut_price"):
                        tg(
                            f"CROR TRADE SKIPPED\n"
                            f"{symbol} {a['strike']} {a['option_type']} missing Fut Price for ATM strike"
                        )
                        continue

                    trade_strike = engine.get_atm(a["fut_price"], symbol)

                    if not engine.strike_ok(symbol, trade_strike):
                        tg(
                            f"CROR TRADE SKIPPED\n"
                            f"{symbol} ATM {trade_strike} strike out of allowed range"
                        )
                        continue

                    active = engine.trades.get(symbol)
                    if not a["entry_allowed"]:
                        if not active:
                            tg(
                                f"CROR ENTRY SKIPPED\n"
                                f"{symbol} {a['strike']} {a['option_type']} is NEAR-ITM\n"
                                f"RULE: NEAR-ITM is exit-only"
                            )
                            continue

                        if active.option_type == a["signal_ot"]:
                            tg(
                                f"CROR EXIT CHECK - NO ACTION\n"
                                f"{symbol} active {active.option_type}, signal {a['signal_ot']}\n"
                                f"RULE: NEAR-ITM exits only on opposite signal"
                            )
                            continue

                    signal_desc = (
                        f"{symbol} ATM {trade_strike} {a['signal_ot']} "
                        f"(CROR {a['action']} {a['lots']} lots)"
                    )
                    source = (
                        f"CROR {a['action']} {a['lots']} lots "
                        f"on {a['option_type']} {a['moneyness']} "
                        f"ATM from Fut Price {a['fut_price']}"
                    )
                    trade, msgs = engine.signal(
                        symbol,
                        trade_strike,
                        a["signal_ot"],
                        False,
                        source,
                    )
                    for msg in msgs:
                        tg(msg)
                    if trade:
                        tg(fmt(trade))

                return

            # =====================
            # FLOW SIGNAL DETECTION
            # =====================
            if "2 MIN" in text.upper() or "5 MIN" in text.upper():
                lbl = "2 MIN FLOW" if "2 MIN" in text.upper() else "5 MIN FLOW"
                short_lbl = "2MIN" if "2 MIN" in text.upper() else "5MIN"

                for symbol in SUPPORTED:
                    section = engine.extract_instrument_section(text, symbol)
                    if not section: continue

                    # Update Price History for Momentum check
                    price = engine.get_future_price(section, symbol)
                    if price: engine.update_price_history(symbol, price)

                    # 1. FUTURES LOT MATCH (2MIN only)
                    if short_lbl == "2MIN":
                        m = re.search(r"(FUT_BUY|FUT_SELL)\s*:\s*(\d+)\s+lots", section, re.IGNORECASE)
                        if m and int(m.group(2)) >= FUT_LOT_THRESHOLD:
                            sig_fut = "CALL" if m.group(1).upper() == "FUT_BUY" else "PUT"
                            strike = engine.get_atm(price, symbol) if price else None
                            if strike:
                                # Apply Momentum Filter
                                m_ok, m_reason = engine.check_momentum(symbol, sig_fut)
                                if m_ok:
                                    trade, msgs = engine.signal(symbol, strike, "CE" if sig_fut == "CALL" else "PE", False, f"{short_lbl} FUT LOTS >= {FUT_LOT_THRESHOLD}")
                                    for msg in msgs: tg(msg)
                                    if trade: tg(fmt(trade))
                                else:
                                    tg(f"⏳ MOMENTUM WAIT: {symbol} {sig_fut} (FUT LOTS)\nReason: {m_reason}. Re-checking in 60s...")
                                    asyncio.create_task(engine.delayed_momentum_signal(symbol, strike, "CE" if sig_fut == "CALL" else "PE", f"{short_lbl} FUT LOTS", price, tg, fmt))
                                continue

                    # 2. ITM WRITER ALERT (2MIN only)
                    metrics = engine.parse_flow_metrics(section)
                    if not metrics: continue

                    if short_lbl == "2MIN":
                        bullish_triggers = []
                        bearish_triggers = []
                        if metrics["put_itm"] >= ITM_WRITER_THRESHOLD_CR:
                            bullish_triggers.append(("PUT_WR", metrics["put_itm"]))
                        if metrics["call_sc_itm"] >= ITM_SC_THRESHOLD_CR:
                            bullish_triggers.append(("CALL_SC", metrics["call_sc_itm"]))
                        if metrics["call_itm"] >= ITM_WRITER_THRESHOLD_CR:
                            bearish_triggers.append(("CALL_WR", metrics["call_itm"]))
                        if metrics["put_sc_itm"] >= ITM_SC_THRESHOLD_CR:
                            bearish_triggers.append(("PUT_SC", metrics["put_sc_itm"]))

                        alert_side = None
                        if bullish_triggers and not bearish_triggers: alert_side = "CALL"
                        elif bearish_triggers and not bullish_triggers: alert_side = "PUT"

                        if alert_side:
                            label, val = max(bullish_triggers if alert_side == "CALL" else bearish_triggers, key=lambda x: x[1])
                            akey = f"{symbol}_{alert_side}_{label}_{now.strftime('%H:%M')}"
                            if akey not in engine.instant_itm_alerts:
                                engine.instant_itm_alerts[akey] = now
                                strike = engine.get_atm(price, symbol) if price else None
                                if strike:
                                    m_ok, m_reason = engine.check_momentum(symbol, alert_side)
                                    if m_ok:
                                        trade, msgs = engine.signal(symbol, strike, "CE" if alert_side == "CALL" else "PE", False, f"2MIN ITM {label} {val:.2f}Cr")
                                        for msg in msgs: tg(msg)
                                        if trade: tg(fmt(trade))
                                    else:
                                        tg(f"⏳ MOMENTUM WAIT: {symbol} {alert_side} (ITM WRITER)\nReason: {m_reason}. Re-checking in 60s...")
                                        asyncio.create_task(engine.delayed_momentum_signal(symbol, strike, "CE" if alert_side == "CALL" else "PE", f"2MIN ITM {label}", price, tg, fmt))
                                    continue

                    # 3. DUAL MATCH
                    sig_type = None
                    if symbol in ("BANKNIFTY", "NIFTY"):
                        m_turn, m_itm = engine.get_dual_match_thresholds(symbol, short_lbl, now)
                        if metrics["bull_t"] >= m_turn and metrics["put_itm"] >= m_itm and metrics["bear_t"] < 1.0: sig_type = "CALL"
                        elif metrics["bear_t"] >= m_turn and metrics["call_itm"] >= m_itm and metrics["bull_t"] < 1.0: sig_type = "PUT"
                    else:
                        if short_lbl == "2MIN":
                            if metrics["bull_t"] >= 6.0 and metrics["put_itm"] >= 3.5 and metrics["bear_t"] < 1.0: sig_type = "CALL"
                            elif metrics["bear_t"] >= 6.0 and metrics["call_itm"] >= 3.5 and metrics["bull_t"] < 1.0: sig_type = "PUT"
                        else:
                            if metrics["bull_t"] >= 1.0 and metrics["put_itm"] < 1.0 and metrics["bear_t"] < 1.0: sig_type = "CALL"
                            elif metrics["bear_t"] >= 1.0 and metrics["call_itm"] < 1.0 and metrics["bull_t"] < 1.0: sig_type = "PUT"

                    if sig_type:
                        if symbol not in engine.last_signals_by_symbol:
                            engine.last_signals_by_symbol[symbol] = {"2 MIN FLOW": None, "5 MIN FLOW": None}
                        engine.last_signals_by_symbol[symbol][lbl] = {"type": sig_type, "time": now}
                        other_lbl = "5 MIN FLOW" if short_lbl == "2MIN" else "2 MIN FLOW"
                        other = engine.last_signals_by_symbol[symbol].get(other_lbl)
                        if other and other["type"] == sig_type and abs((now - other["time"]).total_seconds()) <= DUAL_MATCH_WINDOW_SECONDS:
                            strike = engine.get_atm(price, symbol) if price else None
                            if strike:
                                m_ok, m_reason = engine.check_momentum(symbol, sig_type)
                                if m_ok:
                                    trade, msgs = engine.signal(symbol, strike, "CE" if sig_type == "CALL" else "PE", False, f"DUAL MATCH ({short_lbl} + {other_lbl})")
                                    for msg in msgs: tg(msg)
                                    if trade: tg(fmt(trade))
                                else:
                                    tg(f"⏳ MOMENTUM WAIT: {symbol} {sig_type} (DUAL MATCH)\nReason: {m_reason}. Re-checking in 60s...")
                                    asyncio.create_task(engine.delayed_momentum_signal(symbol, strike, "CE" if sig_type == "CALL" else "PE", f"DUAL MATCH ({short_lbl} + {other_lbl})", price, tg, fmt))
                            engine.last_signals_by_symbol[symbol] = {"2 MIN FLOW": None, "5 MIN FLOW": None}

                    # 4. OTM DUAL MATCH
                    if symbol in ("BANKNIFTY", "NIFTY"):
                        otm_sig = engine.get_otm_dual_signal(metrics, short_lbl)
                        if otm_sig:
                            if symbol not in engine.last_otm_signals_by_symbol:
                                engine.last_otm_signals_by_symbol[symbol] = {"2 MIN FLOW": None, "5 MIN FLOW": None}
                            engine.last_otm_signals_by_symbol[symbol][lbl] = {"type": otm_sig["type"], "time": now}
                            other_lbl = "5 MIN FLOW" if short_lbl == "2MIN" else "2 MIN FLOW"
                            other = engine.last_otm_signals_by_symbol[symbol].get(other_lbl)
                            if other and other["type"] == otm_sig["type"] and abs((now - other["time"]).total_seconds()) <= DUAL_MATCH_WINDOW_SECONDS:
                                strike = engine.get_atm(price, symbol) if price else None
                                if strike:
                                    m_ok, m_reason = engine.check_momentum(symbol, otm_sig["type"])
                                    if m_ok:
                                        trade, msgs = engine.signal(symbol, strike, "CE" if otm_sig["type"] == "CALL" else "PE", False, f"OTM DUAL MATCH ({short_lbl} + {other_lbl})")
                                        for msg in msgs: tg(msg)
                                        if trade: tg(fmt(trade))
                                    else:
                                        tg(f"⏳ MOMENTUM WAIT: {symbol} {otm_sig['type']} (OTM DUAL)\nReason: {m_reason}. Re-checking in 60s...")
                                        asyncio.create_task(engine.delayed_momentum_signal(symbol, strike, "CE" if otm_sig["type"] == "CALL" else "PE", f"OTM DUAL MATCH ({short_lbl} + {other_lbl})", price, tg, fmt))
                                engine.last_otm_signals_by_symbol[symbol] = {"2 MIN FLOW": None, "5 MIN FLOW": None}
                return

            # =====================
            # KEYWORD SIGNAL DETECTION
            # =====================
            cancel_msg = engine.parse_cancel(text)
            if cancel_msg:
                tg(cancel_msg)
                return

            exit_req = engine.parse_exit(text)
            if exit_req:
                u, pending = exit_req
                signal_desc = f"EXIT {u}"
                for msg in engine.exit_only(u, pending): tg(msg)
                return

            p = engine.parse(text)
            if not p:
                return

            u, s, ot = p
            source = engine.signal_source(text)
            signal_desc = f"{u} {s} {ot} ({source})"
            reverse_confirmed = "REVERSE CONFIRMED" in text.upper() or "FULL OPPOSITE" in text.upper()

            trade, msgs = engine.signal(u, s, ot, reverse_confirmed, source)
            for msg in msgs: tg(msg)
            if trade: tg(fmt(trade))

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
