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

# SmartAPI/logzero can print full request headers on failures, including
# Authorization/API keys. Keep library logs silent and print sanitized errors.
loglevel(logging.CRITICAL)

DEFAULT_STEP_POINTS = 30
MAX_TARGET_LEVEL = 4
DUPLICATE_MINUTES = 10
MONITOR_INTERVAL_SECONDS = 3

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
TRADE_UNDERLYINGS = parse_symbols(env("TRADE_UNDERLYINGS", "NIFTY,BANKNIFTY"), {"NIFTY", "BANKNIFTY"})
REAL_ALLOWED_UNDERLYINGS = parse_symbols(env("REAL_ALLOWED_UNDERLYINGS", "NIFTY,BANKNIFTY"), {"NIFTY", "BANKNIFTY"})
KEEPALIVE_ENABLED = env_bool("KEEPALIVE_ENABLED", "true")
KEEPALIVE_INTERVAL_SECONDS = env_int("KEEPALIVE_INTERVAL_SECONDS", "300")
KEEPALIVE_START_RAW = str(env("KEEPALIVE_START", "09:00"))
KEEPALIVE_END_RAW = str(env("KEEPALIVE_END", "15:30"))
KEEPALIVE_START = parse_hhmm(KEEPALIVE_START_RAW, "09:00")
KEEPALIVE_END = parse_hhmm(KEEPALIVE_END_RAW, "15:30")
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


def safe_error(exc: Exception) -> str:
    return type(exc).__name__

def safe_error_detail(exc: Exception) -> str:
    """
    Best-effort error details without leaking secrets into logs.
    """
    try:
        detail = str(exc) or type(exc).__name__
    except Exception:
        return type(exc).__name__

    secrets = [
        TG_API_HASH,
        TG_SESSION_STR,
        OUTPUT_BOT_TOKEN,
        str(OUTPUT_CHAT_ID) if OUTPUT_CHAT_ID else None,
        ANGEL_API_KEY,
        ANGEL_CLIENT_ID,
        ANGEL_PASSWORD,
        ANGEL_TOTP_SECRET,
    ]
    for secret in secrets:
        if secret and secret in detail:
            detail = detail.replace(secret, "***")

    # Telegram bot token pattern: <digits>:<token>
    detail = re.sub(r"\b\d{6,12}:[A-Za-z0-9_-]{20,}\b", "***", detail)
    return detail


class Engine:
    def __init__(self) -> None:
        self.smart = None
        self.df = None
        self.trades: dict[str, Trade] = {}
        self.last_signal: dict[str, datetime] = {}
        self._ws: SmartWebSocketV2 | None = None
        self._ws_thread: threading.Thread | None = None
        self._ws_lock = threading.Lock()
        self._ws_token: str | None = None
        self._ws_exchange: str | None = None
        self._ws_ltp: dict[str, float] = {}
        self._ws_ts: dict[str, float] = {}
        self.real_trade_day = datetime.now(IST).date()
        self.real_trades_today = 0

    def step_points_for(self, underlying: str) -> float:
        if underlying in STOCK_UNDERLYINGS:
            return 3.0
        return float(DEFAULT_STEP_POINTS)

    def ws_exchange_type(self, exchange: str) -> int:
        if not self._ws:
            raise RuntimeError("Angel WS exchange lookup failed: websocket not initialized")
        exchange_map = {
            "NFO": self._ws.NSE_FO,
            "BFO": self._ws.BSE_FO,
        }
        return exchange_map.get(exchange, self._ws.NSE_FO)

    def login(self) -> None:
        try:
            totp = pyotp.TOTP(ANGEL_TOTP_SECRET).now()
            self.smart = SmartConnect(api_key=ANGEL_API_KEY)
            self.smart.generateSession(ANGEL_CLIENT_ID, ANGEL_PASSWORD, totp)
        except Exception as exc:
            raise RuntimeError(f"Angel login failed: {safe_error_detail(exc)}") from None

    def _ensure_ws(self) -> None:
        if not USE_ANGEL_WS:
            return
        if not self.smart:
            return

        with self._ws_lock:
            if self._ws is not None and self._ws_thread is not None and self._ws_thread.is_alive():
                return

            auth_token = getattr(self.smart, "access_token", None)
            feed_token = self.smart.getfeedToken()
            client_code = getattr(self.smart, "userId", None) or ANGEL_CLIENT_ID
            if not auth_token or not feed_token or not client_code:
                raise RuntimeError("Angel WS init failed: missing auth/feed/client code")

            self._ws = SmartWebSocketV2(
                auth_token=auth_token,
                api_key=ANGEL_API_KEY,
                client_code=str(client_code),
                feed_token=feed_token,
            )

            def _on_open(wsapp):
                # Subscribe happens after _ws_token is set.
                token = None
                exchange = None
                with self._ws_lock:
                    token = self._ws_token
                    exchange = self._ws_exchange
                if not token:
                    return
                try:
                    self._ws.subscribe(
                        correlation_id="papertrade",
                        mode=self._ws.LTP_MODE,
                        token_list=[{"exchangeType": self.ws_exchange_type(exchange or "NFO"), "tokens": [str(token)]}],
                    )
                except Exception as exc:
                    print(f"Angel WS subscribe failed: {safe_error_detail(exc)}")

            def _on_data(wsapp, data):
                try:
                    payload = data
                    if isinstance(data, (bytes, bytearray)):
                        import json

                        payload = json.loads(data.decode("utf-8", errors="ignore"))
                    if not isinstance(payload, dict):
                        return

                    token = str(payload.get("token") or payload.get("symbolToken") or payload.get("instrument_token") or "")
                    if not token:
                        return

                    raw = payload.get("last_traded_price") or payload.get("ltp") or payload.get("LTP")
                    if raw is None:
                        return
                    ltp = float(raw)
                    # SmartAPI WS often uses paise; convert when needed.
                    if ltp > 100000:
                        ltp = ltp / 100.0

                    now_ts = time.time()
                    with self._ws_lock:
                        self._ws_ltp[token] = ltp
                        self._ws_ts[token] = now_ts
                except Exception:
                    return

            def _on_error(wsapp, error):
                print(f"Angel WS error: {safe_error_detail(Exception(str(error)))}")

            def _on_close(wsapp):
                print("Angel WS closed.")

            self._ws.on_open = _on_open
            self._ws.on_data = _on_data
            self._ws.on_error = _on_error
            self._ws.on_close = _on_close

            self._ws_thread = threading.Thread(target=self._ws.connect, daemon=True)
            self._ws_thread.start()

    def _subscribe_ws_token(self, exchange: str, token: str) -> None:
        if not USE_ANGEL_WS:
            return
        self._ensure_ws()
        if not self._ws:
            return

        with self._ws_lock:
            if self._ws_token == token and self._ws_exchange == exchange:
                return
            self._ws_token = token
            self._ws_exchange = exchange

        # If already connected, subscribe immediately (on_open also subscribes on reconnect).
        try:
            self._ws.subscribe(
                correlation_id="papertrade",
                mode=self._ws.LTP_MODE,
                token_list=[{"exchangeType": self.ws_exchange_type(exchange), "tokens": [str(token)]}],
            )
        except Exception:
            # Not yet connected; on_open will subscribe.
            pass

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

    def resolve(self, underlying: str, strike: int, option_type: str) -> tuple[str, str, str]:
        df = self.df[
            (self.df.name == underlying)
            & (self.df.symbol.str.contains(f"{strike}{option_type}", regex=False))
        ]
        if df.empty:
            raise RuntimeError(f"Scrip not found: {underlying} {strike} {option_type}")
        row = df.sort_values("expiry").iloc[0]
        return row.symbol, row.token, row.exch_seg

    def ltp(self, exchange: str, symbol: str, token: str) -> float:
        if USE_ANGEL_WS:
            now_ts = time.time()
            with self._ws_lock:
                ws_price = self._ws_ltp.get(str(token))
                ws_time = self._ws_ts.get(str(token), 0.0)
            if ws_price is not None and now_ts - ws_time <= WS_MAX_AGE_SECONDS:
                return float(ws_price)

        try:
            response = self.smart.ltpData(exchange, symbol, token)

            # SmartAPI responses can occasionally come back as JSON strings.
            if isinstance(response, str):
                try:
                    response = json.loads(response)
                except Exception:
                    pass

            if isinstance(response, dict):
                data_block: Any = response.get("data")
                if isinstance(data_block, str):
                    try:
                        data_block = json.loads(data_block)
                    except Exception:
                        pass

                if isinstance(data_block, dict):
                    raw = data_block.get("ltp") or data_block.get("LTP") or data_block.get("last_traded_price")
                    if raw is not None:
                        return float(raw)

                raw = response.get("ltp") or response.get("LTP")
                if raw is not None:
                    return float(raw)

                message = response.get("message") or response.get("error") or response.get("status")
                raise RuntimeError(f"Unexpected ltpData payload: {message or 'missing ltp'}")

            raise RuntimeError(f"Unexpected ltpData response type: {type(response).__name__}")
        except Exception as exc:
            raise RuntimeError(f"LTP fetch failed for {symbol}: {safe_error_detail(exc)}") from None

    def reset_daily_counter_if_needed(self) -> None:
        today = datetime.now(IST).date()
        if today != self.real_trade_day:
            self.real_trade_day = today
            self.real_trades_today = 0

    def qty_for(self, underlying: str) -> int:
        if underlying not in LOT_SIZES:
            raise RuntimeError(f"No lot size configured for {underlying}")
        return LOT_SIZES[underlying]

    def real_entry_block_reason(self, underlying: str) -> str | None:
        if not REAL_TRADE_ENABLED:
            return None

        self.reset_daily_counter_if_needed()
        now_time = datetime.now(IST).time()

        if underlying not in REAL_ALLOWED_UNDERLYINGS:
            return f"{underlying} is not allowed for real trade"
        if now_time < ALLOW_REAL_TRADING_AFTER:
            return f"real trading starts after {ALLOW_REAL_TRADING_AFTER_RAW}"
        if now_time >= STOP_REAL_TRADING_AFTER:
            return f"real trading stops after {STOP_REAL_TRADING_AFTER_RAW}"
        if self.real_trades_today >= MAX_TRADES_PER_DAY:
            return f"max real trades reached: {MAX_TRADES_PER_DAY}"
        return None

    def extract_order_id(self, response: Any) -> str:
        if isinstance(response, dict):
            data = response.get("data")
            if isinstance(data, dict):
                order_id = data.get("orderid") or data.get("order_id")
                if order_id:
                    return str(order_id)
            if data:
                return str(data)
            order_id = response.get("orderid") or response.get("order_id")
            if order_id:
                return str(order_id)
            raise RuntimeError(f"Order rejected: {response.get('message') or response.get('error') or response}")
        if response:
            return str(response)
        raise RuntimeError("Order rejected: empty response")

    def place_real_order(self, trade: Trade, transaction_type: str) -> str:
        if not REAL_TRADE_ENABLED:
            return ""
        if not self.smart:
            raise RuntimeError("Angel order failed: SmartAPI is not logged in")

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

        try:
            response = self.smart.placeOrder(order_params)
            return self.extract_order_id(response)
        except Exception as exc:
            raise RuntimeError(f"{transaction_type} order failed for {trade.symbol}: {safe_error_detail(exc)}") from None

    def open_real_trade(self, trade: Trade) -> None:
        order_id = self.place_real_order(trade, "BUY")
        if order_id:
            trade.entry_order_id = order_id
            self.reset_daily_counter_if_needed()
            self.real_trades_today += 1

    def exit_real_trade(self, trade: Trade) -> str:
        order_id = self.place_real_order(trade, "SELL")
        if order_id:
            trade.exit_order_id = order_id
        return order_id

    def parse_dual_match(self, text: str) -> tuple[str, int, str] | None:
        upper = text.upper()
        if "INSTITUTIONAL DUAL MATCH" not in upper:
            return None
        symbol_pattern = "|".join(sorted(SUPPORTED_UNDERLYINGS, key=len, reverse=True))
        match = re.search(
            rf"ACTION:\s*BUY\s+({symbol_pattern})\s+(\d+)\s*(CE|PE)",
            text,
            re.IGNORECASE,
        )
        if not match:
            match = re.search(
                rf"({symbol_pattern})\s+(\d+)\s*(CE|PE)",
                text,
                re.IGNORECASE,
            )
        if not match:
            return None
        return match.group(1).upper(), int(match.group(2)), match.group(3).upper()

    def duplicate(self, key: str) -> bool:
        now = datetime.now(IST)
        if key in self.last_signal and now - self.last_signal[key] < timedelta(minutes=DUPLICATE_MINUTES):
            return True
        self.last_signal[key] = now
        return False

    def create_trade(self, underlying: str, strike: int, option_type: str) -> Trade:
        symbol, token, exchange = self.resolve(underlying, strike, option_type)
        self._subscribe_ws_token(exchange, token)
        step_points = self.step_points_for(underlying)
        price = self.ltp(exchange, symbol, token)
        targets = [price + step_points * i for i in range(1, MAX_TARGET_LEVEL + 1)]
        qty = self.qty_for(underlying)
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
            qty=qty,
            highest_target=0,
            last_price_alert=price,
        )

    def process_signal(self, underlying: str, strike: int, option_type: str):
        if underlying not in TRADE_UNDERLYINGS:
            return None, "SYMBOL_BLOCKED"

        key = f"{underlying}_{strike}{option_type}"
        if self.duplicate(key):
            return None, "DUP"

        active = self.trades.get(underlying)

        # Reverse only within the same underlying (BANKNIFTY vs NIFTY are independent).
        if active and active.option_type != option_type:
            block_reason = self.real_entry_block_reason(underlying)
            new_trade = None
            if not block_reason:
                new_trade = self.create_trade(underlying, strike, option_type)
            exit_price = self.ltp(active.exchange, active.symbol, active.token)
            old_trade = active
            exit_order_id = self.exit_real_trade(old_trade) if REAL_TRADE_ENABLED else ""

            if block_reason:
                self.trades.pop(underlying, None)
                return ("EXIT_ONLY", old_trade, exit_price, exit_order_id, block_reason)

            if new_trade:
                try:
                    self.open_real_trade(new_trade)
                except Exception:
                    self.trades.pop(underlying, None)
                    raise
                self.trades[underlying] = new_trade
                return ("REV", old_trade, exit_price, exit_order_id, new_trade)

        if active:
            return None, "ACTIVE"

        block_reason = self.real_entry_block_reason(underlying)
        if block_reason:
            return None, f"REAL_BLOCKED: {block_reason}"

        new_trade = self.create_trade(underlying, strike, option_type)
        self.open_real_trade(new_trade)
        self.trades[underlying] = new_trade
        return ("NEW", new_trade)

    def update(self) -> list[str]:
        if not self.trades:
            return []

        messages: list[str] = []
        to_delete: list[str] = []

        for underlying, trade in list(self.trades.items()):
            price = self.ltp(trade.exchange, trade.symbol, trade.token)

            if REAL_TRADE_ENABLED and datetime.now(IST).time() >= STOP_REAL_TRADING_AFTER:
                exit_order_id = self.exit_real_trade(trade)
                suffix = f" | SELL Order: {exit_order_id}" if exit_order_id else ""
                messages.append(f"\u23f1\ufe0f {underlying} TIME EXIT @ {price:.2f}{suffix}")
                to_delete.append(underlying)
                continue

            if price <= trade.sl:
                exit_order_id = self.exit_real_trade(trade) if REAL_TRADE_ENABLED else ""
                suffix = f" | SELL Order: {exit_order_id}" if exit_order_id else ""
                messages.append(f"\u274c {underlying} SL HIT @ {price:.2f}{suffix}")
                to_delete.append(underlying)
                continue

            if trade.highest_target < MAX_TARGET_LEVEL and price >= trade.targets[trade.highest_target]:
                trade.highest_target += 1
                trade.sl = trade.entry if trade.highest_target == 1 else trade.targets[trade.highest_target - 2]
                messages.append(
                    f"\U0001f3af {trade.underlying} {trade.strike} {trade.option_type} "
                    f"T{trade.highest_target} HIT @ {price:.2f}"
                )

            if price > trade.last_price_alert:
                trade.last_price_alert = price
                messages.append(f"\U0001f4c8 {underlying} Price Update: {price:.2f}")

        for underlying in to_delete:
            self.trades.pop(underlying, None)

        return messages


engine = Engine()


def format_trade(trade: Trade) -> str:
    lines = [
        f"\U0001f525 {trade.underlying} {trade.strike} {trade.option_type}",
        "",
        f"Qty: {trade.qty}",
    ]
    if trade.entry_order_id:
        lines.append(f"BUY Order: {trade.entry_order_id}")
    lines.extend(
        [
            f"\U0001f4cd Entry: {trade.entry:.2f}",
            f"\U0001f6e1\ufe0f SL: {trade.sl:.2f}",
            f"\U0001f3af T1: {trade.targets[0]:.2f}",
            f"\U0001f3af T2: {trade.targets[1]:.2f}",
            f"\U0001f3af T3: {trade.targets[2]:.2f}",
            f"\U0001f3af T4: {trade.targets[3]:.2f}",
        ]
    )
    return "\n".join(lines)


def send_output(text: str) -> None:
    try:
        response = requests.post(
            f"https://api.telegram.org/bot{OUTPUT_BOT_TOKEN}/sendMessage",
            data={"chat_id": OUTPUT_CHAT_ID, "text": text},
            timeout=30,
        )
        if response.status_code >= 400:
            raise RuntimeError(f"Telegram send failed with HTTP {response.status_code}")
    except Exception as exc:
        raise RuntimeError(f"Telegram send failed: {safe_error(exc)}") from None


def time_in_window(now_time: datetime.time, start: datetime.time, end: datetime.time) -> bool:
    return start <= now_time < end


def runtime_summary() -> str:
    return "\n".join(
        [
            "REAL TRADE BOT STARTED",
            f"Mode: {'REAL' if REAL_TRADE_ENABLED else 'PAPER'}",
            f"Source: {SOURCE_CHAT}",
            f"Trade symbols: {','.join(sorted(TRADE_UNDERLYINGS))}",
            f"Allowed real symbols: {','.join(sorted(REAL_ALLOWED_UNDERLYINGS))}",
            f"Qty: {','.join(f'{symbol}={qty}' for symbol, qty in sorted(LOT_SIZES.items()))}",
            f"Real entry window: {ALLOW_REAL_TRADING_AFTER_RAW}-{STOP_REAL_TRADING_AFTER_RAW}",
            f"Max real entries/day: {MAX_TRADES_PER_DAY}",
            f"Keepalive: {'ON' if KEEPALIVE_ENABLED else 'OFF'} {KEEPALIVE_START_RAW}-{KEEPALIVE_END_RAW}",
        ]
    )


def telegram_keepalive() -> None:
    if not OUTPUT_BOT_TOKEN:
        return
    response = requests.get(
        f"https://api.telegram.org/bot{OUTPUT_BOT_TOKEN}/getMe",
        timeout=15,
    )
    if response.status_code >= 400:
        raise RuntimeError(f"Telegram keepalive failed with HTTP {response.status_code}")


async def keepalive_loop() -> None:
    while True:
        if KEEPALIVE_ENABLED and time_in_window(datetime.now(IST).time(), KEEPALIVE_START, KEEPALIVE_END):
            try:
                telegram_keepalive()
                print("Market-hours keepalive ok.")
            except Exception as exc:
                print(f"Market-hours keepalive failed: {safe_error_detail(exc)}")
        await asyncio.sleep(KEEPALIVE_INTERVAL_SECONDS)


async def monitor_loop() -> None:
    while True:
        try:
            messages = engine.update()
        except Exception as exc:
            print(f"Monitor update failed: {safe_error_detail(exc)}")
            await asyncio.sleep(MONITOR_INTERVAL_SECONDS)
            continue

        for message in messages:
            try:
                send_output(message)
            except Exception as exc:
                print(f"Monitor send failed: {safe_error_detail(exc)}")
        await asyncio.sleep(MONITOR_INTERVAL_SECONDS)


async def main() -> None:
    engine.login()
    engine.load()

    client = TelegramClient(StringSession(TG_SESSION_STR), TG_API_ID, TG_API_HASH)
    await client.start()
    print(f"Listening to source chat: {SOURCE_CHAT}")
    print(
        "Real trading:",
        "ON" if REAL_TRADE_ENABLED else "OFF",
        "| Trade symbols:",
        ",".join(sorted(TRADE_UNDERLYINGS)),
        "| Qty:",
        ",".join(f"{symbol}={qty}" for symbol, qty in sorted(LOT_SIZES.items())),
        "| Window:",
        f"{ALLOW_REAL_TRADING_AFTER_RAW}-{STOP_REAL_TRADING_AFTER_RAW}",
        "| Max/day:",
        MAX_TRADES_PER_DAY,
    )
    if STARTUP_CONFIRMATION_ENABLED:
        try:
            send_output(runtime_summary())
        except Exception as exc:
            print(f"Startup confirmation send failed: {safe_error_detail(exc)}")

    @client.on(events.NewMessage())
    async def handler(event):
        chat = await event.get_chat()
        text = event.raw_text or ""
        chat_id = getattr(event, "chat_id", None)
        title = getattr(chat, "title", None)
        username = getattr(chat, "username", None)
        first_name = getattr(chat, "first_name", None)

        source_match = False
        source_value = str(SOURCE_CHAT).strip().lower()
        candidates: list[str] = [str(chat_id).lower()]
        for value in (title, username, first_name):
            if value:
                candidates.append(str(value).strip().lower())
        if source_value in candidates:
            source_match = True

        if not source_match:
            return

        print(
            "Source message:",
            {
                "chat_id": chat_id,
                "title": title,
                "username": username,
                "first_name": first_name,
                "length": len(text),
            },
        )

        parsed = engine.parse_dual_match(text)
        if not parsed:
            print("Source message received, but no dual-match pattern found.")
            return

        underlying, strike, option_type = parsed
        print(f"Dual match detected: underlying={underlying}, strike={strike}, option_type={option_type}")
        try:
            result = engine.process_signal(underlying, strike, option_type)
        except Exception as exc:
            print(f"Signal processing failed: {safe_error_detail(exc)}")
            return

        if not result:
            print("No action taken for signal.")
            return
        if result[0] is None:
            print(f"No trade action: {result[1] if len(result) > 1 else 'UNKNOWN'}")
            return

        if result[0] == "REV":
            _, old_trade, exit_price, exit_order_id, new_trade = result
            exit_suffix = f" | SELL Order: {exit_order_id}" if exit_order_id else ""
            try:
                send_output(
                    f"\U0001f501 EXIT {old_trade.underlying} {old_trade.strike} {old_trade.option_type} @ {exit_price:.2f}{exit_suffix}"
                )
                send_output(format_trade(new_trade))
            except Exception as exc:
                print(f"Reversal send failed: {safe_error_detail(exc)}")
                return
            print("Reversal processed and output sent.")
        elif result[0] == "EXIT_ONLY":
            _, old_trade, exit_price, exit_order_id, reason = result
            exit_suffix = f" | SELL Order: {exit_order_id}" if exit_order_id else ""
            try:
                send_output(
                    f"\U0001f501 EXIT {old_trade.underlying} {old_trade.strike} {old_trade.option_type} @ {exit_price:.2f}{exit_suffix}\nNo new entry: {reason}"
                )
            except Exception as exc:
                print(f"Exit-only send failed: {safe_error_detail(exc)}")
                return
            print("Exit-only reversal processed.")
        elif result[0] == "NEW":
            _, trade = result
            try:
                send_output(format_trade(trade))
            except Exception as exc:
                print(f"New trade send failed: {safe_error_detail(exc)}")
                return
            print("New trade created and output sent.")

    await asyncio.gather(client.run_until_disconnected(), monitor_loop(), keepalive_loop())


if __name__ == "__main__":
    asyncio.run(main())
