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


def env(name: str, default: str | None = None) -> str | None:
    return os.getenv(name, default)


USE_ANGEL_WS = str(env("USE_ANGEL_WS", "false")).lower() in ("true", "1", "yes")
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


@dataclass
class Trade:
    underlying: str
    strike: int
    option_type: str
    symbol: str
    token: str
    entry: float
    sl: float
    targets: list[float]
    step_points: float
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
        self._ws_ltp: dict[str, float] = {}
        self._ws_ts: dict[str, float] = {}

    def step_points_for(self, underlying: str) -> float:
        # Stocks (and MIDCPNIFTY) use tighter paper-trade steps.
        if underlying in {"HDFCBANK", "ICICIBANK", "RELIANCE", "MIDCPNIFTY"}:
            return 10.0
        return float(DEFAULT_STEP_POINTS)

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
                with self._ws_lock:
                    token = self._ws_token
                if not token:
                    return
                try:
                    self._ws.subscribe(
                        correlation_id="papertrade",
                        mode=self._ws.LTP_MODE,
                        token_list=[{"exchangeType": self._ws.NSE_FO, "tokens": [str(token)]}],
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

    def _subscribe_ws_token(self, token: str) -> None:
        if not USE_ANGEL_WS:
            return
        self._ensure_ws()
        if not self._ws:
            return

        with self._ws_lock:
            if self._ws_token == token:
                return
            self._ws_token = token

        # If already connected, subscribe immediately (on_open also subscribes on reconnect).
        try:
            self._ws.subscribe(
                correlation_id="papertrade",
                mode=self._ws.LTP_MODE,
                token_list=[{"exchangeType": self._ws.NSE_FO, "tokens": [str(token)]}],
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
            (df.exch_seg == "NFO")
            & (df.name.isin(["BANKNIFTY", "NIFTY", "MIDCPNIFTY", "HDFCBANK", "ICICIBANK"]))
        ].copy()
        df["expiry"] = pd.to_datetime(df["expiry"], format="%d%b%Y")
        self.df = df[df.expiry >= datetime.now()].copy()

    def resolve(self, underlying: str, strike: int, option_type: str) -> tuple[str, str]:
        df = self.df[
            (self.df.name == underlying)
            & (self.df.symbol.str.contains(f"{strike}{option_type}", regex=False))
        ]
        if df.empty:
            raise RuntimeError(f"Scrip not found: {underlying} {strike} {option_type}")
        row = df.sort_values("expiry").iloc[0]
        return row.symbol, row.token

    def ltp(self, symbol: str, token: str) -> float:
        if USE_ANGEL_WS:
            now_ts = time.time()
            with self._ws_lock:
                ws_price = self._ws_ltp.get(str(token))
                ws_time = self._ws_ts.get(str(token), 0.0)
            if ws_price is not None and now_ts - ws_time <= WS_MAX_AGE_SECONDS:
                return float(ws_price)

        try:
            response = self.smart.ltpData("NFO", symbol, token)

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

    def parse_dual_match(self, text: str) -> tuple[str, int, str] | None:
        upper = text.upper()
        if "INSTITUTIONAL DUAL MATCH" not in upper:
            return None
        match = re.search(
            r"ACTION:\s*BUY\s+(BANKNIFTY|NIFTY|MIDCPNIFTY|HDFCBANK|ICICIBANK)\s+(\d+)\s*(CE|PE)",
            text,
            re.IGNORECASE,
        )
        if not match:
            match = re.search(
                r"(BANKNIFTY|NIFTY|MIDCPNIFTY|HDFCBANK|ICICIBANK)\s+(\d+)\s*(CE|PE)",
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
        symbol, token = self.resolve(underlying, strike, option_type)
        self._subscribe_ws_token(token)
        step_points = self.step_points_for(underlying)
        price = self.ltp(symbol, token)
        targets = [price + step_points * i for i in range(1, MAX_TARGET_LEVEL + 1)]
        return Trade(
            underlying=underlying,
            strike=strike,
            option_type=option_type,
            symbol=symbol,
            token=token,
            entry=price,
            sl=price - step_points,
            targets=targets,
            step_points=step_points,
            highest_target=0,
            last_price_alert=price,
        )

    def process_signal(self, underlying: str, strike: int, option_type: str):
        key = f"{underlying}_{strike}{option_type}"
        if self.duplicate(key):
            return None, "DUP"

        active = self.trades.get(underlying)

        # Reverse only within the same underlying (BANKNIFTY vs NIFTY are independent).
        if active and active.option_type != option_type:
            exit_price = self.ltp(active.symbol, active.token)
            old_trade = active
            new_trade = self.create_trade(underlying, strike, option_type)
            self.trades[underlying] = new_trade
            return ("REV", old_trade, exit_price, new_trade)

        if active:
            return None, "ACTIVE"

        new_trade = self.create_trade(underlying, strike, option_type)
        self.trades[underlying] = new_trade
        return ("NEW", new_trade)

    def update(self) -> list[str]:
        if not self.trades:
            return []

        messages: list[str] = []
        to_delete: list[str] = []

        for underlying, trade in list(self.trades.items()):
            price = self.ltp(trade.symbol, trade.token)

            if price <= trade.sl:
                messages.append(f"\u274c {underlying} SL HIT @ {price:.2f}")
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
    return "\n".join(
        [
            f"\U0001f525 {trade.underlying} {trade.strike} {trade.option_type}",
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

        if result[0] == "REV":
            _, old_trade, exit_price, new_trade = result
            try:
                send_output(
                    f"\U0001f501 EXIT {old_trade.underlying} {old_trade.strike} {old_trade.option_type} @ {exit_price:.2f}"
                )
                send_output(format_trade(new_trade))
            except Exception as exc:
                print(f"Reversal send failed: {safe_error_detail(exc)}")
                return
            print("Reversal processed and output sent.")
        elif result[0] == "NEW":
            _, trade = result
            try:
                send_output(format_trade(trade))
            except Exception as exc:
                print(f"New trade send failed: {safe_error_detail(exc)}")
                return
            print("New trade created and output sent.")

    await asyncio.gather(client.run_until_disconnected(), monitor_loop())


if __name__ == "__main__":
    asyncio.run(main())
