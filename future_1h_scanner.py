import json
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import pandas as pd
from SmartApi.smartWebSocketV2 import SmartWebSocketV2

from auth import get_angel_session
from data_manager import fetch_candle_df, load_nfo_futures
from env_config import (
    ANGEL_API_KEY,
    ANGEL_CLIENT_ID,
    FLAT_VOLUME_THRESHOLD_PERCENT,
    GAP_THRESHOLD_PERCENT,
    LOGIC_START_TIME,
    VWAP_REFRESH_SECONDS,
)
from telegram_utils import send_future_scanner_alert


IST = ZoneInfo("Asia/Kolkata")
EXCHANGE_TYPE_NFO = 2
INTERVAL_1M = "ONE_MINUTE"
INTERVAL_1H = "ONE_HOUR"


@dataclass
class Setup:
    token: str
    symbol: str
    name: str
    prev_close: float
    today_open: float
    gap_percent: float
    opening_type: str
    first_candle_color: str
    first_high: float
    first_low: float
    first_volume: float
    prev_hour_volume: float
    volume_change_percent: float | None
    breakout_side: str
    breakout_level: float
    vwap: float
    alert_state: str = "NONE"
    last_vwap_check: float = 0.0


class Future1HScanner:
    def __init__(self):
        self.obj = None
        self.session_data = None
        self.ws = None
        self.ws_connected = False
        self.instruments = {}
        self.setups = {}
        self.latest_prices = {}
        self.startup_alert_sent = False

    def initialize(self):
        print("--- Angel One NFO 1H Futures Scanner ---")
        self.obj, self.session_data = get_angel_session()
        if not self.obj:
            return False

        self.instruments = load_nfo_futures()
        if not self.instruments:
            print("No NFO futures instruments loaded.")
            return False

        self.prepare_setups()
        print(f"Prepared {len(self.setups)} active 1H setups.")
        return True

    def prepare_setups(self):
        self.setups = {}
        now = datetime.now(IST)
        logic_hour, logic_minute = [int(part) for part in LOGIC_START_TIME.split(":", 1)]
        logic_start = now.replace(hour=logic_hour, minute=logic_minute, second=0, microsecond=0)
        if now < logic_start:
            print(f"Waiting until {LOGIC_START_TIME} IST for first 1H candle completion.")
            return

        from_day = now - timedelta(days=7)
        today = now.date()

        for token, inst in self.instruments.items():
            try:
                hour_df = fetch_candle_df(
                    self.obj,
                    "NFO",
                    token,
                    INTERVAL_1H,
                    from_day.replace(hour=9, minute=15, second=0, microsecond=0),
                    now,
                )
                minute_df = fetch_candle_df(
                    self.obj,
                    "NFO",
                    token,
                    INTERVAL_1M,
                    now.replace(hour=9, minute=15, second=0, microsecond=0),
                    now,
                )
                setup = self._build_setup(token, inst, hour_df, minute_df, today)
                if setup:
                    self.setups[token] = setup
            except Exception as exc:
                print(f"Setup build failed for {inst['symbol']}: {exc}")

    def _build_setup(self, token, inst, hour_df, minute_df, today):
        if hour_df.empty or minute_df.empty:
            return None

        minute_today = minute_df[minute_df["timestamp"].dt.date == today].copy()
        hour_today = hour_df[hour_df["timestamp"].dt.date == today].copy()
        prev_hour = hour_df[hour_df["timestamp"].dt.date < today].copy()
        if minute_today.empty or hour_today.empty or prev_hour.empty:
            return None

        first_hour = hour_today.iloc[0]
        prev_day_close = float(prev_hour.iloc[-1]["close"])
        prev_hour_volume = float(prev_hour.iloc[-1]["volume"])
        today_open = float(minute_today.iloc[0]["open"])
        gap_percent = ((today_open - prev_day_close) / prev_day_close) * 100 if prev_day_close else 0.0

        if gap_percent >= GAP_THRESHOLD_PERCENT:
            opening_type = "GAP UP"
        elif gap_percent <= -GAP_THRESHOLD_PERCENT:
            opening_type = "GAP DOWN"
        else:
            opening_type = "FLAT OPENING"

        first_open = float(first_hour["open"])
        first_close = float(first_hour["close"])
        first_high = float(first_hour["high"])
        first_low = float(first_hour["low"])
        first_volume = float(first_hour["volume"])

        if first_close > first_open:
            color = "GREEN"
            breakout_side = "BUY"
            breakout_level = first_high
        elif first_close < first_open:
            color = "RED"
            breakout_side = "SELL"
            breakout_level = first_low
        else:
            return None

        volume_change_percent = None
        if prev_hour_volume > 0:
            volume_change_percent = ((first_volume - prev_hour_volume) / prev_hour_volume) * 100

        if opening_type == "FLAT OPENING":
            if volume_change_percent is None or volume_change_percent < FLAT_VOLUME_THRESHOLD_PERCENT:
                return None

        vwap = self._compute_vwap(minute_today)
        return Setup(
            token=token,
            symbol=inst["symbol"],
            name=inst["name"],
            prev_close=prev_day_close,
            today_open=today_open,
            gap_percent=gap_percent,
            opening_type=opening_type,
            first_candle_color=color,
            first_high=first_high,
            first_low=first_low,
            first_volume=first_volume,
            prev_hour_volume=prev_hour_volume,
            volume_change_percent=volume_change_percent,
            breakout_side=breakout_side,
            breakout_level=breakout_level,
            vwap=vwap,
        )

    def _compute_vwap(self, minute_df):
        if minute_df.empty:
            return 0.0
        vol = pd.to_numeric(minute_df["volume"], errors="coerce").fillna(0.0)
        price = (minute_df["high"] + minute_df["low"] + minute_df["close"]) / 3.0
        denom = vol.sum()
        if denom <= 0:
            return float(minute_df.iloc[-1]["close"])
        return float((price * vol).sum() / denom)

    def connect_ws(self):
        if self.ws is not None:
            return

        self.ws = SmartWebSocketV2(
            auth_token=self.session_data["jwtToken"],
            api_key=ANGEL_API_KEY,
            client_code=ANGEL_CLIENT_ID,
            feed_token=self.obj.getfeedToken(),
        )
        self.ws.on_open = self.on_open
        self.ws.on_data = self.on_data
        self.ws.on_error = self.on_error
        self.ws.on_close = self.on_close
        threading.Thread(target=self.ws.connect, daemon=True).start()

    def on_open(self, ws):
        self.ws_connected = True
        print("WebSocket connected. Subscribing to NFO futures...")
        tokens = list(self.setups.keys())
        chunk_size = 50
        for idx in range(0, len(tokens), chunk_size):
            chunk = tokens[idx: idx + chunk_size]
            self.ws.subscribe(
                correlation_id=f"nfo_1h_{idx}",
                mode=3,
                token_list=[{"exchangeType": EXCHANGE_TYPE_NFO, "tokens": chunk}],
            )
            time.sleep(0.2)

    def on_data(self, ws, message):
        try:
            data = json.loads(message) if isinstance(message, str) else message
            payload = data.get("data", data)
            if not isinstance(payload, list):
                payload = [payload]

            for tick in payload:
                token = str(tick.get("token", tick.get("tk", "")))
                if token not in self.setups:
                    continue

                raw_ltp = tick.get("last_traded_price", tick.get("ltp", 0))
                if not isinstance(raw_ltp, (int, float)):
                    continue

                price = float(raw_ltp)
                if price > 100000:
                    price = price / 100
                if price <= 0:
                    continue

                self.latest_prices[token] = price
                self.evaluate_tick(token, price)
        except Exception as exc:
            print(f"WebSocket message error: {exc}")

    def on_error(self, ws, error):
        print(f"WebSocket error: {error}")

    def on_close(self, ws):
        self.ws_connected = False
        self.ws = None
        print("WebSocket closed.")

    def fetch_latest_vwap(self, setup):
        now = datetime.now(IST)
        minute_df = fetch_candle_df(
            self.obj,
            "NFO",
            setup.token,
            INTERVAL_1M,
            now.replace(hour=9, minute=15, second=0, microsecond=0),
            now,
        )
        minute_today = minute_df[minute_df["timestamp"].dt.date == now.date()].copy()
        if minute_today.empty:
            return setup.vwap
        setup.vwap = self._compute_vwap(minute_today)
        setup.last_vwap_check = time.time()
        return setup.vwap

    def evaluate_tick(self, token, price):
        setup = self.setups.get(token)
        if not setup:
            return

        if setup.breakout_side == "BUY":
            if price <= setup.breakout_level:
                return
            if setup.alert_state == "NONE":
                vwap = self.fetch_latest_vwap(setup)
                confirmed = price > vwap
                self.send_alert(setup, price, confirmed)
                setup.alert_state = "BUY CONFIRMED" if confirmed else "BUY ALERT"
                return
            if setup.alert_state == "BUY ALERT" and time.time() - setup.last_vwap_check >= VWAP_REFRESH_SECONDS:
                vwap = self.fetch_latest_vwap(setup)
                if price > vwap:
                    self.send_alert(setup, price, True)
                    setup.alert_state = "BUY CONFIRMED"
            return

        if price >= setup.breakout_level:
            return
        if setup.alert_state == "NONE":
            vwap = self.fetch_latest_vwap(setup)
            confirmed = price < vwap
            self.send_alert(setup, price, confirmed)
            setup.alert_state = "SELL CONFIRMED" if confirmed else "SELL ALERT"
            return
        if setup.alert_state == "SELL ALERT" and time.time() - setup.last_vwap_check >= VWAP_REFRESH_SECONDS:
            vwap = self.fetch_latest_vwap(setup)
            if price < vwap:
                self.send_alert(setup, price, True)
                setup.alert_state = "SELL CONFIRMED"

    def send_alert(self, setup, price, confirmed):
        heading = "BUY ALERT" if setup.breakout_side == "BUY" else "SELL ALERT"
        signal = f"{setup.breakout_side} CONFIRMED" if confirmed else heading
        logic = self.build_logic_line(setup, confirmed)
        lines = [
            heading,
            "",
            f"Symbol: {setup.symbol}",
            "Timeframe: 1h",
            f"Prev Close: {setup.prev_close:.2f}",
            f"Today Open: {setup.today_open:.2f}",
            f"Gap: {setup.opening_type} ({setup.gap_percent:.2f}%)",
            f"First Candle: {setup.first_candle_color}",
            f"First Candle High: {setup.first_high:.2f}",
            f"First Candle Low: {setup.first_low:.2f}",
            f"First Candle Volume: {setup.first_volume:,.0f}",
            f"Previous 1h Volume: {setup.prev_hour_volume:,.0f}",
            f"Volume Change: {setup.volume_change_percent:.2f}%" if setup.volume_change_percent is not None else "Volume Change: N/A",
            f"Current Price: {price:.2f}",
            f"VWAP: {setup.vwap:.2f}",
            f"Signal: {signal}",
            f"Logic: {logic}",
            f"Time: {datetime.now(IST).strftime('%H:%M:%S')}",
        ]
        message = "\n".join(lines)
        print(message)
        send_future_scanner_alert(message)

    def build_logic_line(self, setup, confirmed):
        direction = "Break first high" if setup.breakout_side == "BUY" else "Break first low"
        vwap_side = "Above VWAP" if setup.breakout_side == "BUY" else "Below VWAP"
        if setup.opening_type == "FLAT OPENING":
            base = (
                f"Flat Opening + First 1h volume >= {FLAT_VOLUME_THRESHOLD_PERCENT:.0f}% higher than previous 1h candle "
                f"+ {setup.first_candle_color.title()} first 1h candle + {direction}"
            )
        else:
            gap_side = f"Gap Up >= {GAP_THRESHOLD_PERCENT:.2f}%" if setup.opening_type == "GAP UP" else f"Gap Down <= -{GAP_THRESHOLD_PERCENT:.2f}%"
            base = f"{gap_side} + {setup.first_candle_color.title()} first 1h candle + {direction}"
        if confirmed:
            return f"{base} + {vwap_side}"
        return base

    def run(self):
        if not self.initialize():
            return

        if not self.startup_alert_sent:
            send_future_scanner_alert("Angel One NFO 1H Futures Scanner Started")
            self.startup_alert_sent = True

        self.connect_ws()
        while True:
            try:
                if not self.ws_connected and self.ws is None:
                    self.prepare_setups()
                    self.connect_ws()
                time.sleep(5)
            except Exception as exc:
                print(f"Scanner loop error: {exc}")
                time.sleep(10)


if __name__ == "__main__":
    Future1HScanner().run()
