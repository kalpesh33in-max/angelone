import time
import threading
from dataclasses import dataclass
from datetime import datetime, timedelta, time as dt_time
from zoneinfo import ZoneInfo

import pandas as pd

from data_manager import fetch_candle_df, load_nfo_futures
from env_config import (
    FLAT_VOLUME_THRESHOLD_PERCENT,
    GAP_THRESHOLD_PERCENT,
    LOGIC_START_TIME,
    VWAP_REFRESH_SECONDS,
)
from telegram_utils import send_future_scanner_alert


IST = ZoneInfo("Asia/Kolkata")
INTERVAL_1M = "ONE_MINUTE"
EXCHANGE_TYPE_NFO = 2
FIRST_HOUR_START = dt_time(9, 15)
FIRST_HOUR_END = dt_time(10, 14)
PREV_CLOSE_HOUR_START = dt_time(14, 30)
PREV_CLOSE_HOUR_END = dt_time(15, 29)


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


class Future1HModule:
    def __init__(self, engine):
        self.engine = engine
        self.smart = engine.smart
        self.instruments = {}
        self.setups = {}
        self.startup_alert_sent = False
        self.setup_thread = None
        self.setup_in_progress = False

    def start(self):
        self.instruments = load_nfo_futures()
        print(f"Futures 1H module loaded {len(self.instruments)} nearest NFO futures.")
        self.engine.register(self)
        print("Futures 1H module registered on shared market-data engine.")
        if not self.startup_alert_sent:
            send_future_scanner_alert("Angel One NFO 1H Futures Scanner Started")
            self.startup_alert_sent = True
        self.start_background_setup()

    def start_background_setup(self):
        if self.setup_in_progress:
            print("Futures 1H setup is already in progress.")
            return
        print("Starting futures 1H setup worker thread...")
        self.setup_thread = threading.Thread(target=self._setup_worker, daemon=True)
        self.setup_thread.start()

    def _setup_worker(self):
        self.setup_in_progress = True
        try:
            self.prepare_setups()
            self.engine.subscribe_tokens(EXCHANGE_TYPE_NFO, list(self.setups.keys()))
            print(f"Futures 1H module activated with {len(self.setups)} active setups.")
        finally:
            self.setup_in_progress = False

    def prepare_setups(self):
        self.setups = {}
        now = datetime.now(IST)
        logic_hour, logic_minute = [int(part) for part in LOGIC_START_TIME.split(":", 1)]
        logic_start = now.replace(hour=logic_hour, minute=logic_minute, second=0, microsecond=0)
        print(f"Preparing 1H futures setups at {now.strftime('%Y-%m-%d %H:%M:%S')} IST")
        if now < logic_start:
            print(f"Waiting until {LOGIC_START_TIME} IST for first 1H candle completion.")
            return

        from_day = now - timedelta(days=7)
        today = now.date()

        for token, inst in self.instruments.items():
            try:
                minute_df = self._fetch_minute_df_with_retry(token, from_day.replace(hour=9, minute=15, second=0, microsecond=0), now)
                setup = self._build_setup(token, inst, minute_df, today)
                if setup:
                    self.setups[token] = setup
            except Exception as exc:
                print(f"Setup build failed for {inst['symbol']}: {exc}")
            time.sleep(0.2)

        print(f"Prepared {len(self.setups)} active 1H setups.")
        if not self.setups:
            print("No active 1H setups were built. This can happen on market-off days or when no symbols match the current rules.")

    def _fetch_minute_df_with_retry(self, token, from_dt, to_dt, max_attempts=3):
        last_exc = None
        for attempt in range(1, max_attempts + 1):
            try:
                return fetch_candle_df(
                    self.smart,
                    "NFO",
                    token,
                    INTERVAL_1M,
                    from_dt,
                    to_dt,
                )
            except Exception as exc:
                last_exc = exc
                if "exceeding access rate" in str(exc).lower() and attempt < max_attempts:
                    time.sleep(2 * attempt)
                    continue
                raise
        raise last_exc

    def _build_setup(self, token, inst, minute_df, today):
        if minute_df.empty:
            return None

        minute_today = minute_df[minute_df["timestamp"].dt.date == today].copy()
        if minute_today.empty:
            return None

        prev_dates = sorted(date for date in minute_df["timestamp"].dt.date.unique() if date < today)
        if not prev_dates:
            return None
        prev_day = prev_dates[-1]
        minute_prev_day = minute_df[minute_df["timestamp"].dt.date == prev_day].copy()
        if minute_prev_day.empty:
            return None

        prev_day_close = float(minute_prev_day.iloc[-1]["close"])
        today_open = float(minute_today.iloc[0]["open"])
        gap_percent = ((today_open - prev_day_close) / prev_day_close) * 100 if prev_day_close else 0.0

        if gap_percent >= GAP_THRESHOLD_PERCENT:
            opening_type = "GAP UP"
        elif gap_percent <= -GAP_THRESHOLD_PERCENT:
            opening_type = "GAP DOWN"
        else:
            opening_type = "FLAT OPENING"

        first_hour_df = minute_today[
            (minute_today["timestamp"].dt.time >= FIRST_HOUR_START) &
            (minute_today["timestamp"].dt.time <= FIRST_HOUR_END)
        ].copy()
        if first_hour_df.empty:
            return None

        prev_close_hour_df = minute_prev_day[
            (minute_prev_day["timestamp"].dt.time >= PREV_CLOSE_HOUR_START) &
            (minute_prev_day["timestamp"].dt.time <= PREV_CLOSE_HOUR_END)
        ].copy()
        if prev_close_hour_df.empty:
            prev_close_hour_df = minute_prev_day.tail(60).copy()
        if prev_close_hour_df.empty:
            return None

        first_open = float(first_hour_df.iloc[0]["open"])
        first_close = float(first_hour_df.iloc[-1]["close"])
        first_high = float(first_hour_df["high"].max())
        first_low = float(first_hour_df["low"].min())
        first_volume = float(pd.to_numeric(first_hour_df["volume"], errors="coerce").fillna(0.0).sum())
        prev_hour_volume = float(pd.to_numeric(prev_close_hour_df["volume"], errors="coerce").fillna(0.0).sum())

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

    def fetch_latest_vwap(self, setup):
        now = datetime.now(IST)
        minute_df = fetch_candle_df(
            self.smart,
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

    def on_tick(self, token, tick):
        setup = self.setups.get(token)
        if not setup:
            return
        price = tick["ltp"]
        self.evaluate_tick(setup, price)

    def evaluate_tick(self, setup, price):
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
        send_future_scanner_alert("\n".join(lines))

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
