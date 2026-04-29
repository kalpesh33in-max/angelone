import threading
import time
from dataclasses import dataclass
from datetime import datetime
from zoneinfo import ZoneInfo

import pandas as pd

from data_manager import load_option_contracts, resolve_underlying_instrument
from telegram_utils import send_future_scanner_alert


IST = ZoneInfo("Asia/Kolkata")
REFRESH_SECONDS = 60
ALERT_COOLDOWN_SECONDS = 15
ATM_STRIKE_RANGE = 10
HIGH_LOTS_MULTIPLIER = 2

# ✅ UPDATED: ALL INDEX + SAME LOGIC
UNDERLYING_CONFIG = {
    "NIFTY": {"threshold_lots": 200, "option_exchange_type": 2},
    "BANKNIFTY": {"threshold_lots": 200, "option_exchange_type": 2},
    "FINNIFTY": {"threshold_lots": 200, "option_exchange_type": 2},
    "MIDCPNIFTY": {"threshold_lots": 200, "option_exchange_type": 2},
    "SENSEX": {"threshold_lots": 200, "option_exchange_type": 4},
}


@dataclass
class ContractState:
    token: str
    symbol: str
    underlying_name: str
    expiry_bucket: str
    strike: float
    option_type: str
    lot_size: int
    strike_step: float
    threshold_lots: int
    baseline_volume: float | None = None
    baseline_oi: float | None = None
    baseline_price: float | None = None
    last_price: float | None = None
    last_oi: float | None = None
    last_volume: float | None = None
    last_alert_bucket: int = 0
    last_alert_at: float = 0.0


class OptionBurstModule:
    def __init__(self, engine):
        self.engine = engine
        self.smart = engine.smart
        self.contract_states = {}
        self.underlying_tokens = {}
        self.underlying_meta = {}
        self.underlying_prices = {}
        self.refresh_thread = None
        self.refresh_in_progress = False
        self.last_refresh_at = 0.0
        self.startup_alert_sent = False
        self._refresh_lock = threading.Lock()
        self.pending_confirmations = {}

    def start(self):
        self.engine.register(self)
        print("Option Flow Scanner Started (Monthly Mode)", flush=True)

        if not self.startup_alert_sent:
            send_future_scanner_alert("Option Flow Scanner Started")
            self.startup_alert_sent = True

        self.start_background_setup()

    def start_background_setup(self):
        if self.refresh_in_progress:
            return
        self.refresh_thread = threading.Thread(target=self._refresh_worker, daemon=True)
        self.refresh_thread.start()

    def _refresh_worker(self):
        with self._refresh_lock:
            self.refresh_in_progress = True
            try:
                self.refresh_watchlist()
                while True:
                    time.sleep(REFRESH_SECONDS)
                    self.refresh_watchlist()
            finally:
                self.refresh_in_progress = False

    def refresh_watchlist(self):
        next_states = {}
        for underlying_name, config in UNDERLYING_CONFIG.items():
            try:
                self._refresh_underlying(underlying_name, config, next_states)
            except Exception as e:
                print(f"ERROR {underlying_name}: {e}", flush=True)

        self.contract_states = next_states
        print(f"[READY] Loaded {len(self.contract_states)} contracts", flush=True)

    def _refresh_underlying(self, underlying_name, config, next_states):
        underlying = resolve_underlying_instrument(underlying_name)
        if not underlying:
            return

        self.underlying_meta[underlying_name] = underlying
        self.underlying_tokens[underlying["token"]] = underlying_name
        self.engine.subscribe_tokens(underlying["exchange_type"], [underlying["token"]])

        underlying_price = self.engine.get_latest_price(underlying["token"])
        if not underlying_price:
            return

        self.underlying_prices[underlying_name] = float(underlying_price)

        df = load_option_contracts(underlying_name, expiry_count=6)
        if df.empty:
            return

        # 🔥 FIX: TRUE MONTHLY EXPIRY (NO WEEKLY)
        df["expiry_dt"] = pd.to_datetime(df["expiry_dt"])

        monthly_expiries = (
            df.groupby(df["expiry_dt"].dt.to_period("M"))["expiry_dt"]
            .max()
            .values
        )

        monthly_df = df[df["expiry_dt"].isin(monthly_expiries)]

        for expiry_bucket, expiry_df in monthly_df.groupby("expiry_bucket"):

            selected_df = self._select_atm_range_contracts(
                expiry_df, underlying_price
            )

            if selected_df.empty:
                continue

            self.engine.subscribe_tokens(
                config["option_exchange_type"],
                selected_df["token"].tolist()
            )

            strike_step = self._infer_strike_step(expiry_df)

            for _, row in selected_df.iterrows():
                token = str(row["token"])

                state = ContractState(
                    token=token,
                    symbol=row["symbol"],
                    underlying_name=underlying_name,
                    expiry_bucket=str(expiry_bucket),
                    strike=float(row["strike_value"]),
                    option_type=row["option_type"],
                    lot_size=max(int(row["lotsize"]), 1),
                    strike_step=strike_step,
                    threshold_lots=config["threshold_lots"],
                )

                next_states[token] = state

    def _select_atm_range_contracts(self, expiry_df, underlying_price):
        strikes = sorted(expiry_df["strike_value"].unique())
        nearest = min(strikes, key=lambda x: abs(x - underlying_price))

        selected = [
            s for s in strikes
            if abs(s - nearest) <= ATM_STRIKE_RANGE
        ]

        return expiry_df[expiry_df["strike_value"].isin(selected)]

    def _infer_strike_step(self, df):
        strikes = sorted(df["strike_value"].unique())
        if len(strikes) < 2:
            return 1
        return min([strikes[i+1] - strikes[i] for i in range(len(strikes)-1)])

    def _extract_open_interest(self, raw):
        for k in ["open_interest", "oi", "oi_qty", "openInterest"]:
            if k in raw and raw[k]:
                return float(raw[k])
        return None

    def _extract_volume(self, raw):
        for k in ["volume", "volume_trade_for_the_day"]:
            if k in raw and raw[k]:
                return float(raw[k])
        return None

    def _classify_flow(self, dp, doi):
        if dp >= 0 and doi >= 0:
            return "ACTION", ""
        if dp < 0 and doi >= 0:
            return "WRITER", "✍️"
        if dp >= 0 and doi < 0:
            return "SHORT COVERING", "↗️"
        return "UNWINDING", "⤵️"

    def on_tick(self, token, tick):
        token = str(token)

        if token in self.underlying_tokens:
            self.underlying_prices[self.underlying_tokens[token]] = float(tick["ltp"])
            return

        state = self.contract_states.get(token)
        if not state:
            return

        raw = tick.get("raw", {})
        price = float(tick["ltp"])
        oi = self._extract_open_interest(raw)
        vol = self._extract_volume(raw)

        if not oi or not vol:
            return

        if state.last_oi is None:
            state.last_oi = oi
            state.last_price = price
            state.last_volume = vol
            return

        doi = oi - state.last_oi
        lots = abs(doi) / state.lot_size

        if lots < state.threshold_lots:
            state.last_oi = oi
            return

        dp = price - state.last_price
        label, suffix = self._classify_flow(dp, doi)

        msg = (
            f"{state.symbol}\n\n"
            f"⚡ {label} {suffix}\n"
            f"Lots: {int(lots)}\n"
            f"Price: {price:.2f}"
        )

        send_future_scanner_alert(msg)

        state.last_oi = oi
        state.last_price = price
