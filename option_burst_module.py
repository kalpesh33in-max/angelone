import math
import threading
import time
from dataclasses import dataclass
from datetime import datetime
from collections import deque
from zoneinfo import ZoneInfo

import pandas as pd

from data_manager import load_option_contracts, resolve_underlying_instrument
from telegram_utils import send_future_scanner_alert


IST = ZoneInfo("Asia/Kolkata")
REFRESH_SECONDS = 60
ALERT_COOLDOWN_SECONDS = 15
ATM_STRIKE_RANGE = 10
HIGH_LOTS_MULTIPLIER = 2
ROLLING_WINDOW_SECONDS = 120

UNDERLYING_CONFIG = {
    "NIFTY": {
        "threshold_lots": 250,
        "option_exchange_type": 2,
    },
    "SENSEX": {
        "threshold_lots": 250,
        "option_exchange_type": 4,
    },
    "CRUDEOIL": {
        "threshold_lots": 5,
        "option_exchange_type": 5,
    },
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
    history: deque | None = None
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
        self.debug_counts = {}
        self.pending_confirmations = {}

    def start(self):
        self.engine.register(self)
        print("Option flow module registered on shared market-data engine.")
        if not self.startup_alert_sent:
            send_future_scanner_alert(
                "Option Flow Scanner Started\n"
                "Tracking NIFTY/SENSEX above 500 lots and CRUDEOIL above 250 lots\n"
                "Expiries: NEAR, NEXT, MONTH\n"
                "Strikes: ATM plus/minus 10 only\n"
                "Alerts: ACTION / WRITER / SHORT COVERING / UNWINDING"
            )
            self.startup_alert_sent = True
        self.start_background_setup()

    def start_background_setup(self):
        if self.refresh_in_progress:
            print("Option flow setup is already in progress.")
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
        now = time.time()
        for underlying_name, config in UNDERLYING_CONFIG.items():
            try:
                self._refresh_underlying(underlying_name, config, next_states)
            except Exception as exc:
                print(f"Option flow setup failed for {underlying_name}: {exc}")
        self.contract_states = next_states
        self.last_refresh_at = now
        print(f"Option flow scanner active with {len(self.contract_states)} option contracts.")
        summary = {}
        for state in self.contract_states.values():
            summary[state.underlying_name] = summary.get(state.underlying_name, 0) + 1
        print(f"Option flow watchlist summary: {summary}")

    def _refresh_underlying(self, underlying_name, config, next_states):
        underlying = resolve_underlying_instrument(underlying_name)
        if not underlying:
            print(f"Underlying instrument not found for {underlying_name}.")
            return

        self.underlying_meta[underlying_name] = underlying
        self.underlying_tokens[underlying["token"]] = underlying_name
        self.engine.subscribe_tokens(underlying["exchange_type"], [underlying["token"]])

        underlying_price = self._get_underlying_price(underlying)
        if underlying_price is None or underlying_price <= 0:
            print(f"Unable to fetch underlying price for {underlying_name}.")
            return
        self.underlying_prices[underlying_name] = underlying_price

        contracts_df = load_option_contracts(underlying_name, expiry_count=3)
        if contracts_df.empty:
            print(f"No option contracts found for {underlying_name}.")
            return

        for expiry_bucket, expiry_df in contracts_df.groupby("expiry_bucket"):
            selected_df = self._select_atm_range_contracts(expiry_df, underlying_price)
            if selected_df.empty:
                continue

            exchange_type = config["option_exchange_type"]
            self.engine.subscribe_tokens(exchange_type, selected_df["token"].tolist())

            strike_step = self._infer_strike_step(expiry_df)
            for _, row in selected_df.iterrows():
                token = str(row["token"])
                state = self.contract_states.get(token)
                if state is None:
                    state = ContractState(
                        token=token,
                        symbol=str(row["symbol"]),
                        underlying_name=underlying_name,
                        expiry_bucket=str(expiry_bucket),
                        strike=float(row["strike_value"]),
                        option_type=str(row["option_type"]),
                        lot_size=max(int(row["lotsize"]), 1),
                        strike_step=strike_step,
                        threshold_lots=int(config["threshold_lots"]),
                        history=deque(),
                    )
                else:
                    state.expiry_bucket = str(expiry_bucket)
                    state.strike = float(row["strike_value"])
                    state.option_type = str(row["option_type"])
                    state.lot_size = max(int(row["lotsize"]), 1)
                    state.strike_step = strike_step
                    state.threshold_lots = int(config["threshold_lots"])
                    if state.history is None:
                        state.history = deque()
                next_states[token] = state

        selected_symbols = [
            state.symbol
            for state in next_states.values()
            if state.underlying_name == underlying_name
        ]
        print(
            f"{underlying_name} underlying={underlying_price:.2f} "
            f"selected_contracts={len(selected_symbols)}"
        )
        if selected_symbols:
            print(f"{underlying_name} sample contracts: {selected_symbols[:8]}")

    def _get_underlying_price(self, underlying):
        token = underlying["token"]
        cached = self.engine.get_latest_price(token)
        if cached is not None:
            return float(cached)
        return float(self.engine.get_ltp_snapshot(underlying["exchange"], underlying["symbol"], token))

    def _select_atm_range_contracts(self, expiry_df, underlying_price):
        strikes = sorted(float(value) for value in expiry_df["strike_value"].dropna().unique())
        if not strikes:
            return pd.DataFrame()

        nearest_idx = min(range(len(strikes)), key=lambda idx: abs(strikes[idx] - underlying_price))
        selected_strikes = {
            strikes[idx]
            for idx in range(max(0, nearest_idx - ATM_STRIKE_RANGE), min(len(strikes), nearest_idx + ATM_STRIKE_RANGE + 1))
        }
        return expiry_df[expiry_df["strike_value"].isin(selected_strikes)].copy()

    def _infer_strike_step(self, expiry_df):
        strikes = sorted(float(value) for value in expiry_df["strike_value"].dropna().unique())
        if len(strikes) < 2:
            return 1.0
        diffs = [round(strikes[idx + 1] - strikes[idx], 6) for idx in range(len(strikes) - 1)]
        positive = [diff for diff in diffs if diff > 0]
        return min(positive) if positive else 1.0

    def _extract_total_volume(self, raw_tick):
        for key in (
            "volume_trade_for_the_day",
            "volume_traded_today",
            "trade_volume",
            "volume",
            "vol_traded",
            "total_volume",
            "vtt",
            "vt",
            "v",
        ):
            value = raw_tick.get(key)
            if value in (None, "", 0, "0"):
                continue
            try:
                return float(value)
            except (TypeError, ValueError):
                continue
        return None

    def _extract_open_interest(self, raw_tick):
        for key in (
            "open_interest",
            "opn_interest",
            "oi",
            "oi_qty",
            "openinterest",
            "openInterest",
        ):
            value = raw_tick.get(key)
            if value in (None, "", 0, "0"):
                continue
            try:
                return float(value)
            except (TypeError, ValueError):
                continue
        return None

    def _normalize_option_price(self, price, strike):
        if price is None:
            return None
        price = float(price)
        if price <= 0:
            return None
        if price >= max(1000.0, strike / 10.0):
            return price / 100.0
        return price

    def _is_in_scope(self, state, underlying_price):
        diff = underlying_price - state.strike if state.option_type == "CE" else state.strike - underlying_price
        max_depth = (ATM_STRIKE_RANGE * state.strike_step) + 1e-6
        one_strike_exception = state.strike_step + 1e-6
        return (-one_strike_exception) <= diff <= max_depth

    def _classify_flow(self, delta_price, delta_oi):
        if delta_price >= 0 and delta_oi >= 0:
            return "ACTION", ""
        if delta_price < 0 and delta_oi >= 0:
            return "WRITER", "✍️"
        if delta_price >= 0 and delta_oi < 0:
            return "SHORT COVERING", "↗️"
        return "UNWINDING", "⤵️"

    def _strike_label(self, state, underlying_price, lots):
        diff = underlying_price - state.strike if state.option_type == "CE" else state.strike - underlying_price
        prefix = "ITM"
        suffix = "-HIGHLOTS" if lots >= state.threshold_lots * HIGH_LOTS_MULTIPLIER else ""
        return f"{prefix}-{abs(diff):.1f}-diff{suffix}"

    def _turnover_cr(self, flow_label, qty, price, oi_lots):
        if flow_label in {"WRITER", "SHORT COVERING"}:
            return max(oi_lots, 0.0) * 100000.0 / 10000000.0
        return (qty * price) / 10000000.0

    def on_tick(self, token, tick):
        token = str(token)
        now_dt = datetime.now(IST)
        if token in self.underlying_tokens:
            underlying_name = self.underlying_tokens[token]
            self.underlying_prices[underlying_name] = float(tick["ltp"])
            if underlying_name == "CRUDEOIL":
                count = self.debug_counts.get(f"underlying:{token}", 0)
                if count < 5:
                    print(
                        f"DEBUG CRUDEOIL underlying tick token={token} "
                        f"ltp={tick['ltp']} raw_keys={sorted((tick.get('raw') or {}).keys())}"
                    )
                    self.debug_counts[f"underlying:{token}"] = count + 1
            return

        state = self.contract_states.get(token)
        if not state:
            return

        underlying_price = self.underlying_prices.get(state.underlying_name)
        if underlying_price is None or not self._is_in_scope(state, underlying_price):
            return

        price = self._normalize_option_price(tick["ltp"], state.strike)
        if price is None:
            return

        raw_tick = tick.get("raw", {})
        total_volume = self._extract_total_volume(raw_tick)
        total_oi = self._extract_open_interest(raw_tick)
        debug_key = f"contract:{token}"
        debug_count = self.debug_counts.get(debug_key, 0)
        if state.underlying_name == "CRUDEOIL" and debug_count < 8:
            print(
                f"DEBUG CRUDEOIL option tick symbol={state.symbol} token={token} "
                f"price={price} volume={total_volume} oi={total_oi} "
                f"raw_keys={sorted(raw_tick.keys())}"
            )
            self.debug_counts[debug_key] = debug_count + 1
        if total_volume is None or total_oi is None:
            return

        if state.baseline_volume is None or state.baseline_oi is None:
            state.baseline_volume = total_volume
            state.baseline_oi = total_oi
            state.baseline_price = price
            state.last_price = price
            state.last_oi = total_oi
            state.last_volume = total_volume
            if state.history is None:
                state.history = deque()
            state.history.append(
                {
                    "time": now_dt,
                    "oi": float(total_oi),
                    "price": float(price),
                    "volume": float(total_volume),
                }
            )
            return

        prev_oi = state.last_oi if state.last_oi is not None else state.baseline_oi
        prev_price = state.last_price if state.last_price is not None else state.baseline_price
        prev_volume = state.last_volume if state.last_volume is not None else state.baseline_volume

        if state.history is None:
            state.history = deque()
        state.history.append(
            {
                "time": now_dt,
                "oi": float(total_oi),
                "price": float(price),
                "volume": float(total_volume),
            }
        )
        while state.history and (now_dt - state.history[0]["time"]).total_seconds() > ROLLING_WINDOW_SECONDS:
            state.history.popleft()

        rolling_base = state.history[0] if state.history else {
            "oi": float(prev_oi),
            "price": float(prev_price),
            "volume": float(prev_volume),
        }

        tick_oi_change = total_oi - float(prev_oi)
        rolling_oi_change = total_oi - float(rolling_base["oi"])
        tick_lots = int(abs(rolling_oi_change) / state.lot_size)
        pending = self.pending_confirmations.get(token)

        if pending is None and tick_lots >= state.threshold_lots:
            self.pending_confirmations[token] = {
                "time": now_dt,
                "baseline_oi": float(rolling_base["oi"]),
                "baseline_price": float(rolling_base["price"]),
                "baseline_volume": float(rolling_base["volume"]),
            }
            if state.underlying_name == "CRUDEOIL":
                print(
                    f"DEBUG CRUDEOIL pending created symbol={state.symbol} "
                    f"rolling_lots={tick_lots} threshold={state.threshold_lots}"
                )
        elif pending is not None:
            confirmed_oi_change = total_oi - float(pending["baseline_oi"])
            confirmed_lots = int(abs(confirmed_oi_change) / state.lot_size)

            if confirmed_lots < state.threshold_lots:
                self.pending_confirmations.pop(token, None)
                if state.underlying_name == "CRUDEOIL":
                    print(
                        f"DEBUG CRUDEOIL pending dropped symbol={state.symbol} "
                        f"confirmed_lots={confirmed_lots} threshold={state.threshold_lots}"
                    )
            elif (now_dt - pending["time"]).total_seconds() >= ALERT_COOLDOWN_SECONDS:
                price_change = price - float(pending["baseline_price"])
                qty = confirmed_lots * state.lot_size
                flow_label, flow_suffix = self._classify_flow(price_change, confirmed_oi_change)
                turnover = self._turnover_cr(flow_label, qty, price, confirmed_lots)
                strike_label = self._strike_label(state, underlying_price, confirmed_lots)
                underlying_label = "Fut Price" if state.underlying_name == "CRUDEOIL" else "Spot Price"

                lines = [
                    f"{state.symbol} ({strike_label})",
                    f"⚡ {flow_label}" + (f" {flow_suffix}" if flow_suffix else ""),
                    f"Turnover: ₹{turnover:.2f} Cr",
                    f"Lots: {confirmed_lots} (Qty: {qty})",
                    f"Price: {price:.2f}",
                    f"{underlying_label}: {underlying_price:.1f}",
                ]
                if state.underlying_name == "CRUDEOIL":
                    print(
                        f"DEBUG CRUDEOIL alert symbol={state.symbol} flow={flow_label} "
                        f"confirmed_lots={confirmed_lots} qty={qty} turnover={turnover:.2f}"
                    )
                send_future_scanner_alert("\n".join(lines))

                state.last_alert_bucket = confirmed_lots // max(state.threshold_lots, 1)
                state.last_alert_at = time.time()
                self.pending_confirmations.pop(token, None)

        delta_qty_from_last = max(total_volume - float(prev_volume), 0.0)
        below_threshold_lots = int(delta_qty_from_last / state.lot_size)
        if state.underlying_name == "CRUDEOIL" and debug_count < 12 and tick_lots < state.threshold_lots:
            print(
                f"DEBUG CRUDEOIL below threshold symbol={state.symbol} "
                f"rolling_lots={tick_lots} qty={int(delta_qty_from_last)} threshold={state.threshold_lots}"
            )
            self.debug_counts[debug_key] = debug_count + 1

        state.last_price = price
        state.last_oi = total_oi
        state.last_volume = total_volume
