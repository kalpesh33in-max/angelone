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

# ✅ Fixed filter
MIN_LOTS = 300

# ✅ Only required indices
UNDERLYING_CONFIG = {
    "NIFTY": {"threshold_lots": 300, "option_exchange_type": 2},
    "BANKNIFTY": {"threshold_lots": 300, "option_exchange_type": 2},
    "FINNIFTY": {"threshold_lots": 300, "option_exchange_type": 2},
    "MIDCPNIFTY": {"threshold_lots": 300, "option_exchange_type": 2},
    "SENSEX": {"threshold_lots": 300, "option_exchange_type": 4},
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
        self._refresh_lock = threading.Lock()
        self.pending_confirmations = {}

    def start(self):
        self.engine.register(self)
        print("Monthly Burst Scanner Started (300+ lots)")
        send_future_scanner_alert("Monthly Burst Scanner Started (300+ lots)")
        self.start_background_setup()

    def start_background_setup(self):
        self.refresh_thread = threading.Thread(target=self._refresh_worker, daemon=True)
        self.refresh_thread.start()

    def _refresh_worker(self):
        while True:
            try:
                self.refresh_watchlist()
            except Exception as e:
                print(e)
            time.sleep(REFRESH_SECONDS)

    def refresh_watchlist(self):
        next_states = {}

        for underlying_name, config in UNDERLYING_CONFIG.items():
            underlying = resolve_underlying_instrument(underlying_name)
            if not underlying:
                continue

            self.underlying_meta[underlying_name] = underlying
            self.underlying_tokens[underlying["token"]] = underlying_name
            self.engine.subscribe_tokens(underlying["exchange_type"], [underlying["token"]])

            underlying_price = self.engine.get_latest_price(underlying["token"])
            if not underlying_price:
                continue

            self.underlying_prices[underlying_name] = underlying_price

            contracts_df = load_option_contracts(underlying_name, expiry_count=3)
            if contracts_df.empty:
                continue

            for expiry_bucket, expiry_df in contracts_df.groupby("expiry_bucket"):

                # ✅ ONLY MONTHLY
                if expiry_bucket != "MONTH":
                    continue

                exchange_type = config["option_exchange_type"]

                self.engine.subscribe_tokens(
                    exchange_type,
                    expiry_df["token"].astype(str).tolist()
                )

                for _, row in expiry_df.iterrows():
                    token = str(row["token"])

                    state = ContractState(
                        token=token,
                        symbol=row["symbol"],
                        underlying_name=underlying_name,
                        expiry_bucket=expiry_bucket,
                        strike=float(row["strike_value"]),
                        option_type=row["option_type"],
                        lot_size=max(int(row["lotsize"]), 1),
                        strike_step=50,
                        threshold_lots=MIN_LOTS,
                    )

                    next_states[token] = state

        self.contract_states = next_states
        print(f"Loaded {len(self.contract_states)} monthly contracts")

    def on_tick(self, token, tick):
        token = str(token)

        # 🚀 FUTURE BURST
        if token in self.underlying_tokens:
            underlying_name = self.underlying_tokens[token]

            price = float(tick["ltp"])
            prev = self.underlying_prices.get(underlying_name, price)

            change = price - prev
            self.underlying_prices[underlying_name] = price

            est_lots = abs(change) * 60

            if est_lots >= MIN_LOTS:
                direction = "ACTION" if change > 0 else "WRITER"

                send_future_scanner_alert(
                    f"{underlying_name} FUTURE\n\n"
                    f"⚡ {direction}\n"
                    f"Lots: {int(est_lots)}\n"
                    f"Price: {price:.2f}"
                )
            return

        state = self.contract_states.get(token)
        if not state:
            return

        price = float(tick["ltp"])
        raw = tick.get("raw", {})

        oi = raw.get("open_interest") or raw.get("oi")
        volume = raw.get("volume_trade_for_the_day") or raw.get("volume")

        if not oi or not volume:
            return

        oi = float(oi)

        if state.baseline_oi is None:
            state.baseline_oi = oi
            state.last_oi = oi
            state.last_price = price
            return

        delta_oi = oi - state.last_oi
        lots = abs(delta_oi) / state.lot_size

        if lots < MIN_LOTS:
            state.last_oi = oi
            return

        price_change = price - state.last_price

        if price_change >= 0 and delta_oi >= 0:
            label = "ACTION"
            suffix = ""
        elif price_change < 0 and delta_oi >= 0:
            label = "WRITER"
            suffix = " ✍️"
        elif price_change >= 0 and delta_oi < 0:
            label = "SHORT COVERING"
            suffix = " ↗️"
        else:
            label = "UNWINDING"
            suffix = " ⤵️"

        send_future_scanner_alert(
            f"{state.symbol}\n\n"
            f"⚡ {label}{suffix}\n"
            f"Lots: {int(lots)}\n"
            f"Price: {price:.2f}"
        )

        state.last_oi = oi
        state.last_price = price
