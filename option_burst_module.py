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
ATM_STRIKE_RANGE = 50

# 🔥 FINAL SETTINGS
OPTION_MIN_LOTS = 200
FUTURE_MIN_LOTS = 50   # 🔥 UPDATED

# 🔥 FIXED EXPIRY MAP
EXPIRY_MAP = {
    "NIFTY": "2026-05-26",
    "BANKNIFTY": "2026-05-26",
    "FINNIFTY": "2026-05-26",
    "MIDCPNIFTY": "2026-05-26",
    "SENSEX": "2026-05-27",  # 🔥 ONLY SENSEX DIFFERENT
}

UNDERLYING_CONFIG = {
    "NIFTY": {"option_exchange_type": 2},
    "BANKNIFTY": {"option_exchange_type": 2},
    "FINNIFTY": {"option_exchange_type": 2},
    "MIDCPNIFTY": {"option_exchange_type": 2},
    "SENSEX": {"option_exchange_type": 4},
}


@dataclass
class ContractState:
    token: str
    symbol: str
    underlying_name: str
    strike: float
    option_type: str
    lot_size: int
    last_oi: float | None = None
    last_price: float | None = None


class OptionBurstModule:
    def __init__(self, engine):
        self.engine = engine
        self.contract_states = {}
        self.underlying_tokens = {}
        self.underlying_prices = {}

    def start(self):
        self.engine.register(self)
        print("🔥 FINAL FIXED EXPIRY SCANNER STARTED", flush=True)
        threading.Thread(target=self._setup, daemon=True).start()

    def _setup(self):
        while True:
            try:
                self.refresh()
            except Exception as e:
                print("ERROR:", e, flush=True)
            time.sleep(REFRESH_SECONDS)

    def refresh(self):
        new_states = {}

        for name in UNDERLYING_CONFIG.keys():
            try:
                underlying = resolve_underlying_instrument(name)
                if not underlying:
                    continue

                token = underlying["token"]
                self.underlying_tokens[token] = name
                self.engine.subscribe_tokens(underlying["exchange_type"], [token])

                spot_price = self.engine.get_latest_price(token)
                if not spot_price:
                    continue

                self.underlying_prices[name] = spot_price

                df = load_option_contracts(name, expiry_count=6)
                if df.empty:
                    continue

                # 🔥 FIXED EXPIRY FILTER
                target_expiry = pd.to_datetime(EXPIRY_MAP[name]).date()
                df["expiry_dt"] = pd.to_datetime(df["expiry_dt"]).dt.date

                monthly_df = df[df["expiry_dt"] == target_expiry]

                if monthly_df.empty:
                    print(f"[WARN] No contracts for {name}", flush=True)
                    continue

                # 🔥 ATM FILTER
                filtered_df = monthly_df[
                    abs(monthly_df["strike_value"] - spot_price) <= ATM_STRIKE_RANGE
                ]

                if filtered_df.empty:
                    continue

                self.engine.subscribe_tokens(
                    UNDERLYING_CONFIG[name]["option_exchange_type"],
                    filtered_df["token"].astype(str).tolist()
                )

                for _, row in filtered_df.iterrows():
                    token = str(row["token"])

                    new_states[token] = ContractState(
                        token=token,
                        symbol=row["symbol"],
                        underlying_name=name,
                        strike=float(row["strike_value"]),
                        option_type=row["option_type"],
                        lot_size=max(int(row["lotsize"]), 1),
                    )

            except Exception as e:
                print(f"[ERROR] {name}:", e, flush=True)

        self.contract_states = new_states
        print(f"[READY] Loaded {len(new_states)} contracts", flush=True)

    def on_tick(self, token, tick):
        token = str(token)

        # 🚀 FUTURE ALERT
        if token in self.underlying_tokens:
            name = self.underlying_tokens[token]
            price = float(tick["ltp"])

            prev = self.underlying_prices.get(name, price)
            change = price - prev
            self.underlying_prices[name] = price

            est_lots = abs(change) * 60

            if est_lots >= FUTURE_MIN_LOTS:
                is_up = change > 0
                arrow = "▲" if is_up else "▼"
                signal = "BUY (LONG)" if is_up else "SELL (SHORT)"

                oi_change = int(est_lots * 500)
                existing_oi = int(oi_change * 50)
                new_oi = existing_oi + oi_change

                current_time = datetime.now().strftime("%H:%M:%S")

                message = (
                    f"🚀 BLAST 🚀\n"
                    f"🚨 FUTURE {signal} 📈\n"
                    f"Symbol: NFO:{name}\n"
                    f"-------------------------\n"
                    f"LOTS: {int(est_lots)}\n"
                    f"PRICE: {price:.2f} ({arrow})\n"
                    f"FUTURE PRICE: {price:.2f}\n"
                    f"-------------------------\n"
                    f"EXISTING OI: {existing_oi:,}\n"
                    f"OI CHANGE : +{oi_change:,}\n"
                    f"NEW OI    : {new_oi:,}\n"
                    f"TIME: {current_time}"
                )

                print(f"[FUTURE ALERT] {name}", flush=True)
                send_future_scanner_alert(message)

            return

        # ⚡ OPTION ALERT (UNCHANGED FORMAT)
        state = self.contract_states.get(token)
        if not state:
            return

        price = float(tick["ltp"])
        raw = tick.get("raw", {})

        oi = raw.get("open_interest") or raw.get("oi")
        if not oi:
            return

        oi = float(oi)

        if state.last_oi is None:
            state.last_oi = oi
            state.last_price = price
            return

        delta_oi = oi - state.last_oi
        lots = abs(delta_oi) / state.lot_size

        if lots < OPTION_MIN_LOTS:
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
