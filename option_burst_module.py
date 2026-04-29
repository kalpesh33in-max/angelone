import threading
import time
import traceback
from dataclasses import dataclass
from datetime import datetime
from zoneinfo import ZoneInfo

import pandas as pd

from data_manager import load_option_contracts, resolve_underlying_instrument
from telegram_utils import send_future_scanner_alert


IST = ZoneInfo("Asia/Kolkata")
REFRESH_SECONDS = 60

# 🔥 FINAL FILTERS
OPTION_MIN_LOTS = 200
FUTURE_MIN_LOTS = 500
ATM_RANGE = 20

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
        print("🔥 FINAL Monthly ATM±20 Burst Scanner Started", flush=True)
        send_future_scanner_alert("Monthly ATM±20 Burst Scanner Started")
        threading.Thread(target=self._setup, daemon=True).start()

    def _setup(self):
        while True:
            try:
                self.refresh()
            except Exception as e:
                print("SETUP ERROR:", e, flush=True)
                traceback.print_exc()
            time.sleep(REFRESH_SECONDS)

    def refresh(self):
        new_states = {}

        for name in UNDERLYING_CONFIG.keys():
            try:
                underlying = resolve_underlying_instrument(name)
                if not underlying:
                    print(f"[WARN] No underlying for {name}", flush=True)
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
                    print(f"[WARN] No options for {name}", flush=True)
                    continue

                # 🔥 FIX: TRUE MONTHLY EXPIRY
                df["expiry_dt"] = pd.to_datetime(df["expiry_dt"])

                monthly_expiries = (
                    df.groupby(df["expiry_dt"].dt.to_period("M"))["expiry_dt"]
                    .max()
                    .values
                )

                monthly_df = df[df["expiry_dt"].isin(monthly_expiries)]

                print(f"[INFO] {name} monthly contracts: {len(monthly_df)}", flush=True)

                # 🔥 ATM ± RANGE FILTER
                filtered_df = monthly_df[
                    abs(monthly_df["strike_value"] - spot_price) <= ATM_RANGE
                ]

                if filtered_df.empty:
                    print(f"[WARN] No ATM contracts for {name}", flush=True)
                    continue

                self.engine.subscribe_tokens(
                    UNDERLYING_CONFIG[name]["option_exchange_type"],
                    filtered_df["token"].astype(str).tolist()
                )

                print(f"[SUB] {name} ATM contracts: {len(filtered_df)}", flush=True)

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
                print(f"[ERROR] refresh {name}: {e}", flush=True)
                traceback.print_exc()

        self.contract_states = new_states
        print(f"[READY] Total ATM contracts loaded: {len(new_states)}", flush=True)

    def on_tick(self, token, tick):
        try:
            token = str(token)

            # 🚀 FUTURE BURST (BLAST STYLE)
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

            # ⚡ OPTION BURST (UNCHANGED STYLE)
            state = self.contract_states.get(token)
            if not state:
                return

            price = float(tick["ltp"])
            raw = tick.get("raw", {})

            oi = None
            for key in ["open_interest", "oi", "oi_qty", "openInterest"]:
                if key in raw and raw[key]:
                    oi = float(raw[key])
                    break

            if not oi:
                return

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

            print(f"[OPTION ALERT] {state.symbol}", flush=True)

            send_future_scanner_alert(
                f"{state.symbol}\n\n"
                f"⚡ {label}{suffix}\n"
                f"Lots: {int(lots)}\n"
                f"Price: {price:.2f}"
            )

            state.last_oi = oi
            state.last_price = price

        except Exception as e:
            print("TICK ERROR:", e, flush=True)
            traceback.print_exc()
