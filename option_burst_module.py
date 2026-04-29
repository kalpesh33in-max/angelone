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
CONFIRMATION_TIME = 15

OPTION_MIN_LOTS = 200
FUTURE_MIN_LOTS = 200

# 🔥 FIXED MONTHLY EXPIRY
EXPIRY_MAP = {
    "NIFTY": "2026-05-26",
    "BANKNIFTY": "2026-05-26",
    "FINNIFTY": "2026-05-26",
    "MIDCPNIFTY": "2026-05-26",
    "SENSEX": "2026-05-27",
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
        self.pending_confirmations = {}

    def start(self):
        self.engine.register(self)
        print("🔥 FINAL SCANNER STARTED", flush=True)
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

                spot = self.engine.get_latest_price(token)
                if not spot:
                    continue

                self.underlying_prices[name] = spot

                df = load_option_contracts(name, expiry_count=6)
                if df.empty:
                    continue

                df["expiry_dt"] = pd.to_datetime(df["expiry_dt"]).dt.date
                target = pd.to_datetime(EXPIRY_MAP[name]).date()

                monthly_df = df[df["expiry_dt"] == target]
                if monthly_df.empty:
                    print(f"[WARN] {name} no contracts", flush=True)
                    continue

                filtered = monthly_df[
                    abs(monthly_df["strike_value"] - spot) <= ATM_STRIKE_RANGE
                ]

                if filtered.empty:
                    continue

                self.engine.subscribe_tokens(
                    UNDERLYING_CONFIG[name]["option_exchange_type"],
                    filtered["token"].astype(str).tolist()
                )

                for _, row in filtered.iterrows():
                    new_states[str(row["token"])] = ContractState(
                        token=str(row["token"]),
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
        now = time.time()

        # ================= FUTURE =================
        if token in self.underlying_tokens:
            name = self.underlying_tokens[token]
            price = float(tick["ltp"])

            prev = self.underlying_prices.get(name, price)
            change = price - prev
            self.underlying_prices[name] = price

            lots = abs(change) * 60
            key = f"FUT_{name}"

            if lots >= FUTURE_MIN_LOTS:
                p = self.pending_confirmations.get(key)

                if not p:
                    self.pending_confirmations[key] = {
                        "start": now,
                        "lots": lots,
                        "price": price,
                        "change": change
                    }
                    return

                if now - p["start"] < CONFIRMATION_TIME:
                    if lots < FUTURE_MIN_LOTS:
                        self.pending_confirmations.pop(key, None)
                    else:
                        p["lots"] = lots
                        p["price"] = price
                    return

                if lots >= p["lots"]:

                    # 🔥 FUTURE TURNOVER (YOUR RULE)
                    turnover = lots * 100000

                    # 🔥 CLASSIFICATION
                    if change >= 0:
                        label = "ACTION"
                        suffix = ""
                    else:
                        label = "WRITER"
                        suffix = " ✍️"

                    msg = (
                        f"{name} FUT\n\n"
                        f"⚡ {label}{suffix}\n"
                        f"Turnover: ₹{turnover/1e7:.2f} Cr\n"
                        f"Lots: {int(lots)}\n"
                        f"Price: {price:.2f}\n"
                        f"Spot Price: {price:.2f}"
                    )

                    send_future_scanner_alert(msg)

                self.pending_confirmations.pop(key, None)

            return

        # ================= OPTION =================
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

        key = f"OPT_{token}"

        if lots >= OPTION_MIN_LOTS:
            p = self.pending_confirmations.get(key)

            if not p:
                self.pending_confirmations[key] = {
                    "start": now,
                    "lots": lots,
                    "price": price,
                    "doi": delta_oi
                }
                return

            if now - p["start"] < CONFIRMATION_TIME:
                if lots < OPTION_MIN_LOTS:
                    self.pending_confirmations.pop(key, None)
                else:
                    p["lots"] = lots
                    p["price"] = price
                return

            if lots >= p["lots"]:
                dp = price - state.last_price

                if dp >= 0 and delta_oi >= 0:
                    label, suffix = "ACTION", ""
                elif dp < 0 and delta_oi >= 0:
                    label, suffix = "WRITER", " ✍️"
                elif dp >= 0 and delta_oi < 0:
                    label, suffix = "SHORT COVERING", " ↗️"
                else:
                    label, suffix = "UNWINDING", " ⤵️"

                send_future_scanner_alert(
                    f"{state.symbol}\n\n"
                    f"⚡ {label}{suffix}\n"
                    f"Lots: {int(lots)}\n"
                    f"Price: {price:.2f}"
                )

            self.pending_confirmations.pop(key, None)

        state.last_oi = oi
        state.last_price = price
