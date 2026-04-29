import time
import threading
from datetime import datetime, timedelta
from dataclasses import dataclass
import pandas as pd

from data_manager import load_option_contracts, resolve_underlying_instrument
from telegram_utils import send_future_scanner_alert


# ================= CONFIG =================
CONFIRMATION_TIME = 15
OPTION_THRESHOLD = 200
FUTURE_THRESHOLD = 200
ATM_RANGE = 50

EXPIRY_MAP = {
    "NIFTY": "2026-05-26",
    "BANKNIFTY": "2026-05-26",
    "FINNIFTY": "2026-05-26",
    "MIDCPNIFTY": "2026-05-26",
    "SENSEX": "2026-05-27",
}


@dataclass
class Contract:
    token: str
    symbol: str
    lot: int
    name: str


# ================= MODULE =================
class OptionBurstModule:

    def __init__(self, engine):
        self.engine = engine

        self.contracts = {}
        self.underlying_tokens = {}

        self.option_history = {}
        self.future_history = {}

        self.active_watches = {}

    # ================= START =================
    def start(self):
        print("🚀 OptionBurstModule started", flush=True)
        self.engine.register(self)

        threading.Thread(target=self._setup, daemon=True).start()

    def _setup(self):
        while True:
            try:
                self.load_contracts()
            except Exception as e:
                print("SETUP ERROR:", e, flush=True)
            time.sleep(60)

    # ================= LOAD =================
    def load_contracts(self):
        new = {}

        for name in EXPIRY_MAP:

            try:
                underlying = resolve_underlying_instrument(name)
                if not underlying:
                    continue

                token = str(underlying["token"])
                self.underlying_tokens[token] = name

                spot = self.engine.get_latest_price(token)
                if not spot:
                    continue

                df = load_option_contracts(name, expiry_count=6)
                if df.empty:
                    continue

                df["expiry_dt"] = pd.to_datetime(df["expiry_dt"]).dt.date
                target = pd.to_datetime(EXPIRY_MAP[name]).date()

                df = df[df["expiry_dt"] == target]
                df = df[abs(df["strike_value"] - spot) <= ATM_RANGE]

                for _, r in df.iterrows():
                    new[str(r["token"])] = Contract(
                        token=str(r["token"]),
                        symbol=r["symbol"],
                        lot=max(int(r["lotsize"]), 1),
                        name=name
                    )

            except Exception as e:
                print(f"LOAD ERROR {name}:", e, flush=True)

        self.contracts = new
        print(f"✅ Loaded {len(new)} contracts", flush=True)

    # ================= TICK =================
    def on_tick(self, token, tick):

        try:
            token = str(token)
            now = datetime.now()

            price = float(tick.get("ltp", 0))
            raw = tick.get("raw", {})

            # ================= FUTURE =================
            if token in self.underlying_tokens:

                name = self.underlying_tokens[token]
                oi = raw.get("open_interest") or raw.get("oi")

                if not oi:
                    return

                if name not in self.future_history:
                    self.future_history[name] = []

                hist = self.future_history[name]

                prev_oi = hist[-1]["oi"] if hist else 0
                prev_price = hist[-1]["price"] if hist else price

                lot_size = 30
                tick_lots = int(abs(oi - prev_oi) / lot_size)

                key = f"FUT_{name}"

                # trigger
                if prev_oi > 0 and tick_lots >= FUTURE_THRESHOLD and key not in self.active_watches:
                    self.active_watches[key] = {
                        "start_oi": prev_oi,
                        "start_price": prev_price,
                        "end_time": now + timedelta(seconds=CONFIRMATION_TIME),
                        "symbol": f"NFO:{name}"
                    }

                # confirm
                if key in self.active_watches:
                    w = self.active_watches[key]

                    if now >= w["end_time"]:
                        oi_chg = oi - w["start_oi"]
                        p_chg = price - w["start_price"]

                        final_lots = int(abs(oi_chg) / lot_size)

                        if final_lots >= FUTURE_THRESHOLD:

                            action = "FUTURE BUY (LONG) 📈" if p_chg >= 0 else "FUTURE SELL (SHORT) 📉"
                            arrow = "▲" if p_chg >= 0 else "▼"

                            msg = (
                                f"🚀 BLAST 🚀\n"
                                f"🚨 {action}\n"
                                f"Symbol: {w['symbol']}\n"
                                f"━━━━━━━━━━━━━━━\n"
                                f"LOTS: {final_lots}\n"
                                f"PRICE: {price:.2f} ({arrow})\n"
                                f"FUTURE PRICE: {price:.2f}\n"
                                f"━━━━━━━━━━━━━━━\n"
                                f"EXISTING OI: {w['start_oi']:,}\n"
                                f"OI CHANGE  : {oi_chg:+,d}\n"
                                f"NEW OI     : {int(oi):,}\n"
                                f"TIME: {now.strftime('%H:%M:%S')}"
                            )

                            send_future_scanner_alert(msg)

                        del self.active_watches[key]

                hist.append({"oi": oi, "price": price})
                if len(hist) > 20:
                    hist.pop(0)

                return

            # ================= OPTION =================
            state = self.contracts.get(token)
            if not state:
                return

            oi = raw.get("open_interest") or raw.get("oi")
            if not oi:
                return

            oi = float(oi)

            if token not in self.option_history:
                self.option_history[token] = []

            hist = self.option_history[token]

            prev_oi = hist[-1]["oi"] if hist else 0
            prev_price = hist[-1]["price"] if hist else price

            lot_size = state.lot
            tick_lots = int(abs(oi - prev_oi) / lot_size)

            key = token

            # trigger
            if prev_oi > 0 and tick_lots >= OPTION_THRESHOLD and key not in self.active_watches:
                self.active_watches[key] = {
                    "start_oi": prev_oi,
                    "start_price": prev_price,
                    "end_time": now + timedelta(seconds=CONFIRMATION_TIME),
                    "symbol": state.symbol
                }

            # confirm
            if key in self.active_watches:
                w = self.active_watches[key]

                if now >= w["end_time"]:
                    oi_chg = oi - w["start_oi"]
                    p_chg = price - w["start_price"]

                    final_lots = int(abs(oi_chg) / lot_size)

                    if final_lots >= OPTION_THRESHOLD:

                        if oi_chg > 0:
                            action = "CALL BUY 🔵" if p_chg >= 0 else "CALL WRITER ✍️"
                        else:
                            action = "SHORT COVERING ⤴️" if p_chg >= 0 else "LONG UNWINDING ⤵️"

                        arrow = "▲" if p_chg >= 0 else "▼"

                        msg = (
                            f"🚀 BLAST 🚀\n"
                            f"🚨 {action}\n"
                            f"Symbol: {w['symbol']}\n"
                            f"━━━━━━━━━━━━━━━\n"
                            f"LOTS: {final_lots}\n"
                            f"PRICE: {price:.2f} ({arrow})\n"
                            f"FUTURE PRICE: {price:.2f}\n"
                            f"━━━━━━━━━━━━━━━\n"
                            f"EXISTING OI: {w['start_oi']:,}\n"
                            f"OI CHANGE  : {oi_chg:+,.0f}\n"
                            f"NEW OI     : {oi:,.0f}\n"
                            f"TIME: {now.strftime('%H:%M:%S')}"
                        )

                        send_future_scanner_alert(msg)

                    del self.active_watches[key]

            hist.append({"oi": oi, "price": price})
            if len(hist) > 20:
                hist.pop(0)

        except Exception as e:
            print("TICK ERROR:", e, flush=True)
