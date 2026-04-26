import json
import os
import threading
import time
from collections import deque
from datetime import datetime

from SmartApi.smartWebSocketV2 import SmartWebSocketV2

from auth import get_angel_session
from data_manager import load_mcx_instruments, load_symbols_from_csv
from env_config import ANGEL_API_KEY, ANGEL_CLIENT_ID, MCX_SYMBOLS
from strategy import instrument_history, process_mcx_tick
from telegram_utils import send_telegram_mcx


class AngelMCXScanner:
    FUTURE_PRICE_BOUNDS = {
        "CRUDEOIL": 20000,
        "NATURALGAS": 2000,
        "SILVER": 200000,
        "GOLD": 200000,
    }

    OPTION_PRICE_BOUNDS = {
        "CRUDEOIL": 1000,
        "NATURALGAS": 500,
        "SILVER": 10000,
        "GOLD": 10000,
    }

    def __init__(self):
        self.obj = None
        self.session_data = None
        self.ws = None
        self.instruments = {}
        self.latest_data = {}
        self.latest_future_price = {}
        self.alerts_queue = []
        self.recent_alerts = deque(maxlen=50)
        self._tick_debug_count = 0
        self._first_msg_seen = False
        self._worker_started = False
        self._ws_connected = False
        self._startup_alert_sent = False

    def initialize(self):
        print("--- Angel One MCX Burst Scanner ---")

        self.obj, self.session_data = get_angel_session()
        if not self.obj:
            return False

        symbols_csv = os.path.join(os.path.dirname(__file__), "symbols.csv")
        if os.path.exists(symbols_csv):
            print("Loading instruments from symbols.csv...")
            self.instruments = load_symbols_from_csv(symbols_csv)
        else:
            print("Loading fresh instruments from Scrip Master...")
            self.instruments = load_mcx_instruments(MCX_SYMBOLS)

        if not self.instruments:
            print("No instruments loaded. Exiting.")
            return False

        print("Tracking Symbols:")
        sample_symbols = [v["symbol"] for v in list(self.instruments.values())[:5]]
        for symbol in sample_symbols:
            print(f" - {symbol}")
        print(f"Total Symbols: {len(self.instruments)}")
        return True

    def start(self):
        if not self.initialize():
            return
        self.start_background_stream()
        self.run_logic_loop()

    def start_background_stream(self):
        if self._worker_started:
            return
        self.connect_ws()
        self._worker_started = True

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

    def normalize_ltp(self, inst, raw_ltp):
        if not isinstance(raw_ltp, (int, float)):
            return 0.0

        ltp = float(raw_ltp)
        if ltp > 100000:
            ltp = ltp / 100

        max_reasonable = self.FUTURE_PRICE_BOUNDS.get(inst["name"])
        if max_reasonable:
            while ltp > max_reasonable:
                ltp = ltp / 100

        if inst["type"] == "OPT":
            option_bound = self.OPTION_PRICE_BOUNDS.get(inst["name"])
            if option_bound:
                while ltp > option_bound:
                    ltp = ltp / 100

            future_price = self.latest_future_price.get(inst["name"])
            strike = inst.get("strike")
            option_type = inst.get("option_type")

            if future_price and strike:
                intrinsic = max(future_price - strike, 0) if option_type == "CE" else max(strike - future_price, 0)
                max_reasonable_option = max(future_price * 0.5, intrinsic + future_price * 0.5)
                while ltp > max_reasonable_option and (ltp / 100) <= max_reasonable_option:
                    ltp = ltp / 100
            elif future_price and ltp > future_price and (ltp / 100) <= future_price:
                ltp = ltp / 100

        return ltp

    def on_open(self, ws):
        self._ws_connected = True
        print("WebSocket Connected. Subscribing to MCX instruments...")
        tokens = list(self.instruments.keys())
        chunk_size = 20

        for i in range(0, len(tokens), chunk_size):
            chunk = tokens[i:i + chunk_size]
            subscription_list = [{"exchangeType": 5, "tokens": chunk}]
            try:
                self.ws.subscribe(
                    correlation_id=f"mcx_chunk_{i}",
                    mode=3,
                    token_list=subscription_list,
                )
                print(f"Subscribed chunk {i // chunk_size + 1}: {len(chunk)} tokens")
                time.sleep(0.3)
            except Exception as e:
                print(f"Subscription Error for chunk {i // chunk_size + 1}: {e}")

        print(f"Subscribed to {len(tokens)} tokens across {len(range(0, len(tokens), chunk_size))} chunks.")

    def on_data(self, ws, message):
        try:
            if not self._first_msg_seen:
                print(f"First WebSocket Message Type: {type(message)}")
                self._first_msg_seen = True

            if isinstance(message, str):
                data = json.loads(message)
            elif isinstance(message, dict):
                data = message
            else:
                return

            if "status" in data and not data.get("status"):
                return

            payload = data.get("data", data)
            if not isinstance(payload, list):
                payload = [payload]

            for tick in payload:
                if not isinstance(tick, dict):
                    continue

                token = str(tick.get("token", tick.get("tk", "")))
                if token not in self.instruments:
                    continue

                raw_ltp = tick.get("last_traded_price", tick.get("ltp", 0))
                ltp = self.normalize_ltp(self.instruments[token], raw_ltp)
                oi = int(tick.get("open_interest", tick.get("oi", 0)))

                if ltp <= 0:
                    continue

                self.latest_data[token] = {"ltp": ltp, "oi": oi}
                if self._tick_debug_count < 5:
                    symbol = self.instruments[token]["symbol"]
                    print(f"Tick {self._tick_debug_count + 1}: {symbol} | LTP={ltp:.2f} | OI={oi}")
                    self._tick_debug_count += 1
        except Exception as e:
            print(f"WebSocket Message Error: {e}, Message: {str(message)[:100]}")

    def on_error(self, ws, error):
        print(f"WebSocket Error: {error}")

    def on_close(self, ws):
        self._ws_connected = False
        self.ws = None
        print("WebSocket Closed")

    def process_once(self, send_alerts=True):
        future_price_map = {}
        for token, data in list(self.latest_data.items()):
            inst = self.instruments.get(token)
            if inst and inst["type"] == "FUT" and data.get("ltp", 0) > 0:
                future_price_map[inst["name"]] = data["ltp"]
                self.latest_future_price[inst["name"]] = data["ltp"]

        for token, data in list(self.latest_data.items()):
            inst = self.instruments.get(token)
            if not inst:
                continue

            history = instrument_history.get(token, [])
            prev_oi = history[-1]["oi"] if history else data["oi"]
            oi_diff = data["oi"] - prev_oi

            if oi_diff != 0:
                now_str = datetime.now().strftime("%H:%M:%S")
                oi_diff_str = f"{oi_diff:+,d}"
                p_icon = "▲" if oi_diff > 0 else "▼"
                print(
                    f"[{now_str}] {inst['symbol']:<20} | Price: {data['ltp']:>8.2f} "
                    f"| OI: {data['oi']:>8,d} ({oi_diff_str:>6}) | {p_icon}"
                )

            process_mcx_tick(
                token,
                inst,
                data["ltp"],
                data["oi"],
                future_price_map.get(inst["name"]) or self.latest_future_price.get(inst["name"]),
                self.alerts_queue,
            )

        if self.alerts_queue:
            for alert in self.alerts_queue:
                lines = alert.splitlines()
                if len(lines) > 1:
                    print(f"Sending MCX Alert: {lines[1]}")
                self.recent_alerts.appendleft(
                    {"time": datetime.now().strftime("%H:%M:%S"), "message": alert}
                )
                if send_alerts:
                    send_telegram_mcx(alert)
            self.alerts_queue.clear()

    def run_logic_loop(self):
        print("Logic loop started. Monitoring for bursts...")
        if not self._startup_alert_sent:
            send_telegram_mcx("Angel One MCX Scanner Started")
            self._startup_alert_sent = True

        while True:
            try:
                self.process_once(send_alerts=True)
                if not self._ws_connected and self.ws is None:
                    self.connect_ws()
                time.sleep(5)
            except Exception as e:
                print(f"Loop Error: {e}")
                time.sleep(10)


if __name__ == "__main__":
    scanner = AngelMCXScanner()
    scanner.start()
