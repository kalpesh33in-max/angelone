import json
import threading
import time
from collections import defaultdict

from SmartApi.smartWebSocketV2 import SmartWebSocketV2

from env_config import ANGEL_API_KEY, ANGEL_CLIENT_ID


class MarketDataEngine:
    def __init__(self, smart, session_data):
        self.smart = smart
        self.session_data = session_data
        self.ws = None
        self.ws_connected = False
        self.latest_ticks = {}
        self.subscribers = []
        self.subscriptions = defaultdict(set)
        self._connect_lock = threading.Lock()

    def register(self, subscriber):
        self.subscribers.append(subscriber)

    def connect(self):
        with self._connect_lock:
            if self.ws is not None:
                return

            self.ws = SmartWebSocketV2(
                auth_token=self.session_data["jwtToken"],
                api_key=ANGEL_API_KEY,
                client_code=ANGEL_CLIENT_ID,
                feed_token=self.smart.getfeedToken(),
            )
            self.ws.on_open = self.on_open
            self.ws.on_data = self.on_data
            self.ws.on_error = self.on_error
            self.ws.on_close = self.on_close
            threading.Thread(target=self.ws.connect, daemon=True).start()

    def subscribe_tokens(self, exchange_type, tokens):
        new_tokens = []
        for token in tokens:
            token_str = str(token)
            if token_str not in self.subscriptions[exchange_type]:
                self.subscriptions[exchange_type].add(token_str)
                new_tokens.append(token_str)

        if self.ws_connected and new_tokens:
            self._subscribe_chunk(exchange_type, new_tokens)

    def on_open(self, ws):
        self.ws_connected = True
        print("Shared WebSocket connected.")
        for exchange_type, tokens in self.subscriptions.items():
            token_list = list(tokens)
            if token_list:
                self._subscribe_chunk(exchange_type, token_list)

    def _subscribe_chunk(self, exchange_type, tokens):
        chunk_size = 50
        for idx in range(0, len(tokens), chunk_size):
            chunk = tokens[idx: idx + chunk_size]
            self.ws.subscribe(
                correlation_id=f"ex_{exchange_type}_{idx}",
                mode=3,
                token_list=[{"exchangeType": exchange_type, "tokens": chunk}],
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
                if not token:
                    continue

                raw_ltp = tick.get("last_traded_price", tick.get("ltp", 0))
                price = float(raw_ltp) if isinstance(raw_ltp, (int, float)) else 0.0
                if price > 100000:
                    price = price / 100
                if price <= 0:
                    continue

                self.latest_ticks[token] = {
                    "token": token,
                    "ltp": price,
                    "raw": tick,
                    "timestamp": time.time(),
                }
                for subscriber in self.subscribers:
                    try:
                        subscriber.on_tick(token, self.latest_ticks[token])
                    except Exception as exc:
                        print(f"Subscriber error for {subscriber.__class__.__name__}: {exc}")
        except Exception as exc:
            print(f"Shared WebSocket message error: {exc}")

    def on_error(self, ws, error):
        print(f"Shared WebSocket error: {error}")

    def on_close(self, ws):
        self.ws_connected = False
        self.ws = None
        print("Shared WebSocket closed.")

    def get_latest_price(self, token):
        tick = self.latest_ticks.get(str(token))
        return tick["ltp"] if tick else None

    def get_ltp_snapshot(self, exchange, symbol, token):
        data = self.smart.ltpData(exchange, symbol, str(token))
        return float(data["data"]["ltp"])
