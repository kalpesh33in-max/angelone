import asyncio
import threading
from dataclasses import dataclass
from datetime import datetime, timedelta

from telethon import TelegramClient, events
from telethon.sessions import StringSession

from data_manager import resolve_nfo_option
from env_config import SOURCE_CHAT, TG_API_HASH, TG_API_ID, TG_SESSION_STR
from telegram_utils import send_paper_trade_alert


IST_OFFSET = timedelta(hours=5, minutes=30)
STEP_POINTS = 30
MAX_TARGET_LEVEL = 4
DUPLICATE_MINUTES = 10
EXCHANGE_TYPE_NFO = 2


def now_ist():
    return datetime.utcnow() + IST_OFFSET


@dataclass
class Trade:
    strike: int
    option_type: str
    symbol: str
    token: str
    entry: float
    sl: float
    targets: list[float]
    highest_target: int = 0
    last_price_alert: float = 0.0


class PaperTradeModule:
    def __init__(self, engine):
        self.engine = engine
        self.trade = None
        self.last_signal = {}
        self.client = None

    def start(self):
        self.engine.register(self)
        print("Paper trade module registered on shared market-data engine.")
        send_paper_trade_alert("Paper Trade Scanner Started")
        self._start_telegram_listener()

    def _start_telegram_listener(self):
        if not (TG_API_ID and TG_API_HASH and TG_SESSION_STR):
            print("Paper trade Telegram listener disabled: missing TG_API_ID/TG_API_HASH/TG_SESSION_STR")
            return

        print("Starting paper trade Telegram listener thread...")
        threading.Thread(target=self._telegram_thread_main, daemon=True).start()

    def _telegram_thread_main(self):
        asyncio.run(self._telegram_main())

    async def _telegram_main(self):
        self.client = TelegramClient(StringSession(TG_SESSION_STR), int(TG_API_ID), TG_API_HASH)
        await self.client.start()
        print(f"Paper trade listener active for source chat: {SOURCE_CHAT}")

        @self.client.on(events.NewMessage())
        async def handler(event):
            chat = await event.get_chat()
            text = event.raw_text or ""
            if not self._source_match(event, chat):
                return

            parsed = self.parse_dual_match(text)
            if not parsed:
                return

            strike, option_type = parsed
            result = self.process_signal(strike, option_type)
            if not result:
                return

            if result[0] == "REV":
                _, old_trade, exit_price, new_trade = result
                send_paper_trade_alert(
                    f"\U0001f501 EXIT BANKNIFTY {old_trade.strike} {old_trade.option_type} @ {exit_price:.2f}"
                )
                send_paper_trade_alert(self.format_trade(new_trade))
            elif result[0] == "NEW":
                _, trade = result
                send_paper_trade_alert(self.format_trade(trade))

        await self.client.run_until_disconnected()

    def _source_match(self, event, chat):
        source_value = str(SOURCE_CHAT).strip().lower()
        candidates = [str(getattr(event, "chat_id", "")).lower()]
        for value in (getattr(chat, "title", None), getattr(chat, "username", None), getattr(chat, "first_name", None)):
            if value:
                candidates.append(str(value).strip().lower())
        return source_value in candidates

    def parse_dual_match(self, text):
        text_upper = text.upper()
        if "INSTITUTIONAL DUAL MATCH" not in text_upper:
            return None

        import re

        match = re.search(r"ACTION:\s*BUY\s+BANKNIFTY\s+(\d+)\s*(CE|PE)", text, re.IGNORECASE)
        if not match:
            match = re.search(r"BANKNIFTY\s+(\d+)\s*(CE|PE)", text, re.IGNORECASE)
        if not match:
            return None
        return int(match.group(1)), match.group(2).upper()

    def duplicate(self, key):
        now = now_ist()
        if key in self.last_signal and now - self.last_signal[key] < timedelta(minutes=DUPLICATE_MINUTES):
            return True
        self.last_signal[key] = now
        return False

    def resolve(self, strike, option_type):
        resolved = resolve_nfo_option("BANKNIFTY", strike, option_type)
        if not resolved:
            raise ValueError(f"Unable to resolve BANKNIFTY {strike} {option_type}")
        return resolved["symbol"], resolved["token"]

    def create_trade(self, strike, option_type):
        symbol, token = self.resolve(strike, option_type)
        self.engine.subscribe_tokens(EXCHANGE_TYPE_NFO, [token])
        price = self.engine.get_latest_price(token)
        if price is None:
            price = self.engine.get_ltp_snapshot("NFO", symbol, token)

        targets = [price + STEP_POINTS * i for i in range(1, MAX_TARGET_LEVEL + 1)]
        return Trade(
            strike=strike,
            option_type=option_type,
            symbol=symbol,
            token=token,
            entry=price,
            sl=price - STEP_POINTS,
            targets=targets,
            highest_target=0,
            last_price_alert=price,
        )

    def process_signal(self, strike, option_type):
        key = f"{strike}{option_type}"
        if self.duplicate(key):
            print(f"Duplicate paper trade signal ignored: {key}")
            return None

        if self.trade and self.trade.option_type != option_type:
            exit_price = self.engine.get_latest_price(self.trade.token)
            if exit_price is None:
                exit_price = self.engine.get_ltp_snapshot("NFO", self.trade.symbol, self.trade.token)
            old_trade = self.trade
            self.trade = self.create_trade(strike, option_type)
            return ("REV", old_trade, exit_price, self.trade)

        if self.trade:
            print("Paper trade signal ignored: active trade already open in same direction.")
            return None

        self.trade = self.create_trade(strike, option_type)
        print(f"Paper trade created: BANKNIFTY {strike} {option_type}")
        return ("NEW", self.trade)

    def on_tick(self, token, tick):
        if not self.trade or token != self.trade.token:
            return

        price = tick["ltp"]
        trade = self.trade

        if price <= trade.sl:
            send_paper_trade_alert(f"\u274c SL HIT @ {price:.2f}")
            self.trade = None
            return

        if trade.highest_target < MAX_TARGET_LEVEL and price >= trade.targets[trade.highest_target]:
            trade.highest_target += 1
            trade.sl = trade.entry if trade.highest_target == 1 else trade.targets[trade.highest_target - 2]
            send_paper_trade_alert(
                f"\U0001f3af BANKNIFTY {trade.strike} {trade.option_type} "
                f"T{trade.highest_target} HIT @ {price:.2f}"
            )

        if price > trade.last_price_alert:
            trade.last_price_alert = price
            send_paper_trade_alert(f"\U0001f4c8 Price Update: {price:.2f}")

    def format_trade(self, trade):
        return "\n".join(
            [
                f"\U0001f525 BANKNIFTY {trade.strike} {trade.option_type}",
                "",
                f"\U0001f4cd Entry: {trade.entry:.2f}",
                f"\U0001f6e1\ufe0f SL: {trade.sl:.2f}",
                f"\U0001f3af T1: {trade.targets[0]:.2f}",
                f"\U0001f3af T2: {trade.targets[1]:.2f}",
                f"\U0001f3af T3: {trade.targets[2]:.2f}",
                f"\U0001f3af T4: {trade.targets[3]:.2f}",
            ]
        )
