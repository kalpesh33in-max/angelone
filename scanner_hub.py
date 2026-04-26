import time

from auth import get_angel_session
from future_1h_module import Future1HModule
from market_data_engine import MarketDataEngine
from paper_trade_module import PaperTradeModule


def main():
    smart, session_data = get_angel_session()
    if not smart:
        return

    engine = MarketDataEngine(smart, session_data)

    paper_trade = PaperTradeModule(engine)
    futures_1h = Future1HModule(engine)

    futures_1h.start()
    paper_trade.start()
    engine.connect()

    print("Scanner hub started: shared Angel One WebSocket feeding paper trade and 1H futures scanners.")
    while True:
        try:
            if not engine.ws_connected and engine.ws is None:
                engine.connect()
            time.sleep(5)
        except KeyboardInterrupt:
            print("Scanner hub stopped by user.")
            break
        except Exception as exc:
            print(f"Scanner hub loop error: {exc}")
            time.sleep(10)


if __name__ == "__main__":
    main()
