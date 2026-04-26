import time

from auth import get_angel_session
from future_1h_module import Future1HModule
from market_data_engine import MarketDataEngine
from paper_trade_module import PaperTradeModule


def main():
    print("Starting scanner hub...")
    smart, session_data = get_angel_session()
    if not smart:
        print("Angel One login failed. Scanner hub exiting.")
        return

    engine = MarketDataEngine(smart, session_data)

    paper_trade = PaperTradeModule(engine)
    futures_1h = Future1HModule(engine)

    print("Initializing paper trade module...")
    paper_trade.start()
    print("Initializing futures 1H module...")
    futures_1h.start()
    print("Connecting shared Angel One WebSocket...")
    engine.connect()

    print("Scanner hub started: shared Angel One WebSocket feeding paper trade and 1H futures scanners.")
    while True:
        try:
            if not engine.ws_connected and engine.ws is None:
                engine.connect()
            if not futures_1h.setups and not futures_1h.setup_in_progress:
                futures_1h.start_background_setup()
            time.sleep(5)
        except KeyboardInterrupt:
            print("Scanner hub stopped by user.")
            break
        except Exception as exc:
            print(f"Scanner hub loop error: {exc}")
            time.sleep(10)


if __name__ == "__main__":
    main()
