import time

from auth import get_angel_session
from market_data_engine import MarketDataEngine
from option_burst_module import OptionBurstModule
from paper_trade_module import PaperTradeModule


def main():
    print("Starting scanner hub...")
    smart, session_data = get_angel_session()
    if not smart:
        print("Angel One login failed. Scanner hub exiting.")
        return

    engine = MarketDataEngine(smart, session_data)

    paper_trade = PaperTradeModule(engine)
    option_burst = OptionBurstModule(engine)

    print("Initializing paper trade module...")
    paper_trade.start()
    print("Initializing option burst module...")
    option_burst.start()
    print("Connecting shared Angel One WebSocket...")
    engine.connect()

    print("Scanner hub started: shared Angel One WebSocket feeding paper trade and option burst scanners.")
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
