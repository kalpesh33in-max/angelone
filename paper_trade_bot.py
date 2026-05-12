import asyncio
import logging
import os
import re
import json
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any
import pandas as pd
import pyotp
import pytz
import requests
from logzero import loglevel
from SmartApi.smartConnect import SmartConnect
from telethon import TelegramClient, events
from telethon.sessions import StringSession

IST = pytz.timezone("Asia/Kolkata")
loglevel(logging.CRITICAL)

# Static Settings
LOT_SIZES = {"NIFTY": 65, "BANKNIFTY": 30}
DEFAULT_STEP_POINTS = 30
MAX_TARGET_LEVEL = 4

class Engine:
    def __init__(self) -> None:
        self.smart = None
        self.df = None
        self.trades: dict[str, Trade] = {}
        self.real_trades_today = 0
        self.real_trade_day = datetime.now(IST).date()

    def extract_order_id(self, response: Any) -> str:
        """PERMANENT FIX: Robustly handles both dict and stringified JSON from API."""
        if isinstance(response, str):
            try:
                response = json.loads(response)
            except:
                return f"RAW_RESP: {str(response)[:50]}"
        
        if isinstance(response, dict):
            data = response.get("data")
            if isinstance(data, dict):
                oid = data.get("orderid") or data.get("order_id")
                if oid: return str(oid)
            if response.get("orderid"):
                return str(response.get("orderid"))
        
        raise RuntimeError(f"Order rejected or empty: {response}")

    def ltp(self, exchange: str, symbol: str, token: str) -> float:
        """Handles stringified responses during LTP fetching."""
        try:
            resp = self.smart.ltpData(exchange, symbol, token)
            if isinstance(resp, str): resp = json.loads(resp)
            
            data = resp.get("data")
            if isinstance(data, str): data = json.loads(data)
            
            if isinstance(data, dict):
                return float(data.get("ltp") or data.get("LTP") or 0)
            return 0.0
        except:
            return 0.0

    def place_real_order(self, trade: "Trade", trans_type: str) -> str:
        """Attempts real trade; returns ERROR string instead of crashing."""
        if not os.getenv("REAL_TRADE_ENABLED", "false").lower() == "true":
            return ""
            
        params = {
            "variety": "NORMAL",
            "tradingsymbol": trade.symbol,
            "symboltoken": trade.token,
            "transactiontype": trans_type,
            "exchange": trade.exchange,
            "ordertype": "MARKET",
            "producttype": "INTRADAY",
            "duration": "DAY",
            "price": "0",
            "quantity": str(trade.qty)
        }
        try:
            res = self.smart.placeOrder(params)
            return self.extract_order_id(res)
        except Exception as e:
            print(f"REAL ORDER FAILURE: {str(e)}")
            return "FAILED_API_ERROR"

    def process_signal(self, underlying: str, strike: int, opt_type: str):
        """Creates paper trade alert even if the real trade fails."""
        symbol, token, exch = self.resolve(underlying, strike, opt_type)
        price = self.ltp(exch, symbol, token)
        
        # Create Trade Object (Paper Trade)
        new_trade = Trade(
            underlying=underlying, strike=strike, option_type=opt_type,
            symbol=symbol, token=token, exchange=exch, entry=price,
            sl=price-30, targets=[price+30, price+60, price+90, price+120],
            qty=LOT_SIZES.get(underlying, 1), step_points=30
        )

        # Attempt Real Trade
        if os.getenv("REAL_TRADE_ENABLED", "false").lower() == "true":
            try:
                oid = self.place_real_order(new_trade, "BUY")
                new_trade.entry_order_id = oid
            except Exception as e:
                print(f"Non-critical failure: Paper trade alert will still send. Error: {e}")

        self.trades[underlying] = new_trade
        return ("NEW", new_trade)

    # ... (Rest of resolution and login logic remains same)
