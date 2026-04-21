import os
from datetime import datetime

import pandas as pd
import requests


BASE_DIR = os.path.dirname(__file__)
SCRIP_MASTER_FILE = os.path.join(BASE_DIR, "OpenAPIScripMaster.json")
SCRIP_MASTER_URL = "https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json"


def download_scrip_master():
    print("Downloading Scrip Master from Angel One...")
    try:
        response = requests.get(SCRIP_MASTER_URL, timeout=30)
        if response.status_code == 200:
            with open(SCRIP_MASTER_FILE, "wb") as f:
                f.write(response.content)
            print("Scrip Master downloaded successfully.")
        else:
            print(f"Failed to download Scrip Master. Status: {response.status_code}")
    except Exception as e:
        print(f"Scrip Master Error: {e}")


def load_mcx_instruments(symbols):
    if not os.path.exists(SCRIP_MASTER_FILE):
        download_scrip_master()

    try:
        df = pd.read_json(SCRIP_MASTER_FILE)
        mcx_df = df[df["exch_seg"] == "MCX"].copy()
        mcx_df["expiry_dt"] = pd.to_datetime(mcx_df["expiry"], format="%d%b%Y", errors="coerce")

        today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        mcx_df = mcx_df[mcx_df["expiry_dt"] >= today].copy()

        target_instruments = {}

        for base_symbol in symbols:
            subset = mcx_df[mcx_df["name"] == base_symbol].copy()
            if subset.empty:
                continue

            futures_all = subset[subset["symbol"].str.contains("FUT", na=False)].copy()
            options_all = subset[subset["symbol"].str.contains("CE|PE", na=False)].copy()

            nearest_future_expiry = futures_all["expiry_dt"].min() if not futures_all.empty else pd.NaT
            nearest_option_expiry = options_all["expiry_dt"].min() if not options_all.empty else pd.NaT

            futures = (
                futures_all[futures_all["expiry_dt"] == nearest_future_expiry].copy()
                if not pd.isna(nearest_future_expiry)
                else pd.DataFrame()
            )

            if not futures.empty:
                row = futures.iloc[0]
                target_instruments[str(row["token"])] = {
                    "symbol": row["symbol"],
                    "name": base_symbol,
                    "lot_size": int(row["lotsize"]),
                    "exch_seg": "MCX",
                    "type": "FUT",
                    "expiry": nearest_future_expiry.strftime("%d%b%Y").upper(),
                    "strike": None,
                    "option_type": None,
                }

            options = (
                options_all[options_all["expiry_dt"] == nearest_option_expiry].copy()
                if not pd.isna(nearest_option_expiry)
                else pd.DataFrame()
            )

            if not options.empty:
                options["strike_num"] = pd.to_numeric(options["strike"], errors="coerce") / 100
                options = options.sort_values("strike_num")
                num_opt = len(options)

                if num_opt > 40:
                    mid = num_opt // 2
                    options = options.iloc[mid - 20: mid + 20]

                for _, row in options.iterrows():
                    strike_value = float(row["strike_num"]) if pd.notna(row["strike_num"]) else None
                    if strike_value is not None and strike_value > 10000:
                        strike_value = strike_value / 100

                    target_instruments[str(row["token"])] = {
                        "symbol": row["symbol"],
                        "name": base_symbol,
                        "lot_size": int(row["lotsize"]),
                        "exch_seg": "MCX",
                        "type": "OPT",
                        "expiry": nearest_option_expiry.strftime("%d%b%Y").upper(),
                        "strike": strike_value,
                        "option_type": "CE" if "CE" in str(row["symbol"]) else "PE",
                    }

        print(f"Loaded {len(target_instruments)} target MCX instruments (Futures + Options).")
        return target_instruments

    except Exception as e:
        print(f"Error loading MCX instruments: {e}")
        return {}


def load_symbols_from_csv(file_path="symbols.csv"):
    if not os.path.isabs(file_path):
        file_path = os.path.join(BASE_DIR, file_path)
    if not os.path.exists(file_path):
        return {}

    try:
        df = pd.read_csv(file_path)
        target_instruments = {
            str(row["token"]): {
                "symbol": row["symbol"],
                "name": row["name"],
                "lot_size": int(row["lotsize"]),
                "exch_seg": row["exch_seg"],
                "type": "OPT" if any(x in str(row["symbol"]) for x in ["CE", "PE"]) else "FUT",
            }
            for _, row in df.iterrows()
        }
        return target_instruments
    except Exception as e:
        print(f"Error reading CSV: {e}")
        return {}
