import os
from datetime import datetime
from zoneinfo import ZoneInfo

import pandas as pd
import requests


IST = ZoneInfo("Asia/Kolkata")
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


def _load_scrip_master_df():
    if not os.path.exists(SCRIP_MASTER_FILE):
        download_scrip_master()

    df = pd.read_json(SCRIP_MASTER_FILE)
    df["expiry_dt"] = pd.to_datetime(df["expiry"], format="%d%b%Y", errors="coerce")
    return df


def load_nfo_futures(max_symbols=None):
    try:
        df = _load_scrip_master_df()
        today = datetime.now(IST).replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=None)

        nfo_df = df[df["exch_seg"] == "NFO"].copy()
        nfo_df = nfo_df[nfo_df["expiry_dt"] >= today].copy()
        nfo_df = nfo_df[
            nfo_df["instrumenttype"].isin(["FUTIDX", "FUTSTK"]) |
            nfo_df["symbol"].astype(str).str.contains("FUT", na=False)
        ].copy()
        nfo_df = nfo_df[~nfo_df["symbol"].astype(str).str.contains("NSETEST", na=False)].copy()
        nfo_df = nfo_df[~nfo_df["name"].astype(str).str.contains("NSETEST", na=False)].copy()

        if nfo_df.empty:
            return {}

        nfo_df = nfo_df.sort_values(["name", "expiry_dt", "symbol"])
        nearest = nfo_df.groupby("name", as_index=False).first()
        if max_symbols:
            nearest = nearest.head(max_symbols)

        instruments = {}
        for _, row in nearest.iterrows():
            expiry_dt = row["expiry_dt"]
            instruments[str(row["token"])] = {
                "symbol": row["symbol"],
                "name": row["name"],
                "lot_size": int(row["lotsize"]),
                "exch_seg": "NFO",
                "type": "FUT",
                "expiry": expiry_dt.strftime("%d%b%Y").upper() if pd.notna(expiry_dt) else "",
                "instrumenttype": row.get("instrumenttype", "FUT"),
            }

        print(f"Loaded {len(instruments)} nearest NFO futures.")
        return instruments
    except Exception as e:
        print(f"Error loading NFO futures: {e}")
        return {}


def load_nfo_options_for_name(base_name):
    try:
        df = _load_scrip_master_df()
        today = datetime.now(IST).replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=None)

        nfo_df = df[df["exch_seg"] == "NFO"].copy()
        nfo_df = nfo_df[nfo_df["expiry_dt"] >= today].copy()
        nfo_df = nfo_df[nfo_df["name"] == base_name].copy()
        nfo_df = nfo_df[nfo_df["symbol"].astype(str).str.contains("CE|PE", na=False)].copy()
        if nfo_df.empty:
            return pd.DataFrame()

        nearest_expiry = nfo_df["expiry_dt"].min()
        options_df = nfo_df[nfo_df["expiry_dt"] == nearest_expiry].copy()
        options_df["token"] = options_df["token"].astype(str)
        return options_df.sort_values(["expiry_dt", "symbol"]).reset_index(drop=True)
    except Exception as e:
        print(f"Error loading NFO options for {base_name}: {e}")
        return pd.DataFrame()


def load_option_contracts(base_name, expiry_count=3):
    try:
        df = _load_scrip_master_df()
        today = datetime.now(IST).replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=None)

        options_df = df[df["name"] == base_name].copy()
        options_df = options_df[options_df["expiry_dt"] >= today].copy()
        options_df = options_df[
            options_df["instrumenttype"].isin(["OPTIDX", "OPTFUT"]) |
            options_df["symbol"].astype(str).str.contains("CE|PE", na=False)
        ].copy()
        if options_df.empty:
            return pd.DataFrame()

        expiries = sorted(options_df["expiry_dt"].dropna().unique())[:expiry_count]
        if not expiries:
            return pd.DataFrame()

        labels = ["NEAR", "NEXT", "MONTH"]
        expiry_buckets = {expiry: labels[idx] if idx < len(labels) else f"EXP{idx + 1}" for idx, expiry in enumerate(expiries)}

        options_df = options_df[options_df["expiry_dt"].isin(expiries)].copy()
        options_df["token"] = options_df["token"].astype(str)
        options_df["lotsize"] = pd.to_numeric(options_df["lotsize"], errors="coerce").fillna(0).astype(int)
        options_df["strike_value"] = pd.to_numeric(options_df["strike"], errors="coerce").fillna(0.0) / 100.0
        options_df["option_type"] = options_df["symbol"].astype(str).str.extract(r"(CE|PE)", expand=False)
        options_df["expiry_bucket"] = options_df["expiry_dt"].map(expiry_buckets)
        return options_df.sort_values(["expiry_dt", "strike_value", "symbol"]).reset_index(drop=True)
    except Exception as e:
        print(f"Error loading option contracts for {base_name}: {e}")
        return pd.DataFrame()


def resolve_underlying_instrument(base_name):
    try:
        df = _load_scrip_master_df()
        today = datetime.now(IST).replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=None)

        if base_name == "CRUDEOIL":
            fut_df = df[df["name"] == base_name].copy()
            fut_df = fut_df[fut_df["instrumenttype"] == "FUTCOM"].copy()
            fut_df = fut_df[fut_df["expiry_dt"] >= today].copy()
            if fut_df.empty:
                return None
            row = fut_df.sort_values(["expiry_dt", "symbol"]).iloc[0]
            return {
                "token": str(row["token"]),
                "symbol": str(row["symbol"]),
                "exchange": "MCX",
                "exchange_type": 5,
                "source": "FUTURE",
            }

        candidates = df[df["name"] == base_name].copy()
        if candidates.empty:
            return None

        preferred = candidates[
            (candidates["symbol"].astype(str) == base_name) &
            candidates["exch_seg"].astype(str).isin(["NSE", "BSE"])
        ].copy()
        if preferred.empty:
            preferred = candidates[
                candidates["instrumenttype"].astype(str).isin(["AMXIDX", ""])
            ].copy()
        if preferred.empty:
            return None

        row = preferred.sort_values(["exch_seg", "symbol"]).iloc[0]
        exchange = str(row["exch_seg"])
        exchange_type = 1 if exchange == "NSE" else 3 if exchange == "BSE" else None
        if exchange_type is None:
            return None

        return {
            "token": str(row["token"]),
            "symbol": str(row["symbol"]),
            "exchange": exchange,
            "exchange_type": exchange_type,
            "source": "SPOT",
        }
    except Exception as e:
        print(f"Error resolving underlying instrument for {base_name}: {e}")
        return None


def resolve_nfo_option(base_name, strike, option_type):
    options_df = load_nfo_options_for_name(base_name)
    if options_df.empty:
        return None

    pattern = f"{int(strike)}{option_type.upper()}"
    matches = options_df[options_df["symbol"].astype(str).str.contains(pattern, na=False)].copy()
    if matches.empty:
        return None

    row = matches.iloc[0]
    return {
        "symbol": str(row["symbol"]),
        "token": str(row["token"]),
        "name": str(row["name"]),
        "exch_seg": str(row["exch_seg"]),
        "expiry": row["expiry_dt"].strftime("%d%b%Y").upper() if pd.notna(row["expiry_dt"]) else "",
    }


def fetch_candle_df(smart, exchange, symboltoken, interval, from_dt, to_dt):
    params = {
        "exchange": exchange,
        "symboltoken": str(symboltoken),
        "interval": interval,
        "fromdate": from_dt.strftime("%Y-%m-%d %H:%M"),
        "todate": to_dt.strftime("%Y-%m-%d %H:%M"),
    }
    response = smart.getCandleData(params)
    rows = (response or {}).get("data") or []
    if not rows:
        return pd.DataFrame(columns=["timestamp", "open", "high", "low", "close", "volume"])

    df = pd.DataFrame(rows, columns=["timestamp", "open", "high", "low", "close", "volume"])
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    try:
        if getattr(df["timestamp"].dt, "tz", None) is not None:
            df["timestamp"] = df["timestamp"].dt.tz_convert(IST).dt.tz_localize(None)
    except Exception:
        pass
    for column in ["open", "high", "low", "close", "volume"]:
        df[column] = pd.to_numeric(df[column], errors="coerce")
    df = df.dropna(subset=["timestamp", "open", "high", "low", "close"]).sort_values("timestamp")
    return df.reset_index(drop=True)


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
