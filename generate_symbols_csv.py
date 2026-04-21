import pandas as pd
import requests
import os

SCRIP_MASTER_URL = "https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json"
SCRIP_MASTER_FILE = "OpenAPIScripMaster.json"
OUTPUT_CSV = "symbols.csv"

# The symbols you are interested in
TARGET_SYMBOLS = ["CRUDEOIL", "GOLD", "SILVER", "NATURALGAS"]

def download_scrip_master():
    print("Downloading latest Scrip Master from Angel One... (this might take a minute)")
    try:
        response = requests.get(SCRIP_MASTER_URL, timeout=60)
        if response.status_code == 200:
            with open(SCRIP_MASTER_FILE, "wb") as f:
                f.write(response.content)
            print("Download complete.")
        else:
            print(f"Failed to download. Status: {response.status_code}")
            return False
    except Exception as e:
        print(f"Error downloading: {e}")
        return False
    return True

def generate_csv():
    if not os.path.exists(SCRIP_MASTER_FILE):
        if not download_scrip_master():
            return

    print(f"Processing {SCRIP_MASTER_FILE}...")
    try:
        # Load the large JSON file
        df = pd.read_json(SCRIP_MASTER_FILE)
        
        # 1. Filter for MCX exchange
        # 2. Filter for your target names (CRUDEOIL, etc.)
        filtered_df = df[
            (df['exch_seg'] == 'MCX') & 
            (df['name'].isin(TARGET_SYMBOLS))
        ].copy()

        # Optional: Filter for only Futures and Options (exclude others if any)
        # We look for symbols containing FUT, CE, or PE
        filtered_df = filtered_df[
            filtered_df['symbol'].str.contains('FUT|CE|PE')
        ]

        if filtered_df.empty:
            print("No matching symbols found. Check your TARGET_SYMBOLS.")
            return

        # Select and rename columns to match what our scanner expects
        # Required: token, symbol, name, lotsize, exch_seg
        final_df = filtered_df[['token', 'symbol', 'name', 'lotsize', 'exch_seg']]
        
        # Save to CSV
        final_df.to_csv(OUTPUT_CSV, index=False)
        print(f"Successfully created {OUTPUT_CSV} with {len(final_df)} instruments!")
        print("You can now open this file in Excel to see the tokens.")

    except Exception as e:
        print(f"Error processing data: {e}")

if __name__ == "__main__":
    generate_csv()
