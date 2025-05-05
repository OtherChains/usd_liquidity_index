#!/usr/bin/env python3
import os, datetime as dt, pandas as pd
from pathlib import Path              # ← NEW
from fredapi import Fred
from notion_client import Client

# --- ENV ---
FRED_KEY  = os.environ["FRED_API_KEY"]          # store as GH secret
NOTION_DB = os.environ["NOTION_DB_ID"]          # store as GH secret
NOTION_TK = os.environ["NOTION_TOKEN"]          # store as GH secret

# --- 1. Fetch data ---
fred = Fred(api_key=FRED_KEY)
today = dt.date.today()

def last_obs(series):
    """Return latest observation as float."""
    return float(fred.get_series_latest_release(series).iloc[-1])

walcl  = last_obs("WALCL")       # Fed balance‑sheet assets
rrp    = last_obs("RRPONTSYD")   # Overnight RRP
tga    = last_obs("WTREGEN")     # Treasury General Account
index  = walcl - rrp - tga

# --- 2. Append to local CSV ---
csv_path = Path(__file__).resolve().parents[1] / "data/liquidity_history.csv"
df = pd.read_csv(csv_path)
new_row = pd.DataFrame(
    [[today.isoformat(), walcl, rrp, tga, index]],
    columns=df.columns,
)
df = pd.concat([df, new_row], ignore_index=True)
df.to_csv(csv_path, index=False)

# --- 3. Push to Notion ---
notion = Client(auth=NOTION_TK)
notion.pages.create(
    parent={"database_id": NOTION_DB},
    properties={
        "Date":      {"date": {"start": today.isoformat()}},
        "WALCL":     {"number": walcl},
        "ON RRP":    {"number": rrp},
        "TGA":       {"number": tga},
        "Net Liquidity": {"number": index},
    },
)
