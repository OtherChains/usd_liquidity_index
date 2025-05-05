#!/usr/bin/env python3
import os, datetime as dt, pandas as pd
from pandas.errors import EmptyDataError          # ← import
from pathlib import Path
from fredapi import Fred
from notion_client import Client

# ---------- env ----------
FRED_KEY  = os.environ["FRED_API_KEY"]
NOTION_DB = os.environ["NOTION_DB_ID"]
NOTION_TK = os.environ["NOTION_TOKEN"]

root = Path(__file__).resolve().parents[1]          # repo root
csv_path = root / "data" / "liquidity_history.csv"  # <-- DEFINED HERE
csv_path.parent.mkdir(exist_ok=True)   
cols = ["Date", "WALCL", "ON_RRP", "TGA", "Net_Liquidity"]

try:
    df = pd.read_csv(csv_path)
except (FileNotFoundError, EmptyDataError):
    df = pd.DataFrame(columns=cols)

# ---------- paths ----------
root = Path(__file__).resolve().parents[1]
csv_path = root / "data" / "liquidity_history.csv"
csv_path.parent.mkdir(exist_ok=True)          # create data/ if missing

# ---------- load or init ----------
cols = ["Date", "WALCL", "ON_RRP", "TGA", "Net_Liquidity"]
if csv_path.exists():
    df = pd.read_csv(csv_path)
else:
    df = pd.DataFrame(columns=cols)

# ---------- fetch latest ----------
fred = Fred(api_key=FRED_KEY)

def latest(series_id):
    return float(fred.get_series_latest_release(series_id).iloc[-1])

today  = dt.date.today().isoformat()
walcl  = latest("WALCL")
rrp    = latest("RRPONTSYD")
tga    = latest("WTREGEN")
netliq = walcl - rrp - tga

# ---------- append & save ----------
new_row = pd.DataFrame([[today, walcl, rrp, tga, netliq]], columns=cols)
df = pd.concat([df, new_row], ignore_index=True).drop_duplicates(subset=["Date"])
df.to_csv(csv_path, index=False)

# ---------- push to Notion ----------
notion = Client(auth=NOTION_TK)
notion.pages.create(
    parent={"database_id": NOTION_DB},
    properties={
        "Date":           {"date":   {"start": today}},
        "WALCL":          {"number": walcl},
        "ON RRP":         {"number": rrp},
        "TGA":            {"number": tga},
        "Net Liquidity":  {"number": netliq},
    },
)
