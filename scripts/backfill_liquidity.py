#!/usr/bin/env python3
"""
Back‑fill the USD‑liquidity index from FRED for the past year and
sync anything missing to both the CSV and the Notion database.

Run **once** (locally or via a manual GitHub‑Action dispatch).
"""
import os, datetime as dt, pandas as pd
from pathlib import Path
from fredapi import Fred
from notion_client import Client
from pandas.errors import EmptyDataError

# ---------- ENV ----------
FRED_KEY  = os.environ["FRED_API_KEY"]
NOTION_DB = os.environ["NOTION_DB_ID"]
NOTION_TK = os.environ["NOTION_TOKEN"]

# ---------- PATHS ----------
root     = Path(__file__).resolve().parents[1]
csv_path = root / "data" / "liquidity_history.csv"
csv_path.parent.mkdir(exist_ok=True)

cols = ["Date", "WALCL", "ON_RRP", "TGA", "Net_Liquidity"]

try:
    df_hist = pd.read_csv(csv_path, parse_dates=["Date"])
except (FileNotFoundError, EmptyDataError):
    df_hist = pd.DataFrame(columns=cols)

already_have = set(df_hist["Date"].dt.date)

# ---------- FETCH ----------
fred   = Fred(api_key=FRED_KEY)
today  = dt.date.today()
start  = today - dt.timedelta(days=400)   # extra buffer for holidays

series = {
    "WALCL":      fred.get_series("WALCL",      observation_start=start),
    "ON_RRP":     fred.get_series("RRPONTSYD",  observation_start=start),
    "TGA":        fred.get_series("WTREGEN",    observation_start=start),
}

# align series to calendar days (forward‑fill weekly values)
df_all = (
    pd.concat(series, axis=1)
      .ffill()                       # forward‑fill weekly obs
      .dropna()                      # keep rows where all 3 present
)
df_all.index.name = "Date"
df_all.reset_index(inplace=True)

df_all["Net_Liquidity"] = (
    df_all["WALCL"] - df_all["ON_RRP"] - df_all["TGA"]
)

# ---------- KEEP ONLY NEW ROWS ----------
df_new = df_all[~df_all["Date"].dt.date.isin(already_have)].copy()
if df_new.empty:
    print("Nothing new to back‑fill; CSV already up to date.")
    exit(0)

# ---------- APPEND CSV ----------
df_full = (
    pd.concat([df_hist, df_new], ignore_index=True)
      .drop_duplicates(subset=["Date"])
      .sort_values("Date")
)
df_full.to_csv(csv_path, index=False)
print(f"✅  Added {len(df_new)} rows to {csv_path.relative_to(root)}")

# ---------- PUSH TO NOTION ----------
notion = Client(auth=NOTION_TK)

for _, row in df_new.iterrows():
    notion.pages.create(
        parent={"database_id": NOTION_DB},
        properties={
            "Date":          {"date":   {"start": row["Date"].date().isoformat()}},
            "WALCL":         {"number": float(row["WALCL"])},
            "ON RRP":        {"number": float(row["ON_RRP"])},
            "TGA":           {"number": float(row["TGA"])},
            "Net Liquidity": {"number": float(row["Net_Liquidity"])},
        },
    )
print("✅  Back‑fill rows pushed to Notion")
