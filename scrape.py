# scrape.py — downloads latest 13-F for Michael Burry (Scion Asset Mgmt)
import duckdb, pandas as pd, datetime as dt, sys, os
from sec_edgar_downloader import Downloader

CIK = "0001166559"        # Scion’s SEC identifier
dl = Downloader("data")   # saves raw files under /data

# get the most recent 13-F
dl.get("13F-HR", CIK, amount=1)

rows = []
for filing in dl.get_filings(CIK, "13F-HR"):
    holdings = filing.get_holdings()
    for h in holdings:
        rows.append({
            "ticker": h["ticker"],
            "value": h["value"],
            "shares": h["shares"],
            "filing_date": filing.filing_date
        })

df = pd.DataFrame(rows)
duckdb.write_parquet(df, "holdings.parquet")   # tiny binary file
print("Saved holdings.parquet with", len(df), "rows")
