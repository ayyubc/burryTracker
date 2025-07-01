"""
scrape.py – downloads the latest 13-F for Scion Asset Management
and writes holdings.parquet

Requires ONE GitHub secret:
  SEC_EMAIL  – your contact e-mail
"""

import os, duckdb, pandas as pd
from sec_edgar_downloader import Downloader

CIK = "0001166559"                       # Scion (Michael Burry)
EMAIL = os.getenv("SEC_EMAIL")           # pulled from GitHub secret
if not EMAIL:
    raise RuntimeError("SEC_EMAIL secret missing")

# v4.3.0 only needs email_address
dl = Downloader(download_folder="data", email_address=EMAIL)

print("Downloading latest 13-F …")
dl.get(form="13F-HR", cik=CIK, amount=1)          # keyword args work

rows = []
for filing in dl.get_filings(cik=CIK, form="13F-HR"):
    for h in filing.get_holdings():
        rows.append(
            {
                "ticker": h["ticker"],
                "value":  h["value"],
                "shares": h["shares"],
                "filing_date": filing.filing_date,
            }
        )

df = pd.DataFrame(rows)
if df.empty:
    raise RuntimeError("Parsed zero rows -- download may have failed")

duckdb.write_parquet(df, "holdings.parquet")
print(f"✅  Saved holdings.parquet with {len(df):,} rows")
