"""
scrape.py – downloads the latest 13-F for Scion Asset Management
and writes holdings.parquet

Needs TWO GitHub secrets:
  SEC_EMAIL   – your contact e-mail
  SEC_COMPANY – short project name (e.g. "Burry Tracker")
"""

import os, duckdb, pandas as pd
from sec_edgar_downloader import Downloader

CIK = "0001166559"     # Scion Asset Mgmt (Michael Burry)

EMAIL   = os.getenv("SEC_EMAIL")
COMPANY = os.getenv("SEC_COMPANY", "Personal Project")
if not EMAIL:
    raise RuntimeError("SEC_EMAIL secret missing")

dl = Downloader(
    download_folder="data",
    email_address=EMAIL,
    company_name=COMPANY,
)

print("Downloading 13-F filings …")
# keyword args work on v4.x
dl.get(form="13F-HR", cik=CIK, amount=1)

rows = []
for filing in dl.get_filings(cik=CIK, form="13F-HR"):
    for h in filing.get_holdings():
        rows.append(
            dict(
                ticker=h["ticker"],
                value=h["value"],
                shares=h["shares"],
                filing_date=filing.filing_date,
            )
        )

df = pd.DataFrame(rows)
if df.empty:
    raise RuntimeError("Parsed zero rows – download may have failed")

duckdb.write_parquet(df, "holdings.parquet")
print(f"✅  Saved holdings.parquet with {len(df):,} rows")
