"""
scrape.py  —  downloads the latest 13-F for Scion Asset Mgmt (Michael Burry)
and saves holdings.parquet.

Requires two GitHub secrets:
  SEC_EMAIL   – your contact e-mail (already added)
  SEC_COMPANY – short project / company name (e.g. "Burry Tracker")
"""

import os
import duckdb, pandas as pd
from sec_edgar_downloader import Downloader

# ---------- configuration ---------- #
DOWNLOAD_DIR = "data"
CIK          = "0001166559"                       # Scion Asset Management

EMAIL   = os.getenv("SEC_EMAIL")                  # must exist
COMPANY = os.getenv("SEC_COMPANY", "Personal Project")  # fallback if not set
# ----------------------------------- #

if not EMAIL:
    raise RuntimeError(
        "SEC_EMAIL is missing – add it as a GitHub secret "
        "(Settings → Secrets → Actions → New secret)."
    )

# pass BOTH email_address and company_name – new library requirement
dl = Downloader(
    download_folder=DOWNLOAD_DIR,
    email_address=EMAIL,
    company_name=COMPANY,
)

print("Fetching most-recent 13-F …")
dl.get(form="13F-HR", cik=CIK, amount=1)

rows = []
for filing in dl.get_filings(cik=CIK, form="13F-HR"):
    for h in filing.get_holdings():
        rows.append(
            dict(
                ticker      = h["ticker"],
                value       = h["value"],
                shares      = h["shares"],
                filing_date = filing.filing_date,
            )
        )

df = pd.DataFrame(rows)
if df.empty:
    raise RuntimeError("No holdings parsed – did the filing download correctly?")

duckdb.write_parquet(df, "holdings.parquet")
print(f"✅  Saved holdings.parquet with {len(df):,} rows")
