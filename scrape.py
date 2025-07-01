"""
scrape.py  —  downloads the most-recent 13-F for Scion Asset Management
and saves holdings.parquet.

Requires two GitHub secrets:
  SEC_EMAIL   – your contact e-mail (already added)
  SEC_COMPANY – short project / company name (e.g. "Burry Tracker")
"""

import os
import duckdb, pandas as pd
from sec_edgar_downloader import Downloader

# ─── CONFIG ──────────────────────────────────────────────────────────
DOWNLOAD_DIR = "data"
CIK          = "0001166559"                       # Scion Asset Management
EMAIL        = os.getenv("SEC_EMAIL")             # MUST exist
COMPANY      = os.getenv("SEC_COMPANY", "Personal Project")
# ─────────────────────────────────────────────────────────────────────

if not EMAIL:
    raise RuntimeError("GitHub secret SEC_EMAIL is missing.")

# Create downloader (library now needs both e-mail & company)
dl = Downloader(download_folder=DOWNLOAD_DIR,
                email_address=EMAIL,
                company_name=COMPANY)

print("Fetching most-recent 13-F …")
# ----► NOTE: positional args, not keywords ◄----
dl.get("13F-HR", CIK, amount=1)

rows = []
# ----► same here: positional order ◄----
for filing in dl.get_filings(CIK, "13F-HR"):
    for h in filing.get_holdings():
        rows.append(dict(
            ticker      = h["ticker"],
            value       = h["value"],
            shares      = h["shares"],
            filing_date = filing.filing_date,
        ))

df = pd.DataFrame(rows)
if df.empty:
    raise RuntimeError("No holdings parsed – check download succeeded.")

duckdb.write_parquet(df, "holdings.parquet")
print(f"✅  Saved holdings.parquet with {len(df):,} rows")
