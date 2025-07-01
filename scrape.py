"""
scrape.py  –  fetch latest 13-F for Scion Asset Management (Michael Burry)
and save holdings.parquet.

GitHub secrets required:
  SEC_EMAIL   – your contact e-mail (already added)
  SEC_COMPANY – project / company name (e.g. "Burry Tracker")
"""

import os
import duckdb, pandas as pd
from sec_edgar_downloader import Downloader

# ── CONFIG ─────────────────────────────────────────────────────────
DOWNLOAD_DIR = "data"
CIK          = "0001166559"                         # Scion Asset Management
EMAIL        = os.getenv("SEC_EMAIL")               # MUST exist
COMPANY      = os.getenv("SEC_COMPANY", "Personal Project")
# ───────────────────────────────────────────────────────────────────

if not EMAIL:
    raise RuntimeError("GitHub secret SEC_EMAIL is missing.")

dl = Downloader(
    download_folder=DOWNLOAD_DIR,
    email_address=EMAIL,
    company_name=COMPANY,
)

print("Downloading 13-F filings …")
# positional args only: filing_type, cik
dl.get("13F-HR", CIK)          # ← no amount= anymore

# gather *all* locally downloaded 13-F filings for that CIK
filings = list(dl.get_filings(CIK, "13F-HR"))
if not filings:
    raise RuntimeError("No 13-F filings found after download.")

# pick the most-recent filing by date
latest = max(filings, key=lambda f: f.filing_date)
print("Using filing dated", latest.filing_date)

rows = [
    {
        "ticker":      h["ticker"],
        "value":       h["value"],
        "shares":      h["shares"],
        "filing_date": latest.filing_date,
    }
    for h in latest.get_holdings()
]

df = pd.DataFrame(rows)
duckdb.write_parquet(df, "holdings.parquet")
print(f"✅  Saved holdings.parquet with {len(df):,} rows")
