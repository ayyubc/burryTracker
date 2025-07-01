"""
scrape.py
~~~~~~~~~
Downloads the most-recent 13-F filing for Michael Burry’s
Scion Asset Management, converts it to a tidy table, and saves
`holdings.parquet` in the repo root.

SEC’s new policy (2024-04) requires a contact e-mail address
for automated scrapers, so we read it from the GitHub secret
`SEC_EMAIL`.

Dependencies (already listed in requirements.txt):
  pandas, duckdb, sec-edgar-downloader
"""

import os
import duckdb
import pandas as pd
from sec_edgar_downloader import Downloader

# ---------- CONFIG ----------------------------------------------------------

DOWNLOAD_DIR = "data"                     # where raw filings land
CIK = "0001166559"                        # Scion Asset Management
EMAIL = os.getenv("SEC_EMAIL")            # set in GitHub → Settings → Secrets

# ---------------------------------------------------------------------------

if not EMAIL:
    raise RuntimeError(
        "Environment variable SEC_EMAIL is missing.\n"
        "Add it as a GitHub secret: Settings → Secrets → Actions → New secret."
    )

# create downloader
dl = Downloader(download_folder=DOWNLOAD_DIR, email_address=EMAIL)

# fetch ONE most-recent 13-F filing (SEC form code: 13F-HR)
print("Downloading latest 13-F for CIK", CIK)
dl.get(form="13F-HR", cik=CIK, amount=1)

# parse all 13-F filings in DOWNLOAD_DIR for this CIK
rows = []
for filing in dl.get_filings(cik=CIK, form="13F-HR"):
    for h in filing.get_holdings():
        rows.append(
            {
                "ticker": h["ticker"],
                "value": h["value"],
                "shares": h["shares"],
                "filing_date": filing.filing_date,
            }
        )

df = pd.DataFrame(rows)
if df.empty:
    raise RuntimeError("No holdings parsed — check that the filing downloaded ok.")

# write a compact columnar file (few KB) that Streamlit can read fast
duckdb.write_parquet(df, "holdings.parquet")
print(f"Saved holdings.parquet with {len(df):,} rows")
