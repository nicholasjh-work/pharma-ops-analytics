"""
CMS Medicare Part D Data Ingestion Pipeline

Downloads real CMS public use files, filters to pharmacy-relevant
therapeutic areas, and loads into PostgreSQL (local or Supabase).

Usage:
    python scripts/ingest.py --target local
    python scripts/ingest.py --target supabase
    python scripts/ingest.py --download-only

Data sources (all public, no auth required):
    - Medicare Part D Spending by Drug (2023)
    - Medicare Part D Prescribers by Provider (2023)
    - Medicare Part D Prescribers by Provider and Drug (2023)
"""

import logging
import os
import sys
from pathlib import Path
from typing import Optional

import pandas as pd
import sqlalchemy as sa
from dotenv import load_dotenv

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

load_dotenv()

PROJECT_ROOT = Path(__file__).resolve().parent.parent
RAW_DIR = PROJECT_ROOT / "data" / "raw"
PROCESSED_DIR = PROJECT_ROOT / "data" / "processed"
SEEDS_DIR = PROJECT_ROOT / "seeds"

# ── CMS data URLs ────────────────────────────────────────────────────
# These are the official data.cms.gov download links.
# File naming convention: MUP_DPR_RY{release_year}_P04_V10_DY{data_year}
CMS_DATASETS = {
    "part_d_spending_2023": {
        "url": (
            "https://data.cms.gov/sites/default/files/2025-05/"
            "DSD_PTD_RY25_P04_V10_DY23_BGM.csv"
        ),
        "filename": "part_d_spending_2023.csv",
        "table": "raw_part_d_spending",
        "description": "Drug-level spending: brand/generic, total cost, claims, beneficiaries",
    },
    "prescribers_summary_2023": {
        "url": (
            "https://data.cms.gov/sites/default/files/2025-05/"
            "MUP_DPR_RY25_P04_V10_DY23_NPI.csv"
        ),
        "filename": "prescribers_summary_2023.csv",
        "table": "raw_prescribers",
        "description": "Per-prescriber summary: NPI, specialty, state, total claims/cost",
    },
    "prescribers_by_drug_2023": {
        "url": (
            "https://data.cms.gov/sites/default/files/2025-05/"
            "MUP_DPR_RY25_P04_V10_DY23_NPIBN.csv"
        ),
        "filename": "prescribers_by_drug_2023.csv",
        "table": "raw_prescribers_drug",
        "description": "Per-prescriber per-drug detail: NPI, drug, claims, day supply, cost",
    },
}

# Prescriber specialties relevant to Gifthealth therapeutic areas
TARGET_SPECIALTIES = [
    "Gastroenterology",
    "Internal Medicine",
    "Family Practice",
    "Endocrinology, Diabetes & Metabolism",
    "Hematology/Oncology",
    "Rheumatology",
    "Allergy/Immunology",
    "Nurse Practitioner",
    "Physician Assistant",
    "General Practice",
    "Dermatology",
]


def download_file(url: str, dest: Path) -> Path:
    """Download a file if not already cached locally."""
    if dest.exists():
        size_mb = dest.stat().st_size / (1024 * 1024)
        log.info(f"Already cached: {dest.name} ({size_mb:.1f} MB)")
        return dest

    log.info(f"Downloading {dest.name} from CMS...")
    log.info(f"  URL: {url}")

    import urllib.request

    dest.parent.mkdir(parents=True, exist_ok=True)
    urllib.request.urlretrieve(url, dest)
    size_mb = dest.stat().st_size / (1024 * 1024)
    log.info(f"  Saved: {dest.name} ({size_mb:.1f} MB)")
    return dest


def load_therapeutic_map() -> set[str]:
    """Load the set of generic drug names from the therapeutic area seed."""
    seed_path = SEEDS_DIR / "seed_therapeutic_area_map.csv"
    df = pd.read_csv(seed_path)
    names = set(df["generic_name"].str.upper().str.strip())
    log.info(f"Loaded {len(names)} generic names from therapeutic area map")
    return names


def process_spending(path: Path) -> pd.DataFrame:
    """Load Part D spending data. Small file, no filtering needed."""
    log.info("Processing Part D spending data...")
    df = pd.read_csv(path)
    log.info(f"  Rows: {len(df):,}")
    log.info(f"  Columns: {list(df.columns)}")

    # Standardize column names to snake_case
    df.columns = [c.strip().lower().replace(" ", "_").replace("/", "_") for c in df.columns]

    return df


def process_prescribers(path: Path) -> pd.DataFrame:
    """Load prescriber summary, filter to target specialties."""
    log.info("Processing prescriber summary data...")
    log.info("  Reading CSV (may take a minute for ~1M rows)...")

    df = pd.read_csv(path, low_memory=False)
    total = len(df)
    log.info(f"  Total rows: {total:,}")

    # Standardize column names
    df.columns = [c.strip().lower().replace(" ", "_").replace("/", "_") for c in df.columns]

    # Identify the specialty column (CMS naming varies by release year)
    specialty_col = None
    for candidate in ["prscrbr_type", "provider_type", "specialty_description"]:
        if candidate in df.columns:
            specialty_col = candidate
            break

    if specialty_col:
        df_filtered = df[df[specialty_col].isin(TARGET_SPECIALTIES)].copy()
        log.info(
            f"  Filtered to {len(df_filtered):,} rows "
            f"({len(df_filtered)/total*100:.1f}%) from {len(TARGET_SPECIALTIES)} specialties"
        )
        return df_filtered

    log.warning("  Could not identify specialty column. Loading all rows.")
    return df


def process_prescribers_by_drug(path: Path, drug_names: set[str]) -> pd.DataFrame:
    """
    Load prescriber-by-drug detail and filter to target drugs.
    This is the large file (~25M rows). We read in chunks to manage memory.
    """
    log.info("Processing prescriber-by-drug data (large file, chunked read)...")
    chunks = []
    rows_total = 0
    rows_kept = 0
    chunk_size = 500_000

    for i, chunk in enumerate(pd.read_csv(path, chunksize=chunk_size, low_memory=False)):
        rows_total += len(chunk)

        # Standardize column names
        chunk.columns = [
            c.strip().lower().replace(" ", "_").replace("/", "_") for c in chunk.columns
        ]

        # Find the generic name column
        gnrc_col = None
        for candidate in ["gnrc_name", "generic_name", "gnrc_drug_name"]:
            if candidate in chunk.columns:
                gnrc_col = candidate
                break

        if gnrc_col:
            mask = chunk[gnrc_col].str.upper().str.strip().isin(drug_names)
            filtered = chunk[mask]
        else:
            filtered = chunk

        if len(filtered) > 0:
            chunks.append(filtered)
            rows_kept += len(filtered)

        if (i + 1) % 10 == 0:
            log.info(
                f"  Chunk {i+1}: {rows_total:,} read, {rows_kept:,} kept "
                f"({rows_kept/max(rows_total,1)*100:.1f}%)"
            )

    df = pd.concat(chunks, ignore_index=True) if chunks else pd.DataFrame()
    log.info(f"  Final: {rows_kept:,} / {rows_total:,} rows kept ({rows_kept/max(rows_total,1)*100:.1f}%)")
    return df


def get_engine(target: str) -> sa.Engine:
    """Create SQLAlchemy engine for the chosen target."""
    if target == "local":
        host = os.getenv("PG_HOST", "localhost")
        user = os.getenv("PG_USER", "demo_user")
        password = os.getenv("PG_PASSWORD", "")
        url = f"postgresql://{user}:{password}@{host}:5432/pharma_ops"
    elif target == "supabase":
        host = os.getenv("SUPABASE_HOST")
        user = os.getenv("SUPABASE_USER", "postgres")
        password = os.getenv("SUPABASE_PASSWORD")
        if not host or not password:
            log.error("SUPABASE_HOST and SUPABASE_PASSWORD must be set in .env")
            sys.exit(1)
        url = f"postgresql://{user}:{password}@{host}:5432/postgres"
    else:
        log.error(f"Unknown target: {target}")
        sys.exit(1)

    return sa.create_engine(url, echo=False)


def load_to_postgres(df: pd.DataFrame, table: str, engine: sa.Engine) -> None:
    """Load a DataFrame into PostgreSQL, replacing existing data."""
    log.info(f"  Loading {len(df):,} rows into {table}...")
    df.to_sql(table, engine, if_exists="replace", index=False, method="multi", chunksize=5000)
    log.info(f"  Done: {table}")


def main(target: Optional[str] = None, download_only: bool = False) -> None:
    """Run the full ingestion pipeline."""
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

    # Step 1: Download all CMS files
    log.info("=" * 60)
    log.info("STEP 1: Download CMS datasets")
    log.info("=" * 60)
    paths = {}
    for key, meta in CMS_DATASETS.items():
        dest = RAW_DIR / meta["filename"]
        paths[key] = download_file(meta["url"], dest)

    if download_only:
        log.info("Download complete. Exiting (--download-only).")
        return

    # Step 2: Load therapeutic area map for filtering
    log.info("=" * 60)
    log.info("STEP 2: Load therapeutic area filter")
    log.info("=" * 60)
    drug_names = load_therapeutic_map()

    # Step 3: Process each dataset
    log.info("=" * 60)
    log.info("STEP 3: Process datasets")
    log.info("=" * 60)

    df_spending = process_spending(paths["part_d_spending_2023"])
    df_prescribers = process_prescribers(paths["prescribers_summary_2023"])
    df_prescribers_drug = process_prescribers_by_drug(
        paths["prescribers_by_drug_2023"], drug_names
    )

    # Save processed files locally for reference
    df_spending.to_csv(PROCESSED_DIR / "spending_processed.csv", index=False)
    df_prescribers.to_csv(PROCESSED_DIR / "prescribers_processed.csv", index=False)
    df_prescribers_drug.to_csv(PROCESSED_DIR / "prescribers_drug_processed.csv", index=False)
    log.info("Processed CSVs saved to data/processed/")

    # Step 4: Load into PostgreSQL
    if target:
        log.info("=" * 60)
        log.info(f"STEP 4: Load into PostgreSQL ({target})")
        log.info("=" * 60)
        engine = get_engine(target)

        load_to_postgres(df_spending, "raw_part_d_spending", engine)
        load_to_postgres(df_prescribers, "raw_prescribers", engine)
        load_to_postgres(df_prescribers_drug, "raw_prescribers_drug", engine)

        log.info("All tables loaded.")
    else:
        log.info("No --target specified. Processed CSVs saved but not loaded to DB.")

    log.info("=" * 60)
    log.info("INGESTION COMPLETE")
    log.info("=" * 60)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="CMS Medicare Part D data ingestion")
    parser.add_argument(
        "--target",
        choices=["local", "supabase"],
        default=None,
        help="Database target for loading",
    )
    parser.add_argument(
        "--download-only",
        action="store_true",
        help="Download CSVs without processing or loading",
    )
    args = parser.parse_args()

    main(target=args.target, download_only=args.download_only)
