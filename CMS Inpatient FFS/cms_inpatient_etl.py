"""
CMS Medicare Inpatient FFS - ETL Pipeline
Data: Medicare Inpatient Hospitals by Provider and Service (2023)
Author: Toni
"""

import pandas as pd
import numpy as np
import psycopg2
from sqlalchemy import create_engine
import logging
import os

# ── Logging setup ─────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────
CSV_PATH = "/Users/toniventura/Downloads/CMS_Inpatient_FFS/2023/MUP_INP_RY25_P03_V10_DY23_PrvSvc.csv"   # update path as needed

DB_CONFIG = {
    "host":     "localhost",
    "port":     5432,
    "database": "cms_analytics",
    "user":     "toniventura",          # update or use env var
    "password": "cms2023"            # update or use env var
}

# ── 1. EXTRACT ────────────────────────────────────────────────────────────────
def extract(path: str) -> pd.DataFrame:
    log.info(f"Loading CSV: {path}")
    df = pd.read_csv(path, dtype=str, low_memory=False, encoding="latin-1")  # read all as str first
    log.info(f"Loaded {len(df):,} rows, {len(df.columns)} columns")
    return df


# ── 2. TRANSFORM ──────────────────────────────────────────────────────────────
def transform(df: pd.DataFrame) -> pd.DataFrame:
    log.info("Starting transformations...")

    # ── 2a. Rename columns to snake_case ──────────────────────────────────────
    rename_map = {
        "Rndrng_Provdr_CCN":        "provider_ccn",
        "Rndrng_Prvdr_Org_Name":    "provider_name",
        "Rndrng_Prvdr_City":        "city",
        "Rndrng_Prvdr_St":          "state_fips",
        "Rndrng_Prvdr_State_FIPS":  "state_fips_code",
        "Rndrng_Prvdr_Zip5":        "zip5",
        "Rndrng_Prvdr_Stat_Abrvtn": "state_abbr",
        "Rndrng_Prvdr_RUCA":        "ruca_code",
        "Rndrng_Prvdr_RUCA_Desc":   "ruca_desc",
        "DRG_Cd":                   "drg_code",
        "DRG_Desc":                 "drg_desc",
        "Tot_Dschrgs":              "total_discharges",
        "Avg_Submtd_Cvrd_Chrg":     "avg_submitted_charge",
        "Avg_Tot_Pymt_Amt":         "avg_total_payment",
        "Avg_Mdcr_Pymt_Amt":        "avg_medicare_payment",
    }
    df = df.rename(columns=rename_map)

    # ── 2b. Cast numeric columns ───────────────────────────────────────────────
    numeric_cols = [
        "total_discharges",
        "avg_submitted_charge",
        "avg_total_payment",
        "avg_medicare_payment",
    ]
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col].str.replace(",", ""), errors="coerce")

    # ── 2c. Zero-pad zip codes (handle dropped leading zeros) ─────────────────
    df["zip5"] = df["zip5"].str.strip().str.zfill(5)

    # ── 2d. DRG code as zero-padded string (e.g., '001') ──────────────────────
    df["drg_code"] = df["drg_code"].str.strip().str.zfill(3)

    # ── 2e. RUCA code as numeric ───────────────────────────────────────────────
    df["ruca_code"] = pd.to_numeric(df["ruca_code"], errors="coerce")

    # ── 2f. Derived / calculated fields ───────────────────────────────────────
    # Charge-to-payment ratio (key risk signal)
    df["charge_to_payment_ratio"] = (
        df["avg_submitted_charge"] / df["avg_medicare_payment"]
    ).replace([np.inf, -np.inf], np.nan).round(4)

    # Medicare payment as % of total payment
    df["medicare_pct_of_total"] = (
        df["avg_medicare_payment"] / df["avg_total_payment"] * 100
    ).replace([np.inf, -np.inf], np.nan).round(2)

    # Non-Medicare payment (other payers / cost-sharing)
    df["avg_non_medicare_payment"] = (
        df["avg_total_payment"] - df["avg_medicare_payment"]
    ).round(2)

    # Volume-weighted total estimated charges
    df["est_total_charges"] = (
        df["avg_submitted_charge"] * df["total_discharges"]
    ).round(2)

    # Volume-weighted total estimated Medicare payments
    df["est_total_medicare_payment"] = (
        df["avg_medicare_payment"] * df["total_discharges"]
    ).round(2)

    # ── 2g. Rural/urban flag from RUCA ────────────────────────────────────────
    # RUCA 1-3 = urban/metro, 4-6 = suburban/large rural, 7-10 = small rural/remote
    def ruca_category(code):
        if pd.isna(code):
            return "Unknown"
        elif code <= 3:
            return "Urban"
        elif code <= 6:
            return "Suburban"
        else:
            return "Rural"

    df["ruca_category"] = df["ruca_code"].apply(ruca_category)

    # ── 2h. Add data year ─────────────────────────────────────────────────────
    df["data_year"] = 2023

    # ── 2i. Null check log ────────────────────────────────────────────────────
    null_summary = df[numeric_cols].isnull().sum()
    log.info(f"Null counts in numeric fields:\n{null_summary}")

    log.info(f"Transform complete. Final shape: {df.shape}")
    return df


# ── 3. LOAD ───────────────────────────────────────────────────────────────────
def load(df: pd.DataFrame, config: dict, table_name: str = "cms_inpatient_2023"):
    conn_str = (
        f"postgresql+psycopg2://{config['user']}:{config['password']}"
        f"@{config['host']}:{config['port']}/{config['database']}"
    )
    engine = create_engine(conn_str)
    log.info(f"Loading {len(df):,} rows into PostgreSQL table: {table_name}")
    df.to_sql(
        table_name,
        engine,
        if_exists="replace",   # change to "append" for incremental loads
        index=False,
        chunksize=5000
    )
    log.info("Load complete.")


# ── 4. MAIN ───────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    raw_df       = extract(CSV_PATH)
    clean_df     = transform(raw_df)

    # Preview before loading
    print("\n── Sample rows ──────────────────────────────────────")
    print(clean_df.head(3).to_string())
    print("\n── Data types ───────────────────────────────────────")
    print(clean_df.dtypes)
    print("\n── Null summary ─────────────────────────────────────")
    print(clean_df.isnull().sum())

    # Uncomment to load to PostgreSQL:
    load(clean_df, DB_CONFIG)
