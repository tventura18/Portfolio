"""
CMS Medicare Inpatient FFS - Multi-Year ETL Pipeline
Data: Medicare Inpatient Hospitals by Provider and Service (2019-2023)
Author: Toni
"""

import pandas as pd
import numpy as np
import os
import re
from sqlalchemy import create_engine, text
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────
#CSV_DIR = "/Users/toniventura/Downloads/csv_downloads/"
CSV_DIR = "/Users/toniventura/Downloads/csv_downloads/"

DB_CONFIG = {
    "host":     "localhost",
    "port":     5432,
    "database": "cms_analytics",
    "user":     "toniventura",
    "password": "cms2023"
}

TABLE_NAME = "cms_inpatient_multiyear"

# ── Helper: extract year from filename ───────────────────────────────────────
def extract_year(filename):
    match = re.search(r"DY(\d{2})", filename)
    if match:
        year = int(match.group(1))
        return 2000 + year
    return None

# ── 1. EXTRACT ────────────────────────────────────────────────────────────────
def extract(path, year):
    log.info(f"Loading {year} CSV: {os.path.basename(path)}")
    df = pd.read_csv(path, dtype=str, low_memory=False, encoding="latin-1")
    log.info(f"  Loaded {len(df):,} rows, {len(df.columns)} columns")
    return df

# ── 2. TRANSFORM ──────────────────────────────────────────────────────────────
def transform(df, year):
    log.info(f"  Transforming {year} data...")

    # ── Rename columns to snake_case ──────────────────────────────────────────
    rename_map = {
        "Rndrng_Provdr_CCN":        "provider_ccn",
        "Rndrng_Prvdr_CCN":         "provider_ccn",
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
    # Only rename columns that exist in this year's file
    rename_map = {k: v for k, v in rename_map.items() if k in df.columns}
    df = df.rename(columns=rename_map)

    # ── Cast numeric columns ──────────────────────────────────────────────────
    numeric_cols = [
        "total_discharges",
        "avg_submitted_charge",
        "avg_total_payment",
        "avg_medicare_payment",
    ]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col].str.replace(",", ""), errors="coerce")

    # ── Zero-pad zip and DRG ──────────────────────────────────────────────────
    if "zip5" in df.columns:
        df["zip5"] = df["zip5"].str.strip().str.zfill(5)
    if "drg_code" in df.columns:
        df["drg_code"] = df["drg_code"].str.strip().str.zfill(3)

    # ── RUCA as numeric ───────────────────────────────────────────────────────
    if "ruca_code" in df.columns:
        df["ruca_code"] = pd.to_numeric(df["ruca_code"], errors="coerce")

    # ── Derived fields ────────────────────────────────────────────────────────
    df["charge_to_payment_ratio"] = (
        df["avg_submitted_charge"] / df["avg_medicare_payment"]
    ).replace([np.inf, -np.inf], np.nan).round(4)

    df["medicare_pct_of_total"] = (
        df["avg_medicare_payment"] / df["avg_total_payment"] * 100
    ).replace([np.inf, -np.inf], np.nan).round(2)

    df["avg_non_medicare_payment"] = (
        df["avg_total_payment"] - df["avg_medicare_payment"]
    ).round(2)

    df["est_total_charges"] = (
        df["avg_submitted_charge"] * df["total_discharges"]
    ).round(2)

    df["est_total_medicare_payment"] = (
        df["avg_medicare_payment"] * df["total_discharges"]
    ).round(2)

    # ── RUCA category ─────────────────────────────────────────────────────────
    def ruca_category(code):
        if pd.isna(code):
            return "Unknown"
        elif code <= 3:
            return "Urban"
        elif code <= 6:
            return "Suburban"
        else:
            return "Rural"

    if "ruca_code" in df.columns:
        df["ruca_category"] = df["ruca_code"].apply(ruca_category)
    else:
        df["ruca_category"] = "Unknown"

    # ── Add data year ─────────────────────────────────────────────────────────
    df["data_year"] = year

    # ── Null check ────────────────────────────────────────────────────────────
    null_counts = df[numeric_cols].isnull().sum()
    log.info(f"  Null counts: {null_counts.to_dict()}")
    log.info(f"  Transform complete. Shape: {df.shape}")

    return df

# ── 3. LOAD ───────────────────────────────────────────────────────────────────
def load(df, config, table_name, if_exists="append"):
    conn_str = (
        f"postgresql+psycopg2://{config['user']}:{config['password']}"
        f"@{config['host']}:{config['port']}/{config['database']}"
    )
    engine = create_engine(conn_str)
    log.info(f"  Loading {len(df):,} rows into {table_name}...")
    df.to_sql(
        table_name,
        engine,
        if_exists=if_exists,
        index=False,
        chunksize=5000
    )
    log.info(f"  Load complete.")
    return engine

# ── 4. ADD INDEXES ────────────────────────────────────────────────────────────
def add_indexes(engine, table_name):
    log.info("Adding indexes for query performance...")
    with engine.connect() as conn:
        conn.execute(text(f"""
            CREATE INDEX IF NOT EXISTS idx_{table_name}_year 
            ON {table_name} (data_year);
        """))
        #conn.execute(text(f"""
        #    CREATE INDEX IF NOT EXISTS idx_{table_name}_state 
        #    ON {table_name} (state_abbr);
        #"""))
        conn.execute(text(f"""
            CREATE INDEX IF NOT EXISTS idx_{table_name}_state 
            ON {table_name} ("Rndrng_Prvdr_State_Abrvtn");
        """))
        conn.execute(text(f"""
            CREATE INDEX IF NOT EXISTS idx_{table_name}_drg 
            ON {table_name} (drg_code);
        """))
        conn.execute(text(f"""
            CREATE INDEX IF NOT EXISTS idx_{table_name}_ruca 
            ON {table_name} (ruca_category);
        """))
        conn.commit()
    log.info("Indexes created.")

# ── 5. VALIDATE ───────────────────────────────────────────────────────────────
def validate(engine, table_name):
    log.info("Validating multi-year table...")
    with engine.connect() as conn:
        result = conn.execute(text(f"""
            SELECT 
                data_year,
                COUNT(*) as rows,
                COUNT(DISTINCT provider_ccn) as providers,
                ROUND(AVG(avg_medicare_payment)::numeric, 2) as avg_medicare_pmt,
                ROUND(AVG(charge_to_payment_ratio)::numeric, 4) as avg_ctp_ratio
            FROM {table_name}
            GROUP BY data_year
            ORDER BY data_year;
        """))
        rows = result.fetchall()
        print("\n── Multi-Year Summary ───────────────────────────────────────")
        print(f"{'Year':<8} {'Rows':>10} {'Providers':>12} {'Avg Pmt':>12} {'Avg CTP':>10}")
        print("-" * 56)
        for row in rows:
            print(f"{row[0]:<8} {row[1]:>10,} {row[2]:>12,} {row[3]:>12} {row[4]:>10}")

# ── MAIN ──────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    # Find all CSV files in the directory
    csv_files = sorted([
        f for f in os.listdir(CSV_DIR)
        if f.endswith(".CSV") and "PRVSVC" in f.upper()
    ])

    log.info(f"Found {len(csv_files)} CSV files: {csv_files}")

    engine = None
    for i, filename in enumerate(csv_files):
        year = extract_year(filename)
        if not year:
            log.warning(f"Could not extract year from {filename}, skipping.")
            continue

        path = os.path.join(CSV_DIR, filename)

        # Extract
        raw_df = extract(path, year)

        # Transform
        clean_df = transform(raw_df, year)

        # Load — replace on first file, append on subsequent
        if_exists = "replace" if i == 0 else "append"
        engine = load(clean_df, DB_CONFIG, TABLE_NAME, if_exists=if_exists)

        log.info(f"Year {year} complete.\n")

    # Add indexes and validate
    if engine:
        add_indexes(engine, TABLE_NAME)
        validate(engine, TABLE_NAME)

    print("\n── All years loaded! ────────────────────────────────────────")
