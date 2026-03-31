"""
CMS Provider of Services — CAH Reference Table ETL
Loads Critical Access Hospital designation and joins to cms_inpatient_2023
Author: Toni
"""

import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────
POS_PATH = "/Users/toniventura/Downloads/Provider of Services File - Quality Improvement and Evaluation System/2025-Q4/Hospital_and_other.DATA.Q4_2025.csv"  # update if needed

DB_CONFIG = {
    "host":     "localhost",
    "port":     5432,
    "database": "cms_analytics",
    "user":     "toniventura",
    "password": "cms2023"
}

# ── Columns we need from the POS file ─────────────────────────────────────────
KEEP_COLS = [
    "PRVDR_NUM",           # CCN — join key
    "FAC_NAME",            # facility name
    "PRVDR_CTGRY_CD",      # provider category
    "PRVDR_CTGRY_SBTYP_CD",# provider subtype
    "GNRL_FAC_TYPE_CD",    # general facility type
    "CAH_SB_SW",           # CAH designation Y/N  ← key field
    "CBSA_URBN_RRL_IND",   # urban/rural indicator
    "STATE_CD",            # state
    "CITY_NAME",           # city
    "ZIP_CD",              # zip
    "BED_CNT",             # bed count
    "CRTFCTN_DT",          # certification date
    "PGM_TRMNTN_CD",       # termination code (active vs terminated)
]

# ── 1. EXTRACT ────────────────────────────────────────────────────────────────
def extract(path):
    log.info(f"Loading POS file: {path}")
    df = pd.read_csv(path, dtype=str, low_memory=False, usecols=KEEP_COLS)
    log.info(f"Loaded {len(df):,} rows")
    return df

# ── 2. TRANSFORM ──────────────────────────────────────────────────────────────
def transform(df):

    log.info("Transforming POS data...")

    # Rename columns to snake_case
    rename_map = {
        "PRVDR_NUM":            "provider_ccn",
        "FAC_NAME":             "facility_name",
        "PRVDR_CTGRY_CD":       "provider_category_cd",
        "PRVDR_CTGRY_SBTYP_CD": "provider_subtype_cd",
        "GNRL_FAC_TYPE_CD":     "facility_type_cd",
        "CAH_SB_SW":            "is_cah",
        "CBSA_URBN_RRL_IND":    "urban_rural_ind",
        "STATE_CD":             "state_cd",
        "CITY_NAME":            "city",
        "ZIP_CD":               "zip",
        "BED_CNT":              "bed_count",
        "CRTFCTN_DT":           "certification_dt",
        "PGM_TRMNTN_CD":        "termination_cd",
    }
    df = df.rename(columns=rename_map)

    print("Termination code values (including nulls):")
    print(df["termination_cd"].value_counts(dropna=False).head(10))
    print(f"Null count: {df['termination_cd'].isna().sum():,}")

    # Clean CCN — zero pad to 6 digits
    df["provider_ccn"] = df["provider_ccn"].str.strip().str.zfill(6)

    # Standardize CAH flag
    df["is_cah"] = df["is_cah"].str.strip().str.upper() == "Y"

    # Bed count as numeric
    df["bed_count"] = pd.to_numeric(df["bed_count"], errors="coerce")

    # Active providers only (no termination code)
    before = len(df)
    #df = df[df["termination_cd"].isna() | (df["termination_cd"].str.strip() == "")]
    df = df[df["termination_cd"].str.strip() == "00"]
    log.info(f"Filtered to active providers: {len(df):,} (removed {before - len(df):,} terminated)")
   

    # Log CAH counts
    cah_count = df["is_cah"].sum()
    log.info(f"CAH providers in POS file: {cah_count:,}")
    log.info(f"Non-CAH providers: {(~df['is_cah']).sum():,}")

    return df

# ── 3. LOAD REFERENCE TABLE ───────────────────────────────────────────────────
def load_reference(df, config):
    conn_str = (
        f"postgresql+psycopg2://{config['user']}:{config['password']}"
        f"@{config['host']}:{config['port']}/{config['database']}"
    )
    engine = create_engine(conn_str)
    log.info("Loading CAH reference table to PostgreSQL...")
    df.to_sql(
        "pos_provider_reference",
        engine,
        if_exists="replace",
        index=False,
        chunksize=5000
    )
    log.info("Reference table loaded: pos_provider_reference")
    return engine

# ── 4. ADD is_cah FLAG TO cms_inpatient_2023 ─────────────────────────────────
def add_cah_flag(engine):
    log.info("Adding is_cah flag to cms_inpatient_2023...")
    with engine.connect() as conn:

        # Add column if it doesn't exist
        conn.execute(text("""
            ALTER TABLE cms_inpatient_2023
            ADD COLUMN IF NOT EXISTS is_cah BOOLEAN DEFAULT FALSE;
        """))

        # Add urban_rural_ind from POS
        conn.execute(text("""
            ALTER TABLE cms_inpatient_2023
            ADD COLUMN IF NOT EXISTS urban_rural_ind VARCHAR(10);
        """))

        # Add bed_count from POS
        conn.execute(text("""
            ALTER TABLE cms_inpatient_2023
            ADD COLUMN IF NOT EXISTS bed_count NUMERIC;
        """))

        # Update is_cah by joining on CCN
        result = conn.execute(text("""
            UPDATE cms_inpatient_2023 AS c
            SET 
                is_cah         = p.is_cah,
                urban_rural_ind = p.urban_rural_ind,
                bed_count       = p.bed_count
            FROM pos_provider_reference AS p
            WHERE LPAD(c."Rndrng_Prvdr_CCN", 6, '0') = p.provider_ccn;
        """))
        conn.commit()

        updated = result.rowcount
        log.info(f"Updated {updated:,} rows with CAH flag")

    return updated

# ── 5. VALIDATION ─────────────────────────────────────────────────────────────
def validate(engine):
    log.info("Validating join results...")
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT 
                is_cah,
                COUNT(*) as row_count,
                COUNT(DISTINCT "Rndrng_Prvdr_CCN") as provider_count
            FROM cms_inpatient_2023
            GROUP BY is_cah
            ORDER BY is_cah;
        """))
        rows = result.fetchall()
        print("\n── CAH Flag Distribution ────────────────────────────")
        print(f"{'is_cah':<10} {'rows':>12} {'providers':>12}")
        print("-" * 36)
        for row in rows:
            print(f"{str(row[0]):<10} {row[1]:>12,} {row[2]:>12,}")

        # Payment gap: CAH vs non-CAH rural
        result2 = conn.execute(text("""
            SELECT 
                ruca_category,
                is_cah,
                COUNT(*) as rows,
                ROUND(AVG(avg_medicare_payment)::numeric, 2) as avg_medicare_pmt,
                ROUND(AVG(charge_to_payment_ratio)::numeric, 4) as avg_ctp_ratio
            FROM cms_inpatient_2023
            WHERE ruca_category IN ('Rural', 'Suburban')
            GROUP BY ruca_category, is_cah
            ORDER BY ruca_category, is_cah;
        """))
        rows2 = result2.fetchall()
        print("\n── Rural/Suburban Payment Gap: CAH vs Non-CAH ───────")
        print(f"{'RUCA':<12} {'is_cah':<8} {'rows':>8} {'avg_pmt':>12} {'avg_ctp':>10}")
        print("-" * 54)
        for row in rows2:
            print(f"{str(row[0]):<12} {str(row[1]):<8} {row[2]:>8,} {row[3]:>12} {row[4]:>10}")

# ── MAIN ──────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    raw_df   = extract(POS_PATH)
    clean_df = transform(raw_df)

    # Preview
    print("\n── Sample rows ──────────────────────────────────────")
    print(clean_df[clean_df["is_cah"] == True].head(3).to_string())
    print(f"\nCAH count: {clean_df['is_cah'].sum():,}")

    engine   = load_reference(clean_df, DB_CONFIG)
    updated  = add_cah_flag(engine)
    validate(engine)

    print("\n── Done! ────────────────────────────────────────────────")
