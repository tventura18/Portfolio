import logging
import os
from google.cloud import bigquery
from google.oauth2 import service_account
from datetime import datetime, timezone
import json
import traceback
from collections import Counter

# --------------------
# Configuration
# --------------------
ETL_CONFIG = {
    "BQ_PROJECT": "fhir-synthea-data",
    "BQ_DATASET_RAW": "fhir_staging",
    "BQ_TABLE_RAW": "explanationofbenefits",
    "BQ_DATASET_CURATED": "fhir_curated_sample",
    "BQ_TABLE_CURATED": "eob_coverage",
    "BATCH_SIZE": 5000,
    "KEY_PATH": "/keys/bq_key.json"
}

# --------------------
# Setup BigQuery client
# --------------------
credentials = service_account.Credentials.from_service_account_file(ETL_CONFIG["KEY_PATH"])
bq_client = bigquery.Client(credentials=credentials, project=ETL_CONFIG["BQ_PROJECT"])

# --------------------
# Helpers
# --------------------
def parse_fhir_datetime(dt_str):
    if not dt_str:
        return None
    try:
        return datetime.fromisoformat(dt_str.replace("Z", "+00:00"))
    except Exception:
        return None

def safe_bq_timestamp(dt_obj):
    return dt_obj.isoformat() if dt_obj else None

# --------------------
# Fetch batches from staging
# --------------------
def fetch_staging_batches(batch_size=5000):
    query = f"""
        SELECT explanationofbenefit_id AS eob_id, resource, load_timestamp
        FROM `{ETL_CONFIG['BQ_PROJECT']}.{ETL_CONFIG['BQ_DATASET_RAW']}.{ETL_CONFIG['BQ_TABLE_RAW']}`
    """
    query_job = bq_client.query(query)
    iterator = query_job.result(page_size=batch_size)

    batch = []
    for row in iterator:
        batch.append({
            "eob_id": row["eob_id"],
            "resource": row["resource"]
        })
        if len(batch) >= batch_size:
            yield batch
            batch = []

    if batch:
        yield batch

# --------------------
# Transform function
# --------------------
def transform_batch(batch):
    curated_records = []

    for row in batch:
        eob_id = row["eob_id"]
        resource = row["resource"]

        # Parse JSON if needed
        resource = json.loads(resource) if isinstance(resource, str) else resource
        if resource.get("resourceType") != "ExplanationOfBenefit":
            continue

        contained_resources = resource.get("contained", [])
        insurance_list = resource.get("insurance", [])

        # Map insurance coverage references -> focal
        coverage_focal_map = {
            ins.get("coverage", {}).get("reference", "").lstrip("#"): ins.get("focal")
            for ins in insurance_list if ins.get("coverage")
        }

        for contained in contained_resources:
            if contained.get("resourceType") != "Coverage":
                continue

            cov_id = contained.get("id")
            type_info = contained.get("type", {})
            type_codings = type_info.get("coding", [])
            main_type = type_codings[0] if type_codings else {}

            identifiers = contained.get("identifier", [])
            main_identifier = identifiers[0] if identifiers else {}

            period = contained.get("period", {})

            curated_records.append({
                "coverage_id": cov_id or f"{eob_id}-coverage",
                "status": contained.get("status"),
                "type_code": main_type.get("code"),
                "type_system": main_type.get("system"),
                "type_display": main_type.get("display") or type_info.get("text"),
                "type_codings": [
                    {"system": c.get("system"), "code": c.get("code"), "display": c.get("display")}
                    for c in type_codings
                ] if type_codings else [],
                "identifier_value": main_identifier.get("value"),
                "identifier_system": main_identifier.get("system"),
                "identifiers": [
                    {"system": i.get("system"), "value": i.get("value")}
                    for i in identifiers
                ] if identifiers else [],
                "beneficiary_ref": contained.get("beneficiary", {}).get("reference"),
                "payor": (contained.get("payor", [{}])[0].get("display") if contained.get("payor") else None),
                "subscriber_id": contained.get("subscriber", {}).get("reference"),
                "period_start": safe_bq_timestamp(parse_fhir_datetime(period.get("start"))),
                "period_end": safe_bq_timestamp(parse_fhir_datetime(period.get("end"))),
                "focal": coverage_focal_map.get(cov_id),
                "load_timestamp": safe_bq_timestamp(datetime.utcnow())
            })

    logging.info(f"Transformed {len(curated_records)} Coverage records in this batch")
    return curated_records

# --------------------
# Load batch into curated table
# --------------------
def load_batch_to_bq(batch):
    table_id = f"{ETL_CONFIG['BQ_PROJECT']}.{ETL_CONFIG['BQ_DATASET_CURATED']}.{ETL_CONFIG['BQ_TABLE_CURATED']}"
    tables = list(bq_client.list_tables(ETL_CONFIG['BQ_DATASET_RAW']))
    print("Tables in staging dataset:", [t.table_id for t in tables])

    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
    job = bq_client.load_table_from_json(batch, table_id, job_config=job_config)
    job.result()
    logging.info(f"Loaded {len(batch)} records into {table_id}")

def estimate_coverage_count(all_rows):
    coverage_count = 0

    for row in all_rows:
        # Access by index because row is a list/tuple
        eob_id = row[0]
        resource = row[1]

        # Parse JSON if it's a string
        resource = json.loads(resource) if isinstance(resource, str) else resource
        if resource.get("resourceType") != "ExplanationOfBenefit":
            continue

        contained_resources = resource.get("contained", [])
        insurance_list = resource.get("insurance", [])

        # Map insurance coverage references -> focal
        coverage_focal_map = {}
        for ins in insurance_list:
            cov_ref = ins.get("coverage", {}).get("reference")
            if cov_ref:
                coverage_focal_map[cov_ref.lstrip("#")] = ins.get("focal", None)

        # Count Coverage records in contained resources
        for contained in contained_resources:
            if contained.get("resourceType") == "Coverage":
                coverage_count += 1

    return coverage_count

# --------------------
# Run pipeline
# --------------------
def run_pipeline():
    for batch_num, batch in enumerate(fetch_staging_batches(ETL_CONFIG["BATCH_SIZE"]), start=1):
        transformed = transform_batch(batch)
        if transformed:
            load_batch_to_bq(transformed)
        logging.info(f"Batch {batch_num} processed successfully")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    #all_eobs = fetch_all_records_from_source()  # however you load your raw EOBs
    all_eobs = fetch_staging_batches()
    total_coverage = estimate_coverage_count(all_eobs)
    print(f"Estimated number of Coverage records: {total_coverage}")

    '''coverage_counts = []
    for row in all_eobs:
        resource = row["resource"]
        resource = json.loads(resource) if isinstance(resource, str) else resource
        if resource.get("resourceType") != "ExplanationOfBenefit":
            continue
        contained_resources = resource.get("contained", [])
        coverage_per_eob = sum(1 for r in contained_resources if r.get("resourceType") == "Coverage")
        coverage_counts.append(coverage_per_eob)

    total_coverage = sum(coverage_counts)
    min_coverage = min(coverage_counts) if coverage_counts else 0
    max_coverage = max(coverage_counts) if coverage_counts else 0
    avg_coverage = total_coverage / len(coverage_counts) if coverage_counts else 0

    print(f"Total Coverage records: {total_coverage}")
    print(f"Min per EOB: {min_coverage}, Max per EOB: {max_coverage}, Avg per EOB: {avg_coverage:.2f}")'''
    run_pipeline()

'''if __name__ == "__main__":
    print("Script started")
    print(f"BQ_PROJECT={ETL_CONFIG['BQ_PROJECT']}, KEY_PATH={ETL_CONFIG['KEY_PATH']}")
    logging.basicConfig(level=logging.INFO)

    # Fetch only the first batch of 5–10 records
    test_batches = fetch_staging_batches(batch_size=5)
    test_batch = next(test_batches, [])

    if not test_batch:
        print("No records found in staging.")
    else:
        print(f"Fetched {len(test_batch)} records for testing.")

        # Transform
        transformed = transform_batch(test_batch)
        print(f"Transformed {len(transformed)} records.")
        print("Sample transformed record:")
        print(transformed[0])

        # Optionally load to BQ
        load_batch_to_bq(transformed)
        print("Test batch loaded successfully.")'''
