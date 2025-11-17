import logging
import os
from google.cloud import bigquery
from google.oauth2 import service_account
from datetime import datetime
import json
import traceback

# --------------------
# Configuration
# --------------------
ETL_CONFIG = {
    "BQ_PROJECT": "fhir-synthea-data",
    "BQ_DATASET_RAW": "fhir_staging",
    "BQ_TABLE_RAW": "explanationofbenefits",
    "BQ_DATASET_CURATED": "fhir_curated_sample",
    "BQ_TABLE_CURATED": "eob_items",
    "BATCH_SIZE": 5000,
    #"KEY_PATH": os.environ.get("BQ_KEY_PATH", "/keys/bq_key.json")  # fallback if env var not set
    "KEY_PATH": "/keys/bq_key.json"

}

# --------------------
# Setup BigQuery client
# --------------------
credentials = service_account.Credentials.from_service_account_file(ETL_CONFIG["KEY_PATH"])
bq_client = bigquery.Client(credentials=credentials, project=ETL_CONFIG["BQ_PROJECT"])
# List tables in fhir_staging
tables = list(bq_client.list_tables("fhir_staging"))
for t in tables:
    print(t.table_id)

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
from datetime import datetime, timezone

def parse_fhir_datetime(dt_str):
    """Parse FHIR datetime string to Python datetime object, safely."""
    if not dt_str:
        return None
    try:
        return datetime.fromisoformat(dt_str.replace("Z", "+00:00"))
    except Exception:
        return None

def safe_bq_timestamp(dt_obj):
    """Convert datetime to ISO string suitable for BigQuery, or None."""
    return dt_obj.isoformat() if dt_obj else None
def parse_fhir_datetime(dt_str):
    """Parse FHIR datetime string to Python datetime object, safely."""
    if not dt_str:
        return None
    try:
        return datetime.fromisoformat(dt_str.replace("Z", "+00:00"))
    except Exception:
        return None

def safe_bq_timestamp(dt_obj):
    """Convert datetime to ISO string suitable for BigQuery, or None."""
    return dt_obj.isoformat() if dt_obj else None

'''def transform_eob_items_bq(batch):
    records = []
    for row in batch:
        rid, resource = row[1], row[2]'''
def transform_batch(batch):
    curated_records = []
    for row in batch:
        #rid, resource = row[1], row[2]
        resource = row["resource"]
        eob_id = row["eob_id"]

        #eob_id = resource.get("id")
        #eob_id = eob_id
        #resource = record["resource"]
        resource = json.loads(resource) if isinstance(resource, str) else resource
        #eob_id = row["eob_id"]
        items = resource.get("item",[])
        #print(f"items:  {items}")
        for item in items:
            try:
                # Sequence
                seq = item.get("sequence")
                if isinstance(seq, list):
                    sequence = seq[0] if seq else None
                else:
                    sequence = seq

                # Category
                category = item.get("category")
                cat_coding = {}

                if isinstance(category, dict):
                    cat_coding = category.get("coding", [{}])[0]
                elif isinstance(category, list) and len(category) > 0:
                    cat_coding = category[0].get("coding", [{}])[0]

                category_system = cat_coding.get("system")
                category_code = cat_coding.get("code")
                category_display = cat_coding.get("display")

                # Product / Service
                product = item.get("productOrService", {})
                prod_coding = product.get("coding", [{}])[0] if product else {}
                product_system = prod_coding.get("system")
                product_code = prod_coding.get("code")
                product_display = prod_coding.get("display")
                product_text = product.get("text")

                # Service period
                serviced = item.get("servicedPeriod", {})
                service_start = safe_bq_timestamp(parse_fhir_datetime(serviced.get("start")))
                service_end = safe_bq_timestamp(parse_fhir_datetime(serviced.get("end")))

                # --------------------
                # Location
                # --------------------
                # Check multiple possible keys: 'location', 'locationCodeableConcept'
                location_field = item.get("location") or item.get("locationCodeableConcept", {})
                if isinstance(location_field, list):
                    loc_obj = location_field[0] if location_field else {}
                elif isinstance(location_field, dict):
                    loc_obj = location_field
                else:
                    loc_obj = {}

                # extract coding
                loc_coding_list = loc_obj.get("coding", [{}])
                loc_coding = loc_coding_list[0] if loc_coding_list else {}

                location_system = loc_coding.get("system")
                location_code = loc_coding.get("code")
                location_display = loc_coding.get("display")

                # --------------------
                    # Encounter
                # --------------------
                encounter_field = item.get("encounter", [])
                if isinstance(encounter_field, list):
                    # array of objects with 'reference'
                    encounter_refs = [e.get("reference") for e in encounter_field if e.get("reference")]
                    encounter = encounter_refs[0] if encounter_refs else None
                elif isinstance(encounter_field, dict):
                    # single dict
                    encounter = encounter_field.get("reference")
                else:
                    encounter = None

                # Quantity / price / net
                quantity = float(item.get("quantity", {}).get("value", 0)) if "quantity" in item else None
                unit_price = float(item.get("quantity", {}).get("unitPrice", 0)) if "quantity" in item else None
                #quantity = float(item.get("quantity", {}).get("value", 0)) if "quantity" in resource else None
                #unit_price = float(item.get("quantity", {}).get("unitPrice", 0)) if "quantity" in resource else None

                # Net / total amounts
                net_value = None
                net_currency = None
                if "net" in item:
                    net_value = float(item["net"].get("value", 0)) if item["net"].get("value") else None
                    net_currency = item["net"].get("currency")
                else:
                    totals = item.get("total", [])
                if totals:
                    net_value = float(totals[0].get("amount", {}).get("value", 0)) if totals[0].get("amount") else None
                    net_currency = totals[0].get("amount", {}).get("currency") if totals[0].get("amount") else None

                '''net_value = None
                net_currency = None
                if "net" in resource:
                    net_value = float(item["net"].get("value", 0)) if item["net"].get("value") else None
                    net_currency =item["net"].get("currency")
                else:
                    # fallback to total field
                    totals = item.get("total", [])
                if totals:
                    net_value = float(totals[0].get("amount", {}).get("value", 0)) if totals[0].get("amount") else None
                    net_currency = totals[0].get("amount", {}).get("currency") if totals[0].get("amount") else None'''

                # Diagnosis sequence
                #diagnosis_sequence = item.get("diagnosisSequence", [])

                #diagnosis_sequences = item.get("diagnosisSequence", [])
                #diagnosis_sequence = diagnosis_sequences[0] if diagnosis_sequences else None
                diagnosis_sequences = item.get("diagnosisSequence", [])
                # Ensure it’s always a list for BigQuery
                diagnosis_sequence = diagnosis_sequences[0] if diagnosis_sequences else None


                # Adjudication
                adjudication_list = []
                for adj in item.get("adjudication", []):
                    adj_cat = adj.get("category", {}).get("coding", [{}])[0]
                    adjudication_list.append({
                        "code": adj_cat.get("code"),
                        "display": adj_cat.get("display"),
                        "value": float(adj.get("amount", {}).get("value", 0)) if adj.get("amount") else None,
                        "currency": adj.get("amount", {}).get("currency") if adj.get("amount") else None
                    })

                # Amount (optional per-item amounts)
                amount_list = []
                for amt in item.get("amount", []):
                    amount_list.append({
                        "value": float(amt.get("value", 0)) if amt.get("value") else None,
                        "currency": amt.get("currency")
                    })

                # Build curated record
                curated_records.append({
                    "eob_id": eob_id,
                    "sequence": sequence,
                    "diagnosis_sequence": diagnosis_sequence,
                    "category_system": category_system,
                    "category_code": category_code,
                    "category_display": category_display,
                    "product_system": product_system,
                    "product_code": product_code,
                    "product_display": product_display,
                    "product_text": product_text,
                    "service_start": service_start,
                    "service_end": service_end,
                    "location_system": location_system,
                    "location_code": location_code,
                    "location_display": location_display,
                    "encounter": encounter,
                    "quantity": quantity,
                    "unit_price": unit_price,
                    "net_value": net_value,
                    "net_currency": net_currency,
                    "adjudication": adjudication_list,
                    "amount": amount_list,
                    "load_timestamp": datetime.utcnow().isoformat()
                })

            except Exception as e:
                #logging.error(f"Failed to transform record {eob_id}: {e}")
                logging.error(f"Failed to transform record {eob_id}: {type(e).__name__} - {e}")
                logging.error(traceback.format_exc())

    logging.info(f"Transformed {len(curated_records)} records in this batch")
    return curated_records

# --------------------
# Load batch into curated table
# --------------------
def load_batch_to_bq(batch):
    table_id = f"{ETL_CONFIG['BQ_PROJECT']}.{ETL_CONFIG['BQ_DATASET_CURATED']}.{ETL_CONFIG['BQ_TABLE_CURATED']}"
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
    job = bq_client.load_table_from_json(batch, table_id, job_config=job_config)
    job.result()
    logging.info(f"Loaded {len(batch)} records into {table_id}")

# --------------------
# Run pipeline
# --------------------
def run_pipeline():
    for batch_num, batch in enumerate(fetch_staging_batches(ETL_CONFIG["BATCH_SIZE"]), start=1):
        transformed = transform_batch(batch)
        load_batch_to_bq(transformed)
        logging.info(f"Batch {batch_num} processed successfully")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run_pipeline()
'''if __name__ == "__main__":
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
