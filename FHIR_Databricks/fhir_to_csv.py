"""
fhir_to_csv.py
--------------
Adapted from original Postgres staging script.
Reads Synthea FHIR JSON bundles and writes flat CSVs
per resource type — no database required.

Output: one CSV per resource type in OUTPUT_DIR
        e.g. patients.csv, conditions.csv, encounters.csv ...

Usage:
    python fhir_to_csv.py

Adjust FHIR_FOLDER and OUTPUT_DIR below before running.
"""

import os
import glob
import json
import csv
import logging
import shutil
import tempfile
import collections
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

# ============================================================
# CONFIG — update these paths before running
# ============================================================

FHIR_FOLDER   = os.path.expanduser("~/Downloads/output_1000/fhir")
OUTPUT_DIR    = os.path.expanduser("~/Downloads/output_1000/csv")
LOG_FILE      = os.path.expanduser("~/Downloads/output_1000/fhir_to_csv.log")
CHECKPOINT_FILE = os.path.expanduser("~/Downloads/output_1000/processed_files.json")

MAX_WORKERS      = 8
FILES_PER_WORKER = 50

# ============================================================
# RESOURCE TYPES TO EXTRACT
# Adjust this list to add or remove resource types.
# ============================================================

RESOURCE_TYPES = [
    "Patient",
    "Condition",
    "Encounter",
    "Medication",
    "MedicationRequest",
    "Immunization",
    "Observation",
    "Procedure",
    "AllergyIntolerance",
    "DiagnosticReport",
    "ExplanationOfBenefit",
    "CarePlan",
    "CareTeam",
    "Device",
    "SupplyDelivery",
    "ImagingStudy",
    "DocumentReference",
    "Provenance",
    "Practitioner",
    "PractitionerRole",
    "Organization",
    "Claim",
    "MedicationAdministration",
]

# ============================================================
# LOGGER
# ============================================================

def setup_logger(name, log_file, level=logging.DEBUG):
    logger = logging.getLogger(name)
    logger.setLevel(level)
    if not logger.handlers:
        fh = logging.FileHandler(log_file, mode="w", encoding="utf-8")
        fh.setLevel(level)
        fh.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
        logger.addHandler(fh)
    return logger

# ============================================================
# CSV WRITER MANAGER
# Manages one open CSV writer per resource type.
# Thread-safe via per-type locks.
# ============================================================

import threading

class CsvWriterManager:
    def __init__(self, output_dir, resource_types):
        os.makedirs(output_dir, exist_ok=True)
        self._locks   = {rt: threading.Lock() for rt in resource_types}
        self._files   = {}
        self._writers = {}

        for rt in resource_types:
            path = os.path.join(output_dir, f"{rt.lower()}.csv")
            f = open(path, "w", newline="", encoding="utf-8")
            writer = csv.writer(f)
            writer.writerow(["id", "resource"])   # header
            self._files[rt]   = f
            self._writers[rt] = writer

    def write_rows(self, resource_type, rows):
        """rows: list of (id, resource_dict)"""
        if resource_type not in self._writers:
            return
        lock   = self._locks[resource_type]
        writer = self._writers[resource_type]
        with lock:
            for rid, resource in rows:
                writer.writerow([rid, json.dumps(resource)])

    def close_all(self):
        for f in self._files.values():
            f.close()

# Global writer manager (initialised in main)
csv_manager: CsvWriterManager = None

# ============================================================
# CHECKPOINT HELPERS
# ============================================================

def load_checkpoint():
    if os.path.exists(CHECKPOINT_FILE):
        try:
            with open(CHECKPOINT_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
                return set(data) if isinstance(data, list) else set()
        except json.JSONDecodeError:
            return set()
    return set()

def save_checkpoint(processed_files):
    tmp = tempfile.NamedTemporaryFile(delete=False, mode="w", encoding="utf-8")
    try:
        json.dump(list(processed_files), tmp, indent=2)
        tmp.close()
        shutil.move(tmp.name, CHECKPOINT_FILE)
    except Exception as e:
        logging.error(f"Failed to save checkpoint: {e}")
        if os.path.exists(tmp.name):
            os.remove(tmp.name)

# ============================================================
# FILE PROCESSING
# ============================================================

def process_file(file_path):
    """Parse one FHIR bundle JSON and write rows to CSVs."""
    batch_map = defaultdict(list)

    try:
        with open(file_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        for entry in data.get("entry", []):
            resource = entry.get("resource")
            if not resource:
                continue
            rtype = resource.get("resourceType")
            rid   = resource.get("id")
            if not rtype or not rid:
                continue
            if rtype in RESOURCE_TYPES:
                batch_map[rtype].append((rid, resource))

        # Write to CSVs
        for rtype, rows in batch_map.items():
            csv_manager.write_rows(rtype, rows)

        summary = {k: len(v) for k, v in batch_map.items()}
        return summary

    except Exception as e:
        logging.error(f"Failed to process {file_path}: {e}")
        return {}

def process_file_chunk(file_chunk):
    """Process a chunk of files and return combined counters."""
    chunk_counters = defaultdict(int)
    for file_path in file_chunk:
        result = process_file(file_path)
        if result:
            for k, v in result.items():
                chunk_counters[k] += v
    return dict(chunk_counters)

def chunk_files(file_list, chunk_size):
    for i in range(0, len(file_list), chunk_size):
        yield file_list[i:i + chunk_size]

# ============================================================
# MAIN
# ============================================================

def main():
    global csv_manager

    os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)
    logger = setup_logger("fhir_to_csv", LOG_FILE)

    # Find all patient JSON files
    files = [
        os.path.abspath(f)
        for f in glob.glob(os.path.join(FHIR_FOLDER, "*.json"))
    ]

    if not files:
        print(f"No JSON files found in {FHIR_FOLDER}")
        return

    # Skip already processed files
    processed = set(os.path.abspath(f) for f in load_checkpoint())
    remaining = [f for f in files if f not in processed]

    print(f"Found {len(files)} files. {len(processed)} already processed. "
          f"Processing {len(remaining)} remaining.")

    if not remaining:
        print("Nothing to do — all files already processed.")
        return

    # Initialise CSV writers
    csv_manager = CsvWriterManager(OUTPUT_DIR, RESOURCE_TYPES)

    global_counters = defaultdict(int)
    file_chunks = list(chunk_files(remaining, FILES_PER_WORKER))

    try:
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {
                executor.submit(process_file_chunk, chunk): chunk
                for chunk in file_chunks
            }

            for future in tqdm(as_completed(futures), total=len(file_chunks)):
                chunk = futures[future]
                try:
                    result = future.result()
                    if result:
                        for k, v in result.items():
                            global_counters[k] += v
                    for f in chunk:
                        processed.add(f)
                    save_checkpoint(processed)
                except Exception as e:
                    logger.error(f"Chunk error: {e}")
    finally:
        csv_manager.close_all()

    print("\n✅ Done! Totals by resource type:")
    for rtype in sorted(global_counters):
        print(f"  {rtype}: {global_counters[rtype]:,}")

    print(f"\nCSV files written to: {OUTPUT_DIR}")


if __name__ == "__main__":
    main()
