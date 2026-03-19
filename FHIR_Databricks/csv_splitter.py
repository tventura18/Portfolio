"""
csv_splitter.py
---------------
Splits large CSVs into chunks under a size limit for upload to
Databricks Community Edition (10MB UI upload limit).

Run AFTER fhir_to_csv.py has finished.

Output: chunked files written to OUTPUT_DIR
        e.g. observation_001.csv, observation_002.csv ...
        Files already under the limit are copied as-is.

Usage:
    python csv_splitter.py
"""

import os
import csv
import glob
import shutil
import sys
csv.field_size_limit(sys.maxsize)

# ============================================================
# CONFIG — should match your fhir_to_csv.py settings
# ============================================================

INPUT_DIR  = os.path.expanduser("~/Downloads/output_readmission")
OUTPUT_DIR = os.path.expanduser("~/Downloads/output_readmission/csv_split")

# Target max size per output file in bytes.
# Set slightly under 10MB to be safe with Databricks.
MAX_BYTES = 9 * 1024 * 1024   # 9 MB

# ============================================================
# SPLITTER
# ============================================================

def split_csv(input_path, output_dir, max_bytes, base_name):
    """
    Reads a CSV with a header row and writes it out in chunks,
    each under max_bytes. Preserves the header in every chunk.
    """
    with open(input_path, "r", newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        header = next(reader)

        chunk_index   = 1
        current_rows  = []
        current_bytes = 0

        def write_chunk(rows, index):
            chunk_name = f"{base_name}_{index:03d}.csv"
            chunk_path = os.path.join(output_dir, chunk_name)
            with open(chunk_path, "w", newline="", encoding="utf-8") as out:
                writer = csv.writer(out)
                writer.writerow(header)
                writer.writerows(rows)
            print(f"  → wrote {chunk_name}  ({len(rows):,} rows)")

        for row in reader:
            # Estimate row size as its CSV-encoded length
            row_size = len(",".join(str(c) for c in row).encode("utf-8")) + 2

            if current_bytes + row_size > max_bytes and current_rows:
                write_chunk(current_rows, chunk_index)
                chunk_index  += 1
                current_rows  = []
                current_bytes = 0

            current_rows.append(row)
            current_bytes += row_size

        # Write any remaining rows
        if current_rows:
            write_chunk(current_rows, chunk_index)

    return chunk_index


def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    csv_files = sorted(glob.glob(os.path.join(INPUT_DIR, "*.csv")))

    if not csv_files:
        print(f"No CSV files found in {INPUT_DIR}")
        return

    print(f"Found {len(csv_files)} CSV file(s) in {INPUT_DIR}\n")

    for input_path in csv_files:
        file_size = os.path.getsize(input_path)
        base_name = os.path.splitext(os.path.basename(input_path))[0]

        if file_size <= MAX_BYTES:
            # Already small enough — copy straight over
            dest = os.path.join(OUTPUT_DIR, os.path.basename(input_path))
            shutil.copy2(input_path, dest)
            print(f"{base_name}.csv  ({file_size / 1024 / 1024:.1f} MB) — under limit, copied as-is")
        else:
            print(f"{base_name}.csv  ({file_size / 1024 / 1024:.1f} MB) — splitting...")
            chunks = split_csv(input_path, OUTPUT_DIR, MAX_BYTES, base_name)
            print(f"  ✓ {chunks} chunk(s) written")

        print()

    print(f"✅ Done! Split files written to:\n  {OUTPUT_DIR}")
    print("\nUpload each file to Databricks via:")
    print("  Workspace → your folder → Import → select CSV file")
    print("  or: Catalog → Add Data → Upload files")


if __name__ == "__main__":
    main()
