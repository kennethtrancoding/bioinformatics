"""
Parse a MobileElementFinder (`mefinder find`) CSV into a compact per-sample summary.

The CSV starts with several '#' comment lines (date, sample, versions) followed by
a real header row: mge_no,name,synonyms,prediction,type,allele_len,...

Writes me_summary.{csv,json}:
  sample_id, mobile_elements (total), by_type {type: count}
"""

import csv
import json
from pathlib import Path

mobile_element_finder_csv_path = snakemake.input.csv
summary_csv = snakemake.output.summary_csv
summary_json = snakemake.output.summary_json
sample_id = snakemake.params.sample_id

total = 0
counts_by_mobile_element_type = {}

if Path(mobile_element_finder_csv_path).exists():
    with open(mobile_element_finder_csv_path, "r") as file_handle:
        data_lines = [data_line for data_line in file_handle if not data_line.startswith("#")]
    if data_lines:
        reader = csv.DictReader(data_lines)
        for csv_row in reader:
            # Only count real detections (rows with an mge_no).
            if not (csv_row.get("mge_no") or "").strip():
                continue
            total += 1
            mobile_element_type = (csv_row.get("type") or "unknown").strip() or "unknown"
            counts_by_mobile_element_type[mobile_element_type] = counts_by_mobile_element_type.get(mobile_element_type, 0) + 1

summary = {
    "sample_id": sample_id,
    "mobile_elements": total,
    "by_type": counts_by_mobile_element_type,
}

Path(summary_json).parent.mkdir(parents=True, exist_ok=True)

with open(summary_json, "w") as file_handle:
    json.dump(summary, file_handle, indent=2)

# Flat CSV: one row per element type (plus a TOTAL row) for easy inspection.
with open(summary_csv, "w", newline="") as file_handle:
    writer = csv.writer(file_handle)
    writer.writerow(["sample_id", "element_type", "count"])
    for mobile_element_type, count in sorted(counts_by_mobile_element_type.items()):
        writer.writerow([sample_id, mobile_element_type, count])
    writer.writerow([sample_id, "TOTAL", total])

mobile_element_type_summary = ", ".join(f"{mobile_element_type}:{count}" for mobile_element_type, count in sorted(counts_by_mobile_element_type.items())) or "none"
print(f"✓ MobileElementFinder summary for {sample_id}: {total} MGEs ({mobile_element_type_summary})")
