"""
QC Validation Script (Snakemake rule: validate_fastq)
Validates FASTQ integrity and generates metadata
"""

import sys
import json
from pathlib import Path

sys.path.insert(0, str(Path.cwd()))
from workflow.lib.preprocess import validate_sample_files, count_fastq_records
from workflow.lib.utils import compute_md5

first_read_path = snakemake.params.first_read_path
second_read_path = snakemake.params.second_read_path
sample_id = snakemake.params.sample_id
report_file = snakemake.output.report
metadata_file = snakemake.output.metadata

Path(report_file).parent.mkdir(parents=True, exist_ok=True)

sample = {"isolate_id": sample_id, "R1_path": first_read_path, "R2_path": second_read_path}

is_valid, errors = validate_sample_files(sample)

# Always record each read's MD5 for provenance; verify it against the sequencing
# company's expected checksums when they're available. import_samples writes those
# to checksums.json next to the sample manifest (from the "DNA Sequencing Stats.xlsx"
# md5sum columns), so the completeness check the tutorial does by hand happens here
# automatically -- a mismatch means a truncated/corrupt download and FAILS QC.
r1_md5 = compute_md5(first_read_path) if Path(first_read_path).exists() else None
r2_md5 = compute_md5(second_read_path) if Path(second_read_path).exists() else None

expected = {}
try:
	manifest = snakemake.config.get("samples_manifest")
	if manifest:
		sidecar = Path(manifest).parent / "checksums.json"
		if sidecar.exists():
			expected = json.loads(sidecar.read_text()).get(sample_id, {})
except (OSError, ValueError):
	expected = {}

md5_status = "not_checked"
if expected:
	md5_status = "PASS"
	for read, actual in (("R1", r1_md5), ("R2", r2_md5)):
		expected_checksum = expected.get(read)
		if expected_checksum and actual and expected_checksum.lower() != actual.lower():
			errors.append(f"{read} MD5 mismatch: expected {expected_checksum}, got {actual}")
			is_valid = False
			md5_status = "FAIL"

metrics = {
 "sample_id": sample_id,
 "r1_path": first_read_path,
 "r2_path": second_read_path,
 "r1_exists": Path(first_read_path).exists(),
 "r2_exists": Path(second_read_path).exists(),
 "r1_size_bytes": Path(first_read_path).stat().st_size if Path(first_read_path).exists() else 0,
 "r2_size_bytes": Path(second_read_path).stat().st_size if Path(second_read_path).exists() else 0,
 "r1_record_estimate": count_fastq_records(first_read_path) if Path(first_read_path).exists() else 0,
 "r2_record_estimate": count_fastq_records(second_read_path) if Path(second_read_path).exists() else 0,
 "r1_md5": r1_md5,
 "r2_md5": r2_md5,
 "md5_status": md5_status,
 "status": "PASS" if is_valid else "FAIL",
 "errors": errors,
}

with open(report_file, "w") as file_handle:
	file_handle.write(f"Sample: {sample_id}\n")
	file_handle.write(f"R1: {first_read_path}\n")
	file_handle.write(f"R2: {second_read_path}\n")
	file_handle.write(f"Status: {metrics['status']}\n")
	file_handle.write(f"R1 exists: {metrics['r1_exists']}\n")
	file_handle.write(f"R2 exists: {metrics['r2_exists']}\n")
	file_handle.write(f"R1 estimated reads: {metrics['r1_record_estimate']}\n")
	file_handle.write(f"R2 estimated reads: {metrics['r2_record_estimate']}\n")
	file_handle.write(f"R1 MD5: {r1_md5}\n")
	file_handle.write(f"R2 MD5: {r2_md5}\n")
	file_handle.write(f"MD5 check: {md5_status}\n")
	if not is_valid:
		file_handle.write("Errors:\n")
		for error in errors:
			file_handle.write(f"- {error}\n")

with open(metadata_file, "w") as file_handle:
	json.dump(metrics, file_handle, indent=2)

print(f"✓ QC validation complete for {sample_id}")
