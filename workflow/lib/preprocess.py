"""
FASTQ preprocessing and validation.
Handles MD5 checksums, file integrity, and sample manifest parsing.
"""

import csv
import logging
from pathlib import Path
from typing import List, Dict, Optional, Tuple
import gzip
from workflow.lib.utils import compute_md5, setup_logger, load_json_safe


logger = setup_logger("preprocess")


# FASTQ Validation


def is_gzipped_fastq(file_path: str) -> bool:
	"""
	Check if file is a gzipped FASTQ.

	Args:
	    file_path: Path to file

	Returns:
	    True if gzipped, False if plain text
	"""
	return file_path.endswith(".fastq.gz") or file_path.endswith(".fq.gz")


def count_fastq_records(file_path: str, sample_size: int = 1000) -> int:
	"""
	Count approximate number of FASTQ records (sampled).
	A FASTQ record is 4 lines.

	Args:
	    file_path: Path to FASTQ file
	    sample_size: Number of records to sample (for speed)

	Returns:
	    Estimated total record count, or -1 if error
	"""
	try:
		open_func = gzip.open if is_gzipped_fastq(file_path) else open

		line_count = 0
		with open_func(file_path, "rt") as file_handle:
			for line_index, fastq_line in enumerate(file_handle):
				if line_index >= sample_size * 4:
					break
				line_count += 1

		estimated_total = (line_count // 4) if line_count > 0 else 0
		return (
		 estimated_total if estimated_total == 0 else int((estimated_total / sample_size) * 1e5)
		)
	except Exception as exception:
		logger.error(f"Failed to count FASTQ records in {file_path}: {exception}")
		return -1


def validate_fastq_integrity(file_path: str) -> Tuple[bool, str]:
	"""
	Check if FASTQ file is valid and not corrupted.

	Args:
	    file_path: Path to FASTQ file

	Returns:
	    Tuple of (is_valid, error_message)
	"""
	if not Path(file_path).exists():
		return False, f"File not found: {file_path}"

	if Path(file_path).stat().st_size == 0:
		return False, f"File is empty: {file_path}"

	try:
		open_func = gzip.open if is_gzipped_fastq(file_path) else open

		with open_func(file_path, "rt") as file_handle:
			line_count = 0
			for fastq_line in file_handle:
				line_count += 1
				if line_count > 100000:
					break

			if line_count % 4 != 0:
				return False, f"FASTQ line count ({line_count}) is not divisible by 4 (corrupted?)"

		return True, "OK"

	except (OSError, EOFError, gzip.BadGzipFile) as exception:
		return False, f"File read error: {exception}"
	except Exception as exception:
		return False, f"Unexpected error: {exception}"


# MD5 Verification


def load_md5_checksums(checksum_file: str) -> Dict[str, str]:
	"""
	Load MD5 checksums from text file (md5sum format: <hash>  <filename>).

	Args:
	    checksum_file: Path to MD5 checksum file

	Returns:
	    Dictionary mapping filename -> MD5 hash
	"""
	checksums = {}

	try:
		with open(checksum_file, "r") as file_handle:
			for fastq_line in file_handle:
				checksum_parts = fastq_line.strip().split()
				if len(checksum_parts) >= 2:
					md5_hash = checksum_parts[0]
					filename = checksum_parts[1].lstrip("*")
					checksums[filename] = md5_hash
	except FileNotFoundError:
		logger.error(f"Checksum file not found: {checksum_file}")
	except Exception as exception:
		logger.error(f"Error parsing checksum file: {exception}")

	return checksums


def verify_file_md5(file_path: str, expected_md5: str) -> Tuple[bool, str]:
	"""
	Verify a file's MD5 checksum.

	Args:
	    file_path: Path to file
	    expected_md5: Expected MD5 hash

	Returns:
	    Tuple of (is_valid, message)
	"""
	if not Path(file_path).exists():
		return False, f"File not found: {file_path}"

	try:
		actual_md5 = compute_md5(file_path)
		if actual_md5.lower() == expected_md5.lower():
			return True, f"MD5 verified: {actual_md5}"
		else:
			return False, f"MD5 mismatch: expected {expected_md5}, got {actual_md5}"
	except Exception as exception:
		return False, f"Error computing MD5: {exception}"


# Sample Manifest


def load_sample_manifest(manifest_file: str) -> List[Dict[str, str]]:
	"""
	Load sample manifest CSV file.
	Expected columns: isolate_id, R1_path, R2_path, [description]

	Args:
	    manifest_file: Path to samples.csv

	Returns:
	    List of sample dictionaries
	"""
	sample_records = []

	if not Path(manifest_file).exists():
		logger.error(f"Manifest file not found: {manifest_file}")
		return sample_records

	try:
		with open(manifest_file, "r") as file_handle:
			reader = csv.DictReader(file_handle)
			for manifest_row in reader:
				if manifest_row.get("isolate_id") and manifest_row.get("R1_path") and manifest_row.get("R2_path"):
					sample_records.append(manifest_row)
				else:
					logger.warning(f"Skipping incomplete row: {manifest_row}")

		logger.info(f"Loaded {len(sample_records)} samples from {manifest_file}")
		return sample_records

	except Exception as exception:
		logger.error(f"Error parsing manifest: {exception}")
		return sample_records


def validate_sample_files(sample: Dict[str, str]) -> Tuple[bool, List[str]]:
	"""
	Validate that sample FASTQ files exist and are readable.

	Args:
	    sample: Sample dictionary with R1_path, R2_path

	Returns:
	    Tuple of (all_valid, list_of_error_messages)
	"""
	validation_errors = []

	for read_type in ["R1_path", "R2_path"]:
		file_path = sample.get(read_type)
		if not file_path:
			validation_errors.append(f"Missing {read_type}")
			continue

		if not Path(file_path).exists():
			validation_errors.append(f"{read_type} not found: {file_path}")
			continue

		file_size = Path(file_path).stat().st_size
		if file_size == 0:
			validation_errors.append(f"{read_type} is empty: {file_path}")
			continue

		is_valid, validation_message = validate_fastq_integrity(file_path)
		if not is_valid:
			validation_errors.append(f"{read_type} integrity check failed: {validation_message}")

	return len(validation_errors) == 0, validation_errors


# Report Generation


def generate_preprocess_report(sample_records: List[Dict[str, str]], output_file: str) -> bool:
	"""
	Generate preprocessing validation report.

	Args:
	    samples: List of sample dictionaries
	    output_file: Path to output report

	Returns:
	    True if report generated successfully
	"""
	try:
		with open(output_file, "w") as file_handle:
			file_handle.write("# Preprocessing Validation Report\n\n")
			file_handle.write(f"Total samples: {len(sample_records)}\n\n")

			valid_count = 0
			for sample in sample_records:
				is_valid, validation_errors = validate_sample_files(sample)
				validation_status = "✓ PASS" if is_valid else "✗ FAIL"
				file_handle.write(f"## {sample.get('isolate_id')} [{validation_status}]\n")

				if is_valid:
					valid_count += 1
					file_handle.write(f"- R1: {sample.get('R1_path')}\n")
					file_handle.write(f"- R2: {sample.get('R2_path')}\n")
				else:
					for error in validation_errors:
						file_handle.write(f"- ERROR: {error}\n")

				file_handle.write("\n")

			file_handle.write(f"\n## Summary\n")
			file_handle.write(f"Valid samples: {valid_count}/{len(sample_records)}\n")

		logger.info(f"Preprocessing report saved to {output_file}")
		return True

	except Exception as exception:
		logger.error(f"Failed to generate report: {exception}")
		return False


if __name__ == "__main__":
	sample_manifest = load_sample_manifest("config/samples.csv")
	for sample in sample_manifest:
		is_valid, validation_errors = validate_sample_files(sample)
		if is_valid:
			logger.info(f"✓ {sample['isolate_id']} is valid")
		else:
			logger.warning(f"✗ {sample['isolate_id']}: {'; '.join(validation_errors)}")
