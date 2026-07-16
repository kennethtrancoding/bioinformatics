"""
FASTQ preprocessing and validation.
Handles MD5 checksums, file integrity, and sample manifest parsing.
"""

import csv
import gzip
from pathlib import Path
from typing import Dict, List, Tuple

from workflow.helpers.utils import compute_md5, setup_logger

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


def scan_fastq(file_path: str) -> Tuple[int, str]:
	"""
	Read a FASTQ end to end, returning its exact record count.

	The whole file is read deliberately. A gzip member only proves itself
	complete at its very end, where the CRC and uncompressed-length trailer sit,
	so any scan that stops early cannot distinguish a whole file from one whose
	download was cut short -- and a truncated R2 is accepted by upload and
	rejected server-side hours later, which is expensive to diagnose.

	Args:
	    file_path: Path to FASTQ file

	Returns:
	    Tuple of (record_count, message); record_count is -1 when unreadable
	"""
	open_func = gzip.open if is_gzipped_fastq(file_path) else open

	line_count = 0
	try:
		with open_func(file_path, "rt") as file_handle:
			for fastq_line in file_handle:
				line_count += 1
	except EOFError as exception:
		return -1, f"compressed stream ends mid-record, file is truncated: {exception}"
	except OSError as exception:
		return -1, f"file read error: {exception}"
	except Exception as exception:
		return -1, f"unexpected error: {exception}"

	if line_count == 0:
		return -1, "file contains no FASTQ records"

	if line_count % 4 != 0:
		return -1, f"line count ({line_count}) is not divisible by 4 (truncated?)"

	return line_count // 4, "OK"


def count_fastq_records(file_path: str) -> int:
	"""
	Count the FASTQ records in a file.

	Args:
	    file_path: Path to FASTQ file

	Returns:
	    Exact record count, or -1 if error
	"""
	record_count, message = scan_fastq(file_path)
	if record_count < 0:
		logger.error(f"Failed to count FASTQ records in {file_path}: {message}")
	return record_count


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

	record_count, message = scan_fastq(file_path)
	if record_count < 0:
		return False, message

	return True, "OK"


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
				if (
					manifest_row.get("isolate_id")
					and manifest_row.get("R1_path")
					and manifest_row.get("R2_path")
				):
					sample_records.append(manifest_row)
				else:
					logger.warning(f"Skipping incomplete row: {manifest_row}")

		logger.info(f"Loaded {len(sample_records)} samples from {manifest_file}")
		return sample_records

	except Exception as exception:
		logger.error(f"Error parsing manifest: {exception}")
		return sample_records


def validate_sample_files(sample: Dict[str, str]) -> Tuple[bool, List[str], Dict[str, int]]:
	"""
	Validate that sample FASTQ files exist and are readable.

	Args:
	    sample: Sample dictionary with R1_path, R2_path

	Returns:
	    Tuple of (all_valid, list_of_error_messages, record_count_by_read).
	    The counts are returned because reading them costs a full pass over
	    every read, and the caller records them alongside the verdict.
	"""
	validation_errors = []
	record_counts = {}

	# These messages are read by whoever is doing the upload, so they name the read
	# the way the person does ("R2"), not the way the dict key does ("R2_path").
	for read_type in ["R1_path", "R2_path"]:
		read_label = read_type.removesuffix("_path")
		file_path = sample.get(read_type)
		if not file_path:
			validation_errors.append(f"Missing {read_label}")
			continue

		if not Path(file_path).exists():
			validation_errors.append(f"{read_label} not found: {file_path}")
			continue

		file_size = Path(file_path).stat().st_size
		if file_size == 0:
			validation_errors.append(f"{read_label} is empty: {file_path}")
			continue

		record_count, validation_message = scan_fastq(file_path)
		if record_count < 0:
			validation_errors.append(f"{read_label} is not readable: {validation_message}")
			continue

		record_counts[read_type] = record_count

	# Both mates of an Illumina pair carry one record per fragment, so unequal
	# counts mean one of the two files is incomplete even when each is a valid
	# gzip on its own -- the mates were written by the same run and cannot
	# legitimately disagree.
	if len(record_counts) == 2 and record_counts["R1_path"] != record_counts["R2_path"]:
		validation_errors.append(
			f"R1/R2 record count mismatch: R1 has {record_counts['R1_path']} reads, "
			f"R2 has {record_counts['R2_path']} (one mate is incomplete)"
		)

	counts_by_read = {
		"R1": record_counts.get("R1_path", -1),
		"R2": record_counts.get("R2_path", -1),
	}
	return len(validation_errors) == 0, validation_errors, counts_by_read


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

			file_handle.write("\n## Summary\n")
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
