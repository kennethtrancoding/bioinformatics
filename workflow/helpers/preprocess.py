"""
FASTQ integrity and checksum verification.

Reading the sample manifest is job_store.JobStore's job, not this module's --
it owns the manifest's format and writes it atomically.
"""

import gzip
from pathlib import Path
from typing import Dict, List, Tuple

from workflow.helpers.utils import compute_md5, setup_logger

logger = setup_logger("preprocess")


# FASTQ Validation


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
	open_func = gzip.open if file_path.endswith((".fastq.gz", ".fq.gz")) else open

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


