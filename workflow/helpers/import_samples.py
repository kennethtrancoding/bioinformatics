"""
Bulk-register paired-end FASTQ files into config/samples.csv.

A sequencing company typically hands you a flat folder of files named like
    SW4E1_S337_R1_001.fastq.gz
    SW4E1_S337_R2_001.fastq.gz
This module pairs R1/R2, derives the isolate ID, and writes them into the
sample manifest the pipeline reads. By default it copies the FASTQs into
data/raw_fastq (matching the web upload).
"""

import argparse
import csv
import json
import re
import shutil
import sys
from pathlib import Path

from workflow.helpers.utils import compute_md5

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DEFAULT_SAMPLES_CSV = PROJECT_ROOT / "config" / "samples.csv"
DEFAULT_DATA_DIR = PROJECT_ROOT / "data" / "raw_fastq"
FIELDS = ["isolate_id", "R1_path", "R2_path", "description"]

_FASTQ_SUFFIX = (".fastq.gz", ".fq.gz", ".fastq", ".fq")
# The R1 marker is "_R1" followed by "_" or "." (e.g. _R1_001.fastq.gz or _R1.fastq.gz).
_R1_MARKER = re.compile(r"_R1([_.])")
_ISOLATE_RE = re.compile(r"_R[12][_.].*$")

# The sequencing company drops a stats workbook alongside the FASTQs; its
# "R1 md5sum"/"R2 md5sum" columns are the authoritative checksums we verify
# bulk imports against.
STATS_XLSX_NAME = "DNA Sequencing Stats.xlsx"
# Illumina names the FASTQs "<sample>_S<index>_R1_001.fastq.gz" but the stats
# sheet lists just "<sample>", so drop the _S<index> tail to match the two up.
_SAMPLE_INDEX_RE = re.compile(r"_S\d+.*$")


def is_fastq(file_name):
	"""Shared with cloud_import, which uses it to decide what is worth pulling
	out of a shared drive folder in the first place."""
	return file_name.lower().endswith(_FASTQ_SUFFIX)


def _isolate_id(file_name):
	"""SW4E1_S337_R1_001.fastq.gz -> SW4E1_S337"""
	return _ISOLATE_RE.sub("", file_name)


def _sample_key(isolate_id):
	"""Normalize an isolate id to the stats sheet's 'Sample Name' (SW4E1_S337 -> SW4E1)."""
	return _SAMPLE_INDEX_RE.sub("", isolate_id)


def _norm_name(file_name):
	"""Collapse a filename to lowercase alphanumerics so the original
	'DNA Sequencing Stats.xlsx' and the web upload's secure_filename'd
	'DNA_Sequencing_Stats.xlsx' (spaces -> underscores) compare equal."""
	return re.sub(r"[^a-z0-9]", "", file_name.lower())


def find_stats_xlsx(directory):
	"""Locate the sequencing company's 'DNA Sequencing Stats.xlsx' in `directory`.

	Matches on a normalized name so it still finds the sheet after the web
	upload's secure_filename rewrite (spaces -> underscores) even when other
	.xlsx files sit alongside it; if the named sheet isn't present but the
	folder holds exactly one .xlsx, use that. Returns a Path or None.
	"""
	directory = Path(directory).expanduser().resolve()
	if not directory.is_dir():
		return None
	target_workbook_name = _norm_name(STATS_XLSX_NAME)
	xlsx_paths = []
	for directory_entry in directory.iterdir():
		if directory_entry.name.startswith("~$") or not directory_entry.name.lower().endswith(
			".xlsx"
		):
			continue  # skip Excel lock files
		if not directory_entry.is_file():
			continue
		if _norm_name(directory_entry.name) == target_workbook_name:
			return directory_entry
		xlsx_paths.append(directory_entry)
	return xlsx_paths[0] if len(xlsx_paths) == 1 else None


def load_stats_checksums(xlsx_path):
	"""Parse the sequencing-stats workbook into {sample_key: {'R1': md5, 'R2': md5}}.

	The sheet has a header row with 'Sample Name', 'R1 md5sum' and 'R2 md5sum'
	columns (plus read/quality stats we ignore). Returns {} if the workbook
	lacks those columns. Raises ImportError if openpyxl is not installed, so the
	caller can tell "dependency missing" apart from "unreadable workbook"
	instead of silently skipping verification.
	"""
	import openpyxl  # ImportError propagates: caller distinguishes it from an empty result

	try:
		workbook = openpyxl.load_workbook(xlsx_path, read_only=True, data_only=True)
	except Exception:
		return {}

	expected_checksums_by_sample = {}
	for worksheet in workbook.worksheets:
		column_indexes = None
		for worksheet_row in worksheet.iter_rows(values_only=True):
			if column_indexes is None:
				# Look for the header row that carries the columns we need.
				cell_values = [
					str(cell_value).strip().lower() if cell_value is not None else ""
					for cell_value in worksheet_row
				]
				sample_name_column_index = next(
					(
						column_index
						for column_index, header_value in enumerate(cell_values)
						if "sample name" in header_value
					),
					None,
				)
				first_read_md5_column_index = next(
					(
						column_index
						for column_index, header_value in enumerate(cell_values)
						if "r1" in header_value and "md5" in header_value
					),
					None,
				)
				second_read_md5_column_index = next(
					(
						column_index
						for column_index, header_value in enumerate(cell_values)
						if "r2" in header_value and "md5" in header_value
					),
					None,
				)
				if (
					sample_name_column_index is not None
					and first_read_md5_column_index is not None
					and second_read_md5_column_index is not None
				):
					column_indexes = {
						"name": sample_name_column_index,
						"R1": first_read_md5_column_index,
						"R2": second_read_md5_column_index,
					}
				continue

			def _cell(column_key):
				column_index = column_indexes[column_key]
				return (
					worksheet_row[column_index]
					if column_index < len(worksheet_row) and worksheet_row[column_index] is not None
					else None
				)

			file_name = _cell("name")
			if not file_name:
				continue
			directory_entry = {}
			for read_label in ("R1", "R2"):
				cell_value = _cell(read_label)
				if cell_value:
					directory_entry[read_label] = str(cell_value).strip()
			if directory_entry:
				expected_checksums_by_sample[_sample_key(str(file_name).strip())] = directory_entry
		if expected_checksums_by_sample:
			break
	return expected_checksums_by_sample


def pair_fastqs(directory, recursive=False):
	"""
	Find R1/R2 FASTQ pairs in a directory.

	Returns (pairs, warnings) where each pair is a dict:
	    {"isolate_id", "R1_path", "R2_path"}  (absolute path strings)
	and warnings is a list of human-readable strings about anything skipped.
	"""
	directory = Path(directory).expanduser().resolve()
	if not directory.is_dir():
		raise NotADirectoryError(directory)

	fastq_files_by_name = {}
	if recursive:
		for directory_entry in directory.rglob("*"):
			if directory_entry.is_file() and is_fastq(directory_entry.name):
				fastq_files_by_name.setdefault(directory_entry.name, directory_entry)
	else:
		for directory_entry in directory.iterdir():
			if directory_entry.is_file() and is_fastq(directory_entry.name):
				fastq_files_by_name[directory_entry.name] = directory_entry

	pairs, warnings = [], []
	seen_isolates = {}

	for file_name in sorted(fastq_files_by_name):
		if not _R1_MARKER.search(file_name):
			continue  # only iterate from the R1 side; R2 handled via its mate
		mate_file_name = _R1_MARKER.sub(r"_R2\1", file_name, count=1)
		if mate_file_name not in fastq_files_by_name:
			warnings.append(f"No R2 mate for {file_name} (expected {mate_file_name}; skipped.")
			continue

		isolate_id = _isolate_id(file_name)
		if isolate_id in seen_isolates:
			warnings.append(
				f"Duplicate isolate '{isolate_id}' from {file_name}; keeping {seen_isolates[isolate_id]}."
			)
			continue
		seen_isolates[isolate_id] = file_name

		pairs.append(
			{
				"isolate_id": isolate_id,
				"R1_path": str(fastq_files_by_name[file_name]),
				"R2_path": str(fastq_files_by_name[mate_file_name]),
			}
		)

	return pairs, warnings


def _relative_path(registered_path, start):
	"""Path.relative_to() without the requirement that `path` be under `start`
	(mirrors os.path.relpath, which walks up with '..' as needed)."""
	target_path_parts, start_path_parts = (
		Path(registered_path).resolve().parts,
		Path(start).resolve().parts,
	)
	common_prefix_length = 0
	for target_path_part, start_path_part in zip(target_path_parts, start_path_parts):
		if target_path_part != start_path_part:
			break
		common_prefix_length += 1
	relative_path_parts = [".."] * (len(start_path_parts) - common_prefix_length) + list(
		target_path_parts[common_prefix_length:]
	)
	return str(Path(*relative_path_parts)) if relative_path_parts else "."


def _read_existing(samples_csv):
	samples_csv = Path(samples_csv)
	if not samples_csv.exists():
		return []
	with samples_csv.open(newline="") as file_handle:
		return list(csv.DictReader(file_handle))


def _copy_into(source_path, dest_dir, move=False):
	"""
	Copy (or move) src into dest_dir, keeping its filename. Skips the copy when
	the file is already there with the same size, so re-imports are fast and
	idempotent. Returns the destination path (str).

	`move` renames src into place instead of copying it — used for the web
	upload and cloud-import paths, where src already sits in a throwaway staging
	directory, so a copy would needlessly double disk usage for large FASTQ dumps.
	"""
	source_path = Path(source_path)
	dest_dir = Path(dest_dir)
	dest_dir.mkdir(parents=True, exist_ok=True)
	destination_path = dest_dir / source_path.name
	if source_path.resolve() == destination_path.resolve():
		return str(destination_path)
	if destination_path.exists() and destination_path.stat().st_size == source_path.stat().st_size:
		return str(destination_path)
	if move:
		shutil.move(str(source_path), str(destination_path))
	else:
		shutil.copy2(source_path, destination_path)
	return str(destination_path)


def import_directory(
	directory,
	samples_csv=DEFAULT_SAMPLES_CSV,
	recursive=False,
	dest_dir=None,
	verify_checksums=True,
	move=False,
	on_pair_imported=None,
):
	"""
	Pair FASTQs in `directory` and merge them into the sample manifest.

	If `dest_dir` is given, each FASTQ is copied there (like the web upload) and
	registered with a path relative to the project root; otherwise the files are
	registered in place using their absolute paths. Pass `move` when `directory`
	is a throwaway staging tree the caller is about to delete anyway, to hand the
	FASTQs over instead of duplicating them on disk.

	When `verify_checksums` is set and a "DNA Sequencing Stats.xlsx" (the
	sequencing company's stats workbook) sits in `directory`, each FASTQ's MD5 is
	checked against the sheet's R1/R2 md5sum columns. A pair that fails is left
	out of the manifest (and any copy of it removed) so bad data never enters the
	pipeline. Files with no matching row in the sheet are imported unverified.

	`on_pair_imported(isolate_id, registered_paths)` runs after each pair is
	verified and registered, while the rest of the batch is still unprocessed --
	the web upload uses it to push that pair to S3 and drop the local copy, so
	peak disk tracks one pair instead of the whole folder. It is only called when
	`dest_dir` is set: the paths handed to it are then copies this function made
	and the callback is free to delete them, whereas an in-place import registers
	the caller's own files and deleting those would destroy the originals.

	Existing isolates are updated in place (idempotent — safe to re-run).
	Returns a summary dict:
	{added, updated, skipped, warnings, samples, verified, failed, checksum_source}.
	"""
	pairs, warnings = pair_fastqs(directory, recursive=recursive)

	# Company-provided checksums, if the stats workbook is present next to the FASTQs.
	expected_checksums_by_sample, checksum_source_name = {}, None
	if verify_checksums:
		stats_workbook_path = find_stats_xlsx(directory)
		if stats_workbook_path:
			try:
				expected_checksums_by_sample = load_stats_checksums(stats_workbook_path)
			except ImportError:
				# Distinct from an unreadable workbook: the dependency is simply
				# missing, so say so plainly instead of blaming the file.
				warnings.append(
					f"Found {stats_workbook_path.name} but openpyxl is not installed, so the "
					f"checksum table could not be read; run 'pip install openpyxl' "
					f"to enable MD5 verification. Imported without checksum verification."
				)
			else:
				if expected_checksums_by_sample:
					checksum_source_name = stats_workbook_path.name
				else:
					warnings.append(
						f"Found {stats_workbook_path.name} but it has no readable R1/R2 md5sum columns; imported without checksum verification."
					)

	manifest_rows = _read_existing(samples_csv)
	rows_by_isolate_id = {
		manifest_row.get("isolate_id"): manifest_row for manifest_row in manifest_rows
	}

	added, updated, verified, failed = [], [], [], []
	for fastq_pair in pairs:
		# Verify the destination copy, which also catches a truncated copy.
		registered_paths = {"R1": fastq_pair["R1_path"], "R2": fastq_pair["R2_path"]}
		if dest_dir:
			registered_paths = {
				read_label: _copy_into(source_path, dest_dir, move=move)
				for read_label, source_path in registered_paths.items()
			}

		expected_checksums = expected_checksums_by_sample.get(
			_sample_key(fastq_pair["isolate_id"]), {}
		)
		mismatched_reads = []
		for read_label, registered_path in registered_paths.items():
			expected_checksum = expected_checksums.get(read_label)
			if (
				expected_checksum
				and compute_md5(registered_path).lower() != expected_checksum.lower()
			):
				mismatched_reads.append(read_label)

		if mismatched_reads:
			warnings.append(
				f"Checksum mismatch for {fastq_pair['isolate_id']} ({', '.join(mismatched_reads)}; not imported)."
			)
			failed.append(fastq_pair["isolate_id"])
			# Remove copies we just made so a bad file isn't left behind.
			if dest_dir:
				for read_label, registered_path in registered_paths.items():
					registered_path = Path(registered_path)
					if (
						registered_path.resolve()
						!= Path(fastq_pair[f"{read_label}_path"]).resolve()
						and registered_path.exists()
					):
						registered_path.unlink()
			continue

		worksheet_row = {
			"isolate_id": fastq_pair["isolate_id"],
			"R1_path": _relative_path(registered_paths["R1"], PROJECT_ROOT)
			if dest_dir
			else registered_paths["R1"],
			"R2_path": _relative_path(registered_paths["R2"], PROJECT_ROOT)
			if dest_dir
			else registered_paths["R2"],
			"description": rows_by_isolate_id.get(fastq_pair["isolate_id"], {}).get(
				"description", ""
			),
		}
		if fastq_pair["isolate_id"] in rows_by_isolate_id:
			rows_by_isolate_id[fastq_pair["isolate_id"]].update(worksheet_row)
			updated.append(fastq_pair["isolate_id"])
		else:
			rows_by_isolate_id[fastq_pair["isolate_id"]] = worksheet_row
			added.append(fastq_pair["isolate_id"])
		if expected_checksums:
			verified.append(fastq_pair["isolate_id"])

		# Hand the pair off before touching the next one. The manifest below is
		# written from rows_by_isolate_id, not from the files, so the callback may
		# take the copies away -- and for a large folder it must, or every read in
		# the batch would sit on disk until the last pair was registered.
		if on_pair_imported and dest_dir:
			on_pair_imported(fastq_pair["isolate_id"], dict(registered_paths))

	samples_csv = Path(samples_csv)
	samples_csv.parent.mkdir(parents=True, exist_ok=True)
	with samples_csv.open("w", newline="") as file_handle:
		writer = csv.DictWriter(file_handle, fieldnames=FIELDS)
		writer.writeheader()
		for worksheet_row in rows_by_isolate_id.values():
			writer.writerow(
				{field_name: worksheet_row.get(field_name, "") for field_name in FIELDS}
			)

	# Keep expected MD5s for QC-time verification; preserve prior imports.
	if expected_checksums_by_sample:
		checksum_sidecar_path = samples_csv.parent / "checksums.json"
		persisted_checksums = {}
		if checksum_sidecar_path.exists():
			try:
				persisted_checksums = json.loads(checksum_sidecar_path.read_text())
			except (OSError, ValueError):
				persisted_checksums = {}
		for fastq_pair in pairs:
			expected_checksum = expected_checksums_by_sample.get(
				_sample_key(fastq_pair["isolate_id"])
			)
			if expected_checksum:
				persisted_checksums[fastq_pair["isolate_id"]] = expected_checksum
		checksum_sidecar_path.write_text(json.dumps(persisted_checksums, indent=2))

	return {
		"added": added,
		"updated": updated,
		"skipped": len(warnings),
		"warnings": warnings,
		"samples": list(rows_by_isolate_id.values()),
		"verified": verified,
		"failed": failed,
		"checksum_source": checksum_source_name,
	}


def main(argv=None):
	parser = argparse.ArgumentParser(description="Bulk-import paired FASTQ files into samples.csv")
	parser.add_argument(
		"directory", help="Folder of *_R1/_R2 FASTQ files (e.g. ~/Downloads/Genomes)"
	)
	parser.add_argument("-r", "--recursive", action="store_true", help="Search subfolders too")
	parser.add_argument(
		"-o", "--samples-csv", default=DEFAULT_SAMPLES_CSV, help="Manifest to write"
	)
	parser.add_argument(
		"--dest",
		default=DEFAULT_DATA_DIR,
		help="Folder to copy FASTQs into (default: data/raw_fastq)",
	)
	parser.add_argument(
		"--in-place", action="store_true", help="Register files where they are instead of copying"
	)
	parser.add_argument(
		"--no-verify",
		action="store_true",
		help=f"Skip MD5 verification against '{STATS_XLSX_NAME}' if present",
	)
	parser.add_argument("-n", "--dry-run", action="store_true", help="Show pairs without writing")
	parsed_args = parser.parse_args(argv)

	if parsed_args.dry_run:
		pairs, warnings = pair_fastqs(parsed_args.directory, recursive=parsed_args.recursive)
		for fastq_pair in pairs:
			print(
				f"{fastq_pair['isolate_id']}\t{Path(fastq_pair['R1_path']).name}\t{Path(fastq_pair['R2_path']).name}"
			)
		for warning_message in warnings:
			print(f"WARN: {warning_message}", file=sys.stderr)
		print(f"\n{len(pairs)} pair(s) found (dry run, nothing written).")
		return

	dest_dir = None if parsed_args.in_place else parsed_args.dest
	if dest_dir:
		print(f"Copying FASTQs into {dest_dir} (large dumps can take a while)...")
	import_result = import_directory(
		parsed_args.directory,
		samples_csv=parsed_args.samples_csv,
		recursive=parsed_args.recursive,
		dest_dir=dest_dir,
		verify_checksums=not parsed_args.no_verify,
	)
	for warning_message in import_result["warnings"]:
		print(f"WARN: {warning_message}", file=sys.stderr)
	print(
		f"✓ {len(import_result['added'])} added, {len(import_result['updated'])} updated → {parsed_args.samples_csv}"
	)
	print(f"  Total registered: {len(import_result['samples'])} sample(s).")
	if import_result["checksum_source"]:
		print(
			f"  Checksums verified against {import_result['checksum_source']}: "
			f"{len(import_result['verified'])} ok, {len(import_result['failed'])} failed."
		)


if __name__ == "__main__":
	main()
