"""Reading pipeline outputs and writing report files.

The I/O half of the report split; `report_format` renders the cells that go
into them.

Every rule script that turns tool output into a report goes through here. These
were previously defined two or three times over -- once per report generator --
and had already drifted: one JSON reader caught a decode error and the other did
not, so a single malformed file degraded gracefully in the per-sample report and
took the whole-batch master report down with it.

This is a leaf module: it imports nothing from the pipeline, so any script run
as a Snakemake `script:` can import it without dragging in the workflow package.
"""

import csv
import json
from pathlib import Path

# Leading characters that make a spreadsheet treat a cell as a formula rather
# than as text. A gene name never starts with one, but tool output is not ours
# and a downloaded CSV is opened in Excel.
_FORMULA_PREFIXES = ("=", "+", "-", "@", "\t", "\r")


def read_csv_rows(input_path):
	"""Every row of a CSV, or [] when it is missing.

	mefinder prefixes its call table with '#' comment lines (a version/date
	banner), which csv would otherwise hand back as the field names, so they are
	dropped here rather than at each call site."""
	if not input_path or not Path(input_path).exists():
		return []
	with open(input_path, newline="") as file_handle:
		data_lines = [line for line in file_handle if not line.startswith("#")]
	return list(csv.DictReader(data_lines))


def read_json(input_path, default=None):
	"""Parsed JSON, or ``default`` ({} unless given) when the file is missing or
	unreadable.

	Unreadable counts as absent on purpose: one sample's malformed output should
	cost that sample its panel, not cost the batch its report."""
	fallback = {} if default is None else default
	if not input_path or not Path(input_path).exists():
		return fallback
	try:
		with open(input_path) as file_handle:
			return json.load(file_handle)
	except (json.JSONDecodeError, OSError):
		return fallback


def safe_spreadsheet_value(value):
	"""Stop a downloaded CSV cell being interpreted as a formula."""
	if isinstance(value, str) and value.startswith(_FORMULA_PREFIXES):
		return "'" + value
	return value


def write_csv(output_path, fieldnames, rows):
	"""Write report rows to CSV, every cell guarded against formula injection."""
	output_path = Path(output_path)
	output_path.parent.mkdir(parents=True, exist_ok=True)
	with open(output_path, "w", newline="") as file_handle:
		writer = csv.DictWriter(file_handle, fieldnames=fieldnames)
		writer.writeheader()
		writer.writerows(
			[{key: safe_spreadsheet_value(value) for key, value in row.items()} for row in rows]
		)
