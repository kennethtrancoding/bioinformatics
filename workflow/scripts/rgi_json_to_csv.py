"""
RGI JSON → CSV table converter (called by Snakemake rule:
card_rgi_analysis)

Reads the CARD RGI `rgi main` JSON output and flattens it into a CSV with one
row per resistance-gene hit. RGI's JSON is nested (contig → ORF → hit), and the
drug class / resistance mechanism / AMR gene family live inside an
`ARO_category` sub-dict, so this walks the structure defensively and pulls the
fields the downstream report steps expect.

Usage:
    python rgi_json_to_csv.py <rgi_results.json> <rgi_results.csv>
"""

import csv
import html
import json
import sys
from pathlib import Path

# Columns written to the CSV, in order.
FIELDS = [
	"contig",
	"orf_id",
	"best_hit_aro",
	"model_name",
	"aro_accession",
	"cut_off",  # Perfect / Strict / Loose
	"percent_identity",
	"percent_coverage",  # % length of reference sequence
	"drug_class",
	"resistance_mechanism",
	"amr_gene_family",
	"orf_start",
	"orf_end",
	"orf_strand",
]


def _looks_like_hit(node_value):
	"""A leaf RGI hit dict carries identifying keys; containers don't."""
	return isinstance(node_value, dict) and (
		"ARO_name" in node_value or "type_match" in node_value or "model_name" in node_value
	)


def extract_aro_category(hit, class_name):
	"""Pull semicolon-joined names from ARO_category matching a class name."""
	category = hit.get("ARO_category")
	if not isinstance(category, dict):
		return ""
	names = []
	for category_entry in category.values():
		if not isinstance(category_entry, dict):
			continue
		if category_entry.get("category_aro_class_name") == class_name:
			category_name = category_entry.get("category_aro_name")
			if category_name and category_name not in names:
				names.append(category_name)
	return "; ".join(names)


# RGI's JSON hits carry no coverage figure at all -- it appears only in the
# tab report `rgi main` writes alongside the JSON, under this column. The two
# outputs share no ORF identifier (the JSON keys ORFs by BLAST ordinal, the tab
# report by Prodigal header), but both record the ORF's contig and coordinates,
# so that triple is the join key.
TAB_COVERAGE_COLUMN = "Percentage Length of Reference Sequence"


def tab_report_path(rgi_json_path):
	return Path(rgi_json_path).with_suffix(".txt")


def load_tab_report(rgi_json_path):
	"""Index `rgi main`'s tab report by (contig, orf_start, orf_end).

	Returns {} when the report is missing or unreadable, so a caller working
	from a JSON-only result tree degrades to whatever the JSON holds rather
	than failing.
	"""
	tab_path = tab_report_path(rgi_json_path)
	if not tab_path.is_file():
		return {}
	try:
		with tab_path.open(newline="") as file_handle:
			return {
				(tab_row.get("Contig"), str(tab_row.get("Start")), str(tab_row.get("Stop"))): tab_row
				for tab_row in csv.DictReader(file_handle, delimiter="\t")
			}
	except (OSError, csv.Error):
		return {}


def tab_row_for_hit(tab_index, hit):
	"""The tab-report row describing the same ORF as this JSON hit, or {}."""
	return (
		tab_index.get(
			(str(hit.get("orf_from")), str(hit.get("orf_start")), str(hit.get("orf_end")))
		)
		or {}
	)


def _walk(node, contig, output_rows, tab_index):
	"""Recursively collect hit dicts from the nested RGI structure."""
	if isinstance(node, dict):
		for node_key, node_value in node.items():
			if isinstance(node_key, str) and node_key.startswith("_"):
				continue  # skip _metadata and similar bookkeeping keys
			if _looks_like_hit(node_value):
				output_rows.append(_hit_to_row(node_key, node_value, contig, tab_index))
			elif isinstance(node_value, (dict, list)):
				# Descend; at the top level the key is the contig name.
				next_contig = node_key if contig is None and isinstance(node_key, str) else contig
				_walk(node_value, next_contig, output_rows, tab_index)
	elif isinstance(node, list):
		for node_item in node:
			_walk(node_item, contig, output_rows, tab_index)


def _hit_to_row(orf_id, hit, contig, tab_index=None):
	tab_row = tab_row_for_hit(tab_index or {}, hit)
	return {
		"contig": contig if contig is not None else "",
		"orf_id": orf_id,
		"best_hit_aro": hit.get("ARO_name", ""),
		"model_name": hit.get("model_name", ""),
		"aro_accession": hit.get("ARO_accession", ""),
		"cut_off": hit.get("type_match", ""),
		"percent_identity": hit.get("perc_identity", ""),
		"percent_coverage": hit.get("percentage_length_of_reference_sequence")
		or tab_row.get(TAB_COVERAGE_COLUMN, ""),
		"drug_class": extract_aro_category(hit, "Drug Class"),
		"resistance_mechanism": extract_aro_category(hit, "Resistance Mechanism"),
		"amr_gene_family": extract_aro_category(hit, "AMR Gene Family"),
		"orf_start": hit.get("orf_start", ""),
		"orf_end": hit.get("orf_end", ""),
		"orf_strand": hit.get("orf_strand", ""),
	}


def _safe_spreadsheet_value(value):
	if isinstance(value, str) and value.startswith(("=", "+", "-", "@", "\t", "\r")):
		return "'" + value
	return value


def main():
	if len(sys.argv) != 3:
		sys.exit("Usage: python rgi_json_to_csv.py <input.json> <output.csv>")

	input_json, output_csv = sys.argv[1], sys.argv[2]

	# RGI's --output_file appends ".json" itself, so the real file may be
	# "<name>.json" even when the rule asked for "<name>". Fall back to that.
	if not Path(input_json).exists() and Path(input_json + ".json").exists():
		input_json = input_json + ".json"

	if not Path(input_json).exists():
		sys.exit(f"RGI JSON not found: {input_json}")

	with open(input_json) as file_handle:
		rgi_data = json.load(file_handle)

	output_rows = []
	_walk(rgi_data, None, output_rows, load_tab_report(input_json))

	out_dir = Path(output_csv).parent
	out_dir.mkdir(parents=True, exist_ok=True)

	with open(output_csv, "w", newline="") as file_handle:
		writer = csv.DictWriter(file_handle, fieldnames=FIELDS)
		writer.writeheader()
		writer.writerows(
			[
				{key: _safe_spreadsheet_value(value) for key, value in row.items()}
				for row in output_rows
			]
		)

	print(f"✓ Wrote {len(output_rows)} resistance-gene hit(s) to {output_csv}")


if __name__ == "__main__":
	main()


def escape_html(value):
	return html.escape(str(value), quote=True)
