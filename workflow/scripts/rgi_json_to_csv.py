"""
RGI JSON → CSV table converter (called by Snakemake rule:
card_rgi_analysis)

Flattens the CARD RGI `rgi main` JSON into a CSV with one row per
resistance-gene hit. Walking RGI's nested structure and joining it to the tab
report is rgi_json.py's job -- this is only the row shape and the CLI.

Usage:
    python rgi_json_to_csv.py <rgi_results.json> <rgi_results.csv>
"""

import json
import sys
from pathlib import Path

from report_io import write_csv
from rgi_json import TAB_COVERAGE_COLUMN, extract_aro_category, iter_hits, load_tab_report, tab_row_for_hit

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

	tab_index = load_tab_report(input_json)
	output_rows = [
		_hit_to_row(orf_id, hit, contig, tab_index)
		for orf_id, hit, contig in iter_hits(rgi_data)
	]

	write_csv(output_csv, FIELDS, output_rows)

	print(f"✓ Wrote {len(output_rows)} resistance-gene hit(s) to {output_csv}")


if __name__ == "__main__":
	main()
