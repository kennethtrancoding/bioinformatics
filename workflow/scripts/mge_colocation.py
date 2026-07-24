"""
Antibiotic-resistance-gene / mobile-element co-location (Snakemake rule:
mobile_element_colocation).

Answers the tutorial's central public-health question: are the antibiotic
resistance genes located ON a mobile genetic element? MobileElementFinder
catalogs the MGEs and RGI locates the resistance genes -- both on the same
assembly contigs -- so this cross-references their coordinates: a resistance
gene is "mobile-element linked" when it overlaps a detected MGE, or sits within
a configurable window of one on the same contig (composite transposons flank
their cargo gene with IS elements rather than overlapping it).

Writes, per sample:
  <sample>_arg_mge_colocation.csv  one row per resistance gene, with its nearest
                                   MGE, overlap/distance, and on/off-MGE call
  <sample>_arg_mge_colocation.json summary counts for the master report
"""

import argparse
import csv
import json
import sys
from pathlib import Path

# Invoked as `python3 workflow/scripts/mge_colocation.py ...` from a shell rule,
# so the scripts directory is not already on the path.
sys.path.insert(0, str(Path(__file__).resolve().parent))

from rgi_json import extract_aro_category, iter_hits  # noqa: E402


def _contig_token(contig_name):
	"""mefinder stores the whole FASTA header ('assembly_contig_2 length ...');
	RGI stores just the id token. Compare on the first whitespace token."""
	return (contig_name or "").split()[0] if contig_name else ""


def _to_int(raw_value):
	try:
		return int(float(raw_value))
	except (TypeError, ValueError):
		return None


def load_mges(mge_csv):
	"""Parse a mefinder results CSV (leading '#' comment lines, then a header)
	into [{name, type, contig, start, end}]."""
	input_path = Path(mge_csv)
	if not input_path.exists():
		return []
	with input_path.open() as file_handle:
		data_lines = [data_line for data_line in file_handle if not data_line.startswith("#")]
	mobile_genetic_elements = []
	for csv_row in csv.DictReader(data_lines):
		start_position, end_position = _to_int(csv_row.get("start")), _to_int(csv_row.get("end"))
		if start_position is None or end_position is None:
			continue
		mobile_genetic_elements.append(
			{
				"name": (csv_row.get("name") or "").strip(),
				"type": (csv_row.get("type") or "").strip(),
				"contig": _contig_token(csv_row.get("contig")),
				"start": min(start_position, end_position),
				"end": max(start_position, end_position),
			}
		)
	return mobile_genetic_elements


def load_ar_genes(rgi_json):
	"""Parse RGI results JSON into [{gene, mechanisms, contig, start, end}],
	de-duplicated by (gene, contig, start, end)."""
	input_path = rgi_json
	if not Path(input_path).exists() and Path(str(input_path) + ".json").exists():
		input_path = str(input_path) + ".json"
	try:
		rgi_data = json.load(open(input_path))
	except (OSError, ValueError):
		return []

	resistance_genes, seen = [], set()
	for _orf_id, hit, contig in iter_hits(rgi_data):
		start_position, end_position = (
			_to_int(hit.get("orf_start")),
			_to_int(hit.get("orf_end")),
		)
		if start_position is None or end_position is None:
			continue
		contig_name = _contig_token(hit.get("orf_from") or (contig or ""))
		gene_name = hit.get("ARO_name", "unknown")
		gene_location_key = (gene_name, contig_name, start_position, end_position)
		if gene_location_key in seen:
			continue
		seen.add(gene_location_key)
		resistance_genes.append(
			{
				"gene": gene_name,
				"mechanisms": extract_aro_category(hit, "Resistance Mechanism"),
				"contig": contig_name,
				"start": min(start_position, end_position),
				"end": max(start_position, end_position),
			}
		)
	return resistance_genes


def _distance(first_start, first_end, second_start, second_end):
	"""0 if the intervals overlap, else the gap between them."""
	if first_end < second_start:
		return second_start - first_end
	if second_end < first_start:
		return first_start - second_end
	return 0


def colocate(card_genes, mobile_genetic_elements, proximity_bp):
	"""For each AR gene, find the nearest MGE on the same contig and decide whether
	it's mobile-element linked (overlap or within proximity_bp)."""
	colocation_rows = []
	for card_gene in card_genes:
		same_contig = [
			mobile_genetic_element
			for mobile_genetic_element in mobile_genetic_elements
			if mobile_genetic_element["contig"] == card_gene["contig"]
		]
		overlapping, nearest, nearest_dist = [], None, None
		for mobile_genetic_element in same_contig:
			distance_bp = _distance(
				card_gene["start"],
				card_gene["end"],
				mobile_genetic_element["start"],
				mobile_genetic_element["end"],
			)
			if distance_bp == 0:
				overlapping.append(mobile_genetic_element)
			if nearest_dist is None or distance_bp < nearest_dist:
				nearest_dist, nearest = distance_bp, mobile_genetic_element
		linked_rows = (
			nearest is not None and nearest_dist is not None and nearest_dist <= proximity_bp
		)
		colocation_rows.append(
			{
				"resistance_gene": card_gene["gene"],
				"mechanism": card_gene["mechanisms"],
				"contig": card_gene["contig"],
				"gene_start": card_gene["start"],
				"gene_end": card_gene["end"],
				"on_mobile_element": "yes" if linked_rows else "no",
				"overlapping_mges": "; ".join(
					sorted(
						{mobile_genetic_element["name"] for mobile_genetic_element in overlapping}
					)
				),
				"nearest_mge": nearest["name"] if nearest else "",
				"nearest_mge_type": nearest["type"] if nearest else "",
				"nearest_distance_bp": nearest_dist if nearest is not None else "",
			}
		)
	return colocation_rows


def main(argv=None):
	argument_parser = argparse.ArgumentParser(description=__doc__)
	argument_parser.add_argument("--mge-csv", required=True)
	argument_parser.add_argument("--rgi-json", required=True)
	argument_parser.add_argument("--out-csv", required=True)
	argument_parser.add_argument("--out-json", required=True)
	argument_parser.add_argument("--sample-id", default="")
	argument_parser.add_argument("--proximity-bp", type=int, default=5000)
	parsed_args = argument_parser.parse_args(argv)

	mobile_genetic_elements = load_mges(parsed_args.mge_csv)
	card_genes = load_ar_genes(parsed_args.rgi_json)
	colocation_rows = colocate(card_genes, mobile_genetic_elements, parsed_args.proximity_bp)

	Path(parsed_args.out_csv).parent.mkdir(parents=True, exist_ok=True)
	field_names = [
		"resistance_gene",
		"mechanism",
		"contig",
		"gene_start",
		"gene_end",
		"on_mobile_element",
		"overlapping_mges",
		"nearest_mge",
		"nearest_mge_type",
		"nearest_distance_bp",
	]
	with open(parsed_args.out_csv, "w", newline="") as file_handle:
		csv_writer = csv.DictWriter(file_handle, fieldnames=field_names)
		csv_writer.writeheader()
		csv_writer.writerows(colocation_rows)

	linked_rows = [
		colocation_row
		for colocation_row in colocation_rows
		if colocation_row["on_mobile_element"] == "yes"
	]
	summary = {
		"sample_id": parsed_args.sample_id,
		"mges_detected": len(mobile_genetic_elements),
		"resistance_genes_total": len(colocation_rows),
		"resistance_genes_on_mge": len(linked_rows),
		"proximity_bp": parsed_args.proximity_bp,
		"mobile_element_linked_genes": sorted(
			{colocation_row["resistance_gene"] for colocation_row in linked_rows}
		),
	}
	with open(parsed_args.out_json, "w") as file_handle:
		json.dump(summary, file_handle, indent=2)

	print(
		f"✓ co-location [{parsed_args.sample_id}]: {len(linked_rows)}/{len(colocation_rows)} resistance gene(s) "
		f"on/near a mobile element (≤{parsed_args.proximity_bp} bp) → {parsed_args.out_csv}"
	)


if __name__ == "__main__":
	main()
