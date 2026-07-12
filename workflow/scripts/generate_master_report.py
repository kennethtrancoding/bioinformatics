"""
Generate Master Report Script (Snakemake rule: generate_master_report)
Generate one CSV summarizing the whole batch: per-isolate species (+ confidence),
MLST sequence type, beta-lactamase genes, antibiotic-inactivation genes,
and mobile-element totals.
"""

import csv
import json
from pathlib import Path

sample_ids = snakemake.params.sample_ids
card_files = snakemake.input.card
mlst_files = snakemake.input.mlst
mobile_element_finder_files = snakemake.input.mobile_element_finder
colocation_files = snakemake.input.colocation
report_file = snakemake.output.csv_report

Path(report_file).parent.mkdir(parents=True, exist_ok=True)


def _safe_spreadsheet_value(value):
	"""Prevent downloaded CSV cells from being interpreted as formulas."""
	if isinstance(value, str) and value.startswith(("=", "+", "-", "@", "\t", "\r")):
		return "'" + value
	return value


def _mlst_data(input_path):
	if not Path(input_path).exists():
		return {}
	with open(input_path) as file_handle:
		return json.load(file_handle)


def _resistance_summary(input_path):
	"""Return (beta_lactamase_genes: list[str], antibiotic_inactivation_genes: list[str])."""
	beta_lactamase_genes = []
	inactivation_genes = []
	if not Path(input_path).exists():
		return beta_lactamase_genes, inactivation_genes
	with open(input_path, newline="") as file_handle:
		for csv_row in csv.DictReader(file_handle):
			family = (csv_row.get("amr_gene_family") or "").lower()
			mechanism = (csv_row.get("resistance_mechanism") or "").lower()
			gene = csv_row.get("best_hit_aro") or csv_row.get("model_name") or ""
			if not gene:
				continue
			if "beta-lactamase" in family:
				beta_lactamase_genes.append(gene)
			if "antibiotic inactivation" in mechanism:
				inactivation_genes.append(gene)
	return sorted(set(beta_lactamase_genes)), sorted(set(inactivation_genes))


def _mobile_element_total(input_path):
	if not Path(input_path).exists():
		return 0
	with open(input_path, newline="") as file_handle:
		for csv_row in csv.DictReader(file_handle):
			if csv_row.get("element_type") == "TOTAL":
				try:
					return int(csv_row.get("count") or 0)
				except ValueError:
					return 0
	return 0


def _mge_linked_genes(input_path):
	"""Names of resistance genes found ON/near a mobile element (from the
	co-location summary JSON) -- the tutorial's public-health signal."""
	if not Path(input_path).exists():
		return []
	try:
		with open(input_path) as file_handle:
			return json.load(file_handle).get("mobile_element_linked_genes", []) or []
	except (OSError, ValueError):
		return []


fieldnames = [
 "isolate_id",
 "species",
 "species_confidence_pct",
 "species_method",
 "mlst_scheme",
 "sequence_type",
 "beta_lactamase_genes",
 "antibiotic_inactivation_genes",
 "mobile_elements_total",
 "mobile_element_linked_resistance_genes",
]

report_rows = []
for sample_id, card_path, mlst_path, mobile_element_finder_path, colocation_path in zip(
 sample_ids, card_files, mlst_files, mobile_element_finder_files, colocation_files
):
	mlst_data = _mlst_data(mlst_path)
	beta_lactamase_genes, inactivation_genes = _resistance_summary(card_path)
	linked_genes = _mge_linked_genes(colocation_path)
	report_rows.append(
	 {
	  "isolate_id": sample_id,
	  "species": mlst_data.get("species", "N/A"),
	  "species_confidence_pct": mlst_data.get("species_support", "N/A"),
	  "species_method": mlst_data.get("species_method", "N/A"),
	  "mlst_scheme": mlst_data.get("scheme", "N/A"),
	  "sequence_type": mlst_data.get("st", "N/A"),
	  "beta_lactamase_genes": "; ".join(beta_lactamase_genes) if beta_lactamase_genes else "none",
	  "antibiotic_inactivation_genes": "; ".join(inactivation_genes) if inactivation_genes else "none",
	  "mobile_elements_total": _mobile_element_total(mobile_element_finder_path),
	  "mobile_element_linked_resistance_genes": "; ".join(linked_genes) if linked_genes else "none",
	 }
	)

with open(report_file, "w", newline="") as file_handle:
	writer = csv.DictWriter(file_handle, fieldnames=fieldnames)
	writer.writeheader()
	writer.writerows(
		[{key: _safe_spreadsheet_value(value) for key, value in row.items()} for row in report_rows]
	)

print(f"✓ Master report generated: {report_file} ({len(report_rows)} isolate(s))")
