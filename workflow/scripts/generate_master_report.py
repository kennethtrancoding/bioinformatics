"""
Generate Master Report Script (Snakemake rule: generate_master_report)
Generate one CSV summarizing the whole batch: per-isolate species (+ confidence),
MLST sequence type, beta-lactamase genes, antibiotic-inactivation genes,
and the mobile elements themselves.
"""

from collections import Counter
from pathlib import Path

from report_io import read_csv_rows, read_json, write_csv

sample_ids = snakemake.params.sample_ids
card_files = snakemake.input.card
mlst_files = snakemake.input.mlst
mobile_element_finder_files = snakemake.input.mobile_element_finder
colocation_files = snakemake.input.colocation
report_file = snakemake.output.csv_report

Path(report_file).parent.mkdir(parents=True, exist_ok=True)


def _resistance_summary(input_path):
	"""Return (beta_lactamase_genes: list[str], antibiotic_inactivation_genes: list[str])."""
	beta_lactamase_genes = []
	inactivation_genes = []
	for csv_row in read_csv_rows(input_path):
		# Keep only Perfect/Strict RGI calls. Loose hits are low-identity
		# partial/homology matches -- a genome yields dozens of them, and they
		# would otherwise flood these category columns with spurious genes.
		if (csv_row.get("cut_off") or "").strip().lower() == "loose":
			continue
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


def _mobile_element_genes(input_path):
	"""Every mobile element mefinder called, by name (ISEhe3, MITEEc1, ...).

	Reads mefinder's own call table rather than the type-count summary: the summary
	says how many insertion sequences and MITEs there are, and a count cannot tell
	you *which* element you are holding. (read_csv_rows strips mefinder's '#'
	comment banner, which csv would otherwise read as the field names.)"""
	element_names = [
		(csv_row.get("name") or "").strip()
		for csv_row in read_csv_rows(input_path)
		if (csv_row.get("name") or "").strip()
	]
	# One element can be called more than once (two copies of MITEEc1, say). Collapse
	# the repeats but keep their multiplicity, so the cell still carries the total the
	# old mobile_elements_total column reported -- naming the genes should not cost
	# the count.
	name_counts = Counter(element_names)
	return [
		element_name if name_counts[element_name] == 1 else f"{element_name} (x{name_counts[element_name]})"
		for element_name in sorted(name_counts)
	]


def _mge_linked_genes(input_path):
	"""Names of resistance genes found ON/near a mobile element (from the
	co-location summary JSON) -- the tutorial's public-health signal."""
	return read_json(input_path).get("mobile_element_linked_genes", []) or []


fieldnames = [
 "isolate_id",
 "species",
 "species_confidence_pct",
 "species_method",
 "mlst_scheme",
 "sequence_type",
 "beta_lactamase_genes",
 "antibiotic_inactivation_genes",
 "mobile_element_genes",
 "mobile_element_linked_resistance_genes",
]

report_rows = []
for sample_id, card_path, mlst_path, mobile_element_finder_path, colocation_path in zip(
 sample_ids, card_files, mlst_files, mobile_element_finder_files, colocation_files
):
	mlst_data = read_json(mlst_path)
	beta_lactamase_genes, inactivation_genes = _resistance_summary(card_path)
	linked_genes = _mge_linked_genes(colocation_path)
	mobile_element_genes = _mobile_element_genes(mobile_element_finder_path)
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
	  "mobile_element_genes": "; ".join(mobile_element_genes) if mobile_element_genes else "none",
	  "mobile_element_linked_resistance_genes": "; ".join(linked_genes) if linked_genes else "none",
	 }
	)

write_csv(report_file, fieldnames, report_rows)

print(f"✓ Master report generated: {report_file} ({len(report_rows)} isolate(s))")
