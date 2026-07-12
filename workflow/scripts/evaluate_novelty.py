"""
Evaluate Novelty Script (Snakemake rule: evaluate_novelty)
Evaluate novelty of resistance genes
"""

import json
from pathlib import Path

from rgi_json_to_csv import (
	TAB_COVERAGE_COLUMN,
	extract_aro_category,
	load_tab_report,
	tab_row_for_hit,
)

rgi_file = snakemake.input.rgi_json
novelty_file = snakemake.output.novelty_report
coverage_min = float(snakemake.params.coverage_min)
identity_min = float(snakemake.params.identity_min)

Path(novelty_file).parent.mkdir(parents=True, exist_ok=True)

with open(rgi_file, "r") as file_handle:
	rgi_data = json.load(file_handle)

# Coverage and drug class are absent from the JSON hits (see rgi_json_to_csv):
# coverage lives only in RGI's tab report, the drug class only under
# ARO_category. Without both, every hit was judged on identity alone and
# reported with an "unknown" antibiotic.
tab_index = load_tab_report(rgi_file)

# CARD RGI writes a flat dict keyed by ORF id; each ORF maps to one or more
# HSP hit dicts ({ORF: {hsp_key: {...hit...}}}). Flatten to a list of hits.
# Also support a top-level 'results'/'card_call_results' list from other RGI
# versions, or a plain list.
if isinstance(rgi_data, dict) and (rgi_data.get("results") or rgi_data.get("card_call_results")):
	novelty_results = rgi_data.get("results") or rgi_data.get("card_call_results")
elif isinstance(rgi_data, dict):
	novelty_results = []
	for orf_result_value in rgi_data.values():
		if isinstance(orf_result_value, dict):
			for high_scoring_pair in orf_result_value.values():
				if isinstance(high_scoring_pair, dict):
					novelty_results.append(high_scoring_pair)
elif isinstance(rgi_data, list):
	novelty_results = rgi_data
else:
	novelty_results = []

novelty_items = []
summary = {
 "total_hits": 0,
 "potential_novel_variants": 0,
 "coverage_threshold": coverage_min,
 "identity_threshold": identity_min,
}

for hit in novelty_results:
	summary["total_hits"] += 1
	tab_row = tab_row_for_hit(tab_index, hit)
	identity = (
	 hit.get("percent_identity")
	 or hit.get("identity")
	 or hit.get("sequence_identity")
	 or hit.get("identity_pct")
	 or hit.get("perc_identity")
	)
	coverage = (
	 hit.get("percent_coverage")
	 or hit.get("coverage")
	 or hit.get("subject_coverage")
	 or hit.get("coverage_pct")
	 or hit.get("percentage_length_of_reference_sequence")
	 or tab_row.get(TAB_COVERAGE_COLUMN)
	)
	gene_name = (
	 hit.get("gene_name")
	 or hit.get("model_name")
	 or hit.get("ARO_name")
	 or hit.get("best_hit_term")
	 or hit.get("predicted_genomic_context")
	 or hit.get("name", "unknown")
	)
	antibiotic = (
	 hit.get("drug_class")
	 or hit.get("drug_family")
	 or hit.get("drug")
	 or extract_aro_category(hit, "Drug Class")
	 or tab_row.get("Drug Class")
	 or "unknown"
	)

	try:
		identity = float(identity)
	except (TypeError, ValueError):
		identity = None

	try:
		coverage = float(coverage)
	except (TypeError, ValueError):
		coverage = None

	novel = False
	reasons = []
	if identity is not None and identity < identity_min:
		novel = True
		reasons.append(f"identity {identity:.1f}% < {identity_min}%")
	if coverage is not None and coverage < coverage_min:
		novel = True
		reasons.append(f"coverage {coverage:.1f}% < {coverage_min}%")

	if novel:
		summary["potential_novel_variants"] += 1

	novelty_items.append(
	 {
	  "gene": gene_name,
	  "antibiotic": antibiotic,
	  "identity_pct": identity if identity is not None else "N/A",
	  "coverage_pct": coverage if coverage is not None else "N/A",
	  "novelty_flag": "YES" if novel else "NO",
	  "reasons": "; ".join(reasons) if reasons else "PASS",
	 }
	)

with open(novelty_file, "w") as file_handle:
	file_handle.write("Novelty Evaluation Report\n")
	file_handle.write(f"Total hits: {summary['total_hits']}\n")
	file_handle.write(f"Potential novel variants: {summary['potential_novel_variants']}\n")
	file_handle.write(f"Coverage threshold: {summary['coverage_threshold']}%\n")
	file_handle.write(f"Identity threshold: {summary['identity_threshold']}%\n\n")
	file_handle.write("Gene\tAntibiotic\tIdentity_pct\tCoverage_pct\tNovel_flag\tNotes\n")
	for novelty_item in novelty_items:
		file_handle.write(
		 f"{novelty_item['gene']}\t{novelty_item['antibiotic']}\t{novelty_item['identity_pct']}\t{novelty_item['coverage_pct']}\t{novelty_item['novelty_flag']}\t{novelty_item['reasons']}\n"
		)

print("✓ Novelty evaluation complete")
