"""Reading CARD RGI's `rgi main` output.

RGI's JSON is nested (ORF -> CARD reference model -> hit) with bookkeeping keys
mixed in at every level, so getting the hits out of it takes a recursive walk
with three rules that are easy to get subtly wrong: skip `_`-prefixed keys,
recognise a leaf hit by the keys it carries, and remember that a hit's own key
is the model it matched, not the ORF it was found in.

That second level is the one to be careful about. RGI records *every* reference
model that passed the cutoff for an ORF, so a single AmpC beta-lactamase arrives
as ~220 hits -- ACT-1..ACT-194, CMH-*, CMY-*, MIR-* -- which are allelic
variants of that one enzyme in CARD, not separate genes in the isolate. Anything
that reports hits as genes has to collapse them first, which is ``iter_best_hits``.

Four scripts need those hits -- rgi_json_to_csv, blast_ncbi, mge_colocation and
extract_rgi_proteins -- and each used to carry its own copy of the walk and of
the leaf predicate, identical in all four down to the whitespace. They differ
only in what they do with a hit, which is what ``iter_best_hits`` yields.

This is a leaf module: it imports nothing from the pipeline, so any script run
as a Snakemake `script:` (or over `$CONDA_PREFIX/bin/python3`) can import it.
"""

import csv
from pathlib import Path

# RGI's JSON hits carry no coverage figure at all -- it appears only in the
# tab report `rgi main` writes alongside the JSON, under this column. The two
# outputs share no ORF identifier (the JSON keys hits by the BLAST subject id of
# the model they matched, the tab report by Prodigal header), but both record the
# ORF's contig and coordinates, so that triple is the join key.
TAB_COVERAGE_COLUMN = "Percentage Length of Reference Sequence"


# What identifies one ORF, in both of RGI's outputs. Used to group a JSON ORF's
# competing model hits and to join a hit to its tab-report row. Deliberately not
# the JSON's nesting depth: the same triple works whatever RGI nests hits under.
def orf_identity(hit):
	"""The (contig, start, end) triple naming the ORF a hit was found in."""
	return (str(hit.get("orf_from")), str(hit.get("orf_start")), str(hit.get("orf_end")))


def looks_like_hit(node_value):
	"""A leaf RGI hit dict carries identifying keys; containers don't."""
	return isinstance(node_value, dict) and (
		"ARO_name" in node_value or "type_match" in node_value or "model_name" in node_value
	)


def iter_hits(node, orf_key=None):
	"""Yield ``(orf_key, hit)`` for every candidate hit in an RGI JSON tree.

	``orf_key`` is the key of the container the hit sits under -- the ORF, whose
	key is RGI's Prodigal header. The hit's *own* key is the BLAST subject id of
	the CARD model it matched (``gnl|BL_ORD_ID|5148|hsp_num:0``) and is not an ORF
	identifier; treating it as one turns every allelic variant of a gene into a
	separate gene.

	This yields all of an ORF's competing models. Callers reporting genes want
	``iter_best_hits`` instead; this is the raw walk it is built on.
	"""
	if isinstance(node, dict):
		for node_key, node_value in node.items():
			if isinstance(node_key, str) and node_key.startswith("_"):
				continue  # skip _metadata and similar bookkeeping keys
			if looks_like_hit(node_value):
				yield orf_key, node_value
			elif isinstance(node_value, (dict, list)):
				yield from iter_hits(node_value, node_key)
	elif isinstance(node, list):
		for node_item in node:
			yield from iter_hits(node_item, orf_key)


def iter_best_hits(rgi_data):
	"""Yield ``(orf_id, hit, contig)`` once per ORF: the hit RGI itself reports.

	RGI keeps every CARD model that passed the cutoff for an ORF, so the JSON holds
	~220 hits for one AmpC gene and 5 for one FosA. Those are allelic variants of a
	single enzyme, and reporting each as a found gene is what made a 31-ORF isolate
	come back with 230 "antibiotic inactivation genes" against the 2 CARD's own site
	shows. `rgi main`'s tab report keeps only the highest-scoring model per ORF as
	Best_Hit_ARO, and selecting on ``bit_score`` reproduces that exactly, so this is
	RGI's own choice rather than a heuristic layered on top of it.

	ORFs are grouped by ``orf_identity`` rather than by their JSON key, so a hit is
	attributed to the ORF it records itself as coming from however RGI nested it.
	Yields in document order of each ORF's first appearance. ``contig`` comes from
	the hit's ``orf_from``; the ORF's JSON key is a Prodigal header, not a contig.
	"""
	best_by_orf = {}
	for orf_key, hit in iter_hits(rgi_data):
		try:
			bit_score = float(hit.get("bit_score") or 0)
		except (TypeError, ValueError):
			bit_score = 0.0
		identity = orf_identity(hit)
		incumbent = best_by_orf.get(identity)
		if incumbent is None or bit_score > incumbent[0]:
			best_by_orf[identity] = (bit_score, orf_key, hit)
	for _bit_score, orf_key, hit in best_by_orf.values():
		yield orf_key, hit, hit.get("orf_from") or None


def aro_category_names(hit, class_name):
	"""Names from a hit's ARO_category matching a class name ("Drug Class",
	"Resistance Mechanism", "AMR Gene Family"), in order and de-duplicated."""
	category = hit.get("ARO_category")
	if not isinstance(category, dict):
		return []
	names = []
	for category_entry in category.values():
		if not isinstance(category_entry, dict):
			continue
		if category_entry.get("category_aro_class_name") == class_name:
			category_name = category_entry.get("category_aro_name")
			if category_name and category_name not in names:
				names.append(category_name)
	return names


def extract_aro_category(hit, class_name):
	"""aro_category_names as one semicolon-joined string, for a report cell."""
	return "; ".join(aro_category_names(hit, class_name))


def load_tab_report(rgi_json_path):
	"""Index `rgi main`'s tab report by (contig, orf_start, orf_end).

	Returns {} when the report is missing or unreadable, so a caller working
	from a JSON-only result tree degrades to whatever the JSON holds rather
	than failing.
	"""
	tab_path = Path(rgi_json_path).with_suffix(".txt")
	if not tab_path.is_file():
		return {}
	try:
		with tab_path.open(newline="") as file_handle:
			return {
				(
					str(tab_row.get("Contig")),
					str(tab_row.get("Start")),
					str(tab_row.get("Stop")),
				): tab_row
				for tab_row in csv.DictReader(file_handle, delimiter="\t")
			}
	except (OSError, csv.Error):
		return {}


def tab_row_for_hit(tab_index, hit):
	"""The tab-report row describing the same ORF as this JSON hit, or {}."""
	return tab_index.get(orf_identity(hit)) or {}
