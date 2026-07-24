"""Reading CARD RGI's `rgi main` output.

RGI's JSON is nested (contig -> ORF -> hit) with bookkeeping keys mixed in at
every level, so getting the hits out of it takes a recursive walk with three
rules that are easy to get subtly wrong: skip `_`-prefixed keys, recognise a
leaf hit by the keys it carries, and remember that the contig name is the
top-level key rather than a field on the hit.

Four scripts need those hits -- rgi_json_to_csv, blast_ncbi, mge_colocation and
extract_rgi_proteins -- and each used to carry its own copy of the walk and of
the leaf predicate, identical in all four down to the whitespace. They differ
only in what they do with a hit, which is what ``iter_hits`` yields.

This is a leaf module: it imports nothing from the pipeline, so any script run
as a Snakemake `script:` (or over `$CONDA_PREFIX/bin/python3`) can import it.
"""

import csv
from pathlib import Path

# RGI's JSON hits carry no coverage figure at all -- it appears only in the
# tab report `rgi main` writes alongside the JSON, under this column. The two
# outputs share no ORF identifier (the JSON keys ORFs by BLAST ordinal, the tab
# report by Prodigal header), but both record the ORF's contig and coordinates,
# so that triple is the join key.
TAB_COVERAGE_COLUMN = "Percentage Length of Reference Sequence"


def looks_like_hit(node_value):
	"""A leaf RGI hit dict carries identifying keys; containers don't."""
	return isinstance(node_value, dict) and (
		"ARO_name" in node_value or "type_match" in node_value or "model_name" in node_value
	)


def iter_hits(node, contig=None):
	"""Yield ``(orf_id, hit, contig)`` for every hit in an RGI JSON tree.

	``orf_id`` is the key the hit sits under, and ``contig`` the top-level key it
	was found beneath (None if it was not nested under one). Depth-first in
	document order, so callers that take "the first hit per ORF" keep the
	behaviour they had when each walked the tree itself.
	"""
	if isinstance(node, dict):
		for node_key, node_value in node.items():
			if isinstance(node_key, str) and node_key.startswith("_"):
				continue  # skip _metadata and similar bookkeeping keys
			if looks_like_hit(node_value):
				yield node_key, node_value, contig
			elif isinstance(node_value, (dict, list)):
				# Descend; at the top level the key is the contig name.
				next_contig = node_key if contig is None and isinstance(node_key, str) else contig
				yield from iter_hits(node_value, next_contig)
	elif isinstance(node, list):
		for node_item in node:
			yield from iter_hits(node_item, contig)


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
