"""
Generate Sample Report Script (Snakemake rule: generate_sample_report)
Generate HTML report for individual sample.

Each tab presents one upstream service's results in that service's own
vocabulary and visual convention, so a value can be cross-referenced against the
service it came from without re-mapping terms:

  BV-BRC  "Table N." captions and a bold key column -- the layout of BV-BRC's
          own FullGenomeReport.html.
  CARD    RGI's column names (Cut_Off / Best_Hit_ARO / Drug Class / AMR Gene
          Family) rather than this pipeline's paraphrase of them.
  BLAST   NCBI's result vocabulary (Description / Query Cover / Per. Ident /
          E value / Accession).
  MLST    PubMLST's allelic profile and rMLST's taxon-prediction table
          (Rank / Taxon / Support / Taxonomy).
  MEF     MobileElementFinder's own results page: the version banner, a
          "Displaying N of M" line, a contig overview, then a section per
          contig headed by its full defline. Grouped by contig --
          which is what the web service shows. Its plain-text result.txt groups
          by element type instead, and following that would match neither.
          Its Resistance column is filled from CARD, and says so: see
          _resistance_by_contig for why it cannot come from mefinder.

The report is served under a strict CSP (see view_result in frontend.py):

    default-src 'none'; style-src 'unsafe-inline'; img-src data:; sandbox

script-src falls back to default-src 'none', so the report cannot run any
JavaScript at all -- the radio-input tabs below are a CSS-only mechanism, not a
stylistic preference. The same fallback blocks external stylesheets, fonts and
images, so everything here is one inline <style> and a system font stack.
"""

import re
from pathlib import Path

from report_format import (
	MISSING,
	empty,
	escape_html,
	float_or_none,
	fraction_as_percent,
	generic_table,
	kv_table,
	number,
	percent,
	seq_box,
	support_bar,
	table,
)
from report_io import read_csv_rows, read_json


def _inputs_from_sample_dir(sample_dir, report_out=None):
	"""Map a per-sample results directory to the same inputs Snakemake passes.

	Lets the report be regenerated straight from an existing output tree --
	`python generate_sample_report.py <results>/<SAMPLE>` -- without going
	through the DAG, which on a stale or partial tree would try to rebuild every
	upstream rule (re-fetching deleted raw reads and so on). The rule keeps
	passing these same paths via `snakemake.input`; this is only another way in."""
	sample_dir = Path(sample_dir)
	sample = sample_dir.name
	return {
		"report_file": str(report_out or sample_dir / "summary" / "report.html"),
		"metrics_file": str(sample_dir / "02_assembly" / "genome_metrics.csv"),
		"card_file": str(sample_dir / "03_resistance" / "novelty_report.txt"),
		"rgi_file": str(sample_dir / "03_resistance" / "rgi_results.csv"),
		"rgi_json_file": str(sample_dir / "03_resistance" / "rgi_results.json"),
		"blast_file": str(sample_dir / "04_blast" / "blast_results.csv"),
		"mlst_file": str(sample_dir / "05_mlst" / "mlst_results.json"),
		"rmlst_raw_file": str(sample_dir / "05_mlst" / "rmlst_raw.json"),
		"mobile_element_finder_file": str(sample_dir / "06_mobile_elements" / "me_summary.csv"),
		"mge_calls_file": str(sample_dir / "06_mobile_elements" / f"{sample}.csv"),
		"colocation_file": str(
			sample_dir / "06_mobile_elements" / f"{sample}_arg_mge_colocation.json"
		),
		"colocation_calls_file": str(
			sample_dir / "06_mobile_elements" / f"{sample}_arg_mge_colocation.csv"
		),
	}


# Snakemake injects `snakemake` as a global when this runs as a rule `script:`;
# run directly it takes a sample directory on the command line instead.
if "snakemake" in globals():
	_paths = {
		"report_file": snakemake.output.html_report,
		"metrics_file": snakemake.input.assembly_metrics,
		"card_file": snakemake.input.card,
		"rgi_file": snakemake.input.rgi,
		"rgi_json_file": snakemake.input.rgi_json,
		"blast_file": snakemake.input.blast,
		"mlst_file": snakemake.input.mlst,
		"rmlst_raw_file": snakemake.input.rmlst_raw,
		"mobile_element_finder_file": snakemake.input.mobile_element_finder,
		"mge_calls_file": snakemake.input.mge_calls,
		"colocation_file": snakemake.input.colocation,
		"colocation_calls_file": snakemake.input.colocation_calls,
	}
else:
	import argparse

	parser = argparse.ArgumentParser(
		description="Render a per-sample HTML report from an existing results directory."
	)
	parser.add_argument("sample_dir", help="e.g. results/<JOB_ID>/<SAMPLE>")
	parser.add_argument(
		"-o", "--out", help="output path (default: <sample_dir>/summary/report.html)"
	)
	_args = parser.parse_args()
	_paths = _inputs_from_sample_dir(_args.sample_dir, _args.out)

report_file = _paths["report_file"]
metrics_file = _paths["metrics_file"]
card_file = _paths["card_file"]
rgi_file = _paths["rgi_file"]
rgi_json_file = _paths["rgi_json_file"]
blast_file = _paths["blast_file"]
mlst_file = _paths["mlst_file"]
rmlst_raw_file = _paths["rmlst_raw_file"]
mobile_element_finder_file = _paths["mobile_element_finder_file"]
mge_calls_file = _paths["mge_calls_file"]
colocation_file = _paths["colocation_file"]
colocation_calls_file = _paths["colocation_calls_file"]

Path(report_file).parent.mkdir(parents=True, exist_ok=True)


def _rgi_sequence_index(input_path):
	"""orf_id -> the predicted and CARD sequences RGI recorded for that hit.

	rgi_results.csv carries no sequences; rgi_results.json does -- the predicted
	ORF's protein/DNA (orf_prot_sequence / orf_dna_sequence), CARD's reference
	protein/DNA (sequence_from_broadstreet / dna_sequence_from_broadstreet), and
	the pairwise protein alignment (query / match / sequence_from_db). Keyed on
	the same orf_id the CSV rows use so the CARD panel can join the two."""
	index = {}

	def _walk(node):
		if isinstance(node, dict):
			for key, value in node.items():
				if isinstance(key, str) and key.startswith("_"):
					continue
				if isinstance(value, dict) and "orf_prot_sequence" in value:
					index[key] = value
				elif isinstance(value, (dict, list)):
					_walk(value)
		elif isinstance(node, list):
			for item in node:
				_walk(item)

	_walk(read_json(input_path))
	return index


def _linked_data_values(linked_data):
	"""The PubMLST "Linked data values" cell: a green database badge followed by
	the species/frequency breakdown recorded for this allele
	("species: Enterobacter pasteurii [n=3]; Enterobacter sp. [n=1]").

	`linked_data` is PubMLST's own nested shape -- {database: {field: [{value,
	frequency}, ...]}} -- and is only present when the query was made with
	details=True (parse_mlst does). Absent or empty, the allele still matched;
	the cell just has no distribution to show."""
	if not linked_data:
		return MISSING
	blocks = []
	for database, database_fields in linked_data.items():
		badge = f'<span class="linked-db">{escape_html(database)}</span>'
		field_parts = []
		for field_name, values in (database_fields or {}).items():
			rendered = "; ".join(
				f"<em>{escape_html(value.get('value', ''))}</em> "
				f"[n={escape_html(value.get('frequency', ''))}]"
				for value in values
			)
			field_parts.append(f"{escape_html(field_name)}: {rendered}")
		blocks.append(f"{badge} {'<br>'.join(field_parts)}")
	return "<br>".join(blocks)


# --- data ---------------------------------------------------------------

assembly_metrics = next(iter(read_csv_rows(metrics_file)), {})
mlst_result_data = read_json(mlst_file)
rmlst_raw = read_json(rmlst_raw_file)
rgi_hits = read_csv_rows(rgi_file)
rgi_sequences = _rgi_sequence_index(rgi_json_file)
blast_hits = read_csv_rows(blast_file)
mge_calls = read_csv_rows(mge_calls_file)
mef_summary_rows = read_csv_rows(mobile_element_finder_file)
colocation = read_json(colocation_file)
colocation_calls = read_csv_rows(colocation_calls_file)


def _contig_token(contig_name):
	"""The bare contig, dropping mefinder's trailing defline fields
	("assembly_contig_2 length 713330 coverage 135.2" -> "assembly_contig_2").
	The same normalisation mge_colocation.py applies, which is what lets its
	per-gene rows line up with mefinder's calls."""
	return (contig_name or "").split()[0] if contig_name else ""


def _resistance_by_contig():
	"""Resistance genes per contig, and which of them sit on a mobile element.

	These are CARD/RGI calls, not mefinder's. MobileElementFinder's own results
	page carries Resistance and Virulence columns, but they come from CGE running
	ResFinder and VirulenceFinder alongside it on the server -- the `mefinder`
	CLI this pipeline runs has no such flags and emits no such data (checked
	against MobileElementFinder 1.1.2: no --resistance, no --virulence, no
	mention of either anywhere in the package). So the column is filled from the
	tool that does call resistance here, and labelled as such rather than passed
	off as mefinder's."""
	by_contig = {}
	for call in colocation_calls:
		contig = _contig_token(call.get("contig"))
		if not contig:
			continue
		entry = by_contig.setdefault(contig, {"genes": set(), "on_mge": set()})
		gene = call.get("resistance_gene", "")
		entry["genes"].add(gene)
		if (call.get("on_mobile_element", "") or "").lower() == "yes":
			entry["on_mge"].add(gene)
	return by_contig


def _novelty_index():
	"""Novel_flag / Notes per hit, keyed by (gene, identity, coverage).

	novelty_report.txt and rgi_results.csv are both derived from the same
	rgi_results.json and are row-for-row parallel, but keying on the values
	rather than on position keeps the join honest if one of them is ever
	regenerated independently. adeF legitimately appears several times per
	sample at different identities, so the gene name alone is not a key.
	"""
	if not Path(card_file).exists():
		return {}, {}
	with open(card_file) as file_handle:
		report_lines = file_handle.read().splitlines()

	preamble = {}
	for report_line in report_lines:
		if ":" in report_line and "\t" not in report_line:
			label, _, value = report_line.partition(":")
			preamble[label.strip()] = value.strip()

	header = "Gene\tAntibiotic\tIdentity_pct\tCoverage_pct\tNovel_flag\tNotes"
	if header not in report_lines:
		return {}, preamble
	columns = header.split("\t")
	novelty_by_hit = {}
	for report_line in report_lines[report_lines.index(header) + 1 :]:
		if not report_line.strip():
			continue
		hit = dict(zip(columns, report_line.split("\t")))
		key = (
			hit.get("Gene"),
			float_or_none(hit.get("Identity_pct")),
			float_or_none(hit.get("Coverage_pct")),
		)
		novelty_by_hit[key] = hit
	return novelty_by_hit, preamble


novelty_by_hit, novelty_preamble = _novelty_index()


def _novelty_for(rgi_hit):
	return novelty_by_hit.get(
		(
			rgi_hit.get("best_hit_aro"),
			float_or_none(rgi_hit.get("percent_identity")),
			float_or_none(rgi_hit.get("percent_coverage")),
		),
		{},
	)


# --- panels -------------------------------------------------------------


def _summary_panel():
	"""Our own cross-service verdict. Every fact is tagged with the service that
	produced it, because that is the tab to open to check it."""
	species = mlst_result_data.get("species")
	sequence_type = mlst_result_data.get("st")
	rows = []

	if species:
		method = mlst_result_data.get("species_method", "")
		support = mlst_result_data.get("species_support")
		detail = f"MLST {escape_html(method)}"
		if support is not None:
			detail += f", {percent(support, 1)} support"
		rows.append(("Species", f"<em>{escape_html(species)}</em><br><small>{detail}</small>"))

	if sequence_type:
		scheme = mlst_result_data.get("scheme", "")
		rows.append(
			(
				"Sequence type",
				f"ST {escape_html(sequence_type)}<br><small>MLST "
				f"scheme {escape_html(scheme)}</small>",
			)
		)

	rst = (rmlst_raw.get("fields") or {}).get("rST")
	if rst:
		loci_matched = len(rmlst_raw.get("exact_matches") or {})
		rows.append(
			(
				"Ribosomal ST",
				f"rST {escape_html(rst)}<br><small>MLST "
				f"{loci_matched}/53 ribosomal loci matched</small>",
			)
		)

	if assembly_metrics:
		rows.append(
			(
				"Genome",
				f"{number(assembly_metrics.get('genome_length'))} bp in "
				f"{number(assembly_metrics.get('contigs'))} contigs<br>"
				f"<small>BV-BRC "
				f"GC {percent(assembly_metrics.get('gc_content'))}, "
				f"N50 {number(assembly_metrics.get('n50'))}</small>",
			)
		)

	if rgi_hits:
		novel_count = sum(
			1 for rgi_hit in rgi_hits if _novelty_for(rgi_hit).get("Novel_flag") == "YES"
		)
		identity_threshold = novelty_preamble.get("Identity threshold", "")
		rows.append(
			(
				"Resistance genes",
				f"{len(rgi_hits)} RGI hits, {novel_count} flagged potential novel variants<br>"
				f"<small>CARD below the "
				f"{escape_html(identity_threshold)} identity threshold</small>",
			)
		)

	if mge_calls:
		by_type = {}
		for mge_call in mge_calls:
			by_type[mge_call.get("type", "?")] = by_type.get(mge_call.get("type", "?"), 0) + 1
		breakdown = ", ".join(
			f"{count} {element_type}" for element_type, count in sorted(by_type.items())
		)
		rows.append(
			(
				"Mobile elements",
				f"{len(mge_calls)} detected<br><small>MEF {escape_html(breakdown)}</small>",
			)
		)

	# The colocation call is the point of the whole run -- a resistance gene
	# sitting on a mobile element is the one that can move between isolates --
	# and it had no home in this report before.
	if colocation:
		linked_genes = colocation.get("mobile_element_linked_genes") or []
		linked_count = colocation.get("resistance_genes_on_mge", len(linked_genes))
		value = f"{linked_count} resistance gene(s) on or near a mobile element"
		if linked_genes:
			value += "<br><small>MEF " + escape_html(", ".join(linked_genes))
			value += f" (within {number(colocation.get('proximity_bp'))} bp)</small>"
		rows.append(("Mobile-element linked", value))

	if not rows:
		return empty("No data available.")
	return kv_table(rows)


def _bvbrc_panel():
	"""BV-BRC's own Table 1: kv-table, bold keys, thousands separators."""
	if not assembly_metrics:
		return empty("No assembly metrics available.")
	return kv_table(
		[
			("Contigs", number(assembly_metrics.get("contigs"))),
			("GC Content", percent(assembly_metrics.get("gc_content"))),
			("Genome Length", f"{number(assembly_metrics.get('genome_length'))} bp"),
			("Contig N50", number(assembly_metrics.get("n50"))),
			("Contig L50", number(assembly_metrics.get("l50"))),
		],
		caption="Table 1. Assembly Details",
	)


def _pairwise_alignment(hit, width=60):
	"""RGI's predicted-vs-CARD protein alignment, laid out BLAST-style: the
	predicted ORF (`query`) over the match midline over CARD's reference
	(`sequence_from_db`), wrapped at `width` residues with running positions."""
	query = hit.get("query") or ""
	match = hit.get("match") or ""
	subject = hit.get("sequence_from_db") or ""
	if not query or not subject:
		return ""

	# Numbered from 1 along the aligned region. RGI's query_start/hit_start are
	# nucleotide coordinates on the contig and CARD DNA (query_start == orf_start),
	# not protein-residue indices, so they cannot label an amino-acid alignment.
	query_pos, subject_pos = 1, 1
	lines = []
	for offset in range(0, len(query), width):
		query_chunk = query[offset : offset + width]
		match_chunk = match[offset : offset + width]
		subject_chunk = subject[offset : offset + width]
		query_residues = len(query_chunk) - query_chunk.count("-")
		subject_residues = len(subject_chunk) - subject_chunk.count("-")
		query_end = query_pos + query_residues - 1 if query_residues else query_pos
		subject_end = subject_pos + subject_residues - 1 if subject_residues else subject_pos
		lines.append(f"{'Predicted':<9} {query_pos:>6}  {escape_html(query_chunk)}  {query_end}")
		lines.append(f"{'':<9} {'':>6}  {escape_html(match_chunk)}")
		lines.append(f"{'CARD':<9} {subject_pos:>6}  {escape_html(subject_chunk)}  {subject_end}")
		lines.append("")
		query_pos += query_residues
		subject_pos += subject_residues
	return '<pre class="aln">' + "\n".join(lines).rstrip() + "</pre>"


def _card_panel():
	"""RGI's vocabulary, not ours: the columns a CARD user already knows.

	RGI blasts each predicted ORF protein from the assembly against CARD's
	reference proteins, so every row is one such query. The queries are numbered
	Query 1..N and listed at the top, and each row carries the inputted predicted
	protein (the ORF) beside the CARD database protein it matched, with the
	percent identity between the two."""
	if not rgi_hits:
		return empty("No resistance gene hits.")
	if "best_hit_aro" not in rgi_hits[0]:
		return generic_table(rgi_hits, caption=f"RGI hits ({len(rgi_hits)})")

	# What protein each numbered query is: Query N -> the CARD gene it matche

	rows = []
	for index, rgi_hit in enumerate(rgi_hits, start=1):
		# Perfect / Strict / Loose are RGI's own detection paradigms.
		rows.append(
			[
				f"<span>{index}</span>",
				f'<span class="mono wrap">{escape_html(rgi_hit.get("orf_id", "") or MISSING)}</span>',
				f"<strong>{escape_html(rgi_hit.get('best_hit_aro', ''))}</strong>",
				f"<span>ARO:{escape_html(rgi_hit.get('aro_accession', ''))}</span>",
				escape_html(rgi_hit.get("cut_off", "")),
				percent(rgi_hit.get("percent_identity")),
				f'<span class="wrap">{escape_html(rgi_hit.get("drug_class", ""))}</span>',
				escape_html(rgi_hit.get("resistance_mechanism", "")),
				f'<span class="wrap">{escape_html(rgi_hit.get("amr_gene_family", ""))}</span>',
			]
		)
	table_html = table(
		[
			"Query",
			"ORF id",
			"CARD reference (Best_Hit_ARO)",
			"ARO",
			"Cut_Off",
			"Best_Identities",
			"Drug Class",
			"Resistance Mechanism",
			"AMR Gene Family",
		],
		rows,
		caption=f"RGI hits ({len(rows)})",
	)

	# The predicted ORF's actual protein/DNA against CARD's, per query: the
	# pairwise protein alignment RGI computed, then both DNA sequences. Long, so
	# each query collapses (native <details>, no script -- the CSP forbids JS) and
	# every sequence sits in its own scrollable box. Comes from rgi_results.json,
	# joined to the CSV rows on orf_id; absent (older tree) it is simply skipped.
	sequence_sections = []
	for index, rgi_hit in enumerate(rgi_hits, start=1):
		hit = rgi_sequences.get(rgi_hit.get("orf_id", ""))
		if not hit:
			continue
		alignment = _pairwise_alignment(hit)
		predicted_dna = hit.get("orf_dna_sequence") or ""
		card_dna = hit.get("dna_sequence_from_broadstreet") or ""
		body = ""
		if alignment:
			body += "<h4>Protein alignment, predicted vs CARD</h4>" + alignment
		if predicted_dna or card_dna:
			body += (
				'<div class="seq-grid">'
				f"<div><h4>Predicted DNA ({len(predicted_dna):,} bp)</h4>{seq_box(predicted_dna)}</div>"
				f"<div><h4>CARD reference DNA ({len(card_dna):,} bp)</h4>{seq_box(card_dna)}</div>"
				"</div>"
			)
		if not body:
			continue
		sequence_sections.append(
			f'<details class="seq-details"><summary>{index}: '
			f"{escape_html(rgi_hit.get('best_hit_aro', '') or '?')} "
			f'<span class="mono">{escape_html(rgi_hit.get("orf_id", ""))}</span></summary>'
			f"{body}</details>"
		)
	sequences_html = ""
	if sequence_sections:
		sequences_html = (
			'<h3 class="section-head">Query sequences, predicted vs CARD</h3>'
			+ "".join(sequence_sections)
		)

	return table_html + sequences_html


def _blast_panel():
	"""NCBI's result vocabulary: Description / Query Cover / Per. Ident / Accession."""
	if not blast_hits:
		return empty("No BLAST hits.")
	if "query_gene" not in blast_hits[0]:
		return generic_table(
			blast_hits, caption=f"BLAST hits on proteins predicted by CARD ({len(blast_hits)})"
		)
	rows = []
	for blast_hit in blast_hits:
		is_novel = (blast_hit.get("is_novel", "") or "").lower() == "yes"
		rows.append(
			[
				f"<strong>{escape_html(blast_hit.get('query_gene', ''))}</strong>",
				f'<span class="wrap">{escape_html(blast_hit.get("ncbi_top_hit", ""))}</span>',
				f'<span class="mono">{escape_html(blast_hit.get("ncbi_accession", ""))}</span>',
				percent(blast_hit.get("ncbi_identity_pct")),
				percent(blast_hit.get("ncbi_coverage_pct")),
				percent(blast_hit.get("card_identity_pct")),
				"novel" if is_novel else "known",
				escape_html(blast_hit.get("source", "")),
				escape_html(blast_hit.get("location", "")),
			]
		)
	return table(
		[
			"Query",
			"Description",
			"Accession",
			"Per. Ident",
			"Query Cover",
			"CARD Ident",
			"Database",
			"Location",
		],
		rows,
		caption=f"Top hit per query protein ({len(rows)})",
	)


def _mlst_panel():
	"""PubMLST's own rMLST results page: the 7-gene allelic profile a PubMLST
	user keys on, then rMLST's "Predicted taxa" table and every exact ribosomal-
	locus match, laid out the way pubmlst.org/rmlst renders them -- a support bar
	per taxon and a green "linked data values" badge per locus."""
	if not mlst_result_data and not rmlst_raw:
		return empty("No MLST data available.")

	panels = []

	profile = mlst_result_data.get("alleles") or []
	profile_html = ", ".join(f"<span>{escape_html(a)}</span>" for a in profile)
	panels.append(
		kv_table(
			[
				("Scheme", escape_html(mlst_result_data.get("scheme", "N/A"))),
				("ST", f"<strong>{escape_html(mlst_result_data.get('st', 'N/A'))}</strong>"),
				("Allelic profile", profile_html or "N/A"),
			],
			caption="MLST (PubMLST scheme)",
		)
	)

	# rmlst_raw is always written by parse_mlst.py, but carries {"error": ...}
	# when PubMLST was unreachable -- in which case mlst_results.json fell back
	# to the scheme label and there is no rST to show.
	if rmlst_raw.get("error"):
		panels.append(empty(f"rMLST species ID unavailable: {rmlst_raw['error']}"))
		return "".join(panels)

	fields = rmlst_raw.get("fields") or {}
	exact_matches = rmlst_raw.get("exact_matches") or {}

	# Predicted taxa -- rMLST's ranked calls, support drawn as a bar.
	taxon_rows = []
	for taxon in rmlst_raw.get("taxon_prediction") or []:
		taxon_rows.append(
			[
				escape_html(str(taxon.get("rank", "")).upper()),
				f"<em>{escape_html(taxon.get('taxon', ''))}</em>",
				support_bar(taxon.get("support")),
				f'<span class="wrap lineage">{escape_html(taxon.get("taxonomy", ""))}</span>',
			]
		)
	if taxon_rows:
		panels.append('<h3 class="pubmlst-head">Predicted taxa</h3>')
		panels.append(
			table(
				["Rank", "Taxon", "Support", "Taxonomy"],
				taxon_rows,
				table_class="predicted-taxa",
			)
		)

	# rST / species identity, and how many of the 53 ribosomal loci matched
	# exactly, over the per-locus detail table -- the "N exact matches found"
	# banner PubMLST prints above that table.
	if fields or exact_matches:
		panels.append(
			'<div class="upload-note">'
			f"<p><strong>rST:</strong> {escape_html(fields.get('rST', 'N/A'))} &nbsp;"
			f"<strong>Species:</strong> <em>{escape_html(fields.get('species', 'N/A'))}</em></p>"
			f"<p>{len(exact_matches)}/53 ribosomal loci matched exactly.</p></div>"
		)

	# Where each matched allele sits on the assembly, and its linked data. Each
	# locus maps to a list of matches; details (length/contig/coordinates/
	# linked_data) are only present when the rMLST query used details=True, so
	# every cell degrades to "—" rather than assuming they are there.
	match_rows = []
	for locus in sorted(exact_matches):
		for match in exact_matches[locus] or []:
			match_rows.append(
				[
					escape_html(locus),
					escape_html(match.get("allele_id", "")),
					number(match.get("length")),
					f'<span class="wrap mono">{escape_html(match.get("contig", ""))}</span>',
					number(match.get("start")),
					number(match.get("end")),
					_linked_data_values(match.get("linked_data")),
					escape_html(match.get("flag", "") or ""),
				]
			)
	if match_rows:
		panels.append(
			table(
				[
					"Locus",
					"Allele",
					"Length",
					"Contig",
					"Start position",
					"End position",
					"Linked data values",
					"Flags",
				],
				match_rows,
				table_class="exact-matches",
			)
		)

	return "".join(panels)


def _mef_panel():
	"""mefinder's own results page, as closely as this data allows: the version
	banner, the count line, a contig overview, then a section per contig headed
	by its full defline. It groups by contig -- not by element type, which is how
	its plain-text result.txt groups them and is not what a user coming from the
	web service is looking at."""
	panels = []

	# mefinder writes the whole FASTA defline into `contig`
	# ("assembly_contig_2 length 713330 coverage 135.2 normalized_cov 1.05"),
	# which is exactly what its results page uses as each section's heading. Key
	# on the bare token so the resistance rows can be joined to it, and keep the
	# defline for the heading.
	calls_by_contig = {}
	deflines = {}
	for mge_call in mge_calls:
		defline = mge_call.get("contig", "") or "unknown"
		contig = _contig_token(defline) or "unknown"
		deflines.setdefault(contig, defline)
		calls_by_contig.setdefault(contig, []).append(mge_call)

	resistance_by_contig = _resistance_by_contig()

	if not mge_calls and not resistance_by_contig:
		panels.append(empty("No mobile genetic elements detected."))
		return "".join(panels)

	# The union, not just the contigs carrying an element: mefinder's own
	# overview lists a contig with 0 MGEs when it has a resistance hit.
	# Natural order, or assembly_contig_10 sorts above assembly_contig_2.
	contigs_in_order = sorted(
		set(calls_by_contig) | set(resistance_by_contig),
		key=lambda contig: [
			int(part) if part.isdigit() else part for part in re.split(r"(\d+)", contig)
		],
	)
	# Only contigs with a section of their own can be linked to.
	anchor_for = {
		contig: index
		for index, contig in enumerate(c for c in contigs_in_order if c in calls_by_contig)
	}

	# "N of M" the way mefinder's page counts: M is parse_mefinder's TOTAL row,
	# which is the whole call set, and N is what this table shows of it. They
	# match unless a call was dropped between the two.
	total_called = next(
		(row.get("count") for row in mef_summary_rows if row.get("element_type") == "TOTAL"),
		len(mge_calls),
	)
	panels.append(
		f'<p class="displaying">Displaying: <strong>{len(mge_calls)}</strong> of '
		f"<strong>{escape_html(total_called)}</strong> mobile elements</p>"
	)

	overview_rows = []
	for contig in contigs_in_order:
		calls = calls_by_contig.get(contig, [])
		contig_cell = escape_html(contig)
		if contig in anchor_for:
			contig_cell = f'<a href="#contig-{anchor_for[contig]}">{contig_cell}</a>'
		resistance = resistance_by_contig.get(contig, {"genes": set(), "on_mge": set()})
		genes = resistance["genes"]
		# 162 distinct genes sit on the chromosome of a real isolate, so the
		# names cannot go in this cell -- the CARD tab lists them in full. The
		# ones on a mobile element are the few worth naming here, and are the
		# question this pipeline exists to answer.
		if genes:
			resistance_cell = f"{', '.join(genes)}"
			if resistance["on_mge"]:
				resistance_cell += (
					f"<br><small>on MGE "
					f"{escape_html(', '.join(sorted(resistance['on_mge'])))}</small>"
				)
		else:
			resistance_cell = MISSING
		overview_rows.append(
			[
				contig_cell,
				number(len(calls)),
				escape_html(", ".join(sorted({call.get("name", "") for call in calls})))
				or MISSING,
				resistance_cell,
			]
		)
	panels.append(
		table(
			["Contig", "#MGEs", "Mobile elements", "Resistance (CARD)"],
			overview_rows,
			table_class="greyhead",
		)
	)
	# The Resistance column is a CARD/RGI join done here by contig, not a field
	# mefinder reported: MobileElementFinder 1.1.2 has no --resistance flag. Say
	# so, or the column reads as something the tool called.

	for index, contig in enumerate(c for c in contigs_in_order if c in calls_by_contig):
		calls = calls_by_contig[contig]
		contig_defline = deflines.get(contig, contig)
		rows = []
		for mge_call in sorted(calls, key=lambda call: int(call.get("start") or 0)):
			template_length = mge_call.get("allele_len")
			rows.append(
				[
					f"<strong>{escape_html(mge_call.get('name', ''))}</strong>",
					escape_html(mge_call.get("type", "")),
					escape_html(mge_call.get("prediction", "")),
					fraction_as_percent(mge_call.get("identity")),
					fraction_as_percent(mge_call.get("coverage")),
					f"{number(template_length)} bp" if template_length else MISSING,
					f'<span class="mono">{number(mge_call.get("start"))}-'
					f"{number(mge_call.get('end'))}</span>",
				]
			)
		panels.append(
			f'<h3 class="contig" id="contig-{index}">Contig: {escape_html(contig_defline)}</h3>'
			"<h4>Mobile element results</h4>"
			+ table(
				[
					"Mge name",
					"Type",
					"Prediction",
					"Identity",
					"Coverage",
					"Template length",
					"Position in contig",
				],
				rows,
				table_class="plain",
			)
		)

	if colocation:
		linked_genes = colocation.get("mobile_element_linked_genes") or []
		panels.append(
			kv_table(
				[
					("MGEs detected", number(colocation.get("mges_detected"))),
					("Resistance genes total", number(colocation.get("resistance_genes_total"))),
					("Resistance genes on MGE", number(colocation.get("resistance_genes_on_mge"))),
					("Proximity window", f"{number(colocation.get('proximity_bp'))} bp"),
					(
						"Linked genes",
						" ".join(
							f'<span class="allele">{escape_html(g)}</span>' for g in linked_genes
						)
						or "None",
					),
				],
				caption="ARG / MGE colocation",
			)
		)

	return "".join(panels)


# --- document -----------------------------------------------------------

sample_id = mlst_result_data.get("sample") or colocation.get("sample_id") or "Sample"

TABS = [
	("summary", "Summary", _summary_panel()),
	("bvbrc", "BV-BRC", _bvbrc_panel()),
	("card", "CARD", _card_panel()),
	("blast", "BLAST", _blast_panel()),
	("mlst", "MLST", _mlst_panel()),
	("mef", "MEF", _mef_panel()),
]

radios = "".join(
	f'<input type="radio" id="{key}" name="tabs"{" checked" if index == 0 else ""}>'
	for index, (key, _, _) in enumerate(TABS)
)
labels = "".join(f'<label for="{key}">{escape_html(title)}</label>' for key, title, _ in TABS)
panels = "".join(
	f'<section id="{key}-content" class="content"><h2>{escape_html(title)}</h2>{body}</section>'
	for key, title, body in TABS
)
checked_rules = ",\n".join(f"#{key}:checked ~ .contents #{key}-content" for key, _, _ in TABS)
label_rules = ",\n".join(f'#{key}:checked ~ .labels label[for="{key}"]' for key, _, _ in TABS)

html_content = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>{escape_html(sample_id)}: Sample Report</title>
<style>
:root {{ --accent: #334155; --line: #ccc; --muted: #777; }}
* {{ box-sizing: border-box; }}
body {{
  font-family: system-ui, -apple-system, "Segoe UI", sans-serif;
  color: #222;
  margin: 0;
  padding: 1.5rem;
  line-height: 1.45;
}}
h1 {{ font-size: 1.3rem; margin: 0 0 0.25rem; }}
h1 .sample {{ font-family: ui-monospace, SFMono-Regular, Menlo, monospace; }}
h2 {{ font-size: 1.1rem; font-weight: 500; margin: 0 0 1rem; color: var(--accent); }}
.sub {{ color: var(--muted); font-size: 0.85rem; margin: 0 0 1.25rem; }}

.tabs input[type="radio"] {{ position: absolute; opacity: 0; pointer-events: none; }}
.labels {{ display: flex; flex-wrap: wrap; gap: 2px; border-bottom: 2px solid var(--line); }}
.labels label {{
  padding: 0.5rem 1rem;
  cursor: pointer;
  font-size: 0.9rem;
  color: var(--muted);
  border-bottom: 3px solid transparent;
  margin-bottom: -2px;
}}
.labels label:hover {{ color: #222; }}
.content {{ display: none; padding: 1.5rem 0; }}
{checked_rules} {{ display: block; }}
{label_rules} {{ color: var(--accent); border-bottom-color: var(--accent); font-weight: 600; }}

/* Keyboard focus is the only affordance a CSS-only tabstrip has; the radios
   themselves are off-screen, so the ring has to be drawn on the label. */
.tabs input[type="radio"]:focus-visible ~ .labels label {{ outline: none; }}
{",".join(f'#{key}:focus-visible ~ .labels label[for="{key}"]' for key, _, _ in TABS)} {{
  outline: 2px solid black;
  outline-offset: -2px;
}}

/* Tables follow the app's own convention (static/style.css .status-table): a
   full 1px cell grid, a light-grey header band and dark text throughout -- no
   per-service colour. A bold key column is kept where a panel uses one. */
.scroll {{ overflow-x: auto; margin: 0 0 1.5rem; }}
table {{ border-collapse: collapse; font-size: 0.875rem; width: 100%; }}
caption {{
  caption-side: top;
  text-align: left;
  font-weight: 600;
  padding: 0.4rem 0.5rem;
  color: #222;
}}
thead th {{
  background: #f4f4f4;
  color: #222;
  font-weight: 600;
  text-align: left;
  padding: 0.4rem 0.6rem;
  white-space: nowrap;
  border: 1px solid var(--line);
}}
td, tbody th {{ padding: 0.4rem 0.6rem; border: 1px solid var(--line); text-align: left; vertical-align: top; }}
tbody th {{ font-weight: 700; white-space: nowrap; width: 1%; background: #f4f4f4; }}
table.kv {{ width: auto; min-width: 22rem; }}

/* mefinder's results page, reproduced: a bold-labelled version banner, a
   "Displaying N of M" line, a contig overview, then a section per contig. The
   sub-tables use the same neutral grid as everywhere else. */
dl.meta {{ margin: 0 0 1rem; }}
dl.meta div {{ display: flex; gap: 0.5rem; padding: 0.05rem 0; }}
dl.meta dt {{ font-weight: 700; min-width: 10rem; }}
dl.meta dd {{ margin: 0; }}
.displaying {{ margin: 0 0 0.75rem; }}
.note {{
  color: var(--muted);
  font-size: 0.8rem;
  margin: -1rem 0 1.5rem;
  max-width: 46rem;
}}
.note code {{ font-family: ui-monospace, SFMono-Regular, Menlo, monospace; }}
h3.contig {{
  font-size: 1.05rem;
  font-weight: 700;
  margin: 1.75rem 0 0.25rem;
  word-break: break-word;
}}
h4 {{ font-size: 0.85rem; font-weight: 700; margin: 0 0 0.25rem; }}
/* A drug-class string runs to ~200 characters. Without a floor the browser
   squeezes those columns to a few characters wide and the table grows tall
   instead of wide; the floor makes it overflow .scroll and scroll sideways,
   which is what RGI's own results table does with the same data. */
td .wrap {{ display: block; min-width: 13rem; max-width: 24rem; white-space: normal; overflow-wrap: anywhere; }}
td.num, .mono {{ font-family: ui-monospace, SFMono-Regular, Menlo, monospace; font-size: 0.9em; }}
.lineage {{ color: var(--muted); font-size: 0.9em; }}
small {{ color: var(--muted); }}
em {{ font-style: italic; }}
.empty {{ color: var(--muted); font-style: italic; }}

.allele {{
  display: inline-block;
  font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
  font-size: 0.8rem;
  background: #f1f3f6;
  border: 1px solid var(--line);
  border-radius: 3px;
  padding: 0.05rem 0.35rem;
  margin: 0 0.15rem 0.15rem 0;
}}
/* CARD panel query legend: which predicted protein each Query N refers to. */
.query-legend {{ margin: 0 0 1rem; line-height: 1.9; }}
.query-item {{ display: inline-block; margin: 0 0.9rem 0.2rem 0; white-space: nowrap; }}
/* CARD panel per-query sequences: the predicted ORF's protein alignment against
   CARD's reference, plus both DNA sequences. Sequences run to ~1000 residues, so
   each query collapses by default and every sequence scrolls inside its box. */
h3.section-head {{ font-size: 1.05rem; font-weight: 700; margin: 1.25rem 0 0.5rem; color: var(--accent); }}
.seq-details {{ border: 1px solid var(--line); border-radius: 3px; margin: 0 0 0.5rem; padding: 0 0.7rem; }}
.seq-details > summary {{ cursor: pointer; padding: 0.45rem 0; font-weight: 600; }}
.seq-details h4 {{ margin: 0.6rem 0 0.25rem; }}
pre.aln {{
  font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
  font-size: 0.75rem;
  line-height: 1.35;
  margin: 0 0 0.6rem;
  padding: 0.5rem;
  background: #f7f8fa;
  border: 1px solid var(--line);
  border-radius: 3px;
  overflow-x: auto;
  white-space: pre;
}}
.seq-grid {{ display: flex; flex-wrap: wrap; gap: 0.75rem; margin: 0 0 0.7rem; }}
.seq-grid > div {{ flex: 1 1 20rem; min-width: 0; }}
.seqbox {{
  font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
  font-size: 0.72rem;
  line-height: 1.4;
  max-height: 9rem;
  overflow: auto;
  padding: 0.4rem 0.5rem;
  background: #f7f8fa;
  border: 1px solid var(--line);
  border-radius: 3px;
  word-break: break-all;
}}

/* PubMLST's rMLST results page, reproduced: a "Predicted taxa" heading over a
   support-bar table, a bordered rST/loci note, then the exact-match table with
   a "linked data values" database badge per row. */
h3.pubmlst-head {{ font-size: 1.05rem; font-weight: 700; margin: 0 0 0.35rem; color: var(--accent); }}
.support-bar {{
  position: relative;
  display: inline-block;
  min-width: 3.5rem;
  background: #e5e7eb;
  border-radius: 2px;
  overflow: hidden;
}}
.support-fill {{ position: absolute; inset: 0 auto 0 0; background: #9ca3af; }}
.support-label {{ position: relative; padding: 0 0.4rem; font-size: 0.85em; font-weight: 600; }}
.linked-db {{
  display: inline-block;
  background: #5a5a5a;
  color: #fff;
  padding: 0.05rem 0.4rem;
  border-radius: 2px;
  font-size: 0.75rem;
  font-weight: 600;
}}
.upload-note {{
  background: #f4f4f4;
  border-left: 4px solid var(--accent);
  padding: 0.6rem 0.9rem;
  margin: 0 0 1.25rem;
  border-radius: 2px;
}}
.upload-note p {{ margin: 0.2rem 0; }}
</style>
</head>
<body>
<h1><span class="sample">{escape_html(sample_id)}</span></h1>
<p class="sub">Per-sample analysis report. Each tab shows one service's results in that service's own terms.</p>
<div class="tabs">
{radios}
<div class="labels">{labels}</div>
<div class="contents">{panels}</div>
</div>
</body>
</html>
"""

with open(report_file, "w") as file_handle:
	file_handle.write(html_content)

print(f"✓ Sample report generated: {report_file}")
