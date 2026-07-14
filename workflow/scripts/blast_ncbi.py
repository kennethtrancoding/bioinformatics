"""
BLASTP for resistance-enzyme novelty (Snakemake rule: blast_ncbi_novelty).

For each antibiotic-inactivation enzyme RGI flagged, find the closest known
sequence and report its %identity/coverage (is the enzyme novel?) and whether the
top hit sits on a plasmid vs a chromosome.

TWO-TIER SEARCH: LOCAL AMR CATALOG FIRST, NCBI ONLY AS A FALLBACK

The search runs against a local BLAST database built from NCBI's AMRFinderPlus
reference protein catalog (~10k curated resistance proteins, ~5 MB -- see
build_amr_blastdb.py). Enzymes with no match there, and only those, are then sent
to NCBI's remote nr.

This ordering exists because remote BLAST is queue-bound, not size-bound. On this
pipeline a remote nr search took 30 minutes to time out and return *nothing*,
identically whether we submitted 216 proteins or 15 -- capping the query did not
help, because the wait is NCBI's queue, not the work. The local catalog answers the
same question in about a minute for the full enzyme set.

It also asks a better question. Against all of nr, every protein hits something, so
"novel" degrades to "not identical to one of a billion sequences". Against the AMR
catalog, a miss means "this does not match any KNOWN resistance protein" -- which is
the finding the pipeline is actually looking for. Those genuine misses are exactly
the ones worth spending a remote NCBI search on.

Nothing is thrown away: the complete tabular output for every hit of every query
(local and remote) is saved to <out_stem>_full.tsv; blast_results.csv is an
additional best-hit-per-enzyme summary (novelty + plasmid/chromosome) for the
report, with a `source` column saying which database answered.

If BLAST+ is missing, the local database has not been built, the network is down, or
NCBI rejects the job, we still write both files (with a note) and exit 0 -- this step
is informational, and the report/pipeline shouldn't hang on a flaky service.
"""

import argparse
import csv
import json
import re
import shutil
import subprocess
import tempfile
from pathlib import Path

# outfmt 6 columns we request from blastp. Rich set so the saved full output
# keeps alignment stats, taxonomy and the subject title (organism + plasmid/
# chromosome) for every hit -- nothing NCBI returns is discarded.
_OUTFMT_COLS = [
	"qseqid",
	"sacc",
	"pident",
	"length",
	"mismatch",
	"gapopen",
	"qstart",
	"qend",
	"sstart",
	"send",
	"evalue",
	"bitscore",
	"qcovs",
	"staxids",
	"stitle",
]
_OUTFMT = "6 " + " ".join(_OUTFMT_COLS)
_PLASMID_RE = re.compile(r"\bplasmid\b", re.I)
_CHROMOSOME_RE = re.compile(r"\bchromosome\b", re.I)


def _looks_like_hit(node_value):
	return isinstance(node_value, dict) and (
		"ARO_name" in node_value or "type_match" in node_value or "model_name" in node_value
	)


def _mechanisms(hit):
	"""Resistance-mechanism names attached to an RGI hit (via ARO_category)."""
	mechanism_names = []
	for category in (hit.get("ARO_category") or {}).values():
		if category.get("category_aro_class_name") == "Resistance Mechanism":
			mechanism_name = category.get("category_aro_name")
			if mechanism_name:
				mechanism_names.append(mechanism_name)
	return mechanism_names


def collect_hits(rgi_data):
	"""Walk an RGI results JSON and yield a flat list of resistance-gene hits."""
	hits = []

	def _walk(node, contig):
		if isinstance(node, dict):
			for node_key, node_value in node.items():
				if isinstance(node_key, str) and node_key.startswith("_"):
					continue
				if _looks_like_hit(node_value):
					hits.append((node_key, node_value, contig))
				elif isinstance(node_value, (dict, list)):
					next_contig = (
						node_key if contig is None and isinstance(node_key, str) else contig
					)
					_walk(node_value, next_contig)
		elif isinstance(node, list):
			for node_item in node:
				_walk(node_item, contig)

	_walk(rgi_data, None)
	return hits


def select_queries(rgi_data, mechanism, max_queries):
	"""Pick the enzymes to BLAST: mechanism matches (e.g. 'antibiotic inactivation'),
	de-duplicated by gene+sequence, sorted most-novel-first (lowest CARD identity).
	max_queries is a safety cap only (<= 0 means no cap -- BLAST them all).
	Returns a list of dicts with sequence + metadata."""
	mechanism_filter_lower = (mechanism or "").lower()
	selected, seen = [], set()
	for orf_id, hit, contig in collect_hits(rgi_data):
		protein_sequence = hit.get("orf_prot_sequence") or hit.get("query")
		if not protein_sequence:
			continue
		mechanism_names = _mechanisms(hit)
		if mechanism_filter_lower and not any(
			mechanism_filter_lower in mechanism_name.lower() for mechanism_name in mechanism_names
		):
			continue
		gene_name = hit.get("ARO_name", "unknown")
		node_key = (gene_name, protein_sequence)
		if node_key in seen:
			continue
		seen.add(node_key)
		try:
			card_identity_pct = float(hit.get("perc_identity") or 0)
		except (TypeError, ValueError):
			card_identity_pct = 0.0
		selected.append(
			{
				"gene": gene_name,
				"sequence": protein_sequence,
				"card_identity_pct": card_identity_pct,
				"contig": hit.get("orf_from") or contig or "",
				"orf_start": hit.get("orf_start", ""),
				"orf_end": hit.get("orf_end", ""),
				"mechanisms": "; ".join(mechanism_names),
			}
		)
	selected.sort(key=lambda query_record: query_record["card_identity_pct"])  # most novel first
	return selected[:max_queries] if max_queries and max_queries > 0 else selected


def _location_from_title(title):
	if _PLASMID_RE.search(title or ""):
		return "plasmid"
	if _CHROMOSOME_RE.search(title or ""):
		return "chromosome"
	return "unknown"


def parse_blast_tab(blast_text):
	"""Parse blastp outfmt-6 text into {query_id: best_hit_dict}, best = highest
	identity then coverage. (The full text is saved separately; this only drives
	the summary view.)"""
	column_indexes = {
		column_name: column_index for column_index, column_name in enumerate(_OUTFMT_COLS)
	}
	best = {}
	for blast_line in blast_text.splitlines():
		blast_columns = blast_line.rstrip("\n").split("\t")
		if len(blast_columns) < len(_OUTFMT_COLS):
			continue
		try:
			identity_pct = float(blast_columns[column_indexes["pident"]])
			query_coverage_pct = float(blast_columns[column_indexes["qcovs"]])
		except ValueError:
			continue
		qseqid = blast_columns[column_indexes["qseqid"]]
		current_best_hit = best.get(qseqid)
		if current_best_hit is None or (identity_pct, query_coverage_pct) > (
			current_best_hit["identity"],
			current_best_hit["coverage"],
		):
			best[qseqid] = {
				"accession": blast_columns[column_indexes["sacc"]],
				"identity": identity_pct,
				"coverage": query_coverage_pct,
				"title": blast_columns[column_indexes["stitle"]],
			}
	return best


FIELDS = [
	"query_gene",
	"card_identity_pct",
	"contig",
	"orf_start",
	"orf_end",
	"ncbi_top_hit",
	"ncbi_accession",
	"ncbi_identity_pct",
	"ncbi_coverage_pct",
	# Which database answered: the local AMR catalog, or NCBI nr as a fallback.
	# Without this an identity of 68% is uninterpretable -- 68% to the nearest known
	# resistance protein and 68% to the nearest of all known proteins are very
	# different claims.
	"source",
	"location",
	"is_novel",
	"note",
]

SOURCE_LOCAL = "AMR catalog (local)"
SOURCE_REMOTE = "NCBI nr (remote)"


def _write_csv(output_path, output_rows):
	Path(output_path).parent.mkdir(parents=True, exist_ok=True)
	with open(output_path, "w", newline="") as file_handle:
		csv_writer = csv.DictWriter(file_handle, fieldnames=FIELDS)
		csv_writer.writeheader()
		csv_writer.writerows(output_rows)


def local_db_is_ready(local_db):
	"""True when build_amr_blastdb.py has published a complete database here.

	Checks the .ready marker its build writes last, not merely the presence of the
	db files: a build that died halfway leaves BLAST-formatted files that would
	search fine and silently return too few hits."""
	if not local_db:
		return False
	return (Path(local_db).parent / ".ready").is_file()


def run_local_blast(query_fasta, local_db, evalue, max_targets, threads=4):
	"""blastp against the local AMR catalog. Returns (stdout_text, error_or_None)."""
	if not shutil.which("blastp"):
		return "", "blastp not found in PATH"
	blast_command = [
		"blastp",
		"-db",
		str(local_db),
		"-query",
		str(query_fasta),
		"-outfmt",
		_OUTFMT,
		"-evalue",
		str(evalue),
		"-max_target_seqs",
		str(max_targets),
		"-num_threads",
		str(threads),
	]
	try:
		blast_process = subprocess.run(blast_command, capture_output=True, text=True)
	except Exception as exception:  # noqa: BLE001 - surface any launch failure as a note
		return "", f"local BLAST failed to launch: {exception}"
	if blast_process.returncode != 0:
		return (
			blast_process.stdout,
			f"local blastp exited {blast_process.returncode}: {blast_process.stderr.strip()[:300]}",
		)
	return blast_process.stdout, None


def write_query_fasta(query_records, destination_handle):
	for query_record in query_records:
		destination_handle.write(f">{query_record['qid']}\n")
		protein_sequence = query_record["sequence"]
		for sequence_offset in range(0, len(protein_sequence), 80):
			destination_handle.write(
				protein_sequence[sequence_offset : sequence_offset + 80] + "\n"
			)


def run_remote_blast(query_fasta, database, evalue, max_targets, timeout):
	"""Run blastp -remote and return (stdout_text, error_or_None)."""
	if not shutil.which("blastp"):
		return "", "blastp not found in PATH"
	blast_command = [
		"blastp",
		"-remote",
		"-db",
		database,
		"-query",
		str(query_fasta),
		"-outfmt",
		_OUTFMT,
		"-evalue",
		str(evalue),
		"-max_target_seqs",
		str(max_targets),
	]
	try:
		blast_process = subprocess.run(
			blast_command, capture_output=True, text=True, timeout=timeout
		)
	except subprocess.TimeoutExpired:
		return "", f"remote BLAST timed out after {timeout}s"
	except Exception as exception:  # noqa: BLE001 - surface any launch failure as a note
		return "", f"remote BLAST failed to launch: {exception}"
	if blast_process.returncode != 0:
		return (
			blast_process.stdout,
			f"blastp exited {blast_process.returncode}: {blast_process.stderr.strip()[:300]}",
		)
	return blast_process.stdout, None


def main(argv=None):
	argument_parser = argparse.ArgumentParser(description=__doc__)
	argument_parser.add_argument("--rgi-json", required=True)
	argument_parser.add_argument("--out", required=True, help="best-hit-per-enzyme summary CSV")
	argument_parser.add_argument(
		"--full-out", help="full tabular BLAST output (all hits); default: <out>_full.tsv"
	)
	argument_parser.add_argument("--database", default="nr", help="remote fallback database")
	argument_parser.add_argument(
		"--local-db",
		default="",
		help="BLAST db prefix for the local AMR catalog (searched first); empty to skip",
	)
	argument_parser.add_argument(
		"--remote-fallback",
		action="store_true",
		help="send enzymes with no local hit to NCBI (these are the interesting ones)",
	)
	argument_parser.add_argument("--evalue", default="1e-5")
	argument_parser.add_argument("--max-target-seqs", type=int, default=50)
	argument_parser.add_argument(
		"--max-queries", type=int, default=0, help="safety cap on enzymes submitted (0 = no cap)"
	)
	argument_parser.add_argument("--mechanism", default="antibiotic inactivation")
	argument_parser.add_argument(
		"--timeout", type=int, default=1800, help="remote BLAST timeout (s)"
	)
	argument_parser.add_argument(
		"--threads", type=int, default=4, help="threads for the local blastp tier"
	)
	parsed_args = argument_parser.parse_args(argv)

	full_output_path = parsed_args.full_out or (
		str(Path(parsed_args.out).with_suffix("")) + "_full.tsv"
	)

	def _write_full(blast_text, failure_note=None):
		Path(full_output_path).parent.mkdir(parents=True, exist_ok=True)
		with open(full_output_path, "w") as file_handle:
			if failure_note:
				file_handle.write(f"# {failure_note}\n")
			file_handle.write("\t".join(_OUTFMT_COLS) + "\n")
			if blast_text:
				file_handle.write(blast_text if blast_text.endswith("\n") else blast_text + "\n")

	rgi_path = parsed_args.rgi_json
	if not Path(rgi_path).exists() and Path(rgi_path + ".json").exists():
		rgi_path = rgi_path + ".json"
	try:
		with open(rgi_path) as rgi_file:
			rgi_data = json.load(rgi_file)
	except (OSError, ValueError) as exception:
		_write_csv(
			parsed_args.out,
			[{"query_gene": "", "note": f"could not read RGI results: {exception}"}],
		)
		_write_full("", failure_note=f"could not read RGI results: {exception}")
		print(f"⚠ blast_ncbi: could not read {rgi_path}: {exception}")
		return

	queries = select_queries(rgi_data, parsed_args.mechanism, parsed_args.max_queries)
	if not queries:
		_write_csv(
			parsed_args.out,
			[{"query_gene": "", "note": f"no '{parsed_args.mechanism}' enzymes to BLAST"}],
		)
		_write_full("", failure_note=f"no '{parsed_args.mechanism}' enzymes to BLAST")
		print(f"✓ blast_ncbi: no '{parsed_args.mechanism}' enzymes to BLAST; wrote empty result.")
		return

	# Stable per-query ids so we can map BLAST rows back to gene metadata.
	for column_index, query_record in enumerate(queries):
		query_record["qid"] = (
			f"q{column_index}_{re.sub(r'[^A-Za-z0-9]', '_', query_record['gene'])}"
		)

	with tempfile.NamedTemporaryFile("w", suffix=".fasta", delete=False) as temporary_fasta_file:
		write_query_fasta(queries, temporary_fasta_file)
		query_fasta = temporary_fasta_file.name

	# --- Tier 1: the local AMR catalog. Seconds, and it answers the real question.
	local_stdout, local_error = "", None
	best_local = {}
	if local_db_is_ready(parsed_args.local_db):
		print(
			f"→ blast_ncbi: searching {len(queries)} '{parsed_args.mechanism}' enzyme(s) "
			f"against the local AMR catalog..."
		)
		local_stdout, local_error = run_local_blast(
			query_fasta,
			parsed_args.local_db,
			parsed_args.evalue,
			parsed_args.max_target_seqs,
			threads=parsed_args.threads,
		)
		best_local = parse_blast_tab(local_stdout) if local_stdout else {}
		print(f"  {len(best_local)}/{len(queries)} matched a known resistance protein")
	elif parsed_args.local_db:
		local_error = f"local AMR database not built at {parsed_args.local_db}"
		print(f"⚠ blast_ncbi: {local_error}")

	# --- Tier 2: NCBI, for the enzymes the catalog could not name. Those are the
	# genuinely interesting ones, and they are few, so the remote wait buys something.
	unmatched = [query for query in queries if query["qid"] not in best_local]
	remote_stdout, remote_error = "", None
	best_remote = {}
	if unmatched and parsed_args.remote_fallback:
		with tempfile.NamedTemporaryFile("w", suffix=".fasta", delete=False) as remote_fasta_file:
			write_query_fasta(unmatched, remote_fasta_file)
			remote_query_fasta = remote_fasta_file.name
		print(
			f"→ blast_ncbi: {len(unmatched)} enzyme(s) matched nothing in the catalog; "
			f"falling back to NCBI {parsed_args.database} (timeout {parsed_args.timeout}s)..."
		)
		remote_stdout, remote_error = run_remote_blast(
			remote_query_fasta,
			parsed_args.database,
			parsed_args.evalue,
			parsed_args.max_target_seqs,
			parsed_args.timeout,
		)
		best_remote = parse_blast_tab(remote_stdout) if remote_stdout else {}
		Path(remote_query_fasta).unlink(missing_ok=True)
	elif unmatched:
		print(f"  {len(unmatched)} enzyme(s) unmatched; remote fallback disabled")

	Path(query_fasta).unlink(missing_ok=True)

	# Save the complete BLAST output -- every hit of every query, from both tiers.
	combined_note = "; ".join(note for note in (local_error, remote_error) if note) or None
	_write_full(
		(local_stdout or "") + (remote_stdout or ""),
		failure_note=combined_note,
	)

	output_rows = []
	for query_record in queries:
		hit = best_local.get(query_record["qid"])
		source = SOURCE_LOCAL
		hit_error = local_error
		if hit is None:
			hit = best_remote.get(query_record["qid"])
			source = SOURCE_REMOTE
			hit_error = remote_error
		output_row = {
			"query_gene": query_record["gene"],
			"card_identity_pct": query_record["card_identity_pct"],
			"contig": query_record["contig"],
			"orf_start": query_record["orf_start"],
			"orf_end": query_record["orf_end"],
		}
		if hit:
			output_row.update(
				{
					"ncbi_top_hit": hit["title"][:200],
					"ncbi_accession": hit["accession"],
					"ncbi_identity_pct": round(hit["identity"], 2),
					"ncbi_coverage_pct": round(hit["coverage"], 2),
					"source": source,
					"location": _location_from_title(hit["title"]),
					# Anything short of an identical match is a candidate variant.
					# Read this against `source`: from the AMR catalog it means "not an
					# identical copy of a known resistance protein"; from nr it means
					# "not identical to anything known at all".
					"is_novel": "yes" if hit["identity"] < 100.0 else "no",
					"note": hit_error or "",
				}
			)
		else:
			# No hit in the catalog, and nr either was not asked or did not answer.
			no_hit_note = combined_note or (
				"no hit in the AMR catalog"
				if not parsed_args.remote_fallback
				else "no hit in the AMR catalog or NCBI nr"
			)
			output_row.update(
				{
					"source": "",
					"location": "unknown",
					"is_novel": "unknown",
					"note": no_hit_note,
				}
			)
		output_rows.append(output_row)
	blast_error = combined_note

	_write_csv(parsed_args.out, output_rows)
	from_local = sum(1 for row in output_rows if row.get("source") == SOURCE_LOCAL)
	from_remote = sum(1 for row in output_rows if row.get("source") == SOURCE_REMOTE)
	unresolved = sum(1 for row in output_rows if not row.get("source"))
	print(
		f"  sources: {from_local} from the AMR catalog, {from_remote} from NCBI, "
		f"{unresolved} unresolved"
	)
	if blast_error:
		print(
			f"⚠ blast_ncbi: {blast_error}; wrote {len(output_rows)} row(s) with the note (non-fatal). Full: {full_output_path}"
		)
	else:
		novel = sum(1 for output_row in output_rows if output_row.get("is_novel") == "yes")
		print(
			f"✓ blast_ncbi: {len(output_rows)} enzyme(s) BLASTed, {novel} potentially novel "
			f"→ {parsed_args.out} (all hits: {full_output_path})"
		)


if __name__ == "__main__":
	main()
