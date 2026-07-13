#!/usr/bin/env python3
"""
Build the local AMR BLAST database (Snakemake rule: build_amr_blast_db).

Downloads NCBI's AMRFinderPlus reference protein catalog -- AMRProt.fa, the
curated set of known antimicrobial-resistance proteins (~10k sequences, ~5 MB) --
and formats it with makeblastdb so blast_ncbi.py can search it locally.

WHY LOCAL, AND WHY THIS DATABASE

A remote BLAST against nr is queue-bound, not size-bound: on this pipeline it took
30 minutes to time out and return nothing whether we submitted 216 proteins or 15.
nr itself cannot come local -- it is 733 GB. The AMR catalog is 5 MB and answers
the question the pipeline is actually asking ("how close is this enzyme to the
nearest KNOWN resistance protein?") in seconds rather than never.

It is deliberately narrow: it says nothing about non-resistance proteins. That is
what blast_ncbi.py's remote fallback is for -- an enzyme with no match in the
catalog is the interesting case, and only those go to NCBI.

WHY IT LIVES IN THE IMAGE

The pipeline's databases are image content, not volume content (see
DATABASE_UPDATES.md), so the weekly rebuild in deploy/refresh-databases.sh
re-runs this script and refreshes the AMR catalog in the same pass that refreshes
CARD and MGEdb -- one mechanism, three databases.

DEFLINE REWRITING

AMRProt.fa deflines are pipe-delimited with no whitespace:

    >AAA16360.1|1|1|stxA2b|stxA2b||1|stxA2b|STX2|Shiga_toxin_Stx2b_subunit_A

BLAST would treat that entire string as the sequence id, leaving `stitle` empty
and `sacc` unresolvable -- so a hit would come back with no name and no accession.
We rewrite each defline to "<accession> <symbol> <product>" so that BLAST's
tabular output carries a real accession and a readable title.
"""

import argparse
import shutil
import subprocess
import sys
import tempfile
import urllib.request
from pathlib import Path

DEFAULT_URL_BASE = (
	"https://ftp.ncbi.nlm.nih.gov/pathogen/Antimicrobial_resistance/"
	"AMRFinderPlus/database/latest"
)


def _fetch(url, destination_path, timeout):
	"""Download url to destination_path.

	Falls back to curl when urllib cannot complete the TLS handshake. Networks that
	intercept TLS (corporate proxies, and this project's own dev machines) present a
	self-signed certificate that Python's bundled trust store rejects but the system
	trust store accepts, so the fallback is the difference between the database
	building and the whole step being unavailable."""
	try:
		with urllib.request.urlopen(url, timeout=timeout) as response:
			destination_path.write_bytes(response.read())
		return
	except urllib.error.URLError as url_error:
		print(f"  urllib could not fetch ({url_error.reason}); retrying with curl")

	# Prefer the system curl over whatever is first on PATH. Inside a Snakemake rule
	# PATH leads with the conda env, and a conda env that has been moved on disk
	# carries a CA-bundle path baked in at build time that no longer exists -- its
	# curl then fails on every https URL. The system curl uses the system trust
	# store and is immune to that.
	curl_candidates = [path for path in ("/usr/bin/curl", shutil.which("curl")) if path]
	if not curl_candidates:
		raise RuntimeError("neither urllib nor curl could fetch the AMR catalog")

	last_error = ""
	for curl_path in curl_candidates:
		curl_result = subprocess.run(
			[curl_path, "-fsSL", "--max-time", str(timeout), "-o", str(destination_path), url],
			capture_output=True,
			text=True,
		)
		if curl_result.returncode == 0:
			return
		last_error = f"{curl_path} exited {curl_result.returncode}: {curl_result.stderr.strip()[:200]}"
	raise RuntimeError(last_error)


def rewrite_deflines(source_fasta, destination_fasta):
	"""Turn AMRProt's pipe-delimited deflines into '<accession> <symbol> <product>'.

	Returns the number of sequences written. Duplicate accessions are suffixed
	rather than dropped: makeblastdb -parse_seqids rejects a database with repeated
	ids, and losing a reference protein would silently weaken every novelty call
	made against it."""
	sequence_count = 0
	seen_ids = {}
	with open(source_fasta) as source, open(destination_fasta, "w") as destination:
		for line in source:
			if not line.startswith(">"):
				destination.write(line)
				continue
			fields = line[1:].rstrip("\n").split("|")
			accession = (fields[0] or "").strip() or f"AMR_{sequence_count}"
			gene_symbol = ""
			for candidate_index in (3, 4, 7):
				if len(fields) > candidate_index and fields[candidate_index].strip():
					gene_symbol = fields[candidate_index].strip()
					break
			product = fields[-1].replace("_", " ").strip() if len(fields) > 1 else ""

			seen_ids[accession] = seen_ids.get(accession, 0) + 1
			if seen_ids[accession] > 1:
				accession = f"{accession}_{seen_ids[accession]}"

			description = " ".join(part for part in (gene_symbol, product) if part)
			destination.write(f">{accession} {description}\n")
			sequence_count += 1
	return sequence_count


def main(argv=None):
	argument_parser = argparse.ArgumentParser(description=__doc__)
	argument_parser.add_argument("--url-base", default=DEFAULT_URL_BASE)
	argument_parser.add_argument(
		"--out-dir", required=True, help="directory to hold the formatted BLAST database"
	)
	argument_parser.add_argument("--db-name", default="AMRProt")
	argument_parser.add_argument("--timeout", type=int, default=300)
	parsed_args = argument_parser.parse_args(argv)

	output_directory = Path(parsed_args.out_dir)
	output_directory.mkdir(parents=True, exist_ok=True)

	if not shutil.which("makeblastdb"):
		print("✗ build_amr_blastdb: makeblastdb not found in PATH", file=sys.stderr)
		return 1

	with tempfile.TemporaryDirectory() as staging_directory_name:
		staging_directory = Path(staging_directory_name)
		# Not named AMRProt*: the publish step below copies everything matching the db
		# name, and the raw download has no business in the finished database.
		raw_fasta = staging_directory / "download.raw.fa"
		clean_fasta = staging_directory / "AMRProt.fa"

		catalog_version = "unknown"
		try:
			version_path = staging_directory / "version.txt"
			_fetch(f"{parsed_args.url_base}/version.txt", version_path, parsed_args.timeout)
			catalog_version = version_path.read_text().strip()
		except Exception as exception:  # noqa: BLE001 - version is a label, not a gate
			print(f"⚠ build_amr_blastdb: could not read catalog version: {exception}")

		print(f"→ build_amr_blastdb: downloading AMRProt.fa (catalog {catalog_version})...")
		try:
			_fetch(f"{parsed_args.url_base}/AMRProt.fa", raw_fasta, parsed_args.timeout)
		except Exception as exception:  # noqa: BLE001
			print(f"✗ build_amr_blastdb: download failed: {exception}", file=sys.stderr)
			return 1

		sequence_count = rewrite_deflines(raw_fasta, clean_fasta)
		print(f"  rewrote {sequence_count} deflines")

		staged_db_prefix = staging_directory / parsed_args.db_name
		makeblastdb_command = [
			"makeblastdb",
			"-in",
			str(clean_fasta),
			"-dbtype",
			"prot",
			"-parse_seqids",
			"-out",
			str(staged_db_prefix),
			"-title",
			f"NCBI AMRFinderPlus AMRProt {catalog_version}",
		]
		makeblastdb_result = subprocess.run(makeblastdb_command, capture_output=True, text=True)
		if makeblastdb_result.returncode != 0:
			print(
				f"✗ build_amr_blastdb: makeblastdb exited {makeblastdb_result.returncode}: "
				f"{makeblastdb_result.stderr.strip()[:400]}",
				file=sys.stderr,
			)
			return 1

		# Publish only once the database is complete. The .ready marker is written
		# last and is what every reader checks, so a build that dies halfway leaves
		# the previous database in place rather than a half-formatted one.
		for built_file in staging_directory.iterdir():
			if built_file.name.startswith(parsed_args.db_name) and built_file != clean_fasta:
				shutil.copy2(built_file, output_directory / built_file.name)
		shutil.copy2(clean_fasta, output_directory / "AMRProt.fa")
		(output_directory / ".ready").write_text(f"{catalog_version}\n{sequence_count} sequences\n")

	print(
		f"✓ build_amr_blastdb: {sequence_count} AMR proteins → "
		f"{output_directory / parsed_args.db_name} (catalog {catalog_version})"
	)
	return 0


if __name__ == "__main__":
	sys.exit(main())
