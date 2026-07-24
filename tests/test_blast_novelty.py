"""Two-tier novelty BLAST: local AMR catalog first, NCBI only for what it misses.

The remote-only design this replaces was not slow because the query was big -- it
was slow because NCBI's public queue is. A 216-protein nr search and a capped
15-protein one both took 30 minutes to time out and return nothing. So the local
AMR catalog is now the primary search, and the remote call is reserved for the
enzymes it cannot name, which are the only ones where the wait buys anything.

These cover the routing (which database answers which enzyme), the honesty of the
result (a hit must say where it came from), and the failure paths, which matter
because this step is non-fatal by design and must never take the run down with it.
"""

import csv
import importlib.util
import json
import unittest
from pathlib import Path

from tests._isolation import REAL_ROOT, TMP_ROOT  # noqa: F401  (must import first)


def _load(script_name):
	path = REAL_ROOT / "workflow" / "scripts" / script_name
	spec = importlib.util.spec_from_file_location(script_name.replace(".py", ""), path)
	module = importlib.util.module_from_spec(spec)
	spec.loader.exec_module(module)
	return module


blast_ncbi = _load("blast_ncbi.py")
build_amr = _load("build_amr_blastdb.py")

WORK = TMP_ROOT / "blast"
WORK.mkdir(parents=True, exist_ok=True)


def rgi_json(path, genes):
	"""genes: list of (name, card_identity, sequence). All antibiotic inactivation."""
	payload = {
		"contig_1": {
			f"orf_{index}": {
				"ARO_name": gene_name,
				"orf_prot_sequence": sequence,
				"perc_identity": identity,
				"orf_start": 100 + index,
				"orf_end": 500 + index,
				"ARO_category": {
					"36696": {
						"category_aro_class_name": "Resistance Mechanism",
						"category_aro_name": "antibiotic inactivation",
					}
				},
			}
			for index, (gene_name, identity, sequence) in enumerate(genes)
		}
	}
	Path(path).write_text(json.dumps(payload))
	return path


def blast_row(qid, accession, identity, coverage, title):
	"""One outfmt-6 line in the column order blast_ncbi requests."""
	columns = {
		"qseqid": qid,
		"sacc": accession,
		"pident": str(identity),
		"length": "300",
		"mismatch": "5",
		"gapopen": "0",
		"qstart": "1",
		"qend": "300",
		"sstart": "1",
		"send": "300",
		"evalue": "0.0",
		"bitscore": "600",
		"qcovs": str(coverage),
		"staxids": "562",
		"stitle": title,
	}
	return "\t".join(columns[name] for name in blast_ncbi._OUTFMT_COLS)


def run_blast(
	rgi_path,
	out_csv,
	local_hits=None,
	remote_hits=None,
	local_error=None,
	remote_error=None,
	db_ready=True,
	remote_fallback=True,
	max_queries=0,
):
	"""Drive main() with both BLAST tiers stubbed. Records what each tier was asked."""
	asked = {"local": None, "remote": None}

	def fake_local(query_fasta, local_db, evalue, max_targets, threads=4):
		asked["local"] = Path(query_fasta).read_text()
		return ("\n".join(local_hits or []), local_error)

	def fake_remote(query_fasta, database, evalue, max_targets, timeout):
		asked["remote"] = Path(query_fasta).read_text()
		return ("\n".join(remote_hits or []), remote_error)

	# Restore afterwards: these are module-level rebinds, and a stub left in place
	# would silently answer for every later test in the file.
	originals = (
		blast_ncbi.run_local_blast,
		blast_ncbi.run_remote_blast,
		blast_ncbi.local_db_is_ready,
	)
	blast_ncbi.run_local_blast = fake_local
	blast_ncbi.run_remote_blast = fake_remote
	blast_ncbi.local_db_is_ready = lambda local_db: db_ready

	argv = [
		"--rgi-json",
		str(rgi_path),
		"--out",
		str(out_csv),
		"--local-db",
		str(WORK / "amr" / "AMRProt"),
		"--max-queries",
		str(max_queries),
	]
	if remote_fallback:
		argv.append("--remote-fallback")
	try:
		blast_ncbi.main(argv)
	finally:
		(
			blast_ncbi.run_local_blast,
			blast_ncbi.run_remote_blast,
			blast_ncbi.local_db_is_ready,
		) = originals
	rows = list(csv.DictReader(Path(out_csv).open()))
	return rows, asked


class TestRouting(unittest.TestCase):
	def test_an_enzyme_in_the_catalog_never_reaches_ncbi(self):
		"""The whole point: the common case must not pay NCBI's queue."""
		path = rgi_json(WORK / "known.json", [("ACT-12", 99.0, "MKKLLPAT")])
		rows, asked = run_blast(
			path,
			WORK / "known.csv",
			local_hits=[blast_row("q0_ACT_12", "WP_001", 98.5, 99, "ACT-12 beta-lactamase")],
		)
		self.assertEqual(len(rows), 1)
		self.assertEqual(rows[0]["source"], blast_ncbi.SOURCE_LOCAL)
		self.assertEqual(rows[0]["ncbi_accession"], "WP_001")
		self.assertEqual(rows[0]["ncbi_identity_pct"], "98.5")
		self.assertIsNone(asked["remote"], "a catalog hit must not be sent to NCBI")

	def test_only_the_unmatched_enzymes_are_sent_to_ncbi(self):
		"""The fallback must carry the misses and nothing else -- sending the whole
		set would reintroduce exactly the 30-minute stall this design removes."""
		path = rgi_json(
			WORK / "mixed.json",
			[
				("ACT-12", 99.0, "MKKLLPAT"),  # in the catalog
				("novelX", 55.0, "WWWWCCCC"),  # not in the catalog
			],
		)
		rows, asked = run_blast(
			path,
			WORK / "mixed.csv",
			local_hits=[blast_row("q1_ACT_12", "WP_001", 98.5, 99, "ACT-12 beta-lactamase")],
			remote_hits=[blast_row("q0_novelX", "CAA9", 71.2, 90, "hypothetical protein")],
		)
		# Sorted most-novel-first, so novelX (55%) is q0 and ACT-12 (99%) is q1.
		self.assertIn("q0_novelX", asked["remote"])
		self.assertNotIn("q1_ACT_12", asked["remote"])
		self.assertIn("q1_ACT_12", asked["local"])

		by_gene = {row["query_gene"]: row for row in rows}
		self.assertEqual(by_gene["ACT-12"]["source"], blast_ncbi.SOURCE_LOCAL)
		self.assertEqual(by_gene["novelX"]["source"], blast_ncbi.SOURCE_REMOTE)
		self.assertEqual(by_gene["novelX"]["ncbi_accession"], "CAA9")

	def test_the_cap_takes_the_most_novel_enzymes_first(self):
		path = rgi_json(
			WORK / "cap.json",
			[
				("high", 99.9, "AAAA"),
				("low", 40.0, "BBBB"),
				("mid", 70.0, "CCCC"),
			],
		)
		rows, _ = run_blast(path, WORK / "cap.csv", max_queries=2, remote_fallback=False)
		self.assertEqual([row["query_gene"] for row in rows], ["low", "mid"])


class TestHonesty(unittest.TestCase):
	def test_an_unresolved_enzyme_is_not_called_novel(self):
		"""No hit anywhere means we do not know -- not that the enzyme is new."""
		path = rgi_json(WORK / "miss.json", [("ghost", 50.0, "ZZZZ")])
		rows, _ = run_blast(path, WORK / "miss.csv", remote_hits=[])
		self.assertEqual(rows[0]["is_novel"], "unknown")
		self.assertEqual(rows[0]["source"], "")
		self.assertIn("no hit", rows[0]["note"])

	def test_a_catalog_hit_below_100pct_is_a_candidate_variant(self):
		path = rgi_json(WORK / "variant.json", [("ACT-99", 68.0, "MKKL")])
		rows, _ = run_blast(
			path,
			WORK / "variant.csv",
			local_hits=[blast_row("q0_ACT_99", "WP_7", 92.0, 98, "ACT-1 beta-lactamase")],
		)
		self.assertEqual(rows[0]["is_novel"], "yes")

	def test_an_identical_catalog_hit_is_not_novel(self):
		path = rgi_json(WORK / "exact.json", [("ACT-1", 100.0, "MKKL")])
		rows, _ = run_blast(
			path,
			WORK / "exact.csv",
			local_hits=[blast_row("q0_ACT_1", "WP_7", 100.0, 100, "ACT-1 beta-lactamase")],
		)
		self.assertEqual(rows[0]["is_novel"], "no")

	def test_plasmid_location_is_read_from_the_hit_title(self):
		path = rgi_json(WORK / "plasmid.json", [("ACT-5", 80.0, "MKKL")])
		rows, _ = run_blast(
			path,
			WORK / "plasmid.csv",
			local_hits=[
				blast_row("q0_ACT_5", "WP_9", 95.0, 99, "ACT-5 [Escherichia coli plasmid pX]")
			],
		)
		self.assertEqual(rows[0]["location"], "plasmid")


class TestFailuresAreNonFatal(unittest.TestCase):
	def test_an_unbuilt_local_database_does_not_crash_the_step(self):
		path = rgi_json(WORK / "nodb.json", [("ACT-3", 90.0, "MKKL")])
		rows, asked = run_blast(
			path,
			WORK / "nodb.csv",
			db_ready=False,
			remote_hits=[blast_row("q0_ACT_3", "WP_2", 88.0, 95, "ACT-3 beta-lactamase")],
		)
		# Local was skipped; everything fell through to NCBI rather than failing.
		self.assertIsNone(asked["local"])
		self.assertEqual(rows[0]["source"], blast_ncbi.SOURCE_REMOTE)

	def test_a_remote_timeout_still_writes_rows_and_keeps_the_catalog_hits(self):
		"""A dead NCBI must not discard what the local catalog already answered."""
		path = rgi_json(
			WORK / "timeout.json",
			[
				("ACT-12", 99.0, "MKKL"),
				("novelY", 45.0, "WWWW"),
			],
		)
		rows, _ = run_blast(
			path,
			WORK / "timeout.csv",
			local_hits=[blast_row("q1_ACT_12", "WP_001", 98.5, 99, "ACT-12 beta-lactamase")],
			remote_hits=[],
			remote_error="remote BLAST timed out after 1800s",
		)
		by_gene = {row["query_gene"]: row for row in rows}
		self.assertEqual(by_gene["ACT-12"]["ncbi_accession"], "WP_001")
		self.assertEqual(by_gene["ACT-12"]["source"], blast_ncbi.SOURCE_LOCAL)
		self.assertEqual(by_gene["novelY"]["is_novel"], "unknown")
		self.assertIn("timed out", by_gene["novelY"]["note"])

	def test_fallback_can_be_disabled_and_then_ncbi_is_never_called(self):
		path = rgi_json(WORK / "local_only.json", [("novelZ", 40.0, "QQQQ")])
		rows, asked = run_blast(
			path,
			WORK / "local_only.csv",
			remote_fallback=False,
		)
		self.assertIsNone(asked["remote"])
		self.assertEqual(rows[0]["is_novel"], "unknown")
		self.assertIn("no hit in the AMR catalog", rows[0]["note"])


class TestCatalogBuild(unittest.TestCase):
	def test_pipe_delimited_deflines_become_searchable_ones(self):
		"""AMRProt deflines have no whitespace, so BLAST would take the whole string
		as the id and return an empty title and no accession."""
		source = WORK / "amrprot_raw.fa"
		source.write_text(
			">AAA16360.1|1|1|stxA2b|stxA2b||1|stxA2b|STX2|Shiga_toxin_Stx2b_subunit_A\n"
			"MKKLLPAT\n"
			">WP_000027057.1|1|1|blaACT|blaACT||1|blaACT|BETA-LACTAM|class_C_beta-lactamase_ACT\n"
			"MRYIRLCII\n"
		)
		destination = WORK / "amrprot_clean.fa"
		written = build_amr.rewrite_deflines(source, destination)

		self.assertEqual(written, 2)
		deflines = [line for line in destination.read_text().splitlines() if line.startswith(">")]
		self.assertEqual(deflines[0], ">AAA16360.1 stxA2b Shiga toxin Stx2b subunit A")
		self.assertEqual(deflines[1], ">WP_000027057.1 blaACT class C beta-lactamase ACT")

	def test_duplicate_accessions_are_kept_not_dropped(self):
		"""makeblastdb -parse_seqids rejects repeated ids, and silently dropping a
		reference protein would weaken every novelty call made against it."""
		source = WORK / "dupes.fa"
		source.write_text(
			">SAME.1|1|1|geneA|geneA||1|geneA|X|product_one\nMKKL\n"
			">SAME.1|1|1|geneB|geneB||1|geneB|X|product_two\nMRYI\n"
		)
		destination = WORK / "dupes_clean.fa"
		written = build_amr.rewrite_deflines(source, destination)

		ids = [
			line.split()[0][1:]
			for line in destination.read_text().splitlines()
			if line.startswith(">")
		]
		self.assertEqual(written, 2)
		self.assertEqual(len(set(ids)), 2, f"ids collided: {ids}")
		self.assertIn("SAME.1", ids)

	def test_the_ready_marker_gates_use_of_the_database(self):
		"""A half-finished build leaves searchable files that would quietly return
		too few hits, so readers check the marker, not the db files."""
		db_dir = WORK / "gated"
		db_dir.mkdir(parents=True, exist_ok=True)
		(db_dir / "AMRProt.psq").write_text("not really a database")
		self.assertFalse(blast_ncbi.local_db_is_ready(db_dir / "AMRProt"))

		(db_dir / ".ready").write_text("2026-05-15.1\n")
		self.assertTrue(blast_ncbi.local_db_is_ready(db_dir / "AMRProt"))


if __name__ == "__main__":
	unittest.main()
