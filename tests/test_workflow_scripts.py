"""Drives every local analysis script with realistic fabricated inputs.

The BV-BRC/RGI/mefinder tools themselves need credentials and hours, but the
scripts that turn their output into the reports a user downloads are pure
local logic — this exercises that whole chain: RGI JSON -> CSV -> novelty ->
proteins, mefinder CSV -> summary, ARG/MGE co-location, and both reports.
"""

import csv
import gzip
import json
import runpy
import subprocess
import sys
import unittest
from pathlib import Path

from tests._isolation import REAL_ROOT, TMP_ROOT  # noqa: F401  (must import first)

ROOT = REAL_ROOT
SCRIPTS = ROOT / "workflow" / "scripts"
WORK = TMP_ROOT / "results" / "SCRIPTSMOKE" / "S1"

sys.path.insert(0, str(SCRIPTS))


class NS:
    """Stands in for snakemake.input/output/params: attribute + index access."""

    def __init__(self, *positional, **named):
        self._positional = [str(p) for p in positional]
        for key, value in named.items():
            setattr(self, key, str(value) if isinstance(value, Path) else value)

    def __getitem__(self, index):
        return self._positional[index]


class FakeSnakemake:
    def __init__(self, input=None, output=None, params=None, config=None, wildcards=None):
        self.input = input or NS()
        self.output = output or NS()
        self.params = params or NS()
        self.config = config or {}
        self.wildcards = wildcards or NS()
        self.log = NS(str(WORK / "script.log"))


def run_script(name, **kw):
    """Execute a Snakemake `script:` module with an injected snakemake global."""
    runpy.run_path(str(SCRIPTS / name), init_globals={"snakemake": FakeSnakemake(**kw)})


# Fixtures
CONTIG = "assembly_contig_1"


def rgi_hit(aro, model, identity, start, end, mechanism, family, drug):
    """Shaped like a real `rgi main` JSON hit: no coverage figure anywhere, and
    the drug class reachable only through ARO_category."""
    return {
        "ARO_name": aro,
        "model_name": model,
        "ARO_accession": "3001876",
        "type_match": "Strict",
        "perc_identity": identity,
        "orf_from": CONTIG,
        "orf_start": start,
        "orf_end": end,
        "orf_strand": "+",
        "orf_prot_sequence": "MVKKSLRQFTLMATATVTLLLGSVPLYAQTAD",
        "ARO_category": {
            "1": {"category_aro_class_name": "Drug Class", "category_aro_name": drug},
            "2": {"category_aro_class_name": "Resistance Mechanism", "category_aro_name": mechanism},
            "3": {"category_aro_class_name": "AMR Gene Family", "category_aro_name": family},
        },
    }


def rgi_tab_report(rows):
    """`rgi main`'s tab report — the only place it records coverage."""
    header = ["Contig", "Start", "Stop", "Best_Hit_ARO", "Best_Identities",
              "Percentage Length of Reference Sequence", "Drug Class"]
    lines = ["\t".join(header)]
    for aro, start, end, identity, coverage, drug in rows:
        lines.append("\t".join(
            [CONTIG, str(start), str(end), aro, str(identity), f"{coverage:.2f}", drug]
        ))
    return "\n".join(lines) + "\n"


class TestAnalysisChain(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        WORK.mkdir(parents=True, exist_ok=True)
        for sub in ("01_raw_qc", "02_assembly", "03_resistance", "04_blast", "05_mlst",
                    "06_mobile_elements", "summary"):
            (WORK / sub).mkdir(exist_ok=True)

        # Three hits, each exercising one arm of the novelty call:
        #   blaCTX-M-15  92.5% identity, full length -> novel on identity
        #   aac(6')-Ib   100% identity, full length  -> not novel
        #   tetA         99% identity, 45% coverage  -> novel on COVERAGE only,
        #                                               which is the whole point
        cls.rgi_json = WORK / "03_resistance" / "rgi_results.json"
        cls.rgi_json.write_text(json.dumps({
            CONTIG: {
                "orf1": rgi_hit("blaCTX-M-15", "CTX-M-15", 92.5, 1000, 1876,
                                "antibiotic inactivation", "CTX-M beta-lactamase", "cephalosporin"),
                "orf2": rgi_hit("aac(6')-Ib", "AAC(6')-Ib", 100.0, 8000, 8600,
                                "antibiotic inactivation", "AAC(6') aminoglycoside", "aminoglycoside"),
                "orf3": rgi_hit("tetA", "TetA", 99.0, 20000, 20300,
                                "antibiotic efflux", "major facilitator superfamily", "tetracycline"),
            },
            "_metadata": {"rgi_version": "6.0.3"},
        }))
        (WORK / "03_resistance" / "rgi_results.txt").write_text(rgi_tab_report([
            ("blaCTX-M-15", 1000, 1876, 92.5, 100.00, "cephalosporin"),
            ("aac(6')-Ib", 8000, 8600, 100.0, 100.00, "aminoglycoside"),
            ("tetA", 20000, 20300, 99.0, 45.00, "tetracycline"),
        ]))

        # mefinder: an IS26 overlapping blaCTX-M-15, and a distant transposon
        cls.mef_csv = WORK / "06_mobile_elements" / "S1.csv"
        cls.mef_csv.write_text(
            "# mefinder result\n# db version 1.1.0\n"
            "mge_no,name,type,contig,start,end\n"
            "1,IS26,insertion sequence,assembly_contig_1 length=50000,900,1700\n"
            "2,Tn3,transposon,assembly_contig_1 length=50000,40000,44000\n"
        )

        cls.genome_report = WORK / "02_assembly" / "genome_report.json"
        cls.genome_report.write_text(json.dumps([{
            "contigs": 42, "gc_content": 50.6, "genome_length": 5231455,
            "contig_n50": 210344, "contig_l50": 8,
        }]))

    # Individual scripts
    def test_01_qc_validate(self):
        r1 = WORK / "S1_R1.fastq.gz"
        r2 = WORK / "S1_R2.fastq.gz"
        for p in (r1, r2):
            with gzip.open(p, "wt") as fh:
                fh.write("@r\nACGT\n+\nIIII\n")
        report = WORK / "01_raw_qc" / "validation.txt"
        metadata = WORK / "01_raw_qc" / "metadata.json"
        run_script(
            "qc_validate.py",
            # The reads are inputs now, not params: that is what lets Snakemake fetch
            # them from S3 first and delete them after (workflow/rules/raw.smk).
            input=NS(first_read=str(r1), second_read=str(r2)),
            params=NS(sample_id="S1"),
            output=NS(report=report, metadata=metadata),
            config={"samples_manifest": str(ROOT / "config" / "samples.csv")},
        )
        self.assertTrue(report.is_file())
        meta = json.loads(metadata.read_text())
        self.assertEqual(meta["sample_id"], "S1")
        self.assertIn("md5", json.dumps(meta).lower())

    def test_02_parse_genome_report(self):
        metrics = WORK / "02_assembly" / "genome_metrics.csv"
        run_script("parse_genome_report.py",
                   input=NS(self.genome_report), output=NS(metrics=metrics))
        row = next(csv.DictReader(metrics.open()))
        self.assertEqual(row["contigs"], "42")
        self.assertEqual(row["n50"], "210344")
        self.assertEqual(row["genome_length"], "5231455")

    def test_03_rgi_json_to_csv(self):
        out = WORK / "03_resistance" / "rgi_results.csv"
        proc = subprocess.run(
            [sys.executable, str(SCRIPTS / "rgi_json_to_csv.py"), str(self.rgi_json), str(out)],
            cwd=ROOT, capture_output=True, text=True)
        self.assertEqual(proc.returncode, 0, proc.stderr)
        rows = list(csv.DictReader(out.open()))
        self.assertEqual(len(rows), 3)
        genes = {r["best_hit_aro"]: r for r in rows}
        self.assertIn("blaCTX-M-15", genes)
        self.assertEqual(genes["blaCTX-M-15"]["contig"], CONTIG)
        self.assertEqual(genes["blaCTX-M-15"]["amr_gene_family"], "CTX-M beta-lactamase")
        self.assertEqual(genes["blaCTX-M-15"]["resistance_mechanism"], "antibiotic inactivation")
        self.assertEqual(genes["blaCTX-M-15"]["percent_identity"], "92.5")
        # coverage exists only in RGI's tab report; the CSV column was empty
        self.assertEqual(genes["blaCTX-M-15"]["percent_coverage"], "100.00")
        self.assertEqual(genes["tetA"]["percent_coverage"], "45.00")

    def _novelty_rows(self, coverage_min=80, identity_min=95):
        novelty = WORK / "03_resistance" / "novelty_report.txt"
        run_script("evaluate_novelty.py",
                   input=NS(self.rgi_json, rgi_json=self.rgi_json),
                   output=NS(novelty_report=novelty),
                   params=NS(coverage_min=coverage_min, identity_min=identity_min))
        text = novelty.read_text()
        columns = "Gene\tAntibiotic\tIdentity_pct\tCoverage_pct\tNovel_flag\tNotes".split("\t")
        rows = {}
        for line in text.splitlines()[text.splitlines().index("\t".join(columns)) + 1:]:
            if line.strip():
                row = dict(zip(columns, line.split("\t")))
                rows[row["Gene"]] = row
        return text, rows

    def test_04_evaluate_novelty_flags_low_identity(self):
        text, rows = self._novelty_rows()
        self.assertIn("Total hits: 3", text)
        self.assertEqual(rows["CTX-M-15"]["Novel_flag"], "YES")
        self.assertIn("identity", rows["CTX-M-15"]["Notes"])
        self.assertEqual(rows["AAC(6')-Ib"]["Novel_flag"], "NO")

    def test_04b_novelty_applies_the_coverage_threshold(self):
        """A full-identity hit covering only 45% of the reference is a fragment,
        not a match — the configured coverage_min must catch it."""
        text, rows = self._novelty_rows(coverage_min=80, identity_min=95)
        self.assertEqual(rows["TetA"]["Coverage_pct"], "45.0",
                         "coverage never reached the novelty call")
        self.assertEqual(rows["TetA"]["Novel_flag"], "YES")
        self.assertIn("coverage", rows["TetA"]["Notes"])
        # identity alone would have passed it
        self.assertNotIn("identity", rows["TetA"]["Notes"])
        self.assertIn("Potential novel variants: 2", text)

    def test_04c_novelty_reports_the_drug_class(self):
        """The Antibiotic column read a key RGI does not emit, so every row said
        'unknown'. It lives under ARO_category."""
        _, rows = self._novelty_rows()
        self.assertEqual(rows["CTX-M-15"]["Antibiotic"], "cephalosporin")
        self.assertEqual(rows["TetA"]["Antibiotic"], "tetracycline")
        self.assertNotIn("unknown", [row["Antibiotic"] for row in rows.values()])

    def test_04d_novelty_survives_a_missing_tab_report(self):
        """Older result trees have only the JSON; still report, on identity."""
        json_only = WORK / "03_resistance" / "json_only.json"
        json_only.write_text(self.rgi_json.read_text())  # no sibling .txt
        novelty = WORK / "03_resistance" / "json_only_novelty.txt"
        run_script("evaluate_novelty.py",
                   input=NS(json_only, rgi_json=json_only),
                   output=NS(novelty_report=novelty),
                   params=NS(coverage_min=80, identity_min=95))
        text = novelty.read_text()
        self.assertIn("Total hits: 3", text)
        self.assertIn("N/A", text)  # coverage unavailable, but no crash

    def test_05_extract_rgi_proteins(self):
        fasta = WORK / "04_blast" / "rgi_proteins.fasta"
        csv_out = WORK / "04_blast" / "rgi_proteins.csv"
        proc = subprocess.run(
            [sys.executable, str(SCRIPTS / "extract_rgi_proteins.py"),
             str(self.rgi_json), str(fasta), str(csv_out)],
            cwd=ROOT, capture_output=True, text=True)
        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertIn(">", fasta.read_text())
        self.assertIn("MVKKSLRQ", fasta.read_text())
        self.assertEqual(len(list(csv.DictReader(csv_out.open()))), 3)

    def test_06_parse_mefinder(self):
        summary_csv = WORK / "06_mobile_elements" / "me_summary.csv"
        summary_json = WORK / "06_mobile_elements" / "me_summary.json"
        run_script("parse_mefinder.py",
                   input=NS(csv=self.mef_csv),
                   output=NS(summary_csv=summary_csv, summary_json=summary_json),
                   params=NS(sample_id="S1"))
        summary = json.loads(summary_json.read_text())
        self.assertEqual(summary["mobile_elements"], 2)
        self.assertEqual(summary["by_type"]["insertion sequence"], 1)
        total = [r for r in csv.DictReader(summary_csv.open()) if r["element_type"] == "TOTAL"]
        self.assertEqual(total[0]["count"], "2")

    def test_07_mge_colocation_links_arg_to_mge(self):
        out_csv = WORK / "06_mobile_elements" / "S1_arg_mge_colocation.csv"
        out_json = WORK / "06_mobile_elements" / "S1_arg_mge_colocation.json"
        proc = subprocess.run(
            [sys.executable, str(SCRIPTS / "mge_colocation.py"),
             "--mge-csv", str(self.mef_csv), "--rgi-json", str(self.rgi_json),
             "--out-csv", str(out_csv), "--out-json", str(out_json),
             "--sample-id", "S1", "--proximity-bp", "5000"],
            cwd=ROOT, capture_output=True, text=True)
        self.assertEqual(proc.returncode, 0, proc.stderr)
        summary = json.loads(out_json.read_text())
        # blaCTX-M-15 (1000-1876) overlaps IS26 (900-1700) -> must be linked
        self.assertIn("blaCTX-M-15", summary["mobile_element_linked_genes"])
        rows = {r["resistance_gene"]: r for r in csv.DictReader(out_csv.open())}
        self.assertEqual(len(rows), 3)
        self.assertEqual(rows["blaCTX-M-15"]["on_mobile_element"], "yes")
        self.assertEqual(rows["blaCTX-M-15"]["nearest_distance_bp"], "0")
        self.assertEqual(rows["aac(6')-Ib"]["on_mobile_element"], "no")

    def test_08_generate_sample_report(self):
        report = WORK / "summary" / "report.html"
        mlst_json = WORK / "05_mlst" / "mlst_results.json"
        mlst_json.write_text(json.dumps({
            "sample": "S1", "scheme": "ecloacae", "st": "114",
            "species": "Enterobacter hormaechei", "species_support": 98.7,
            "species_method": "rMLST",
        }))
        blast_csv = WORK / "04_blast" / "blast_results.csv"
        blast_csv.write_text("query,subject,identity,evalue,title\n"
                             "orf1,WP_000123.1,99.4,0.0,class A beta-lactamase\n")
        run_script(
            "generate_sample_report.py",
            input=NS(assembly_metrics=WORK / "02_assembly" / "genome_metrics.csv",
                     card=WORK / "03_resistance" / "rgi_results.csv",
                     blast=blast_csv,
                     mlst=mlst_json,
                     mobile_element_finder=WORK / "06_mobile_elements" / "me_summary.csv"),
            output=NS(html_report=report),
            params=NS(sample_id="S1"),
            wildcards=NS(sample="S1"),
        )
        html = report.read_text()
        self.assertIn("<!DOCTYPE html>", html)
        for expected in ["blaCTX-M-15", "Enterobacter hormaechei", "114", "210344"]:
            self.assertIn(str(expected), html, f"missing {expected} in report")

    def test_09_generate_master_report(self):
        master = TMP_ROOT / "results" / "SCRIPTSMOKE" / "master_report.csv"
        run_script(
            "generate_master_report.py",
            params=NS(sample_ids=["S1"]),
            input=NS(card=[str(WORK / "03_resistance" / "rgi_results.csv")],
                     mlst=[str(WORK / "05_mlst" / "mlst_results.json")],
                     mobile_element_finder=[str(self.mef_csv)],
                     colocation=[str(WORK / "06_mobile_elements" / "S1_arg_mge_colocation.json")]),
            output=NS(csv_report=master),
        )
        row = next(csv.DictReader(master.open()))
        self.assertEqual(row["isolate_id"], "S1")
        self.assertEqual(row["species"], "Enterobacter hormaechei")
        self.assertEqual(row["sequence_type"], "114")
        self.assertIn("blaCTX-M-15", row["beta_lactamase_genes"])
        self.assertIn("aac(6')-Ib", row["antibiotic_inactivation_genes"])
        # The elements are named now, not merely counted.
        self.assertEqual(row["mobile_element_genes"], "IS26; Tn3")
        self.assertIn("blaCTX-M-15", row["mobile_element_linked_resistance_genes"])

    def test_09b_master_report_names_repeated_elements_with_multiplicity(self):
        """Two copies of one element must not collapse into a single name: naming
        the genes should not quietly lose the count the old column carried."""
        repeated = WORK / "06_mobile_elements" / "repeated.csv"
        repeated.write_text(
            "# mefinder result\n"
            "mge_no,name,type,contig,start,end\n"
            "1,MITEEc1,mite,c1,10,100\n"
            "2,MITEEc1,mite,c1,500,600\n"
            "3,ISEhe3,insertion sequence,c1,900,1700\n"
        )
        master = TMP_ROOT / "results" / "SCRIPTSMOKE" / "master_repeated.csv"
        run_script(
            "generate_master_report.py",
            params=NS(sample_ids=["S1"]),
            input=NS(card=[str(WORK / "03_resistance" / "rgi_results.csv")],
                     mlst=[str(WORK / "05_mlst" / "mlst_results.json")],
                     mobile_element_finder=[str(repeated)],
                     colocation=[str(WORK / "06_mobile_elements" / "S1_arg_mge_colocation.json")]),
            output=NS(csv_report=master),
        )
        row = next(csv.DictReader(master.open()))
        self.assertEqual(row["mobile_element_genes"], "ISEhe3; MITEEc1 (x2)")

    def test_09c_master_report_says_none_when_no_elements_were_called(self):
        empty = WORK / "06_mobile_elements" / "empty.csv"
        empty.write_text("# mefinder result\nmge_no,name,type,contig,start,end\n")
        master = TMP_ROOT / "results" / "SCRIPTSMOKE" / "master_empty.csv"
        run_script(
            "generate_master_report.py",
            params=NS(sample_ids=["S1"]),
            input=NS(card=[str(WORK / "03_resistance" / "rgi_results.csv")],
                     mlst=[str(WORK / "05_mlst" / "mlst_results.json")],
                     mobile_element_finder=[str(empty)],
                     colocation=[str(WORK / "06_mobile_elements" / "S1_arg_mge_colocation.json")]),
            output=NS(csv_report=master),
        )
        self.assertEqual(next(csv.DictReader(master.open()))["mobile_element_genes"], "none")

    def test_10_master_report_neutralizes_csv_formula_injection(self):
        """A gene name from an external DB must not execute in Excel."""
        evil_rgi = WORK / "03_resistance" / "evil.csv"
        evil_rgi.write_text("best_hit_aro,amr_gene_family,resistance_mechanism\n"
                            '"=cmd|calc!A1",beta-lactamase,antibiotic inactivation\n')
        master = TMP_ROOT / "results" / "SCRIPTSMOKE" / "master_evil.csv"
        run_script(
            "generate_master_report.py",
            params=NS(sample_ids=["EVIL"]),
            input=NS(card=[str(evil_rgi)], mlst=["/nonexistent.json"],
                     mobile_element_finder=["/nonexistent.csv"], colocation=["/nonexistent.json"]),
            output=NS(csv_report=master),
        )
        row = next(csv.DictReader(master.open()))
        self.assertTrue(row["beta_lactamase_genes"].startswith("'="),
                        f"formula not neutralized: {row['beta_lactamase_genes']!r}")

    def test_11_sample_report_escapes_untrusted_gene_names(self):
        """Report values come from external DBs; they must not inject HTML."""
        xss_rgi = WORK / "03_resistance" / "xss.csv"
        xss_rgi.write_text("best_hit_aro,drug_class\n"
                           '"<script>alert(1)</script>",cephalosporin\n')
        report = WORK / "summary" / "xss_report.html"
        run_script(
            "generate_sample_report.py",
            input=NS(assembly_metrics=WORK / "02_assembly" / "genome_metrics.csv",
                     card=xss_rgi, blast="/nonexistent.csv",
                     mlst=WORK / "05_mlst" / "mlst_results.json",
                     mobile_element_finder=WORK / "06_mobile_elements" / "me_summary.csv"),
            output=NS(html_report=report),
            params=NS(sample_id="S1"),
            wildcards=NS(sample="S1"),
        )
        html = report.read_text()
        self.assertNotIn("<script>alert(1)</script>", html)
        self.assertIn("&lt;script&gt;", html)

    def test_12_scripts_tolerate_missing_upstream_inputs(self):
        """Remote BLAST/MLST can be unavailable; reports must still generate."""
        report = TMP_ROOT / "results" / "SCRIPTSMOKE" / "degraded.html"
        run_script(
            "generate_sample_report.py",
            input=NS(assembly_metrics="/nonexistent.csv", card="/nonexistent.csv",
                     blast="/nonexistent.csv", mlst="/nonexistent.json",
                     mobile_element_finder="/nonexistent.csv"),
            output=NS(html_report=report),
            params=NS(sample_id="S1"),
            wildcards=NS(sample="S1"),
        )
        self.assertIn("No data available", report.read_text())

    def test_13_empty_rgi_results_produce_empty_novelty_report(self):
        empty = WORK / "03_resistance" / "empty.json"
        empty.write_text("{}")
        novelty = WORK / "03_resistance" / "empty_novelty.txt"
        run_script("evaluate_novelty.py", input=NS(empty, rgi_json=empty),
                   output=NS(novelty_report=novelty),
                   params=NS(coverage_min=80, identity_min=95))
        self.assertIn("Total hits: 0", novelty.read_text())


if __name__ == "__main__":
    unittest.main(verbosity=2)
