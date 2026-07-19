"""Several stats workbooks arriving in one import.

The sequencing company ships one "DNA Sequencing Stats.xlsx" per run, and its
R1/R2 md5sum columns are the only thing standing between a corrupted FASTQ and
the pipeline. But a user does not always hand us one run at a time: two runs get
dropped into one folder for a single upload, or a folder holds Run1/ and Run2/
subfolders with a workbook in each. Every sample in that upload has a company
checksum on paper, so every sample should be verified against it.

These tests ask one question -- when more than one workbook is entered at once,
does the app see all of them? -- at the level that matters: not "was the file
found" but "did a sample whose checksum is on paper actually get checked".

The last test is the one with teeth. Verification exists so bad data never
enters the pipeline; a sample listed as corrupt in the second workbook must be
rejected no matter which workbook it was listed in.
"""

import io
import unittest
from pathlib import Path

import frontend  # noqa: E402
from tests._isolation import REAL_ROOT  # noqa: F401  (must import first)
from tests.test_cloud_import import fastq_bytes, md5  # noqa: E402
from workflow.helpers import import_samples, jobs  # noqa: E402

ROOT = Path(frontend.PROJECT_ROOT)  # temp root, see tests/_isolation


def workbook_at(path, rows):
	"""Write a stats workbook to `path`. Same shape as the company's sheet, but
	the caller names the file: the whole point here is several of them at once,
	which means several names."""
	import openpyxl

	workbook = openpyxl.Workbook()
	sheet = workbook.active
	sheet.append(["Sample Name", "R1 md5sum", "R2 md5sum"])
	for row in rows:
		sheet.append(row)
	path.parent.mkdir(parents=True, exist_ok=True)
	workbook.save(path)


def write_pair(directory, sample_name):
	"""Drop an R1/R2 pair for `sample_name` in `directory`; return their bytes."""
	directory.mkdir(parents=True, exist_ok=True)
	r1, r2 = fastq_bytes(1, "r1"), fastq_bytes(1, "r2")
	(directory / f"{sample_name}_S1_R1_001.fastq.gz").write_bytes(r1)
	(directory / f"{sample_name}_S1_R2_001.fastq.gz").write_bytes(r2)
	return r1, r2


class TwoWorkbooksInOneFolder(unittest.TestCase):
	"""Two runs' worth of FASTQs and both their workbooks, dumped in one folder."""

	def _import(self, directory, name):
		return import_samples.import_directory(
			directory,
			samples_csv=ROOT / "config" / "jobs" / name / "samples.csv",
			dest_dir=directory / "dest",
		)

	def test_both_run_workbooks_are_read(self):
		"""Neither file is named "DNA Sequencing Stats.xlsx" -- companies label
		them per run -- but both are plainly stats workbooks."""
		d = ROOT / "data" / "two_run_workbooks"
		alpha_r1, alpha_r2 = write_pair(d, "ALPHA")
		beta_r1, beta_r2 = write_pair(d, "BETA")
		workbook_at(d / "Run1 Stats.xlsx", [["ALPHA", md5(alpha_r1), md5(alpha_r2)]])
		workbook_at(d / "Run2 Stats.xlsx", [["BETA", md5(beta_r1), md5(beta_r2)]])

		result = self._import(d, "TWOWBJOB0001")

		self.assertEqual(sorted(result["added"]), ["ALPHA_S1", "BETA_S1"])
		self.assertEqual(
			sorted(result["verified"]),
			["ALPHA_S1", "BETA_S1"],
			"both samples have a company checksum on paper, so both should be verified",
		)

	def test_canonical_workbook_alongside_a_second_one(self):
		"""One run keeps the stock name, the second is labelled -- the mix you get
		when a second run's sheet is added to a folder that already had one."""
		d = ROOT / "data" / "canonical_plus_one"
		named_r1, named_r2 = write_pair(d, "NAMED")
		extra_r1, extra_r2 = write_pair(d, "EXTRA")
		workbook_at(d / "DNA Sequencing Stats.xlsx", [["NAMED", md5(named_r1), md5(named_r2)]])
		workbook_at(d / "Run2 Stats.xlsx", [["EXTRA", md5(extra_r1), md5(extra_r2)]])

		result = self._import(d, "CANONWBJOB001")

		self.assertIn("NAMED_S1", result["verified"])
		self.assertIn(
			"EXTRA_S1",
			result["verified"],
			"the second workbook's samples are checksummed too, not just the stock-named one's",
		)

	def test_unverifiable_import_says_so(self):
		"""If the app will not or cannot read a workbook it was given, it has to
		warn. Silently importing unverified data is the failure that hides."""
		d = ROOT / "data" / "silent_skip"
		gamma_r1, gamma_r2 = write_pair(d, "GAMMA")
		workbook_at(d / "Run1 Stats.xlsx", [["GAMMA", md5(gamma_r1), md5(gamma_r2)]])
		workbook_at(d / "Run2 Stats.xlsx", [["NOBODY", "0" * 32, "0" * 32]])

		result = self._import(d, "SILENTJOB0001")

		if not result["verified"]:
			self.assertTrue(
				result["warnings"],
				"imported nothing verified and said nothing about it",
			)

	def test_corrupt_sample_in_the_second_workbook_is_rejected(self):
		"""The one that matters. DELTA's reads do not match the checksums its own
		run's workbook lists, so DELTA is corrupt and must not reach the pipeline
		-- being listed in the second workbook rather than the first changes
		nothing about that."""
		d = ROOT / "data" / "corrupt_in_second"
		clean_r1, clean_r2 = write_pair(d, "CLEAN")
		write_pair(d, "DELTA")
		workbook_at(d / "Run1 Stats.xlsx", [["CLEAN", md5(clean_r1), md5(clean_r2)]])
		workbook_at(d / "Run2 Stats.xlsx", [["DELTA", "0" * 32, "0" * 32]])

		result = self._import(d, "CORRUPTJOB001")

		self.assertIn("CLEAN_S1", result["added"])
		self.assertIn("DELTA_S1", result["failed"], "a corrupt sample must never be imported")
		self.assertNotIn("DELTA_S1", result["added"])


class WorkbookPerSubfolder(unittest.TestCase):
	"""A folder upload of two run subfolders, each with its own workbook -- how a
	two-run delivery actually arrives from the browser."""

	@classmethod
	def setUpClass(cls):
		frontend.app.config.update(TESTING=True)
		cls.client = frontend.app.test_client()

	def setUp(self):
		frontend.limiter.enabled = False

	def test_import_route_sees_a_workbook_in_every_run_subfolder(self):
		staging = ROOT / "data" / "subfolder_src"
		first_r1, first_r2 = write_pair(staging / "Run1", "SUBA")
		second_r1, second_r2 = write_pair(staging / "Run2", "SUBB")
		workbook_at(staging / "Run1" / "DNA Sequencing Stats.xlsx", [["SUBA", md5(first_r1), md5(first_r2)]])
		workbook_at(staging / "Run2" / "DNA Sequencing Stats.xlsx", [["SUBB", md5(second_r1), md5(second_r2)]])

		# Post it the way the browser does: every file carries its path within
		# the chosen folder, workbooks included.
		files = []
		for run in ("Run1", "Run2"):
			for source_path in sorted((staging / run).iterdir()):
				files.append(
					(io.BytesIO(source_path.read_bytes()), f"Delivery/{run}/{source_path.name}")
				)
		response = self.client.post(
			"/import", data={"files": files}, content_type="multipart/form-data"
		)
		self.assertEqual(response.status_code, 200, response.get_data(as_text=True))
		result = response.get_json()

		# FASTQs are paired recursively, so both runs' samples land...
		rows, _ = frontend._read_samples(jobs.job_samples_csv(result["job_id"]))
		self.assertEqual(sorted(row["isolate_id"] for row in rows), ["SUBA_S1", "SUBB_S1"])
		# ...and their checksums came up with them, so both should be verified.
		self.assertEqual(
			sorted(result["verified"]),
			["SUBA_S1", "SUBB_S1"],
			"workbook lookup must reach as deep as FASTQ pairing does",
		)


class ChecksumsCarryAcrossUploadRounds(unittest.TestCase):
	"""A job filled by more than one upload round. A stats workbook entered with
	one round must verify samples that arrive in a later round of the same job:
	the sheet lists a whole run, but its FASTQs may be uploaded piecemeal, and a
	sample uploaded after its checksum should be no less verified than one
	uploaded with it."""

	def _round(self, directory, job):
		"""One upload round into `job`'s manifest -- the same samples.csv every
		round, which is what makes them the same job."""
		return import_samples.import_directory(
			directory,
			samples_csv=ROOT / "config" / "jobs" / job / "samples.csv",
			dest_dir=directory / "dest",
		)

	def test_earlier_workbook_verifies_a_later_round_sample(self):
		job = "CARRYJOB0001"
		later_r1, later_r2 = fastq_bytes(1, "later_r1"), fastq_bytes(1, "later_r2")

		# Round 1: EARLY's reads arrive with a workbook that lists BOTH EARLY and
		# LATER -- but LATER's FASTQs are not in this round's folder yet.
		round1 = ROOT / "data" / "carry_round1"
		early_r1, early_r2 = write_pair(round1, "EARLY")
		workbook_at(round1 / "DNA Sequencing Stats.xlsx", [
			["EARLY", md5(early_r1), md5(early_r2)],
			["LATER", md5(later_r1), md5(later_r2)],
		])
		result1 = self._round(round1, job)
		self.assertIn("EARLY_S1", result1["verified"])

		# Round 2: LATER's FASTQs, and no workbook anywhere in this round.
		round2 = ROOT / "data" / "carry_round2"
		round2.mkdir(parents=True, exist_ok=True)
		(round2 / "LATER_S1_R1_001.fastq.gz").write_bytes(later_r1)
		(round2 / "LATER_S1_R2_001.fastq.gz").write_bytes(later_r2)
		result2 = self._round(round2, job)

		self.assertIn(
			"LATER_S1",
			result2["verified"],
			"round 1's workbook must verify a sample that arrives in round 2",
		)
		self.assertNotIn("LATER_S1", result2["failed"])

	def test_later_round_sample_corrupt_against_an_earlier_workbook_is_rejected(self):
		"""The carry-forward is a real check, not a rubber stamp: reads that do
		not match the earlier round's workbook are still rejected."""
		job = "CARRYJOB0002"
		good_r1, good_r2 = fastq_bytes(1, "good_r1"), fastq_bytes(1, "good_r2")

		round1 = ROOT / "data" / "carry_bad_round1"
		round1.mkdir(parents=True, exist_ok=True)
		# Workbook names BADLATER with the checksums of the *good* reads...
		workbook_at(round1 / "DNA Sequencing Stats.xlsx", [
			["BADLATER", md5(good_r1), md5(good_r2)],
		])
		self._round(round1, job)

		# ...but round 2 delivers different bytes under that sample name.
		round2 = ROOT / "data" / "carry_bad_round2"
		round2.mkdir(parents=True, exist_ok=True)
		(round2 / "BADLATER_S1_R1_001.fastq.gz").write_bytes(fastq_bytes(1, "corrupt_r1"))
		(round2 / "BADLATER_S1_R2_001.fastq.gz").write_bytes(fastq_bytes(1, "corrupt_r2"))
		result2 = self._round(round2, job)

		self.assertIn("BADLATER_S1", result2["failed"], "a corrupt later-round sample must be rejected")
		self.assertNotIn("BADLATER_S1", result2["added"])


if __name__ == "__main__":
	unittest.main()
