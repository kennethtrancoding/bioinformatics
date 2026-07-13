"""Retention sweep: it deletes users' results, so its rules must hold exactly.

README contract: a finished job's results are deleted 3h after the user first
views a terminal status, or 7 days after completion, whichever comes first
(unless the job carries a .pinned marker).
"""

import json
import os
import shutil
import time
import unittest

import frontend  # noqa: E402
from tests._isolation import REAL_ROOT  # noqa: F401  (must import first)
from workflow.lib import jobs  # noqa: E402

VIEW_TTL = frontend._VIEW_TTL_SECONDS
MAX_TTL = frontend._MAX_TTL_SECONDS


def make_job(finished_ago=0.0, viewed_ago=None, pinned=False, token=True):
	job_id = jobs.generate_job_id()
	results = jobs.job_results_dir(job_id)
	(results / "SAMP" / "summary").mkdir(parents=True, exist_ok=True)
	(results / "SAMP" / "summary" / "report.html").write_text("<h1>data</h1>")

	status = jobs.job_status_path(job_id)
	status.write_text(json.dumps({"done": True, "success": True, "finished_at": time.time()}))
	finished_at = time.time() - finished_ago
	os.utime(status, (finished_at, finished_at))

	if viewed_ago is not None:
		marker = jobs.job_first_viewed_path(job_id)
		marker.touch()
		viewed_at = time.time() - viewed_ago
		os.utime(marker, (viewed_at, viewed_at))

	if pinned:
		jobs.job_pinned_path(job_id).touch()

	if token:
		tok = jobs.job_token_path(job_id)
		tok.parent.mkdir(parents=True, exist_ok=True)
		tok.write_text(json.dumps({"access_token": "t", "user_id": "u"}))

	return job_id


class TestRetention(unittest.TestCase):
	def test_recently_viewed_results_are_kept(self):
		job_id = make_job(finished_ago=60, viewed_ago=60)
		frontend._sweep_expired_results()
		self.assertTrue(jobs.job_results_dir(job_id).is_dir(), "fresh results were deleted")

	def test_results_deleted_3h_after_first_view(self):
		job_id = make_job(finished_ago=VIEW_TTL + 60, viewed_ago=VIEW_TTL + 1)
		frontend._sweep_expired_results()
		self.assertFalse(jobs.job_results_dir(job_id).is_dir(), "expired results were not deleted")
		self.assertFalse(jobs.job_token_path(job_id).exists(), "BV-BRC token outlived results")

	def test_unviewed_results_deleted_after_7_days(self):
		job_id = make_job(finished_ago=MAX_TTL + 60, viewed_ago=None)
		frontend._sweep_expired_results()
		self.assertFalse(jobs.job_results_dir(job_id).is_dir(), "7-day safety net did not fire")

	def test_unviewed_results_kept_before_7_days(self):
		job_id = make_job(finished_ago=MAX_TTL - 3600, viewed_ago=None)
		frontend._sweep_expired_results()
		self.assertTrue(jobs.job_results_dir(job_id).is_dir())

	def test_pinned_job_is_never_deleted(self):
		job_id = make_job(finished_ago=MAX_TTL * 2, viewed_ago=VIEW_TTL * 2, pinned=True)
		frontend._sweep_expired_results()
		self.assertTrue(jobs.job_results_dir(job_id).is_dir(), "pinned demo job was deleted")

	def test_running_job_is_never_deleted(self):
		"""No .run_status.json yet == still running; must survive the sweep."""
		job_id = jobs.generate_job_id()
		results = jobs.job_results_dir(job_id)
		results.mkdir(parents=True, exist_ok=True)
		(results / "partial.txt").write_text("in progress")
		frontend._sweep_expired_results()
		self.assertTrue(results.is_dir(), "in-progress run was deleted")

	def test_abandoned_upload_token_expires(self):
		"""An upload that never runs must not leave a bearer token on disk forever."""
		job_id = jobs.generate_job_id()
		tok = jobs.job_token_path(job_id)
		tok.parent.mkdir(parents=True, exist_ok=True)
		tok.write_text(json.dumps({"access_token": "t"}))
		old = time.time() - (MAX_TTL + 60)
		os.utime(tok, (old, old))
		frontend._sweep_expired_results()
		self.assertFalse(tok.exists(), "stale token was not destroyed")

	def test_stray_directory_does_not_wedge_the_sweep(self):
		"""A results dir whose name isn't a valid job ID must not stop the sweep
		from pruning the expired jobs that come after it.

		A manual `snakemake --config results_dir=results/<name>` run, or a
		pre-migration isolate folder, leaves exactly such a directory.
		"""
		stray = frontend.RESULTS_ROOT / "manual_run"
		stray.mkdir(parents=True, exist_ok=True)
		(stray / ".run_status.json").write_text('{"done": true, "success": true}')
		old = time.time() - (MAX_TTL + 60)
		os.utime(stray / ".run_status.json", (old, old))

		expired = make_job(finished_ago=MAX_TTL + 60)
		try:
			frontend._sweep_expired_results()
		except Exception as exc:
			self.fail(f"sweep crashed on a stray directory: {exc!r}")
		finally:
			# Leave no stray dir behind: it would wedge every later sweep too,
			# which is precisely the failure this test exists to catch.
			shutil.rmtree(stray, ignore_errors=True)
		self.assertFalse(
			jobs.job_results_dir(expired).is_dir(),
			"expired job survived because a stray directory aborted the sweep",
		)


if __name__ == "__main__":
	unittest.main(verbosity=2)
