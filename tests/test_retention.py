"""Retention sweep: it deletes users' results, so its rules must hold exactly.

README contract: a finished job's results are deleted 3h after the user first
views a terminal status, or 7 days after completion, whichever comes first
(unless the job carries a .pinned marker). The job ID goes with them: nothing
keyed by an expired ID -- manifest, upload log, token, run log, or the reports
stored in S3 -- outlives the results it describes.
"""

import json
import os
import shutil
import time
import unittest

import frontend  # noqa: E402
from tests._isolation import REAL_ROOT  # noqa: F401  (must import first)
from workflow.helpers import jobs  # noqa: E402

VIEW_TTL = frontend._VIEW_TTL_SECONDS
MAX_TTL = frontend._MAX_TTL_SECONDS


class FakeStorage:
	"""Stands in for s3_storage. The real one is a no-op without a bucket, which
	would make every "deleted from S3" assertion below pass vacuously."""

	def __init__(self):
		self.results = {}
		self.raw = {}

	def add(self, job_id, isolates=("SAMP",)):
		self.results[job_id] = list(isolates)
		self.raw[job_id] = ["r1.fastq.gz"]

	def list_isolates(self, job_id):
		return sorted(self.results.get(job_id, []))

	def delete_results(self, job_id):
		self.results.pop(job_id, None)

	def delete_raw(self, job_id):
		self.raw.pop(job_id, None)


def make_record(job_id, age=0.0):
	"""The config/jobs/<id> record every upload leaves behind."""
	samples = jobs.job_samples_csv(job_id)
	samples.parent.mkdir(parents=True, exist_ok=True)
	samples.write_text("isolate_id,R1_path,R2_path\nSAMP,r1.fastq.gz,r2.fastq.gz\n")
	log_path = jobs.job_log_path(job_id)
	log_path.parent.mkdir(parents=True, exist_ok=True)
	log_path.write_text("snakemake log")
	written_at = time.time() - age
	os.utime(samples, (written_at, written_at))
	return samples


def make_job(finished_ago=0.0, viewed_ago=None, pinned=False, token=True):
	job_id = jobs.generate_job_id()
	results = jobs.job_results_dir(job_id)
	(results / "SAMP" / "summary").mkdir(parents=True, exist_ok=True)
	(results / "SAMP" / "summary" / "report.html").write_text("<h1>data</h1>")
	make_record(job_id, age=finished_ago)

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


class TestJobRecordDeletion(unittest.TestCase):
	"""The job ID is deleted with the contents it names."""

	def setUp(self):
		self.storage = FakeStorage()
		self._real_storage = frontend._retention_service.storage
		frontend._retention_service.storage = self.storage
		self.addCleanup(
			setattr, frontend._retention_service, "storage", self._real_storage
		)

	def test_expired_job_takes_its_id_with_it(self):
		job_id = make_job(finished_ago=MAX_TTL + 60)
		self.storage.add(job_id)
		frontend._sweep_expired_results()
		self.assertFalse(jobs.job_results_dir(job_id).is_dir(), "results survived")
		self.assertFalse(jobs.job_config_dir(job_id).exists(), "job ID record outlived its results")
		self.assertFalse(jobs.job_log_path(job_id).exists(), "run log outlived its job")
		self.assertEqual(self.storage.list_isolates(job_id), [], "S3 reports outlived their results")

	def test_stored_results_obey_the_view_ttl(self):
		"""The 3h rule is what the upload page promises; S3 must honour it too,
		or the view route keeps serving a job that was told to expire."""
		job_id = make_job(finished_ago=VIEW_TTL + 60, viewed_ago=VIEW_TTL + 1)
		self.storage.add(job_id)
		frontend._sweep_expired_results()
		self.assertEqual(self.storage.list_isolates(job_id), [], "S3 reports ignored the 3h rule")
		self.assertFalse(jobs.job_config_dir(job_id).exists())

	def test_live_job_keeps_its_id(self):
		job_id = make_job(finished_ago=60, viewed_ago=60)
		self.storage.add(job_id)
		frontend._sweep_expired_results()
		self.assertTrue(jobs.job_config_dir(job_id).is_dir(), "fresh job lost its record")
		self.assertEqual(self.storage.list_isolates(job_id), ["SAMP"])

	def test_pinned_job_keeps_its_id(self):
		job_id = make_job(finished_ago=MAX_TTL * 2, viewed_ago=VIEW_TTL * 2, pinned=True)
		self.storage.add(job_id)
		frontend._sweep_expired_results()
		self.assertTrue(jobs.job_config_dir(job_id).is_dir(), "pinned job lost its record")
		self.assertEqual(self.storage.list_isolates(job_id), ["SAMP"], "pinned job lost its reports")

	def test_abandoned_upload_id_expires(self):
		"""An upload that was never run has no results to sweep, so nothing would
		ever collect its record."""
		job_id = jobs.generate_job_id()
		make_record(job_id, age=MAX_TTL + 60)
		frontend._sweep_expired_results()
		self.assertFalse(jobs.job_config_dir(job_id).exists(), "abandoned upload kept its ID forever")

	def test_upload_waiting_to_be_run_keeps_its_id(self):
		"""Same shape as an abandoned one -- a record and no results -- and it
		must survive, or a job would be deleted between upload and Run."""
		job_id = jobs.generate_job_id()
		make_record(job_id, age=60)
		frontend._sweep_expired_results()
		self.assertTrue(jobs.job_config_dir(job_id).is_dir(), "pending upload was deleted")

	def test_record_kept_while_stored_results_can_still_be_served(self):
		"""A job from before delete_results existed: local copy long gone, S3
		copy still live. The ID has to resolve for as long as the bucket answers."""
		job_id = jobs.generate_job_id()
		make_record(job_id, age=MAX_TTL + 60)
		self.storage.add(job_id)
		frontend._sweep_expired_results()
		self.assertTrue(
			jobs.job_config_dir(job_id).is_dir(),
			"record deleted while S3 could still serve the job's results",
		)

	def test_s3_outage_does_not_delete_records(self):
		"""list_isolates raising must not read as 'no contents left'."""

		def explode(job_id):
			raise RuntimeError("S3 unreachable")

		self.storage.list_isolates = explode
		job_id = jobs.generate_job_id()
		make_record(job_id, age=MAX_TTL + 60)
		frontend._sweep_expired_results()
		self.assertTrue(
			jobs.job_config_dir(job_id).is_dir(), "an S3 outage deleted a job's record"
		)

	def test_a_failed_s3_delete_keeps_the_id_resolving(self):
		"""If the reports could not be deleted the bucket can still serve them,
		and view/download answer on the ID alone. Deleting the record anyway
		would leave an ID that serves results but has no samples table."""

		def explode(job_id):
			raise RuntimeError("S3 unreachable")

		self.storage.delete_results = explode
		job_id = make_job(finished_ago=MAX_TTL + 60)
		self.storage.add(job_id)
		frontend._sweep_expired_results()
		self.assertTrue(
			jobs.job_config_dir(job_id).is_dir(),
			"record deleted even though its stored results survived",
		)

	def test_app_state_files_are_not_job_records(self):
		"""config/jobs also holds the persisted queue, run history and drain flag."""
		queue_path = jobs.pipeline_queue_path()
		queue_path.parent.mkdir(parents=True, exist_ok=True)
		queue_path.write_text("[]")
		old = time.time() - (MAX_TTL * 10)
		os.utime(queue_path, (old, old))
		frontend._sweep_expired_results()
		self.assertTrue(queue_path.is_file(), "sweep ate the persisted pipeline queue")


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
