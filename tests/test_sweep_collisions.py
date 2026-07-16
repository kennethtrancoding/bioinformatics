"""What the retention sweep must never do to a job that is still alive.

The sweep runs on its own thread every 15 minutes and deletes results, BV-BRC tokens
and raw reads by age. Nothing stops it running in the middle of an admission, a
queue, a run, or a database refresh -- and every one of those is a moment when a job
still needs the things the sweep is empowered to delete.

That got sharper with two recent changes. Raw reads now live in S3 rather than on
local disk, so a sweep that purges a job's raw is destroying the ONLY copy of its
inputs. And a database refresh drains the queue rather than killing it, so jobs can
now legitimately sit queued for hours -- which is exactly the window in which a sweep
fires.

These tests assert the invariant (a job that is running, queued, or being admitted
keeps everything it needs), not the current behaviour.
"""

import os
import sys
import threading
import time
import unittest
from unittest import mock

import frontend  # noqa: E402
from tests._isolation import REAL_ROOT  # noqa: F401  (must import first)
from tests.test_batching import _REAL_POPEN, Base, token_for  # noqa: E402
from tests.test_s3_streaming import FakeS3  # noqa: E402
from workflow.helpers import jobs, s3_storage  # noqa: E402

_LONG_AGO = time.time() - (30 * 24 * 60 * 60)  # a month back: well past every TTL


def _age(path, when=_LONG_AGO):
	"""Backdate a file or directory so the sweep considers it expired."""
	os.utime(path, (when, when))


class SweepBase(Base):
	def setUp(self):
		super().setUp()
		jobs.drain_flag_path().unlink(missing_ok=True)
		jobs.pipeline_queue_path().unlink(missing_ok=True)
		self.s3 = FakeS3()
		for attribute, value in (("_client", self.s3), ("_BUCKET", "test-bucket")):
			patcher = mock.patch.object(s3_storage, attribute, value)
			patcher.start()
			self.addCleanup(patcher.stop)
		frontend.subprocess.Popen = self._held_popen
		self.addCleanup(self._teardown)

	def _teardown(self):
		for pipeline_process in list(frontend._pipeline_processes.values()):
			pipeline_process.kill()
		frontend._pipeline_processes.clear()
		frontend._pipeline_queue.clear()
		frontend.subprocess.Popen = _REAL_POPEN
		frontend.MAX_CONCURRENT_PIPELINES = 2
		jobs.drain_flag_path().unlink(missing_ok=True)

	@staticmethod
	def _held_popen(argv, **kwargs):
		return _REAL_POPEN([sys.executable, "-c", "import time; time.sleep(30)"], **kwargs)

	def runnable(self, name):
		job_id = self.submit_pair(name).get_json()["job_id"]
		token_for(job_id)
		return job_id

	def token_exists(self, job_id):
		return jobs.job_token_path(job_id).is_file()

	def raw_in_s3(self, job_id):
		return [key for key in self.s3.raw_keys() if job_id in key]


# Running jobs
class TestSweepVersusARunningJob(SweepBase):
	def test_a_running_job_keeps_its_results_directory(self):
		job_id = self.runnable("RUNNING_RESULTS")
		self.client.post("/run", data={"job_id": job_id})
		results_directory = jobs.job_results_dir(job_id)
		results_directory.mkdir(parents=True, exist_ok=True)
		(results_directory / "partial.csv").write_text("half a run")
		_age(results_directory)

		frontend._sweep_expired_results()

		self.assertTrue(
			(results_directory / "partial.csv").is_file(),
			"the sweep deleted the results directory Snakemake is writing into",
		)

	def test_a_running_job_keeps_its_bvbrc_token(self):
		"""The token is what the run authenticates to BV-BRC with, all the way through
		assembly. Deleting it mid-run breaks the job."""
		job_id = self.runnable("RUNNING_TOKEN")
		self.client.post("/run", data={"job_id": job_id})
		_age(jobs.job_token_path(job_id))

		frontend._sweep_expired_results()

		self.assertTrue(
			self.token_exists(job_id), "the sweep destroyed a running job's BV-BRC token"
		)

	def test_a_running_job_keeps_its_reads_in_s3(self):
		"""S3 holds the only copy of the reads now, and the run has not fetched them
		all yet."""
		job_id = self.runnable("RUNNING_RAW")
		self.client.post("/run", data={"job_id": job_id})
		_age(jobs.job_data_dir(job_id))

		frontend._sweep_expired_results()

		self.assertTrue(self.raw_in_s3(job_id), "the sweep deleted a running job's inputs")


# Queued jobs
class TestSweepVersusTheQueue(SweepBase):
	def _queue_one_behind_a_running_job(self, running_name, queued_name):
		frontend.MAX_CONCURRENT_PIPELINES = 1
		running = self.runnable(running_name)
		self.client.post("/run", data={"job_id": running})
		queued = self.runnable(queued_name)
		self.assertEqual(self.client.post("/run", data={"job_id": queued}).status_code, 202)
		return running, queued

	def test_a_queued_job_keeps_its_reads(self):
		_running, queued = self._queue_one_behind_a_running_job("Q_RUN_A", "Q_WAIT_A")
		_age(jobs.job_data_dir(queued))

		frontend._sweep_expired_results()

		self.assertTrue(self.raw_in_s3(queued), "the sweep deleted a queued job's inputs")

	def test_a_queued_job_keeps_its_bvbrc_token(self):
		"""A job can now sit queued for hours -- a database refresh drains the queue
		rather than killing it -- so this is a window the sweep really does fire in."""
		_running, queued = self._queue_one_behind_a_running_job("Q_RUN_B", "Q_WAIT_B")
		_age(jobs.job_token_path(queued))

		frontend._sweep_expired_results()

		self.assertTrue(
			self.token_exists(queued),
			"the sweep destroyed a queued job's token; it will fail BV-BRC auth when its turn comes",
		)


# Database refresh
class TestSweepVersusADatabaseRefresh(SweepBase):
	def test_jobs_parked_by_a_refresh_survive_a_sweep(self):
		"""During a refresh the queue is deliberately held: nothing is running, so a
		parked job looks idle to a sweep that only checks age."""
		jobs.drain_flag_path().touch()
		parked = self.runnable("REFRESH_PARKED")
		self.assertEqual(self.client.post("/run", data={"job_id": parked}).status_code, 202)
		self.assertEqual(frontend._is_draining(), True)

		_age(jobs.job_data_dir(parked))
		_age(jobs.job_token_path(parked))
		frontend._sweep_expired_results()

		self.assertTrue(self.raw_in_s3(parked), "the refresh's parked job lost its inputs")
		self.assertTrue(self.token_exists(parked), "the refresh's parked job lost its token")


# Admission race
class TestSweepVersusAdmission(SweepBase):
	"""The two must be mutually exclusive. The sweep destroys the token before it
	deletes anything, so a run resurrected halfway through would start on a job whose
	credential is already gone and whose inputs are going. The one outcome that must be
	impossible is a run that starts on deleted data -- whichever side wins the race, the
	result has to be coherent.
	"""

	def _expired_finished_job(self, name):
		job_id = self.runnable(name)
		results_directory = jobs.job_results_dir(job_id)
		results_directory.mkdir(parents=True, exist_ok=True)
		(results_directory / "master_report.csv").write_text("old results")
		frontend._write_job_status(job_id, True)
		_age(jobs.job_status_path(job_id))
		jobs.job_first_viewed_path(job_id).touch()
		_age(jobs.job_first_viewed_path(job_id))
		return job_id, results_directory

	def test_a_rerun_that_arrives_mid_deletion_is_refused_rather_than_half_started(self):
		"""The TOCTOU: the sweep has committed to deleting an expired job, and the user
		re-runs it in the gap. Admission must lose, and must say so."""
		job_id, results_directory = self._expired_finished_job("RERUN_RACE")

		# Hold the sweep between claiming the job and deleting it -- exactly the gap a
		# 15-minute sweep and an impatient user contend for.
		decided = threading.Event()
		may_delete = threading.Event()
		real_rmtree = frontend.shutil.rmtree

		def _paused_rmtree(path, **kwargs):
			decided.set()
			may_delete.wait(timeout=5)
			return real_rmtree(path, **kwargs)

		with mock.patch.object(frontend.shutil, "rmtree", _paused_rmtree):
			sweeper = threading.Thread(target=frontend._sweep_expired_results, daemon=True)
			sweeper.start()
			self.assertTrue(decided.wait(timeout=5), "the sweep never reached the deletion")

			run_response = self.client.post("/run", data={"job_id": job_id})
			may_delete.set()
			sweeper.join(timeout=10)

		# Refused, with an explanation the user can act on...
		self.assertEqual(run_response.status_code, 410)
		self.assertIn("expired", run_response.get_json()["error"].lower())
		# ...and crucially, no run was started on a job being demolished.
		self.assertNotIn(job_id, frontend._pipeline_processes)
		self.assertNotIn(job_id, frontend._pipeline_queue)
		# The deletion completed, so the outcome is coherent: nothing half-alive.
		self.assertFalse(results_directory.exists())
		self.assertEqual(self.raw_in_s3(job_id), [])

	def test_a_rerun_that_gets_in_first_stops_the_sweep_deleting_the_job(self):
		"""The other ordering. Admission clears the status marker as its first act, so
		from that instant the job is live and the sweep must leave it alone."""
		job_id, results_directory = self._expired_finished_job("RERUN_WINS")

		self.assertEqual(self.client.post("/run", data={"job_id": job_id}).status_code, 200)
		frontend._sweep_expired_results()

		self.assertIn(job_id, frontend._pipeline_processes)
		self.assertTrue(results_directory.exists(), "the sweep deleted a running job's results")
		self.assertTrue(self.token_exists(job_id), "the sweep destroyed a running job's token")
		self.assertTrue(self.raw_in_s3(job_id), "the sweep deleted a running job's inputs")


# Expiration
class TestTheSweepStillDoesItsJob(SweepBase):
	def test_a_failed_run_that_nobody_is_using_still_expires(self):
		job_id = self.runnable("FAILED_OLD")
		results_directory = jobs.job_results_dir(job_id)
		results_directory.mkdir(parents=True, exist_ok=True)
		(results_directory / "partial.csv").write_text("failed run")
		frontend._write_job_status(job_id, False)
		_age(jobs.job_status_path(job_id))

		frontend._sweep_expired_results()

		self.assertFalse(results_directory.exists(), "an expired failed run was not cleaned up")

	def test_a_pinned_job_is_never_expired(self):
		job_id = self.runnable("PINNED")
		results_directory = jobs.job_results_dir(job_id)
		results_directory.mkdir(parents=True, exist_ok=True)
		(results_directory / "keep.csv").write_text("pinned")
		frontend._write_job_status(job_id, True)
		_age(jobs.job_status_path(job_id))
		jobs.job_pinned_path(job_id).touch()

		frontend._sweep_expired_results()

		self.assertTrue((results_directory / "keep.csv").is_file())


if __name__ == "__main__":
	unittest.main()
