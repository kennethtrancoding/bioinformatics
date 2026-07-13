"""A normal user's whole day: fill jobs by every input method, run more of them than
there are slots, and have the weekly database refresh land in the middle.

Everything here goes through the real HTTP endpoints (/submit, /import,
/cloud-import, /run, /abort, /status, /api/health) against real uploaded bytes. Two
things are substituted, and only two:

  * Snakemake, by a process this test can hold open and release on demand. Slot
    contention is about process lifetime, not about genomics, and a real run is ~96
    minutes -- filling two slots plus a queue plus a restart would be a multi-hour
    test that could never run in CI.
  * Google Drive, by cloud_import's own fake session (tests/test_cloud_import.py's
    FakeDrive), which serves real FASTQ bytes over the real _google_list /
    _google_walk / _download code path. A live Drive folder needs an API key and
    Google's uptime, neither of which belongs in a repeatable test.

The scenario the drain has to survive is the whole point: a refresh restarts the
service, Snakemake is a child of it, and a run in flight dies with it. So the refresh
sets a drain flag, waits for the running jobs to finish, and restarts onto an idle
app -- while everything submitted meanwhile queues to disk instead of starting, and
resumes by itself on the other side.
"""

import subprocess
import sys
import time
import unittest
from pathlib import Path
from unittest import mock

from tests._isolation import TMP_ROOT, REAL_ROOT  # noqa: F401  (must import first)

import frontend  # noqa: E402
from workflow.lib import cloud_import, jobs  # noqa: E402

from tests.test_batching import Base, token_for, _REAL_POPEN  # noqa: E402
from tests.test_cloud_import import FakeDrive, SHARED_FOLDER, fastq_bytes, md5, stats_workbook  # noqa: E402

RELEASE_DIR = TMP_ROOT / "release"
RELEASE_DIR.mkdir(parents=True, exist_ok=True)

# Blocks until its release file appears, so a test decides exactly when a run ends.
_HOLD_SCRIPT = (
	"import os, sys, time\n"
	"path = sys.argv[1]\n"
	"while not os.path.exists(path):\n"
	"    time.sleep(0.02)\n"
)


def _held_popen(argv, **kwargs):
	"""Stand in for snakemake: a real process, held open until its job is released.

	Reads the job id out of the argv the app built, so each run gets its own release
	file and runs can be finished in any order -- which is what lets this test free
	one slot while another run is still going."""
	job_id = next(
		argument.split("=", 1)[1] for argument in argv if argument.startswith("job_id=")
	)
	return _REAL_POPEN(
		[sys.executable, "-c", _HOLD_SCRIPT, str(RELEASE_DIR / job_id)], **kwargs
	)


def _wait_until(predicate, timeout=10, what="condition"):
	deadline = time.time() + timeout
	while time.time() < deadline:
		if predicate():
			return True
		time.sleep(0.02)
	raise AssertionError(f"timed out waiting for {what}")


class NormalUserFlow(Base):
	def setUp(self):
		super().setUp()
		jobs.drain_flag_path().unlink(missing_ok=True)
		jobs.pipeline_queue_path().unlink(missing_ok=True)
		frontend.subprocess.Popen = _held_popen
		# Two slots, as in production (MAX_CONCURRENT_PIPELINES defaults to 2).
		self._real_max = frontend.MAX_CONCURRENT_PIPELINES
		frontend.MAX_CONCURRENT_PIPELINES = 2
		self.addCleanup(self._teardown)

	def _teardown(self):
		for job_id in list(frontend._pipeline_processes):
			self.release(job_id)
		for pipeline_process in list(frontend._pipeline_processes.values()):
			pipeline_process.kill()
		frontend._pipeline_processes.clear()
		frontend._pipeline_queue.clear()
		frontend.MAX_CONCURRENT_PIPELINES = self._real_max
		frontend.subprocess.Popen = _REAL_POPEN
		jobs.drain_flag_path().unlink(missing_ok=True)

	# User actions
	def release(self, job_id):
		"""Let a held run finish, the way a real run finishing frees its slot."""
		(RELEASE_DIR / job_id).touch()

	def cloud_import(self, sample_name, job_id=None):
		"""Pull a pair from a Google Drive share link, over the real code path."""
		r1, r2 = fastq_bytes(), fastq_bytes(2)
		drive = FakeDrive(
			{
				"FOLDER1": {"name": "Run", "mimeType": cloud_import._GOOGLE_FOLDER_MIME},
				"f1": {
					"name": f"{sample_name}_R1_001.fastq.gz",
					"mimeType": "application/gzip",
					"parent": "FOLDER1",
					"body": r1,
				},
				"f2": {
					"name": f"{sample_name}_R2_001.fastq.gz",
					"mimeType": "application/gzip",
					"parent": "FOLDER1",
					"body": r2,
				},
				"x1": {
					"name": "DNA Sequencing Stats.xlsx",
					"mimeType": "application/vnd.ms-excel",
					"parent": "FOLDER1",
					"body": stats_workbook([[sample_name, md5(r1), md5(r2)]]),
				},
			}
		)
		payload = {"share_url": SHARED_FOLDER}
		if job_id:
			payload["job_id"] = job_id
		with mock.patch.object(cloud_import, "_SESSION", drive), mock.patch.object(
			cloud_import, "GOOGLE_API_KEY", "test-key"
		):
			response = self.client.post("/cloud-import", data=payload)
			self.assertEqual(response.status_code, 202, response.get_data(as_text=True))
			imported_job_id = response.get_json()["job_id"]
			_wait_until(
				lambda: self.client.get(
					f"/cloud-import/status?job_id={imported_job_id}"
				).get_json().get("state")
				!= "running",
				timeout=30,
				what="cloud import to finish",
			)
		record = self.client.get(f"/cloud-import/status?job_id={imported_job_id}").get_json()
		self.assertEqual(record["state"], "done", record)
		return imported_job_id

	def runnable(self, job_id):
		token_for(job_id)
		return job_id

	def start(self, job_id):
		return self.client.post("/run", data={"job_id": job_id})

	def health(self):
		return self.client.get("/api/health").get_json()["pipelines"]

	# Tests
	@unittest.skip("Cloud import is disabled; this test needs the /cloud-import route.")
	def test_every_input_method_produces_a_runnable_job(self):
		"""Each of the three doors into the app, on its own."""
		by_pair = self.runnable(self.submit_pair("SOLO_PAIR").get_json()["job_id"])
		by_folder = self.runnable(
			self.import_folder(["SOLO_FOLD_S1", "SOLO_FOLD_S2"]).get_json()["job_id"]
		)
		by_cloud = self.runnable(self.cloud_import("SOLO_CLOUD_S3"))

		self.assertEqual(self.isolates(by_pair), ["SOLO_PAIR"])
		self.assertEqual(self.isolates(by_folder), ["SOLO_FOLD_S1", "SOLO_FOLD_S2"])
		self.assertEqual(self.isolates(by_cloud), ["SOLO_CLOUD_S3"])

		# One at a time, so each starts into a free slot rather than queueing behind
		# the previous one -- releasing a run does not free its slot instantly, the
		# process still has to exit and the watcher still has to reap it.
		for job_id in (by_pair, by_folder, by_cloud):
			self.assertEqual(self.start(job_id).status_code, 200)
			self.release(job_id)
			_wait_until(
				lambda job_id=job_id: job_id not in frontend._pipeline_processes,
				what=f"{job_id} to release its slot",
			)

	@unittest.skip("Cloud import is disabled; this test needs the /cloud-import route.")
	def test_all_three_methods_fill_one_job_and_it_runs(self):
		"""The combination: one batch assembled from a folder import, a stray pair and
		a cloud pull, then run as a single job."""
		job_id = self.import_folder(["MIX_FOLD_S1"]).get_json()["job_id"]
		self.submit_pair("MIX_PAIR", job_id=job_id)
		self.cloud_import("MIX_CLOUD_S2", job_id=job_id)
		self.runnable(job_id)

		self.assertEqual(self.isolates(job_id), ["MIX_CLOUD_S2", "MIX_FOLD_S1", "MIX_PAIR"])
		# Every upload is recorded, with the method that brought it in.
		methods = sorted(entry["method"] for entry in frontend._read_uploads(job_id))
		self.assertEqual(methods, ["cloud", "folder", "pair"])

		self.assertEqual(self.start(job_id).status_code, 200)
		self.assertIn(job_id, frontend._pipeline_processes)
		self.release(job_id)

	def test_slots_fill_then_further_runs_queue_in_order(self):
		jobs_by_letter = {
			letter: self.runnable(self.submit_pair(f"LOAD_{letter}").get_json()["job_id"])
			for letter in "ABCD"
		}

		# Two slots, so A and B start...
		self.assertEqual(self.start(jobs_by_letter["A"]).status_code, 200)
		self.assertEqual(self.start(jobs_by_letter["B"]).status_code, 200)
		# ...and C and D queue, FIFO, reporting their place in line.
		for expected_position, letter in ((1, "C"), (2, "D")):
			response = self.start(jobs_by_letter[letter])
			self.assertEqual(response.status_code, 202)
			payload = response.get_json()
			self.assertTrue(payload["queued"])
			self.assertEqual(payload["queue_position"], expected_position)

		self.assertEqual(self.health(), {"draining": False, "running": 2, "queued": 2})
		# Starting a job that is already in flight is refused, running or queued.
		self.assertEqual(self.start(jobs_by_letter["A"]).status_code, 409)
		self.assertEqual(self.start(jobs_by_letter["C"]).status_code, 409)

		# A finishes -> its slot goes to C, the head of the queue, not to D.
		self.release(jobs_by_letter["A"])
		_wait_until(
			lambda: jobs_by_letter["C"] in frontend._pipeline_processes,
			what="C to be promoted into A's slot",
		)
		self.assertEqual(list(frontend._pipeline_queue), [jobs_by_letter["D"]])
		self.assertEqual(self.health()["running"], 2)

	def test_a_queued_run_can_be_cancelled_before_it_ever_starts(self):
		first = self.runnable(self.submit_pair("CANCEL_A").get_json()["job_id"])
		second = self.runnable(self.submit_pair("CANCEL_B").get_json()["job_id"])
		waiting = self.runnable(self.submit_pair("CANCEL_C").get_json()["job_id"])
		self.start(first)
		self.start(second)
		self.assertEqual(self.start(waiting).status_code, 202)

		self.assertEqual(self.client.post("/abort", data={"job_id": waiting}).status_code, 200)

		self.assertNotIn(waiting, frontend._pipeline_queue)
		status = self.client.get(f"/status?job_id={waiting}").get_json()
		self.assertTrue(status["done"])
		self.assertFalse(status["success"])
		self.assertIn("aborted", status["error"].lower())

	def test_a_database_refresh_lands_in_the_middle_of_a_busy_queue(self):
		"""The scenario the whole drain design exists for.

		Two runs in flight, two queued, and the weekly refresh starts. Nothing in
		flight may be killed; nothing queued may be lost; nothing new may start into
		the restart. Afterwards the queue picks up by itself.
		"""
		running_a = self.runnable(self.submit_pair("REF_A").get_json()["job_id"])
		running_b = self.runnable(self.import_folder(["REF_B_S1"]).get_json()["job_id"])
		# Was a cloud import; that feature is disabled, and what this test is about is
		# the drain, not the door the job came in through.
		queued_c = self.runnable(self.import_folder(["REF_C_S2"]).get_json()["job_id"])
		queued_d = self.runnable(self.submit_pair("REF_D").get_json()["job_id"])

		self.assertEqual(self.start(running_a).status_code, 200)
		self.assertEqual(self.start(running_b).status_code, 200)
		self.assertEqual(self.start(queued_c).status_code, 202)
		self.assertEqual(self.start(queued_d).status_code, 202)

		# --- refresh-databases.sh begins: drain, then build the image (slow), then wait.
		jobs.drain_flag_path().touch()
		self.assertEqual(self.health()["draining"], True)

		# A user submits during the refresh. It must be accepted and parked, not refused.
		queued_e = self.runnable(self.submit_pair("REF_E").get_json()["job_id"])
		response = self.start(queued_e)
		self.assertEqual(response.status_code, 202)
		self.assertIn("database refresh", response.get_json()["message"])

		# The first in-flight run finishes. Its slot must stay EMPTY: promoting the
		# queue now would feed a fresh run straight into the restart that is coming.
		self.release(running_a)
		_wait_until(
			lambda: running_a not in frontend._pipeline_processes,
			what="the running job to finish",
		)
		self.assertNotIn(queued_c, frontend._pipeline_processes)
		self.assertEqual(self.health()["running"], 1)
		self.assertEqual(list(frontend._pipeline_queue), [queued_c, queued_d, queued_e])

		# The second finishes too. The host now sees running == 0 and restarts.
		self.release(running_b)
		_wait_until(lambda: self.health()["running"] == 0, what="the app to go idle")

		# Both in-flight runs completed normally -- the refresh killed neither.
		for job_id in (running_a, running_b):
			self.assertTrue(self.client.get(f"/status?job_id={job_id}").get_json()["success"])

		# --- the restart itself: a new process, with no memory of either structure.
		frontend._pipeline_processes.clear()
		frontend._pipeline_queue.clear()
		frontend.run_startup_recovery()

		# The queue survived on disk, the drain lifted, and the parked runs started
		# themselves -- in order, up to the slot limit.
		self.assertFalse(jobs.drain_flag_path().is_file())
		self.assertIn(queued_c, frontend._pipeline_processes)
		self.assertIn(queued_d, frontend._pipeline_processes)
		self.assertEqual(list(frontend._pipeline_queue), [queued_e])
		self.assertEqual(self.health(), {"draining": False, "running": 2, "queued": 1})

		# And the run that only ever waited was not mistaken for one the restart killed.
		self.assertIsNone(frontend._read_job_status(queued_e))

	def test_a_run_killed_by_the_restart_surfaces_as_failed_not_as_silence(self):
		"""If the refresh does not drain -- or the box simply dies -- the run that was
		executing must still tell the user something."""
		victim = self.runnable(self.submit_pair("VICTIM").get_json()["job_id"])
		self.assertEqual(self.start(victim).status_code, 200)
		_wait_until(lambda: victim in frontend._pipeline_processes, what="the run to start")

		# The container dies. In production the watcher thread dies with the process and
		# nothing is ever recorded -- but here it is alive in this very interpreter and
		# will happily write a status of its own. So let it finish first, and only then
		# clear the status, which is the state a real restart actually leaves behind:
		# a run that began, and no record of how it ended.
		frontend._pipeline_processes[victim].kill()
		_wait_until(
			lambda: jobs.job_status_path(victim).is_file(),
			what="the watcher to finish, so it cannot race the recovery",
		)
		frontend._pipeline_processes.clear()
		frontend._pipeline_queue.clear()
		jobs.job_status_path(victim).unlink()

		frontend.run_startup_recovery()

		status = self.client.get(f"/status?job_id={victim}").get_json()
		self.assertTrue(status["done"])
		self.assertFalse(status["success"])
		self.assertIn("restarted", status["error"])


if __name__ == "__main__":
	unittest.main()
