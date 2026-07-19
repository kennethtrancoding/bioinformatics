"""Surviving a restart: draining before one, and recovering after one.

The running set and the queue live in memory, so a restart -- a weekly database
refresh (deploy/refresh-databases.sh), a crash, a redeploy -- used to erase both.
A run that was executing died with no terminal status, so /status answered 404
and the browser fell silent on a run that neither finished nor failed; a run that
was merely queued vanished without even partial results, because admission clears
its run markers before queueing it.

These cover the two halves of the fix: the drain that keeps a planned restart from
killing anything, and the recovery that cleans up after an unplanned one.
"""

import io
import sys
import threading
import unittest

import frontend  # noqa: E402
from tests._isolation import REAL_ROOT  # noqa: F401  (must import first)
from tests.test_batching import _REAL_POPEN, Base, token_for  # noqa: E402
from tests.test_cloud_import import fastq_bytes  # noqa: E402
from workflow.helpers import jobs  # noqa: E402
from workflow.helpers.bvbrc_client import BVBRCClient  # noqa: E402


def _sleeping_popen(argv, **kwargs):
	"""A stand-in for snakemake that stays alive, so it occupies a slot."""
	return _REAL_POPEN([sys.executable, "-c", "import time; time.sleep(30)"], **kwargs)


class DrainBase(Base):
	def setUp(self):
		super().setUp()
		jobs.drain_flag_path().unlink(missing_ok=True)
		jobs.pipeline_queue_path().unlink(missing_ok=True)
		self.addCleanup(setattr, frontend, "subprocess", frontend.subprocess)
		self.addCleanup(self._kill_pipelines)

	def _kill_pipelines(self):
		frontend.subprocess.Popen = _REAL_POPEN
		for pipeline_process in list(frontend._pipeline_processes.values()):
			pipeline_process.kill()
		frontend._pipeline_processes.clear()
		frontend._pipeline_queue.clear()

	def runnable_job(self, name):
		job_id = self.submit_pair(name).get_json()["job_id"]
		token_for(job_id)
		return job_id

	def start_drain(self):
		path = jobs.drain_flag_path()
		path.parent.mkdir(parents=True, exist_ok=True)
		path.touch()

	def persisted_queue(self):
		import json

		return json.loads(jobs.pipeline_queue_path().read_text())


# Draining
class TestDrainQueuesInsteadOfStarting(DrainBase):
	def test_run_queues_while_draining_even_with_a_free_slot(self):
		"""The whole point: slots are free, but a run started now would be killed
		by the restart the drain is preparing for."""
		job_id = self.runnable_job("DRAIN1")
		self.start_drain()
		frontend.subprocess.Popen = _sleeping_popen

		response = self.client.post("/run", data={"job_id": job_id})

		self.assertEqual(response.status_code, 202)
		self.assertTrue(response.get_json()["queued"])
		self.assertIn("database refresh", response.get_json()["message"])
		self.assertEqual(frontend._pipeline_processes, {})
		self.assertEqual(list(frontend._pipeline_queue), [job_id])

	def test_a_queue_does_not_drain_into_slots_while_draining(self):
		"""A finishing run frees a slot mid-drain. That slot must stay empty."""
		job_id = self.runnable_job("DRAIN2")
		self.start_drain()
		frontend.subprocess.Popen = _sleeping_popen
		self.client.post("/run", data={"job_id": job_id})

		with frontend._pipeline_lock:
			frontend._drain_pipeline_queue()

		self.assertEqual(frontend._pipeline_processes, {})
		self.assertEqual(list(frontend._pipeline_queue), [job_id])

	def test_health_reports_drain_state_without_leaking_job_ids(self):
		"""deploy/refresh-databases.sh polls this to know when it is safe to
		restart. It is unauthenticated, and a job ID is a credential."""
		job_id = self.runnable_job("DRAIN3")
		self.start_drain()
		frontend.subprocess.Popen = _sleeping_popen
		self.client.post("/run", data={"job_id": job_id})

		payload = self.client.get("/api/health").get_json()["pipelines"]

		self.assertEqual(payload, {"draining": True, "running": 0, "queued": 1})
		self.assertNotIn(job_id, self.client.get("/api/health").get_data(as_text=True))


# Uploading mid-refresh
class TestUploadsDuringADrain(DrainBase):
	"""A refresh drains the pipeline. It must not close the front door.

	The weekly refresh takes a while, and someone pasting files into the page has
	no way to know one is running. Uploading is not running: the bytes only have to
	land on disk and in the manifest, and the restart disturbs neither. So an upload
	that arrives mid-refresh is still accepted -- and if it asked to auto-run, the
	run it triggers has to respect the drain and queue, exactly as POST /run does.
	"""

	def setUp(self):
		super().setUp()
		# Any run that does slip past the drain would occupy a slot rather than
		# exit instantly, so an escaped start is visible instead of racing us.
		frontend.subprocess.Popen = _sleeping_popen

	def test_a_pair_uploaded_mid_refresh_is_still_accepted(self):
		self.start_drain()

		response = self.submit_pair("MIDREF_PAIR")

		self.assertEqual(response.status_code, 200)
		job_id = response.get_json()["job_id"]
		self.assertEqual(self.isolates(job_id), ["MIDREF_PAIR"])
		self.assertEqual(frontend._pipeline_processes, {})

	def test_a_folder_imported_mid_refresh_is_still_accepted(self):
		self.start_drain()

		response = self.import_folder(["MIDREF_F_S1", "MIDREF_F_S2"])

		self.assertEqual(response.status_code, 200)
		job_id = response.get_json()["job_id"]
		self.assertEqual(self.isolates(job_id), ["MIDREF_F_S1", "MIDREF_F_S2"])
		self.assertEqual(frontend._pipeline_processes, {})

	def test_an_upload_can_still_be_added_to_an_existing_job_mid_refresh(self):
		"""The refresh lands between two halves of one batch."""
		job_id = self.submit_pair("MIDREF_FIRST").get_json()["job_id"]
		self.start_drain()

		response = self.submit_pair("MIDREF_SECOND", job_id=job_id)

		self.assertEqual(response.status_code, 200)
		self.assertEqual(self.isolates(job_id), ["MIDREF_FIRST", "MIDREF_SECOND"])

	def test_an_auto_run_upload_mid_refresh_queues_instead_of_starting(self):
		"""The files land and the run they asked for waits for the restart."""
		self.start_drain()
		real_login = BVBRCClient.login
		BVBRCClient.login = lambda self, username, password: (token_for(self.job_id) or True)
		try:
			body = self.submit_pair(
				"MIDREF_AUTO", auto_run="1", username="user", password="pass"
			).get_json()
		finally:
			BVBRCClient.login = real_login

		self.assertTrue(body["auto_run"]["started"])
		self.assertTrue(body["auto_run"]["queued"])
		self.assertEqual(self.isolates(body["job_id"]), ["MIDREF_AUTO"])
		self.assertEqual(frontend._pipeline_processes, {})
		self.assertEqual(list(frontend._pipeline_queue), [body["job_id"]])

	def test_an_upload_queued_mid_refresh_runs_once_the_refresh_is_over(self):
		"""Nothing uploaded during the refresh is lost: the queue picks it up when
		the drain lifts, without the user having to press anything again."""
		self.start_drain()
		real_login = BVBRCClient.login
		BVBRCClient.login = lambda self, username, password: (token_for(self.job_id) or True)
		try:
			job_id = self.submit_pair(
				"MIDREF_LATER", auto_run="1", username="user", password="pass"
			).get_json()["job_id"]
		finally:
			BVBRCClient.login = real_login
		self.assertEqual(list(frontend._pipeline_queue), [job_id])

		jobs.drain_flag_path().unlink(missing_ok=True)
		with frontend._pipeline_lock:
			frontend._drain_pipeline_queue()

		self.assertIn(job_id, frontend._pipeline_processes)
		self.assertEqual(list(frontend._pipeline_queue), [])


# Persistence
class TestQueueSurvivesRestart(DrainBase):
	def test_queueing_writes_the_queue_to_disk(self):
		job_id = self.runnable_job("PERSIST1")
		self.start_drain()
		self.client.post("/run", data={"job_id": job_id})

		self.assertEqual(self.persisted_queue(), [job_id])

	def test_cancelling_a_queued_run_removes_it_from_disk(self):
		"""Otherwise the next boot would resurrect a run the user cancelled."""
		job_id = self.runnable_job("PERSIST2")
		self.start_drain()
		self.client.post("/run", data={"job_id": job_id})

		self.assertEqual(self.client.post("/abort", data={"job_id": job_id}).status_code, 200)

		self.assertEqual(self.persisted_queue(), [])


# Recovery
class TestStartupRecovery(DrainBase):
	def test_a_run_killed_mid_flight_is_recorded_as_failed(self):
		"""The bug this exists for: no status on disk means /status 404s and the
		browser's poller goes quiet, so the run neither finishes nor fails -- it
		just stops existing."""
		job_id = self.runnable_job("ORPHAN")
		# Exactly the state a restart leaves behind: started, no terminal status,
		# and no process of ours still alive.
		jobs.job_results_dir(job_id).mkdir(parents=True, exist_ok=True)
		jobs.job_run_started_path(job_id).write_text("1783915052.0")
		self.assertFalse(jobs.job_status_path(job_id).is_file())

		frontend._reconcile_interrupted_runs()

		status = self.client.get(f"/status?job_id={job_id}").get_json()
		self.assertTrue(status["done"])
		self.assertFalse(status["success"])
		self.assertIn("restarted", status["error"])

	def test_a_finished_run_is_left_alone(self):
		job_id = self.runnable_job("FINISHED")
		jobs.job_results_dir(job_id).mkdir(parents=True, exist_ok=True)
		jobs.job_run_started_path(job_id).write_text("1783915052.0")
		frontend._write_job_status(job_id, True)

		frontend._reconcile_interrupted_runs()

		self.assertTrue(self.client.get(f"/status?job_id={job_id}").get_json()["success"])

	def test_queued_runs_are_reloaded_and_started_after_the_restart(self):
		"""The sequence the user asked for: queued through the refresh, started
		once it lands."""
		job_id = self.runnable_job("RESUMED")
		self.start_drain()
		self.client.post("/run", data={"job_id": job_id})
		# Simulate the restart: the new process has no memory of either structure.
		frontend._pipeline_queue.clear()
		frontend._pipeline_processes.clear()
		frontend.subprocess.Popen = _sleeping_popen

		frontend._reconcile_interrupted_runs()

		self.assertIn(job_id, frontend._pipeline_processes)
		self.assertEqual(list(frontend._pipeline_queue), [])
		self.assertEqual(self.persisted_queue(), [])

	def test_boot_clears_the_drain_flag(self):
		"""The flag is on a volume, so it outlives the refresh that set it. If a
		refresh died after setting it, nothing else would ever lift it and the app
		would queue runs forever."""
		self.start_drain()

		frontend._reconcile_interrupted_runs()

		self.assertFalse(jobs.drain_flag_path().is_file())
		self.assertFalse(frontend._is_draining())

	def test_a_queued_job_is_not_mistaken_for_an_interrupted_run(self):
		"""A queued job never started, so it must not be marked failed."""
		job_id = self.runnable_job("QUEUEDNOTRUN")
		self.start_drain()
		self.client.post("/run", data={"job_id": job_id})
		frontend._pipeline_queue.clear()
		frontend.subprocess.Popen = _sleeping_popen

		frontend._reconcile_interrupted_runs()

		self.assertIsNone(frontend._read_job_status(job_id))

	def test_a_queued_job_whose_data_vanished_does_not_wedge_the_queue(self):
		job_id = self.runnable_job("GONE")
		self.start_drain()
		self.client.post("/run", data={"job_id": job_id})
		jobs.job_samples_csv(job_id).unlink()
		frontend._pipeline_queue.clear()

		frontend._reconcile_interrupted_runs()

		self.assertEqual(list(frontend._pipeline_queue), [])
		self.assertEqual(self.persisted_queue(), [])

	def test_a_corrupt_queue_file_does_not_stop_the_app_booting(self):
		"""The queue is rewritten on every change, so a restart can land mid-write.
		The write is atomic to prevent that -- but if the file is damaged anyway,
		booting still matters more than the queue."""
		jobs.pipeline_queue_path().parent.mkdir(parents=True, exist_ok=True)
		jobs.pipeline_queue_path().write_text("{ truncated")

		frontend.run_startup_recovery()

		self.assertEqual(list(frontend._pipeline_queue), [])


class UploadsAreVisibleToTheDrain(Base):
	"""deploy/refresh-databases.sh restarts as soon as the app reports nothing in
	flight, so that number decides what a planned restart is allowed to destroy.

	An upload does its whole job inside the request that carries it -- staging,
	pairing, checksum verification and the S3 push all happen before the response.
	If it is not counted, the drain sees an idle app and restarts straight through
	someone's upload: the connection drops and the job keeps only the samples that
	happened to be registered first."""

	def setUp(self):
		super().setUp()
		# /api/health probes nine external services on every call, which takes
		# seconds of real network time. Read through it while an upload is being
		# held open and the hold expires mid-request: the upload finishes, the
		# counter drops back to 0, and the assertion below fails for a reason that
		# has nothing to do with what it is testing. Stub the probe out -- this
		# test is about the in_flight field, not about reaching PubMLST.
		real_check_all = frontend.api_registry.check_all
		frontend.api_registry.check_all = lambda *args, **kwargs: []
		self.addCleanup(setattr, frontend.api_registry, "check_all", real_check_all)

	def health(self):
		# A client of its own: the upload below is running on another thread, and
		# sharing one test client across both is asking for a flaky test.
		return frontend.app.test_client().get("/api/health").get_json()

	def test_idle_app_reports_no_uploads(self):
		self.assertEqual(self.health()["uploads"]["in_flight"], 0)

	def test_an_upload_in_progress_is_counted(self):
		entered, release = threading.Event(), threading.Event()
		real_import = frontend._import_service.import_directory

		def blocking_import(*args, **kwargs):
			"""Hold the request open inside the import, where a real upload spends
			its time, so the drain can be asked what it sees."""
			entered.set()
			release.wait(10)
			return real_import(*args, **kwargs)

		frontend._import_service.import_directory = blocking_import
		self.addCleanup(setattr, frontend._import_service, "import_directory", real_import)

		def post_upload():
			files = [
				(io.BytesIO(fastq_bytes()), "Run/INFLIGHT_R1_001.fastq.gz"),
				(io.BytesIO(fastq_bytes()), "Run/INFLIGHT_R2_001.fastq.gz"),
			]
			frontend.app.test_client().post(
				"/import", data={"files": files}, content_type="multipart/form-data"
			)

		upload = threading.Thread(target=post_upload)
		upload.start()
		try:
			self.assertTrue(entered.wait(10), "upload never reached the import step")
			self.assertEqual(
				self.health()["uploads"]["in_flight"],
				1,
				"an upload still inside its request must be visible to the drain",
			)
		finally:
			release.set()
			upload.join(20)

		self.assertEqual(
			self.health()["uploads"]["in_flight"],
			0,
			"the count must come back down once the upload finishes",
		)


if __name__ == "__main__":
	unittest.main()
