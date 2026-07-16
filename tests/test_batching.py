"""Filling one job from several uploads, timing them, and auto-running afterwards.

The three upload methods used to each mint their own job ID, so a batch could
only ever come from one upload by one method. These cover the three things that
changes: an upload can name an existing job, every upload is timed and logged,
and an upload can start the pipeline itself the moment it lands.
"""

import io
import subprocess
import sys
import threading
import time
import unittest
from unittest import mock

import frontend  # noqa: E402
from tests._isolation import REAL_ROOT  # noqa: F401  (must import first)
from tests.test_cloud_import import FakeOneDrive, fastq_bytes, md5, stats_workbook  # noqa: E402
from workflow.helpers import jobs  # noqa: E402
from workflow.helpers.bvbrc_client import BVBRCClient  # noqa: E402

_REAL_POPEN = subprocess.Popen


def _fake_popen(argv, **kwargs):
	"""A stand-in for snakemake that exits immediately."""
	return _REAL_POPEN([sys.executable, "-c", "pass"], **kwargs)


def _failing_popen(argv, **kwargs):
	"""A stand-in for snakemake that exits non-zero, i.e. a run that failed."""
	return _REAL_POPEN([sys.executable, "-c", "import sys; sys.exit(1)"], **kwargs)


def token_for(job_id):
	path = jobs.job_token_path(job_id)
	path.parent.mkdir(parents=True, exist_ok=True)
	path.write_text('{"access_token": "test-token", "user_id": "tester@bvbrc"}')


class Base(unittest.TestCase):
	@classmethod
	def setUpClass(cls):
		frontend.app.config.update(TESTING=True)
		cls.client = frontend.app.test_client()

	def setUp(self):
		frontend.limiter.enabled = False
		frontend._pipeline_processes.clear()
		frontend._pipeline_queue.clear()
		# Cloud import is disabled, so frontend has no _cloud_imports registry.
		# frontend._cloud_imports.clear()

	def submit_pair(self, name, job_id=None, **extra):
		data = {
			"fastq_file_1": (io.BytesIO(fastq_bytes()), f"{name}_R1_001.fastq.gz"),
			"fastq_file_2": (io.BytesIO(fastq_bytes(2)), f"{name}_R2_001.fastq.gz"),
			**extra,
		}
		if job_id:
			data["job_id"] = job_id
		return self.client.post("/submit", data=data, content_type="multipart/form-data")

	def import_folder(self, names, job_id=None, **extra):
		files = []
		for name in names:
			files.append((io.BytesIO(fastq_bytes()), f"Run/{name}_R1_001.fastq.gz"))
			files.append((io.BytesIO(fastq_bytes()), f"Run/{name}_R2_001.fastq.gz"))
		data = {"files": files, **extra}
		if job_id:
			data["job_id"] = job_id
		return self.client.post("/import", data=data, content_type="multipart/form-data")

	def isolates(self, job_id):
		rows, _ = frontend._read_samples(jobs.job_samples_csv(job_id))
		return sorted(row["isolate_id"] for row in rows)

	def run_to_terminal_status(self, job_id, popen=_fake_popen):
		"""Run the job with a stubbed snakemake until it records a terminal status."""
		token_for(job_id)
		frontend.subprocess.Popen = popen
		try:
			self.assertEqual(self.client.post("/run", data={"job_id": job_id}).status_code, 200)
			deadline = time.time() + 15
			while time.time() < deadline:
				if self.client.get(f"/status?job_id={job_id}").get_json().get("done"):
					return
				time.sleep(0.05)
		finally:
			frontend.subprocess.Popen = _REAL_POPEN
		self.fail("run did not reach a terminal status in time")


# Adding to a job
class TestMultipleUploadsPerJob(Base):
	def test_reserved_job_accepts_repeated_and_mixed_uploads(self):
		reservation = self.client.post("/job/new")
		self.assertEqual(reservation.status_code, 201)
		job_id = reservation.get_json()["job_id"]
		self.assertEqual(self.isolates(job_id), [])
		self.submit_pair("RESERVED_PAIR", job_id=job_id)
		self.import_folder(["RESERVED_FOLDER"], job_id=job_id)
		self.assertEqual(self.isolates(job_id), ["RESERVED_FOLDER", "RESERVED_PAIR"])

	def test_blank_job_id_still_starts_a_new_job(self):
		first = self.submit_pair("ONE").get_json()["job_id"]
		second = self.submit_pair("TWO").get_json()["job_id"]
		self.assertNotEqual(first, second, "an unnamed upload must not join an existing job")

	def test_second_pair_can_be_added_to_the_same_job(self):
		job_id = self.submit_pair("PAIRA").get_json()["job_id"]
		response = self.submit_pair("PAIRB", job_id=job_id)
		self.assertEqual(response.status_code, 200)
		self.assertEqual(response.get_json()["job_id"], job_id)
		self.assertEqual(self.isolates(job_id), ["PAIRA", "PAIRB"])

	@unittest.skip("Cloud import is disabled; this test needs the /cloud-import route.")
	def test_different_input_methods_fill_one_job(self):
		"""The whole point: a pair, a folder, and a cloud pull into one batch."""
		job_id = self.submit_pair("BYPAIR").get_json()["job_id"]

		response = self.import_folder(["BYFOLDER_S1"], job_id=job_id)
		self.assertEqual(response.status_code, 200)
		self.assertEqual(response.get_json()["job_id"], job_id)

		r1, r2 = fastq_bytes(1), fastq_bytes(2)
		onedrive = FakeOneDrive(
			{
				"BYCLOUD_S2_R1_001.fastq.gz": r1,
				"BYCLOUD_S2_R2_001.fastq.gz": r2,
				"DNA Sequencing Stats.xlsx": stats_workbook([["BYCLOUD", md5(r1), md5(r2)]]),
			}
		)
		with mock.patch.object(frontend.cloud_import, "_SESSION", onedrive):
			response = self.client.post(
				"/cloud-import",
				data={"share_url": "https://1drv.ms/f/s!AoXyZ", "job_id": job_id},
			)
			self.assertEqual(response.status_code, 202)
			self.assertEqual(response.get_json()["job_id"], job_id)
			deadline = time.time() + 30
			while time.time() < deadline:
				record = self.client.get(f"/cloud-import/status?job_id={job_id}").get_json()
				if record.get("state") != "running":
					break
				time.sleep(0.05)
		self.assertEqual(record["state"], "done", record)

		# One job, one manifest, all three methods' samples in it.
		self.assertEqual(self.isolates(job_id), ["BYCLOUD_S2", "BYFOLDER_S1", "BYPAIR"])
		snapshot = self.client.get(f"/job/{job_id}").get_json()
		self.assertEqual(len(snapshot["samples"]), 3)

	def test_re_adding_an_isolate_updates_rather_than_duplicates(self):
		job_id = self.submit_pair("SAME").get_json()["job_id"]
		response = self.submit_pair("SAME", job_id=job_id)
		body = response.get_json()
		self.assertEqual(body["updated"], ["SAME"])
		self.assertEqual(body["added"], [])
		self.assertEqual(self.isolates(job_id), ["SAME"])

	def test_unknown_and_malformed_target_jobs_are_refused(self):
		self.assertEqual(self.submit_pair("X", job_id="ABCDEFGHJKMN").status_code, 404)
		self.assertEqual(self.submit_pair("X", job_id="not-a-job").status_code, 400)
		self.assertEqual(self.import_folder(["X_S1"], job_id="ABCDEFGHJKMN").status_code, 404)

	def test_cannot_add_to_a_job_whose_pipeline_is_running(self):
		job_id = self.submit_pair("BUSY").get_json()["job_id"]
		token_for(job_id)
		frontend.subprocess.Popen = lambda argv, **kw: _REAL_POPEN(
			[sys.executable, "-c", "import time; time.sleep(30)"], **kw
		)
		try:
			self.assertEqual(self.client.post("/run", data={"job_id": job_id}).status_code, 200)
			response = self.submit_pair("LATE", job_id=job_id)
			self.assertEqual(response.status_code, 409)
			self.assertIn("running", response.get_json()["error"].lower())
		finally:
			for process in list(frontend._pipeline_processes.values()):
				process.kill()
			frontend.subprocess.Popen = _REAL_POPEN

	def test_cannot_add_to_a_job_after_it_has_run(self):
		job_id = self.submit_pair("DONEADD").get_json()["job_id"]
		self.run_to_terminal_status(job_id)
		response = self.submit_pair("TOOLATE", job_id=job_id)
		self.assertEqual(response.status_code, 409)
		self.assertIn("already run", response.get_json()["error"].lower())
		self.assertEqual(self.isolates(job_id), ["DONEADD"])

	def test_a_failed_run_freezes_the_samples_too(self):
		job_id = self.submit_pair("FAILADD").get_json()["job_id"]
		self.run_to_terminal_status(job_id, popen=_failing_popen)
		self.assertEqual(self.submit_pair("TOOLATE", job_id=job_id).status_code, 409)
		self.assertEqual(self.import_folder(["ALSOLATE"], job_id=job_id).status_code, 409)

	def test_cannot_delete_from_a_job_after_it_has_run(self):
		job_id = self.submit_pair("DONEDEL").get_json()["job_id"]
		self.run_to_terminal_status(job_id)
		response = self.client.delete(
			"/delete", json={"job_id": job_id, "files": ["DONEDEL_R1_001.fastq.gz"]}
		)
		self.assertEqual(response.status_code, 409)
		# Refused, not half-applied: the manifest row is still there.
		self.assertEqual(self.isolates(job_id), ["DONEDEL"])

	def test_cannot_delete_from_a_job_while_it_is_running(self):
		job_id = self.submit_pair("DELBUSY").get_json()["job_id"]
		token_for(job_id)
		frontend.subprocess.Popen = lambda argv, **kw: _REAL_POPEN(
			[sys.executable, "-c", "import time; time.sleep(30)"], **kw
		)
		try:
			self.assertEqual(self.client.post("/run", data={"job_id": job_id}).status_code, 200)
			response = self.client.delete(
				"/delete", json={"job_id": job_id, "files": ["DELBUSY_R1_001.fastq.gz"]}
			)
			self.assertEqual(response.status_code, 409)
			self.assertIn("running", response.get_json()["error"].lower())
		finally:
			for process in list(frontend._pipeline_processes.values()):
				process.kill()
			frontend.subprocess.Popen = _REAL_POPEN

	def test_concurrent_adds_to_one_job_do_not_lose_samples(self):
		"""Two methods uploading at once are both read-modify-writes of the same
		samples.csv; without the per-job lock one of them silently disappears."""
		job_id = self.submit_pair("SEED").get_json()["job_id"]
		errors = []

		def add(name):
			try:
				response = self.submit_pair(name, job_id=job_id)
				if response.status_code != 200:
					errors.append((name, response.status_code))
			except Exception as exception:  # pragma: no cover
				errors.append((name, exception))

		threads = [threading.Thread(target=add, args=(f"CONC{n}",)) for n in range(8)]
		for thread in threads:
			thread.start()
		for thread in threads:
			thread.join(timeout=30)

		self.assertEqual(errors, [])
		self.assertEqual(self.isolates(job_id), sorted(["SEED"] + [f"CONC{n}" for n in range(8)]))


# Upload and run time
class TestTiming(Base):
	def test_each_upload_is_timed_and_labelled(self):
		job_id = self.submit_pair("TIMEA").get_json()["job_id"]
		self.import_folder(["TIMEB_S1"], job_id=job_id)

		uploads = self.client.get(f"/job/{job_id}").get_json()["uploads"]
		self.assertEqual([u["method"] for u in uploads], ["pair", "folder"])
		self.assertEqual([u["label"] for u in uploads], ["paired upload", "folder import"])
		self.assertEqual(uploads[0]["added"], ["TIMEA"])
		self.assertEqual(uploads[1]["added"], ["TIMEB_S1"])
		for upload in uploads:
			self.assertIsInstance(upload["seconds"], float)
			self.assertGreaterEqual(upload["seconds"], 0.0)
			self.assertGreater(upload["finished_at"], 0)

	def test_submit_response_carries_its_own_timing(self):
		body = self.submit_pair("TIMEC").get_json()
		self.assertIn("upload", body)
		self.assertEqual(body["upload"]["method"], "pair")
		self.assertGreaterEqual(body["upload"]["seconds"], 0.0)

	def test_run_status_reports_start_and_finish_so_duration_can_be_shown(self):
		job_id = self.submit_pair("DURATION").get_json()["job_id"]
		token_for(job_id)
		frontend.subprocess.Popen = _fake_popen
		try:
			self.client.post("/run", data={"job_id": job_id})
			deadline = time.time() + 15
			while time.time() < deadline:
				run_status = self.client.get(f"/status?job_id={job_id}").get_json()
				if run_status.get("done"):
					break
				time.sleep(0.05)
		finally:
			frontend.subprocess.Popen = _REAL_POPEN

		self.assertTrue(run_status["done"])
		self.assertIsNotNone(run_status["started_at"])
		self.assertIsNotNone(run_status["finished_at"])
		self.assertGreaterEqual(run_status["finished_at"], run_status["started_at"])


# Auto-run
class TestAutoRun(Base):
	def setUp(self):
		super().setUp()
		frontend.subprocess.Popen = _fake_popen

	def tearDown(self):
		for process in list(frontend._pipeline_processes.values()):
			try:
				process.kill()
			except Exception:
				pass
		frontend.subprocess.Popen = _REAL_POPEN

	def test_upload_without_auto_run_starts_nothing(self):
		body = self.submit_pair("NOAUTO").get_json()
		self.assertIsNone(body["auto_run"])
		self.assertEqual(frontend._pipeline_processes, {})

	def test_auto_run_starts_the_pipeline_when_credentials_work(self):
		real_login = BVBRCClient.login
		BVBRCClient.login = lambda self, username, password: (token_for(self.job_id) or True)
		try:
			body = self.submit_pair(
				"AUTOGO", auto_run="1", username="user", password="pass"
			).get_json()
		finally:
			BVBRCClient.login = real_login

		self.assertEqual(
			body["auto_run"], {"started": True, "queued": False, "queue_position": None}
		)
		run_status = self.client.get(f"/status?job_id={body['job_id']}").get_json()
		self.assertIn("done", run_status)

	def test_auto_run_without_a_bvbrc_login_reports_why_but_keeps_the_upload(self):
		"""The files are registered either way -- a run that cannot start must not
		read as an upload that failed."""
		response = self.submit_pair("AUTONOAUTH", auto_run="1")
		self.assertEqual(response.status_code, 200)
		body = response.get_json()

		self.assertFalse(body["auto_run"]["started"])
		self.assertIn("BV-BRC login required", body["auto_run"]["error"])
		self.assertEqual(self.isolates(body["job_id"]), ["AUTONOAUTH"])
		self.assertEqual(frontend._pipeline_processes, {})

	def test_auto_run_reports_bad_credentials_without_failing_the_upload(self):
		real_login = BVBRCClient.login
		BVBRCClient.login = lambda self, username, password: False
		try:
			response = self.submit_pair("AUTOBAD", auto_run="1", username="user", password="wrong")
		finally:
			BVBRCClient.login = real_login

		self.assertEqual(response.status_code, 200)
		body = response.get_json()
		self.assertFalse(body["auto_run"]["started"])
		self.assertIn("authentication failed", body["auto_run"]["error"].lower())
		self.assertEqual(self.isolates(body["job_id"]), ["AUTOBAD"])

	def test_folder_import_can_auto_run_too(self):
		real_login = BVBRCClient.login
		BVBRCClient.login = lambda self, username, password: (token_for(self.job_id) or True)
		try:
			body = self.import_folder(
				["AUTOF_S1"], auto_run="1", username="user", password="pass"
			).get_json()
		finally:
			BVBRCClient.login = real_login
		self.assertTrue(body["auto_run"]["started"])

	def test_auto_run_queues_behind_a_full_pipeline_cap(self):
		saved_cap = frontend.MAX_CONCURRENT_PIPELINES
		frontend.MAX_CONCURRENT_PIPELINES = 1
		frontend.subprocess.Popen = lambda argv, **kw: _REAL_POPEN(
			[sys.executable, "-c", "import time; time.sleep(30)"], **kw
		)
		real_login = BVBRCClient.login
		BVBRCClient.login = lambda self, username, password: (token_for(self.job_id) or True)
		try:
			first = self.submit_pair("QFIRST", auto_run="1", username="u", password="p").get_json()
			self.assertTrue(first["auto_run"]["started"])
			self.assertFalse(first["auto_run"]["queued"])

			second = self.submit_pair(
				"QSECOND", auto_run="1", username="u", password="p"
			).get_json()
			self.assertTrue(second["auto_run"]["started"])
			self.assertTrue(second["auto_run"]["queued"])
			self.assertEqual(second["auto_run"]["queue_position"], 1)
		finally:
			BVBRCClient.login = real_login
			frontend.MAX_CONCURRENT_PIPELINES = saved_cap


if __name__ == "__main__":
	unittest.main(verbosity=2)
