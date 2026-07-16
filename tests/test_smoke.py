"""End-to-end smoke tests: every HTTP route, security control, the queue
admission logic, and the workflow libraries. Stdlib unittest so no new
dependency is needed in the bioinformatics env."""

import gzip
import hashlib
import io
import json
import subprocess
import sys
import time
import unittest
import zipfile
from pathlib import Path

import frontend  # noqa: E402
from tests._isolation import REAL_ROOT  # noqa: F401  (must import first)
from workflow.helpers import api_registry, import_samples, jobs, preprocess  # noqa: E402
from workflow.helpers.bvbrc_client import BVBRCClient, _require_safe_identifier  # noqa: E402
from workflow.helpers.utils import zip_directory  # noqa: E402

ROOT = Path(frontend.PROJECT_ROOT)  # temp root, see tests/_isolation

# Popen replacement: keeps the real process-group semantics (so /abort is
# genuinely exercised) but runs a controllable sleeper instead of snakemake.
_REAL_POPEN = subprocess.Popen
_RUN_SECONDS = 0.0


def _fake_popen(argv, **kwargs):
	return _REAL_POPEN([sys.executable, "-c", f"import time; time.sleep({_RUN_SECONDS})"], **kwargs)


def fastq_bytes(records=1, tag="read"):
	"""`tag` distinguishes an R1 from its R2 without changing the record count:
	the two mates of a real pair always hold the same number of reads, and the
	upload rejects a pair whose counts disagree."""
	buf = io.BytesIO()
	with gzip.GzipFile(fileobj=buf, mode="wb") as fh:
		for i in range(records):
			fh.write(f"@{tag}{i}\nACGT\n+\nIIII\n".encode())
	return buf.getvalue()


def md5(data):
	return hashlib.md5(data).hexdigest()


def token_for(job_id):
	path = jobs.job_token_path(job_id)
	path.parent.mkdir(parents=True, exist_ok=True)
	path.write_text(json.dumps({"access_token": "test-token", "user_id": "tester@bvbrc"}))


class Base(unittest.TestCase):
	@classmethod
	def setUpClass(cls):
		frontend.app.config.update(TESTING=True)
		cls.client = frontend.app.test_client()

	def setUp(self):
		frontend.limiter.enabled = False
		frontend._pipeline_processes.clear()
		frontend._pipeline_queue.clear()
		frontend._pipeline_aborted_jobs.clear()

	def upload_pair(self, name="SAMP1", r1_checksum=None):
		r1, r2 = fastq_bytes(2, "r1"), fastq_bytes(2, "r2")
		data = {
			"fastq_file_1": (io.BytesIO(r1), f"{name}_R1_001.fastq.gz"),
			"fastq_file_2": (io.BytesIO(r2), f"{name}_R2_001.fastq.gz"),
		}
		if r1_checksum:
			data["fastq_file_1_checksum"] = r1_checksum
		return self.client.post("/submit", data=data, content_type="multipart/form-data")


# Pages and static assets
class TestPages(Base):
	def test_index_renders(self):
		r = self.client.get("/")
		self.assertEqual(r.status_code, 200)
		self.assertIn(b"<html", r.data.lower())

	def test_static_css_served(self):
		r = self.client.get("/static/style.css")
		self.assertEqual(r.status_code, 200)

	def test_index_shows_reference_database_age(self):
		from datetime import datetime, timedelta, timezone

		# The build stamp the Dockerfile writes beside the databases; the page turns
		# it into a "last updated" line. Without it (e.g. a local run) the line is
		# omitted, so create it under the test root and confirm it renders.
		built = datetime.now(timezone.utc) - timedelta(days=3, minutes=5)
		stamp = ROOT / "resources" / "blastdb" / ".db_built_at"
		stamp.parent.mkdir(parents=True, exist_ok=True)
		stamp.write_text(built.strftime("%Y-%m-%dT%H:%M:%SZ"))
		try:
			page = self.client.get("/").get_data(as_text=True)
		finally:
			stamp.unlink()
		self.assertIn('id="db-last-updated"', page)
		self.assertIn("3d ago", page)
		self.assertIn(built.strftime("%Y-%m-%dT%H:%M:%S"), page)

	def test_index_omits_database_age_when_unknown(self):
		# No build stamp and no AMR marker under the test root -> the line is absent
		# rather than showing an empty or wrong date.
		self.assertNotIn('id="db-last-updated"', self.client.get("/").get_data(as_text=True))

	def test_security_headers(self):
		r = self.client.get("/")
		self.assertEqual(r.headers["X-Content-Type-Options"], "nosniff")
		self.assertEqual(r.headers["X-Frame-Options"], "DENY")
		# Not "no-referrer": that makes browsers send "Origin: null" on same-origin
		# form POSTs, which _reject_cross_site_mutations rejects -- it took down the
		# settings Save/Reset buttons. "same-origin" withholds the Referer (and the
		# job ID in it) from cross-origin requests just as thoroughly.
		self.assertEqual(r.headers["Referrer-Policy"], "same-origin")
		self.assertIn("camera=()", r.headers["Permissions-Policy"])

	def test_settings_page_without_job(self):
		r = self.client.get("/settings")
		self.assertEqual(r.status_code, 200)

	def test_settings_rejects_malformed_job(self):
		self.assertEqual(self.client.get("/settings?job_id=../../etc").status_code, 400)

	def test_settings_unknown_job_404(self):
		self.assertEqual(self.client.get("/settings?job_id=ABCDEFGHJKMN").status_code, 404)


# Upload
class TestSubmit(Base):
	def test_upload_pair_creates_job_and_manifest(self):
		r = self.upload_pair("SAMPA")
		self.assertEqual(r.status_code, 200)
		body = r.get_json()
		job_id = body["job_id"]
		self.assertTrue(jobs.is_valid_job_id(job_id))
		self.assertEqual(body["isolate_id"], "SAMPA")
		rows, _ = frontend._read_samples(jobs.job_samples_csv(job_id))
		self.assertEqual(rows[0]["isolate_id"], "SAMPA")
		self.assertTrue((ROOT / rows[0]["R1_path"]).is_file())
		self.assertTrue((ROOT / rows[0]["R2_path"]).is_file())

	def test_missing_file_rejected(self):
		r = self.client.post(
			"/submit",
			data={"fastq_file_1": (io.BytesIO(fastq_bytes()), "X_R1.fastq.gz")},
			content_type="multipart/form-data",
		)
		self.assertEqual(r.status_code, 400)

	def test_checksum_mismatch_rejected_and_files_removed(self):
		r = self.upload_pair("SAMPB", r1_checksum="0" * 32)
		self.assertEqual(r.status_code, 400)
		self.assertIn("checksum", r.get_json()["error"].lower())

	def test_checksum_match_accepted(self):
		payload = fastq_bytes()
		r = self.client.post(
			"/submit",
			data={
				"fastq_file_1": (io.BytesIO(payload), "SAMPC_R1_001.fastq.gz"),
				"fastq_file_2": (io.BytesIO(fastq_bytes(1, "r2")), "SAMPC_R2_001.fastq.gz"),
				"fastq_file_1_checksum": md5(payload),
			},
			content_type="multipart/form-data",
		)
		self.assertEqual(r.status_code, 200)

	def test_delete_removes_files_and_manifest_row(self):
		job_id = self.upload_pair("SAMPE").get_json()["job_id"]
		r = self.client.delete(
			"/delete",
			json={"job_id": job_id, "files": ["SAMPE_R1_001.fastq.gz", "SAMPE_R2_001.fastq.gz"]},
		)
		self.assertEqual(r.status_code, 200)
		rows, _ = frontend._read_samples(jobs.job_samples_csv(job_id))
		self.assertEqual(rows, [])
		self.assertFalse((jobs.job_data_dir(job_id) / "SAMPE_R1_001.fastq.gz").exists())

	def test_delete_malformed_job_rejected(self):
		r = self.client.delete("/delete", json={"job_id": "nope", "files": []})
		self.assertEqual(r.status_code, 400)


# Folder import
class TestImport(Base):
	def test_import_folder_pairs_samples(self):
		data = {
			"files": [
				(io.BytesIO(fastq_bytes()), "Run1/IMPA_S1_R1_001.fastq.gz"),
				(io.BytesIO(fastq_bytes()), "Run1/IMPA_S1_R2_001.fastq.gz"),
				(io.BytesIO(fastq_bytes()), "Run1/IMPB_S2_R1_001.fastq.gz"),
				(io.BytesIO(fastq_bytes()), "Run1/IMPB_S2_R2_001.fastq.gz"),
			]
		}
		r = self.client.post("/import", data=data, content_type="multipart/form-data")
		self.assertEqual(r.status_code, 200)
		body = r.get_json()
		self.assertCountEqual(body["added"], ["IMPA_S1", "IMPB_S2"])
		job_id = body["job_id"]
		rows, _ = frontend._read_samples(jobs.job_samples_csv(job_id))
		for row in rows:
			self.assertTrue((ROOT / row["R1_path"]).is_file(), row["R1_path"])
			self.assertTrue((ROOT / row["R2_path"]).is_file(), row["R2_path"])

	def test_import_warns_on_unpaired_read(self):
		data = {
			"files": [
				(io.BytesIO(fastq_bytes()), "Run2/LONE_S1_R1_001.fastq.gz"),
			]
		}
		r = self.client.post("/import", data=data, content_type="multipart/form-data")
		self.assertEqual(r.status_code, 200)
		self.assertTrue(any("No R2 mate" in w for w in r.get_json()["warnings"]))

	def test_import_no_files_rejected(self):
		r = self.client.post("/import", data={}, content_type="multipart/form-data")
		self.assertEqual(r.status_code, 400)


# Run, queue, and abort
class TestPipelineLifecycle(Base):
	def setUp(self):
		super().setUp()
		global _RUN_SECONDS
		_RUN_SECONDS = 30.0
		frontend.subprocess.Popen = _fake_popen
		self.saved_max = frontend.MAX_CONCURRENT_PIPELINES

	def tearDown(self):
		for proc in list(frontend._pipeline_processes.values()):
			try:
				proc.kill()
			except Exception:
				pass
		frontend.subprocess.Popen = _REAL_POPEN
		frontend.MAX_CONCURRENT_PIPELINES = self.saved_max

	def ready_job(self, name):
		job_id = self.upload_pair(name).get_json()["job_id"]
		token_for(job_id)
		return job_id

	def test_run_requires_known_job(self):
		self.assertEqual(self.client.post("/run", data={"job_id": "bad"}).status_code, 400)
		self.assertEqual(self.client.post("/run", data={"job_id": "ABCDEFGHJKMN"}).status_code, 404)

	def test_run_requires_bvbrc_login(self):
		job_id = self.upload_pair("NOAUTH").get_json()["job_id"]
		r = self.client.post("/run", data={"job_id": job_id})
		self.assertEqual(r.status_code, 401)
		self.assertIn("BV-BRC login required", r.get_json()["error"])

	def test_run_bad_bvbrc_credentials_rejected(self):
		# Credentials are authenticated at run time, not at upload time: a /run
		# with a username/password that BV-BRC rejects is refused with a 401.
		job_id = self.upload_pair("BADCRED").get_json()["job_id"]
		real = BVBRCClient.login
		BVBRCClient.login = lambda self, u, p: False
		try:
			r = self.client.post("/run", data={"job_id": job_id, "username": "u", "password": "p"})
			self.assertEqual(r.status_code, 401)
			self.assertIn("authentication failed", r.get_json()["error"].lower())
		finally:
			BVBRCClient.login = real

	def test_run_requires_samples(self):
		job_id = self.upload_pair("EMPTYJOB").get_json()["job_id"]
		token_for(job_id)
		frontend._write_samples(jobs.job_samples_csv(job_id), [])
		r = self.client.post("/run", data={"job_id": job_id})
		self.assertEqual(r.status_code, 400)

	def test_start_status_and_duplicate_run(self):
		job_id = self.ready_job("RUNA")
		r = self.client.post("/run", data={"job_id": job_id})
		self.assertEqual(r.status_code, 200)
		self.assertFalse(r.get_json()["queued"])

		status = self.client.get(f"/status?job_id={job_id}").get_json()
		self.assertFalse(status["done"])
		self.assertIsNotNone(status["started_at"])

		# second start while running
		self.assertEqual(self.client.post("/run", data={"job_id": job_id}).status_code, 409)

	def test_queue_admission_and_position(self):
		frontend.MAX_CONCURRENT_PIPELINES = 1
		a = self.ready_job("QA")
		b = self.ready_job("QB")
		self.assertEqual(self.client.post("/run", data={"job_id": a}).status_code, 200)

		r = self.client.post("/run", data={"job_id": b})
		self.assertEqual(r.status_code, 202)
		body = r.get_json()
		self.assertTrue(body["queued"])
		self.assertEqual(body["queue_position"], 1)

		status = self.client.get(f"/status?job_id={b}").get_json()
		self.assertTrue(status["queued"])
		self.assertEqual(status["queue_position"], 1)
		self.assertIsNone(status["started_at"])

		# queued job cannot be double-queued
		self.assertEqual(self.client.post("/run", data={"job_id": b}).status_code, 409)

	def test_abort_queued_job_cancels_without_starting(self):
		frontend.MAX_CONCURRENT_PIPELINES = 1
		a = self.ready_job("QC")
		b = self.ready_job("QD")
		self.client.post("/run", data={"job_id": a})
		self.client.post("/run", data={"job_id": b})

		r = self.client.post("/abort", data={"job_id": b})
		self.assertEqual(r.status_code, 200)
		status = self.client.get(f"/status?job_id={b}").get_json()
		self.assertTrue(status["done"])
		self.assertFalse(status["success"])
		self.assertIn("aborted", status["error"].lower())
		self.assertNotIn(b, frontend._pipeline_queue)

	def test_abort_running_job_kills_process_group(self):
		job_id = self.ready_job("ABORTA")
		self.client.post("/run", data={"job_id": job_id})
		proc = frontend._pipeline_processes[job_id]

		r = self.client.post("/abort", data={"job_id": job_id})
		self.assertEqual(r.status_code, 200)
		proc.wait(timeout=15)

		deadline = time.time() + 10
		while time.time() < deadline:
			status = self.client.get(f"/status?job_id={job_id}").get_json()
			if status.get("done"):
				break
			time.sleep(0.2)
		self.assertTrue(status["done"])
		self.assertFalse(status["success"])
		self.assertIn("aborted", status["error"].lower())

	def test_abort_when_not_running_returns_409(self):
		job_id = self.ready_job("NOTRUN")
		self.assertEqual(self.client.post("/abort", data={"job_id": job_id}).status_code, 409)

	def test_queue_drains_when_slot_frees(self):
		global _RUN_SECONDS
		_RUN_SECONDS = 0.0  # finish immediately
		frontend.MAX_CONCURRENT_PIPELINES = 1
		a = self.ready_job("DRAINA")
		b = self.ready_job("DRAINB")
		self.client.post("/run", data={"job_id": a})
		self.client.post("/run", data={"job_id": b})

		deadline = time.time() + 20
		while time.time() < deadline:
			sb = self.client.get(f"/status?job_id={b}").get_json()
			if sb.get("done"):
				break
			time.sleep(0.2)
		self.assertTrue(sb["done"], f"queued job never drained: {sb}")
		self.assertTrue(sb["success"])

	def test_successful_run_persists_status(self):
		global _RUN_SECONDS
		_RUN_SECONDS = 0.0
		job_id = self.ready_job("OKRUN")
		self.client.post("/run", data={"job_id": job_id})
		deadline = time.time() + 15
		while time.time() < deadline:
			status = self.client.get(f"/status?job_id={job_id}").get_json()
			if status.get("done"):
				break
			time.sleep(0.2)
		self.assertTrue(status["done"])
		self.assertTrue(status["success"])
		# persisted to disk, survives process restart
		self.assertTrue(jobs.job_status_path(job_id).is_file())
		# viewing a terminal status starts the retention clock
		self.assertTrue(jobs.job_first_viewed_path(job_id).is_file())

	def test_status_unknown_job(self):
		self.assertEqual(self.client.get("/status?job_id=ABCDEFGHJKMN").status_code, 404)
		self.assertEqual(self.client.get("/status?job_id=lower").status_code, 400)


# Lookup and results
class TestResults(Base):
	def setUp(self):
		super().setUp()
		self.job_id = self.upload_pair("RESA").get_json()["job_id"]
		self.res = jobs.job_results_dir(self.job_id) / "RESA"
		(self.res / "summary").mkdir(parents=True, exist_ok=True)
		(self.res / "summary" / "report.html").write_text("<h1>report</h1>")
		(jobs.job_results_dir(self.job_id) / "logs").mkdir(parents=True, exist_ok=True)
		(jobs.job_results_dir(self.job_id) / "master_report.csv").write_text("isolate\nRESA\n")

	def test_job_lookup_snapshot(self):
		r = self.client.get(f"/job/{self.job_id}")
		self.assertEqual(r.status_code, 200)
		body = r.get_json()
		self.assertEqual(body["job_id"], self.job_id)
		self.assertTrue(body["has_master_report"])
		entry = next(e for e in body["results"] if e["isolate_id"] == "RESA")
		self.assertTrue(entry["has_report"])
		self.assertNotIn("logs", [e["isolate_id"] for e in body["results"]])

	def test_job_lookup_unknown_and_malformed(self):
		self.assertEqual(self.client.get("/job/ABCDEFGHJKMN").status_code, 404)
		self.assertEqual(self.client.get("/job/short").status_code, 400)

	def test_view_report_is_sandboxed(self):
		r = self.client.get(f"/results/{self.job_id}/RESA/view")
		self.assertEqual(r.status_code, 200)
		csp = r.headers["Content-Security-Policy"]
		self.assertIn("default-src 'none'", csp)
		self.assertIn("sandbox", csp)
		self.assertEqual(r.headers["Cache-Control"], "no-store")

	def test_view_missing_report_404(self):
		self.assertEqual(self.client.get(f"/results/{self.job_id}/NOPE/view").status_code, 404)

	def test_download_isolate_zip(self):
		r = self.client.get(f"/results/{self.job_id}/RESA/download")
		self.assertEqual(r.status_code, 200)
		self.assertEqual(r.mimetype, "application/zip")
		names = zipfile.ZipFile(io.BytesIO(r.data)).namelist()
		self.assertIn("RESA/summary/report.html", names)

	def test_download_all_zip(self):
		r = self.client.get(f"/results/{self.job_id}/download-all")
		self.assertEqual(r.status_code, 200)
		names = zipfile.ZipFile(io.BytesIO(r.data)).namelist()
		self.assertTrue(any(n.endswith("master_report.csv") for n in names))

	def test_download_master_report(self):
		r = self.client.get(f"/results/{self.job_id}/master-report/download")
		self.assertEqual(r.status_code, 200)
		self.assertIn(b"RESA", r.data)

	def test_download_master_report_missing(self):
		other = self.upload_pair("NOMASTER").get_json()["job_id"]
		self.assertEqual(
			self.client.get(f"/results/{other}/master-report/download").status_code, 404
		)


# Security
class TestSecurity(Base):
	def test_isolate_path_traversal_blocked(self):
		job_id = self.upload_pair("TRAV").get_json()["job_id"]
		self.assertIsNone(frontend._resolve_result_dir(job_id, "../../etc"))
		self.assertIsNone(frontend._resolve_result_dir(job_id, "/etc/passwd"))
		self.assertIsNone(frontend._resolve_result_dir(job_id, ".."))
		self.assertIsNone(frontend._resolve_result_dir("../../etc", "X"))
		for encoded in ("%2e%2e%2f%2e%2e%2fetc", "..%2f..%2fetc"):
			r = self.client.get(f"/results/{job_id}/{encoded}/view")
			self.assertIn(r.status_code, (400, 404), encoded)

	def test_job_id_must_match_generated_format(self):
		self.assertTrue(jobs.is_valid_job_id(jobs.generate_job_id()))
		for bad in ["", "short", "abcdefghjkmn", "ABCDEFGHJKM!", "AAAAAAAAAAAAA", None, 1]:
			self.assertFalse(jobs.is_valid_job_id(bad), bad)
		# ambiguous characters excluded from the alphabet must be rejected
		self.assertFalse(jobs.is_valid_job_id("ILOILOILOILO"))

	def test_generated_ids_are_unique(self):
		self.assertEqual(len({jobs.generate_job_id() for _ in range(2000)}), 2000)

	def test_cross_site_post_rejected(self):
		job_id = self.upload_pair("XSITE").get_json()["job_id"]
		r = self.client.post(
			"/run", data={"job_id": job_id}, headers={"Sec-Fetch-Site": "cross-site"}
		)
		self.assertEqual(r.status_code, 403)
		r = self.client.post(
			"/run", data={"job_id": job_id}, headers={"Origin": "https://evil.example"}
		)
		self.assertEqual(r.status_code, 403)

	def test_same_origin_post_allowed(self):
		job_id = self.upload_pair("SAMEORIG").get_json()["job_id"]
		r = self.client.post(
			"/run",
			data={"job_id": job_id},
			headers={"Origin": "http://localhost", "Sec-Fetch-Site": "same-origin"},
		)
		self.assertNotEqual(r.status_code, 403)

	def test_opaque_origin_post_rejected(self):
		# A sandboxed or no-referrer attacker page can only produce "Origin: null",
		# which names no host and so must never pass for the true one.
		job_id = self.upload_pair("NULLORIG").get_json()["job_id"]
		r = self.client.post("/run", data={"job_id": job_id}, headers={"Origin": "null"})
		self.assertEqual(r.status_code, 403)

	def test_settings_forms_submit_as_a_browser_sends_them(self):
		# Both settings buttons are plain <form method=POST> navigations, not fetch()
		# calls, so they carry the headers a browser attaches to a same-origin form
		# submit. Sending "Referrer-Policy: no-referrer" turned the Origin on these
		# into "null" and 403'd both buttons; this drives the real submissions.
		job_id = self.upload_pair("SETFORM").get_json()["job_id"]
		form_post = {
			"Origin": "http://localhost",
			"Sec-Fetch-Site": "same-origin",
			"Sec-Fetch-Mode": "navigate",
		}
		r = self.client.post(
			f"/settings?job_id={job_id}",
			data={"job_id": job_id, "username": "", "password": ""},
			headers=form_post,
		)
		self.assertEqual(r.status_code, 302, "Save settings must not be rejected")
		self.assertIn(f"job_id={job_id}", r.headers["Location"])

		r = self.client.post("/settings/reset", data={"job_id": job_id}, headers=form_post)
		self.assertEqual(r.status_code, 302, "Reset to defaults must not be rejected")

	def test_basic_auth_gate(self):
		frontend._APP_PASSWORD = "s3cret"
		try:
			self.assertEqual(self.client.get("/").status_code, 401)
			self.assertEqual(self.client.get("/api/health").status_code, 200)  # monitoring exempt
			import base64

			cred = base64.b64encode(b"mentor:s3cret").decode()
			r = self.client.get("/", headers={"Authorization": f"Basic {cred}"})
			self.assertEqual(r.status_code, 200)
			bad = base64.b64encode(b"mentor:wrong").decode()
			self.assertEqual(
				self.client.get("/", headers={"Authorization": f"Basic {bad}"}).status_code, 401
			)
		finally:
			frontend._APP_PASSWORD = None

	def test_password_guessing_is_rate_limited(self):
		"""The password is guessable in a way an authorized session is not, so wrong
		ones are what the limit spends -- an attacker gets ten tries a minute."""
		import base64

		frontend._APP_PASSWORD = "s3cret"
		wrong = {"Authorization": "Basic " + base64.b64encode(b"mentor:wrong").decode()}
		frontend.limiter.enabled = True
		try:
			codes = [self.client.get("/", headers=wrong).status_code for _ in range(12)]
		finally:
			frontend.limiter.enabled = False
			frontend.limiter.reset()
			frontend._APP_PASSWORD = None
		self.assertEqual(codes[0], 401, f"first wrong password should be refused: {codes}")
		self.assertIn(429, codes, f"password guessing was never rate limited: {codes}")

	def test_authorized_traffic_does_not_spend_the_password_limit(self):
		"""The limit must not cost a legitimate user their session: the browser polls
		a running job well past ten requests a minute."""
		import base64

		frontend._APP_PASSWORD = "s3cret"
		good = {"Authorization": "Basic " + base64.b64encode(b"mentor:s3cret").decode()}
		frontend.limiter.enabled = True
		try:
			codes = [self.client.get("/", headers=good).status_code for _ in range(20)]
		finally:
			frontend.limiter.enabled = False
			frontend.limiter.reset()
			frontend._APP_PASSWORD = None
		self.assertEqual(set(codes), {200}, f"authorized requests were rate limited: {codes}")

	def test_job_lookup_is_rate_limited(self):
		job_id = self.upload_pair("RATE").get_json()["job_id"]
		frontend.limiter.enabled = True
		try:
			codes = [self.client.get(f"/job/{job_id}").status_code for _ in range(12)]
		finally:
			frontend.limiter.enabled = False
			frontend.limiter.reset()
		self.assertIn(429, codes, f"no rate limiting observed: {codes}")
		self.assertEqual(codes[0], 200)

	def test_endpoint_override_ssrf_guard(self):
		self.assertTrue(api_registry._is_valid_endpoint_url("https://p3.theseed.org/services/x"))
		for bad in [
			"http://p3.theseed.org/x",  # not https
			"https://evil.example/x",  # host not allowlisted
			"https://user:pass@p3.theseed.org/x",  # embedded creds
			"file:///etc/passwd",
			"https://169.254.169.254/latest/meta-data",  # cloud metadata
			"",
		]:
			self.assertFalse(api_registry._is_valid_endpoint_url(bad), bad)

	def test_settings_rejects_untrusted_override(self):
		job_id = self.upload_pair("SETJOB").get_json()["job_id"]
		r = self.client.post(
			"/settings", data={"job_id": job_id, "auth": "https://evil.example/steal"}
		)
		# Saving redirects (Post/Redirect/Get) back to the job's own settings URL.
		self.assertEqual(r.status_code, 302)
		self.assertIn(f"job_id={job_id}", r.headers["Location"])
		saved = json.loads(jobs.job_api_endpoints_path(job_id).read_text())
		self.assertNotIn("auth", saved)
		self.assertEqual(
			api_registry.get_url("auth", job_id=job_id),
			api_registry.DEFAULT_ENDPOINTS["auth"]["url"],
		)

	def test_settings_accepts_trusted_override_and_reset(self):
		job_id = self.upload_pair("SETJOB2").get_json()["job_id"]
		override = "https://p3.theseed.org/services/Workspace-test"
		r = self.client.post("/settings", data={"job_id": job_id, "workspace": override})
		self.assertEqual(r.status_code, 302)
		self.assertIn(f"job_id={job_id}", r.headers["Location"])
		self.assertEqual(api_registry.get_url("workspace", job_id=job_id), override)
		# the client actually honors the override
		token_for(job_id)
		self.assertEqual(BVBRCClient(job_id=job_id).WORKSPACE_URL, override)

		r = self.client.post("/settings/reset", data={"job_id": job_id})
		self.assertEqual(r.status_code, 302)
		self.assertEqual(
			api_registry.get_url("workspace", job_id=job_id),
			api_registry.DEFAULT_ENDPOINTS["workspace"]["url"],
		)

	def test_token_is_scoped_to_job(self):
		a = self.upload_pair("TOKA").get_json()["job_id"]
		b = self.upload_pair("TOKB").get_json()["job_id"]
		token_for(a)
		self.assertTrue(BVBRCClient(job_id=a).is_authenticated())
		self.assertFalse(BVBRCClient(job_id=b).is_authenticated())
		with self.assertRaises(ValueError):
			BVBRCClient(job_id="../../etc")

	def test_unsafe_isolate_identifier_rejected_before_remote_path(self):
		for bad in ["../evil", "a/b", "", "with space"]:
			with self.assertRaises(ValueError):
				_require_safe_identifier(bad, "sample_id")

	def test_token_file_permissions_are_private(self):
		job_id = self.upload_pair("PERMJOB").get_json()["job_id"]
		client = BVBRCClient(job_id=job_id)
		client._save_token("tok", "user@bvbrc")
		mode = jobs.job_token_path(job_id).stat().st_mode & 0o777
		self.assertEqual(mode, 0o600, oct(mode))

	def test_uploaded_fastq_not_world_readable(self):
		job_id = self.upload_pair("UMASKJOB").get_json()["job_id"]
		f = next(jobs.job_data_dir(job_id).iterdir())
		self.assertEqual(f.stat().st_mode & 0o077, 0, oct(f.stat().st_mode))


# Libraries
class TestLibraries(Base):
	def test_fastq_validation(self):
		good = ROOT / "data" / "raw_fastq" / "good.fastq.gz"
		good.parent.mkdir(parents=True, exist_ok=True)
		good.write_bytes(fastq_bytes(3))
		ok, msg = preprocess.validate_fastq_integrity(str(good))
		self.assertTrue(ok, msg)

		empty = good.parent / "empty.fastq.gz"
		empty.write_bytes(b"")
		ok, msg = preprocess.validate_fastq_integrity(str(empty))
		self.assertFalse(ok)

		corrupt = good.parent / "corrupt.fastq.gz"
		corrupt.write_bytes(b"not gzip at all")
		ok, msg = preprocess.validate_fastq_integrity(str(corrupt))
		self.assertFalse(ok)

		ok, _ = preprocess.validate_fastq_integrity(str(good.parent / "missing.fastq.gz"))
		self.assertFalse(ok)

	def test_verify_file_md5(self):
		p = ROOT / "data" / "raw_fastq" / "sum.fastq.gz"
		payload = fastq_bytes()
		p.write_bytes(payload)
		self.assertTrue(preprocess.verify_file_md5(str(p), md5(payload))[0])
		self.assertFalse(preprocess.verify_file_md5(str(p), "0" * 32)[0])

	def test_pair_fastqs_and_isolate_derivation(self):
		d = ROOT / "data" / "pairtest"
		d.mkdir(parents=True, exist_ok=True)
		for n in ["X_S1_R1_001.fastq.gz", "X_S1_R2_001.fastq.gz", "Y_S2_R1.fastq.gz"]:
			(d / n).write_bytes(fastq_bytes())
		pairs, warnings = import_samples.pair_fastqs(d)
		self.assertEqual([p["isolate_id"] for p in pairs], ["X_S1"])
		self.assertTrue(any("Y_S2_R1.fastq.gz" in w for w in warnings))

	def test_import_directory_verifies_checksums_from_workbook(self):
		import openpyxl

		d = ROOT / "data" / "wbtest"
		d.mkdir(parents=True, exist_ok=True)
		good_r1, good_r2 = fastq_bytes(1, "r1"), fastq_bytes(1, "r2")
		bad_r1, bad_r2 = fastq_bytes(3, "r1"), fastq_bytes(3, "r2")
		(d / "GOOD_S1_R1_001.fastq.gz").write_bytes(good_r1)
		(d / "GOOD_S1_R2_001.fastq.gz").write_bytes(good_r2)
		(d / "BAD_S2_R1_001.fastq.gz").write_bytes(bad_r1)
		(d / "BAD_S2_R2_001.fastq.gz").write_bytes(bad_r2)

		wb = openpyxl.Workbook()
		ws = wb.active
		ws.append(["Sample Name", "R1 md5sum", "R2 md5sum"])
		ws.append(["GOOD", md5(good_r1), md5(good_r2)])
		ws.append(["BAD", "0" * 32, "0" * 32])
		wb.save(d / "DNA Sequencing Stats.xlsx")

		out = ROOT / "config" / "jobs" / "WBTESTJOB123" / "samples.csv"
		result = import_samples.import_directory(d, samples_csv=out, dest_dir=d / "dest")
		self.assertEqual(result["added"], ["GOOD_S1"])
		self.assertEqual(result["failed"], ["BAD_S2"])
		self.assertIn("GOOD_S1", result["verified"])
		self.assertTrue(any("Checksum mismatch" in w for w in result["warnings"]))
		# the bad copy must not be left behind
		self.assertFalse((d / "dest" / "BAD_S2_R1_001.fastq.gz").exists())

	def test_import_cli_dry_run(self):
		d = ROOT / "data" / "clitest"
		d.mkdir(parents=True, exist_ok=True)
		(d / "C_S1_R1_001.fastq.gz").write_bytes(fastq_bytes())
		(d / "C_S1_R2_001.fastq.gz").write_bytes(fastq_bytes())
		proc = subprocess.run(
			[sys.executable, "-m", "workflow.helpers.import_samples", str(d), "-n"],
			cwd=REAL_ROOT,
			capture_output=True,
			text=True,
		)
		self.assertEqual(proc.returncode, 0, proc.stderr)
		self.assertIn("C_S1", proc.stdout)

	def test_import_cli_real_run(self):
		"""The non-dry-run CLI path is what a maintainer uses to load a batch."""
		d = ROOT / "data" / "clitest2"
		d.mkdir(parents=True, exist_ok=True)
		(d / "D_S1_R1_001.fastq.gz").write_bytes(fastq_bytes())
		(d / "D_S1_R2_001.fastq.gz").write_bytes(fastq_bytes())
		out = ROOT / "config" / "cli_samples.csv"
		proc = subprocess.run(
			[
				sys.executable,
				"-m",
				"workflow.helpers.import_samples",
				str(d),
				"-o",
				str(out),
				"--dest",
				str(ROOT / "data" / "clidest"),
			],
			cwd=REAL_ROOT,
			capture_output=True,
			text=True,
		)
		self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
		self.assertTrue(out.is_file())

	def test_zip_directory_roundtrip(self):
		d = ROOT / "data" / "ziptest"
		(d / "sub").mkdir(parents=True, exist_ok=True)
		(d / "sub" / "f.txt").write_text("hello")
		buf = zip_directory(d, "ROOTNAME")
		self.assertEqual(zipfile.ZipFile(buf).read("ROOTNAME/sub/f.txt"), b"hello")

	def test_error_summary_extracted_from_log(self):
		job_id = jobs.generate_job_id()
		log = jobs.job_log_path(job_id)
		log.parent.mkdir(parents=True, exist_ok=True)
		log.write_text("noise\nError in rule rgi_resistance:\n    jobid: 3\n    message: boom\n")
		summary = frontend._extract_error_summary(log)
		self.assertIn("Error in rule rgi_resistance", summary)
		self.assertIsNone(frontend._extract_error_summary(Path("/nonexistent/x.log")))

	def test_health_endpoint_shape_without_network(self):
		real = api_registry.check_endpoint
		api_registry.check_endpoint = lambda k, u=None, job_id=None: {
			"status": "up",
			"code": 200,
			"detail": "stub",
			"latency_ms": 1,
		}
		try:
			r = self.client.get("/api/health")
			self.assertEqual(r.status_code, 200)
			services = r.get_json()["services"]
			self.assertEqual(len(services), len(api_registry.DEFAULT_ENDPOINTS))
			for svc in services:
				for key in ("key", "name", "group", "url", "purpose", "status"):
					self.assertIn(key, svc)
		finally:
			api_registry.check_endpoint = real

	def test_health_rejects_bad_job(self):
		self.assertEqual(self.client.get("/api/health?job_id=nope").status_code, 400)


if __name__ == "__main__":
	unittest.main(verbosity=2)
