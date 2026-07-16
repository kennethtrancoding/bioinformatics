"""Cloud import: pulling a batch's FASTQ files from a OneDrive or Google Drive
share link.

Two things are worth testing here. First the host allowlist: the URL comes from
the user but the *server* is what fetches it, so an unfiltered version of this
feature is an SSRF hole pointed at the instance metadata service. Second the
claim the feature actually makes -- that a pulled file is treated exactly like an
uploaded one -- which means checksum verification, filetype validation, R1/R2
pairing, and a normal job ID with normal samples in it.

No network: every HTTP call cloud_import makes goes through its module-level
_SESSION, which these tests replace with a fake Drive/OneDrive.
"""

import base64
import gzip
import hashlib
import io
import re
import time
import unittest
from unittest import mock
from urllib.parse import parse_qs, quote, unquote, urlparse

import frontend  # noqa: E402
from tests._isolation import REAL_ROOT  # noqa: F401  (must import first)
from workflow.helpers import cloud_import, jobs  # noqa: E402

SHARED_FOLDER = "https://drive.google.com/drive/folders/FOLDER1?usp=sharing"


def fastq_bytes(records=1, tag="read"):
	"""`tag` distinguishes an R1 from its R2 without changing the record count:
	the two mates of a real pair always hold the same number of reads, and the
	import rejects a pair whose counts disagree."""
	buf = io.BytesIO()
	with gzip.GzipFile(fileobj=buf, mode="wb") as fh:
		for i in range(records):
			fh.write(f"@{tag}{i}\nACGT\n+\nIIII\n".encode())
	return buf.getvalue()


def md5(data):
	return hashlib.md5(data).hexdigest()


def stats_workbook(rows):
	"""The sequencing company's 'DNA Sequencing Stats.xlsx', as bytes."""
	import openpyxl

	workbook = openpyxl.Workbook()
	sheet = workbook.active
	sheet.append(["Sample Name", "R1 md5sum", "R2 md5sum"])
	for row in rows:
		sheet.append(row)
	buf = io.BytesIO()
	workbook.save(buf)
	return buf.getvalue()


# Fake providers
class FakeResponse:
	def __init__(self, status_code=200, headers=None, json_body=None, body=b""):
		self.status_code = status_code
		self.headers = headers or {}
		self._json_body = json_body
		self._body = body

	def json(self):
		if self._json_body is None:
			raise ValueError("not JSON")
		return self._json_body

	def iter_content(self, chunk_size=1):
		for start in range(0, len(self._body), chunk_size):
			yield self._body[start : start + chunk_size]

	def close(self):
		pass


class RecordingSession:
	"""Base fake: records every URL (and header) cloud_import asks for, so a test
	can assert on what was *not* requested as well as what was."""

	def __init__(self):
		self.requested = []

	def request(self, method, url, headers=None, stream=False, timeout=None, allow_redirects=None):
		self.requested.append((url, dict(headers or {})))
		return self.respond(url)

	def urls(self):
		return [url for url, _headers in self.requested]

	def respond(self, url):
		raise NotImplementedError


class FakeDrive(RecordingSession):
	"""Enough of the Drive v3 API for cloud_import: item metadata, one folder
	listing, and alt=media downloads."""

	def __init__(self, items):
		super().__init__()
		self.items = items  # id -> {name, mimeType, parent, body, content_type}

	def respond(self, url):
		parsed = urlparse(url)
		query = parse_qs(parsed.query)

		if parsed.path == "/drive/v3/files":  # folder listing
			folder_id = re.search(r"'([^']+)' in parents", query["q"][0]).group(1)
			return FakeResponse(
				200,
				json_body={
					"files": [
						{
							"id": item_id,
							"name": item["name"],
							"mimeType": item["mimeType"],
							"size": str(len(item.get("body", b""))),
						}
						for item_id, item in self.items.items()
						if item.get("parent") == folder_id
					]
				},
			)

		item_match = re.fullmatch(r"/drive/v3/files/([^/]+)", parsed.path)
		if item_match:
			item = self.items.get(item_match.group(1))
			if item is None:
				return FakeResponse(404, json_body={"error": {"message": "File not found"}})
			if query.get("alt") == ["media"]:
				return FakeResponse(
					200,
					headers={"Content-Type": item.get("content_type", "application/octet-stream")},
					body=item["body"],
				)
			return FakeResponse(
				200,
				json_body={
					"id": item_match.group(1),
					"name": item["name"],
					"mimeType": item["mimeType"],
					"size": str(len(item.get("body", b""))),
				},
			)
		return FakeResponse(404, json_body={})


class FakeOneDrive(RecordingSession):
	"""Enough of the consumer OneDrive share API: a shared folder, its children,
	and /content downloads that 302 to a content host -- which is what the real
	service does, and what the redirect allowlist has to survive."""

	CONTENT_HOST = "https://public.bn.files.1drv.com"

	def __init__(self, files):
		super().__init__()
		self.files = files  # name -> bytes

	def respond(self, url):
		parsed = urlparse(url)
		if url.startswith(self.CONTENT_HOST):
			file_name = unquote(parsed.path).lstrip("/")
			return FakeResponse(
				200,
				headers={"Content-Type": "application/octet-stream"},
				body=self.files[file_name],
			)

		share_match = re.fullmatch(r"/v1\.0/shares/([^/]+)/root(.*)", unquote(parsed.path))
		if not share_match:
			return FakeResponse(404, json_body={})
		remainder = share_match.group(2)

		if remainder == "":  # the shared item itself
			return FakeResponse(200, json_body={"name": "Run", "folder": {"childCount": 2}})
		if remainder == "/children":
			return FakeResponse(
				200,
				json_body={
					"value": [
						{"name": name, "size": len(body), "file": {"mimeType": "application/gzip"}}
						for name, body in self.files.items()
					]
				},
			)
		content_match = re.fullmatch(r":/(.+):/content", remainder)
		if content_match:
			return FakeResponse(
				302,
				headers={"Location": f"{self.CONTENT_HOST}/{quote(content_match.group(1))}"},
			)
		return FakeResponse(404, json_body={})


# Allowlist and SSRF protection
class TestAllowlist(unittest.TestCase):
	def test_accepts_provider_and_content_hosts(self):
		for url in [
			"https://drive.google.com/drive/folders/1AbC_def-123",
			"https://drive.google.com/file/d/1AbC_def-123/view?usp=sharing",
			"https://docs.google.com/uc?export=download&id=1AbC",
			"https://drive.usercontent.google.com/download?id=1AbC",
			"https://www.googleapis.com/drive/v3/files/1AbC?alt=media",
			"https://1drv.ms/f/s!AoXyZ",
			"https://onedrive.live.com/?id=root",
			"https://api.onedrive.com/v1.0/shares/u!aGk/root",
			"https://graph.microsoft.com/v1.0/shares/u!aGk/driveItem",
			# Content hosts, only ever reached by following a provider's redirect.
			"https://public.bn.files.1drv.com/y4mABC",
			"https://contoso-my.sharepoint.com/personal/x/Documents/run.fastq.gz",
			"https://doc-0s-8c-docs.googleusercontent.com/docs/securesc/abc",
		]:
			self.assertTrue(cloud_import.is_allowed_url(url), url)

	def test_rejects_ssrf_targets_and_lookalike_hosts(self):
		for url in [
			"http://drive.google.com/file/d/1AbC/view",  # not https
			"https://169.254.169.254/latest/meta-data/iam/",  # cloud metadata
			"https://127.0.0.1:5001/run",
			"https://localhost/admin",
			"https://10.0.0.5/internal",
			"https://drive.google.com.evil.example/file/d/1",  # suffix lookalike
			"https://evil.example/drive.google.com/file/d/1",  # path lookalike
			"https://evil-sharepoint.com/x",  # dash, not a subdomain
			"https://sharepoint.com/x",  # bare apex, not a tenant
			"https://user:pass@drive.google.com/file/d/1",  # embedded credentials
			"file:///etc/passwd",
			"ftp://drive.google.com/x",
			"",
			None,
		]:
			self.assertFalse(cloud_import.is_allowed_url(url), url)

	def test_provider_detection(self):
		self.assertEqual(cloud_import.provider_for("https://drive.google.com/x"), "google")
		self.assertEqual(cloud_import.provider_for("https://1drv.ms/f/s!x"), "onedrive")
		self.assertEqual(
			cloud_import.provider_for("https://contoso.sharepoint.com/:f:/g/x"), "onedrive"
		)
		self.assertIsNone(cloud_import.provider_for("https://example.com/x"))


class TestRedirectGuard(unittest.TestCase):
	def test_redirect_off_the_allowlist_is_refused_before_it_is_followed(self):
		session = mock.Mock()
		session.request.return_value = FakeResponse(
			302, headers={"Location": "https://169.254.169.254/latest/meta-data/"}
		)
		with mock.patch.object(cloud_import, "_SESSION", session):
			with self.assertRaises(cloud_import.CloudImportError) as caught:
				cloud_import._request("GET", "https://www.googleapis.com/drive/v3/files/X")

		self.assertIn("Refused to follow", str(caught.exception))
		requested_urls = [call.args[1] for call in session.request.call_args_list]
		self.assertNotIn(
			"https://169.254.169.254/latest/meta-data/",
			requested_urls,
			"the redirect target must be rejected before it is fetched, not after",
		)

	def test_redirect_to_a_content_host_is_followed(self):
		session = FakeOneDrive({"R1.fastq.gz": b"payload"})
		with mock.patch.object(cloud_import, "_SESSION", session):
			response = cloud_import._request(
				"GET",
				cloud_import._ms_url(
					cloud_import._share_token("https://1drv.ms/f/s!x"), "R1.fastq.gz", "/content"
				),
			)
		self.assertEqual(response.status_code, 200)
		self.assertTrue(any(FakeOneDrive.CONTENT_HOST in url for url in session.urls()))

	def test_graph_token_is_not_forwarded_to_the_content_host(self):
		"""The download URL Graph redirects to is on a content host that never
		needed the bearer token; sending it along would leak the app's token."""
		session = mock.Mock()
		session.request.side_effect = [
			FakeResponse(302, headers={"Location": "https://contoso-my.sharepoint.com/download/x"}),
			FakeResponse(200, body=b"payload"),
		]
		with mock.patch.object(cloud_import, "_SESSION", session):
			with mock.patch.object(cloud_import, "MS_GRAPH_TOKEN", "super-secret-token"):
				cloud_import._request(
					"GET", "https://graph.microsoft.com/v1.0/shares/u!x/root/content"
				)

		graph_headers = session.request.call_args_list[0].kwargs["headers"]
		content_headers = session.request.call_args_list[1].kwargs["headers"]
		self.assertEqual(graph_headers.get("Authorization"), "Bearer super-secret-token")
		self.assertNotIn("Authorization", content_headers)

	def test_redirect_loop_is_bounded(self):
		session = mock.Mock()
		session.request.return_value = FakeResponse(
			302, headers={"Location": "https://drive.google.com/loop"}
		)
		with mock.patch.object(cloud_import, "_SESSION", session):
			with self.assertRaises(cloud_import.CloudImportError) as caught:
				cloud_import._request("GET", "https://drive.google.com/loop")
		self.assertIn("redirected too many times", str(caught.exception))


class TestLinkParsing(unittest.TestCase):
	def test_google_ids_from_every_link_shape(self):
		for url, expected in {
			"https://drive.google.com/file/d/1AbC_def-123/view?usp=sharing": (
				"1AbC_def-123",
				False,
			),
			"https://drive.google.com/drive/folders/1Folder_9?usp=drive_link": ("1Folder_9", True),
			"https://drive.google.com/drive/u/0/folders/1Folder_9": ("1Folder_9", True),
			"https://drive.google.com/open?id=1Open_x": ("1Open_x", False),
			"https://docs.google.com/uc?export=download&id=1Uc_x": ("1Uc_x", False),
			"https://drive.google.com/": (None, False),
		}.items():
			self.assertEqual(cloud_import._google_ids(url), expected, url)

	def test_onedrive_share_token_is_microsofts_encoding(self):
		share_url = "https://1drv.ms/f/s!AoXyZ-abc"
		expected = base64.urlsafe_b64encode(share_url.encode()).decode().rstrip("=")
		self.assertEqual(cloud_import._share_token(share_url), "u!" + expected)
		self.assertNotIn("=", cloud_import._share_token(share_url))

	def test_google_folder_without_an_api_key_says_what_is_missing(self):
		with mock.patch.object(cloud_import, "GOOGLE_API_KEY", ""):
			with self.assertRaises(cloud_import.CloudImportError) as caught:
				cloud_import._google_list(SHARED_FOLDER)
		self.assertIn("GOOGLE_DRIVE_API_KEY", str(caught.exception))

	def test_request_urls_are_redacted_of_their_secrets(self):
		"""An error message is shown to the user, and a request URL carries the
		Drive API key in its query string."""
		redacted = cloud_import._redact(
			"https://www.googleapis.com/drive/v3/files/X?alt=media&key=SECRETKEY"
		)
		self.assertNotIn("SECRETKEY", redacted)
		self.assertEqual(redacted, "https://www.googleapis.com/drive/v3/files/X")


# End-to-end behavior
# The cloud import feature is disabled: frontend.py's /cloud-import routes are
# commented out, so everything below -- which drives those routes -- is skipped.
# The allowlist, redirect-guard and link-parsing tests above exercise
# workflow/lib/cloud_import.py directly, which is untouched, so they still run.
class Base(unittest.TestCase):
	@classmethod
	def setUpClass(cls):
		frontend.app.config.update(TESTING=True)
		cls.client = frontend.app.test_client()

	def setUp(self):
		frontend.limiter.enabled = False
		frontend._cloud_imports.clear()

	def wait_for(self, job_id, timeout=30):
		deadline = time.time() + timeout
		record = {}
		while time.time() < deadline:
			record = self.client.get(f"/cloud-import/status?job_id={job_id}").get_json()
			if record.get("state") != "running":
				return record
			time.sleep(0.05)
		self.fail(f"cloud import never finished: {record}")

	def pull(self, share_url, session, api_key="TESTKEY"):
		"""POST the link, then hold the fake provider in place until the
		background import thread is done with it."""
		with (
			mock.patch.object(cloud_import, "_SESSION", session),
			mock.patch.object(cloud_import, "GOOGLE_API_KEY", api_key),
		):
			response = self.client.post("/cloud-import", data={"share_url": share_url})
			self.assertEqual(response.status_code, 202, response.get_data(as_text=True))
			job_id = response.get_json()["job_id"]
			return job_id, self.wait_for(job_id)


@unittest.skip("Cloud import is disabled; these tests drive the /cloud-import routes.")
class TestGoogleDriveImport(Base):
	def setUp(self):
		super().setUp()
		self.good_r1, self.good_r2 = fastq_bytes(1, "r1"), fastq_bytes(1, "r2")
		self.mismatch_r1, self.mismatch_r2 = fastq_bytes(3, "r1"), fastq_bytes(3, "r2")

		def fastq(name, body, content_type="application/octet-stream"):
			return {
				"name": name,
				"mimeType": "application/gzip",
				"parent": "FOLDER1",
				"body": body,
				"content_type": content_type,
			}

		self.drive = FakeDrive(
			{
				"FOLDER1": {"name": "Run", "mimeType": cloud_import._GOOGLE_FOLDER_MIME},
				"g1": fastq("GOOD_S1_R1_001.fastq.gz", self.good_r1),
				"g2": fastq("GOOD_S1_R2_001.fastq.gz", self.good_r2),
				# Drive answers an unshared file with a 200 and a sign-in page.
				"h1": fastq("HTML_S2_R1_001.fastq.gz", b"<html>Sign in</html>", "text/html"),
				"h2": fastq("HTML_S2_R2_001.fastq.gz", fastq_bytes()),
				# Served with a plausible content type, but not actually a FASTQ.
				"t1": fastq("TRUNC_S3_R1_001.fastq.gz", fastq_bytes()),
				"t2": fastq("TRUNC_S3_R2_001.fastq.gz", b"not gzip at all"),
				# Real files, but the company's workbook says they are corrupt.
				"m1": fastq("MISMATCH_S4_R1_001.fastq.gz", self.mismatch_r1),
				"m2": fastq("MISMATCH_S4_R2_001.fastq.gz", self.mismatch_r2),
				"n1": {
					"name": "notes.txt",
					"mimeType": "text/plain",
					"parent": "FOLDER1",
					"body": b"read me",
				},
				"x1": {
					"name": "DNA Sequencing Stats.xlsx",
					"mimeType": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
					"parent": "FOLDER1",
					"body": stats_workbook(
						[
							["GOOD", md5(self.good_r1), md5(self.good_r2)],
							["MISMATCH", "0" * 32, "0" * 32],
						]
					),
				},
			}
		)

	def test_pulled_folder_becomes_a_normal_job(self):
		job_id, record = self.pull(SHARED_FOLDER, self.drive)
		self.assertEqual(record["state"], "done", record)
		result = record["result"]

		# Only the pair the company's checksums vouch for makes it in.
		self.assertEqual(result["added"], ["GOOD_S1"])
		self.assertEqual(result["verified"], ["GOOD_S1"])
		self.assertEqual(result["provider"], "google")
		self.assertEqual(result["checksum_source"], "DNA_Sequencing_Stats.xlsx")

		# ...and it is a job like any other: same manifest, same data dir, so
		# /run and /job cannot tell it apart from an uploaded batch.
		self.assertTrue(jobs.is_valid_job_id(job_id))
		rows, _ = frontend._read_samples(jobs.job_samples_csv(job_id))
		self.assertEqual([row["isolate_id"] for row in rows], ["GOOD_S1"])
		for row in rows:
			self.assertTrue((jobs.PROJECT_ROOT / row["R1_path"]).is_file(), row["R1_path"])
			self.assertTrue((jobs.PROJECT_ROOT / row["R2_path"]).is_file(), row["R2_path"])

		snapshot = self.client.get(f"/job/{job_id}").get_json()
		self.assertEqual([s["isolate_id"] for s in snapshot["samples"]], ["GOOD_S1"])

	def test_checksum_mismatch_keeps_the_pair_out_of_the_manifest(self):
		job_id, record = self.pull(SHARED_FOLDER, self.drive)
		result = record["result"]

		self.assertEqual(result["failed"], ["MISMATCH_S4"])
		self.assertTrue(
			any("Checksum mismatch" in w for w in result["warnings"]), result["warnings"]
		)
		data_dir = jobs.job_data_dir(job_id)
		self.assertFalse((data_dir / "MISMATCH_S4_R1_001.fastq.gz").exists())

	def test_a_sign_in_page_is_not_mistaken_for_a_fastq(self):
		_job_id, record = self.pull(SHARED_FOLDER, self.drive)
		warnings = record["result"]["warnings"]
		self.assertTrue(
			any("HTML_S2_R1_001.fastq.gz" in w and "web page" in w for w in warnings), warnings
		)
		self.assertNotIn("HTML_S2", record["result"]["added"])

	def test_unreadable_fastq_is_discarded_even_with_a_plausible_content_type(self):
		_job_id, record = self.pull(SHARED_FOLDER, self.drive)
		warnings = record["result"]["warnings"]
		self.assertTrue(
			any("TRUNC_S3_R2_001.fastq.gz" in w and "readable FASTQ" in w for w in warnings),
			warnings,
		)
		self.assertNotIn("TRUNC_S3", record["result"]["added"])

	def test_non_fastq_files_are_never_downloaded(self):
		_job_id, record = self.pull(SHARED_FOLDER, self.drive)
		self.assertTrue(
			any("notes.txt" in w for w in record["result"]["warnings"]),
			record["result"]["warnings"],
		)
		# The filetype allowlist runs off the listing, so the bytes are never fetched.
		self.assertNotIn("n1", [urlparse(url).path.rsplit("/", 1)[-1] for url in self.drive.urls()])

	def test_per_file_size_cap_skips_the_file(self):
		with mock.patch.object(cloud_import, "_MAX_FILE_BYTES", 4):
			_job_id, record = self.pull(SHARED_FOLDER, self.drive)
		self.assertEqual(record["state"], "error", record)
		self.assertIn("No readable FASTQ", record["error"])

	def test_total_size_cap_aborts_the_import(self):
		with mock.patch.object(cloud_import, "_MAX_TOTAL_BYTES", 4):
			_job_id, record = self.pull(SHARED_FOLDER, self.drive)
		self.assertEqual(record["state"], "error", record)
		self.assertIn("limit for one import", record["error"])


@unittest.skip("Cloud import is disabled; these tests drive the /cloud-import routes.")
class TestOneDriveImport(Base):
	def test_shared_onedrive_folder_imports_through_the_content_host(self):
		r1, r2 = fastq_bytes(1, "r1"), fastq_bytes(1, "r2")
		onedrive = FakeOneDrive(
			{
				"OD1_S9_R1_001.fastq.gz": r1,
				"OD1_S9_R2_001.fastq.gz": r2,
				"DNA Sequencing Stats.xlsx": stats_workbook([["OD1", md5(r1), md5(r2)]]),
			}
		)
		job_id, record = self.pull("https://1drv.ms/f/s!AoXyZ-abc", onedrive)

		self.assertEqual(record["state"], "done", record)
		self.assertEqual(record["result"]["added"], ["OD1_S9"])
		self.assertEqual(record["result"]["verified"], ["OD1_S9"])
		self.assertEqual(record["result"]["provider"], "onedrive")

		rows, _ = frontend._read_samples(jobs.job_samples_csv(job_id))
		self.assertEqual([row["isolate_id"] for row in rows], ["OD1_S9"])


@unittest.skip("Cloud import is disabled; these tests drive the /cloud-import routes.")
class TestCloudImportRoute(Base):
	def test_link_off_the_allowlist_is_refused_without_creating_a_job(self):
		response = self.client.post(
			"/cloud-import", data={"share_url": "https://169.254.169.254/latest/meta-data/"}
		)
		self.assertEqual(response.status_code, 400)
		self.assertIn("Google Drive and OneDrive", response.get_json()["error"])
		self.assertEqual(frontend._cloud_imports, {})

	def test_empty_link_is_refused(self):
		self.assertEqual(self.client.post("/cloud-import", data={}).status_code, 400)

	def test_import_is_capped_and_queued_callers_are_told_so(self):
		with mock.patch.object(frontend, "MAX_CONCURRENT_CLOUD_IMPORTS", 1):
			frontend._cloud_imports["AAAAAAAAAAAA"] = {"state": "running", "finished_at": None}
			response = self.client.post("/cloud-import", data={"share_url": SHARED_FOLDER})
		self.assertEqual(response.status_code, 429)

	def test_status_of_unknown_or_malformed_job(self):
		self.assertEqual(
			self.client.get("/cloud-import/status?job_id=ABCDEFGHJKMN").status_code, 404
		)
		self.assertEqual(self.client.get("/cloud-import/status?job_id=nope").status_code, 400)

	def test_finished_records_are_pruned(self):
		frontend._cloud_imports["AAAAAAAAAAAA"] = {
			"state": "done",
			"finished_at": time.time() - frontend._CLOUD_IMPORT_RECORD_TTL - 1,
		}
		frontend._cloud_imports["BBBBBBBBBBBB"] = {"state": "running", "finished_at": None}
		frontend._prune_cloud_imports()
		self.assertNotIn("AAAAAAAAAAAA", frontend._cloud_imports)
		self.assertIn("BBBBBBBBBBBB", frontend._cloud_imports)


if __name__ == "__main__":
	unittest.main(verbosity=2)
