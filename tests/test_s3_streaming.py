"""Raw uploads reach S3 while they are still arriving.

An upload used to be written to disk in full, and then read back off disk in a
second pass to be pushed to S3 -- and that second pass re-uploaded the job's *whole*
data directory, so filling one batch from ten uploads re-sent the first nine files
nine times over.

Now the request body is teed: the same pass that writes the FASTQ where the pipeline
will read it also streams it to S3. That makes S3 part of the upload rather than a
step after it, which is what these cover -- along with the thing that must remain
true regardless: the local copy is the source of truth, so nothing S3 does may cost
us the file.
"""

import io
import sys
import unittest
from unittest import mock

from botocore.exceptions import ClientError

import frontend  # noqa: E402
from tests._isolation import REAL_ROOT  # noqa: F401  (must import first)
from tests.test_batching import _REAL_POPEN, Base, token_for  # noqa: F401,E402
from tests.test_cloud_import import fastq_bytes, md5  # noqa: E402
from workflow.helpers import jobs, s3_storage  # noqa: E402


class _FakePaginator:
	def __init__(self, objects):
		self._objects = objects

	def paginate(self, Bucket=None, Prefix=""):
		yield {
			"Contents": [{"Key": key} for key in sorted(self._objects) if key.startswith(Prefix)]
		}


class FakeS3:
	"""Records what was uploaded and how it was tagged, and can blow up partway
	through a stream. Tags matter here: the bucket's lifecycle rule expires raw objects
	by tag, so the tag is what decides whether a job's only copy of its reads survives."""

	def __init__(self, fail_after_bytes=None):
		self.objects = {}
		self.tags = {}
		self.deleted = []
		self.fail_after_bytes = fail_after_bytes

	@staticmethod
	def _tag_from(extra_args):
		tagging = (extra_args or {}).get("Tagging", "")
		return dict(pair.split("=", 1) for pair in tagging.split("&") if "=" in pair)

	def upload_fileobj(self, fileobj, bucket, key, ExtraArgs=None, Config=None):
		received = bytearray()
		while True:
			chunk = fileobj.read(64 * 1024)
			if not chunk:
				break
			received += chunk
			if self.fail_after_bytes is not None and len(received) >= self.fail_after_bytes:
				raise RuntimeError("S3 went away mid-stream")
		self.objects[key] = bytes(received)
		self.tags[key] = self._tag_from(ExtraArgs)

	def upload_file(self, file_path, bucket, key, ExtraArgs=None):
		with open(file_path, "rb") as file_handle:
			self.objects[key] = file_handle.read()
		self.tags[key] = self._tag_from(ExtraArgs)

	def download_file(self, bucket, key, file_path):
		if key not in self.objects:
			raise KeyError(f"no such object: {key}")
		with open(file_path, "wb") as file_handle:
			file_handle.write(self.objects[key])

	def put_object_tagging(self, Bucket=None, Key=None, Tagging=None):
		self.tags[Key] = {tag["Key"]: tag["Value"] for tag in Tagging["TagSet"]}

	def head_object(self, Bucket=None, Key=None):
		"""Exists/does not exist, answered the way boto does it.

		s3_storage.object_exists reads a missing object off the *exception*, not
		off a return value, so a fake that returned None for an absent key would
		report every object as present. Admission asks this question before every
		run (pipeline_manager._check_missing_files): with the reads released from
		local disk, S3 is the only place left to find them, so a fake that always
		said yes would let a run start on reads that are not anywhere."""
		if Key not in self.objects:
			raise ClientError(
				{"Error": {"Code": "404", "Message": "Not Found"}}, "HeadObject"
			)
		return {"ContentLength": len(self.objects[Key]), "ETag": '"fake"'}

	def get_paginator(self, _operation):
		return _FakePaginator(self.objects)

	def delete_object(self, Bucket, Key):
		self.deleted.append(Key)
		self.objects.pop(Key, None)
		self.tags.pop(Key, None)

	def delete_objects(self, Bucket=None, Delete=None):
		for stored_object in Delete["Objects"]:
			self.delete_object(Bucket, stored_object["Key"])

	def raw_keys(self):
		return sorted(key for key in self.objects if key.startswith("raw/"))

	def raw_states(self):
		"""What the lifecycle rule would see: key -> raw-state tag."""
		return {key: self.tags.get(key, {}).get("raw-state") for key in self.raw_keys()}


class S3Base(Base):
	def setUp(self):
		super().setUp()
		self.s3 = FakeS3()
		self._enable(self.s3)

	def _enable(self, fake):
		patcher_client = mock.patch.object(s3_storage, "_client", fake)
		patcher_bucket = mock.patch.object(s3_storage, "_BUCKET", "test-bucket")
		patcher_client.start()
		patcher_bucket.start()
		self.addCleanup(patcher_client.stop)
		self.addCleanup(patcher_bucket.stop)

	def local_bytes(self, job_id, name):
		return (jobs.job_data_dir(job_id) / name).read_bytes()


class TestUploadReachesS3DuringTheUpload(S3Base):
	def test_a_pair_is_in_s3_by_the_time_the_request_returns(self):
		r1, r2 = fastq_bytes(3, "r1"), fastq_bytes(3, "r2")
		response = self.client.post(
			"/submit",
			data={
				"fastq_file_1": (io.BytesIO(r1), "STREAM_R1_001.fastq.gz"),
				"fastq_file_2": (io.BytesIO(r2), "STREAM_R2_001.fastq.gz"),
			},
			content_type="multipart/form-data",
		)
		self.assertEqual(response.status_code, 200)
		job_id = response.get_json()["job_id"]

		self.assertEqual(
			self.s3.raw_keys(),
			[f"raw/{job_id}/STREAM_R1_001.fastq.gz", f"raw/{job_id}/STREAM_R2_001.fastq.gz"],
		)
		for name, expected in (("STREAM_R1_001.fastq.gz", r1), ("STREAM_R2_001.fastq.gz", r2)):
			# S3 holds the reads, byte for byte...
			self.assertEqual(self.s3.objects[f"raw/{job_id}/{name}"], expected)
			# ...and the local copy is gone. This is the disk win: reads no longer sit
			# on the box between an upload and its run. fetch_raw_read pulls them back
			# when the run needs them (workflow/rules/raw.smk).
			self.assertFalse(
				(jobs.job_data_dir(job_id) / name).exists(),
				f"{name} was left on local disk after S3 took it",
			)

	def test_filling_one_job_from_many_uploads_does_not_re_upload_the_earlier_ones(self):
		"""The quadratic bug: the old backup globbed the whole data directory, so every
		new pair re-sent every pair before it."""
		first = self.submit_pair("QUAD_A").get_json()["job_id"]
		self.assertEqual(len(self.s3.raw_keys()), 2)

		self.s3.objects.clear()  # forget the first pair; watch what the second sends
		self.submit_pair("QUAD_B", job_id=first)

		self.assertEqual(
			self.s3.raw_keys(),
			[f"raw/{first}/QUAD_B_R1_001.fastq.gz", f"raw/{first}/QUAD_B_R2_001.fastq.gz"],
			"the second upload re-sent files from the first",
		)

	def test_a_folder_import_uploads_only_the_isolates_it_added(self):
		"""The import paths stage their files on disk before we see them, so they cannot
		stream -- but they must still send only what they brought."""
		job_id = self.import_folder(["FOLD_A_S1"]).get_json()["job_id"]
		self.assertEqual(len(self.s3.raw_keys()), 2)

		self.s3.objects.clear()
		self.import_folder(["FOLD_B_S2"], job_id=job_id)

		self.assertEqual(
			self.s3.raw_keys(),
			[f"raw/{job_id}/FOLD_B_S2_R1_001.fastq.gz", f"raw/{job_id}/FOLD_B_S2_R2_001.fastq.gz"],
		)


class TestS3MustNeverCostUsTheFile(S3Base):
	def setUp(self):
		super().setUp()
		# S3 dies a few bytes into every stream.
		self.s3.fail_after_bytes = 1

	def test_an_s3_failure_mid_stream_still_writes_the_whole_file_to_disk(self):
		"""The local copy is what the pipeline reads. If the tee stopped when S3 did,
		the run would silently analyse a truncated FASTQ."""
		r1, r2 = fastq_bytes(50, "r1"), fastq_bytes(50, "r2")
		response = self.client.post(
			"/submit",
			data={
				"fastq_file_1": (io.BytesIO(r1), "SAFE_R1_001.fastq.gz"),
				"fastq_file_2": (io.BytesIO(r2), "SAFE_R2_001.fastq.gz"),
			},
			content_type="multipart/form-data",
		)

		# The upload succeeded despite S3 being down...
		self.assertEqual(response.status_code, 200)
		job_id = response.get_json()["job_id"]
		# ...and both files are complete on disk, not truncated where S3 gave up.
		self.assertEqual(self.local_bytes(job_id, "SAFE_R1_001.fastq.gz"), r1)
		self.assertEqual(self.local_bytes(job_id, "SAFE_R2_001.fastq.gz"), r2)

	def test_the_manifest_still_points_at_the_files(self):
		job_id = self.submit_pair("SAFE2").get_json()["job_id"]
		self.assertEqual(self.isolates(job_id), ["SAFE2"])


class TestReadsComeBackForTheRun(S3Base):
	def test_a_released_read_can_be_fetched_again_from_s3(self):
		"""The round trip the whole design rests on: if a read cannot come back, the
		run has nothing to analyse."""
		r1 = fastq_bytes(7, "r1")
		response = self.client.post(
			"/submit",
			data={
				"fastq_file_1": (io.BytesIO(r1), "TRIP_R1_001.fastq.gz"),
				"fastq_file_2": (io.BytesIO(fastq_bytes(7, "r2")), "TRIP_R2_001.fastq.gz"),
			},
			content_type="multipart/form-data",
		)
		job_id = response.get_json()["job_id"]
		read_path = jobs.job_data_dir(job_id) / "TRIP_R1_001.fastq.gz"
		self.assertFalse(read_path.exists())

		# This is what workflow/rules/raw.smk does before validate_fastq runs.
		s3_storage.download_raw_file(job_id, "TRIP_R1_001.fastq.gz", read_path)

		self.assertTrue(read_path.exists())
		self.assertEqual(read_path.read_bytes(), r1)

	def test_a_missing_object_fails_loudly_rather_than_yielding_an_empty_file(self):
		"""A rule that quietly analysed nothing would be far worse than one that
		stops, so the fetch is deliberately not best-effort."""
		with self.assertRaises(Exception):
			s3_storage.download_raw_file(
				"ABCDEFGHJKMN", "nope.fastq.gz", jobs.job_data_dir("ABCDEFGHJKMN") / "nope.fastq.gz"
			)

	def test_a_run_is_refused_when_a_read_is_in_neither_place(self):
		"""Admission asks S3 whether the released reads are still there, and has to
		believe a no. The upload deletes the local copy once S3 confirms it, so with
		the object gone the read is nowhere -- and a run admitted on it dies an hour
		later inside raw.smk, having already frozen the job's samples. Refusing here
		is a 409 the user can act on; the alternative is a crash they cannot."""
		job_id = self.submit_pair("VANISHED").get_json()["job_id"]
		token_for(job_id)
		self.assertFalse((jobs.job_data_dir(job_id) / "VANISHED_R1_001.fastq.gz").exists())

		self.s3.objects.pop(s3_storage.raw_key_for(job_id, "VANISHED_R1_001.fastq.gz"))

		response = self.client.post("/run", data={"job_id": job_id})
		self.assertEqual(response.status_code, 409)
		self.assertIn("upload still in progress", response.get_json()["error"].lower())
		self.assertEqual(frontend._pipeline_manager.processes, {})


class TestWithoutABucketTheLocalCopyIsTheOnlyCopy(Base):
	"""No S3 configured -- local dev, and the default in these tests. Nothing may be
	released, because there would be nowhere to fetch it back from."""

	def test_reads_stay_on_disk_when_there_is_no_bucket(self):
		self.assertFalse(s3_storage.is_enabled())
		job_id = self.submit_pair("NOBUCKET").get_json()["job_id"]

		raw_files = sorted(path.name for path in jobs.job_data_dir(job_id).glob("*.fastq.gz"))
		self.assertEqual(raw_files, ["NOBUCKET_R1_001.fastq.gz", "NOBUCKET_R2_001.fastq.gz"])


class TestTheExpiryClockNeverEatsReadsAJobNeeds(S3Base):
	"""deploy/s3-lifecycle.json expires raw objects tagged `unrun` after 7 days. Since
	the upload releases the local copy, those objects are the only copy there is -- so
	the tag is the single thing standing between a queued job and the deletion of its
	own inputs."""

	def runnable(self, name):
		job_id = self.submit_pair(name).get_json()["job_id"]
		token_for(job_id)
		return job_id

	def test_a_fresh_upload_is_on_the_clock(self):
		job_id = self.runnable("CLOCK_A")
		self.assertEqual(
			set(self.s3.raw_states().values()),
			{"unrun"},
			"an uploaded-but-unrun read must be expirable, or the bucket fills forever",
		)

	def test_running_a_job_takes_its_reads_off_the_clock(self):
		job_id = self.runnable("CLOCK_B")
		frontend.subprocess.Popen = lambda argv, **kwargs: _REAL_POPEN(
			[sys.executable, "-c", "import time; time.sleep(30)"], **kwargs
		)
		self.addCleanup(self._kill_running)

		self.assertEqual(self.client.post("/run", data={"job_id": job_id}).status_code, 200)

		self.assertEqual(set(self.s3.raw_states().values()), {"in-use"})

	def test_merely_QUEUEING_a_job_takes_its_reads_off_the_clock(self):
		"""The case that would actually bite: a job waiting behind a full slot needs its
		reads just as much as one that is running, and a busy queue is exactly where a
		week could quietly go by."""
		frontend.MAX_CONCURRENT_PIPELINES = 1
		self.addCleanup(setattr, frontend, "MAX_CONCURRENT_PIPELINES", 2)
		frontend.subprocess.Popen = lambda argv, **kwargs: _REAL_POPEN(
			[sys.executable, "-c", "import time; time.sleep(30)"], **kwargs
		)
		self.addCleanup(self._kill_running)

		running = self.runnable("CLOCK_RUNNING")
		self.client.post("/run", data={"job_id": running})
		queued = self.runnable("CLOCK_QUEUED")
		response = self.client.post("/run", data={"job_id": queued})
		self.assertEqual(response.status_code, 202)

		queued_keys = [key for key in self.s3.raw_keys() if queued in key]
		self.assertTrue(queued_keys)
		for key in queued_keys:
			self.assertEqual(
				self.s3.raw_states()[key],
				"in-use",
				"a queued job's reads are still on the 7-day fuse",
			)

	def test_cancelling_a_queued_run_puts_its_reads_back_on_the_clock(self):
		"""Otherwise an abandoned job keeps its FASTQ in the bucket forever -- the very
		accumulation the rule exists to prevent."""
		frontend.MAX_CONCURRENT_PIPELINES = 1
		self.addCleanup(setattr, frontend, "MAX_CONCURRENT_PIPELINES", 2)
		frontend.subprocess.Popen = lambda argv, **kwargs: _REAL_POPEN(
			[sys.executable, "-c", "import time; time.sleep(30)"], **kwargs
		)
		self.addCleanup(self._kill_running)

		running = self.runnable("CANCEL_RUNNING")
		self.client.post("/run", data={"job_id": running})
		queued = self.runnable("CANCEL_QUEUED")
		self.client.post("/run", data={"job_id": queued})

		self.assertEqual(self.client.post("/abort", data={"job_id": queued}).status_code, 200)

		for key in [key for key in self.s3.raw_keys() if queued in key]:
			self.assertEqual(self.s3.raw_states()[key], "unrun")

	def _kill_running(self):
		for pipeline_process in list(frontend._pipeline_manager.processes.values()):
			pipeline_process.kill()
		frontend._pipeline_manager.processes.clear()
		frontend._pipeline_manager.queue.clear()
		frontend.subprocess.Popen = _REAL_POPEN


class TestRejectedUploadsLeaveNothingBehind(S3Base):
	def test_a_checksum_failure_deletes_the_s3_copy_too(self):
		"""An upload now reaches S3 before it has been verified, so a file rejected
		afterwards would otherwise linger in the bucket as bad data forever."""
		r1, r2 = fastq_bytes(3, "r1"), fastq_bytes(3, "r2")
		response = self.client.post(
			"/submit",
			data={
				"fastq_file_1": (io.BytesIO(r1), "BAD_R1_001.fastq.gz"),
				"fastq_file_2": (io.BytesIO(r2), "BAD_R2_001.fastq.gz"),
				"fastq_file_1_checksum": md5(b"something else entirely"),
			},
			content_type="multipart/form-data",
		)

		self.assertEqual(response.status_code, 400)
		self.assertIn("checksum mismatch", response.get_json()["error"])
		# Gone from S3, and gone from disk.
		self.assertEqual(self.s3.raw_keys(), [])
		self.assertEqual(len(self.s3.deleted), 2)


if __name__ == "__main__":
	unittest.main()
