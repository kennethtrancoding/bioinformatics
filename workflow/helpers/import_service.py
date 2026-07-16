"""Input handling shared by pair, folder, and cloud imports."""

import shutil
import tempfile
import threading
import time
from pathlib import Path

from workflow.helpers import cloud_import, import_samples


class StreamToDiskAndS3:
	"""Read proxy that preserves a local copy while S3 consumes an upload."""

	def __init__(self, source_stream, destination_path, chunk_bytes=1024 * 1024):
		self._source_stream = source_stream
		self._destination_file = open(destination_path, "wb")
		self._chunk_bytes = chunk_bytes
		self.bytes_written = 0

	def read(self, size=-1):
		chunk = self._source_stream.read(size)
		if chunk:
			self._destination_file.write(chunk)
			self.bytes_written += len(chunk)
		return chunk

	def drain(self):
		while self.read(self._chunk_bytes):
			pass

	def close(self):
		self._destination_file.close()


class ImportService:
	"""Own raw-read persistence and manifest registration dependencies."""

	def __init__(self, storage, job_store, project_root, job_lock):
		self.storage = storage
		self.job_store = job_store
		self.project_root = project_root
		self.job_lock = job_lock

	def save_upload(self, job_id, upload_file, destination_path):
		stream = StreamToDiskAndS3(upload_file.stream, destination_path)
		stored_in_s3 = False
		try:
			try:
				self.storage.upload_raw_fileobj(job_id, destination_path.name, stream)
				stored_in_s3 = self.storage.is_enabled()
			except Exception as exception:
				print(
					f"[s3] failed to stream raw {destination_path.name} for job {job_id}: {exception}"
				)
			stream.drain()
		finally:
			stream.close()
		return stored_in_s3

	def release_local_raw(self, job_id, *paths):
		if not self.storage.is_enabled():
			return
		for path in paths:
			try:
				path.unlink(missing_ok=True)
			except OSError as exception:
				print(f"[raw] could not release local {path.name} for job {job_id}: {exception}")

	def discard_raw_upload(self, job_id, *paths):
		for path in paths:
			path.unlink(missing_ok=True)
			try:
				self.storage.delete_raw_file(job_id, path.name)
			except Exception as exception:
				print(
					f"[s3] failed to delete rejected raw {path.name} for job {job_id}: {exception}"
				)

	def backup_raw_files(self, job_id, raw_paths):
		if not self.storage.is_enabled():
			return []
		uploaded_paths = []
		for raw_path in raw_paths:
			if not raw_path.is_file():
				continue
			try:
				self.storage.upload_raw_file(job_id, raw_path)
				uploaded_paths.append(raw_path)
			except Exception as exception:
				print(f"[s3] failed to upload raw {raw_path.name} for job {job_id}: {exception}")
		return uploaded_paths

	def raw_paths_for_isolates(self, job_id, isolate_ids, samples_csv):
		if not isolate_ids:
			return []
		wanted = set(isolate_ids)
		raw_paths = []
		for sample_row in self.job_store.read_samples(samples_csv)[0]:
			if sample_row.get("isolate_id") not in wanted:
				continue
			for path_column in ("R1_path", "R2_path"):
				manifest_path = sample_row.get(path_column)
				if manifest_path:
					raw_paths.append(self.project_root() / manifest_path)
		return raw_paths

	def offload_pair(self, job_id, registered_paths):
		"""Send one freshly imported pair to S3 and drop the local copy.

		Called by import_directory as each pair lands, rather than once the whole
		folder has been registered: the reads are only needed locally long enough to
		verify and upload them, so releasing per pair keeps peak disk at one pair
		instead of the size of the batch. A read S3 refused stays on disk --
		backup_raw_files only returns what it confirmed -- so it is never missing
		from both places at once."""
		self.release_local_raw(
			job_id,
			*self.backup_raw_files(job_id, [Path(path) for path in registered_paths.values()]),
		)

	def import_directory(self, job_id, source_dir, samples_csv, data_dir, method, started_at):
		with self.job_lock(job_id):
			result = import_samples.import_directory(
				source_dir,
				samples_csv=samples_csv,
				recursive=True,
				dest_dir=data_dir,
				move=True,
				on_pair_imported=lambda isolate_id, registered_paths: self.offload_pair(
					job_id, registered_paths
				),
			)
			result["upload"] = self.job_store.record_upload(
				job_id, method, started_at, result["added"], result["updated"]
			)
		return result


class CloudImportManager:
	"""Tracks asynchronous cloud imports independently of Flask request state."""

	def __init__(self, import_service, max_concurrent, on_complete):
		self.import_service = import_service
		self.max_concurrent = max_concurrent
		self.on_complete = on_complete
		self.lock = threading.Lock()
		self.records = {}
		self.record_ttl = 60 * 60

	def set(self, job_id, **fields):
		with self.lock:
			self.records.setdefault(job_id, {}).update(fields)

	def prune(self):
		cutoff_time = time.time() - self.record_ttl
		for job_id, record in list(self.records.items()):
			if record.get("finished_at") is not None and record["finished_at"] < cutoff_time:
				self.records.pop(job_id, None)

	def start(self, job_id, share_url, upload_form, samples_csv, data_dir):
		with self.lock:
			self.prune()
			running = sum(1 for record in self.records.values() if record.get("state") == "running")
			if running >= self.max_concurrent:
				return (
					False,
					"Too many cloud imports are already running. Try again in a few minutes.",
				)
			if self.records.get(job_id, {}).get("state") == "running":
				return False, "This job already has a cloud import running."
			self.records[job_id] = {
				"state": "running",
				"message": "Reading the shared folder…",
				"files_done": 0,
				"files_total": None,
				"finished_at": None,
			}
		threading.Thread(
			target=self._run,
			args=(job_id, share_url, upload_form, samples_csv, data_dir),
			daemon=True,
		).start()
		return True, None

	def get(self, job_id):
		with self.lock:
			record = self.records.get(job_id)
			return dict(record) if record else None

	def _run(self, job_id, share_url, upload_form, samples_csv, data_dir):
		started_at = time.time()
		staging_dir = Path(tempfile.mkdtemp(prefix="cloud_import_"))
		try:
			fetch_result = cloud_import.fetch_share_link(
				share_url,
				staging_dir,
				progress=lambda done, total, message: self.set(
					job_id, files_done=done, files_total=total, message=message
				),
			)
			self.set(job_id, message="Verifying checksums and registering samples…")
			result = self.import_service.import_directory(
				job_id, staging_dir, samples_csv, data_dir, "cloud", started_at
			)
			result.update(job_id=job_id, provider=fetch_result["provider"])
			result["warnings"] = fetch_result["warnings"] + result["warnings"]
			result["skipped"] = len(result["warnings"])
			self.on_complete(job_id, result, upload_form)
			self.set(
				job_id,
				state="done",
				message="Import complete.",
				result=result,
				finished_at=time.time(),
			)
		except cloud_import.CloudImportError as exception:
			self.set(job_id, state="error", error=str(exception), finished_at=time.time())
		except Exception as exception:
			print(f"[cloud-import] job {job_id} failed: {exception!r}")
			self.set(
				job_id,
				state="error",
				error="The import failed unexpectedly. Check the server log for details.",
				finished_at=time.time(),
			)
		finally:
			shutil.rmtree(staging_dir, ignore_errors=True)
