"""Retention policy for completed and abandoned jobs."""

import shutil
import threading
import time

from workflow.helpers import jobs


class RetentionService:
	def __init__(
		self, pipeline_manager, storage, client_factory, project_root, data_root, results_root
	):
		self.pipeline_manager = pipeline_manager
		self.storage = storage
		self.client_factory = client_factory
		self.project_root = project_root
		self.data_root = data_root
		self.results_root = results_root
		self.view_ttl_seconds = 3 * 60 * 60
		self.max_ttl_seconds = 7 * 24 * 60 * 60
		self.sweep_interval = 15 * 60
		self._thread = None

	def _claim_finished(self, job_id):
		with self.pipeline_manager.lock:
			if job_id in self.pipeline_manager.processes or job_id in self.pipeline_manager.queue:
				return False
			if not jobs.job_status_path(job_id).is_file():
				return False
			self.pipeline_manager.expiring_jobs.add(job_id)
			return True

	def _claim_unrun(self, job_id):
		with self.pipeline_manager.lock:
			if job_id in self.pipeline_manager.processes or job_id in self.pipeline_manager.queue:
				return False
			self.pipeline_manager.expiring_jobs.add(job_id)
			return True

	def _finish(self, job_id):
		with self.pipeline_manager.lock:
			self.pipeline_manager.expiring_jobs.discard(job_id)

	def _has_durable_results(self, job_id):
		"""Whether the bucket still holds this job's reports.

		Fails closed. An S3 error read as "no contents left" would delete the
		record of every job the outage covered, so an unanswered question keeps
		the record instead."""
		try:
			return bool(self.storage.list_isolates(job_id))
		except Exception as exception:
			print(f"[retention] could not list S3 results for {job_id}: {exception}")
			return True

	def _record_last_touched(self, directory):
		"""Newest mtime among the record's own files.

		Not the directory's mtime: unlinking the token rewrites that, so the
		record's deadline would be pushed out by a further TTL every time the
		token sweep fired, and a record whose token expired would never reach
		its own deadline at all."""
		file_mtimes = [path.stat().st_mtime for path in directory.iterdir() if path.is_file()]
		return max(file_mtimes, default=directory.stat().st_mtime)

	def _expire_token(self, directory, current_time):
		token_path = directory / ".bvbrc_token"
		if (
			not token_path.is_file()
			or current_time - token_path.stat().st_mtime < self.max_ttl_seconds
		):
			return
		with self.pipeline_manager.lock:
			if (
				directory.name not in self.pipeline_manager.processes
				and directory.name not in self.pipeline_manager.queue
			):
				token_path.unlink(missing_ok=True)

	def _delete_record(self, job_id):
		"""Delete the job ID itself.

		The record -- the samples manifest, the upload log, the imported
		checksums, the endpoint overrides, the token -- is what a job ID means,
		and the run log is keyed by it. All of it used to outlive every byte it
		described: the sweep deleted results and reads and left the record
		untouched, so config/jobs/ kept one directory per job ID ever issued,
		forever."""
		shutil.rmtree(jobs.job_config_dir(job_id), ignore_errors=True)
		jobs.job_log_path(job_id).unlink(missing_ok=True)

	def _expire_job_record(self, job_id, directory, current_time):
		"""Delete the record of a job whose results will never be swept.

		_expire_results takes the record out with the contents, so the records
		that reach here are the ones it never runs for: an upload that was never
		run, a run that died before writing a status, and jobs issued before
		records were deleted at all. None of them has contents to wait on -- only
		an ID holding a manifest open.
		"""
		if jobs.job_results_dir(job_id).is_dir():
			# Results still on disk: either not due yet, or .pinned and never
			# due. Their sweep owns the record.
			return
		if current_time - self._record_last_touched(directory) < self.max_ttl_seconds:
			# Too young to judge -- an upload waiting to be run has a record and
			# no results, and looks exactly like an abandoned one.
			return
		if self._has_durable_results(job_id):
			# A job from before delete_results existed, or one whose delete
			# failed: the bucket can still serve it, so the ID still has to
			# resolve. The lifecycle rule will drop the objects and a later
			# sweep will take the record.
			return
		if not self._claim_unrun(job_id):
			return
		try:
			self._delete_record(job_id)
		finally:
			self._finish(job_id)

	def _expire_results(self, job_id, directory, current_time):
		if jobs.job_pinned_path(job_id).is_file():
			return
		status_path = jobs.job_status_path(job_id)
		if not status_path.is_file():
			return
		viewed_path = jobs.job_first_viewed_path(job_id)
		expired = current_time - status_path.stat().st_mtime >= self.max_ttl_seconds
		expired = expired or (
			viewed_path.is_file()
			and current_time - viewed_path.stat().st_mtime >= self.view_ttl_seconds
		)
		if not expired or not self._claim_finished(job_id):
			return
		try:
			self.client_factory(job_id).destroy_token()
			shutil.rmtree(directory, ignore_errors=True)
			shutil.rmtree(jobs.job_data_dir(job_id), ignore_errors=True)
			try:
				self.storage.delete_raw(job_id)
			except Exception as exception:
				print(f"[s3] failed to delete raw uploads for job {job_id}: {exception}")
			# The stored reports are the same results this sweep just deleted
			# locally, and the view/download routes fall back to them. Leaving
			# them would mean the job kept serving results it had been told to
			# expire.
			stored_results_deleted = True
			try:
				self.storage.delete_results(job_id)
			except Exception as exception:
				stored_results_deleted = False
				print(f"[s3] failed to delete stored results for job {job_id}: {exception}")
			if stored_results_deleted:
				self._delete_record(job_id)
			# Otherwise the bucket can still answer for this ID, and the record is
			# what the samples table and the settings page read from. Keeping the
			# two consistent matters more than collecting the record now:
			# _expire_job_record takes it once the objects are actually gone.
		finally:
			self._finish(job_id)

	def sweep(self):
		current_time = time.time()
		results_root = self.results_root()
		if results_root.is_dir():
			for directory in results_root.iterdir():
				job_id = directory.name
				if not directory.is_dir() or not jobs.is_valid_job_id(job_id):
					continue
				try:
					self._expire_results(job_id, directory, current_time)
				except Exception as exception:
					print(f"[retention] could not expire {job_id}: {exception}")
		# After the results loop, not before: on a local-only deployment the sweep
		# above is what makes a job's contents gone, and the record should follow
		# it out in the same pass rather than linger for another 15 minutes.
		config_root = self.project_root() / "config" / "jobs"
		if config_root.is_dir():
			for directory in config_root.iterdir():
				# config/jobs also holds the persisted queue, the run history and
				# the drain flag. They are app state, not job records.
				if not directory.is_dir() or not jobs.is_valid_job_id(directory.name):
					continue
				try:
					self._expire_token(directory, current_time)
					self._expire_job_record(directory.name, directory, current_time)
				except Exception as exception:
					print(f"[retention] could not expire record {directory.name}: {exception}")
		data_root = self.data_root()
		if data_root.is_dir():
			for directory in data_root.iterdir():
				job_id = directory.name
				if not directory.is_dir() or not jobs.is_valid_job_id(job_id):
					continue
				if (
					jobs.job_status_path(job_id).is_file()
					or current_time - directory.stat().st_mtime < self.max_ttl_seconds
				):
					continue
				if not self._claim_unrun(job_id):
					continue
				try:
					shutil.rmtree(directory, ignore_errors=True)
					try:
						self.storage.delete_raw(job_id)
					except Exception as exception:
						print(f"[s3] failed to delete raw uploads for job {job_id}: {exception}")
				finally:
					self._finish(job_id)

	def start(self):
		"""Start the optional background loop exactly once, after app setup."""
		if self._thread is not None:
			return
		self._thread = threading.Thread(target=self._loop, daemon=True, name="job-retention")
		self._thread.start()

	def _loop(self):
		while True:
			time.sleep(self.sweep_interval)
			try:
				self.sweep()
			except Exception as exception:
				print(f"[retention] sweep failed: {exception}")
