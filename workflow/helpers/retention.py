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
		config_root = self.project_root() / "config" / "jobs"
		if config_root.is_dir():
			for directory in config_root.iterdir():
				token_path = directory / ".bvbrc_token"
				if (
					not token_path.is_file()
					or current_time - token_path.stat().st_mtime < self.max_ttl_seconds
				):
					continue
				with self.pipeline_manager.lock:
					if (
						directory.name not in self.pipeline_manager.processes
						and directory.name not in self.pipeline_manager.queue
					):
						token_path.unlink(missing_ok=True)
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
