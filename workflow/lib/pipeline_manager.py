"""In-process orchestration for bounded Snakemake runs.

The manager deliberately contains no Flask objects. Routes provide authentication
and turn its ``(payload, status)`` results into HTTP responses.
"""

import json
import subprocess
import threading
import time
from collections import deque

from workflow.lib import jobs


class PipelineManager:
	def __init__(self, job_store, storage, project_root, *, max_concurrent, cores, popen_module=None):
		self.job_store = job_store
		self.storage = storage
		self.project_root = project_root
		self.max_concurrent = max_concurrent
		self.cores = cores
		self.popen_module = popen_module or subprocess
		self.lock = threading.Lock()
		self.processes = {}
		self.queue = deque()
		self.aborted_jobs = set()
		self.expiring_jobs = set()

	def is_draining(self):
		return jobs.drain_flag_path().is_file()

	def persist_queue(self):
		queue_path = jobs.pipeline_queue_path()
		queue_path.parent.mkdir(parents=True, exist_ok=True)
		temporary_path = queue_path.with_name(queue_path.name + ".tmp")
		temporary_path.write_text(json.dumps(list(self.queue)))
		temporary_path.replace(queue_path)

	def _write_status(self, job_id, success, *, aborted=False, interrupted=False):
		error = None
		if aborted:
			error = "Pipeline run aborted by user."
		elif interrupted:
			error = (
				"The server restarted while this run was in progress, so the run was stopped "
				"before it finished. No results were lost -- re-run the job to complete it."
			)
		elif not success:
			error = self._extract_error_summary(jobs.job_log_path(job_id))
		self.job_store.write_status(job_id, success, error=error)

	@staticmethod
	def _extract_error_summary(log_path, max_chars=800):
		try:
			log_text = log_path.read_text(errors="replace")
		except OSError:
			return None
		error_start = log_text.rfind("Error in rule ")
		if error_start == -1:
			error_start = log_text.rfind("\nError")
		return None if error_start == -1 else log_text[error_start : error_start + max_chars].strip()

	def start(self, job_id):
		"""Spawn one run. Caller must hold ``lock``."""
		log_path = jobs.job_log_path(job_id)
		log_path.parent.mkdir(parents=True, exist_ok=True)
		results_dir = jobs.job_results_dir(job_id)
		results_dir.mkdir(parents=True, exist_ok=True)
		jobs.job_run_started_path(job_id).write_text(str(time.time()))
		with log_path.open("w") as pipeline_log_file:
			process = self.popen_module.Popen(
				[
					"snakemake", "--cores", str(self.cores), "--use-conda", "--rerun-incomplete",
					"--nolock", "--config", f"job_id={job_id}",
					f"results_dir={results_dir.relative_to(self.project_root())}",
					f"samples_manifest={jobs.job_samples_csv(job_id).relative_to(self.project_root())}",
				],
				cwd=self.project_root(), stdout=pipeline_log_file, stderr=self.popen_module.STDOUT,
				start_new_session=True,
			)
		self.processes[job_id] = process
		threading.Thread(target=self.watch, args=(job_id, process), daemon=True).start()

	def drain(self):
		"""Start waiting runs while capacity exists. Caller must hold ``lock``."""
		if self.is_draining():
			return
		queue_changed = False
		while self.queue and len(self.processes) < self.max_concurrent:
			job_id = self.queue.popleft()
			queue_changed = True
			try:
				self.start(job_id)
			except Exception as exception:
				self._write_status(job_id, False)
				print(f"[pipeline] failed to start queued job {job_id}: {exception}")
		if queue_changed:
			self.persist_queue()

	def claim_raw(self, job_id):
		try:
			self.storage.mark_raw_in_use(job_id)
		except Exception as exception:
			print(f"[s3] could not mark raw reads in-use for job {job_id}: {exception}")

	def return_raw(self, job_id):
		try:
			self.storage.mark_raw_unrun(job_id)
		except Exception as exception:
			print(f"[s3] could not return raw reads to the expiry clock for job {job_id}: {exception}")

	def watch(self, job_id, process):
		returncode = process.wait()
		with self.lock:
			aborted = job_id in self.aborted_jobs
			self.aborted_jobs.discard(job_id)
			self._write_status(job_id, returncode == 0, aborted=aborted)
			if self.processes.get(job_id) is process:
				self.processes.pop(job_id, None)
			self.drain()
		if returncode == 0 and not aborted:
			try:
				self.storage.upload_job_results(job_id, jobs.job_results_dir(job_id))
			except Exception as exception:
				print(f"[s3] failed to upload results for job {job_id}: {exception}")
			try:
				self.storage.delete_raw(job_id)
			except Exception as exception:
				print(f"[s3] failed to delete raw uploads for job {job_id}: {exception}")
		else:
			self.return_raw(job_id)

	def is_busy(self, job_id):
		with self.lock:
			process = self.processes.get(job_id)
			return (process is not None and process.poll() is None) or job_id in self.queue

	def admit(self, job_id, authenticated):
		if not jobs.job_samples_csv(job_id).exists():
			return {"error": "Unknown job ID"}, 404
		if not self.job_store.read_samples(jobs.job_samples_csv(job_id))[0]:
			return {"error": "No FASTQ data has been uploaded for this job yet"}, 400
		with self.lock:
			process = self.processes.get(job_id)
			if (process is not None and process.poll() is None) or job_id in self.queue:
				return {"error": "This pipeline job is already in progress"}, 409
			if job_id in self.expiring_jobs:
				return {"error": "This job has expired and its data is being deleted. Upload again to start a new job."}, 410
			if not authenticated():
				return {"error": "BV-BRC login required: submit your BV-BRC username and password before running"}, 401
			self.job_store.reset_run_markers(job_id)
			draining = self.is_draining()
			if draining or len(self.processes) >= self.max_concurrent:
				self.queue.append(job_id)
				self.persist_queue()
				payload = {
					"message": "Pipeline queued for a database refresh; it starts automatically when the refresh finishes" if draining else "Pipeline queued",
					"job_id": job_id, "queued": True, "queue_position": len(self.queue),
				}
				status = 202
			else:
				self.start(job_id)
				payload, status = {"message": "Pipeline started", "job_id": job_id, "queued": False}, 200
		self.claim_raw(job_id)
		return payload, status

	def reconcile(self, results_root):
		"""Restore persisted queued work and mark interrupted runs after boot."""
		queued_job_ids = []
		try:
			persisted_queue = json.loads(jobs.pipeline_queue_path().read_text())
			queued_job_ids = [
				job_id for job_id in persisted_queue
				if jobs.is_valid_job_id(job_id) and jobs.job_samples_csv(job_id).is_file()
			]
		except FileNotFoundError:
			pass
		except (OSError, ValueError) as exception:
			print(f"[pipeline] could not read the persisted queue: {exception}")
		queued_set = set(queued_job_ids)
		if results_root.is_dir():
			for directory in results_root.iterdir():
				job_id = directory.name
				if not directory.is_dir() or not jobs.is_valid_job_id(job_id) or job_id in queued_set:
					continue
				if jobs.job_run_started_path(job_id).is_file() and not jobs.job_status_path(job_id).is_file():
					print(f"[pipeline] job {job_id} was interrupted by a restart; recording it as failed")
					try:
						self._write_status(job_id, False, interrupted=True)
					except OSError as exception:
						print(f"[pipeline] could not record interrupted job {job_id}: {exception}")
					self.return_raw(job_id)
		with self.lock:
			self.queue.clear()
			self.queue.extend(queued_job_ids)
			jobs.drain_flag_path().unlink(missing_ok=True)
			self.persist_queue()
			if queued_job_ids:
				print(f"[pipeline] resuming {len(queued_job_ids)} queued job(s) after restart")
			self.drain()
