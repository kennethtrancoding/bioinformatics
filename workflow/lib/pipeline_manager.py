"""In-process orchestration for bounded Snakemake runs.

The manager deliberately contains no Flask objects. Routes provide authentication
and turn its ``(payload, status)`` results into HTTP responses.

WHAT BOUNDS A RUN

Three different things are scarce, and they are not the same thing, so this hands
Snakemake three separate budgets rather than one:

  cpu      the cores the box actually has. Charged by the rules that compute --
           RGI and MobileElementFinder charge their full thread count, everything
           else charges 1 by default (--default-resources cpu=1).
  bvbrc    how many samples this run may have in flight at BV-BRC. The assembly is
           40-60 minutes of *waiting* on someone else's cluster, costing this box
           nothing, so this is the pool that decides how fast a batch gets through
           and it is sized for the batch, not for the hardware.
  uploads  how many samples may have their raw FASTQ on local disk at once. Reads
           are streamed to S3 and pulled back only to be handed to BV-BRC (see
           rules/raw.smk); the whole batch's reads on disk at once is the thing
           that design exists to prevent, so this pool stays small.

``--cores`` is then not a CPU budget at all -- it is a job-slot budget, sized so
that it is never the binding constraint and the three pools above are. This is the
whole reason a ten-sample batch assembles in one round rather than three: before,
every one of those 40-minute waits sat in a core slot doing nothing, and only
`cores` of them could wait at a time.
"""

import heapq
import json
import subprocess
import threading
import time
from collections import deque

from workflow.lib import jobs, run_estimates


class PipelineManager:
	def __init__(
		self,
		job_store,
		storage,
		project_root,
		*,
		max_concurrent,
		cores,
		bvbrc_in_flight,
		upload_batch,
		popen_module=None,
	):
		self.job_store = job_store
		self.storage = storage
		self.project_root = project_root
		self.max_concurrent = max_concurrent
		self.cores = cores
		self.bvbrc_in_flight = bvbrc_in_flight
		self.upload_batch = upload_batch
		self.popen_module = popen_module or subprocess
		self.lock = threading.Lock()
		self.processes = {}
		self.queue = deque()
		self.aborted_jobs = set()
		self.expiring_jobs = set()
		# Samples each running job had left to do when it started. Frozen at the
		# start, because it is the size of the run, and the run does not get
		# smaller as it goes: recomputing it live would shrink the estimate by
		# every sample that finished while the elapsed time it is measured against
		# grew by the same work, and the time-left the page shows would race to
		# zero long before the run was done.
		self.pending_at_start = {}

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

	def job_slots(self):
		"""What to hand Snakemake as ``--cores``.

		Not a count of cores. Snakemake will not start a job it cannot give a core
		slot to, whatever that job actually does, so a run that is meant to have
		``bvbrc_in_flight`` samples sitting in a poll loop needs at least that many
		slots or the poll loops queue behind each other again -- which is the bug
		this whole arrangement exists to remove. Sized to cover every job that can
		legitimately be in flight at once, so that the cpu/bvbrc/uploads pools are
		what bind and this never is. The reads groups count double: each fetches an
		R1 and an R2 in parallel."""
		return self.cores + self.bvbrc_in_flight + 2 * self.upload_batch

	def start(self, job_id):
		"""Spawn one run. Caller must hold ``lock``."""
		log_path = jobs.job_log_path(job_id)
		log_path.parent.mkdir(parents=True, exist_ok=True)
		results_dir = jobs.job_results_dir(job_id)
		results_dir.mkdir(parents=True, exist_ok=True)
		self.pending_at_start[job_id] = self.pending_sample_count(job_id)
		jobs.job_run_started_path(job_id).write_text(str(time.time()))
		with log_path.open("w") as pipeline_log_file:
			process = self.popen_module.Popen(
				[
					"snakemake", "--cores", str(self.job_slots()),
					# Every rule costs a core of the box unless it says otherwise; the
					# ones that only wait on BV-BRC say otherwise (resources: cpu=0).
					"--default-resources", "cpu=1",
					"--resources",
					f"cpu={self.cores}",
					f"bvbrc={self.bvbrc_in_flight}",
					f"uploads={self.upload_batch}",
					"--use-conda", "--rerun-incomplete",
					# Re-run a step only when its output is actually missing or stale in time.
					#
					# Snakemake's default triggers also include `code` and `params`, which are
					# wrong for this workflow: most steps here are not local computations that
					# a new release can simply redo. Uploading 48 read pairs to BV-BRC costs
					# half an hour of somebody else's bandwidth, and an assembly is an hour of
					# their cluster. Under the default triggers, editing a comment in a rule
					# file marks every one of those steps out of date, and the next resumed run
					# re-uploads reads that are already sitting in the workspace and reassembles
					# genomes that are already assembled.
					#
					# The cost of this: after a genuine fix to a rule's logic, outputs that
					# already exist are NOT recomputed -- delete them (or --forcerun) to redo
					# them. That is the right trade when the output is an assembly, which does
					# not change because our code did.
					"--rerun-triggers", "mtime",
					"--nolock", "--config", f"job_id={job_id}",
					f"results_dir={results_dir.relative_to(self.project_root())}",
					f"samples_manifest={jobs.job_samples_csv(job_id).relative_to(self.project_root())}",
					# The CPU budget again, this time so the rules can size their own thread
					# counts to it. `--resources cpu=` above only bounds how many of them run
					# at once; it does not reach inside a rule to tell RGI how many threads to
					# ask for, and RGI fails outright when it asks for more than the box has.
					f"pipeline_cores={self.cores}",
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
			if returncode == 0 and not aborted:
				# Only a run that ran to completion says anything about how long the
				# work takes; a crash or an abort would drag every later estimate
				# down. Recorded here, before the queue drains, so the job promoted
				# into this slot is estimated from a history that includes this run.
				self._record_duration(job_id)
			if self.processes.get(job_id) is process:
				self.processes.pop(job_id, None)
			self.pending_at_start.pop(job_id, None)
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

	def pending_sample_count(self, job_id):
		"""Samples a run of this job would actually have to do.

		Not the manifest: re-running is how a user recovers from a failed run, and
		admission clears a job's run markers but leaves its results, so Snakemake
		skips every sample that already finished. A re-run of a 10-sample job that
		died on sample 7 is a 4-sample run, and costs -- and teaches -- what a
		4-sample run costs."""
		sample_rows, _ = self.job_store.read_samples(jobs.job_samples_csv(job_id))
		results_dir = jobs.job_results_dir(job_id)
		return sum(
			1
			for sample_row in sample_rows
			if not run_estimates.sample_is_complete(results_dir, sample_row.get("isolate_id", ""))
		)

	def estimated_seconds(self, job_id):
		"""How long this job's run should take, by the history of past runs. A job
		already running is measured by the work it started with; one still waiting,
		by the work it has now."""
		pending = self.pending_at_start.get(job_id)
		if pending is None:
			pending = self.pending_sample_count(job_id)
		return run_estimates.estimate_seconds(pending, self.bvbrc_in_flight, self.cores)

	def remaining_seconds(self, job_id, now=None):
		"""How much longer a running job has. A run that has already outlasted its
		estimate reports 0 rather than a negative number: the estimate was wrong,
		which is a thing an estimate is allowed to be, and the page says so."""
		started_at = self.job_store.read_run_started(job_id)
		estimate = self.estimated_seconds(job_id)
		if started_at is None:
			return estimate
		return max(0.0, estimate - ((now or time.time()) - started_at))

	def queue_wait_seconds(self, job_id, now=None):
		"""Seconds until a queued job starts. Caller must hold ``lock``.

		Simulates the drain that will actually start it: each running job holds its
		slot until its estimate says it finishes, then every job ahead in the queue
		takes the first slot to come free and holds it for its own estimated run.
		None when the wait is not a wait on slots at all -- the job is not queued, or
		the app is draining for a restart and nothing will start until it comes back.
		"""
		if job_id not in self.queue or self.is_draining():
			return None
		now = now or time.time()
		slot_free_in = [
			self.remaining_seconds(running_id, now)
			for running_id, process in self.processes.items()
			if process.poll() is None
		]
		slot_free_in.extend([0.0] * max(0, self.max_concurrent - len(slot_free_in)))
		heapq.heapify(slot_free_in)
		for queued_id in self.queue:
			starts_in = heapq.heappop(slot_free_in)
			if queued_id == job_id:
				return starts_in
			heapq.heappush(slot_free_in, starts_in + self.estimated_seconds(queued_id))
		return None

	def _record_duration(self, job_id):
		started_at = self.job_store.read_run_started(job_id)
		if started_at is None:
			return
		# What the run worked on, not what the job holds: a re-run that skipped
		# eight of ten samples came back fast because it did little, and charging
		# its length to ten samples' worth of work would record the pipeline as a
		# fraction of its real cost and drag every later estimate down with it.
		run_estimates.record(
			self.pending_at_start.get(job_id, 0),
			self.bvbrc_in_flight,
			self.cores,
			time.time() - started_at,
		)

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
					"queue_wait_seconds": self.queue_wait_seconds(job_id),
					"estimated_seconds": self.estimated_seconds(job_id),
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
