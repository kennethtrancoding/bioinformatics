"""Durable, per-job filesystem state.

Keeping this small storage boundary separate from Flask makes job data usable by
the web app, background services, and command-line maintenance code without
each caller reimplementing atomic CSV/JSON writes.
"""

import csv
import json
import os
import time
from pathlib import Path

from workflow.helpers import jobs

SAMPLE_FIELDS = ["isolate_id", "R1_path", "R2_path", "description"]
UPLOAD_METHOD_LABELS = {
	"pair": "paired upload",
	"folder": "folder import",
	"cloud": "cloud import",
}


class JobStore:
	"""Read and write job state using the path helpers in :mod:`jobs`.

	The helpers intentionally resolve ``jobs.PROJECT_ROOT`` at call time. This
	keeps the store configurable and lets tests use an isolated filesystem.
	"""

	def read_samples(self, samples_csv):
		samples_csv = Path(samples_csv)
		if not samples_csv.exists():
			return [], SAMPLE_FIELDS
		with samples_csv.open(newline="") as file_handle:
			reader = csv.DictReader(file_handle)
			return list(reader), reader.fieldnames or SAMPLE_FIELDS

	def write_samples(self, samples_csv, sample_rows, fieldnames=SAMPLE_FIELDS):
		samples_csv = Path(samples_csv)
		samples_csv.parent.mkdir(parents=True, exist_ok=True)
		temporary_path = samples_csv.with_suffix(".csv.tmp")
		with temporary_path.open("w", newline="") as file_handle:
			writer = csv.DictWriter(file_handle, fieldnames=fieldnames)
			writer.writeheader()
			writer.writerows(sample_rows)
		os.replace(temporary_path, samples_csv)

	def upsert_sample(self, job_id, isolate_id, r1_path, r2_path):
		samples_csv = jobs.job_samples_csv(job_id)
		sample_rows, fieldnames = self.read_samples(samples_csv)
		sample_rows = [
			sample_row for sample_row in sample_rows if sample_row.get("isolate_id") != isolate_id
		]
		sample_rows.append(
			{"isolate_id": isolate_id, "R1_path": r1_path, "R2_path": r2_path, "description": ""}
		)
		self.write_samples(samples_csv, sample_rows, fieldnames)

	def remove_sample(self, job_id, isolate_id):
		samples_csv = jobs.job_samples_csv(job_id)
		sample_rows, fieldnames = self.read_samples(samples_csv)
		self.write_samples(
			samples_csv,
			[
				sample_row
				for sample_row in sample_rows
				if sample_row.get("isolate_id") != isolate_id
			],
			fieldnames,
		)

	def read_uploads(self, job_id):
		try:
			uploads = json.loads(jobs.job_uploads_path(job_id).read_text())
		except (OSError, ValueError):
			return []
		return uploads if isinstance(uploads, list) else []

	def record_upload(self, job_id, method, started_at, added, updated):
		upload_entry = {
			"method": method,
			"label": UPLOAD_METHOD_LABELS.get(method, method),
			"finished_at": time.time(),
			"seconds": round(time.time() - started_at, 1),
			"added": list(added),
			"updated": list(updated),
		}
		try:
			uploads_path = jobs.job_uploads_path(job_id)
			uploads_path.parent.mkdir(parents=True, exist_ok=True)
			temporary_path = uploads_path.with_suffix(".json.tmp")
			temporary_path.write_text(
				json.dumps(self.read_uploads(job_id) + [upload_entry], indent=2)
			)
			os.replace(temporary_path, uploads_path)
		except OSError as exception:
			print(f"[uploads] could not record upload for job {job_id}: {exception}")
		return upload_entry

	def read_run_admitted(self, job_id):
		"""When this job was admitted to run -- when its wait for a slot began -- or
		None if that was never recorded."""
		try:
			return float(jobs.job_run_admitted_path(job_id).read_text())
		except (OSError, ValueError):
			return None

	def read_run_started(self, job_id):
		"""When this job's pipeline process started, or None if it never did."""
		try:
			return float(jobs.job_run_started_path(job_id).read_text())
		except (OSError, ValueError):
			return None

	def read_status(self, job_id):
		try:
			return json.loads(jobs.job_status_path(job_id).read_text())
		except (OSError, ValueError):
			return None

	def write_status(self, job_id, success, *, error=None):
		status_path = jobs.job_status_path(job_id)
		status_path.parent.mkdir(parents=True, exist_ok=True)
		status_payload = {"done": True, "success": success, "finished_at": time.time()}
		if error:
			status_payload["error"] = error
		status_path.write_text(json.dumps(status_payload))

	def reset_run_markers(self, job_id):
		results_dir = jobs.job_results_dir(job_id)
		results_dir.mkdir(parents=True, exist_ok=True)
		jobs.job_status_path(job_id).unlink(missing_ok=True)
		jobs.job_first_viewed_path(job_id).unlink(missing_ok=True)
		jobs.job_run_admitted_path(job_id).unlink(missing_ok=True)
		jobs.job_run_started_path(job_id).unlink(missing_ok=True)

	def mark_first_viewed(self, job_id):
		viewed_marker_path = jobs.job_first_viewed_path(job_id)
		if not viewed_marker_path.exists():
			viewed_marker_path.parent.mkdir(parents=True, exist_ok=True)
			viewed_marker_path.touch()
