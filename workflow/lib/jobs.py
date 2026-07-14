"""
Per-batch job IDs.

Every upload (single pair or bulk folder import) is assigned an opaque
alphanumeric job ID. The ID is the only credential gating access to that
batch's data and results so it must
be unguessable (drawn from `secrets`, not `random`) and never accepted from a
caller without being validated against JOB_ID_RE first.
"""

import re
import secrets
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent

# Exclude characters that are easy to misread.
_ALPHABET = "ABCDEFGHJKMNPQRSTUVWXYZ23456789"
_LENGTH = 12

JOB_ID_RE = re.compile(rf"^[{_ALPHABET}]{{{_LENGTH}}}$")

# Isolate IDs are derived from uploaded filenames and used in paths.
ISOLATE_ID_RE = re.compile(r"^[A-Za-z0-9_.-]+$")


def generate_job_id() -> str:
	return "".join(secrets.choice(_ALPHABET) for character_index in range(_LENGTH))


def is_valid_job_id(job_id) -> bool:
	return isinstance(job_id, str) and bool(JOB_ID_RE.fullmatch(job_id))


def is_valid_isolate_id(isolate_id) -> bool:
	return (
		isinstance(isolate_id, str)
		and bool(ISOLATE_ID_RE.fullmatch(isolate_id))
		and isolate_id not in (".", "..")
	)


def job_data_dir(job_id: str) -> Path:
	return PROJECT_ROOT / "data" / "raw_fastq" / job_id


def job_results_dir(job_id: str) -> Path:
	return PROJECT_ROOT / "results" / job_id


def job_config_dir(job_id: str) -> Path:
	return PROJECT_ROOT / "config" / "jobs" / job_id


def job_samples_csv(job_id: str) -> Path:
	return job_config_dir(job_id) / "samples.csv"


def job_uploads_path(job_id: str) -> Path:
	"""Log of every upload that added samples to this job: how they arrived, when,
	and how long each took. A job can be filled by several uploads through several
	different methods, so this is a list, not a single timestamp."""
	return job_config_dir(job_id) / "uploads.json"


def job_log_path(job_id: str) -> Path:
	return PROJECT_ROOT / "logs" / f"{job_id}.log"


def job_status_path(job_id: str) -> Path:
	"""Persisted terminal status for later job lookups."""
	return job_results_dir(job_id) / ".run_status.json"


def job_run_started_path(job_id: str) -> Path:
	"""Epoch timestamp written when the pipeline process starts."""
	return job_results_dir(job_id) / ".run_started"


def job_first_viewed_path(job_id: str) -> Path:
	"""Marker that starts the viewed-result retention period."""
	return job_results_dir(job_id) / ".first_viewed"


def job_pinned_path(job_id: str) -> Path:
	"""Marker that exempts a job from result retention cleanup."""
	return job_results_dir(job_id) / ".pinned"


def job_api_endpoints_path(job_id: str) -> Path:
	"""Per-job API endpoint overrides, allowing different jobs to use
	different services (e.g., test vs. production BV-BRC instances)."""
	return job_config_dir(job_id) / "api_endpoints.json"


def job_token_path(job_id: str) -> Path:
	"""Private BV-BRC bearer token belonging only to this job."""
	return job_config_dir(job_id) / ".bvbrc_token"


# The two paths below are app-wide rather than per-job, but they live under
# config/jobs/ because that is the directory mounted on a persistent volume (see
# deploy/bioinformatics.service). The container filesystem is ephemeral, so
# anything that has to outlive a restart has to sit on a volume -- and outliving a
# restart is the entire point of both of these.


def pipeline_queue_path() -> Path:
	"""FIFO of jobs admitted but not yet started.

	The queue is held in memory, so a restart used to drop every job waiting in
	it -- and drop it silently, because admission clears a job's run markers
	before queueing, leaving nothing on disk to say the run was ever coming.
	Persisting the queue is what lets a restart resume instead of forget."""
	return PROJECT_ROOT / "config" / "jobs" / ".pipeline_queue.json"


def run_history_path() -> Path:
	"""Durations of recent successful runs, used to estimate how long a queued
	run will wait and how much longer a running one has to go. Kept next to the
	queue, and on the same volume, because an estimate built from history is
	worth nothing if a restart throws the history away."""
	return PROJECT_ROOT / "config" / "jobs" / ".run_history.json"


def drain_flag_path() -> Path:
	"""Set by the host while it prepares to restart the service (see
	deploy/refresh-databases.sh). While it exists, runs queue instead of starting,
	so the in-flight set drains to empty and the restart lands on an idle app
	rather than killing a run mid-assembly. Cleared on boot."""
	return PROJECT_ROOT / "config" / "jobs" / ".drain"
