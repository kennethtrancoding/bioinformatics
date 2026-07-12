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

# Unambiguous alphabet
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
