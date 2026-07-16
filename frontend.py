import hmac
import os
import re
import shutil
import signal
import subprocess
import tempfile
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse

from flask import (
	Flask,
	Response,
	abort,
	jsonify,
	redirect,
	render_template,
	request,
	send_file,
	url_for,
)
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
from werkzeug.middleware.proxy_fix import ProxyFix
from werkzeug.utils import secure_filename

from workflow.helpers import api_registry, s3_storage
from workflow.helpers.bvbrc_client import BVBRCClient
from workflow.helpers.import_service import ImportService

# Cloud import is disabled; these two are only needed by the commented-out
# /cloud-import routes below.
# from workflow.helpers import cloud_import
# from workflow.helpers.import_service import CloudImportManager
from workflow.helpers.job_store import SAMPLE_FIELDS, JobStore
from workflow.helpers.jobs import (
	generate_job_id,
	is_valid_isolate_id,
	is_valid_job_id,
	job_data_dir,
	job_results_dir,
	job_samples_csv,
)
from workflow.helpers.pipeline_manager import PipelineManager
from workflow.helpers.preprocess import validate_sample_files, verify_file_md5
from workflow.helpers.retention import RetentionService
from workflow.helpers.utils import stream_directory_zip

PROJECT_ROOT = Path(__file__).resolve().parent

# Files created by the web process can contain credentials or genomic data.
os.umask(0o077)

app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY") or os.urandom(32)

# Behind a reverse proxy (deploy/Caddyfile) every request reaches Flask from the
# proxy's own address, which would collapse the per-IP rate limit below into one
# bucket shared by all users. ProxyFix recovers the real client IP from
# X-Forwarded-For. Trusting more hops than actually sit in front of us would let a
# client forge that header and evade the limit, so this stays off unless the
# deployment declares how many proxies it has.
# `or 0` so a blank value (TRUSTED_PROXY_HOPS=) reads as "no proxy" rather than
# crashing on int(""); systemd passes the variable through empty when app.env
# declares it without a value.
_TRUSTED_PROXY_HOPS = int(os.environ.get("TRUSTED_PROXY_HOPS") or 0)
if _TRUSTED_PROXY_HOPS:
	app.wsgi_app = ProxyFix(
		app.wsgi_app,
		x_for=_TRUSTED_PROXY_HOPS,
		x_proto=_TRUSTED_PROXY_HOPS,
		x_host=_TRUSTED_PROXY_HOPS,
	)

# Two things here are guessable and grant access to results: a job ID, and the
# Basic Auth password. Both are rate limited per client address. In-memory
# counters are sufficient while Gunicorn is restricted to one worker.
#
# The default limit costs a request only when it is answered with a 401, so it
# bounds password guessing without touching legitimate traffic -- an authorized
# browser polling a run's status never spends from this bucket, while an attacker
# working through a wordlist spends one per attempt and is refused after ten a
# minute. Guessing a 401 apart from a 429 does not help them: both refuse.
limiter = Limiter(
	get_remote_address,
	app=app,
	storage_uri="memory://",
	default_limits=["10 per minute"],
	default_limits_deduct_when=lambda response: response.status_code == 401,
)

# Folder uploads can exceed Werkzeug's default 1,000 form parts.
app.config["MAX_FORM_PARTS"] = 100_000
app.config["MAX_CONTENT_LENGTH"] = None

# Set APP_PASSWORD to enable site-wide HTTP Basic Auth.
_APP_USERNAME = os.environ.get("APP_USERNAME", "mentor")
_APP_PASSWORD = os.environ.get("APP_PASSWORD")


@app.before_request
def _require_login():
	"""Gate every route behind HTTP Basic Auth when APP_PASSWORD is configured."""
	if not _APP_PASSWORD:
		return
	if request.path == "/api/health":
		return
	basic_auth_credentials = request.authorization
	if (
		not basic_auth_credentials
		or basic_auth_credentials.username != _APP_USERNAME
		or not hmac.compare_digest(basic_auth_credentials.password or "", _APP_PASSWORD)
	):
		return Response(
			"Authentication required.",
			401,
			{"WWW-Authenticate": 'Basic realm="Bioinformatics Pipeline"'},
		)


@app.before_request
def _reject_cross_site_mutations():
	"""Job IDs are bearer credentials; do not accept them from cross-site forms."""
	if request.method not in {"POST", "PUT", "PATCH", "DELETE"}:
		return
	if request.headers.get("Sec-Fetch-Site") == "cross-site":
		abort(403, description="Cross-site request rejected.")
	request_origin = request.headers.get("Origin")
	if request_origin and urlparse(request_origin).netloc != request.host:
		abort(403, description="Cross-site request rejected.")


@app.after_request
def _security_headers(response):
	response.headers.setdefault("X-Content-Type-Options", "nosniff")
	response.headers.setdefault("X-Frame-Options", "DENY")
	# "same-origin", not "no-referrer": under no-referrer a browser sends
	# "Origin: null" on a form-navigation POST even when it is same-origin (Fetch
	# spec: a non-CORS POST from a no-referrer document gets an opaque origin), so
	# the settings forms below would trip the cross-site check above. same-origin
	# still strips the Referer entirely on cross-origin requests, which is what
	# keeps the job ID in the query string from leaking off-site.
	response.headers.setdefault("Referrer-Policy", "same-origin")
	response.headers.setdefault("Permissions-Policy", "camera=(), microphone=(), geolocation=()")
	if request.path.startswith(("/job/", "/status", "/results/", "/cloud-import")):
		# Not setdefault: send_file/send_from_directory already stamp their own
		# Cache-Control (a conditional "no-cache", meant for static assets) by
		# the time this after_request hook runs, so setdefault would be a no-op
		# here. These routes serve per-job genomic data gated only by the job
		# ID, so the header must force "no-store" outright, not just default to it.
		response.headers["Cache-Control"] = "no-store"
	return response


DATA_ROOT = PROJECT_ROOT / "data" / "raw_fastq"
RESULTS_ROOT = PROJECT_ROOT / "results"
DATA_ROOT.mkdir(parents=True, exist_ok=True)
RESULTS_ROOT.mkdir(parents=True, exist_ok=True)

# Limit memory-heavy pipeline processes; additional jobs wait in FIFO order.
MAX_CONCURRENT_PIPELINES = max(1, int(os.environ.get("MAX_CONCURRENT_PIPELINES", "2")))
# The cores the box has, and so the budget for the rules that actually compute.
# Capped by the cores that actually exist: a value above them is not a preference
# the box can honour, it is a run that fails an hour in, because RGI rejects a
# thread count higher than the CPUs it can see rather than scaling down.
PIPELINE_CORES = max(1, min(int(os.environ.get("PIPELINE_CORES", "4")), os.cpu_count() or 1))
# Samples a run may have assembling at BV-BRC at once. This is not a hardware
# limit -- the assembly runs on BV-BRC's cluster and costs this box nothing but a
# poll loop -- so it is sized for the batch, and it is what decides how quickly a
# batch of samples gets through. Each in-flight sample is a small idle Python
# process here (tens of MB), so it is bounded rather than unlimited: a stray
# 200-sample job should not open 200 poll loops or dump 200 jobs on a shared
# public service at once. Raise it if you routinely run larger batches.
BVBRC_MAX_IN_FLIGHT = max(1, int(os.environ.get("BVBRC_MAX_IN_FLIGHT", "12")))
# Samples that may hold raw FASTQ on local disk at once, feeding it to BV-BRC.
# Small on purpose: peak raw disk is this many pairs, not the batch's (rules/raw.smk).
BVBRC_UPLOAD_BATCH = max(1, int(os.environ.get("BVBRC_UPLOAD_BATCH", "4")))

_job_store = JobStore()
_pipeline_manager = PipelineManager(
	_job_store,
	s3_storage,
	lambda: PROJECT_ROOT,
	max_concurrent=MAX_CONCURRENT_PIPELINES,
	cores=PIPELINE_CORES,
	bvbrc_in_flight=BVBRC_MAX_IN_FLIGHT,
	upload_batch=BVBRC_UPLOAD_BATCH,
	popen_module=subprocess,
)
# Compatibility aliases for operational tooling and the existing test suite.
_pipeline_lock = _pipeline_manager.lock
_pipeline_processes = _pipeline_manager.processes
_pipeline_queue = _pipeline_manager.queue
_pipeline_aborted_jobs = _pipeline_manager.aborted_jobs
_expiring_jobs = _pipeline_manager.expiring_jobs


def _persist_pipeline_queue():
	_pipeline_manager.persist_queue()


def _is_draining():
	return _pipeline_manager.is_draining()


# A job can be filled by several uploads, and by different methods at the same
# time -- a folder import and a cloud pull can both be adding samples to one job
# while the user drops a single pair on top. Every one of those is a
# read-modify-write of the same samples.csv, so they take the job's lock: without
# it two adds that overlap silently lose one of the two sets of samples.
# Per-job, not global, so uploads to unrelated jobs still run in parallel.
_job_locks_guard = threading.Lock()
_job_locks = {}


def _job_lock(job_id):
	with _job_locks_guard:
		return _job_locks.setdefault(job_id, threading.Lock())


_import_service = ImportService(s3_storage, _job_store, lambda: PROJECT_ROOT, _job_lock)


def _job_or_400(job_id):
	if not is_valid_job_id(job_id):
		abort(400, description="Malformed job ID.")


def _resolve_result_dir(job_id, isolate_id):
	"""Map a (job_id, isolate_id) pair to its results folder, guarding against
	path traversal and rejecting anything that isn't our own generated job-ID
	format or a boring isolate identifier."""
	if not is_valid_job_id(job_id) or not is_valid_isolate_id(isolate_id):
		return None
	results_root = job_results_dir(job_id).resolve()
	resolved_path = (results_root / isolate_id).resolve()
	if not resolved_path.is_relative_to(RESULTS_ROOT.resolve()):
		return None
	if not resolved_path.is_dir():
		return None
	return resolved_path


_CSV_FIELDS = SAMPLE_FIELDS


def _read_samples(samples_csv):
	return _job_store.read_samples(samples_csv)


def _write_samples(samples_csv, sample_rows, fieldnames=_CSV_FIELDS):
	_job_store.write_samples(samples_csv, sample_rows, fieldnames)


def _upsert_sample(samples_csv, isolate_id, r1_path, r2_path):
	_job_store.upsert_sample(Path(samples_csv).parent.name, isolate_id, r1_path, r2_path)


def _remove_sample(samples_csv, isolate_id):
	_job_store.remove_sample(Path(samples_csv).parent.name, isolate_id)


def _read_uploads(job_id):
	return _job_store.read_uploads(job_id)


def _record_upload(job_id, method, started_at, added, updated):
	return _job_store.record_upload(job_id, method, started_at, added, updated)


def _extract_error_summary(log_path, max_chars=800):
	return _pipeline_manager._extract_error_summary(log_path, max_chars)


def _write_job_status(job_id, success, aborted=False, interrupted=False):
	_pipeline_manager._write_status(job_id, success, aborted=aborted, interrupted=interrupted)


def _reset_run_markers(job_id):
	_job_store.reset_run_markers(job_id)


def _start_pipeline(job_id):
	_pipeline_manager.cores = PIPELINE_CORES
	_pipeline_manager.bvbrc_in_flight = BVBRC_MAX_IN_FLIGHT
	_pipeline_manager.upload_batch = BVBRC_UPLOAD_BATCH
	_pipeline_manager.start(job_id)


def _drain_pipeline_queue():
	_pipeline_manager.drain()


def _claim_raw_for_job(job_id):
	_pipeline_manager.claim_raw(job_id)


def _return_raw_to_the_clock(job_id):
	_pipeline_manager.return_raw(job_id)


def _watch_pipeline(job_id, pipeline_process):
	_pipeline_manager.watch(job_id, pipeline_process)


def _read_job_status(job_id):
	return _job_store.read_status(job_id)


def _mark_first_viewed(job_id):
	_job_store.mark_first_viewed(job_id)


def _job_snapshot(job_id):
	"""Everything the lookup box needs to show for one job: its samples, each
	isolate's result availability, and pipeline status (live if this job is
	currently running, persisted from disk otherwise -- so a crash stays
	visible on every later lookup, not just to a tab that was open when it
	happened)."""
	samples, sample_fieldnames = _read_samples(job_samples_csv(job_id))
	results_dir = job_results_dir(job_id)
	results_by_isolate = {}
	if results_dir.is_dir():
		for result_entry in sorted(results_dir.iterdir()):
			# "logs" is the pipeline's own log directory, not an isolate.
			if not result_entry.is_dir() or result_entry.name == "logs":
				continue
			results_by_isolate[result_entry.name] = {
				"has_report": (result_entry / "summary" / "report.html").is_file(),
			}
	if not results_by_isolate:
		# Local results are gone (retention sweep, or a fresh instance after
		# replacement) -- fall back to what was durably uploaded to S3.
		for isolate_id in s3_storage.list_isolates(job_id):
			results_by_isolate[isolate_id] = {"has_report": True}

	started_at = _job_store.read_run_started(job_id)

	run_status = None
	with _pipeline_lock:
		pipeline_process = _pipeline_processes.get(job_id)
		queue_position = _pipeline_queue.index(job_id) + 1 if job_id in _pipeline_queue else None
		# Both estimates are read under the lock because both are computed from the
		# live queue and process table, which any finishing run may rewrite.
		queue_wait_seconds = _pipeline_manager.queue_wait_seconds(job_id)
		estimated_seconds = _pipeline_manager.estimated_seconds(job_id)
	if queue_position is not None:
		# Admitted but not started. Not done, and deliberately has no started_at:
		# the run hasn't begun, so there is no elapsed time to report yet -- only an
		# estimate of how long it will sit here, which is null while draining.
		run_status = {
			"done": False,
			"success": None,
			"queued": True,
			"queue_position": queue_position,
			"queue_wait_seconds": queue_wait_seconds,
			"estimated_seconds": estimated_seconds,
			"started_at": None,
		}
	elif pipeline_process is not None and pipeline_process.poll() is None:
		# started_at and estimated_seconds together are what let the page count down
		# the time left without asking the server again every second.
		run_status = {
			"done": False,
			"success": None,
			"queued": False,
			"estimated_seconds": estimated_seconds,
			"started_at": started_at,
			"finished_at": None,
		}
	else:
		persisted_status = _read_job_status(job_id)
		if persisted_status is not None:
			# started_at and finished_at together are what let the page report how
			# long the run actually took, rather than only that it ended.
			run_status = {
				"done": True,
				"success": persisted_status.get("success"),
				"error": persisted_status.get("error"),
				"started_at": started_at,
				"finished_at": persisted_status.get("finished_at"),
			}
			_mark_first_viewed(job_id)

	return {
		"job_id": job_id,
		"samples": samples,
		"uploads": _read_uploads(job_id),
		"results": [
			{"isolate_id": isolate_id, **isolate_result_info}
			for isolate_id, isolate_result_info in results_by_isolate.items()
		],
		"run_status": run_status,
		"has_master_report": (results_dir / "master_report.csv").is_file()
		or s3_storage.object_exists(s3_storage.key_for(job_id, "master_report.csv")),
	}


# One-time migration: fold in any pre-job-ID data (a global config/samples.csv
# + flat data/raw_fastq/ + flat results/<isolate>/) into a freshly generated
# job ID, so upgrading this app doesn't strand already-imported samples.
_MIGRATION_MARKER = PROJECT_ROOT / "data" / ".migrated"
_LEGACY_SAMPLES_CSV = PROJECT_ROOT / "config" / "samples.csv"


def _migrate_legacy_samples():
	if _MIGRATION_MARKER.exists() or not _LEGACY_SAMPLES_CSV.exists():
		_MIGRATION_MARKER.parent.mkdir(parents=True, exist_ok=True)
		_MIGRATION_MARKER.touch(exist_ok=True)
		return

	sample_rows, legacy_sample_fieldnames = _read_samples(_LEGACY_SAMPLES_CSV)
	if not sample_rows:
		_MIGRATION_MARKER.touch(exist_ok=True)
		return

	job_id = generate_job_id()
	new_data_dir = job_data_dir(job_id)
	new_results_dir = job_results_dir(job_id)
	new_data_dir.mkdir(parents=True, exist_ok=True)

	migrated_rows = []
	for sample_row in sample_rows:
		isolate_id = sample_row.get("isolate_id")
		for read_path_key in ("R1_path", "R2_path"):
			legacy_file_path = PROJECT_ROOT / sample_row[read_path_key]
			if legacy_file_path.is_file():
				migrated_file_path = new_data_dir / legacy_file_path.name
				shutil.move(str(legacy_file_path), str(migrated_file_path))
				sample_row[read_path_key] = str(migrated_file_path.relative_to(PROJECT_ROOT))
		if isolate_id:
			legacy_results_dir = RESULTS_ROOT / isolate_id
			if legacy_results_dir.is_dir():
				new_results_dir.mkdir(parents=True, exist_ok=True)
				shutil.move(str(legacy_results_dir), str(new_results_dir / isolate_id))
		migrated_rows.append(sample_row)

	_write_samples(job_samples_csv(job_id), migrated_rows)
	_LEGACY_SAMPLES_CSV.unlink()
	_MIGRATION_MARKER.parent.mkdir(parents=True, exist_ok=True)
	_MIGRATION_MARKER.write_text(job_id + "\n")
	print(f"[migration] {len(migrated_rows)} pre-existing sample(s) moved under job ID: {job_id}")


_migrate_legacy_samples()


# Data retention: raw FASTQ is deleted by a dedicated Snakemake rule as soon as
# a sample no longer needs it (see workflow/rules/cleanup.smk). A finished
# job's results are deleted when *either*:
#   - 3 hours after the user first views them (checked via job_first_viewed_path)
#   - 7 days after the pipeline completed (checked via job_status_path)
# Whichever comes first. This gives actively-downloading users a 3-hour window
# and ensures results never linger unwatched beyond 7 days.
_VIEW_TTL_SECONDS = 3 * 60 * 60  # 3 hours after first view
_MAX_TTL_SECONDS = 7 * 24 * 60 * 60  # 7 days from completion
_RETENTION_SWEEP_INTERVAL = 15 * 60
_retention_service = RetentionService(
	_pipeline_manager,
	s3_storage,
	lambda job_id: BVBRCClient(job_id=job_id),
	lambda: PROJECT_ROOT,
	lambda: DATA_ROOT,
	lambda: RESULTS_ROOT,
)
_retention_service.view_ttl_seconds = _VIEW_TTL_SECONDS
_retention_service.max_ttl_seconds = _MAX_TTL_SECONDS
_retention_service.sweep_interval = _RETENTION_SWEEP_INTERVAL


def _job_is_live(job_id):
	return job_id in _pipeline_manager.processes or job_id in _pipeline_manager.queue


def _claim_for_expiry(job_id):
	return _retention_service._claim_finished(job_id)


def _claim_for_expiry_of_unrun_job(job_id):
	return _retention_service._claim_unrun(job_id)


def _finish_expiry(job_id):
	_retention_service._finish(job_id)


def _expire_job_results(job_id, job_results_directory, current_time):
	_retention_service._expire_results(job_id, job_results_directory, current_time)


def _sweep_expired_results():
	_retention_service.sweep()


def _retention_loop():
	_retention_service._loop()


def _reconcile_interrupted_runs():
	_pipeline_manager.reconcile(RESULTS_ROOT)


def run_startup_recovery():
	"""Entry point for the two ways this app is served: gunicorn's post_worker_init
	hook in production (see gunicorn.conf.py) and the __main__ block locally.

	Deliberately NOT called at import. Importing this module must stay free of side
	effects on the real job tree -- tests import frontend before repointing
	PROJECT_ROOT at a temp directory (tests/_isolation.py), so reconciling at import
	time would scan the developer's actual results/ and mark whatever run they had
	in progress as failed."""
	try:
		_reconcile_interrupted_runs()
		_retention_service.start()
	except Exception as exception:
		# Never let recovery keep the app from booting: a frontend that will not
		# start is strictly worse than one whose job states are stale.
		print(f"[pipeline] startup reconciliation failed: {exception}")


@app.route("/")
def analysis():
	databases_updated_at = _reference_databases_updated_at()
	return render_template(
		"index.html",
		db_last_updated=_human_readable_time_ago(databases_updated_at) if databases_updated_at else None,
		db_last_updated_iso=databases_updated_at.isoformat(timespec="seconds")
		if databases_updated_at
		else None,
	)


@app.route("/job/new", methods=["POST"])
@limiter.limit("20/minute", override_defaults=False)
def create_job():
	"""Reserve one empty job before concurrent browser uploads begin."""
	job_id = generate_job_id()
	_write_samples(job_samples_csv(job_id), [])
	return jsonify({"job_id": job_id}), 201


@app.route("/settings", methods=["GET", "POST"])
def settings():
	job_id = request.values.get("job_id", "").strip().upper()
	if not job_id:
		return render_template("settings.html", job_id=None, endpoints=[], settings_saved=False)
	_job_or_400(job_id)
	if not job_samples_csv(job_id).is_file():
		abort(404)
	if request.method == "POST":
		api_registry.save_job_overrides(job_id, request.form)
		username = request.form.get("username", "").strip()
		password = request.form.get("password", "")
		login_failed = bool(username or password) and (
			not username or not password or not BVBRCClient(job_id=job_id).login(username, password)
		)
		# Post/Redirect/Get. Without it the browser is left sitting on the result of
		# a POST to a bare /settings: a URL with no job in it, so every link on the
		# page has nothing to go back to, and a reload re-submits the form (and the
		# password) rather than re-rendering. Redirecting to /settings?job_id=... ends
		# every save on a plain GET URL that carries the job and can be reloaded.
		return redirect(
			url_for(
				"settings",
				job_id=job_id,
				saved=1,
				login_error=1 if login_failed else None,
			)
		)
	return render_template(
		"settings.html",
		job_id=job_id,
		endpoints=list(api_registry.load_endpoints(job_id=job_id).values()),
		defaults=api_registry.DEFAULT_ENDPOINTS,
		settings_saved=request.args.get("saved") == "1",
		login_error=(
			"BV-BRC authentication failed." if request.args.get("login_error") == "1" else None
		),
	)


@app.route("/settings/reset", methods=["POST"])
def settings_reset():
	job_id = request.form.get("job_id", "").strip().upper()
	_job_or_400(job_id)
	if not job_samples_csv(job_id).is_file():
		abort(404)
	api_registry.save_job_overrides(job_id, {})
	return redirect(url_for("settings", job_id=job_id))


def _reference_databases_updated_at():
	"""When the pipeline's reference databases (CARD, MGEdb, AMRProt) were last
	built, or None if that can't be determined.

	They are image content -- refreshed only by the weekly image rebuild in
	deploy/refresh-databases.sh, never in place (see DATABASE_UPDATES.md) -- so the
	build stamp the Dockerfile writes beside them is the true "last updated" time.
	Falls back to the AMR catalog's own build marker for local/CLI setups that
	never went through a Docker build, and returns None when neither exists so the
	page simply omits the line rather than showing a wrong or empty date."""
	build_stamp = PROJECT_ROOT / "resources" / "blastdb" / ".db_built_at"
	try:
		stamped = build_stamp.read_text().strip()
		if stamped:
			# The Dockerfile writes UTC as ...Z; fromisoformat only learned to parse
			# a trailing Z in 3.11, so normalise it for the conda base's interpreter.
			if stamped.endswith("Z"):
				stamped = stamped[:-1] + "+00:00"
			return datetime.fromisoformat(stamped)
	except (OSError, ValueError):
		pass

	amr_marker = PROJECT_ROOT / "resources" / "blastdb" / "amr" / ".ready"
	try:
		return datetime.fromtimestamp(amr_marker.stat().st_mtime, tz=timezone.utc)
	except OSError:
		return None


def _human_readable_time_ago(dt):
	"""Convert a datetime to a human-readable 'time ago' string."""
	now = datetime.now(timezone.utc) if dt.tzinfo else datetime.now()
	diff = now - dt
	seconds = diff.total_seconds()

	if seconds < 60:
		return "just now"
	elif seconds < 3600:
		minutes = int(seconds / 60)
		return f"{minutes}m ago"
	elif seconds < 86400:
		hours = int(seconds / 3600)
		return f"{hours}h ago"
	elif seconds < 604800:
		days = int(seconds / 86400)
		return f"{days}d ago"
	else:
		weeks = int(seconds / 604800)
		return f"{weeks}w ago"


@app.route("/api/health")
def api_health():
	job_id = request.args.get("job_id")
	if job_id:
		_job_or_400(job_id)
		if not job_samples_csv(job_id).is_file():
			abort(404)
	with _pipeline_lock:
		running_count = sum(
			1
			for pipeline_process in _pipeline_processes.values()
			if pipeline_process.poll() is None
		)
		queued_count = len(_pipeline_queue)
	return jsonify(
		{
			"services": api_registry.check_all(job_id=job_id),
			# How deploy/refresh-databases.sh knows when it is safe to restart.
			# Counts only, never job IDs: this route is deliberately exempt from
			# auth (see the before_request hook) and a job ID is a credential.
			"pipelines": {
				"draining": _is_draining(),
				"running": running_count,
				"queued": queued_count,
			},
		}
	)


def _save_upload(job_id, upload_file, destination_path):
	return _import_service.save_upload(job_id, upload_file, destination_path)


def _release_local_raw(job_id, *paths):
	_import_service.release_local_raw(job_id, *paths)


def _discard_raw_upload(job_id, *paths):
	_import_service.discard_raw_upload(job_id, *paths)


def _backup_raw_files_to_s3(job_id, raw_paths):
	return _import_service.backup_raw_files(job_id, raw_paths)


def _raw_paths_for_isolates(job_id, isolate_ids):
	return _import_service.raw_paths_for_isolates(job_id, isolate_ids, job_samples_csv(job_id))


def _job_is_busy(job_id):
	return _pipeline_manager.is_busy(job_id)


def _frozen_samples_response(job_id):
	"""A job's samples may be added to or deleted from only until its pipeline is
	first run. Returns an ``(response, status)`` to send back when they are frozen,
	or None when edits are still allowed.

	Two windows are closed, for two reasons. While a run is queued or running, the
	manifest and the FASTQ it points at are being read, so a change is either
	silently ignored or read half-written. Once a run has finished -- completed OR
	failed -- the samples are the inputs that produced its results, and editing them
	would leave results that no longer correspond to their inputs. A different set of
	files is a different job: start a new one."""
	if _job_is_busy(job_id):
		return (
			jsonify(
				{
					"error": "This job's pipeline is running or queued. Wait for it "
					"to finish before changing its samples."
				}
			),
			409,
		)
	if _read_job_status(job_id) is not None:
		return (
			jsonify(
				{
					"error": "This job's pipeline has already run, so its samples are "
					"locked. Start a new job to analyze a different set of files."
				}
			),
			409,
		)
	return None


def _resolve_upload_job(requested_job_id):
	"""The job an upload lands in: the one the caller named, or a brand new one.

	Naming a job is how a batch gets filled by more than one upload, and by more
	than one method -- a folder import, then a cloud pull, then a stray pair, all
	into the same job ID. Returns (job_id, error_response); exactly one is None.
	"""
	requested_job_id = (requested_job_id or "").strip().upper()
	if not requested_job_id:
		return generate_job_id(), None
	if not is_valid_job_id(requested_job_id):
		return None, (jsonify({"error": "Malformed job ID."}), 400)
	if not job_samples_csv(requested_job_id).is_file():
		return None, (jsonify({"error": "Unknown job ID."}), 404)
	frozen = _frozen_samples_response(requested_job_id)
	if frozen is not None:
		return None, frozen
	return requested_job_id, None


def _authenticate_job(job_id, username, password):
	"""Log this job in to BV-BRC when credentials came with the request. Returns
	an error message, or None when there is nothing to do or it worked."""
	if not username or not password:
		return None
	if not BVBRCClient(job_id=job_id).login(username, password):
		return "BV-BRC authentication failed"
	return None


def _admit_pipeline_run(job_id):
	"""Start or queue a run. Shared by /run and by the auto-run option on every
	upload path.

	Returns (payload, http_status)."""
	authentication_error = _authenticate_job(
		job_id,
		request.form.get("username", "").strip(),
		request.form.get("password", "").strip(),
	)

	if authentication_error:
		return jsonify({"error": authentication_error}), 401

	_pipeline_manager.max_concurrent = MAX_CONCURRENT_PIPELINES
	return _pipeline_manager.admit(job_id, lambda: BVBRCClient(job_id=job_id).is_authenticated())


def _auto_run_if_requested(job_id, form):
	"""Start the pipeline as soon as an upload finishes, when the user asked for it.

	Returns what happened, or None when auto-run was not requested. It never
	raises and never fails the upload: the samples are registered either way, so a
	run that cannot start (no BV-BRC login, say) is reported alongside a
	successful upload rather than turning it into an error the user reads as
	"my files were lost"."""
	if (form.get("auto_run") or "").strip().lower() not in ("1", "true", "on", "yes"):
		return None

	authentication_error = _authenticate_job(
		job_id, (form.get("username") or "").strip(), (form.get("password") or "").strip()
	)
	if authentication_error:
		return {"started": False, "error": authentication_error}

	run_payload, run_status_code = _admit_pipeline_run(job_id)
	if run_status_code >= 400:
		return {"started": False, "error": run_payload["error"]}
	return {
		"started": True,
		"queued": run_payload.get("queued", False),
		"queue_position": run_payload.get("queue_position"),
	}


@app.route("/submit", methods=["POST"])
def submit():
	upload_started_at = time.time()
	first_fastq_upload = request.files.get("fastq_file_1")
	second_fastq_upload = request.files.get("fastq_file_2")
	if not first_fastq_upload or not second_fastq_upload:
		return jsonify({"error": "Both files are required"}), 400

	first_read_checksum = request.form.get("fastq_file_1_checksum", "").strip()
	second_read_checksum = request.form.get("fastq_file_2_checksum", "").strip()

	# Blank job_id starts a new batch; naming one adds this pair to it.
	job_id, job_error = _resolve_upload_job(request.form.get("job_id"))
	if job_error:
		return job_error

	data_dir = job_data_dir(job_id)
	data_dir.mkdir(parents=True, exist_ok=True)

	# Write each pair to this batch's raw_fastq directory, streaming it to S3 as it
	# arrives rather than reading the finished file back off disk to upload it.
	r1_path = data_dir / secure_filename(first_fastq_upload.filename)
	r2_path = data_dir / secure_filename(second_fastq_upload.filename)
	r1_in_s3 = _save_upload(job_id, first_fastq_upload, r1_path)
	r2_in_s3 = _save_upload(job_id, second_fastq_upload, r2_path)

	# Verify MD5 checksums if provided. Deliberately re-reads the files from disk
	# rather than digesting the bytes in flight: what has to be correct is what the
	# pipeline will actually read, so this checks the artifact, not our copy of it.
	if first_read_checksum:
		checksum_valid, checksum_message = verify_file_md5(r1_path, first_read_checksum)
		if not checksum_valid:
			_discard_raw_upload(job_id, r1_path, r2_path)
			return jsonify({"error": f"R1 checksum mismatch: {checksum_message}"}), 400

	if second_read_checksum:
		checksum_valid, checksum_message = verify_file_md5(r2_path, second_read_checksum)
		if not checksum_valid:
			_discard_raw_upload(job_id, r1_path, r2_path)
			return jsonify({"error": f"R2 checksum mismatch: {checksum_message}"}), 400

	isolate_id = re.sub(r"_R[12].*", "", Path(first_fastq_upload.filename).name)
	if not is_valid_isolate_id(isolate_id):
		_discard_raw_upload(job_id, r1_path, r2_path)
		return jsonify({"error": "Could not derive a valid isolate ID from the filename"}), 400

	# Read both reads through before accepting the pair. This costs a full pass over
	# each file, which is why it runs last -- after the cheap checks have had their
	# chance to reject it -- but a truncated or mismatched pair that gets in here is
	# not caught until the pipeline uploads it to BV-BRC and BV-BRC refuses it, hours
	# later and with the run already dead. The person who can fix it is standing here
	# now, so tell them now.
	pair_valid, pair_errors, _ = validate_sample_files(
		{"R1_path": str(r1_path), "R2_path": str(r2_path)}
	)
	if not pair_valid:
		_discard_raw_upload(job_id, r1_path, r2_path)
		return jsonify({"error": "; ".join(pair_errors), "errors": pair_errors}), 400

	with _job_lock(job_id):
		existing_isolates = {
			sample_row.get("isolate_id") for sample_row in _read_samples(job_samples_csv(job_id))[0]
		}
		_upsert_sample(
			job_samples_csv(job_id),
			isolate_id,
			str(r1_path.relative_to(PROJECT_ROOT)),
			str(r2_path.relative_to(PROJECT_ROOT)),
		)
		was_update = isolate_id in existing_isolates
		upload_entry = _record_upload(
			job_id,
			"pair",
			upload_started_at,
			added=[] if was_update else [isolate_id],
			updated=[isolate_id] if was_update else [],
		)

	# No backup pass here: both FASTQs went to S3 while they were being received. Now
	# that they are verified and in the manifest, the local copies can go -- the run
	# fetches them back from S3 when it actually needs them. Released per file, and
	# only for files S3 confirmed: a read that failed to upload simply stays on disk,
	# so it is never missing from both places at once.
	_release_local_raw(
		job_id,
		*[path for path, in_s3 in ((r1_path, r1_in_s3), (r2_path, r2_in_s3)) if in_s3],
	)

	return jsonify(
		{
			"job_id": job_id,
			"f1": first_fastq_upload.filename,
			"f2": second_fastq_upload.filename,
			"isolate_id": isolate_id,
			"added": upload_entry["added"],
			"updated": upload_entry["updated"],
			"upload": upload_entry,
			"auto_run": _auto_run_if_requested(job_id, request.form),
		}
	), 200


@app.route("/delete", methods=["DELETE"])
def delete_file():
	request_data = request.get_json(silent=True) or {}
	job_id = request_data.get("job_id")
	_job_or_400(job_id)
	frozen = _frozen_samples_response(job_id)
	if frozen is not None:
		return frozen
	uploaded_file_names = request_data.get("files", [])

	data_dir = job_data_dir(job_id)
	for file_name in uploaded_file_names:
		file_name = secure_filename(file_name)
		resolved_path = data_dir / file_name
		if resolved_path.is_file() and resolved_path.resolve().is_relative_to(DATA_ROOT.resolve()):
			resolved_path.unlink()
	for isolate_id in {re.sub(r"_R[12].*", "", file_name) for file_name in uploaded_file_names}:
		if is_valid_isolate_id(isolate_id):
			_remove_sample(job_samples_csv(job_id), isolate_id)
	return jsonify({"message": "Deleted"}), 200


def _flatten_single_root(resolved_path):
	"""Descend through directories that hold only one subdirectory.

	A browser folder upload nests every file under the chosen folder's own
	name (and any real subfolders inside it), so the staging dir we just
	wrote has one extra wrapper level compared to a typed path that pointed
	straight at the folder. Unwrap it so stats-workbook lookup (which only
	checks the top level) still finds it.
	"""
	resolved_path = Path(resolved_path)
	while True:
		directory_entries = list(resolved_path.iterdir())
		if len(directory_entries) != 1 or not directory_entries[0].is_dir():
			return resolved_path
		resolved_path = directory_entries[0]


@app.route("/import", methods=["POST"])
def import_folder():
	upload_started_at = time.time()
	uploaded_file_names = request.files.getlist("files")
	if not uploaded_file_names:
		return jsonify({"error": "No files uploaded"}), 400

	# Blank job_id starts a new batch; naming one merges this folder into it.
	job_id, job_error = _resolve_upload_job(request.form.get("job_id"))
	if job_error:
		return job_error

	temporary_import_dir = Path(tempfile.mkdtemp(prefix="import_"))
	try:
		try:
			for file_handle in uploaded_file_names:
				relative_upload_path = (file_handle.filename or "").replace("\\", "/")
				parts = [
					path_part
					for path_part in relative_upload_path.split("/")
					if path_part not in ("", ".", "..")
				]
				if not parts:
					continue
				safe_relative_path = Path(*[secure_filename(path_part) for path_part in parts])
				staged_upload_path = temporary_import_dir / safe_relative_path
				staged_upload_path.parent.mkdir(parents=True, exist_ok=True)
				file_handle.save(staged_upload_path)
		except OSError as exception:
			return jsonify({"error": f"Could not stage uploaded files: {exception}"}), 500

		import_root = _flatten_single_root(temporary_import_dir)
		try:
			import_result = _import_service.import_directory(
				job_id,
				import_root,
				job_samples_csv(job_id),
				job_data_dir(job_id),
				"folder",
				upload_started_at,
			)
		except NotADirectoryError:
			return jsonify({"error": "No valid files found in upload"}), 400
		except Exception as exception:
			return jsonify({"error": str(exception)}), 500
		import_result["job_id"] = job_id
		# No backup pass here: ImportService pushed each pair to S3 and released the
		# local copy as import_directory registered it, so by now the reads this
		# request added are already durable and off the disk.
		import_result["auto_run"] = _auto_run_if_requested(job_id, request.form)
		return jsonify(import_result), 200
	finally:
		shutil.rmtree(temporary_import_dir, ignore_errors=True)


# Cloud import: pull a batch's FASTQ files from a OneDrive/Google Drive share
# link instead of having the browser push them. The sequencing company usually
# mails a link to the run folder, and re-uploading tens of GB through a laptop
# only to send it back out again is the slowest way to move it.
#
# The download happens on the server and, for a real run folder, takes far longer
# than an HTTP request can be held open, so it runs on a background thread while
# the browser polls /cloud-import/status -- the same shape /run and /status use.
# Once the files land, they go through import_samples.import_directory() exactly
# like a browser folder upload: same R1/R2 pairing, same MD5 verification against
# the stats workbook, same manifest, same job ID. From /run's point of view a
# cloud-imported job is indistinguishable from an uploaded one.
#
# The feature is disabled: the two routes below, the manager that backs them, the
# fieldset in templates/index.html, and the handlers in static/app.js are all
# commented out. workflow/lib/cloud_import.py and its unit tests are untouched, so
# re-enabling means uncommenting these four call sites -- nothing needs rewriting.

# MAX_CONCURRENT_CLOUD_IMPORTS = max(1, int(os.environ.get("MAX_CONCURRENT_CLOUD_IMPORTS", "2")))
#
#
# def _complete_cloud_import(job_id, import_result, upload_form):
# 	"""Finalize a completed cloud transfer through the same S3/run path as uploads."""
# 	_release_local_raw(
# 		job_id,
# 		*_backup_raw_files_to_s3(
# 			job_id,
# 			_raw_paths_for_isolates(job_id, import_result["added"] + import_result["updated"]),
# 		),
# 	)
# 	import_result["auto_run"] = _auto_run_if_requested(job_id, upload_form)
#
#
# _cloud_import_manager = CloudImportManager(
# 	_import_service, MAX_CONCURRENT_CLOUD_IMPORTS, _complete_cloud_import
# )
# _CLOUD_IMPORT_RECORD_TTL = _cloud_import_manager.record_ttl
# _cloud_import_lock = _cloud_import_manager.lock
# _cloud_imports = _cloud_import_manager.records
#
#
# def _set_cloud_import(job_id, **fields):
# 	_cloud_import_manager.set(job_id, **fields)
#
#
# def _prune_cloud_imports():
# 	_cloud_import_manager.prune()
#
#
# def _run_cloud_import(job_id, share_url, upload_form):
# 	_cloud_import_manager._run(
# 		job_id, share_url, upload_form, job_samples_csv(job_id), job_data_dir(job_id)
# 	)
#
#
# @app.route("/cloud-import", methods=["POST"])
# @limiter.limit("5/minute")
# def cloud_import_start():
# 	share_url = (request.form.get("share_url") or "").strip()
# 	if not share_url:
# 		return jsonify({"error": "Paste a OneDrive or Google Drive share link first."}), 400
# 	# Refuse the link before a job ID exists: one we are never going to fetch
# 	# should not leave an empty job behind for the retention sweep to clean up.
# 	if not cloud_import.is_allowed_url(share_url) or cloud_import.provider_for(share_url) is None:
# 		return jsonify(
# 			{
# 				"error": "Only Google Drive and OneDrive/SharePoint https:// share links can be imported."
# 			}
# 		), 400
#
# 	# Blank job_id starts a new batch; naming one pulls this link into it.
# 	requested_job_id, job_error = _resolve_upload_job(request.form.get("job_id"))
# 	if job_error:
# 		return job_error
#
# 	# The thread outlives this request, so take a copy of what it needs now.
# 	upload_form = {
# 		field: request.form.get(field, "") for field in ("auto_run", "username", "password")
# 	}
#
# 	job_id = requested_job_id
# 	_cloud_import_manager.max_concurrent = MAX_CONCURRENT_CLOUD_IMPORTS
# 	started, error_message = _cloud_import_manager.start(
# 		job_id, share_url, upload_form, job_samples_csv(job_id), job_data_dir(job_id)
# 	)
# 	if not started:
# 		status_code = 409 if "already has" in error_message else 429
# 		return jsonify({"error": error_message}), status_code
# 	return jsonify({"job_id": job_id, "state": "running"}), 202
#
#
# @app.route("/cloud-import/status")
# def cloud_import_status():
# 	job_id = request.args.get("job_id")
# 	_job_or_400(job_id)
# 	import_record = _cloud_import_manager.get(job_id)
# 	if import_record is None:
# 		return jsonify({"error": "No cloud import found for this job ID"}), 404
# 	return jsonify(import_record)


@app.route("/run", methods=["POST"])
def run_pipeline():
	job_id = request.form.get("job_id")
	_job_or_400(job_id)

	authentication_error = _authenticate_job(
		job_id,
		request.form.get("username", "").strip(),
		request.form.get("password", "").strip(),
	)
	if authentication_error:
		return jsonify({"error": authentication_error}), 401

	run_payload, run_status_code = _admit_pipeline_run(job_id)
	return jsonify(run_payload), run_status_code


@app.route("/abort", methods=["POST"])
def abort_pipeline():
	"""Stop a run's process group, escalating to SIGKILL after a grace period."""
	job_id = request.form.get("job_id")
	_job_or_400(job_id)
	with _pipeline_lock:
		# Still waiting for a slot: it has no process to signal, so drop it from
		# the queue and record the outcome here -- no watcher will ever run for it.
		if job_id in _pipeline_queue:
			_pipeline_queue.remove(job_id)
			_persist_pipeline_queue()
			_pipeline_aborted_jobs.discard(job_id)
			_write_job_status(job_id, False, aborted=True)
			cancelled_before_starting = True
		else:
			cancelled_before_starting = False

	if cancelled_before_starting:
		# Nothing needs these reads any more, so put them back on the expiry clock.
		# Outside the lock: this talks to S3.
		_return_raw_to_the_clock(job_id)
		return jsonify({"message": "Queued run cancelled", "job_id": job_id}), 200

	with _pipeline_lock:
		# Already running: signal it. Its watcher records the abort and returns the
		# reads to the expiry clock, so there is nothing to do for them here.
		pipeline_process = _pipeline_processes.get(job_id)
		if pipeline_process is None or pipeline_process.poll() is not None:
			return jsonify(
				{"error": "No pipeline run is currently in progress for this job ID"}
			), 409
		_pipeline_aborted_jobs.add(job_id)
	try:
		os.killpg(os.getpgid(pipeline_process.pid), signal.SIGTERM)
	except ProcessLookupError:
		pass

	def _escalate():
		for termination_check_number in range(20):  # ~10s grace period at 0.5s intervals
			if pipeline_process.poll() is not None:
				return
			time.sleep(0.5)
		try:
			os.killpg(os.getpgid(pipeline_process.pid), signal.SIGKILL)
		except ProcessLookupError:
			pass

	threading.Thread(target=_escalate, daemon=True).start()
	return jsonify({"message": "Abort requested", "job_id": job_id}), 200


@app.route("/status")
def pipeline_status():
	job_id = request.args.get("job_id")
	_job_or_400(job_id)
	if not job_samples_csv(job_id).exists():
		return jsonify({"error": "No run found for this job ID"}), 404
	run_status = _job_snapshot(job_id)["run_status"]
	if run_status is None:
		return jsonify({"error": "No run found for this job ID"}), 404
	return jsonify(run_status)


@app.route("/job/<job_id>")
@limiter.limit("10/minute", override_defaults=False)
def job_lookup(job_id):
	_job_or_400(job_id)
	if not job_samples_csv(job_id).is_file():
		abort(404)
	return jsonify(_job_snapshot(job_id))


@app.route("/results/<job_id>/<isolate_id>/view")
def view_result(job_id, isolate_id):
	_job_or_400(job_id)
	if not is_valid_isolate_id(isolate_id):
		abort(404)
	resolved_path = _resolve_result_dir(job_id, isolate_id)
	report_html = None
	if resolved_path is not None:
		report_path = resolved_path / "summary" / "report.html"
		if report_path.is_file():
			report_html = report_path.read_bytes()
	if report_html is None:
		report_html = s3_storage.get_object_bytes(
			s3_storage.key_for(job_id, isolate_id, "report.html")
		)
	if report_html is None:
		abort(404)
	response = Response(report_html, mimetype="text/html")
	# Reports contain externally-derived text. Keep them inert even if a future
	# report generator accidentally fails to escape a value.
	response.headers["Content-Security-Policy"] = (
		"default-src 'none'; style-src 'unsafe-inline'; img-src data:; sandbox"
	)
	return response


def _zip_stream_response(directory, arc_root, download_name):
	"""Serve a directory as a zip built on the fly, so the worker never holds more
	than a chunk of it. download_name is always derived from an ID we have already
	validated, so it is safe to put in the header verbatim."""
	response = Response(stream_directory_zip(directory, arc_root), mimetype="application/zip")
	response.headers["Content-Disposition"] = f'attachment; filename="{download_name}"'
	return response


# S3 first, local disk second -- the reverse of what you might expect, and the
# reason the box used to fall over on "download all".
#
# upload_job_results already puts these exact archives in S3 when a run finishes, so
# for any completed job the artifact the user wants exists, pre-built, and a presigned
# redirect hands it over without the bytes touching this process at all. Zipping the
# local copy instead means rebuilding, in the web worker, something S3 is already
# holding. We only do that when S3 cannot serve it: a run still in progress, or S3
# switched off entirely. presigned_download_url returns None in both cases (it checks
# the object exists), which is what makes the fallback safe to hang off it.
@app.route("/results/<job_id>/<isolate_id>/download")
def download_result(job_id, isolate_id):
	_job_or_400(job_id)
	if not is_valid_isolate_id(isolate_id):
		abort(404)
	download_url = s3_storage.presigned_download_url(
		s3_storage.key_for(job_id, f"{isolate_id}_results.zip"),
		filename=f"{isolate_id}_results.zip",
		content_type="application/zip",
	)
	if download_url is not None:
		return redirect(download_url)
	resolved_path = _resolve_result_dir(job_id, isolate_id)
	if resolved_path is None:
		abort(404)
	return _zip_stream_response(resolved_path, isolate_id, f"{isolate_id}_results.zip")


@app.route("/results/<job_id>/download-all")
def download_all_results(job_id):
	_job_or_400(job_id)
	# Downloading results counts as viewing them: start the 3-hour TTL clock so a
	# user who jumps straight to the archive still gets the full download window.
	_mark_first_viewed(job_id)
	download_url = s3_storage.presigned_download_url(
		s3_storage.key_for(job_id, f"{job_id}_results.zip"),
		filename=f"{job_id}_results.zip",
		content_type="application/zip",
	)
	if download_url is not None:
		return redirect(download_url)
	results_dir = job_results_dir(job_id)
	if not (results_dir.is_dir() and any(results_dir.iterdir())):
		abort(404)
	return _zip_stream_response(results_dir, job_id, f"{job_id}_results.zip")


@app.route("/results/<job_id>/master-report/download")
def download_master_report(job_id):
	_job_or_400(job_id)
	# Downloading results counts as viewing them: start the 3-hour TTL clock so a
	# user who jumps straight to the archive still gets the full download window.
	_mark_first_viewed(job_id)
	resolved_path = job_results_dir(job_id) / "master_report.csv"
	if resolved_path.is_file():
		return send_file(
			resolved_path,
			mimetype="text/csv",
			as_attachment=True,
			download_name=f"{job_id}_master_report.csv",
		)
	download_url = s3_storage.presigned_download_url(
		s3_storage.key_for(job_id, "master_report.csv"),
		filename=f"{job_id}_master_report.csv",
		content_type="text/csv",
	)
	if download_url is None:
		abort(404)
	return redirect(download_url)


if __name__ == "__main__":
	# The Werkzeug debugger is unsafe on a public server; enable it explicitly.
	debug_enabled = os.environ.get("FLASK_DEBUG") == "1"
	host_name = os.environ.get("HOST", "127.0.0.1")
	port_number = int(os.environ.get("PORT", "5001"))
	# Skip under the reloader's parent process, which exists only to respawn the
	# child: recovering there would mark the child's own running jobs as failed.
	if not debug_enabled or os.environ.get("WERKZEUG_RUN_MAIN") == "true":
		run_startup_recovery()
	app.run(debug=debug_enabled, host=host_name, port=port_number)
