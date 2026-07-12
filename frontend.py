import csv
import hmac
import io
import json
import os
import re
import shutil
import signal
import subprocess
import tempfile
import threading
import time
import zipfile
from collections import deque
from datetime import datetime
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
	send_from_directory,
	url_for,
)
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
from werkzeug.utils import secure_filename

from workflow.lib import api_registry, import_samples
from workflow.lib.bvbrc_client import BVBRCClient
from workflow.lib.jobs import (
	generate_job_id,
	is_valid_isolate_id,
	is_valid_job_id,
	job_data_dir,
	job_first_viewed_path,
	job_log_path,
	job_pinned_path,
	job_results_dir,
	job_run_started_path,
	job_samples_csv,
	job_status_path,
)
from workflow.lib.preprocess import verify_file_md5

PROJECT_ROOT = Path(__file__).resolve().parent

# Files created by the web process can contain credentials or genomic data.
os.umask(0o077)

app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY") or os.urandom(32)

# Only the job-ID lookup is rate limited because job IDs grant access to results.
# In-memory counters are sufficient while Gunicorn is restricted to one worker.
limiter = Limiter(get_remote_address, app=app, storage_uri="memory://", default_limits=[])

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
		return  # auth disabled (local development)
	if request.path == "/api/health":
		return  # allow unauthenticated health checks for monitoring
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
	response.headers.setdefault("Referrer-Policy", "no-referrer")
	response.headers.setdefault("Permissions-Policy", "camera=(), microphone=(), geolocation=()")
	if request.path.startswith(("/job/", "/status", "/results/")):
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
PIPELINE_CORES = max(1, int(os.environ.get("PIPELINE_CORES", "4")))

# Gunicorn threads share these structures, so updates use one lock.
_pipeline_lock = threading.Lock()
_pipeline_processes = {}
_pipeline_queue = deque()
_pipeline_aborted_jobs = set()


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


def _zip_directory(directory, arc_root):
	zip_buffer = io.BytesIO()
	directory = Path(directory)
	with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zip_archive:
		for archive_file_path in directory.rglob("*"):
			if archive_file_path.is_file():
				archive_name = Path(arc_root) / archive_file_path.relative_to(directory)
				zip_archive.write(archive_file_path, archive_name)
	zip_buffer.seek(0)
	return zip_buffer


_CSV_FIELDS = ["isolate_id", "R1_path", "R2_path", "description"]


def _read_samples(samples_csv):
	samples_csv = Path(samples_csv)
	if not samples_csv.exists():
		return [], _CSV_FIELDS
	with samples_csv.open(newline="") as file_handle:
		reader = csv.DictReader(file_handle)
		fieldnames = reader.fieldnames or _CSV_FIELDS
		return list(reader), fieldnames


def _write_samples(samples_csv, sample_rows, fieldnames=_CSV_FIELDS):
	samples_csv = Path(samples_csv)
	samples_csv.parent.mkdir(parents=True, exist_ok=True)
	with samples_csv.open("w", newline="") as file_handle:
		writer = csv.DictWriter(file_handle, fieldnames=fieldnames)
		writer.writeheader()
		writer.writerows(sample_rows)


def _upsert_sample(samples_csv, isolate_id, r1_path, r2_path):
	sample_rows, fieldnames = _read_samples(samples_csv)
	sample_rows = [
		sample_row for sample_row in sample_rows if sample_row.get("isolate_id") != isolate_id
	]
	sample_rows.append(
		{"isolate_id": isolate_id, "R1_path": r1_path, "R2_path": r2_path, "description": ""}
	)
	_write_samples(samples_csv, sample_rows, fieldnames)


def _remove_sample(samples_csv, isolate_id):
	sample_rows, fieldnames = _read_samples(samples_csv)
	_write_samples(
		samples_csv,
		[sample_row for sample_row in sample_rows if sample_row.get("isolate_id") != isolate_id],
		fieldnames,
	)


def _extract_error_summary(log_path, max_chars=800):
	"""Best-effort excerpt of the failure from a run's snakemake log, so a
	crash reads as an actual explanation instead of a bare boolean."""
	try:
		log_text = log_path.read_text(errors="replace")
	except OSError:
		return None
	error_start_index = log_text.rfind("Error in rule ")
	if error_start_index == -1:
		error_start_index = log_text.rfind("\nError")
	if error_start_index == -1:
		return None
	return log_text[error_start_index : error_start_index + max_chars].strip()


def _write_job_status(job_id, success, aborted=False):
	"""Persist terminal status so it survives browser and process lifetimes."""
	status_path = job_status_path(job_id)
	status_path.parent.mkdir(parents=True, exist_ok=True)
	status_payload = {"done": True, "success": success, "finished_at": time.time()}
	if aborted:
		status_payload["error"] = "Pipeline run aborted by user."
	elif not success:
		status_payload["error"] = _extract_error_summary(job_log_path(job_id))
	status_path.write_text(json.dumps(status_payload))


def _reset_run_markers(job_id):
	"""Clear status and retention markers before admitting a rerun."""
	results_dir = job_results_dir(job_id)
	results_dir.mkdir(parents=True, exist_ok=True)
	job_status_path(job_id).unlink(missing_ok=True)
	job_first_viewed_path(job_id).unlink(missing_ok=True)
	job_run_started_path(job_id).unlink(missing_ok=True)


def _start_pipeline(job_id):
	"""Spawn the Snakemake subprocess for job_id and register it as holding a
	slot. Caller must hold _pipeline_lock."""
	log_path = job_log_path(job_id)
	log_path.parent.mkdir(parents=True, exist_ok=True)
	results_dir = job_results_dir(job_id)
	results_dir.mkdir(parents=True, exist_ok=True)
	# Queued jobs do not receive a start time until they acquire a slot.
	job_run_started_path(job_id).write_text(str(time.time()))
	with log_path.open("w") as pipeline_log_file:
		pipeline_process = subprocess.Popen(
			[
				"snakemake",
				"--cores",
				str(PIPELINE_CORES),
				"--use-conda",
				"--rerun-incomplete",
				# Jobs write to separate result trees. Snakemake's shared lock
				# directory is unsafe because one concurrent run can remove it
				# while another is still completing.
				"--nolock",
				"--config",
				f"job_id={job_id}",
				f"results_dir={results_dir.relative_to(PROJECT_ROOT)}",
				f"samples_manifest={job_samples_csv(job_id).relative_to(PROJECT_ROOT)}",
			],
			cwd=PROJECT_ROOT,
			stdout=pipeline_log_file,
			stderr=subprocess.STDOUT,
			# A process group lets /abort stop Snakemake and all rule children.
			start_new_session=True,
		)
	_pipeline_processes[job_id] = pipeline_process
	threading.Thread(target=_watch_pipeline, args=(job_id, pipeline_process), daemon=True).start()


def _drain_pipeline_queue():
	"""Promote queued jobs into free slots. Caller must hold _pipeline_lock."""
	while _pipeline_queue and len(_pipeline_processes) < MAX_CONCURRENT_PIPELINES:
		job_id = _pipeline_queue.popleft()
		try:
			_start_pipeline(job_id)
		except Exception as exception:
			# Record startup failures so one invalid job cannot wedge the queue.
			_write_job_status(job_id, False)
			print(f"[pipeline] failed to start queued job {job_id}: {exception}")


def _watch_pipeline(job_id, pipeline_process):
	returncode = pipeline_process.wait()
	with _pipeline_lock:
		aborted = job_id in _pipeline_aborted_jobs
		_pipeline_aborted_jobs.discard(job_id)
		_write_job_status(job_id, returncode == 0, aborted=aborted)
		if _pipeline_processes.get(job_id) is pipeline_process:
			_pipeline_processes.pop(job_id, None)
		# This run's slot is now free -- hand it to whoever is waiting.
		_drain_pipeline_queue()
	# Token persists across reruns for the same job; destroyed when results
	# are auto-deleted by the retention sweep, not immediately after each run.


def _read_job_status(job_id):
	try:
		return json.loads(job_status_path(job_id).read_text())
	except (OSError, ValueError):
		return None


def _mark_first_viewed(job_id):
	"""Start the download retention window on the first terminal-status view."""
	viewed_marker_path = job_first_viewed_path(job_id)
	if not viewed_marker_path.exists():
		viewed_marker_path.parent.mkdir(parents=True, exist_ok=True)
		viewed_marker_path.touch()


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
			if not result_entry.is_dir():
				continue
			results_by_isolate[result_entry.name] = {
				"has_report": (result_entry / "summary" / "report.html").is_file(),
				"has_mobile_elements": (
					result_entry / "06_mobile_elements" / "me_summary.csv"
				).is_file(),
			}

	started_at = None
	try:
		started_at = float(job_run_started_path(job_id).read_text())
	except (OSError, ValueError):
		pass

	run_status = None
	with _pipeline_lock:
		pipeline_process = _pipeline_processes.get(job_id)
		queue_position = (
			_pipeline_queue.index(job_id) + 1 if job_id in _pipeline_queue else None
		)
	if queue_position is not None:
		# Admitted but not started. Not done, and deliberately has no started_at:
		# the run hasn't begun, so there is no elapsed time to report yet.
		run_status = {
			"done": False,
			"success": None,
			"queued": True,
			"queue_position": queue_position,
			"started_at": None,
		}
	elif pipeline_process is not None and pipeline_process.poll() is None:
		run_status = {"done": False, "success": None, "queued": False, "started_at": started_at}
	else:
		persisted_status = _read_job_status(job_id)
		if persisted_status is not None:
			run_status = {
				"done": True,
				"success": persisted_status.get("success"),
				"error": persisted_status.get("error"),
				"started_at": started_at,
			}
			_mark_first_viewed(job_id)

	return {
		"job_id": job_id,
		"samples": samples,
		"results": [
			{"isolate_id": isolate_id, **isolate_result_info}
			for isolate_id, isolate_result_info in results_by_isolate.items()
		],
		"run_status": run_status,
		"has_master_report": (results_dir / "master_report.csv").is_file(),
	}


# ---------------------------------------------------------------------------
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


# ---------------------------------------------------------------------------
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


def _expire_job_results(job_id, job_results_directory, current_time):
	"""Delete one job's results if its retention window has closed."""
	if job_pinned_path(job_id).is_file():
		return  # e.g. a hand-built demo job -- never auto-deleted
	status_path = job_status_path(job_id)
	if not status_path.is_file():
		return  # Run still in progress
	run_finished_time = status_path.stat().st_mtime
	# Delete if 7 days have passed since the run finished (safety net), or if 3
	# hours have passed since the user first viewed it (active download window).
	viewed_marker_path = job_first_viewed_path(job_id)
	expired_unviewed = current_time - run_finished_time >= _MAX_TTL_SECONDS
	expired_after_view = (
		viewed_marker_path.is_file()
		and current_time - viewed_marker_path.stat().st_mtime >= _VIEW_TTL_SECONDS
	)
	if expired_unviewed or expired_after_view:
		BVBRCClient(job_id=job_id).destroy_token()
		shutil.rmtree(job_results_directory, ignore_errors=True)


def _sweep_expired_results():
	current_time = time.time()
	if not RESULTS_ROOT.is_dir():
		return
	for job_results_directory in RESULTS_ROOT.iterdir():
		if not job_results_directory.is_dir():
			continue
		job_id = job_results_directory.name
		# Anything not named like a job ID was not created by this app -- a
		# manual `snakemake --config results_dir=...` run, say. It owns no token
		# and no retention markers, so there is nothing here to expire.
		if not is_valid_job_id(job_id):
			continue
		# One unreadable job must not abort the pass: every job after it in the
		# listing would then go unpruned, and the next sweep would fail at the
		# same place, so results and tokens would accumulate forever.
		try:
			_expire_job_results(job_id, job_results_directory, current_time)
		except Exception as exception:
			print(f"[retention] could not expire {job_id}: {exception}")
	# Abandoned uploads may never acquire a result status. Their bearer token
	# must still expire locally instead of remaining on disk forever.
	jobs_config_root = PROJECT_ROOT / "config" / "jobs"
	if jobs_config_root.is_dir():
		for job_config_directory in jobs_config_root.iterdir():
			token_path = job_config_directory / ".bvbrc_token"
			if (
				token_path.is_file()
				and current_time - token_path.stat().st_mtime >= _MAX_TTL_SECONDS
			):
				token_path.unlink(missing_ok=True)


def _retention_loop():
	while True:
		time.sleep(_RETENTION_SWEEP_INTERVAL)
		try:
			_sweep_expired_results()
		except Exception as exception:
			# Keep sweeping on the next tick, but say so: a silent failure here
			# means results and BV-BRC tokens quietly stop being deleted.
			print(f"[retention] sweep failed: {exception}")


threading.Thread(target=_retention_loop, daemon=True).start()


@app.route("/")
def analysis():
	return render_template("index.html")


@app.route("/settings", methods=["GET", "POST"])
def settings():
	job_id = request.values.get("job_id", "").strip().upper()
	if not job_id:
		return render_template("settings.html", job_id=None, endpoints=[], settings_saved=False)
	_job_or_400(job_id)
	if not job_samples_csv(job_id).is_file():
		abort(404)
	settings_saved = False
	login_error = None
	if request.method == "POST":
		api_registry.save_job_overrides(job_id, request.form)
		settings_saved = True
		username = request.form.get("username", "").strip()
		password = request.form.get("password", "")
		if username or password:
			if (
				not username
				or not password
				or not BVBRCClient(job_id=job_id).login(username, password)
			):
				login_error = "BV-BRC authentication failed."
	return render_template(
		"settings.html",
		job_id=job_id,
		endpoints=list(api_registry.load_endpoints(job_id=job_id).values()),
		defaults=api_registry.DEFAULT_ENDPOINTS,
		settings_saved=settings_saved,
		login_error=login_error,
	)


@app.route("/settings/reset", methods=["POST"])
def settings_reset():
	job_id = request.form.get("job_id", "").strip().upper()
	_job_or_400(job_id)
	if not job_samples_csv(job_id).is_file():
		abort(404)
	api_registry.save_job_overrides(job_id, {})
	return redirect(url_for("settings", job_id=job_id))


def _human_readable_time_ago(dt):
	"""Convert a datetime to a human-readable 'time ago' string."""
	from datetime import timezone

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
	return jsonify({"services": api_registry.check_all(job_id=job_id)})


@app.route("/submit", methods=["POST"])
def submit():
	first_fastq_upload = request.files.get("fastq_file_1")
	second_fastq_upload = request.files.get("fastq_file_2")
	if not first_fastq_upload or not second_fastq_upload:
		return jsonify({"error": "Both files are required"}), 400

	username = request.form.get("username", "").strip()
	password = request.form.get("password", "").strip()
	first_read_checksum = request.form.get("fastq_file_1_checksum", "").strip()
	second_read_checksum = request.form.get("fastq_file_2_checksum", "").strip()

	job_id = generate_job_id()

	# Authenticate with BV-BRC before doing anything else
	# Pass job_id to use job-specific API endpoint overrides if they exist
	if username and password:
		client = BVBRCClient(job_id=job_id)
		if not client.login(username, password):
			return jsonify({"error": "BV-BRC authentication failed"}), 401
	data_dir = job_data_dir(job_id)
	data_dir.mkdir(parents=True, exist_ok=True)

	# Save files to this batch's raw_fastq directory
	r1_path = data_dir / secure_filename(first_fastq_upload.filename)
	r2_path = data_dir / secure_filename(second_fastq_upload.filename)
	first_fastq_upload.save(r1_path)
	second_fastq_upload.save(r2_path)

	# Verify MD5 checksums if provided
	if first_read_checksum:
		checksum_valid, checksum_message = verify_file_md5(r1_path, first_read_checksum)
		if not checksum_valid:
			r1_path.unlink()
			r2_path.unlink()
			return jsonify({"error": f"R1 checksum mismatch: {checksum_message}"}), 400

	if second_read_checksum:
		checksum_valid, checksum_message = verify_file_md5(r2_path, second_read_checksum)
		if not checksum_valid:
			r1_path.unlink()
			r2_path.unlink()
			return jsonify({"error": f"R2 checksum mismatch: {checksum_message}"}), 400

	# Derive isolate_id from R1 filename (strips _R1 and everything after)
	isolate_id = re.sub(r"_R[12].*", "", Path(first_fastq_upload.filename).name)
	if not is_valid_isolate_id(isolate_id):
		r1_path.unlink()
		r2_path.unlink()
		return jsonify({"error": "Could not derive a valid isolate ID from the filename"}), 400

	_upsert_sample(
		job_samples_csv(job_id),
		isolate_id,
		str(r1_path.relative_to(PROJECT_ROOT)),
		str(r2_path.relative_to(PROJECT_ROOT)),
	)

	return jsonify(
		{
			"job_id": job_id,
			"f1": first_fastq_upload.filename,
			"f2": second_fastq_upload.filename,
			"isolate_id": isolate_id,
		}
	), 200


@app.route("/delete", methods=["DELETE"])
def delete_file():
	request_data = request.get_json(silent=True) or {}
	job_id = request_data.get("job_id")
	_job_or_400(job_id)
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
	uploaded_file_names = request.files.getlist("files")
	if not uploaded_file_names:
		return jsonify({"error": "No files uploaded"}), 400

	job_id = generate_job_id()
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
			# move=True: root is our own throwaway staging dir, so hand the
			# files off to this job's data dir instead of copying — for large
			# FASTQ dumps a copy needlessly doubles disk usage on top of what
			# Werkzeug already spooled while parsing the upload.
			import_result = import_samples.import_directory(
				import_root,
				samples_csv=job_samples_csv(job_id),
				recursive=True,
				dest_dir=job_data_dir(job_id),
			)
		except NotADirectoryError:
			return jsonify({"error": "No valid files found in upload"}), 400
		except Exception as exception:
			return jsonify({"error": str(exception)}), 500
		import_result["job_id"] = job_id
		return jsonify(import_result), 200
	finally:
		shutil.rmtree(temporary_import_dir, ignore_errors=True)


@app.route("/run", methods=["POST"])
def run_pipeline():
	job_id = request.form.get("job_id")
	_job_or_400(job_id)
	if not job_samples_csv(job_id).exists():
		return jsonify({"error": "Unknown job ID"}), 404

	samples, sample_fieldnames = _read_samples(job_samples_csv(job_id))
	if not samples:
		return jsonify({"error": "No FASTQ data has been uploaded for this job yet"}), 400

	# Background BV-BRC upload cannot answer an interactive login prompt.
	if not BVBRCClient(job_id=job_id).is_authenticated():
		return jsonify(
			{
				"error": "BV-BRC login required: submit your BV-BRC username and password before running"
			}
		), 401

	with _pipeline_lock:
		existing_process = _pipeline_processes.get(job_id)
		if (
			existing_process is not None and existing_process.poll() is None
		) or job_id in _pipeline_queue:
			return jsonify({"error": "This pipeline job is already in progress"}), 409

		_reset_run_markers(job_id)

		# Queue the job when all configured pipeline slots are occupied.
		if len(_pipeline_processes) >= MAX_CONCURRENT_PIPELINES:
			_pipeline_queue.append(job_id)
			return jsonify(
				{
					"message": "Pipeline queued",
					"job_id": job_id,
					"queued": True,
					"queue_position": len(_pipeline_queue),
				}
			), 202

		_start_pipeline(job_id)
	return jsonify({"message": "Pipeline started", "job_id": job_id, "queued": False}), 200


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
			_pipeline_aborted_jobs.discard(job_id)
			_write_job_status(job_id, False, aborted=True)
			return jsonify({"message": "Queued run cancelled", "job_id": job_id}), 200

		pipeline_process = _pipeline_processes.get(job_id)
		if pipeline_process is None or pipeline_process.poll() is not None:
			return jsonify({"error": "No pipeline run is currently in progress for this job ID"}), 409
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
@limiter.limit("10/minute")
def job_lookup(job_id):
	_job_or_400(job_id)
	if not job_samples_csv(job_id).is_file():
		abort(404)
	return jsonify(_job_snapshot(job_id))


@app.route("/results/<job_id>/<isolate_id>/view")
def view_result(job_id, isolate_id):
	resolved_path = _resolve_result_dir(job_id, isolate_id)
	if resolved_path is None:
		abort(404)
	report_dir = resolved_path / "summary"
	if not (report_dir / "report.html").is_file():
		abort(404)
	response = send_from_directory(report_dir, "report.html")
	# Reports contain externally-derived text. Keep them inert even if a future
	# report generator accidentally fails to escape a value.
	response.headers["Content-Security-Policy"] = (
		"default-src 'none'; style-src 'unsafe-inline'; img-src data:; sandbox"
	)
	return response


@app.route("/results/<job_id>/<isolate_id>/download")
def download_result(job_id, isolate_id):
	resolved_path = _resolve_result_dir(job_id, isolate_id)
	if resolved_path is None:
		abort(404)
	zip_buffer = _zip_directory(resolved_path, isolate_id)
	return send_file(
		zip_buffer,
		mimetype="application/zip",
		as_attachment=True,
		download_name=f"{isolate_id}_results.zip",
	)


@app.route("/results/<job_id>/download-all")
def download_all_results(job_id):
	_job_or_400(job_id)
	results_dir = job_results_dir(job_id)
	if not results_dir.is_dir():
		abort(404)
	zip_buffer = _zip_directory(results_dir, job_id)
	return send_file(
		zip_buffer,
		mimetype="application/zip",
		as_attachment=True,
		download_name=f"{job_id}_results.zip",
	)


@app.route("/results/<job_id>/master-report/download")
def download_master_report(job_id):
	_job_or_400(job_id)
	resolved_path = job_results_dir(job_id) / "master_report.csv"
	if not resolved_path.is_file():
		abort(404)
	return send_file(
		resolved_path,
		mimetype="text/csv",
		as_attachment=True,
		download_name=f"{job_id}_master_report.csv",
	)


if __name__ == "__main__":
	# The Werkzeug debugger is unsafe on a public server; enable it explicitly.
	debug_enabled = os.environ.get("FLASK_DEBUG") == "1"
	host_name = os.environ.get("HOST", "127.0.0.1")
	port_number = int(os.environ.get("PORT", "5001"))
	app.run(debug=debug_enabled, host=host_name, port=port_number)
