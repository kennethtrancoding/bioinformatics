"""
Optional S3-backed durable storage for finished job results.

Local disk under results/<JOB_ID>/ remains the working directory Snakemake
writes to during a run, and stays the fastest path for viewing/downloading a
job immediately after it finishes. When RESULTS_S3_BUCKET is set, a
successful job's reports are also pushed to S3 so they survive the local
retention sweep (frontend.py's _expire_job_results) and EC2 instance
replacement. Every function here is a no-op / returns a falsy value when the
bucket is not configured, so S3 stays entirely optional -- local dev and
tests never need it.
"""

import io
import os
import zipfile
from pathlib import Path

import boto3
from botocore.exceptions import ClientError

_BUCKET = os.environ.get("RESULTS_S3_BUCKET")
_PREFIX = os.environ.get("RESULTS_S3_PREFIX", "results").strip("/")
# Raw uploads live under their own prefix so durable inputs never collide with
# the results objects written under _PREFIX for the same job ID.
_RAW_PREFIX = os.environ.get("RAW_S3_PREFIX", "raw").strip("/")
_PRESIGN_EXPIRES_IN = int(os.environ.get("RESULTS_S3_PRESIGN_SECONDS", "300"))
_client = boto3.client("s3") if _BUCKET else None


def is_enabled() -> bool:
	return _client is not None


def key_for(job_id: str, *parts: str) -> str:
	return "/".join([_PREFIX, job_id, *parts])


def raw_key_for(job_id: str, *parts: str) -> str:
	return "/".join([_RAW_PREFIX, job_id, *parts])


def _zip_directory(directory: Path, arc_root: str) -> io.BytesIO:
	buffer = io.BytesIO()
	with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as archive:
		for file_path in Path(directory).rglob("*"):
			if file_path.is_file():
				archive.write(file_path, Path(arc_root) / file_path.relative_to(directory))
	buffer.seek(0)
	return buffer


def upload_job_results(job_id: str, results_dir: Path) -> None:
	"""Push a finished job's reports to S3: the master report, a per-isolate
	zip plus raw report.html (for inline viewing), and a whole-job zip.
	Caller is expected to swallow exceptions -- a failed upload must not
	affect the job's recorded pipeline status."""
	if not is_enabled():
		return
	results_dir = Path(results_dir)

	master_report = results_dir / "master_report.csv"
	if master_report.is_file():
		_client.upload_file(str(master_report), _BUCKET, key_for(job_id, "master_report.csv"))

	isolate_dirs = sorted(p for p in results_dir.iterdir() if p.is_dir())
	for isolate_dir in isolate_dirs:
		isolate_id = isolate_dir.name
		report_html = isolate_dir / "summary" / "report.html"
		if not report_html.is_file():
			continue  # incomplete isolate (e.g. a failed sample); nothing durable to store
		_client.upload_file(
			str(report_html),
			_BUCKET,
			key_for(job_id, isolate_id, "report.html"),
			ExtraArgs={"ContentType": "text/html"},
		)
		_client.upload_fileobj(
			_zip_directory(isolate_dir, isolate_id),
			_BUCKET,
			key_for(job_id, f"{isolate_id}_results.zip"),
		)

	if isolate_dirs:
		_client.upload_fileobj(
			_zip_directory(results_dir, job_id), _BUCKET, key_for(job_id, f"{job_id}_results.zip")
		)


def upload_raw_file(job_id: str, file_path: Path) -> None:
	"""Push one uploaded raw FASTQ to S3 for durable storage, keyed by its
	basename under this job's raw prefix. The local copy the pipeline reads from
	stays the source of truth, so callers are expected to swallow exceptions --
	a failed backup must not fail the upload request or the pipeline run."""
	if not is_enabled():
		return
	file_path = Path(file_path)
	_client.upload_file(str(file_path), _BUCKET, raw_key_for(job_id, file_path.name))


def delete_raw(job_id: str) -> None:
	"""Delete every raw upload stored for a job. Best-effort; caller swallows."""
	if not is_enabled():
		return
	paginator = _client.get_paginator("list_objects_v2")
	for page in paginator.paginate(Bucket=_BUCKET, Prefix=raw_key_for(job_id, "")):
		object_keys = [{"Key": obj["Key"]} for obj in page.get("Contents", [])]
		if object_keys:
			_client.delete_objects(Bucket=_BUCKET, Delete={"Objects": object_keys})


def list_isolates(job_id: str) -> list:
	"""Isolate IDs with durable results in S3, derived from the per-isolate
	report.html objects uploaded by upload_job_results. Used as a fallback
	once local results have been pruned."""
	if not is_enabled():
		return []
	isolate_ids = []
	paginator = _client.get_paginator("list_objects_v2")
	for page in paginator.paginate(Bucket=_BUCKET, Prefix=key_for(job_id, ""), Delimiter="/"):
		for common_prefix in page.get("CommonPrefixes", []):
			isolate_ids.append(common_prefix["Prefix"].rstrip("/").rsplit("/", 1)[-1])
	return sorted(isolate_ids)


def get_object_bytes(key: str):
	if not is_enabled():
		return None
	try:
		return _client.get_object(Bucket=_BUCKET, Key=key)["Body"].read()
	except ClientError:
		return None


def object_exists(key: str) -> bool:
	if not is_enabled():
		return False
	try:
		_client.head_object(Bucket=_BUCKET, Key=key)
		return True
	except ClientError:
		return False


def presigned_download_url(key: str, filename: str, content_type: str):
	"""Short-lived URL for a direct browser download from S3, so large result
	archives don't have to be proxied back through the app server."""
	if not is_enabled():
		return None
	if not object_exists(key):
		return None
	return _client.generate_presigned_url(
		"get_object",
		Params={
			"Bucket": _BUCKET,
			"Key": key,
			"ResponseContentDisposition": f'attachment; filename="{filename}"',
			"ResponseContentType": content_type,
		},
		ExpiresIn=_PRESIGN_EXPIRES_IN,
	)
