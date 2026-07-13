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
from boto3.s3.transfer import TransferConfig
from botocore.exceptions import ClientError

_BUCKET = os.environ.get("RESULTS_S3_BUCKET")
_PREFIX = os.environ.get("RESULTS_S3_PREFIX", "results").strip("/")
# Raw uploads live under their own prefix so durable inputs never collide with
# the results objects written under _PREFIX for the same job ID.
_RAW_PREFIX = os.environ.get("RAW_S3_PREFIX", "raw").strip("/")
_PRESIGN_EXPIRES_IN = int(os.environ.get("RESULTS_S3_PRESIGN_SECONDS", "300"))
_client = boto3.client("s3") if _BUCKET else None

# Raw reads are input data, not a backup: the upload releases the local copy, so
# between an upload and its run S3 holds the only copy there is. The bucket's
# lifecycle rule (deploy/s3-lifecycle.json) expires them after 7 days, which would be
# a catastrophe for a job that simply sat in the queue for a week -- so the rule does
# not match on age alone. It matches on this tag.
#
# Objects are uploaded UNRUN, and the rule only expires objects tagged UNRUN. When a
# job is admitted -- started, or merely queued -- its reads are retagged IN_USE, the
# rule stops matching them, and no amount of waiting can delete them out from under
# the run. Snakemake removes them itself once the last rule that reads them is done.
_RAW_STATE_TAG = "raw-state"
RAW_UNRUN = "unrun"
RAW_IN_USE = "in-use"


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
	_client.upload_file(
		str(file_path),
		_BUCKET,
		raw_key_for(job_id, file_path.name),
		ExtraArgs={"Tagging": f"{_RAW_STATE_TAG}={RAW_UNRUN}"},
	)


def upload_raw_fileobj(job_id: str, file_name: str, fileobj) -> None:
	"""Stream a raw FASTQ to S3 straight from the request body, as it arrives.

	upload_raw_file's counterpart for the path where the bytes are still in flight:
	it reads the object rather than a file on disk, so an upload reaches S3 during
	the upload instead of being read back off disk once it has landed.

	The stream is not seekable, so concurrency is capped: boto3 holds
	multipart_chunksize x max_concurrency bytes in memory while it works, and the
	default (8 MB x 10) is a lot to hold per concurrent upload on a small box."""
	if not is_enabled():
		return
	_client.upload_fileobj(
		fileobj,
		_BUCKET,
		raw_key_for(job_id, file_name),
		ExtraArgs={"Tagging": f"{_RAW_STATE_TAG}={RAW_UNRUN}"},
		Config=TransferConfig(multipart_chunksize=8 * 1024 * 1024, max_concurrency=4),
	)


def download_raw_file(job_id: str, file_name: str, destination_path: Path) -> None:
	"""Pull one raw FASTQ back down for a run.

	Raw reads are no longer kept on local disk between the upload and the run: S3
	holds them, and Snakemake fetches each one just before the rules that read it
	(see workflow/rules/raw.smk), which is what keeps peak disk proportional to the
	samples in flight rather than to the size of the batch.

	Raises if the object is missing -- unlike the backup helpers, this is not best
	effort. A run cannot proceed on reads it does not have, and a rule that fails
	loudly here is far better than one that quietly analyses nothing."""
	if not is_enabled():
		raise RuntimeError(
			"No S3 bucket is configured (RESULTS_S3_BUCKET), so raw reads cannot be "
			f"fetched: {destination_path} is missing and there is nowhere to get it from."
		)
	destination_path = Path(destination_path)
	destination_path.parent.mkdir(parents=True, exist_ok=True)
	# Download to a sibling temp file and rename, so a half-downloaded FASTQ can never
	# be mistaken for the real thing by a rule that only checks the path exists.
	staging_path = destination_path.with_name(destination_path.name + ".partial")
	try:
		_client.download_file(_BUCKET, raw_key_for(job_id, file_name), str(staging_path))
		staging_path.replace(destination_path)
	finally:
		staging_path.unlink(missing_ok=True)


def delete_raw_file(job_id: str, file_name: str) -> None:
	"""Remove a single raw object. Needed because an upload now reaches S3 before it
	has been checksum-verified: a file that fails verification is deleted locally, and
	its S3 copy has to go with it rather than linger as an orphan."""
	if not is_enabled():
		return
	_client.delete_object(Bucket=_BUCKET, Key=raw_key_for(job_id, file_name))


def _raw_keys(job_id: str):
	paginator = _client.get_paginator("list_objects_v2")
	for page in paginator.paginate(Bucket=_BUCKET, Prefix=raw_key_for(job_id, "")):
		for stored_object in page.get("Contents", []):
			yield stored_object["Key"]


def _set_raw_state(job_id: str, state: str) -> int:
	"""Retag every raw object of a job, and report how many were changed."""
	if not is_enabled():
		return 0
	tagged_count = 0
	for object_key in _raw_keys(job_id):
		_client.put_object_tagging(
			Bucket=_BUCKET,
			Key=object_key,
			Tagging={"TagSet": [{"Key": _RAW_STATE_TAG, "Value": state}]},
		)
		tagged_count += 1
	return tagged_count


def mark_raw_in_use(job_id: str) -> int:
	"""Exempt a job's reads from the 7-day expiry, because the job now needs them.

	Called when a run is admitted -- started OR queued, since a job waiting behind a
	full slot needs its reads just as much as one that is running, and a busy queue is
	exactly when a week could quietly go by. Once tagged, the lifecycle rule no longer
	matches the objects at all, so the reads survive any length of queue and any length
	of run; Snakemake deletes them when the rules that read them are finished.

	Raises on failure. This is not best effort: if the tag does not stick, the reads
	remain on a 7-day fuse while the job depends on them, and the caller needs to know."""
	return _set_raw_state(job_id, RAW_IN_USE)


def mark_raw_unrun(job_id: str) -> int:
	"""Put a job's reads back on the clock after a run ends without success.

	A failed or aborted run leaves reads that nothing is using. Without this they would
	stay tagged in-use and never expire, so every abandoned job would keep its FASTQ in
	the bucket forever -- the accumulation the 7-day rule exists to prevent. Expiry is
	by object age, so reads already older than 7 days when their run fails will be
	collected on the next pass rather than getting a fresh week."""
	return _set_raw_state(job_id, RAW_UNRUN)


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
