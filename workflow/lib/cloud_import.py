"""
Pull FASTQ files from a OneDrive or Google Drive share link.

The browser paths (/submit, /import) hand the app bytes the user already has on
their machine. Many times files may exist on a cloud service like OneDrive or
Google Drive, so this module lets the server fetch that folder itself.

That link is user-supplied input which the *server* then requests, which makes it
an SSRF vector: an unfiltered fetcher can be aimed at 169.254.169.254 or at any
internal address the instance can reach. Every request therefore goes through
_request(), which checks the URL against ALLOWED_HOSTS / ALLOWED_HOST_SUFFIXES
and re-checks *each redirect hop* -- a 1drv.ms short link bounces through two
hosts before it reaches the content server, so validating only the URL the user
pasted would leave the guard trivially bypassable.

What comes back over that connection is still untrusted, so the fetch stays
deliberately narrow: only FASTQ files and the sequencing company's .xlsx stats
workbook are pulled, each is capped in size, and each FASTQ must actually parse
as FASTQ before it is kept. The staging directory this leaves behind is handed
to import_samples.import_directory() by frontend._run_cloud_import(), so a
cloud pull reaches the manifest through exactly the same R1/R2 pairing and MD5
verification as a browser folder upload.
"""

import base64
import os
import re
from collections import deque
from pathlib import Path
from typing import NamedTuple, Optional
from urllib.parse import parse_qs, quote, urlencode, urljoin, urlparse, urlunparse

import requests
from werkzeug.utils import secure_filename

from workflow.lib import import_samples
from workflow.lib.preprocess import validate_fastq_integrity


class CloudImportError(Exception):
	"""A share link could not be turned into FASTQ files on disk.

	The message is shown to the user, so it says what to do about the failure --
	and never quotes a request URL, which carries the API key or access token in
	its query string (see _redact)."""

	def __init__(self, message, status_code=None):
		super().__init__(message)
		self.status_code = status_code


class CloudImportLimitError(CloudImportError):
	"""The pull has written as much as this server allows.

	Every other failure is per-file and is skipped with a warning, so one
	unreadable file cannot cost the user the other 23 isolates in the folder.
	This one ends the import instead: the disk is filling, and each further file
	only makes that worse."""


class RemoteFile(NamedTuple):
	"""One downloadable file behind a share link.

	`name` is None only on the anonymous single-file Google path, where there is
	no listing to read it from and it arrives with the download response's
	Content-Disposition header instead."""

	name: Optional[str]
	size: Optional[int]
	url: str


# Hosts the server will talk to. Google's Drive API and OneDrive/Graph both hand
# out one-time download URLs on per-region content hosts that cannot be
# enumerated, so those are matched by domain suffix; everything else is exact.
GOOGLE_HOSTS = {
	"drive.google.com",
	"docs.google.com",
	"drive.usercontent.google.com",
	"www.googleapis.com",
}
MICROSOFT_HOSTS = {
	"1drv.ms",
	"onedrive.live.com",
	"api.onedrive.com",
	"graph.microsoft.com",
}
GOOGLE_HOST_SUFFIXES = (".googleusercontent.com",)
MICROSOFT_HOST_SUFFIXES = (
	".sharepoint.com",
	".files.1drv.com",
	".microsoftpersonalcontent.com",
)

# Extra *content* hosts a deployment may need to accept on a redirect hop. It
# does not widen what counts as a share link: an entry URL still has to resolve
# to one of the two providers below.
_EXTRA_HOSTS = {
	host.strip().lower()
	for host in os.environ.get("CLOUD_IMPORT_ALLOWED_HOSTS", "").split(",")
	if host.strip()
}
ALLOWED_HOSTS = GOOGLE_HOSTS | MICROSOFT_HOSTS | _EXTRA_HOSTS
ALLOWED_HOST_SUFFIXES = GOOGLE_HOST_SUFFIXES + MICROSOFT_HOST_SUFFIXES

# Listing a Drive folder is not possible anonymously; with a key, folders and
# files both work. Without one, only a link to a single public file can be
# pulled. Graph is what reaches OneDrive for Business / SharePoint; consumer
# OneDrive share links resolve anonymously through api.onedrive.com.
GOOGLE_API_KEY = os.environ.get("GOOGLE_DRIVE_API_KEY", "").strip()
MS_GRAPH_TOKEN = os.environ.get("MS_GRAPH_ACCESS_TOKEN", "").strip()


def _int_env(variable_name, default):
	try:
		return max(1, int(os.environ.get(variable_name, "") or default))
	except ValueError:
		return default


# A share link is a blank cheque against the app's disk, so the pull is bounded.
# The defaults are far above a real sequencing run (a bacterial isolate pair is
# ~1 GB gzipped) and exist to stop a mistaken or hostile link, not normal use.
_MAX_FILE_BYTES = _int_env("CLOUD_IMPORT_MAX_FILE_BYTES", 20 * 1024**3)
_MAX_TOTAL_BYTES = _int_env("CLOUD_IMPORT_MAX_TOTAL_BYTES", 200 * 1024**3)
_MAX_FILES = _int_env("CLOUD_IMPORT_MAX_FILES", 500)
_MAX_DEPTH = 5

_SESSION = requests.Session()
_TIMEOUT = (10, 120)  # (connect, read) seconds
_MAX_REDIRECTS = 5
_REDIRECT_CODES = {301, 302, 303, 307, 308}
_CHUNK_BYTES = 1 << 20

_GOOGLE_FOLDER_MIME = "application/vnd.google-apps.folder"
_GOOGLE_ID_RE = re.compile(r"[A-Za-z0-9_-]+")
_GOOGLE_PATH_ID_RES = (
	re.compile(r"/file/d/([A-Za-z0-9_-]+)"),
	re.compile(r"/folders/([A-Za-z0-9_-]+)"),
	re.compile(r"/d/([A-Za-z0-9_-]+)"),
)


# Allowlist


def is_allowed_url(url):
	"""True if the server may issue a request to this URL."""
	try:
		parsed_url = urlparse(url or "")
	except ValueError:
		return False
	host_name = (parsed_url.hostname or "").lower()
	return (
		parsed_url.scheme == "https"
		and bool(host_name)
		and parsed_url.username is None
		and parsed_url.password is None
		and (host_name in ALLOWED_HOSTS or host_name.endswith(ALLOWED_HOST_SUFFIXES))
	)


def provider_for(url):
	"""'google', 'onedrive', or None for a host that is allowlisted for
	downloads but is not somewhere a share link can be resolved."""
	host_name = (urlparse(url or "").hostname or "").lower()
	if host_name in GOOGLE_HOSTS or host_name.endswith(GOOGLE_HOST_SUFFIXES):
		return "google"
	if host_name in MICROSOFT_HOSTS or host_name.endswith(MICROSOFT_HOST_SUFFIXES):
		return "onedrive"
	return None


def _redact(url):
	"""A URL without its query string. Request URLs carry the Drive API key and
	OneDrive share token, neither of which belongs in a message shown to a user
	or written to a log."""
	return urlunparse(urlparse(url)._replace(query="", fragment=""))


# HTTP


def _auth_headers(url):
	"""Bearer token for Graph only. The download URL Graph redirects us to lives
	on a content host, and forwarding the token there would hand it to a service
	that never needed it."""
	if (urlparse(url).hostname or "").lower() == "graph.microsoft.com" and MS_GRAPH_TOKEN:
		return {"Authorization": f"Bearer {MS_GRAPH_TOKEN}"}
	return {}


def _request(method, url, stream=False):
	"""Issue one request, following redirects by hand so that every hop is
	re-checked against the allowlist."""
	current_url = url
	for _redirect_count in range(_MAX_REDIRECTS + 1):
		if not is_allowed_url(current_url):
			raise CloudImportError(
				f"Refused to follow {_redact(current_url)}: not a Google Drive or OneDrive host."
			)
		try:
			response = _SESSION.request(
				method,
				current_url,
				headers=_auth_headers(current_url),
				stream=stream,
				timeout=_TIMEOUT,
				allow_redirects=False,
			)
		except requests.RequestException as exception:
			raise CloudImportError(
				f"Could not reach {_redact(current_url)}: {exception.__class__.__name__}."
			) from exception
		if response.status_code not in _REDIRECT_CODES:
			return response
		redirect_target = response.headers.get("Location", "")
		response.close()
		if not redirect_target:
			raise CloudImportError(f"{_redact(current_url)} redirected without a destination.")
		current_url = urljoin(current_url, redirect_target)
	raise CloudImportError("That share link redirected too many times.")


def _json(url, what):
	response = _request("GET", url)
	try:
		if response.status_code == 200:
			try:
				return response.json()
			except ValueError as exception:
				raise CloudImportError(f"{what} did not answer with JSON.") from exception
		if response.status_code in (401, 403):
			raise CloudImportError(
				f"{what} refused access ({response.status_code}). Check the item is shared with "
				f"“anyone with the link”.",
				status_code=response.status_code,
			)
		if response.status_code == 404:
			raise CloudImportError(
				f"{what} could not find that item (404). Check the link is complete and still shared.",
				status_code=404,
			)
		raise CloudImportError(
			f"{what} answered with HTTP {response.status_code}.", status_code=response.status_code
		)
	finally:
		response.close()


def _with_params(base_url, params):
	return f"{base_url}?{urlencode(params)}"


# Google Drive


def _google_ids(share_url):
	"""(file_or_folder_id, looks_like_folder) for a Drive link, or (None, ...)."""
	parsed_url = urlparse(share_url)
	query_id = parse_qs(parsed_url.query).get("id", [""])[0]
	if query_id and _GOOGLE_ID_RE.fullmatch(query_id):
		drive_id = query_id
	else:
		matches = (pattern.search(parsed_url.path) for pattern in _GOOGLE_PATH_ID_RES)
		drive_id = next((match.group(1) for match in matches if match), None)
	looks_like_folder = "/folders/" in parsed_url.path or parsed_url.path.endswith("/folderview")
	return drive_id, looks_like_folder


def _google_entry(item):
	drive_id = str(item.get("id", ""))
	if not _GOOGLE_ID_RE.fullmatch(drive_id):
		raise CloudImportError("Google Drive returned an item with an unusable ID.")
	raw_size = str(item.get("size", ""))
	return RemoteFile(
		name=item.get("name"),
		size=int(raw_size) if raw_size.isdigit() else None,
		url=_with_params(
			f"https://www.googleapis.com/drive/v3/files/{drive_id}",
			{"alt": "media", "key": GOOGLE_API_KEY, "supportsAllDrives": "true"},
		),
	)


def _google_walk(folder_id):
	"""Every file at or under a Drive folder, flattened."""
	remote_files, warnings = [], []
	folder_queue = deque([(folder_id, 0)])
	while folder_queue and len(remote_files) <= _MAX_FILES:
		current_folder_id, depth = folder_queue.popleft()
		page_token = None
		while True:
			list_params = {
				"q": f"'{current_folder_id}' in parents and trashed = false",
				"key": GOOGLE_API_KEY,
				"fields": "nextPageToken,files(id,name,size,mimeType)",
				"pageSize": "1000",
				"supportsAllDrives": "true",
				"includeItemsFromAllDrives": "true",
			}
			if page_token:
				list_params["pageToken"] = page_token
			page = _json(
				_with_params("https://www.googleapis.com/drive/v3/files", list_params),
				"Google Drive",
			)
			for item in page.get("files", []):
				if item.get("mimeType") == _GOOGLE_FOLDER_MIME:
					if depth < _MAX_DEPTH:
						folder_queue.append((str(item.get("id", "")), depth + 1))
					else:
						warnings.append(
							f"Skipped subfolder {item.get('name')}: more than {_MAX_DEPTH} levels deep."
						)
					continue
				remote_files.append(_google_entry(item))
			page_token = page.get("nextPageToken")
			if not page_token:
				break
	return remote_files, warnings


def _google_list(share_url):
	drive_id, looks_like_folder = _google_ids(share_url)
	if not drive_id:
		raise CloudImportError("That Google Drive link carries no file or folder ID.")

	if not GOOGLE_API_KEY:
		if looks_like_folder:
			raise CloudImportError(
				"Reading a Google Drive folder needs a Drive API key, which this server does not "
				"have configured (GOOGLE_DRIVE_API_KEY). Paste a link to a single FASTQ file "
				"instead, or upload the folder with “Bulk Import” above."
			)
		# No listing to read a name from; it arrives with the download headers.
		return [
			RemoteFile(
				name=None,
				size=None,
				url=_with_params(
					"https://drive.usercontent.google.com/download",
					{"id": drive_id, "export": "download", "confirm": "t"},
				),
			)
		], []

	item = _json(
		_with_params(
			f"https://www.googleapis.com/drive/v3/files/{drive_id}",
			{"key": GOOGLE_API_KEY, "fields": "id,name,size,mimeType", "supportsAllDrives": "true"},
		),
		"Google Drive",
	)
	if item.get("mimeType") == _GOOGLE_FOLDER_MIME:
		return _google_walk(drive_id)
	return [_google_entry(item)], []


# OneDrive / SharePoint


def _share_token(share_url):
	"""Microsoft's encoding of a sharing URL into an addressable share ID."""
	return "u!" + base64.urlsafe_b64encode(share_url.encode()).decode().rstrip("=")


def _ms_url(share_token, relative_path, suffix):
	"""Address an item under a share root. Downloads go through /content rather
	than the listing's @downloadUrl so there is one code path, and so the
	redirect to the content host is walked (and re-checked) by _request."""
	api_base = (
		"https://graph.microsoft.com/v1.0" if MS_GRAPH_TOKEN else "https://api.onedrive.com/v1.0"
	)
	if relative_path:
		return f"{api_base}/shares/{share_token}/root:/{quote(relative_path)}:{suffix}"
	return f"{api_base}/shares/{share_token}/root{suffix}"


def _ms_children(share_token, relative_path):
	children, next_url = [], _ms_url(share_token, relative_path, "/children")
	while next_url:
		page = _json(next_url, "OneDrive")
		children.extend(page.get("value", []))
		next_url = page.get("@odata.nextLink")
	return children


def _onedrive_walk(share_token):
	remote_files, warnings = [], []
	folder_queue = deque([("", 0)])
	while folder_queue and len(remote_files) <= _MAX_FILES:
		relative_path, depth = folder_queue.popleft()
		try:
			children = _ms_children(share_token, relative_path)
		except CloudImportError as exception:
			# One unreadable subfolder must not sink the whole pull: the FASTQs
			# almost always sit at the top level of what the company shared.
			warnings.append(f"Could not list {relative_path or 'the shared folder'}: {exception}")
			continue
		for child in children:
			child_name = child.get("name") or ""
			if not child_name or "/" in child_name or ":" in child_name:
				warnings.append(f"Skipped an item with an unusable name ({child_name!r}).")
				continue
			child_path = f"{relative_path}/{child_name}".lstrip("/")
			if "folder" in child:
				if depth < _MAX_DEPTH:
					folder_queue.append((child_path, depth + 1))
				else:
					warnings.append(
						f"Skipped subfolder {child_name}: more than {_MAX_DEPTH} levels deep."
					)
				continue
			remote_files.append(
				RemoteFile(
					name=child_name,
					size=child.get("size"),
					url=_ms_url(share_token, child_path, "/content"),
				)
			)
	return remote_files, warnings


def _onedrive_list(share_url):
	share_token = _share_token(share_url)
	try:
		root_item = _json(_ms_url(share_token, "", ""), "OneDrive")
	except CloudImportError as exception:
		if exception.status_code in (401, 403) and not MS_GRAPH_TOKEN:
			raise CloudImportError(
				f"{exception} A OneDrive for Business or SharePoint link also needs the server to "
				f"have MS_GRAPH_ACCESS_TOKEN configured.",
				status_code=exception.status_code,
			) from exception
		raise
	if "folder" not in root_item:
		return [
			RemoteFile(
				name=root_item.get("name"),
				size=root_item.get("size"),
				url=_ms_url(share_token, "", "/content"),
			)
		], []
	return _onedrive_walk(share_token)


# Download


def _is_stats_workbook(file_name):
	return file_name.lower().endswith(".xlsx") and not file_name.startswith("~$")


def _accept_name(file_name):
	"""(safe_name, None) for a file worth downloading, else (None, reason).

	The filetype allowlist: the pipeline reads FASTQ, and import_directory reads
	the sequencing company's stats workbook for the MD5s it verifies against.
	Nothing else in a shared folder has any business on this server."""
	safe_name = secure_filename(file_name or "")
	if not safe_name:
		return None, f"Skipped a file whose name cannot be used ({file_name!r})."
	if import_samples.is_fastq(safe_name) or _is_stats_workbook(safe_name):
		return safe_name, None
	return None, f"Skipped {safe_name}: not a FASTQ file or a sequencing stats workbook."


def _filename_from_headers(response_headers):
	content_disposition = response_headers.get("Content-Disposition", "")
	extended_match = re.search(r"filename\*=UTF-8''([^;]+)", content_disposition, re.IGNORECASE)
	if extended_match:
		from urllib.parse import unquote

		return unquote(extended_match.group(1)).strip()
	quoted_match = re.search(r'filename="([^"]+)"', content_disposition, re.IGNORECASE)
	if quoted_match:
		return quoted_match.group(1).strip()
	return ""


def _download(remote_file, dest_dir, remaining_bytes):
	"""Stream one remote file into dest_dir. Returns the path it was written to."""
	response = _request("GET", remote_file.url, stream=True)
	try:
		if response.status_code != 200:
			raise CloudImportError(
				f"Downloading {remote_file.name or 'the shared file'} failed with HTTP "
				f"{response.status_code}.",
				status_code=response.status_code,
			)
		if response.headers.get("Content-Type", "").startswith("text/html"):
			# Both providers answer an unshared, throttled, or scan-warned link
			# with a 200 and an HTML page rather than an HTTP error, so a fetcher
			# that trusts the status code writes a sign-in page into a .fastq.gz.
			raise CloudImportError(
				f"{remote_file.name or 'That link'} came back as a web page instead of a file. "
				f"It is probably not shared with “anyone with the link”."
			)

		safe_name, reason = _accept_name(
			remote_file.name or _filename_from_headers(response.headers)
		)
		if reason:
			raise CloudImportError(reason)

		destination_path = Path(dest_dir) / safe_name
		bytes_written = 0
		try:
			with destination_path.open("wb") as destination_file:
				for chunk in response.iter_content(chunk_size=_CHUNK_BYTES):
					bytes_written += len(chunk)
					if bytes_written > _MAX_FILE_BYTES:
						raise CloudImportError(
							f"{safe_name} is bigger than this server's {_MAX_FILE_BYTES}-byte "
							f"per-file import limit."
						)
					if bytes_written > remaining_bytes:
						raise CloudImportLimitError(
							f"The files behind that link exceed this server's "
							f"{_MAX_TOTAL_BYTES}-byte limit for one import."
						)
					destination_file.write(chunk)
		except CloudImportError:
			destination_path.unlink(missing_ok=True)
			raise
		return destination_path
	finally:
		response.close()


def fetch_share_link(share_url, dest_dir, progress=None):
	"""Download the FASTQ files behind a OneDrive/Google Drive share link into
	dest_dir, flattened, alongside the stats workbook if the folder carries one.

	The caller is expected to hand dest_dir straight to
	import_samples.import_directory(), which does the R1/R2 pairing and verifies
	each file's MD5 against that workbook -- the same treatment a browser folder
	upload gets. Raises CloudImportError, whose message is safe to show the user.
	"""
	share_url = (share_url or "").strip()
	if not is_allowed_url(share_url):
		raise CloudImportError(
			"That is not a Google Drive or OneDrive share link. Copy the https:// link the "
			"sequencing company sent you straight from the browser's address bar."
		)

	provider = provider_for(share_url)
	if provider == "google":
		remote_files, warnings = _google_list(share_url)
	elif provider == "onedrive":
		remote_files, warnings = _onedrive_list(share_url)
	else:
		raise CloudImportError("That host can be downloaded from but is not a share link.")

	dest_dir = Path(dest_dir)
	dest_dir.mkdir(parents=True, exist_ok=True)

	# Decide what to pull *before* pulling any of it, so a folder full of BAMs
	# and PDFs costs one listing call rather than a disk full of skipped files.
	queued_files, claimed_names = [], set()
	for remote_file in remote_files:
		if remote_file.name is None:
			queued_files.append(remote_file)  # anonymous Drive file; named by its headers
			continue
		safe_name, reason = _accept_name(remote_file.name)
		if reason:
			warnings.append(reason)
			continue
		if safe_name in claimed_names:
			warnings.append(f"Skipped a second copy of {safe_name} from elsewhere in the folder.")
			continue
		claimed_names.add(safe_name)
		queued_files.append(remote_file)

	if not queued_files:
		raise CloudImportError(
			"Nothing behind that link is a FASTQ file (looked for .fastq, .fq, .fastq.gz, .fq.gz)."
		)
	if len(queued_files) > _MAX_FILES:
		raise CloudImportError(
			f"That link holds {len(queued_files)} files, over this server's {_MAX_FILES}-file "
			f"limit for one import."
		)

	downloaded_names, total_bytes = [], 0
	for file_number, remote_file in enumerate(queued_files, start=1):
		if progress:
			progress(
				file_number - 1,
				len(queued_files),
				f"Downloading {remote_file.name or 'file'} ({file_number} of {len(queued_files)})…",
			)
		try:
			destination_path = _download(remote_file, dest_dir, _MAX_TOTAL_BYTES - total_bytes)
		except CloudImportLimitError:
			raise
		except CloudImportError as exception:
			# One unusable file must not cost the user the rest of the folder --
			# the same call import_directory makes for an unpaired read or a
			# checksum mismatch. Its mate is left without a partner and drops out
			# of the manifest on its own.
			warnings.append(str(exception))
			continue
		total_bytes += destination_path.stat().st_size

		if import_samples.is_fastq(destination_path.name):
			is_valid, validation_message = validate_fastq_integrity(str(destination_path))
			if not is_valid:
				# The bytes on the far end of a share link are whatever the sharer
				# put there. Only a file that actually reads as FASTQ is allowed
				# to reach the manifest.
				destination_path.unlink(missing_ok=True)
				warnings.append(
					f"Discarded {destination_path.name}: it did not download as a readable FASTQ "
					f"({validation_message})."
				)
				continue
		downloaded_names.append(destination_path.name)

	if progress:
		progress(len(queued_files), len(queued_files), "Download complete.")

	if not any(import_samples.is_fastq(file_name) for file_name in downloaded_names):
		raise CloudImportError(
			"No readable FASTQ file could be downloaded from that link. " + " ".join(warnings)
		)

	return {
		"provider": provider,
		"downloaded": downloaded_names,
		"warnings": warnings,
		"bytes": total_bytes,
	}
