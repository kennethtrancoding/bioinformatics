"""
BV-BRC API Client
Wrapper for BV-BRC REST API endpoints
Handles authentication, upload, job submission, polling, and download
"""

import base64
import json
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests

from workflow.lib.jobs import is_valid_isolate_id
from workflow.lib.utils import retry, setup_logger

logger = setup_logger("bvbrc_client")


def _require_safe_identifier(identifier_value: str, label: str) -> None:
	"""Guard against a sample_id/workspace_name that would corrupt a BV-BRC
	remote workspace path (e.g. containing '/' or '..') before it's interpolated
	into one."""
	if not is_valid_isolate_id(identifier_value):
		raise ValueError(f"Unsafe {label}: {identifier_value!r}")


class BVBRCClient:
	"""
	BV-BRC API client for genome analysis workflow.

	BV-BRC uses two JSON-RPC 1.1 services:
	  - Workspace: https://p3.theseed.org/services/Workspace
	  - AppService: https://p3.theseed.org/services/app_service
	"""

	AUTH_URL = "https://user.patricbrc.org/authenticate"
	WORKSPACE_URL = "https://p3.theseed.org/services/Workspace"
	APP_SERVICE_URL = "https://p3.theseed.org/services/app_service"

	def __init__(self, token_file: str = ".bvbrc_token", job_id: Optional[str] = None):
		"""
		Initialize BV-BRC client.

		Args:
		    token_file: Path to stored access token
		    job_id: Optional job ID for job-specific API endpoint overrides
		"""
		if job_id is not None:
			from workflow.lib.jobs import is_valid_job_id, job_token_path

			if not is_valid_job_id(job_id):
				raise ValueError("Invalid job ID")
			# A job ID is the authorization boundary. Never fall back to the old
			# process-wide token, even when a caller passes the legacy YAML value.
			self.token_file = job_token_path(job_id)
		else:
			self.token_file = Path(token_file)
		self.token = None
		self.workspace = None
		self.user_id = None
		self.job_id = job_id

		# Apply any endpoint URL overrides saved from the Settings page so the
		# UI is the single place endpoints are configured. Falls back silently
		# to the class defaults if the registry isn't importable.
		# Job-specific overrides take precedence over global overrides.
		try:
			from workflow.lib.api_registry import load_overrides

			overrides = load_overrides(job_id=job_id)
			self.AUTH_URL = overrides.get("auth", self.AUTH_URL)
			self.WORKSPACE_URL = overrides.get("workspace", self.WORKSPACE_URL)
			self.APP_SERVICE_URL = overrides.get("app_service", self.APP_SERVICE_URL)
		except Exception:
			pass

		# Try to load existing token
		if self.token_file.exists():
			self._load_token()

	# ========================================================================
	# Authentication
	# ========================================================================

	def _load_token(self) -> bool:
		"""
		Load saved BV-BRC access token.

		Returns:
		    True if token loaded successfully, False otherwise
		"""
		try:
			with self.token_file.open("r") as file_handle:
				response_data = json.load(file_handle)
				self.token = response_data.get("access_token")
				self.user_id = response_data.get("user_id")
				if self.token:
					logger.info(f"Loaded BV-BRC token for user: {self.user_id}")
					return True
		except Exception as exception:
			logger.warning(f"Failed to load token: {exception}")

		return False

	def _save_token(self, token: str, user_id: str) -> bool:
		"""
		Save BV-BRC access token to file.

		Args:
		    token: Access token
		    user_id: BV-BRC user ID

		Returns:
		    True if saved successfully
		"""
		try:
			response_data = {"access_token": token, "user_id": user_id, "saved_at": time.time()}
			self.token_file.parent.mkdir(parents=True, exist_ok=True)
			with self.token_file.open("w") as file_handle:
				json.dump(response_data, file_handle, indent=2)
			self.token_file.chmod(0o600)  # Restrict file permissions
			logger.info(f"Saved BV-BRC token for user: {user_id}")
			return True
		except Exception as exception:
			logger.error(f"Failed to save token: {exception}")
			return False

	@retry(max_attempts=3, delay=2.0, backoff=2.0, exceptions=(requests.RequestException,))
	def login(self, username: str, password: str) -> bool:
		"""
		Authenticate with BV-BRC and obtain access token.

		Args:
		    username: BV-BRC username or email
		    password: BV-BRC password

		Returns:
		    True if authentication successful
		"""
		logger.info(f"Authenticating with BV-BRC as {username}...")

		try:
			response = requests.post(
				self.AUTH_URL,
				data={"username": username, "password": password},
				headers={"Content-Type": "application/x-www-form-urlencoded"},
				timeout=10,
			)
			response.raise_for_status()

			token = response.text.strip()
			if not token:
				logger.error("Empty token in response")
				return False

			self.token = token
			# Parse the real user ID from the token (format: un=user@bvbrc|tokenid=...|...)
			token_fields = {
				token_field_key: token_field_value
				for token_field_key, token_field_value in (
					token_field.split("=", 1)
					for token_field in token.split("|")
					if "=" in token_field
				)
			}
			self.user_id = token_fields.get("un", username)
			self._save_token(self.token, self.user_id)
			logger.info(f"✓ Authentication successful for {username}")
			return True

		except requests.exceptions.HTTPError as exception:
			logger.error(
				f"Authentication failed: {exception.response.status_code} {exception.response.text}"
			)
			return False
		except Exception as exception:
			logger.error(f"Authentication error: {exception}")
			raise

	def login_interactive(self) -> bool:
		"""
		Interactive login prompt for username and password.

		Returns:
		    True if authentication successful
		"""
		import getpass

		username = input("BV-BRC username/email: ").strip()
		password = getpass.getpass("BV-BRC password: ")

		return self.login(username, password)

	def is_authenticated(self) -> bool:
		"""
		Check if client has valid token.

		Returns:
		    True if token exists
		"""
		return self.token is not None

	def destroy_token(self) -> bool:
		"""
		Securely destroy the saved BV-BRC access token.

		Returns:
		    True if destroyed successfully
		"""
		try:
			if self.token_file.exists():
				self.token_file.unlink()
			self.token = None
			self.user_id = None
			logger.info("BV-BRC token destroyed")
			return True
		except Exception as exception:
			logger.error(f"Failed to destroy token: {exception}")
			return False

	def _get_headers(self) -> Dict[str, str]:
		headers = {"Content-Type": "application/json", "Accept": "application/json"}
		if self.token:
			headers["Authorization"] = self.token
		return headers

	def _rpc(self, service_url: str, method: str, params: list, timeout: int = 300) -> Any:
		"""
		Make a JSON-RPC 1.1 call to a BV-BRC service.

		Args:
		    service_url: WORKSPACE_URL or APP_SERVICE_URL
		    method: Full method name e.g. "Workspace.create"
		    params: List of positional params
		    timeout: Request timeout in seconds

		Returns:
		    The 'result' field from the JSON-RPC response

		Raises:
		    RuntimeError: If the server returns an error object
		"""
		payload = {"method": method, "version": "1.1", "id": 1, "params": params}
		response = requests.post(
			service_url,
			json=payload,
			headers=self._get_headers(),
			timeout=timeout,
		)
		response.raise_for_status()
		response_data = response.json()
		if "error" in response_data and response_data["error"]:
			raise RuntimeError(f"RPC error from {method}: {response_data['error']}")
		return response_data.get("result")

	# ========================================================================
	# Workspace Management
	# ========================================================================

	def get_or_create_workspace(self, workspace_name: str) -> Optional[str]:
		"""
		Get or create a folder in the BV-BRC workspace.
		BV-BRC workspace paths follow the format:
		/{username}@patricbrc.org/{workspace_name}

		Args:
		    workspace_name: Name of workspace folder

		Returns:
		    Workspace path string, or None if not authenticated
		"""
		_require_safe_identifier(workspace_name, "workspace_name")

		if not self.is_authenticated():
			logger.error("Not authenticated. Call login() first.")
			return None

		# user_id is already the full identifier e.g. "kennethtran@bvbrc"
		workspace_path = f"/{self.user_id}/home/{workspace_name}"
		self._ensure_remote_dir(workspace_path)
		self.workspace = workspace_path
		logger.info(f"Using workspace: {workspace_path}")
		return workspace_path

	# ========================================================================
	# File Upload
	# ========================================================================

	@retry(max_attempts=3, delay=5.0, backoff=2.0, exceptions=(requests.RequestException,))
	def upload_file(self, local_path: str, remote_path: str, file_type: str = "reads") -> bool:
		"""
		Upload a file to the BV-BRC workspace using the shock two-step protocol.

		For large files, BV-BRC uses shock storage:
		  1. Call Workspace.create with createUploadNodes=1 to reserve a slot
		     and receive a shock upload URL.
		  2. HTTP PUT the file to that shock URL as multipart/form-data
		     with Authorization: OAuth {token}.

		Args:
		    local_path: Path to local file
		    remote_path: Destination path in workspace
		    file_type: BV-BRC file type: "reads", "fasta", "contigs"

		Returns:
		    True if upload successful
		"""
		if not self.is_authenticated():
			logger.error("Not authenticated")
			return False

		local_path = Path(local_path)
		if not local_path.exists():
			logger.error(f"Local file not found: {local_path}")
			return False

		file_size = local_path.stat().st_size
		logger.info(f"Uploading {local_path} ({file_size / 1e6:.1f} MB) → {remote_path}")

		try:
			parent_path = remote_path.rsplit("/", 1)[0]
			self._ensure_remote_dir(parent_path)

			# Step 1: reserve a workspace slot and get the shock upload URL
			rpc_result = self._rpc(
				self.WORKSPACE_URL,
				"Workspace.create",
				[
					{
						"objects": [[remote_path, file_type, {}, None]],
						"overwrite": 1,
						"createUploadNodes": 1,
					}
				],
				timeout=30,
			)

			if not rpc_result or not rpc_result[0] or not rpc_result[0][0]:
				raise RuntimeError("Workspace.create returned no result")

			shock_url = rpc_result[0][0][11]
			if not shock_url:
				raise RuntimeError("No shock URL returned by Workspace.create")

			logger.info(f"Uploading to shock node: {shock_url}")

			# Step 2: PUT file content to the shock URL as multipart/form-data
			with local_path.open("rb") as file_handle:
				response = requests.put(
					shock_url,
					files={"upload": (local_path.name, file_handle, "application/octet-stream")},
					headers={"Authorization": f"OAuth {self.token}"},
					timeout=600,
				)
			response.raise_for_status()

			logger.info(f"✓ Upload complete: {remote_path}")
			return True

		except requests.exceptions.RequestException as exception:
			logger.error(f"Upload failed: {exception}")
			raise
		except Exception as exception:
			logger.error(f"Upload error: {exception}")
			return False

	def upload_paired_reads(
		self, r1_path: str, r2_path: str, sample_id: str
	) -> Tuple[bool, Optional[str], Optional[str]]:
		"""
		Upload paired-end FASTQ files to workspace.

		Args:
		    r1_path: Path to R1 FASTQ file
		    r2_path: Path to R2 FASTQ file
		    sample_id: Sample identifier for remote paths

		Returns:
		    Tuple of (success, r1_remote_path, r2_remote_path)
		"""
		if not self.workspace:
			logger.error("No workspace set")
			return False, None, None

		try:
			_require_safe_identifier(sample_id, "sample_id")

			# Construct remote paths
			r1_remote = f"{self.workspace}/reads/{sample_id}_R1.fastq.gz"
			r2_remote = f"{self.workspace}/reads/{sample_id}_R2.fastq.gz"

			# Upload both files
			success_r1 = self.upload_file(r1_path, r1_remote, "reads")
			success_r2 = self.upload_file(r2_path, r2_remote, "reads")

			if success_r1 and success_r2:
				logger.info(f"✓ Paired reads uploaded for {sample_id}")
				return True, r1_remote, r2_remote
			else:
				logger.error(f"Failed to upload paired reads for {sample_id}")
				return False, None, None

		except Exception as exception:
			logger.error(f"Error uploading paired reads: {exception}")
			return False, None, None

	def _ensure_remote_dir(self, remote_path: str) -> bool:
		"""Create a folder in the BV-BRC workspace if it doesn't exist."""
		try:
			self._rpc(
				self.WORKSPACE_URL,
				"Workspace.create",
				[
					{
						"objects": [[remote_path, "folder", {}, None]],
						"overwrite": 1,
					}
				],
				timeout=30,
			)
			return True
		except Exception as exception:
			logger.debug(f"Could not create directory {remote_path}: {exception}")
			return False

	# ========================================================================
	# Job Submission
	# ========================================================================

	@retry(max_attempts=2, delay=3.0, backoff=2.0, exceptions=(requests.RequestException,))
	def submit_taxonomic_classification(
		self, r1_file: str, r2_file: str, sample_id: str
	) -> Optional[str]:
		"""
		Submit TaxonomicClassification (Kraken2) job to identify bacterial genus.

		Args:
		    r1_file: Remote workspace path to R1 FASTQ
		    r2_file: Remote workspace path to R2 FASTQ
		    sample_id: Sample identifier used for output folder name

		Returns:
		    Job/task ID, or None if submission failed
		"""
		if not self.workspace:
			logger.error("No workspace set")
			return None

		logger.info(f"Submitting TaxonomicClassification job for {sample_id}...")

		try:
			_require_safe_identifier(sample_id, "sample_id")

			output_name = f"taxclass_{sample_id}"
			# NOTE: As of 2026-06-17 BV-BRC's TaxonomicClassification app fails
			# server-side at runtime regardless of these params — its wrapper.py
			# aborts with a generic "wrapper command failed 256" ~7s after start
			# (the preflight passes, so params are accepted; the failure is inside
			# BV-BRC's app). Tried database "standard" and "bvbrc", with/without
			# save_*/confidence_interval/host_genome — all fail identically. The
			# genus step is non-blocking (callers fall back to "Unknown" genus, and
			# species is obtained reliably from the MLST step instead).
			# "bvbrc" is the lighter curated bacterial DB. (valid: bvbrc, standard,
			# Greengenes, SILVA)
			params = {
				"input_type": "reads",
				"paired_end_libs": [
					{
						"sample_id": sample_id,
						"read1": r1_file,
						"read2": r2_file,
						"platform": "illumina",
					}
				],
				"algorithm": "Kraken2",
				"database": "bvbrc",
				"output_path": self.workspace,
				"output_file": output_name,
			}
			rpc_result = self._rpc(
				self.APP_SERVICE_URL,
				"AppService.start_app",
				["TaxonomicClassification", params, self.workspace],
				timeout=60,
			)
			job_id = (
				str(rpc_result[0]["id"])
				if isinstance(rpc_result, list) and rpc_result
				else str(rpc_result)
			)
			logger.info(f"✓ TaxonomicClassification job submitted: {job_id}")
			return job_id

		except Exception as exception:
			logger.error(f"Failed to submit TaxonomicClassification: {exception}")
			return None

	@retry(max_attempts=2, delay=3.0, backoff=2.0, exceptions=(requests.RequestException,))
	def submit_comprehensive_genome_analysis(
		self,
		r1_file: str,
		r2_file: str,
		output_name: str,
		genus: Optional[str] = None,
		taxonomy_id: Optional[int] = None,
		assembly_method: str = "auto",
	) -> Optional[str]:
		"""
		Submit Comprehensive Genome Analysis job (assembly + annotation).

		Args:
		    r1_file: Remote workspace path to R1 FASTQ
		    r2_file: Remote workspace path to R2 FASTQ
		    output_name: Name for output folder
		    genus: Bacterial genus (optional, improves annotation)
		    assembly_method: "auto", "spades", or "kmer"

		Returns:
		    Job/task ID, or None if submission failed
		"""
		if not self.workspace:
			logger.error("No workspace set")
			return None

		logger.info("Submitting Comprehensive Genome Analysis job...")

		try:
			_require_safe_identifier(output_name, "output_name")

			output_path = f"{self.workspace}/{output_name}"
			# scientific_name + taxonomy_id are required by BV-BRC annotation.
			# Providing taxonomy_id directly avoids BV-BRC doing a taxonomy lookup
			# that fails when the name isn't in NCBI (e.g. "Unknown bacterium").
			if genus and genus not in ("Unknown", ""):
				scientific_name = f"{genus} sp."
			else:
				scientific_name = "Bacteria"

			# If no taxonomy_id supplied, look it up (falls back to 2 = Bacteria)
			if taxonomy_id is None:
				taxonomy_id = self.get_taxonomy_id(genus or "")

			recipe = (
				assembly_method
				if assembly_method
				in (
					"auto",
					"unicycler",
					"canu",
					"spades",
					"meta-spades",
					"plasmid-spades",
					"single-cell",
				)
				else "auto"
			)

			params = {
				"input_type": "reads",
				"paired_end_libs": [{"read1": r1_file, "read2": r2_file, "platform": "illumina"}],
				"recipe": recipe,
				"scientific_name": scientific_name,
				"taxonomy_id": taxonomy_id,
				"domain": "Bacteria",
				"code": 11,
				"output_file": output_name,
				"output_path": self.workspace,
				"min_contig_len": 500,
			}

			rpc_result = self._rpc(
				self.APP_SERVICE_URL,
				"AppService.start_app",
				["ComprehensiveGenomeAnalysis", params, output_path],
				timeout=60,
			)
			# result is [task_dict]; extract the integer task id
			job_id = (
				str(rpc_result[0]["id"])
				if isinstance(rpc_result, list) and rpc_result
				else str(rpc_result)
			)
			logger.info(f"✓ Comprehensive Genome Analysis job submitted: {job_id}")
			return job_id

		except Exception as exception:
			logger.error(f"Failed to submit Comprehensive Genome Analysis: {exception}")
			return None

	# ========================================================================
	# Job Status & Results
	# ========================================================================

	@retry(max_attempts=3, delay=2.0, backoff=2.0, exceptions=(requests.RequestException,))
	def get_job_status(self, job_id: str) -> Optional[Dict[str, Any]]:
		"""
		Query job status via AppService.

		Args:
		    job_id: BV-BRC task ID

		Returns:
		    Dictionary with job status info, or None if error
		"""
		try:
			rpc_result = self._rpc(
				self.APP_SERVICE_URL, "AppService.query_tasks", [[job_id]], timeout=30
			)
			# result is [mapping<task_id, task_dict>]; key is always the string job_id
			if rpc_result and isinstance(rpc_result, list) and rpc_result[0]:
				task_map = rpc_result[0]
				return task_map.get(str(job_id))
			return None
		except Exception as exception:
			logger.error(f"Failed to get job status: {exception}")
			return None

	def wait_for_job(
		self, job_id: str, max_wait_seconds: int = 3600, poll_interval: int = 30
	) -> Tuple[bool, Optional[str]]:
		"""
		Poll job status until completion or timeout.

		Args:
		    job_id: BV-BRC job ID
		    max_wait_seconds: Maximum time to wait (default: 1 hour)
		    poll_interval: Seconds between polls (default: 30)

		Returns:
		    Tuple of (is_complete, final_status)
		"""
		start_time = time.time()
		last_status = None

		while True:
			elapsed = time.time() - start_time

			if elapsed > max_wait_seconds:
				logger.warning(f"Job {job_id} did not complete after {max_wait_seconds}s")
				return False, last_status

			# Query job status
			job_info = self.get_job_status(job_id)

			if job_info:
				# query_tasks returns status: 'in-progress', 'completed', 'failed'
				status = job_info.get("status", "unknown")
				last_status = status

				if status == "completed":
					logger.info(f"✓ Job {job_id} completed")
					return True, status

				elif status == "failed":
					logger.error(f"✗ Job {job_id} failed")
					return False, status

				else:
					logger.info(f"Job {job_id} status: {status} [{elapsed:.0f}s elapsed]")

			# Wait before next poll
			time.sleep(poll_interval)

	def get_taxonomy_id(self, genus: str) -> Optional[int]:
		"""
		Look up the NCBI taxonomy ID for a bacterial genus via BV-BRC taxonomy API.

		Returns the taxon_id integer, or None if not found.
		Falls back to 2 (Bacteria) so callers always get something usable.
		"""
		if not genus or genus in ("Unknown", ""):
			return 2  # Bacteria superkingdom
		try:
			url = (
				f"https://www.bv-brc.org/api/taxonomy"
				f"?eq(taxon_name,{genus})"
				f"&eq(taxon_rank,genus)"
				f"&select(taxon_id,taxon_name,taxon_rank)"
				f"&limit(1)"
			)
			response = requests.get(url, headers={"Accept": "application/json"}, timeout=30)
			response.raise_for_status()
			response_data = response.json()
			if (
				response_data
				and isinstance(response_data, list)
				and response_data[0].get("taxon_id")
			):
				taxon_id = int(response_data[0]["taxon_id"])
				logger.info(f"Taxonomy lookup: {genus} → taxon_id {taxon_id}")
				return taxon_id
		except Exception as exception:
			logger.warning(f"Taxonomy lookup failed for {genus}: {exception}")
		return 2  # fallback: Bacteria

	# ========================================================================
	# Download Results
	# ========================================================================

	@retry(max_attempts=3, delay=2.0, backoff=2.0, exceptions=(requests.RequestException,))
	def download_file(self, remote_path: str, local_path: str) -> bool:
		"""
		Download a file from the BV-BRC workspace.

		Args:
		    remote_path: Workspace file path
		    local_path: Local destination path

		Returns:
		    True if download successful
		"""
		if not self.is_authenticated():
			logger.error("Not authenticated")
			return False

		local_path = Path(local_path)
		logger.info(f"Downloading {remote_path} → {local_path}...")

		try:
			rpc_result = self._rpc(
				self.WORKSPACE_URL,
				"Workspace.get",
				[{"objects": [remote_path], "decoded": 1}],
				timeout=300,
			)

			logger.info(f"Workspace.get result: {rpc_result}")

			local_path.parent.mkdir(parents=True, exist_ok=True)

			# result is a list of [metadata..., content]
			objects = rpc_result[0] if rpc_result else []
			if not objects:
				logger.error(f"No data returned for {remote_path}")
				return False

			workspace_entry = objects[0]
			content = workspace_entry[1]

			# BV-BRC returns one of two things as the second element:
			#   - a Shock node URL (large files are stored in Shock, not inline)
			#   - the file content itself (small files, base64-encoded)
			if isinstance(content, str) and content.startswith(("http://", "https://")):
				# Shock-stored file: fetch the bytes with the auth token.
				shock_download_url = content + "?download"
				logger.info(f"Fetching from Shock node: {content}")
				with requests.get(
					shock_download_url,
					headers={"Authorization": f"OAuth {self.token}"},
					stream=True,
					timeout=600,
				) as response:
					response.raise_for_status()
					with local_path.open("wb") as file_handle:
						for chunk in response.iter_content(chunk_size=1 << 20):
							if chunk:
								file_handle.write(chunk)
			else:
				# Inline content.
				with local_path.open("wb") as file_handle:
					if isinstance(content, str):
						file_handle.write(base64.b64decode(content))
					else:
						file_handle.write(content)

			file_size = local_path.stat().st_size
			logger.info(f"✓ Download complete: {local_path} ({file_size / 1e6:.1f} MB)")
			return True

		except Exception as exception:
			logger.error(f"Download failed: {exception}")
			if local_path.exists():
				local_path.unlink()
			return False

	def download_assembly(self, job_output_path: str, local_dir: str, sample_id: str) -> bool:
		"""
		Download assembled contigs FASTA from job results.

		Args:
		    job_output_path: Remote output path from job
		    local_dir: Local output directory
		    sample_id: Sample identifier for filename

		Returns:
		    True if download successful
		"""
		try:
			_require_safe_identifier(sample_id, "sample_id")

			# Expected FASTA file path in BV-BRC output
			remote_fasta = f"{job_output_path}/assembly_contigs.fasta"
			local_fasta = Path(local_dir) / f"{sample_id}_assembly_contigs.fasta"

			Path(local_dir).mkdir(parents=True, exist_ok=True)

			return self.download_file(remote_fasta, local_fasta)

		except Exception as exception:
			logger.error(f"Error downloading assembly: {exception}")
			return False

	def download_genome_report(self, job_output_path: str, local_dir: str, sample_id: str) -> bool:
		"""
		Download genome analysis report JSON.

		Args:
		    job_output_path: Remote output path from job
		    local_dir: Local output directory
		    sample_id: Sample identifier for filename

		Returns:
		    True if download successful
		"""
		try:
			_require_safe_identifier(sample_id, "sample_id")

			# Expected genome report path
			remote_report = f"{job_output_path}/genome_report.json"
			local_report = Path(local_dir) / f"{sample_id}_genome_report.json"

			Path(local_dir).mkdir(parents=True, exist_ok=True)

			return self.download_file(remote_report, local_report)

		except Exception as exception:
			logger.error(f"Error downloading genome report: {exception}")
			return False

	# ========================================================================
	# Utility Methods
	# ========================================================================

	def check_file_exists(self, remote_path: str) -> bool:
		"""Check if a file exists in the BV-BRC workspace."""
		try:
			rpc_result = self._rpc(
				self.WORKSPACE_URL,
				"Workspace.get",
				[{"objects": [remote_path], "metadata_only": 1}],
				timeout=15,
			)
			return bool(rpc_result and rpc_result[0])
		except Exception as exception:
			logger.debug(f"Error checking file existence: {exception}")
			return False

	def list_workspace_files(self, workspace_path: str = None) -> List[Dict[str, Any]]:
		"""List files in a workspace directory."""
		if workspace_path is None:
			workspace_path = self.workspace
		try:
			rpc_result = self._rpc(
				self.WORKSPACE_URL, "Workspace.ls", [{"paths": [workspace_path]}], timeout=15
			)
			if rpc_result and isinstance(rpc_result, list) and rpc_result[0]:
				return rpc_result[0].get(workspace_path, [])
			return []
		except Exception as exception:
			logger.error(f"Error listing workspace files: {exception}")
			return []

	# Workspace.ls metadata tuple layout: [name, type, parent_path, ...]
	_LS_NAME, _LS_TYPE, _LS_PARENT = 0, 1, 2
	_FOLDER_TYPES = ("folder", "Directory", "job_result")

	def walk_workspace(self, base_path: str, max_depth: int = 3) -> List[Dict[str, str]]:
		"""
		Recursively list files under a workspace folder.

		BV-BRC app results are nested (and partly in dot-prefixed folders), so a
		flat listing isn't enough to find a job's output files. This walks the
		tree breadth-first up to ``max_depth`` and returns file entries as
		dicts: {"name", "type", "path"} where ``path`` is the full workspace path.

		Args:
		    base_path: Folder to start from.
		    max_depth: How many folder levels to descend (0 = base only).

		Returns:
		    List of file (non-folder) entries found beneath ``base_path``.
		"""
		workspace_files: List[Dict[str, str]] = []
		frontier = [(base_path, 0)]
		seen = set()

		while frontier:
			workspace_path, depth = frontier.pop(0)
			if workspace_path in seen:
				continue
			seen.add(workspace_path)

			for workspace_entry in self.list_workspace_files(workspace_path):
				if (
					not isinstance(workspace_entry, (list, tuple))
					or len(workspace_entry) <= self._LS_TYPE
				):
					continue
				workspace_entry_name = workspace_entry[self._LS_NAME]
				etype = workspace_entry[self._LS_TYPE]
				full_path = f"{workspace_path.rstrip('/')}/{workspace_entry_name}"

				if etype in self._FOLDER_TYPES:
					if depth < max_depth:
						frontier.append((full_path, depth + 1))
				else:
					workspace_files.append(
						{"name": workspace_entry_name, "type": etype, "path": full_path}
					)

		return workspace_files


if __name__ == "__main__":
	# Example usage
	client = BVBRCClient()

	if not client.is_authenticated():
		print("Please login first:")
		if client.login_interactive():
			print("✓ Login successful")
		else:
			print("✗ Login failed")
	else:
		print("✓ Already authenticated")
