"""
Every external API the pipeline talks to.

This is the single source of truth for the BV-BRC service endpoints. Both the
Settings page (view/edit/persist URLs) and the service-status table (live health
checks) read from here, and BVBRCClient picks up any saved URL overrides so the
Settings page actually controls what the pipeline calls.
"""

import json
import os
import time
from pathlib import Path
from urllib.parse import urlparse

import requests

_HERE = Path(__file__).resolve().parent.parent

# key: default definition, ordered the same way the pipeline uses them.
# "wired" marks endpoints whose saved URL is read by the pipeline
# (the BV-BRC client); the others are external tool services we health-check
# and surface, but whose URL is fixed inside the underlying CLI tool.
DEFAULT_ENDPOINTS = {
	"auth": {
		"name": "Authentication",
		"group": "BV-BRC Platform",
		"url": "https://user.patricbrc.org/authenticate",
		"purpose": "Log in and issue an access token.",
		"wired": True,
	},
	"public_key": {
		"name": "Token Keyserver",
		"group": "BV-BRC Platform",
		"url": "https://user.patricbrc.org/public_key",
		"purpose": "Public key used by BV-BRC services to validate your token.",
		"wired": False,
	},
	"workspace": {
		"name": "Workspace",
		"group": "BV-BRC Platform",
		"url": "https://p3.theseed.org/services/Workspace",
		"purpose": "Upload reads and download assembly results.",
		"wired": True,
	},
	"app_service": {
		"name": "App Service",
		"group": "BV-BRC Platform",
		"url": "https://p3.theseed.org/services/app_service",
		"purpose": "Submit and poll genome-analysis jobs.",
		"wired": True,
	},
	"data_api": {
		"name": "Data API (Taxonomy)",
		"group": "BV-BRC Platform",
		"url": "https://www.bv-brc.org/api/taxonomy",
		"purpose": "Look up taxonomy IDs for a genus.",
		"wired": False,
	},
	"ncbi_blast": {
		"name": "NCBI BLAST",
		"group": "Sequence Databases",
		"url": "https://blast.ncbi.nlm.nih.gov/Blast.cgi",
		"purpose": "Remote BLASTN of specialty genes (blast tool = remote).",
		"wired": False,
	},
	"card": {
		"name": "CARD (RGI)",
		"group": "Sequence Databases",
		"url": "https://card.mcmaster.ca",
		"purpose": "Resistance-gene reference database behind RGI.",
		"wired": False,
	},
	"pubmlst": {
		"name": "PubMLST",
		"group": "Sequence Databases",
		"url": "https://rest.pubmlst.org",
		"purpose": "MLST scheme source behind the mlst tool.",
		"wired": False,
	},
	"mef": {
		"name": "MGEdb (MobileElementFinder)",
		"group": "Sequence Databases",
		"url": "https://cge.food.dtu.dk/services/MobileElementFinder/",
		"purpose": "Mobile element database for genome annotation.",
		"wired": False,
	},
}

# Endpoints health-checked with a plain GET reachability probe.
_GENERIC_GET = {"ncbi_blast", "card", "pubmlst", "mef"}
_DEFAULT_ALLOWED_HOSTS = {
	urlparse(endpoint["url"]).hostname for endpoint in DEFAULT_ENDPOINTS.values()
}


def _is_valid_endpoint_url(endpoint_url):
	"""Allow only HTTPS URLs on built-in or deployer-approved service hosts."""
	try:
		parsed_url = urlparse(endpoint_url)
	except ValueError:
		return False
	allowed_hosts = _DEFAULT_ALLOWED_HOSTS | {
		host.strip().lower()
		for host in os.environ.get("ALLOWED_API_HOSTS", "").split(",")
		if host.strip()
	}
	return (
		parsed_url.scheme == "https"
		and parsed_url.hostname is not None
		and parsed_url.hostname.lower() in allowed_hosts
		and parsed_url.username is None
		and parsed_url.password is None
	)


def load_overrides(job_id=None):
	"""Return overrides for one job; there is deliberately no global fallback."""
	if not job_id:
		return {}
	try:
		from workflow.lib.jobs import is_valid_job_id, job_api_endpoints_path

		if not is_valid_job_id(job_id):
			return {}
		with job_api_endpoints_path(job_id).open() as file_handle:
			override_data = json.load(file_handle)
		return {
			endpoint_key: endpoint_url
			for endpoint_key, endpoint_url in override_data.items()
			if endpoint_key in DEFAULT_ENDPOINTS
			and endpoint_url
			and _is_valid_endpoint_url(endpoint_url)
		}
	except Exception:
		return {}


def load_endpoints(job_id=None):
	"""Return the endpoint definitions with any saved URL overrides applied.

	Args:
		job_id: If provided, loads job-specific overrides; otherwise uses global.
	"""
	endpoint_overrides = load_overrides(job_id=job_id)
	endpoint_definitions = {}
	for endpoint_key, default_endpoint in DEFAULT_ENDPOINTS.items():
		endpoint_definition = dict(default_endpoint)
		endpoint_definition["key"] = endpoint_key
		endpoint_definition["url"] = endpoint_overrides.get(endpoint_key, default_endpoint["url"])
		endpoint_definition["is_override"] = (
			endpoint_key in endpoint_overrides
			and endpoint_overrides[endpoint_key] != default_endpoint["url"]
		)
		endpoint_definitions[endpoint_key] = endpoint_definition
	return endpoint_definitions


def save_job_overrides(job_id, form):
	"""Persist job-specific API endpoint overrides.

	Args:
		job_id: The job ID for which to save overrides.
		form: Dictionary of endpoint keys and URLs.

	Returns:
		Dictionary of saved overrides.
	"""
	from workflow.lib.jobs import is_valid_job_id, job_api_endpoints_path

	if not is_valid_job_id(job_id):
		raise ValueError("Invalid job ID")

	endpoint_overrides = {}
	for endpoint_key, default_endpoint in DEFAULT_ENDPOINTS.items():
		endpoint_url = (form.get(endpoint_key) or "").strip()
		if (
			endpoint_url
			and endpoint_url != default_endpoint["url"]
			and _is_valid_endpoint_url(endpoint_url)
		):
			endpoint_overrides[endpoint_key] = endpoint_url
	job_overrides_path = job_api_endpoints_path(job_id)
	job_overrides_path.parent.mkdir(parents=True, exist_ok=True)
	with job_overrides_path.open("w") as file_handle:
		json.dump(endpoint_overrides, file_handle, indent=2)
	return endpoint_overrides


def get_url(endpoint_key, job_id=None):
	"""Resolve the live URL for one endpoint key (override or default).

	Args:
		endpoint_key: The endpoint identifier.
		job_id: If provided, loads job-specific overrides first.
	"""
	return load_overrides(job_id=job_id).get(endpoint_key, DEFAULT_ENDPOINTS[endpoint_key]["url"])


def _load_token(job_id=None):
	if not job_id:
		return None
	try:
		from workflow.lib.jobs import job_token_path

		with job_token_path(job_id).open() as file_handle:
			return json.load(file_handle).get("access_token")
	except Exception:
		return None


def _result(status, code, detail, started_at):
	return {
		"status": status,  # "up" | "degraded" | "down"
		"code": code,
		"detail": detail,
		"latency_ms": int((time.time() - started_at) * 1000),
	}


def check_endpoint(endpoint_key, endpoint_url=None, job_id=None):
	"""
	Probe one endpoint and classify it as up / degraded / down.

	Each service uses a different protocol, so the probe is per-endpoint:
	a reachable service that refuses unauthenticated input still counts
	as "up." We only flag "down" when the service can't be reached at all,
	and "degraded" for the known BV-BRC token-validation outage.
	"""
	endpoint_url = endpoint_url or get_url(endpoint_key)
	started_at = time.time()
	try:
		if endpoint_key == "auth":
			response = requests.post(
				endpoint_url,
				data={"username": "healthcheck@bvbrc", "password": "x"},
				headers={"Content-Type": "application/x-www-form-urlencoded"},
				timeout=15,
			)
			if response.status_code in (400, 401, 403):
				return _result(
					"up", response.status_code, "Reachable, rejects bad credentials", started_at
				)
			if response.status_code == 200:
				return _result("up", response.status_code, "Reachable", started_at)
			return _result("down", response.status_code, response.text[:120], started_at)

		if endpoint_key == "public_key":
			response = requests.get(endpoint_url, timeout=15)
			if response.status_code == 200 and "pubkey" in response.text:
				return _result("up", response.status_code, "Serving public key", started_at)
			if response.status_code in (403, 429) or 500 <= response.status_code < 600:
				return _result(
					"degraded",
					response.status_code,
					"Keyserver flapping; token validation may fail",
					started_at,
				)
			return _result("down", response.status_code, response.text[:120], started_at)

		if endpoint_key == "workspace":
			response = requests.post(
				endpoint_url,
				json={
					"method": "Workspace.ls",
					"version": "1.1",
					"id": 1,
					"params": [{"paths": ["/"]}],
				},
				headers={"Content-Type": "application/json"},
				timeout=20,
			)
			if response.status_code == 200 and '"result"' in response.text:
				return _result("up", response.status_code, "JSON-RPC responding.", started_at)
			return _result("down", response.status_code, response.text[:120], started_at)

		if endpoint_key == "app_service":
			token = _load_token(job_id)
			request_headers = {"Content-Type": "application/json"}
			if token:
				request_headers["Authorization"] = token
			response = requests.post(
				endpoint_url,
				json={
					"method": "AppService.enumerate_apps",
					"version": "1.1",
					"id": 1,
					"params": [],
				},
				headers=request_headers,
				timeout=20,
			)
			if response.status_code == 200:
				return _result(
					"up", response.status_code, "Authenticated and responding.", started_at
				)
			if "signer pubkey" in response.text:
				return _result(
					"degraded",
					response.status_code,
					"Up, but BV-BRC can't validate tokens (keyserver issue).",
					started_at,
				)
			if "Authentication required" in response.text:
				return _result(
					"up",
					response.status_code,
					"Reachable (add a token to submit jobs).",
					started_at,
				)
			return _result("down", response.status_code, response.text[:120], started_at)

		if endpoint_key == "data_api":
			query_separator = "&" if "?" in endpoint_url else "?"
			response = requests.get(
				f"{endpoint_url}{query_separator}limit(1)",
				headers={"Accept": "application/json"},
				timeout=15,
			)
			if response.status_code == 200:
				return _result("up", response.status_code, "Query API responding.", started_at)
			return _result("down", response.status_code, response.text[:120], started_at)

		if endpoint_key in _GENERIC_GET:
			# Tool-backed databases: a plain reachability probe is enough.
			# Any non-server-error response means the host is serving us.
			response = requests.get(endpoint_url, timeout=15, allow_redirects=True)
			if response.status_code < 500:
				return _result("up", response.status_code, "Reachable.", started_at)
			return _result("down", response.status_code, response.text[:120], started_at)

		return _result("down", None, f"Unknown endpoint key: {endpoint_key}", started_at)

	except requests.RequestException as exception:
		return _result("down", None, f"Unreachable: {exception.__class__.__name__}", started_at)


def check_all(job_id=None):
	"""Health-check every endpoint; returns a list aligned with load_endpoints()."""
	health_results = []
	for endpoint_key, endpoint_definition in load_endpoints(job_id=job_id).items():
		health_check = check_endpoint(endpoint_key, endpoint_definition["url"], job_id=job_id)
		health_results.append({**endpoint_definition, **health_check})
	return health_results
