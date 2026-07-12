"""
BV-BRC Comprehensive Genome Analysis Script (Snakemake rule: bvbrc_comprehensive_genome_analysis)
Run BV-BRC Comprehensive Genome Analysis (assembly + annotation)
"""

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path.cwd()))

from workflow.lib.bvbrc_client import BVBRCClient
from workflow.lib.utils import setup_logger

logger = setup_logger("bvbrc_cga", snakemake.log[0])

sample_id = snakemake.params.sample_id
upload_log_file = snakemake.input.upload_log
genus_file = snakemake.input.genus_file
assembly_fasta = snakemake.output.assembly_fasta
genome_report = snakemake.output.genome_report
full_report = snakemake.output.full_report
cga_raw_dir = snakemake.output.cga_raw_dir
max_wait = snakemake.params.max_wait_time

Path(assembly_fasta).parent.mkdir(parents=True, exist_ok=True)

# Load upload results and genus
with open(upload_log_file) as file_handle:
	upload_data = json.load(file_handle)

with open(genus_file) as file_handle:
	genus = file_handle.read().strip()

r1_remote = upload_data["r1_remote"]
r2_remote = upload_data["r2_remote"]
workspace = upload_data["workspace"]

# Initialize client
client = BVBRCClient(
	token_file=snakemake.config["bvbrc"]["token_file"],
	job_id=snakemake.config.get("job_id"),
)
if not client.is_authenticated():
	raise RuntimeError("BV-BRC not authenticated")

client.workspace = workspace

taxonomy_id = client.get_taxonomy_id(genus)
logger.info(f"Submitting Comprehensive Genome Analysis for {sample_id}...")
logger.info(f"Genus: {genus} (taxon_id: {taxonomy_id})")

with open(snakemake.output.taxonomy_lookup, "w") as file_handle:
	json.dump({"genus": genus, "taxon_id": taxonomy_id}, file_handle, indent=2)

# Re-use an already-running job if recorded (avoids duplicate submissions on restart)
job_cache = Path(assembly_fasta).parent / "cga_job_id.txt"
job_id = None
if job_cache.exists():
	with open(job_cache) as file_handle:
		cached_id = file_handle.read().strip()
	status_info = client.get_job_status(cached_id)
	if status_info and status_info.get("status") not in ("failed", None):
		job_id = cached_id
		logger.info(f"Resuming existing CGA job: {job_id} (status: {status_info.get('status')})")

if not job_id:
	assembly_method = snakemake.config["bvbrc"]["assembly_method"]

	params = {
	 "r1_file": r1_remote,
	 "r2_file": r2_remote,
	 "output_name": f"cga_{sample_id}",
	 "taxonomy_id": taxonomy_id,
	 "assembly_method": assembly_method,
	}

	if genus != "Unknown":
		params["genus"] = genus

	job_id = client.submit_comprehensive_genome_analysis(**params)
	if not job_id:
		raise RuntimeError("Failed to submit Comprehensive Genome Analysis job")
	with open(job_cache, "w") as file_handle:
		file_handle.write(job_id)

logger.info(f"Job ID: {job_id}")

# Poll job until completion
poll_interval = snakemake.config["bvbrc"]["poll_interval"]
is_complete, final_status = client.wait_for_job(
 job_id, max_wait_seconds=max_wait, poll_interval=poll_interval
)

if not is_complete:
	raise RuntimeError(f"CGA job failed or timed out: {final_status}")

logger.info("Comprehensive Genome Analysis complete")

# Download assembly and genome report.
# BV-BRC writes CGA results into the job's output folder (and nested/dot-prefixed
# sub-folders) under names that vary by recipe, so we resolve the actual remote
# files by walking the workspace rather than guessing a single path. A missing
# result is a hard error: silently substituting a placeholder hides a failed or
# empty assembly and corrupts every downstream step (RGI, novelty, reports).
output_name = f"cga_{sample_id}"
result_roots = [
 f"{workspace}/{output_name}",
 f"{workspace}/.{output_name}",
]

logger.info(f"Locating CGA results under {result_roots}...")
remote_file_entries = []
for result_root in result_roots:
	remote_file_entries.extend(client.walk_workspace(result_root, max_depth=4))

if not remote_file_entries:
	raise RuntimeError(
	 f"No CGA result files found for {sample_id} under {result_roots}. "
	 f"Job {job_id} reported complete but produced no downloadable output."
	)


def _resolve(candidate_paths):
	"""Return the full remote path of the first matching result file."""
	for candidate_path in candidate_paths:
		for remote_entry in remote_file_entries:
			if candidate_path(remote_entry):
				return remote_entry["path"]
	return None


def _name(remote_entry):
	return remote_entry["name"].lower()


# Assembly contigs: prefer an explicit contigs FASTA, then any assembly FASTA/FNA.
assembly_remote = _resolve(
 [
  lambda remote_entry: _name(remote_entry).endswith((".fasta", ".fna", ".fa")) and "contig" in _name(remote_entry),
  lambda remote_entry: _name(remote_entry).endswith((".fasta", ".fna", ".fa")) and "assembl" in _name(remote_entry),
  lambda remote_entry: remote_entry["type"] == "contigs",
  lambda remote_entry: _name(remote_entry).endswith((".fna", ".fasta", ".fa")),
 ]
)
if not assembly_remote:
	raise RuntimeError(
	 f"Could not find an assembly FASTA in CGA results for {sample_id}. "
	 f"Files seen: {[remote_entry['path'] for remote_entry in remote_file_entries]}"
	)

logger.info(f"Downloading assembly: {assembly_remote}")
if not client.download_file(assembly_remote, assembly_fasta):
	raise RuntimeError(f"Failed to download assembly from {assembly_remote}")

# Genome report: prefer an explicit genome_report.json, then any genome/report JSON.
report_remote = _resolve(
 [
  lambda remote_entry: _name(remote_entry) == "genome_report.json",
  lambda remote_entry: _name(remote_entry).endswith(".json") and ("genome" in _name(remote_entry) or "report" in _name(remote_entry)),
 ]
)
if not report_remote:
	raise RuntimeError(
	 f"Could not find a genome report JSON in CGA results for {sample_id}. "
	 f"Files seen: {[remote_entry['path'] for remote_entry in remote_file_entries]}"
	)

logger.info(f"Downloading genome report: {report_remote}")
if not client.download_file(report_remote, genome_report):
	raise RuntimeError(f"Failed to download genome report from {report_remote}")

# FullGenomeReport.html
html_remote = _resolve(
 [
  lambda remote_entry: _name(remote_entry) == "fullgenomereport.html",
  lambda remote_entry: _name(remote_entry).endswith(".html") and ("full" in _name(remote_entry) or "genome" in _name(remote_entry)),
 ]
)
if html_remote:
	logger.info(f"Downloading HTML report: {html_remote}")
	if not client.download_file(html_remote, full_report):
		logger.warning(f"Failed to download HTML report from {html_remote}")
else:
	logger.warning("Could not find FullGenomeReport.html in CGA results")

# Archive every remaining file the CGA job produced (annotation, protein/gene
# FASTAs, quality report, specialty-gene report, etc.) verbatim, mirroring the
# workspace layout. The three downloads above are curated copies for
# downstream steps; this is the full raw bundle so nothing the job wrote is
# lost even if a future step needs a file we don't parse today.
Path(cga_raw_dir).mkdir(parents=True, exist_ok=True)


def _relative_to_a_root(remote_path):
	for result_root in result_roots:
		prefix = result_root.rstrip("/") + "/"
		if remote_path.startswith(prefix):
			return remote_path[len(prefix) :]
	return Path(remote_path).name


archived, failed = 0, 0
for remote_entry in remote_file_entries:
	relative_path = _relative_to_a_root(remote_entry["path"])
	local_path = Path(cga_raw_dir) / relative_path
	local_path.parent.mkdir(parents=True, exist_ok=True)
	if client.download_file(remote_entry["path"], local_path):
		archived += 1
	else:
		failed += 1
		logger.warning(f"Failed to archive {remote_entry['path']} into {cga_raw_dir}")

logger.info(f"✓ Archived {archived} raw CGA file(s) to {cga_raw_dir} ({failed} failed)")

logger.info(f"✓ Comprehensive Genome Analysis complete for {sample_id}")
logger.info(f"  Assembly: {assembly_fasta}")
logger.info(f"  Report: {genome_report}")
logger.info(f"  HTML Report: {full_report}")
logger.info(f"  Full raw output: {cga_raw_dir}")
