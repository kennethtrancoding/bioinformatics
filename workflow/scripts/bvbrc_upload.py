"""
BV-BRC Upload Script (Snakemake rule: bvbrc_upload_reads)
Upload paired-end reads to BV-BRC workspace
"""

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path.cwd()))

from workflow.lib.bvbrc_client import BVBRCClient
from workflow.lib.utils import setup_logger

logger = setup_logger("bvbrc_upload", snakemake.log[0])

sample_id = snakemake.params.sample_id
first_read_path = snakemake.input.first_read
second_read_path = snakemake.input.second_read
workspace_name = snakemake.config["bvbrc"]["workspace_name"]
output_log = snakemake.output.upload_log

Path(output_log).parent.mkdir(parents=True, exist_ok=True)

client = BVBRCClient(
	token_file=snakemake.config["bvbrc"]["token_file"],
	job_id=snakemake.config.get("job_id"),
)

if not client.is_authenticated():
	logger.info("BV-BRC authentication required")
	if not client.login_interactive():
		raise RuntimeError("BV-BRC authentication failed")

workspace = client.get_or_create_workspace(workspace_name)
if not workspace:
	raise RuntimeError(f"Failed to create/access workspace: {workspace_name}")

logger.info(f"Using workspace: {workspace}")

success, r1_remote, r2_remote = client.upload_paired_reads(first_read_path, second_read_path, sample_id)

if not success:
	raise RuntimeError(f"Failed to upload paired reads for {sample_id}")

upload_data = {
 "sample_id": sample_id,
 "workspace": workspace,
 "r1_remote": r1_remote,
 "r2_remote": r2_remote,
 "status": "success",
}

with open(output_log, "w") as file_handle:
	json.dump(upload_data, file_handle, indent=2)

logger.info(f"✓ Upload complete for {sample_id}")
logger.info(f"  R1: {r1_remote}")
logger.info(f"  R2: {r2_remote}")
