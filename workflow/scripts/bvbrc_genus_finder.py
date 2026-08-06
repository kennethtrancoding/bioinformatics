"""
BV-BRC Genus Finder Script (Snakemake rule: bvbrc_similar_genome_finder)
Use BV-BRC TaxonomicClassification (Kraken2) to identify bacterial genus.
Falls back to "Unknown" on any error, and genus gets omitted in CGA.
"""

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path.cwd()))

from workflow.helpers.bvbrc_client import authenticated_client
from workflow.helpers.utils import setup_logger

logger = setup_logger("bvbrc_genus_finder", snakemake.log[0])

sample_id = snakemake.params.sample_id
upload_log_file = snakemake.input[0]
genus_file = snakemake.output.genus_file
kraken_report_file = snakemake.output.kraken_report

Path(genus_file).parent.mkdir(parents=True, exist_ok=True)

with open(upload_log_file) as file_handle:
	upload_data = json.load(file_handle)

r1_remote = upload_data["r1_remote"]
r2_remote = upload_data["r2_remote"]
workspace = upload_data["workspace"]
poll_interval = snakemake.config["bvbrc"]["poll_interval"]
max_attempts = snakemake.config["bvbrc"].get("max_job_attempts", 3)
retry_delay = snakemake.config["bvbrc"].get("retry_delay", 60)
output_folder = f"{workspace}/taxclass_{sample_id}"

client = authenticated_client(
	snakemake.config["bvbrc"]["token_file"], snakemake.config.get("job_id")
)

client.workspace = workspace
genus = "Unknown"

logger.info(f"Submitting TaxonomicClassification for {sample_id}...")

try:
	# Waited on with no deadline, and resubmitted if BV-BRC fails it -- same as
	# the assembly. run_job raises once BV-BRC has failed every attempt, which
	# the handler below turns into an Unknown genus rather than a failed sample.
	job_id = client.run_job(
		lambda: client.submit_taxonomic_classification(r1_remote, r2_remote, sample_id),
		poll_interval=poll_interval,
		max_attempts=max_attempts,
		retry_delay=retry_delay,
		on_submit=lambda new_job_id: logger.info(f"Job submitted: {new_job_id}"),
	)

	logger.info("TaxonomicClassification complete, downloading report...")

	kraken_remote = f"{output_folder}/TaxonomicClassification.txt"

	if client.download_file(kraken_remote, kraken_report_file):
		# Kraken2 report format: pct, covered, assigned, rank, taxid, name
		# rank codes: D=domain, P=phylum, C=class, O=order, F=family, G=genus, S=species
		best_genus = None
		best_pct = 0.0
		with open(kraken_report_file) as file_handle:
			for report_line in file_handle:
				parts = report_line.strip().split("\t")
				if len(parts) < 6:
					continue
				try:
					pct = float(parts[0])
					rank = parts[3].strip()
					taxon_name = parts[5].strip()
				except (ValueError, IndexError):
					continue
				if rank == "G" and pct > best_pct:
					best_pct = pct
					best_genus = taxon_name
		if best_genus:
			genus = best_genus
			logger.info(f"Identified genus: {genus} ({best_pct:.1f}%)")
		else:
			logger.warning("No genus-level hits in Kraken2 report, using Unknown")
	else:
		logger.warning("Could not download Kraken2 report, using Unknown genus")

except Exception as exception:
	logger.warning(f"TaxonomicClassification failed ({exception}), using Unknown genus")
	genus = "Unknown"

# The rule declares kraken_report_file as an output, so it must always exist --
# write a placeholder when the job failed or the report couldn't be downloaded
# (submit_taxonomic_classification raising, or download_file returning False).
if not Path(kraken_report_file).exists():
	with open(kraken_report_file, "w") as file_handle:
		file_handle.write(
			f"TaxonomicClassification unavailable for {sample_id}: "
			f"job failed or the report could not be downloaded from BV-BRC.\n"
		)

with open(genus_file, "w") as file_handle:
	file_handle.write(genus + "\n")

logger.info(f"✓ Genus identification complete: {genus}")
