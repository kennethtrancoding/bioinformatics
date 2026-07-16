#!/usr/bin/env python3
"""
Fetch one raw FASTQ from S3 for the duration of a run (Snakemake rule:
fetch_raw_read, in workflow/rules/raw.smk).

Raw reads are not kept on local disk between the upload and the run. The upload
streams them to S3 and drops the local copy; this pulls each one back just before
the rules that read it, and Snakemake's temp() deletes it again the moment the last
of those rules is done.

That is what stops peak disk from scaling with batch size. Reads used to sit on disk
from upload until the run finished, so a 50-sample batch held ~19 GB of FASTQ for
hours -- and every *queued* job's reads sat there too, doing nothing. Now only the
samples actually in flight are on disk.

If the file is already present (a local dev run with no bucket configured, say),
Snakemake never invokes this: the output exists, so the rule does not fire.
"""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path.cwd()))

from workflow.helpers import s3_storage  # noqa: E402


def main(argv=None):
	argument_parser = argparse.ArgumentParser(description=__doc__)
	argument_parser.add_argument("--job-id", required=True)
	argument_parser.add_argument(
		"--name", required=True, help="the FASTQ's basename, as stored in S3"
	)
	argument_parser.add_argument("--out", required=True, help="where the run expects to read it")
	parsed_args = argument_parser.parse_args(argv)

	destination_path = Path(parsed_args.out)
	print(f"→ fetch_raw_read: {parsed_args.name} → {destination_path}")
	s3_storage.download_raw_file(parsed_args.job_id, parsed_args.name, destination_path)
	size_mb = destination_path.stat().st_size / (1024 * 1024)
	print(f"✓ fetch_raw_read: {parsed_args.name} ({size_mb:.1f} MB)")
	return 0


if __name__ == "__main__":
	sys.exit(main())
