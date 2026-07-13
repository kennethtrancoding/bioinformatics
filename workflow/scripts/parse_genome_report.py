"""
Parse Genome Report Script (Snakemake rule: parse_genome_report)
Extract key metrics from BV-BRC genome report
"""

import csv
import json
from pathlib import Path

genome_report_file = snakemake.input[0]
metrics_file = snakemake.output.metrics

Path(metrics_file).parent.mkdir(parents=True, exist_ok=True)

with open(genome_report_file) as file_handle:
	report = json.load(file_handle)

# BV-BRC returns the genome report as a single-element list wrapping the dict.
if isinstance(report, list):
	report = report[0] if report else {}

# Extract metrics (BV-BRC keys N50/L50 as contig_n50/contig_l50)
metrics = {
	"contigs": report.get("contigs", "N/A"),
	"gc_content": report.get("gc_content", "N/A"),
	"genome_length": report.get("genome_length", "N/A"),
	"n50": report.get("contig_n50", "N/A"),
	"l50": report.get("contig_l50", "N/A"),
}

with open(metrics_file, "w") as file_handle:
	writer = csv.DictWriter(file_handle, fieldnames=metrics.keys())
	writer.writeheader()
	writer.writerow(metrics)

print("✓ Genome metrics extracted")
