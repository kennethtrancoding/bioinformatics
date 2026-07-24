"""
Extract RGI-predicted proteins (Snakemake rule: extract_rgi_proteins)

Pulls the ORF-predicted protein sequence CARD RGI found for each resistance
gene hit (`orf_prot_sequence`) out of `rgi_results.json` so it can be
independently re-BLASTed (blastp) against a local CARD protein database.
"""

import csv
import json
import sys
from pathlib import Path

# Invoked as `python3 workflow/scripts/extract_rgi_proteins.py ...` from a shell
# rule, so the scripts directory is not already on the path.
sys.path.insert(0, str(Path(__file__).resolve().parent))

from rgi_json import iter_hits  # noqa: E402

rgi_json = sys.argv[1]
proteins_fasta = sys.argv[2]
proteins_csv = sys.argv[3]

# RGI's --output_file appends ".json" itself, so the real file may be
# "<name>.json" even when the rule asked for "<name>".
if not Path(rgi_json).exists() and Path(rgi_json + ".json").exists():
    rgi_json = rgi_json + ".json"

Path(proteins_fasta).parent.mkdir(parents=True, exist_ok=True)

with open(rgi_json) as file_handle:
    rgi_data = json.load(file_handle)


hits = list(iter_hits(rgi_data))

with open(proteins_fasta, "w") as fasta_file_handle, open(proteins_csv, "w", newline="") as csv_file_handle:
    writer = csv.writer(csv_file_handle)
    writer.writerow(["orf_id", "best_hit_aro", "protein_length"])

    for orf_id, hit, contig in hits:
        protein_seq = hit.get("orf_prot_sequence") or hit.get("query")
        if not protein_seq:
            continue
        header = f"{orf_id}|{hit.get('ARO_name', 'unknown')}".replace(" ", "_")
        fasta_file_handle.write(f">{header}\n")
        for sequence_offset in range(0, len(protein_seq), 80):
            fasta_file_handle.write(protein_seq[sequence_offset : sequence_offset + 80] + "\n")
        writer.writerow([orf_id, hit.get("ARO_name", ""), len(protein_seq)])

print(f"✓ Extracted {len(hits)} RGI-predicted protein(s) to {proteins_fasta}")
