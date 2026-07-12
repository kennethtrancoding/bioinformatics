"""
BLAST Rules: Step 13 (NCBI novelty confirmation)

Takes each resistance enzyme RGI flagged and BLASTs its protein against NCBI to
answer two questions: is this enzyme globally novel (no 100% match anywhere),
and do its closest hits sit on a plasmid vs a chromosome? So this runs a REMOTE
BLASTP against NCBI's nr -- not a re-BLAST against a local CARD database (which
RGI already compared against internally).

Only the antibiotic-inactivation enzymes are submitted, most-novel-first, capped by config.
"""

rule extract_rgi_proteins:
    """
    Step 12: Extract the ORF-predicted protein sequences CARD RGI found for each
    resistance gene hit -- the "download the protein sequence" artifact from the
    tutorial, kept as a full FASTA of every resistance protein for the record.
    """
    input:
        rgi_json = f"{config['results_dir']}/{{sample}}/03_resistance/rgi_results.json"
    params:
        sample_id = lambda wildcards: wildcards.sample
    output:
        proteins_fasta = f"{config['results_dir']}/{{sample}}/04_blast/rgi_proteins.fasta",
        proteins_csv = f"{config['results_dir']}/{{sample}}/04_blast/rgi_proteins.csv"
    log:
        f"{config['results_dir']}/logs/{{sample}}_extract_rgi_proteins.log"
    shell:
        """
        python3 workflow/scripts/extract_rgi_proteins.py \
            {input.rgi_json} {output.proteins_fasta} {output.proteins_csv} \
            2>&1 | tee {log}
        """


rule blast_ncbi_novelty:
    """
    Step 13: Remote BLASTP of the antibiotic-inactivation enzymes against NCBI nr
    to confirm global novelty and infer plasmid vs chromosome location, mirroring
    the tutorial's manual NCBI BLAST. Non-fatal: NCBI is flaky/rate-limited, so a
    failed submission writes an empty result with a note rather than crashing the
    run (this step is informational, not a gate).
    """
    input:
        rgi_json = f"{config['results_dir']}/{{sample}}/03_resistance/rgi_results.json"
    params:
        sample_id = lambda wildcards: wildcards.sample,
        database = config['blast']['database'],
        evalue = config['blast']['evalue_threshold'],
        max_targets = config['blast']['max_target_seqs'],
        max_queries = config['blast']['max_query_proteins'],
        mechanism = config['blast']['mechanism_filter']
    output:
        blast_csv = f"{config['results_dir']}/{{sample}}/04_blast/blast_results.csv",
        # Complete NCBI response -- every hit of every query, saved verbatim.
        blast_full = f"{config['results_dir']}/{{sample}}/04_blast/blast_results_full.tsv"
    conda:
        "../envs/blast.yml"
    log:
        f"{config['results_dir']}/logs/{{sample}}_blast.log"
    shell:
        """
        "$CONDA_PREFIX/bin/python3" workflow/scripts/blast_ncbi.py \
            --rgi-json {input.rgi_json} \
            --out {output.blast_csv} \
            --full-out {output.blast_full} \
            --database {params.database} \
            --evalue {params.evalue} \
            --max-target-seqs {params.max_targets} \
            --max-queries {params.max_queries} \
            --mechanism "{params.mechanism}" \
            2>&1 | tee {log}
        """
