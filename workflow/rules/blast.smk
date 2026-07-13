"""
BLAST Rules: Step 13 (novelty confirmation)

Takes each resistance enzyme RGI flagged and BLASTs its protein to answer two
questions: is this enzyme novel (no identical match), and do its closest hits sit
on a plasmid vs a chromosome?

The search is two-tier. It goes first against a LOCAL database built from NCBI's
AMRFinderPlus reference protein catalog (build_amr_blast_db below) -- not a
re-BLAST against CARD, which RGI already did internally, but against the curated
catalog of known resistance proteins. Only the enzymes that match nothing there
fall through to a remote BLASTP against NCBI's nr.

Remote BLAST is queue-bound, not size-bound: a full 216-protein nr search and a
capped 15-protein one both took 30 minutes to time out and return nothing. The
local catalog resolves the same enzymes in about a minute, so NCBI is now reserved
for the handful of genuinely unrecognised proteins, where the wait buys something.

Only the antibiotic-inactivation enzymes are submitted, most-novel-first, capped by config.
"""

AMR_DB_DIR = config['blast']['local_db_dir']
AMR_DB_PREFIX = f"{AMR_DB_DIR}/{config['blast']['local_db_name']}"


rule build_amr_blast_db:
    """
    Download NCBI's AMRFinderPlus reference protein catalog and format it as a local
    BLAST database. ~5 MB, ~10k proteins, builds in seconds.

    The Docker image pre-builds this (see Dockerfile), so in production the marker
    already exists and this rule never fires -- which is also what makes the weekly
    image rebuild in deploy/refresh-databases.sh refresh the AMR catalog in the same
    pass as CARD and MGEdb. Locally it builds on first use.

    The output is shared by every sample and every job rather than living under a
    results_dir, so two concurrent runs can both want it at once. The build publishes
    its .ready marker last, so the worst a race can do is build the same database twice.
    """
    output:
        marker = f"{AMR_DB_DIR}/.ready"
    params:
        out_dir = AMR_DB_DIR,
        db_name = config['blast']['local_db_name'],
        url = config['blast']['amr_catalog_url']
    conda:
        "../envs/blast.yml"
    log:
        f"{config['results_dir']}/logs/build_amr_blast_db.log"
    shell:
        """
        "$CONDA_PREFIX/bin/python3" workflow/scripts/build_amr_blastdb.py \
            --url-base "{params.url}" \
            --out-dir {params.out_dir} \
            --db-name {params.db_name} \
            2>&1 | tee {log}
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
    Step 13: BLASTP of the antibiotic-inactivation enzymes -- local AMR catalog
    first, NCBI nr only for what it cannot name -- to confirm novelty and infer
    plasmid vs chromosome location. Non-fatal: NCBI is flaky/rate-limited and the
    local db may be unbuilt, so a failed search writes a result with a note rather
    than crashing the run (this step is informational, not a gate).
    """
    input:
        rgi_json = f"{config['results_dir']}/{{sample}}/03_resistance/rgi_results.json",
        # Guarantees the local catalog exists before we search it.
        amr_db = f"{AMR_DB_DIR}/.ready"
    params:
        sample_id = lambda wildcards: wildcards.sample,
        database = config['blast']['database'],
        local_db = AMR_DB_PREFIX,
        remote_fallback = "--remote-fallback" if config['blast']['remote_fallback'] else "",
        timeout = config['blast']['remote_timeout_seconds'],
        evalue = config['blast']['evalue_threshold'],
        max_targets = config['blast']['max_target_seqs'],
        max_queries = config['blast']['max_query_proteins'],
        mechanism = config['blast']['mechanism_filter']
    output:
        blast_csv = f"{config['results_dir']}/{{sample}}/04_blast/blast_results.csv",
        # Complete BLAST response from both tiers -- every hit of every query, verbatim.
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
            --local-db {params.local_db} \
            --database {params.database} \
            {params.remote_fallback} \
            --timeout {params.timeout} \
            --evalue {params.evalue} \
            --max-target-seqs {params.max_targets} \
            --max-queries {params.max_queries} \
            --mechanism "{params.mechanism}" \
            2>&1 | tee {log}
        """
