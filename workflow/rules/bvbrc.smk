"""
BV-BRC Rules: Steps 4-7
Upload reads, run genome analysis, download assembly

WHAT THESE RULES COST, AND WHAT THEY DO NOT

Almost all of the time a sample spends here is spent waiting on a machine that is
not this one. The genus finder and the CGA submit a job to BV-BRC and then sleep in
a poll loop -- 40-60 minutes for the assembly -- using no local CPU and holding no
local data. Rationing them by cores, as a Snakemake job slot implicitly does, buys
nothing and costs everything: it is what used to serialise a ten-sample batch into
three sequential rounds of waiting.

So they declare `cpu=0` (they compute nothing) and `bvbrc=1` (they are one of the
jobs this run is allowed to have in flight at BV-BRC). The bvbrc pool, not the core
count, is what decides how many samples assemble at once -- see
BVBRC_MAX_IN_FLIGHT in workflow/helpers/pipeline_manager.py.

The upload is the exception, and is bounded on purpose. It is the one BV-BRC rule
that touches a local FASTQ, and a FASTQ on disk is the scarce thing (see
rules/raw.smk): letting the whole batch upload at once would put the whole batch's
reads on disk at once, which is precisely what raw.smk exists to prevent. It takes
`uploads=1` from a small pool, so reads are fed to BV-BRC a few samples at a time
while any number of already-uploaded samples assemble in parallel.
"""

rule bvbrc_upload_reads:
    """
    Step 4: Upload paired-end reads to BV-BRC workspace

    The reads are `input`, not `params` -- see rules/raw.smk. This rule and
    validate_fastq are the only readers of a sample's FASTQ, so once both are done
    Snakemake deletes it: this is the last thing that needs the raw data locally.
    """
    input:
        first_read = lambda wildcards: SAMPLES[wildcards.sample]['R1_path'],
        second_read = lambda wildcards: SAMPLES[wildcards.sample]['R2_path']
    params:
        sample_id = lambda wildcards: wildcards.sample
    output:
        upload_log = f"{config['results_dir']}/{{sample}}/02_assembly/upload.log"
    resources:
        # Streams bytes to BV-BRC; the work is theirs and the network's, not this
        # box's. Bounded instead by the reads it needs on disk to do it.
        cpu = 0,
        uploads = 1
    log:
        f"{config['results_dir']}/logs/{{sample}}_bvbrc_upload.log"
    group:
        "sample_reads"
    script:
        "../scripts/bvbrc_upload.py"


rule bvbrc_similar_genome_finder:
    """
    Step 5: Identify bacterial genus using Similar Genome Finder
    """
    input:
        f"{config['results_dir']}/{{sample}}/02_assembly/upload.log"
    params:
        sample_id = lambda wildcards: wildcards.sample
    output:
        genus_file = f"{config['results_dir']}/{{sample}}/02_assembly/genus.txt",
        # Raw Kraken2 TaxonomicClassification report, saved verbatim -- not just
        # the best-genus line this rule extracts from it. Always written (a
        # placeholder explaining the failure if the BV-BRC job doesn't complete),
        # so it's never left behind in a scratch/tmp location.
        kraken_report = f"{config['results_dir']}/{{sample}}/02_assembly/taxonomic_classification.txt"
    resources:
        cpu = 0,
        bvbrc = 1
    log:
        f"{config['results_dir']}/logs/{{sample}}_bvbrc_genus.log"
    script:
        "../scripts/bvbrc_genus_finder.py"


rule bvbrc_comprehensive_genome_analysis:
    """
    Step 6: Run Comprehensive Genome Analysis (assembly + annotation)
    Typical runtime: 40-60 minutes
    """
    input:
        genus_file = f"{config['results_dir']}/{{sample}}/02_assembly/genus.txt",
        upload_log = f"{config['results_dir']}/{{sample}}/02_assembly/upload.log"
    params:
        sample_id = lambda wildcards: wildcards.sample,
        max_wait_time = config['bvbrc']['max_wait_time']
    output:
        assembly_fasta = f"{config['results_dir']}/{{sample}}/02_assembly/assembly_contigs.fasta",
        genome_report = f"{config['results_dir']}/{{sample}}/02_assembly/genome_report.json",
        full_report = f"{config['results_dir']}/{{sample}}/02_assembly/FullGenomeReport.html",
        # Everything else the CGA job produced (annotation, protein/feature
        # files, quality report, etc.), mirrored verbatim from the workspace --
        # not just the three curated files above.
        cga_raw_dir = directory(f"{config['results_dir']}/{{sample}}/02_assembly/cga_full"),
        # BV-BRC taxonomy API lookup (genus -> taxon_id) used to submit the
        # job; otherwise only ever logged, never saved as its own result.
        taxonomy_lookup = f"{config['results_dir']}/{{sample}}/02_assembly/taxonomy_lookup.json"
    resources:
        # The 40-60 minutes are BV-BRC's, spent in a poll loop here. This is the
        # rule the bvbrc pool exists for: with it, a batch assembles all at once
        # instead of `cores` at a time.
        cpu = 0,
        bvbrc = 1
    log:
        f"{config['results_dir']}/logs/{{sample}}_bvbrc_cga.log"
    script:
        "../scripts/bvbrc_cga.py"


rule parse_genome_report:
    """
    Step 8: Parse and extract key metrics from genome report
    (contigs, GC content, genome length, N50, L50, etc.)
    """
    input:
        f"{config['results_dir']}/{{sample}}/02_assembly/genome_report.json"
    output:
        metrics = f"{config['results_dir']}/{{sample}}/02_assembly/genome_metrics.csv"
    log:
        f"{config['results_dir']}/logs/{{sample}}_parse_report.log"
    script:
        "../scripts/parse_genome_report.py"
