"""
BV-BRC Rules: Steps 4-7
Upload reads, run genome analysis, download assembly
"""

rule bvbrc_upload_reads:
    """
    Step 4: Upload paired-end reads to BV-BRC workspace
    """
    params:
        first_read_path = lambda wildcards: SAMPLES[wildcards.sample]['R1_path'],
        second_read_path = lambda wildcards: SAMPLES[wildcards.sample]['R2_path'],
        sample_id = lambda wildcards: wildcards.sample
    output:
        upload_log = f"{config['results_dir']}/{{sample}}/02_assembly/upload.log"
    log:
        f"{config['results_dir']}/logs/{{sample}}_bvbrc_upload.log"
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
