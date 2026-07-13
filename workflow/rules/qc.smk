"""
QC Rules: Steps 1-3
Verify MD5 checksums, download/validate FASTQ files, prepare metadata
"""

rule validate_fastq:
    """
    Step 1-3: Validate FASTQ integrity and generate QC report

    The reads are `input`, not `params`: that is what tells Snakemake this rule
    depends on them, so it can fetch them from S3 first (rules/raw.smk) and delete
    them again once nothing else needs them.
    """
    input:
        first_read = lambda wildcards: SAMPLES[wildcards.sample]['R1_path'],
        second_read = lambda wildcards: SAMPLES[wildcards.sample]['R2_path']
    params:
        sample_id = lambda wildcards: wildcards.sample
    output:
        report = f"{config['results_dir']}/{{sample}}/01_raw_qc/validation.txt",
        metadata = f"{config['results_dir']}/{{sample}}/01_raw_qc/metadata.json"
    log:
        f"{config['results_dir']}/logs/{{sample}}_validate_fastq.log"
    group:
        "sample_reads"
    script:
        "../scripts/qc_validate.py"
