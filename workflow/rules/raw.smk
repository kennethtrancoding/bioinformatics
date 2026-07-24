"""
Raw reads: fetched from S3 just before they are needed, deleted the moment they
are not.

WHY THIS RULE EXISTS

Raw FASTQ used to be written to local disk at upload time and stay there until the
run finished. That makes peak disk scale with the size of the batch rather than with
what is actually being worked on: a 50-sample job held ~19 GB of reads for hours, and
the reads of every *queued* job sat on disk too, doing nothing at all. On a 100 GB
box that is how you run out of room.

Now the upload streams reads to S3 and keeps no local copy. This rule pulls one back
just before the rules that read it, and its output is marked temp(), so Snakemake
deletes it again as soon as the last of those rules is finished. Peak raw disk
becomes a function of the samples in flight, not of the batch.

WHY THE READS HAD TO BECOME `input:`

validate_fastq and bvbrc_upload_reads used to take the FASTQ paths as `params`, which
means Snakemake had no dependency edge to those files: it could not know the rules
read them, could not sequence a fetch before them, and could not tell when they were
finished with. That is why the old rules/cleanup.smk existed -- a hand-written rule
that waited on the two rules' *outputs* and then unlink()ed the reads itself. It was a
hand-rolled temp(). Declaring the reads as `input:` gives Snakemake the edges, and
temp() then does the cleanup properly, so cleanup.smk is gone.

THE GROUP

fetch + validate + upload for one sample share a group, so Snakemake runs them as a
unit and the fetched reads live only for the span of that unit. Without it the
scheduler would happily run every cheap fetch job up front -- the reads are quick to
download and the BV-BRC upload is slow -- and the whole batch would be on disk again,
which is the very thing this is here to prevent.
"""

RAW_READS_DIR = "data/raw_fastq"


rule fetch_raw_read:
    """
    Materialise one raw FASTQ from S3. Keyed on the file's basename, which is exactly
    how the upload stores it (s3_storage.raw_key_for(job, basename)), so the local
    path the manifest points at and the S3 key are the same name in two places.
    """
    output:
        read = temp(f"{RAW_READS_DIR}/{{job}}/{{filename}}")
    wildcard_constraints:
        # A job ID (see workflow/helpers/jobs.py: 12 chars, no I/L/O/0/1) and a FASTQ
        # basename. Constrained so this rule cannot volunteer to produce arbitrary
        # paths elsewhere in the tree.
        job = r"[A-Z2-9]{12}",
        filename = r"[^/]+\.fastq\.gz"
    resources:
        # An S3 download: no local compute. The group it belongs to is what is
        # rationed (bvbrc_upload_reads takes `uploads=1`), because what is scarce
        # is the reads this rule puts on disk, not the cycles it spends doing it.
        cpu = 0
    log:
        f"{config['results_dir']}/logs/fetch_{{job}}_{{filename}}.log"
    group:
        "sample_reads"
    shell:
        """
        python3 workflow/scripts/fetch_raw_reads.py \
            --job-id {wildcards.job} \
            --name {wildcards.filename} \
            --out {output.read} \
            2>&1 | tee {log}
        """
