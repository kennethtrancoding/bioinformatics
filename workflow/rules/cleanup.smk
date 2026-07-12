"""
Data retention: delete a sample's raw FASTQ as soon as nothing else needs it.

Both validate_fastq (01_raw_qc) and bvbrc_upload_reads (02_assembly) read the
raw R1/R2 files directly from the sample manifest and have no dependency on each
other, so this rule waits on both of their outputs explicitly rather than
assuming one runs before the other.
"""

from pathlib import Path

rule cleanup_raw_fastq:
    input:
        qc = f"{config['results_dir']}/{{sample}}/01_raw_qc/validation.txt",
        upload = f"{config['results_dir']}/{{sample}}/02_assembly/upload.log",
    output:
        marker = f"{config['results_dir']}/{{sample}}/.raw_deleted",
    run:
        for key in ("R1_path", "R2_path"):
            raw = Path(SAMPLES[wildcards.sample][key])
            if raw.is_file():
                raw.unlink()
        Path(output.marker).touch()
