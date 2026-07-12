# Production image for the bioinformatics pipeline web app.
#
# Two-layer conda setup:
#   1. The "bioinformatics" env (this Dockerfile) runs Flask/gunicorn/Snakemake.
#   2. Snakemake's per-rule envs (bvbrc, rgi, blast, mlst_arm64, mefinder --
#      see workflow/envs/) are pre-built below with a throwaway job so the
#      first real upload on a fresh container doesn't stall for minutes, and
#      two jobs starting at once don't race to create the same env (see the
#      README's "Concurrent Runs" section).
#
# Build (from the repo root):
#   docker build -t bioinformatics-pipeline .
#
# Run:
#   docker run -d --name bioinformatics -p 5001:5001 \
#     -e APP_PASSWORD=... -e SECRET_KEY=$(openssl rand -hex 32) \
#     -e RESULTS_S3_BUCKET=your-bucket \
#     -v bioinformatics-data:/app/data \
#     -v bioinformatics-results:/app/results \
#     -v bioinformatics-config:/app/config/jobs \
#     -v bioinformatics-logs:/app/logs \
#     bioinformatics-pipeline
#
# data/, results/, config/jobs/, and logs/ hold in-flight job state (raw
# uploads, samples.csv manifests, BV-BRC tokens, Snakemake logs) and must be
# on a persistent volume -- an ephemeral container filesystem would lose a
# running job on every restart/redeploy. Finished-job *results* additionally
# get pushed to S3 when RESULTS_S3_BUCKET is set (see workflow/lib/s3_storage.py),
# which is what actually survives instance replacement.

FROM condaforge/miniforge3:24.9.2-0

WORKDIR /app

# Conda solves are the slowest and least-often-changed part of the build, so
# they get their own layer, cached across app-code-only rebuilds.
COPY workflow/envs/bioinformatics.yml workflow/envs/bioinformatics.yml
COPY requirements.txt requirements.txt
RUN mamba env create -f workflow/envs/bioinformatics.yml \
    && conda run -n bioinformatics pip install --no-cache-dir -r requirements.txt \
    && mamba clean -afy

COPY . .

# Pre-build the per-rule envs against a throwaway job so Snakemake's content
# hash lookup (.snakemake/conda/<hash>/) is already warm before real traffic
# arrives. Uses empty placeholder FASTQ files -- conda-create-envs-only never
# executes rule bodies, it only needs the DAG to resolve down to files that
# exist.
RUN set -eux; \
    mkdir -p /tmp/imgbuild config/jobs/IMGBUILD0001 results/IMGBUILD0001; \
    : | gzip > /tmp/imgbuild/R1.fastq.gz; \
    : | gzip > /tmp/imgbuild/R2.fastq.gz; \
    printf 'isolate_id,R1_path,R2_path,description\nBUILD,/tmp/imgbuild/R1.fastq.gz,/tmp/imgbuild/R2.fastq.gz,\n' \
        > config/jobs/IMGBUILD0001/samples.csv; \
    conda run --no-capture-output -n bioinformatics snakemake \
        --use-conda --conda-create-envs-only --cores 1 \
        --config job_id=IMGBUILD0001 \
                  samples_manifest=config/jobs/IMGBUILD0001/samples.csv \
                  results_dir=results/IMGBUILD0001; \
    rm -rf config/jobs/IMGBUILD0001 results/IMGBUILD0001 /tmp/imgbuild; \
    mamba clean -afy

ENV PORT=5001
EXPOSE 5001

HEALTHCHECK --interval=30s --timeout=5s --start-period=30s --retries=3 \
    CMD conda run -n bioinformatics python -c \
        "import os,urllib.request; urllib.request.urlopen('http://127.0.0.1:' + os.environ.get('PORT','5001') + '/api/health', timeout=3)"

ENTRYPOINT ["./docker-entrypoint.sh"]
