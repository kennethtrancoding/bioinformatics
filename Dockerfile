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
#     -e RESULTS_S3_BUCKET=kennethtrancoding-bioinformatics-bucket \
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

# Conda solves are the slowest and least-often-changed part of the build, so the
# layers below are ordered least- to most-frequently-changed. This one is keyed
# on just the two dependency files, so it survives any change to the rest of the
# tree -- including the per-rule env specs.
COPY workflow/envs/bioinformatics.yml workflow/envs/bioinformatics.yml
COPY requirements.txt requirements.txt
RUN mamba env create -f workflow/envs/bioinformatics.yml \
    && conda run -n bioinformatics pip install --no-cache-dir -r requirements.txt \
    && mamba clean -afy

# Only the files Snakemake parses to resolve the DAG, so that editing app code
# (frontend.py, templates/, workflow/lib/) does not invalidate the expensive
# env pre-build below. Everything else arrives in the COPY . . after it.
#   Snakefile      -- the workflow itself
#   rules/         -- include:d, and carry the conda: directives
#   envs/          -- the env specs those directives point at
#   scripts/       -- rules declare script:, which must resolve at parse time
#   config.yaml    -- configfile:
COPY workflow/Snakefile workflow/Snakefile
COPY workflow/rules/ workflow/rules/
COPY workflow/envs/ workflow/envs/
COPY workflow/scripts/ workflow/scripts/
COPY config/config.yaml config/config.yaml

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
    conda run --no-capture-output -n bioinformatics snakemake \
        resources/blastdb/amr/.ready \
        --use-conda --cores 1 --nolock \
        --config job_id=IMGBUILD0001 \
                  samples_manifest=config/jobs/IMGBUILD0001/samples.csv \
                  results_dir=results/IMGBUILD0001; \
    date -u +%Y-%m-%dT%H:%M:%SZ > resources/blastdb/.db_built_at; \
    rm -rf config/jobs/IMGBUILD0001 results/IMGBUILD0001 /tmp/imgbuild; \
    mamba clean -afy

# The frontend reads resources/blastdb/.db_built_at to show "reference databases
# last updated" (see _reference_databases_updated_at). It is written in the same
# layer that builds CARD/MGEdb/AMRProt above, so it moves exactly when they do:
# if that layer is cache-reused, the databases are unchanged and so is the stamp.
# It sits under resources/blastdb/, which .dockerignore excludes, so the COPY . .
# below never overwrites it with a developer's local copy.

# The second snakemake call above is not part of the env warm-up: it actually RUNS
# rule build_amr_blast_db, which downloads NCBI's AMRFinderPlus reference protein
# catalog (~10k proteins, ~5 MB) and formats it with makeblastdb. It is invoked
# through snakemake rather than directly because makeblastdb lives in the *blast*
# per-rule env (just created above), not in the `bioinformatics` env that runs Flask.
#
# The catalog lands in resources/blastdb/, inside the image rather than on a volume,
# for the same reason CARD and MGEdb do (see DATABASE_UPDATES.md): the image IS the
# database, so the weekly rebuild in deploy/refresh-databases.sh refreshes all three
# in a single pass.

# Last, so a code-only change rebuilds just this layer and reuses every conda
# env above. Safe to re-copy workflow/ and config/ over themselves: .snakemake/
# (the envs the pre-build just created) and resources/blastdb/ (the AMR database it
# just built) are both in .dockerignore, so this cannot clobber either.
COPY . .

ENV PORT=5001
EXPOSE 5001

HEALTHCHECK --interval=30s --timeout=5s --start-period=30s --retries=3 \
    CMD conda run -n bioinformatics python -c \
        "import os,urllib.request; urllib.request.urlopen('http://127.0.0.1:' + os.environ.get('PORT','5001') + '/api/health', timeout=3)"

ENTRYPOINT ["./docker-entrypoint.sh"]
