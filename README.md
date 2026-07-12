# Automated Bioinformatics Genome Analysis Pipeline

A Flask + Snakemake pipeline for paired-end Illumina bacterial isolates. It
uploads reads to BV-BRC for assembly/annotation, then runs local resistance,
novelty, MLST, mobile-element, and reporting steps.

The repository can be used in two ways:

- Web app: upload or import FASTQ files in a browser, receive a job ID, run the
  pipeline, then view/download reports.
- Direct Snakemake: provide a sample manifest and run the workflow from the
  command line.

## What The Pipeline Does

For each isolate, the workflow produces:

1. FASTQ validation and metadata.
2. BV-BRC upload, genus detection, Comprehensive Genome Analysis, assembly,
   taxonomy lookup, and raw CGA output mirror.
3. Assembly metrics parsed from the genome report.
4. CARD RGI resistance gene calls.
5. Resistance novelty report using configured identity/coverage thresholds.
6. Resistance-protein FASTA/CSV extracted from RGI.
7. Remote NCBI BLASTP of selected resistance enzymes against `nr` for
   novelty/location context.
8. MLST and rMLST species identification.
9. MobileElementFinder catalog of known MGEs from MGEdb.
10. Resistance-gene/mobile-element colocation calls.
11. Per-sample HTML report and a batch `master_report.csv`.

## Prerequisites

- macOS or Linux.
- Conda or Mamba.
- Python 3.11 for the base app/workflow environment.
- BV-BRC account.
- Network access to BV-BRC, NCBI BLAST, PubMLST/rMLST, MEF, and package channels
  when creating environments.
- Recommended runtime host: 4+ cores, 8 GB RAM, and enough disk for uploaded
  FASTQ files plus results.

## Installation

Create the base workflow environment:

```bash
conda env create -f workflow/envs/bioinformatics.yml
conda activate bioinformatics
pip install -r requirements.txt
```

Snakemake creates the per-rule tool environments automatically on pipeline runs
when invoked with `--use-conda`. Those envs are defined in `workflow/envs/`:

- `bvbrc.yml`
- `rgi.yml`
- `blast.yml`
- `mlst_arm64.yml`
- `mefinder.yml`

The first run can be slow because Conda builds these environments and downloads
tool/database packages.

## Web App Usage

Start the local development server:

```bash
conda activate bioinformatics
gunicorn -c gunicorn.conf.py frontend:app
```

Open `http://127.0.0.1:5001`.

Typical browser workflow:

1. Upload one R1/R2 pair, or import a folder containing paired FASTQ files.
2. For a paired upload, enter BV-BRC credentials with the upload. For a bulk
   import, open that job's API Settings page afterward and log in there.
3. Save the generated 12-character job ID.
4. Click run.
5. Use the job ID lookup to return to the batch, view per-sample reports,
   download individual ZIPs, download all results, or download the master
   report.

Job IDs are the access boundary for a batch. The app also supports optional HTTP
Basic Auth for public deployments through `APP_USERNAME` and `APP_PASSWORD`.

### Web App Data Layout

The web app stores each batch under a job ID:

```text
data/raw_fastq/<JOB_ID>/              uploaded/imported FASTQ files
config/jobs/<JOB_ID>/samples.csv      Snakemake sample manifest
config/jobs/<JOB_ID>/checksums.json   optional imported MD5 checksums
config/jobs/<JOB_ID>/.bvbrc_token     private BV-BRC token for this job only
config/jobs/<JOB_ID>/api_endpoints.json optional trusted endpoint overrides
results/<JOB_ID>/                     pipeline outputs
results/<JOB_ID>/logs/                per-rule tool logs for the run
logs/<JOB_ID>.log                     full Snakemake log for the run
```

### Concurrent Runs

Pipeline runs are independent per job and different jobs may execute
concurrently. Starting the same job again while it is running (or while it is
queued) returns HTTP 409.

A run is expensive: it is a `snakemake` process that itself runs RGI and
MobileElementFinder, and RGI holds the CARD database in memory. Letting every
user's run start at once is what takes a shared server down — the kernel's OOM
killer reaps processes indiscriminately, so unrelated users' jobs die too.

The app therefore executes at most `MAX_CONCURRENT_PIPELINES` runs at a time and
queues the rest, admitting them FIFO as slots free up:

| Variable                   | Default | Meaning                                 |
| -------------------------- | ------- | --------------------------------------- |
| `MAX_CONCURRENT_PIPELINES` | `2`     | Runs executing at once; the rest queue. |
| `PIPELINE_CORES`           | `4`     | `--cores` given to each run.            |

Budget roughly `MAX_CONCURRENT_PIPELINES × PIPELINE_CORES` cores and a few GB of
RAM per slot. The default of 2 suits the 4-core/8 GB host recommended above, and
can be raised on a bigger box — much of a run's wall time is spent waiting on
BV-BRC assembly and remote NCBI BLAST rather than burning local CPU, so slots
are cheaper than they look.

`POST /run` returns `200` with `queued: false` when the run starts immediately,
and `202` with `queued: true` and a `queue_position` when it is queued. `/status`
reports `queued` and `queue_position` while a job waits. Aborting a queued job
cancels it without ever starting it.

Because the queue lives in the web process's memory, **gunicorn must stay at
`workers = 1`** (see `gunicorn.conf.py`). With multiple workers each would
enforce the cap against only its own slots, so the real limit would silently
become `workers × MAX_CONCURRENT_PIPELINES` — precisely the overload the cap
exists to prevent. Use threads, not workers, for request concurrency.

Concurrent runs pass `--nolock` to Snakemake. Its working-directory lock is
unnecessary here (every path a run writes is scoped to `results/<JOB_ID>/`, so
two jobs never touch the same file) and its teardown deletes the _shared_
`.snakemake/locks` directory rather than just its own lock files — so one run
finishing would strip a concurrent run's locks, and that run would then abort on
its own unlock, reporting a crash despite having completed all its work.

Before serving concurrent traffic on a fresh install, build the per-rule Conda
environments once, so that two runs starting simultaneously don't race to create
the same environment:

```bash
snakemake --use-conda --conda-create-envs-only --cores 1 \
  --config samples_manifest=config/jobs/<JOB_ID>/samples.csv \
           results_dir=results/<JOB_ID>
```

Raw FASTQ files are deleted by Snakemake after QC and BV-BRC upload no longer
need them. Finished job results are retained for 3 hours after the first
terminal status lookup, unless the job has a `.pinned` marker.

## Deployment (AWS EC2 + S3)

The app is deployed as a Docker container on a single EC2 instance, with
finished results pushed to S3 for durability.

**It is a single-host design and cannot be scaled horizontally as written.** The
pipeline queue and rate-limit counters live in one Gunicorn process's memory
(see [Concurrent Runs](#concurrent-runs)), so `gunicorn.conf.py` pins
`workers = 1`. Scale by moving to a bigger instance, not by adding app servers.

Deployment assets live in [deploy/](deploy/):

```text
deploy/app.env.example          every environment variable the app reads
deploy/ec2-user-data.sh         installs Docker + git at instance boot
deploy/iam-policy-s3-results.json  least-privilege S3 access for the instance role
deploy/s3-lifecycle.json        expires stored results after 90 days
deploy/bioinformatics.service   systemd unit (restart on crash and on reboot)
deploy/refresh-databases.sh     rebuilds the image to refresh CARD/MGEdb
```

The `Dockerfile` pre-builds all five per-rule Snakemake environments at image
build time, so a fresh container never stalls on a Conda solve mid-run and two
simultaneous jobs cannot race to create the same environment. The first build is
slow for that reason.

Results storage is controlled by `RESULTS_S3_BUCKET`. When it is set, a
successful run's reports are uploaded to S3, and the view/download routes fall
back to S3 once the local retention sweep removes the on-disk copy. When it is
unset, the app is local-only and results are gone for good after the sweep — see
[Results disappeared](#results-disappeared). Credentials come from the EC2
instance's IAM role; the app never reads static AWS keys.

Persist `data/`, `results/`, `config/jobs/`, and `logs/` on Docker volumes. They
hold in-flight job state — uploads, sample manifests, BV-BRC tokens, and run
logs — and a container restart without them loses any job that is mid-run.

## Direct Snakemake Usage

Create a sample manifest with this header:

```csv
isolate_id,R1_path,R2_path,description
DEMO01,data/raw_fastq/DEMO01_R1.fastq.gz,data/raw_fastq/DEMO01_R2.fastq.gz,optional text
```

By default, `config/config.yaml` points to `config/samples.csv`. You can either
create that file or pass a job-specific manifest and result directory on the
command line:

```bash
conda activate bioinformatics

snakemake --use-conda --cores 4 \
  --config samples_manifest=config/jobs/FYNR2FDVMNF3/samples.csv \
           results_dir=results/FYNR2FDVMNF3
```

Dry-run first when changing manifests or configuration:

```bash
snakemake --dry-run --use-conda --cores 1 \
  --config samples_manifest=config/jobs/FYNR2FDVMNF3/samples.csv \
           results_dir=results/FYNR2FDVMNF3
```

To run one report target:

```bash
snakemake --use-conda --cores 4 \
  --config samples_manifest=config/jobs/FYNR2FDVMNF3/samples.csv \
           results_dir=results/FYNR2FDVMNF3 \
  results/FYNR2FDVMNF3/DEMO01/summary/report.html
```

## Configuration

Main configuration is in [config/config.yaml](config/config.yaml).

Important sections:

- `samples_manifest`: CSV read by Snakemake.
- `results_dir`: root output directory for the current run.
- `logs_dir`: Snakemake/tool logs.
- `bvbrc`: workspace, CLI fallback token file, assembly settings, and polling
  timeout. Web jobs always use their job-specific token.
- `card`: RGI output settings.
- `blast`: remote NCBI BLASTP settings for resistance enzymes.
- `mlst`: scheme detection settings.
- `mobile_elements`: MobileElementFinder threads and ARG/MGE colocation
  distance.
- `resources`: core/memory/time defaults used by rules.
- `report.novelty_thresholds`: thresholds for possible novel resistance
  variants.

The web app overrides `job_id`, `samples_manifest`, and `results_dir` per job
when it launches Snakemake.

## Output Structure

For a web job, outputs are under `results/<JOB_ID>/<ISOLATE_ID>/`:

```text
01_raw_qc/
  validation.txt
  metadata.json
02_assembly/
  upload.log
  genus.txt
  taxonomic_classification.txt
  assembly_contigs.fasta
  genome_report.json
  genome_metrics.csv
  FullGenomeReport.html
  taxonomy_lookup.json
  cga_full/
03_resistance/
  rgi_results.json
  rgi_results.csv
  rgi_results.txt
  novelty_report.txt
04_blast/
  rgi_proteins.fasta
  rgi_proteins.csv
  blast_results.csv
  blast_results_full.tsv
05_mlst/
  mlst_results.txt
  mlst_results.json
  rmlst_raw.json
summary/
  report.html
06_mobile_elements/
  <ISOLATE_ID>.csv
  <ISOLATE_ID>_result.txt
  <ISOLATE_ID>_mge_sequences.fna
  me_summary.csv
  me_summary.json
  <ISOLATE_ID>_arg_mge_colocation.csv
  <ISOLATE_ID>_arg_mge_colocation.json
.raw_deleted
```

Batch-level output:

```text
results/<JOB_ID>/master_report.csv
```

## Mobile Elements

MobileElementFinder runs on each assembly with no reference genome. It aligns
contigs against MGEdb and reports known insertion sequences, transposons, MITEs,
ICEs, IMEs, CIMEs, and related elements.

The workflow also compares RGI resistance-gene coordinates with MGE coordinates.
A resistance gene is treated as mobile-element linked if it overlaps an MGE or
is within `mobile_elements.colocation_proximity_bp` on the same contig.

MGEdb is installed through the `MobileElementFinder` Python package when
`workflow/envs/mefinder.yml` is built. Pin `MobileElementFinder==<version>` in
that env file if exact database reproducibility is required.

## Runtime Expectations

Approximate per-sample runtime:

- BV-BRC assembly/annotation: 40-60 minutes, depending on BV-BRC load.
- RGI: 5-10 minutes.
- Remote NCBI BLASTP: variable; can be rate-limited or unavailable.
- MLST/rMLST: 1-2 minutes.
- MobileElementFinder: 1-3 minutes.
- Full sample: commonly 1.5-2 hours.

Remote BLAST is informational. The rule writes declared outputs even when NCBI
is unavailable so the rest of the report can still complete.

## Development Notes

Main files:

```text
frontend.py                    Flask web app and job lifecycle
workflow/Snakefile             Snakemake entry point
workflow/rules/*.smk           Pipeline rules
workflow/scripts/*.py          Rule implementation scripts
workflow/lib/jobs.py           Job ID paths and validation
workflow/lib/bvbrc_client.py   BV-BRC API client
workflow/lib/import_samples.py Folder import and FASTQ pairing
workflow/lib/preprocess.py     FASTQ/manifest/checksum helpers
workflow/lib/utils.py          Logging, JSON, retry, and file helpers
templates/                     Web UI templates
static/                        Web UI styling
```

## Troubleshooting

### `snakemake: command not found`

Activate the base environment:

```bash
conda activate bioinformatics
```

### `ModuleNotFoundError: No module named 'workflow'`

Run commands from the repository root.

### BV-BRC authentication fails

Confirm the account works at BV-BRC, then re-authenticate through the
paired-upload form or the imported job's API Settings page. The pipeline needs
`config/jobs/<JOB_ID>/.bvbrc_token` before `/run` starts.

### Per-rule Conda environment build fails

Retry with a clean Conda cache or build the specific env manually:

```bash
conda env create -f workflow/envs/rgi.yml
```

In production this should not happen at run time: the [Dockerfile](Dockerfile)
pre-builds every per-rule env at image build time.

### A job says it is already in progress

The same job cannot run twice concurrently. Wait for that run to finish or
abort it from the job page. Other jobs may run at the same time.

### A job sits at "Queued"

All `MAX_CONCURRENT_PIPELINES` slots are busy, so the run is waiting its turn and
will start automatically when a slot frees. This is the server protecting itself
from being overloaded by simultaneous users, not an error. Raise
`MAX_CONCURRENT_PIPELINES` if the host has cores and RAM to spare (see
[Concurrent Runs](#concurrent-runs)), or abort the queued run to cancel it.

### Results disappeared

Local results are pruned 3 hours after their terminal status is first viewed, or
after 7 days.

On a deployment with `RESULTS_S3_BUCKET` set, this is mostly invisible: reports
were uploaded to S3 when the run finished, and the view/download routes fall
back to S3 automatically. Confirm with:

```bash
aws s3 ls s3://<BUCKET>/results/<JOB_ID>/
```

Without S3 configured, pruned results are gone permanently — download them
promptly. For demo jobs that should never expire locally, create:

```bash
touch results/<JOB_ID>/.pinned
```

## References

- BV-BRC API: https://www.bv-brc.org/api/doc/
- CARD RGI: https://github.com/arpcard/rgi
- Snakemake: https://snakemake.readthedocs.io/
- NCBI BLAST: https://blast.ncbi.nlm.nih.gov/doc/blast-help/
- PubMLST: https://pubmlst.org/
- MobileElementFinder: https://cge.food.dtu.dk/services/MobileElementFinder/

## License

Internal research project. Citation requested for publication use.

Created by Kenneth Tran under Professor Xu Yang and California Polytechnic
University, Pomona.
