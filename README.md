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
   (Pasting a OneDrive/Google Drive share link is currently disabled — see
   [Cloud Imports](#cloud-imports-onedrive--google-drive).)
2. Add files through any combination of the pair and folder methods.
3. Enter BV-BRC credentials, then press Run Pipeline after every upload finishes.
4. Save the generated 12-character job ID.
5. Use the job ID lookup to return to the batch, view per-sample reports,
   download individual ZIPs, download all results, or download the master
   report.

Job IDs are the access boundary for a batch. The app also supports optional HTTP
Basic Auth for public deployments through `APP_USERNAME` and `APP_PASSWORD`.

### Building One Batch From Several Uploads

The browser reserves one job automatically when the first Add action is pressed.
Every upload method reuses that job, so a batch can be assembled from any mix of
them — a folder import for the bulk of a run, and individual pairs on top (plus,
when cloud import is enabled, a cloud pull for isolates that arrived late). They
may be in flight at the same time, and every later Add on the page continues
filling the same batch. Once all
files have arrived, press Run Pipeline.

Re-adding an isolate that is already in the batch replaces its rows rather than
duplicating them (`updated` in the response, not `added`), so re-uploading to fix
a bad file is safe — as long as the pipeline has not been run yet (see below).

Each upload is a read-modify-write of one `samples.csv`, so concurrent adds to
the same job are serialized on a per-job lock; adds to _different_ jobs never
block each other. **A job's samples can only be added to or deleted from before
its pipeline is first run.** Once a run is queued or running, changing the
manifest or the FASTQ it points at would be silently ignored or read
half-written; once a run has finished — completed **or** failed — the samples are
the inputs that produced its results, so editing them would leave results that no
longer match their inputs. Both `/submit`/`/import` and `/delete` return `409` in
those states. A different set of files is a different job: start a new one.

### Timing

The job panel reports how long each upload took, broken down by the method that
brought it in, and how long the run took. A run in progress shows elapsed time
(recomputed locally each second between the 3-second status polls); a finished
run shows its total duration. `/job/<id>` returns this as `uploads[]` and
`run_status.started_at` / `run_status.finished_at`.

### Cloud Imports (OneDrive / Google Drive)

> **Disabled.** The `/cloud-import` routes in `frontend.py`, the fieldset in
> `templates/index.html` and the handlers in `static/app.js` are commented out, so
> the share-link box does not appear and the routes return 404. Uploading a pair
> and importing a folder are unaffected. `workflow/lib/cloud_import.py` and its
> unit tests are untouched; re-enabling means uncommenting those three call sites.
> The rest of this section describes the feature as it works when enabled.

A sequencing company usually leaves the run in a cloud folder and mails a share
link. Pasting that link into "Import From OneDrive or Google Drive" makes the
_server_ fetch the files, so tens of GB never have to be pulled down to a laptop
and pushed back up again.

The link must be shared as "anyone with the link" and may point at a folder or a
single FASTQ file. What arrives is treated exactly like a browser folder upload:
the same R1/R2 pairing, the same MD5 verification against the company's
`DNA Sequencing Stats.xlsx`, the same job ID, the same Samples table, the same
`/run`. Nothing downstream can tell a cloud-imported job from an uploaded one.

The download runs in the background because a real run folder takes far longer
than an HTTP request can be held open. `POST /cloud-import` returns `202` with a
job ID immediately; the page then polls `/cloud-import/status` and shows the
batch when it lands.

Because the URL comes from the user but the _server_ is what requests it, an
unfiltered version of this feature is an SSRF hole aimed at the instance metadata
service. Requests are therefore restricted to Google Drive and OneDrive/SharePoint
hosts, and **every redirect hop is re-checked against that allowlist** — a
`1drv.ms` link bounces through two hosts before reaching the content server, so
validating only the pasted URL would not be a guard at all. The bytes that come
back are still untrusted, so only FASTQ files and the `.xlsx` stats workbook are
pulled, each is size-capped, and each FASTQ must parse as FASTQ before it is
allowed into the manifest — both providers answer an unshared link with a `200`
and an HTML sign-in page rather than an HTTP error, and that page must never land
on disk as a `.fastq.gz`.

| Variable                       | Default | Meaning                                                        |
| ------------------------------ | ------- | -------------------------------------------------------------- |
| `GOOGLE_DRIVE_API_KEY`         | unset   | Required to list a Drive **folder**. Without it only single-file Drive links work. |
| `MS_GRAPH_ACCESS_TOKEN`        | unset   | Required for OneDrive for **Business**/SharePoint links. Consumer OneDrive links work without it. |
| `MAX_CONCURRENT_CLOUD_IMPORTS` | `2`     | Server-side pulls running at once; further requests get `429`.  |
| `CLOUD_IMPORT_MAX_FILE_BYTES`  | 20 GiB  | Per-file cap. An oversized file is skipped with a warning.      |
| `CLOUD_IMPORT_MAX_TOTAL_BYTES` | 200 GiB | Cap for one import. Exceeding it aborts the import.             |
| `CLOUD_IMPORT_MAX_FILES`       | `500`   | Most files one link may hold.                                   |
| `CLOUD_IMPORT_ALLOWED_HOSTS`   | unset   | Extra **content** hosts to accept on a redirect hop. Does not widen what counts as a share link. |

Both credentials are optional and read-only: a Drive API key with the Drive API
enabled is enough for public folders, and neither is needed to try the feature
with a consumer OneDrive link.

### Web App Data Layout

The web app stores each batch under a job ID:

```text
data/raw_fastq/<JOB_ID>/              uploaded/imported FASTQ files
config/jobs/<JOB_ID>/samples.csv      Snakemake sample manifest
config/jobs/<JOB_ID>/uploads.json     every upload that filled this job, and how long each took
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

Because the queue is enforced in the web process's memory, **gunicorn must stay at
`workers = 1`** (see `gunicorn.conf.py`). With multiple workers each would
enforce the cap against only its own slots, so the real limit would silently
become `workers × MAX_CONCURRENT_PIPELINES` — precisely the overload the cap
exists to prevent. Use threads, not workers, for request concurrency.

### Where raw reads live

Uploaded FASTQ is streamed to S3 **as it arrives** and the local copy is then dropped.
The pipeline fetches each read back from S3 immediately before the rules that need it
(`fetch_raw_read` in `workflow/rules/raw.smk`), and that rule's output is `temp()`, so
Snakemake deletes it again the moment the last reader — `validate_fastq` and
`bvbrc_upload_reads` — is done.

The point is that **peak disk tracks the samples in flight, not the size of the
batch.** Reads used to sit on disk from upload until the run finished, and the reads
of every *queued* job sat there too, doing nothing: a 50-sample batch held ~19 GB of
FASTQ for hours. Now it holds well under a gigabyte.

This is why `validate_fastq` and `bvbrc_upload_reads` declare the reads as `input:`
rather than `params:`. As params, Snakemake had no dependency edge to the files — it
could not sequence a fetch before them, and could not tell when they were finished
with. (The old `rules/cleanup.smk` existed for exactly that reason: a hand-written
rule that waited on the two rules' *outputs* and unlinked the reads itself. It was a
hand-rolled `temp()`, and it is gone.)

Without `RESULTS_S3_BUCKET` set, none of this applies: the local copy is the only
copy, so it is kept and the fetch rule never fires. **With** a bucket set, S3 is the
system of record for raw reads — a run cannot start while S3 is unreachable, where
previously it could.

### Runs and restarts

A run's Snakemake process is a child of the web process, so anything that takes
the web process down — a crash, a redeploy, the weekly database refresh — takes
the run with it. Both halves of that are handled:

- **Planned restarts drain first.** `deploy/refresh-databases.sh` sets a drain
  flag before it restarts the service. While it is set, new runs are queued rather
  than started (even when slots are free), and the script waits for the running
  ones to finish before restarting — or gives up and leaves the databases
  untouched, rather than killing a multi-hour assembly. It waits on **in-flight
  uploads too**: `/submit` and `/import` stage, verify and push their reads to S3
  inside the request, so a restart under one loses that upload. Both counts come
  from `/api/health` (`pipelines.running`, `uploads.in_flight`). Uploads are still
  *accepted* while draining — refusing one would cost the user the very thing the
  drain protects — so if they keep arriving the refresh times out and skips. See
  [DATABASE_UPDATES.md](DATABASE_UPDATES.md).
- **Unplanned restarts are reconciled on boot.** The queue is persisted to
  `config/jobs/.pipeline_queue.json` (on the config volume) and reloaded at
  startup, so queued runs start rather than disappear. A run that was *executing*
  cannot be resumed — it is recorded as failed, with an explanation, so it surfaces
  as a failure the user can re-run instead of a run that silently stops reporting.

That recovery runs from gunicorn's `post_worker_init` hook, not at import: tests
import `frontend` before repointing it at a temp directory, so reconciling at
import time would scan the real `results/` and fail whatever run was in progress.

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

**The job ID is deleted with them.** When a job expires, the sweep removes
everything keyed by that ID in one go: the local results, the raw reads, the
reports stored in S3, `config/jobs/<JOB_ID>/` (manifest, upload log, checksums,
endpoint overrides, BV-BRC token) and `logs/<JOB_ID>.log`. The ID stops
resolving because nothing it named is left. An upload that is never run is
collected the same way once it has sat unused for 7 days.

Deleting the stored reports is what makes the 3-hour and 7-day windows real
rather than local-only: the view and download routes fall back to S3 when the
on-disk copy is gone, so a job whose results were left in the bucket kept
serving them long after it had been told to expire.

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
deploy/s3-lifecycle.json        backstop expiry for objects no sweep reached
deploy/bioinformatics.service   systemd unit (restart on crash and on reboot)
deploy/refresh-databases.sh     rebuilds the image to refresh CARD/MGEdb
deploy/bioinformatics-db-refresh.service  runs that script as a systemd unit
deploy/bioinformatics-db-refresh.timer    fires it Sunday 03:00 America/Los_Angeles
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

### State durability

Those volumes survive a container restart, but not the loss of the instance or its
EBS volume. Finished results are safe — with `RESULTS_S3_BUCKET` set they are in S3
— but the in-flight state (manifests, tokens, the persisted queue, run history, and
any run in progress) lives only on the instance's disk. A dead EBS volume loses it.

`deploy/dlm-snapshot-policy.json` is an AWS Data Lifecycle Manager policy that
snapshots the state volume every 6 hours and keeps a week of them, so a lost
instance costs at most the jobs submitted since the last snapshot — and re-running
those is cheap and safe (Snakemake skips completed samples). It is the
scale-appropriate fix at two-active-runs: a block-level snapshot with no
application code, and no BV-BRC tokens copied into object storage. Setup and restore
are in [deploy/backups.md](deploy/backups.md).

## Scaling beyond one host

The single most common question about this deployment is how to auto-scale it. The
honest answer is that **it cannot be horizontally auto-scaled as written**, and the
reason is worth understanding before reaching for an Auto Scaling Group.

The concurrency cap and the run queue live in **one Gunicorn process's memory**
(`gunicorn.conf.py` pins `workers = 1`; see [Concurrent Runs](#concurrent-runs)),
and each job's state is a set of files on that instance's own volume. Put a second
app instance behind a load balancer and three things break at once:

- **The cap stops meaning anything.** Each instance enforces
  `MAX_CONCURRENT_PIPELINES` against only its own runs, so the real ceiling becomes
  `instances × MAX_CONCURRENT_PIPELINES` — precisely the overload the cap exists to
  prevent, and the OOM the box was protecting itself from.
- **Jobs vanish between instances.** A job created on instance A is a directory on
  A's volume; a status poll routed to B returns 404.
- **The queue forks.** Each instance has its own in-memory queue and its own view of
  what is running, so neither can schedule against the whole box.

An Auto Scaling Group that adds and removes instances would split and lose job state
on every scaling action. So "auto scaling" here has three real meanings, only one of
which is horizontal:

1. **Vertical — the supported answer.** Move to a bigger instance and raise
   `PIPELINE_CORES` / `MAX_CONCURRENT_PIPELINES`. The local stage of a run is
   CPU-bound and linear in sample count, so cores are the lever that shortens a big
   batch (see [What a batch costs](#what-a-batch-costs-and-what-bounds-it)). Not
   automatic, but it is the honest way to add capacity to this design.

2. **An ASG of size 1 — for self-healing, not scale-out.** `min = max = desired = 1`
   plus a health check never runs two app instances; it just replaces a dead one
   automatically. For that to preserve jobs, the state must outlive the instance —
   which is exactly what the dedicated data volume + snapshots in
   [deploy/backups.md](deploy/backups.md) provide. Without decoupled or restorable
   state, the replacement comes up empty, and you have bought availability of the app
   tier while losing the work. This is the most availability you can add without the
   redesign below.

3. **True horizontal scale — decouple state, then move execution off the box.** This
   is the point at which the usual "scale it" advice (a metadata database, a message
   broker, a control/data-plane split) finally earns its keep, because there are
   finally multiple workers and app instances to coordinate:

   - **Shared state.** Move job metadata and the queue out of process memory and
     local files into a shared store — an RDS/PostgreSQL instance for metadata and
     queue, with S3 continuing to hold the blobs. Now any app instance can see any
     job, and the cap can be enforced centrally instead of per-process.
   - **Execution off the app box.** Submit the pipeline's local rules to **AWS Batch**
     or a cluster via Snakemake's native executors (already flagged in
     [What a batch costs](#what-a-batch-costs-and-what-bounds-it) as the escape hatch
     past a few hundred samples). The app tier becomes stateless and *can* sit behind
     an ASG that scales on request load, while the heavy compute scales on Batch's own
     managed capacity — the control plane and data plane separating along the seam
     that already exists informally today.

**When to actually do #3.** Not request volume — this app will never be QPS-bound;
its unit of work is an hours-long batch, not a web request. The trigger is either
(a) genuine multi-tenant use, where several institutions need isolation and
independent capacity rather than one shared filesystem and one shared queue, or
(b) batches past a few hundred samples, where one box's local stage takes days.
Until one of those is real, **vertical scaling plus an ASG of 1 plus snapshots** is
the correct, proportionate posture, and the redesign above is premature. When it
does become real, [docs/horizontal-scale.md](docs/horizontal-scale.md) is the full
redesign plan and AWS console runbook for it.

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

### What a batch costs, and what bounds it

Most of that per-sample time is spent *waiting on BV-BRC*, not working here. A run
therefore hands Snakemake three separate budgets instead of one core count, because
three different things are scarce and they are not the same thing:

| Pool | Env | Default | What it bounds |
| --- | --- | --- | --- |
| `cpu` | `PIPELINE_CORES` | 4 | The cores the box has. RGI and MobileElementFinder charge their full thread count; every other rule charges 1. |
| `bvbrc` | `BVBRC_MAX_IN_FLIGHT` | 12 | Samples assembling at BV-BRC at once. Not a hardware limit -- the work is theirs -- so it is sized for the batch. |
| `uploads` | `BVBRC_UPLOAD_BATCH` | 4 | Samples holding raw FASTQ on local disk while they feed it to BV-BRC. Small on purpose (see `workflow/rules/raw.smk`). |

`--cores` is then not a CPU budget at all but a job-slot budget, sized so it is
never the binding constraint. This is deliberate: a Snakemake job slot used to be
required for each of those 40-minute waits, so only `PIPELINE_CORES` samples could
*wait* at a time and a ten-sample batch took three sequential rounds of assembly to
do what one round can. Now the batch is uploaded a few samples at a time (disk is
the constraint there) and assembled all at once.

The consequence is that the two stages of a run scale differently, and the second
one is what grows with batch size:

- **Remote** (upload, genus, assembly): `~90 min x ceil(N / BVBRC_MAX_IN_FLIGHT)`.
  Flat for any batch that fits in the pool. Costs this box nothing.
- **Local** (RGI, BLAST, MLST, MobileElementFinder, reports): `~3600 core-seconds x
  N / PIPELINE_CORES` -- 15 min a sample on four cores. RGI and MEF each take the
  whole CPU pool, so they serialise, and this term is linear in N however many BV-BRC
  slots there are. **Cores are the only thing that shorten it.**

Which means the two knobs matter at different scales, and it is worth being blunt
about where this design stops working:

| Batch | Assembly rounds (in-flight 12) | Remote | Local (4 cores) | Total |
| --- | --- | --- | --- | --- |
| 10 | 1 | 1.5 h | 2.5 h | **~4 h** |
| 100 | 9 | 13.5 h | 25 h | **~38 h** |
| 1000 | 84 | 126 h | 250 h | **~16 days** |

At ten samples, raising `BVBRC_MAX_IN_FLIGHT` is what buys you the win (it is the
whole reason this batch takes one round of assembly instead of three). At a hundred
it buys much less, and at a thousand almost nothing: the local stage is 250 hours of
RGI and MobileElementFinder on four cores, and no amount of BV-BRC parallelism
touches it. Batches of that size need a bigger `PIPELINE_CORES` on a bigger
instance -- or, past a few hundred samples, a different execution backend
(Snakemake can submit the local rules to a cluster or to AWS Batch, which is the
point at which one box stops being the right shape).

Two things to know before raising `BVBRC_MAX_IN_FLIGHT`:

- **BV-BRC's queue is not free.** The more jobs you have queued there at once, the
  longer each one sits, and `bvbrc.max_wait_time` (`config/config.yaml`, 2 h) is the
  point at which the poll gives up and the sample *fails*. Pushing a hundred
  assemblies at a shared public service in one go is a good way to convert their
  queue time into timeouts. Raise `max_wait_time` alongside it.
- **Each in-flight sample is a live poll process here** (tens of MB). Twelve is
  nothing; two hundred is gigabytes of idle Python.

### The estimates the page shows

The page turns the above into the two times a waiting user actually wants: how long
a queued run has until it starts, and how much longer a running one has to go. Both
come from one estimate in `workflow/lib/run_estimates.py`, which is the two-term
model above (`RUN_REMOTE_SECONDS`, `RUN_LOCAL_CORE_SECONDS_PER_SAMPLE`) multiplied by
a correction learned from this instance: every successful run divides what it actually
took by what the model predicted, and the estimate uses the median of the last 25
such ratios (`config/jobs/.run_history.json`, on the persistent volume -- an estimate
built from history is worth nothing if a restart throws the history away). A fresh
instance has no ratios and trusts the model as written, which is why its constants
are anchored on the figures above: one sample is 90 + 15 min, the 1h45m quoted there.

N is the samples a run *has to do*, not the samples in the manifest. Re-running is
how a failed run is recovered, and a re-run keeps every sample that already finished
-- admission clears the run markers but not the results, so Snakemake skips them.
Counting them would quote the re-run a full job's runtime and then record its short
actual duration as the cost of a full job, teaching every later estimate that the
pipeline is several times faster than it is.

A queued run's wait is a simulation of the drain that will start it: each running job
holds its slot until its estimate says it finishes, and each job ahead in the queue
takes the first slot to come free.

They are estimates, and the page says so. A run whose BV-BRC assembly sits in a
long queue will overrun its estimate, and is reported as running past it rather
than as counting down to a time that has already passed.

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
workflow/lib/cloud_import.py   OneDrive/Google Drive share-link pulls
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

### A OneDrive/Google Drive link will not import

The message on the page says which of these it is:

- _"Only Google Drive and OneDrive/SharePoint https:// share links can be
  imported"_ — the host is not on the allowlist. Copy the `https://` link from
  the browser's address bar rather than a forwarded or shortened one.
- _"came back as a web page instead of a file"_ / _"refused access"_ — the item
  is not shared with "anyone with the link", so the provider served a sign-in
  page instead of the bytes.
- _"needs a Drive API key"_ — set `GOOGLE_DRIVE_API_KEY`; listing a Drive folder
  is not possible anonymously. A link to a single file still works without it.
- _"also needs MS_GRAPH_ACCESS_TOKEN"_ — the link is OneDrive for Business or
  SharePoint, which cannot be read anonymously.
- _"Nothing behind that link is a FASTQ file"_ — the folder holds no `.fastq`,
  `.fq`, `.fastq.gz`, or `.fq.gz`. Only those and the `.xlsx` stats workbook are
  ever downloaded.

See [Cloud Imports](#cloud-imports-onedrive--google-drive).

### Results disappeared

Results are deleted 3 hours after their terminal status is first viewed, or
after 7 days — and the job ID goes with them, so an expired ID no longer
resolves anywhere. That is expected, and it applies to the copies in S3 too: a
`.pinned` marker is the only thing that holds a job open. A different set of
files is a new job.

The S3 fallback does not extend that window. It exists so a job that has *not*
expired survives losing its local disk (an instance replaced, a volume
recreated): the reports were uploaded when the run finished, and the
view/download routes read them from the bucket when the on-disk copy is missing.
Check what is still stored for a job with:

```bash
aws s3 ls s3://kennethtrancoding-bioinformatics-bucket/results/<JOB_ID>/
```

An expired job lists nothing. If a job that should still be live lists nothing
either, its run never finished successfully — nothing is uploaded until it does.

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

Created by Kenneth Tran under Professor Xu Yang at California Polytechnic
University, Pomona.
