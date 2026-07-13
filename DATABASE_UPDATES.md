# Database Updates

The pipeline depends on three reference databases:

| Database  | Used by                | Comes from                                    |
| --------- | ---------------------- | --------------------------------------------- |
| **CARD**  | RGI (resistance genes) | the `rgi` package, `workflow/envs/rgi.yml`     |
| **MGEdb** | MobileElementFinder    | the `MobileElementFinder` pip package, `workflow/envs/mefinder.yml` |
| **AMRProt** | `blast_ncbi_novelty` (local BLAST) | NCBI's AMRFinderPlus reference catalog, downloaded and formatted by `workflow/scripts/build_amr_blastdb.py` |

All three refresh by the same mechanism (a rebuild), for the same reason (they are
image content). See below.

## The AMR catalog, and why BLAST is no longer remote

`blast_ncbi_novelty` used to send every RGI-called resistance enzyme to NCBI's
public remote BLAST against `nr`. That step reliably burned its full 1800s timeout
and returned **nothing** — and it did so identically whether it submitted 216
proteins or 15, because the cost is NCBI's queue, not the size of the query.
Capping the query does not help.

`nr` itself cannot be brought local: it is **733 GB** (1.12 billion sequences).
NCBI's AMRFinderPlus reference protein catalog is **~5 MB** (about 10,000 curated
resistance proteins), and it answers the question this pipeline is actually asking —
"how close is this enzyme to the nearest *known resistance protein*?" — in seconds.

So the search is now two-tier:

1. **Local AMR catalog.** The full enzyme set (221 proteins) searches in ~60s; the
   default capped set of 15 takes ~4s. In practice it resolves nearly everything.
2. **NCBI `nr`, only for what the catalog cannot name.** An enzyme matching nothing
   in the curated catalog is the genuinely interesting case, and is the only one
   where NCBI's wait buys anything. Set `blast.remote_fallback: false` in
   `config/config.yaml` to disable it and stay entirely local.

`blast_results.csv` carries a `source` column saying which database answered, because
"68% identity" means very different things against the two.

## How a database actually gets refreshed

RGI and MobileElementFinder run inside **Snakemake's own per-rule Conda
environments**, built from `workflow/envs/*.yml` and cached under
`.snakemake/conda/<hash>/`. Neither tool is installed in the `bioinformatics`
environment that runs Flask and Snakemake themselves.

That single fact determines everything else here: **the only way to move a
database version is to rebuild the per-rule environment that contains it.**
Neither `rgi.yml` nor `mefinder.yml` pins a version, so a rebuild resolves to
the current released package, and with it the current database.

The AMR catalog is not a conda package, but it lands in the same place and follows
the same rule: the Dockerfile runs `build_amr_blastdb.py` at image build time, so it
too is image content, and a rebuild re-downloads the current release. One mechanism,
three databases.

### Docker / EC2 (production)

Rebuilding the image re-solves the per-rule environments, which is what pulls
fresh CARD and MGEdb. `deploy/refresh-databases.sh` does exactly this and
restarts the service; run it from cron:

```bash
sudo crontab -e
# weekly, Sunday 03:00
0 3 * * 0 /home/ec2-user/bioinformatics/deploy/refresh-databases.sh >> /var/log/bioinformatics-db-refresh.log 2>&1
```

`.snakemake/` is not on a volume, so the per-rule environments live in the image
itself: **the image is the database.** That is why refreshing one necessarily
produces a new image, and a new image necessarily means a restart — there is no
in-place update that survives the next container start.

### What the refresh does to jobs that are running

Snakemake runs as a child of the container, so the restart at the end of a refresh
would kill any run in flight. The script does not pick a quiet-looking window and
hope. It drains first:

1. **Drain.** It touches `config/jobs/.drain` inside the container. While that flag
   exists, the app queues new runs instead of starting them — even when slots are
   free — because a run started now would only be fed to the restart.
2. **Build.** The image rebuild happens *during* the drain, so the wait is mostly
   free: the conda solve is slow, and in-flight runs spend that time finishing.
3. **Wait, or give up.** It polls `/api/health` (`pipelines.running`) until no runs
   are left. If they are still going after `DRAIN_TIMEOUT_SECONDS` (default 4h — a
   BV-BRC assembly can legitimately take hours), it **skips the refresh**, lifts the
   drain, and leaves the databases alone. A weekly database refresh is never worth
   killing a multi-hour run; skipping costs one cycle.
4. **Restart.** The new container clears the drain flag on boot and starts
   everything that queued up while it was draining.

So a job submitted during a refresh is not rejected and not lost — it waits, and
starts by itself on the new databases. A job already running is never interrupted.

The drain flag lives on the config volume, so it outlives the refresh that set it.
Boot clears it unconditionally: otherwise a refresh that died after setting the flag
would leave the app queueing runs forever with no process alive to lift it.

### Local / CLI

Delete the cached environment and let Snakemake rebuild it on the next
`--use-conda` run:

```bash
snakemake --conda-cleanup-envs
```

Or rebuild one environment directly:

```bash
conda env create --force -f workflow/envs/rgi.yml
```

## Reproducible database versions

Latest-on-rebuild is the default. If a study needs the *same* database across
rebuilds, pin the package version in the env file — that pin is the only thing
that makes a database version reproducible:

```yaml
# workflow/envs/mefinder.yml
- pip:
      - MobileElementFinder==1.0.3
```

```yaml
# workflow/envs/rgi.yml
- rgi=6.0.3
```

## A note on `workflow/scripts/daily_update_databases.py`

This script runs `pip install --upgrade rgi MobileElementFinder` against
**whatever interpreter invokes it**, and writes a timestamp to
`results/.db_update_info.json`.

Because RGI and MobileElementFinder live in Snakemake's per-rule environments
(see above), that `pip install` targets an interpreter the pipeline never calls.
It will report success while changing nothing the analysis actually uses. The
timestamp it writes is not read anywhere — the frontend does not display it.

**Do not rely on this script to keep databases current.** Use the rebuild path
above. The script is left in place because it is harmless and may still be
useful if you ever install the tools into the base environment directly.

## Technical details

- `workflow/rules/setup.smk` declares `rule setup_fresh_databases`, which the
  RGI and MobileElementFinder rules depend on. It only `touch`es the marker file
  `results/<JOB_ID>/.databases_fresh` — it performs no update and branches on no
  configuration. It exists to give those two rules a common ordering dependency.
- `config/config.yaml`'s `databases.update_packages` list is informational; no
  rule reads it.
