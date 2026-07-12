# Database Updates

The pipeline depends on two reference databases:

| Database  | Used by                | Comes from                                    |
| --------- | ---------------------- | --------------------------------------------- |
| **CARD**  | RGI (resistance genes) | the `rgi` package, `workflow/envs/rgi.yml`     |
| **MGEdb** | MobileElementFinder    | the `MobileElementFinder` pip package, `workflow/envs/mefinder.yml` |

## How a database actually gets refreshed

Both tools run inside **Snakemake's own per-rule Conda environments**, built
from `workflow/envs/*.yml` and cached under `.snakemake/conda/<hash>/`. Neither
tool is installed in the `bioinformatics` environment that runs Flask and
Snakemake themselves.

That single fact determines everything else here: **the only way to move a
database version is to rebuild the per-rule environment that contains it.**
Neither `rgi.yml` nor `mefinder.yml` pins a version, so a rebuild resolves to
the current released package, and with it the current database.

### Docker / EC2 (production)

Rebuilding the image re-solves the per-rule environments, which is what pulls
fresh CARD and MGEdb. `deploy/refresh-databases.sh` does exactly this and
restarts the service; run it from cron:

```bash
sudo crontab -e
# weekly, Sunday 03:00 -- pick a window when the job queue is empty, since the
# restart drops any run currently in flight
0 3 * * 0 /home/ec2-user/bioinformatics/deploy/refresh-databases.sh >> /var/log/bioinformatics-db-refresh.log 2>&1
```

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
