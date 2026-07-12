# Database Update Strategies

This pipeline keeps RGI (CARD) and MobileElementFinder (MGEdb) databases fresh using one of two strategies, configurable in `config/config.yaml`:

## Strategy 1: `fresh_on_every_run` (Always Latest)

**Config:**
```yaml
databases:
    update_strategy: "fresh_on_every_run"
```

**Behavior:**
- Reinstalls RGI and MobileElementFinder from PyPI before the first sample runs
- Guarantees latest databases every pipeline invocation
- Runs `pip install --upgrade --force-reinstall` for both packages

**Pros:**
- ✅ Always uses the newest CARD and MGEdb databases
- ✅ No separate scheduling needed

**Cons:**
- ❌ Slower (pip reinstall overhead ~30-60 seconds per run)
- ❌ Requires network access to PyPI on every run
- ❌ May fail if PyPI is unreachable

**Usage:**
```bash
snakemake --use-conda  # Will update databases before processing samples
```

---

## Strategy 2: `daily_check` (Efficient Caching) — **Default**

**Config:**
```yaml
databases:
    update_strategy: "daily_check"
```

**Behavior:**
- Checks for database updates once per day via an external scheduled script
- Pipeline runs use whatever databases are currently installed (fast)
- Displays last-update timestamp on frontend for visibility

**Pros:**
- ✅ Fast pipeline runs (no reinstall overhead)
- ✅ Databases are reasonably fresh (~24 hour max lag)
- ✅ Better for production workflows with many samples

**Cons:**
- ❌ Databases may lag by up to 24 hours
- ❌ Requires setting up a scheduled job

**Setup Instructions:**

### Option A: System Cron
Add to crontab (runs daily at 2 AM):
```bash
crontab -e
# Add this line:
0 2 * * * cd /Users/KennethTran/Desktop/bioinformatics && python3 workflow/scripts/daily_update_databases.py >> logs/daily_update.log 2>&1
```

### Option B: Claude Code Schedule (Recommended)
Run from your Claude Code session:
```bash
/schedule "daily_update_databases.py" --cron "0 2 * * *"
```

This creates a managed cloud agent that runs daily at 2 AM UTC.

### Option C: Manual
Run anytime to update databases:
```bash
python3 workflow/scripts/daily_update_databases.py
```

**Checking Last Update:**
The frontend displays the last update timestamp from `results/.db_update_info.json`:
```json
{
  "last_updated_utc": "2026-02-15T02:15:30.123456",
  "strategy": "daily_check",
  "packages": ["rgi", "MobileElementFinder"]
}
```

---

## Switching Strategies

To switch strategies, edit `config/config.yaml`:
```yaml
databases:
    update_strategy: "daily_check"  # or "fresh_on_every_run"
```

Changes take effect on the next pipeline run.

---

## Technical Details

- **Setup Rule:** `workflow/rules/setup.smk`
  - Reads `config.databases.update_strategy`
  - `fresh_on_every_run`: Runs `pip install --upgrade --force-reinstall`
  - `daily_check`: Creates flag immediately (actual updates via external script)

- **Database Dependencies:**
  - RGI rule (`card_rgi_analysis`) depends on `results/.databases_fresh`
  - Mobile elements rule (`mobile_element_finder`) depends on `results/.databases_fresh`
  - Both rules wait for flag before running

- **Update Script:** `workflow/scripts/daily_update_databases.py`
  - Runs independently of the pipeline
  - Records timestamp to `results/.db_update_info.json`
  - Used by frontend to display "DBs last updated: ..."
