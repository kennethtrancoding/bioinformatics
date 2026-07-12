"""Redirect the app's writes into a throwaway directory for the duration of a
test run.

Import this before anything else in a test module. Without it, importing
frontend binds DATA_ROOT/RESULTS_ROOT to the real project, so a test upload
would land in the developer's data/raw_fastq and the retention sweep would
delete their actual results.

Every path helper reads its module-level PROJECT_ROOT at call time, so
rebinding those globals is enough to move all writes under a temp root. The
real tree stays readable (templates, static, workflow scripts) but is never
written to.
"""

import atexit
import shutil
import sys
import tempfile
from pathlib import Path

REAL_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REAL_ROOT))

TMP_ROOT = Path(tempfile.mkdtemp(prefix="pipeline_tests_"))
atexit.register(shutil.rmtree, TMP_ROOT, ignore_errors=True)

for subdirectory in ("data/raw_fastq", "results", "logs", "config/jobs"):
    (TMP_ROOT / subdirectory).mkdir(parents=True, exist_ok=True)

import frontend  # noqa: E402
from workflow.lib import import_samples, jobs  # noqa: E402

# jobs.* helpers build every job path from this.
jobs.PROJECT_ROOT = TMP_ROOT

# frontend keeps its own copies for upload staging and the retention sweep.
frontend.PROJECT_ROOT = TMP_ROOT
frontend.DATA_ROOT = TMP_ROOT / "data" / "raw_fastq"
frontend.RESULTS_ROOT = TMP_ROOT / "results"

# import_samples writes manifest paths relative to its own PROJECT_ROOT; without
# this the imported FASTQs would be registered as ../../var/folders/... escapes.
import_samples.PROJECT_ROOT = TMP_ROOT
import_samples.DEFAULT_SAMPLES_CSV = TMP_ROOT / "config" / "samples.csv"
import_samples.DEFAULT_DATA_DIR = TMP_ROOT / "data" / "raw_fastq"
