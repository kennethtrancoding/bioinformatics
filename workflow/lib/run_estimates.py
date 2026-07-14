"""How long a pipeline run takes, learned from the runs that already happened.

The page has to answer two questions it cannot know for certain: a queued run's
"when do I start?" and a running one's "how much longer?". Both reduce to one
estimate of a job's total runtime, so they are computed from one model here.

THE SHAPE

A run has two stages, and they do not scale the same way, so one number per sample
cannot describe both.

  The remote stage -- upload, genus, and the 40-60 minute BV-BRC assembly -- is
  spent waiting on someone else's cluster. It costs this box nothing, so a run
  waits on as many samples at once as ``bvbrc_in_flight`` allows. Twelve samples
  and twelve slots is *one* round of waiting, not twelve. Its cost is therefore
  REMOTE_SECONDS x ceil(N / bvbrc_in_flight), and for any batch that fits in the
  pool that is a constant.

  The local stage -- RGI, MobileElementFinder, BLAST, MLST, the reports -- is real
  work on this box's cores. RGI and MEF each take the whole CPU pool, so they
  serialise: two samples cost twice what one does. Its cost is linear in N, and
  past a batch of a dozen or so it is the stage that dominates the run -- at a
  hundred samples it is most of the runtime, and no number of BV-BRC slots touches
  it. Cores are the only thing that do, which is why it is measured in core-seconds
  and divided by them: it is the one term in this model a bigger box improves.

  estimate = REMOTE x ceil(N / bvbrc_in_flight) + LOCAL_CORE_SECONDS x N / cores

THE SCALE

The shape comes from the pipeline; the scale comes from this instance. Every
successful run divides what it actually took by what this model predicted it would
take, and stores the ratio. The estimate is that baseline times the median of the
recent ratios -- the median rather than the mean so that one BV-BRC outage that
stretched a run to six hours does not poison every estimate after it. A fresh
instance has no ratios and trusts the model as written (a factor of 1.0), which is
why the two constants below are anchored on the README's figures rather than on
anything convenient: one sample on the default four cores is 5400 + 3600/4 = 6300s,
which is the 1h45m the README quotes for a full sample.

N is the samples the run *has to do*, not the samples in the manifest. Re-running
is how a user recovers from a failed run, and a re-run keeps every sample that
already finished: admission clears the run markers but not the results, so
Snakemake finds those outputs on disk and skips them. Counting them would quote the
re-run a full job's runtime and then, when it came back in a fraction of that,
record the fraction as the cost of a full job -- teaching every later estimate that
the pipeline is several times faster than it is. See ``sample_is_complete``.

It is an estimate, and a crude one. One ratio cannot tell a slow BV-BRC queue apart
from a slow box, and nothing here knows whether the run had the machine to itself
(it does not, whenever MAX_CONCURRENT_PIPELINES > 1). Callers must present what
comes out of here as approximate, and must not let it drive anything but what the
user reads.
"""

import json
import math
import os
import statistics
import time

from workflow.lib import jobs


# What one sample costs at BV-BRC: the upload, the Similar Genome Finder, and the
# 40-60 minute Comprehensive Genome Analysis, plus BV-BRC's own queue. Paid once
# per round of in-flight samples, not once per sample. Does not depend on this
# box at all -- the work is not happening here.
REMOTE_SECONDS = max(0, int(os.environ.get("RUN_REMOTE_SECONDS", "5400")))

# What one sample costs on this box afterwards -- RGI, MobileElementFinder, BLAST,
# MLST, the reports -- measured in CORE-seconds, not seconds, so that it can be
# divided by the cores available. RGI and MEF each take the whole CPU pool, so this
# work serialises across samples: the local stage is CORE_SECONDS x N / cores, and
# cores is the only term in the whole model that a bigger box improves. The default
# is 3600 core-seconds, which on the default four cores is 15 minutes a sample.
LOCAL_CORE_SECONDS_PER_SAMPLE = max(
	0, int(os.environ.get("RUN_LOCAL_CORE_SECONDS_PER_SAMPLE", "3600"))
)

# Enough history to be robust to a couple of freak runs, short enough that the
# estimate still moves when the pipeline or the hardware genuinely changes.
HISTORY_LIMIT = 25

# No run of real work finishes inside a minute. A "run" that did is Snakemake
# finding every output already on disk and exiting -- a re-run of a job whose
# results were never cleaned up. It succeeded, but it did none of the work, and
# folding it in would drag every later estimate towards zero.
MINIMUM_RECORDED_SECONDS = 60

# What ``rule all`` asks of every sample (workflow/Snakefile, get_all_outputs).
# A sample with all of these on disk is one Snakemake will skip, so it is work a
# re-run does not have to do. Mirrors the Snakefile: a target added there and not
# here makes a half-finished sample look finished, which under-counts the work and
# under-estimates the run.
SAMPLE_FINAL_OUTPUTS = (
	"01_raw_qc/validation.txt",
	"02_assembly/assembly_contigs.fasta",
	"02_assembly/genome_report.json",
	"03_resistance/rgi_results.json",
	"04_blast/rgi_proteins.fasta",
	"04_blast/blast_results.csv",
	"04_blast/blast_results_full.tsv",
	"05_mlst/mlst_results.txt",
	"06_mobile_elements/me_summary.csv",
	"06_mobile_elements/{sample}_arg_mge_colocation.csv",
	"summary/report.html",
)

# The history is re-read for every job the status page touches -- each running
# job's remaining time and each queued job's wait is an estimate, and a queued
# job's wait estimates every job ahead of it too. That is O(queue) reads and JSON
# parses of the same small file on every poll of every open tab. Cached on the
# file's identity and mtime, so a write still takes effect immediately.
_cached_history_key = None
_cached_history = []


def assembly_rounds(sample_count, bvbrc_in_flight):
	"""Rounds of waiting on BV-BRC a run of ``sample_count`` samples needs.

	Zero when there is nothing to do: a re-run of a job whose samples are all
	already on disk is a Snakemake no-op, not a round of work."""
	return math.ceil(max(0, sample_count) / max(1, bvbrc_in_flight))


def baseline_seconds(sample_count, bvbrc_in_flight, cores):
	"""What the model says a run of ``sample_count`` samples costs, before this
	instance's own history is allowed to correct it."""
	return (
		REMOTE_SECONDS * assembly_rounds(sample_count, bvbrc_in_flight)
		+ LOCAL_CORE_SECONDS_PER_SAMPLE * max(0, sample_count) / max(1, cores)
	)


def sample_is_complete(results_dir, isolate_id):
	"""Whether a re-run would skip this sample, its outputs all being on disk."""
	return all(
		(results_dir / relative_path.format(sample=isolate_id)).is_file()
		for relative_path in SAMPLE_FINAL_OUTPUTS
	)


def _read_history():
	global _cached_history_key, _cached_history
	history_path = jobs.run_history_path()
	try:
		file_status = history_path.stat()
	except OSError:
		_cached_history_key, _cached_history = None, []
		return []
	# The path is part of the key because tests point PROJECT_ROOT at a fresh
	# temporary tree, and a new file there could otherwise match a stale entry.
	history_key = (str(history_path), file_status.st_mtime_ns, file_status.st_size)
	if history_key == _cached_history_key:
		return _cached_history
	try:
		history = json.loads(history_path.read_text())
	except (OSError, ValueError):
		return []
	if not isinstance(history, list):
		history = []
	_cached_history = [
		entry
		for entry in history
		if isinstance(entry, dict)
		and isinstance(entry.get("factor"), (int, float))
		and entry["factor"] > 0
	]
	_cached_history_key = history_key
	return _cached_history


def calibration():
	"""How wrong the model has been on this instance, as a multiplier. 1.0 until a
	run has finished here and said otherwise."""
	history = _read_history()
	if not history:
		return 1.0
	return float(statistics.median(entry["factor"] for entry in history))


def estimate_seconds(sample_count, bvbrc_in_flight, cores):
	"""Estimated wall-clock seconds for a run of ``sample_count`` samples."""
	return baseline_seconds(sample_count, bvbrc_in_flight, cores) * calibration()


def record(sample_count, bvbrc_in_flight, cores, seconds):
	"""Fold one finished run into the history. Only successful runs belong here:
	a crash or an abort says nothing about how long the work takes.

	``sample_count`` is what the run actually worked on. A run that skipped most of
	its manifest did not cost a full job, and charging its length to the full
	manifest would record the pipeline as far faster than it is.

	What is stored is a ratio, not a duration, so that runs of different sizes can
	be compared at all: a one-sample run and a ten-sample run are both evidence
	about the same instance, and the ratio is what they have in common."""
	baseline = baseline_seconds(sample_count, bvbrc_in_flight, cores)
	if not baseline or seconds is None or seconds < MINIMUM_RECORDED_SECONDS:
		return
	entry = {
		"finished_at": time.time(),
		"samples": sample_count,
		"bvbrc_in_flight": bvbrc_in_flight,
		"cores": cores,
		"seconds": round(float(seconds), 1),
		"baseline_seconds": baseline,
		"factor": float(seconds) / baseline,
	}
	history_path = jobs.run_history_path()
	try:
		history_path.parent.mkdir(parents=True, exist_ok=True)
		temporary_path = history_path.with_name(history_path.name + ".tmp")
		temporary_path.write_text(json.dumps((_read_history() + [entry])[-HISTORY_LIMIT:], indent=2))
		temporary_path.replace(history_path)
	except OSError as exception:
		# An estimate is a nicety; losing one must never fail a run that worked.
		print(f"[pipeline] could not record run duration: {exception}")
