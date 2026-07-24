"""How long a pipeline run takes, learned from the runs that already happened.

The page has to answer two questions it cannot know for certain: a queued run's
"when do I start?" and a running one's "how much longer?". Both reduce to one
estimate of a job's total runtime, so they are computed from one model here.

THE SHAPE

A run has two stages, and they do not scale the same way, so one number per sample
cannot describe both.

  The remote stage -- genus, and the 40-60 minute BV-BRC assembly -- is spent
  waiting on someone else's cluster. It costs this box nothing, so a run waits on
  as many samples at once as ``bvbrc_in_flight`` allows. Twelve samples and twelve
  slots is *one* round of waiting, not twelve. Its cost is REMOTE_SECONDS x
  ceil(N / bvbrc_in_flight), and for any batch that fits in the pool it is a
  constant.

  The local stage -- RGI, MobileElementFinder, BLAST, MLST, the reports -- is real
  work on this box's cores, measured in core-seconds and divided by them, because
  cores are the one term here a bigger box improves.

AND THEY OVERLAP

The two stages are charged to different pools (cpu and bvbrc, see
workflow/helpers/pipeline_manager.py), so they do not queue behind each other: while round k+1
is assembling at BV-BRC, round k's samples are already going through RGI here. The
model used to *add* the stages, which quoted every run as if the box sat idle
through every assembly and BV-BRC sat idle through every RGI. On a 48-sample batch
that was the difference between a quoted 30 hours and a real 5.

So what a run actually costs is whichever of these binds:

  remote-bound   REMOTE x rounds + (the last round's local work, which has no
                 assembly left to hide behind)
  local-bound    REMOTE (the first round; nothing local can start before it lands)
                 + all the local work, because the cores never catch up

  estimate = max(of those two)

A small batch on a decent box is remote-bound and the local stage is nearly free.
Enough samples on few enough cores and the box becomes the bottleneck instead. The
max() picks whichever is true rather than assuming one of them.

THE SCALE

The shape comes from the pipeline; the scale comes from this instance. Every
successful run divides what it actually took by what this model predicted it would
take, and stores the ratio. The estimate is that baseline times a correction learned
from those ratios. A fresh instance has no ratios and trusts the model as written (a
factor of 1.0).

The correction is learned in two stages, because a few runs and forty runs support
very different claims:

  thin history    the median of the recent ratios -- the median rather than the mean
                  so that one BV-BRC outage that stretched a run to six hours does
                  not poison every estimate after it. One number for every run,
                  whatever its shape.
  enough history  a small network (workflow/helpers/run_estimate_net.py) that predicts the ratio
                  from the run's shape, so that a 1-sample cold run and a 40-sample
                  re-run can be wrong in different directions rather than sharing one
                  average. It trains on the same ratios, its output is clamped, and
                  below run_estimate_net.MIN_TRAIN_RUNS it declines to answer at all
                  and this falls back to the median. See ``correction_for``.

What is *not* learned is the shape. ceil(N / in_flight) is what a pool of twelve
slots does, and dividing core-seconds by cores is what cores are for; a network made
to rediscover that from a few dozen runs would learn it worse than it is already
known, and would have nothing to say about a run size it had never seen. The
arithmetic scales; the network only says how wrong the arithmetic tends to be here.

The two constants below are measured, from a real 48-sample run, and the numbers
that produced them are recorded next to each. They were guesses once -- anchored on
the README rather than on anything the pipeline had been observed to do -- and the
local one was six times too big, which is how a five-hour batch came to be quoted at
thirty. Re-measure them; do not re-guess them. The network corrects a scale error; it
does not excuse one, and a constant left wrong is a constant the network has to spend
its small capacity apologising for.

THE WORK IS TWO NUMBERS, NOT ONE

What a run has to do is the samples it has to do -- not the manifest. Re-running is
how a user recovers from a failed run, and a re-run keeps everything already on
disk: admission clears the run markers but not the results, so Snakemake skips what
it finds. But "skipped" is not all-or-nothing, because the two stages are skipped
independently, and the usual re-run is a job that got its assemblies and then died
in the local analysis. Those samples are nowhere near complete and owe BV-BRC
nothing.

So the work is counted as (samples, assemblies), and both matter. Charge a re-run
for assemblies it already has and it reads hours too long -- and then it finishes in
a fraction of that, and ``record`` writes the fraction down as the cost of a cold
run, teaching every later estimate that the pipeline is several times faster than it
is. See ``sample_is_complete`` and ``sample_needs_assembly``.

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

from workflow.helpers import jobs, run_estimate_net

# What one sample costs at BV-BRC: the Similar Genome Finder, then the
# Comprehensive Genome Analysis, plus BV-BRC's own queue. Paid once per round of
# in-flight samples, not once per sample. Does not depend on this box at all --
# the work is not happening here.
#
# 4000s, from a measured 48-sample run: genus 852s median, and a cold assembly
# 40-66 minutes. (The README's 90-minute figure was for both stages plus slack.)
REMOTE_SECONDS = max(0, int(os.environ.get("RUN_REMOTE_SECONDS", "4000")))

# What one sample costs on this box afterwards -- RGI, MobileElementFinder, BLAST,
# MLST, the reports -- measured in CORE-seconds, not seconds, so that it can be
# divided by the cores available.
#
# 600 core-seconds, and that number is measured, not guessed: on the 48-sample run
# RGI was 112s at 4 threads (448 core-s), MobileElementFinder 19s at 4 (76), MLST
# 30s at 1, BLAST 2s at 4 (8) -- the local AMR catalog answers nearly everything
# and the NCBI tier almost never fires -- and every other rule under a second.
#
# The old default was 3600, six times this, and it was the whole reason a 48-sample
# batch was quoted at 30 hours when it really took about five. If you change the
# pipeline's local work, re-measure this from a real run rather than estimating it.
LOCAL_CORE_SECONDS_PER_SAMPLE = max(
	0, int(os.environ.get("RUN_LOCAL_CORE_SECONDS_PER_SAMPLE", "600"))
)

# Bumped whenever the shape of the model or its constants change. A stored factor
# is the ratio of a real run to what *some* model predicted, so it is only meaning-
# ful against the model that produced it: carrying the old ones across a
# recalibration would correct the new model by how wrong the old one was, and a
# 6x change in a constant would land as a 6x error in the other direction.
# Entries from another version are ignored rather than deleted -- they are still a
# record of what the box did.
MODEL_VERSION = 2

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
	"{sample}/01_raw_qc/validation.txt",
	"{sample}/02_assembly/assembly_contigs.fasta",
	"{sample}/02_assembly/genome_report.json",
	"{sample}/03_resistance/rgi_results.json",
	"{sample}/04_blast/rgi_proteins.fasta",
	"{sample}/04_blast/blast_results.csv",
	"{sample}/04_blast/blast_results_full.tsv",
	"{sample}/05_mlst/mlst_results.txt",
	"{sample}/06_mobile_elements/me_summary.csv",
	"{sample}/06_mobile_elements/{sample}_arg_mge_colocation.csv",
	"{sample}/summary/report.html",
)

# What a sample has on disk once BV-BRC is done with it. This is the expensive half
# and it has to be counted separately, because "pending" is not one thing.
#
# A re-run of a crashed job is the normal case, not the exception, and it is usually
# holding samples that are assembled but not yet analysed. Judged by
# SAMPLE_FINAL_OUTPUTS alone such a sample looks entirely undone -- so the estimate
# charges it another hour of BV-BRC that it does not need, and then the run comes
# back in a fraction of the quoted time and ``record`` writes that fraction down as
# what a full run costs. One resumed run taught the old model that the pipeline was
# 7.6x faster than it is. Counting the two stages separately is what stops that.
SAMPLE_ASSEMBLY_OUTPUTS = (
	"{sample}/02_assembly/assembly_contigs.fasta",
	"{sample}/02_assembly/genome_report.json",
)

# The history is re-read for every job the status page touches -- each running
# job's remaining time and each queued job's wait is an estimate, and a queued
# job's wait estimates every job ahead of it too. That is O(queue) reads and JSON
# parses of the same small file on every poll of every open tab. Cached on the
# file's identity and mtime, so a write still takes effect immediately.
_cached_history_key = None
_cached_history = []

# The trained network, cached on the same key for the same reason -- see
# ``_correction_model``. Held separately from the history because a fit can fail
# (or be declined, while the history is thin) without the history being unusable.
_cached_net_key = object()
_cached_net = None


def assembly_rounds(sample_count, bvbrc_in_flight):
	"""Rounds of waiting on BV-BRC a run of ``sample_count`` samples needs.

	Zero when there is nothing to do: a re-run of a job whose samples are all
	already on disk is a Snakemake no-op, not a round of work."""
	return math.ceil(max(0, sample_count) / max(1, bvbrc_in_flight))


def baseline_seconds(sample_count, bvbrc_in_flight, cores, assembly_count=None):
	"""What the model says a run costs, before this instance's own history corrects it.

	``sample_count`` is the samples with local work left to do; ``assembly_count`` is
	the subset of those that also still owe BV-BRC an assembly. They differ on every
	re-run of a crashed job, which is precisely when an estimate is being asked for,
	so they are counted separately -- see SAMPLE_ASSEMBLY_OUTPUTS. When it is not
	given, assume the worst: every sample needs everything (a cold run).

	The stages overlap (see THE SHAPE above), so this is the larger of the two ways a
	run can be bound, not the sum of them."""
	sample_count = max(0, sample_count)
	if assembly_count is None:
		assembly_count = sample_count
	assembly_count = min(max(0, assembly_count), sample_count)

	if not sample_count:
		# A re-run whose every output is already on disk is a Snakemake no-op, not a
		# round of waiting.
		return 0.0

	cores = max(1, cores)
	local_per_sample = LOCAL_CORE_SECONDS_PER_SAMPLE / cores

	if not assembly_count:
		# Everything is assembled already: no BV-BRC stage to hide the local work
		# behind, so the run is exactly its local work.
		return local_per_sample * sample_count

	in_flight = max(1, bvbrc_in_flight)
	rounds = assembly_rounds(assembly_count, in_flight)
	# The last round is whatever did not fill a whole one. Its samples are the only
	# ones whose local work has no assembly still running to hide behind.
	samples_in_last_round = assembly_count - (rounds - 1) * in_flight

	remote_bound = REMOTE_SECONDS * rounds + local_per_sample * samples_in_last_round
	local_bound = REMOTE_SECONDS + local_per_sample * sample_count
	return max(remote_bound, local_bound)


def sample_is_complete(results_dir, isolate_id):
	"""Whether a re-run would skip this sample, its outputs all being on disk."""
	return all(
		(results_dir / relative_path.format(sample=isolate_id)).is_file()
		for relative_path in SAMPLE_FINAL_OUTPUTS
	)


def sample_needs_assembly(results_dir, isolate_id):
	"""Whether this sample still owes a trip to BV-BRC -- the expensive half.

	A sample that has its assembly costs a re-run no remote time at all, however
	much local work it still has left."""
	return not all(
		(results_dir / relative_path.format(sample=isolate_id)).is_file()
		for relative_path in SAMPLE_ASSEMBLY_OUTPUTS
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
		# A factor only means anything against the model that produced it.
		and entry.get("model") == MODEL_VERSION
	]
	_cached_history_key = history_key
	return _cached_history


def calibration():
	"""How wrong the model has been on this instance, as a multiplier. 1.0 until a
	run has finished here and said otherwise.

	The flat answer: one number for every run, whatever its shape. It is what the
	estimate uses until there is enough history to fit the network that replaces
	it (see ``correction_for`` and workflow/helpers/run_estimate_net.py), and what it falls
	back to if that fit is ever unavailable."""
	history = _read_history()
	if not history:
		return 1.0
	return float(statistics.median(entry["factor"] for entry in history))


def _rounds_for_entry(entry):
	"""A history entry's rounds of BV-BRC waiting, by the same arithmetic a live
	estimate uses. Passed to the network so it cannot form its own idea of a round."""
	return assembly_rounds(
		entry.get("assemblies", entry.get("samples", 0)),
		entry.get("bvbrc_in_flight", 1),
	)


def _correction_model():
	"""The trained network for the current history, or None while the history is
	too thin to fit one.

	Cached on the same key as the history itself: a status-page poll estimates
	every running job and every job ahead of every queued one, and each of those
	is a call through here. Training is milliseconds, but it is not free, and
	doing it per job per poll per open tab would be. A write to the history moves
	the key, so a finished run retrains on the next poll rather than at some later
	time of the cache's choosing."""
	global _cached_net_key, _cached_net
	history = _read_history()
	if _cached_net_key == _cached_history_key:
		return _cached_net
	try:
		model = run_estimate_net.train_from_history(history, _rounds_for_entry)
	except Exception as exception:
		# An estimate is a nicety. A network that will not fit must never be able
		# to take the status page down with it -- fall back to the median.
		print(f"[pipeline] could not fit run-estimate network: {exception}")
		model = None
	_cached_net, _cached_net_key = model, _cached_history_key
	return model


def correction_for(sample_count, bvbrc_in_flight, cores, assembly_count):
	"""The multiplier this instance's history puts on the arithmetic model, for a
	run of this particular shape.

	The network when there is enough history to have fitted one, the flat median
	otherwise. Unlike the median, this can answer differently for a 1-sample cold
	run and a 40-sample re-run, which are not equally wrong in the same direction."""
	model = _correction_model()
	if model is None:
		return calibration()
	return model.correction(
		sample_count,
		assembly_count,
		bvbrc_in_flight,
		cores,
		assembly_rounds(assembly_count, bvbrc_in_flight),
	)


def estimate_seconds(sample_count, bvbrc_in_flight, cores, assembly_count=None):
	"""Estimated wall-clock seconds for a run of ``sample_count`` samples."""
	baseline = baseline_seconds(sample_count, bvbrc_in_flight, cores, assembly_count)
	if not baseline:
		return 0.0
	if assembly_count is None:
		assembly_count = sample_count
	assembly_count = min(max(0, assembly_count), max(0, sample_count))
	return baseline * correction_for(sample_count, bvbrc_in_flight, cores, assembly_count)


def record(sample_count, bvbrc_in_flight, cores, seconds, assembly_count=None, queue_seconds=None):
	"""Fold one finished run into the history. Only successful runs belong here:
	a crash or an abort says nothing about how long the work takes.

	``sample_count`` is what the run actually worked on, and ``assembly_count`` how
	much of that it had to fetch from BV-BRC. Both, because a run that skipped most
	of its manifest did not cost a full job, and neither did one that found half its
	assemblies already on disk -- charge either against a full cold run and the
	history learns that the pipeline is several times faster than it is.

	``seconds`` is the runtime -- the process wall clock -- and ``queue_seconds`` the
	wait for a slot before it. Two separate numbers because they answer to two
	different things: the runtime is what the pipeline cost, and the wait is what the
	instance's own contention cost, which depends on how many other jobs were ahead
	and nothing about this one's work. So the wait is recorded but kept out of the
	``factor`` below -- fold it in and the same job would look several times more
	expensive on a busy box than an idle one, which is the one thing the factor must
	not learn. ``queue_seconds`` is None when it could not be measured (see
	pipeline_manager._record_duration), and stored as such rather than as a false zero.

	What is stored for the estimate is a ratio, not a duration, so that runs of
	different sizes can be compared at all: a one-sample run and a ten-sample run are
	both evidence about the same instance, and the ratio is what they have in common."""
	baseline = baseline_seconds(sample_count, bvbrc_in_flight, cores, assembly_count)
	if not baseline or seconds is None or seconds < MINIMUM_RECORDED_SECONDS:
		return
	entry = {
		"finished_at": time.time(),
		"model": MODEL_VERSION,
		"samples": sample_count,
		"assemblies": sample_count if assembly_count is None else assembly_count,
		"bvbrc_in_flight": bvbrc_in_flight,
		"cores": cores,
		"seconds": round(float(seconds), 1),
		"queue_seconds": None if queue_seconds is None else round(float(queue_seconds), 1),
		"baseline_seconds": baseline,
		"factor": float(seconds) / baseline,
	}
	history_path = jobs.run_history_path()
	try:
		history_path.parent.mkdir(parents=True, exist_ok=True)
		temporary_path = history_path.with_name(history_path.name + ".tmp")
		temporary_path.write_text(
			json.dumps((_read_history() + [entry])[-HISTORY_LIMIT:], indent=2)
		)
		temporary_path.replace(history_path)
	except OSError as exception:
		# An estimate is a nicety; losing one must never fail a run that worked.
		print(f"[pipeline] could not record run duration: {exception}")
