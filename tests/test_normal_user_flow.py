"""A normal user's whole day: fill jobs by every input method, run more of them than
there are slots, and have the weekly database refresh land in the middle.

Everything here goes through the real HTTP endpoints (/submit, /import,
/cloud-import, /run, /abort, /status, /api/health) against real uploaded bytes. Two
things are substituted, and only two:

  * Snakemake, by a process this test can hold open and release on demand. Slot
    contention is about process lifetime, not about genomics, and a real run is ~96
    minutes -- filling two slots plus a queue plus a restart would be a multi-hour
    test that could never run in CI.
  * Google Drive, by cloud_import's own fake session (tests/test_cloud_import.py's
    FakeDrive), which serves real FASTQ bytes over the real _google_list /
    _google_walk / _download code path. A live Drive folder needs an API key and
    Google's uptime, neither of which belongs in a repeatable test.

The scenario the drain has to survive is the whole point: a refresh restarts the
service, Snakemake is a child of it, and a run in flight dies with it. So the refresh
sets a drain flag, waits for the running jobs to finish, and restarts onto an idle
app -- while everything submitted meanwhile queues to disk instead of starting, and
resumes by itself on the other side.
"""

import json
import sys
import time
import unittest
from unittest import mock

import frontend  # noqa: E402
from tests._isolation import REAL_ROOT, TMP_ROOT  # noqa: F401  (must import first)
from tests.test_batching import _REAL_POPEN, Base, token_for  # noqa: E402
from tests.test_cloud_import import (  # noqa: E402
	SHARED_FOLDER,
	FakeDrive,
	fastq_bytes,
	md5,
	stats_workbook,
)
from workflow.helpers import cloud_import, jobs, run_estimate_net, run_estimates  # noqa: E402

RELEASE_DIR = TMP_ROOT / "release"
RELEASE_DIR.mkdir(parents=True, exist_ok=True)

# Blocks until its release file appears, so a test decides exactly when a run ends.
_HOLD_SCRIPT = (
	"import os, sys, time\n"
	"path = sys.argv[1]\n"
	"while not os.path.exists(path):\n"
	"    time.sleep(0.02)\n"
)


def _held_popen(argv, **kwargs):
	"""Stand in for snakemake: a real process, held open until its job is released.

	Reads the job id out of the argv the app built, so each run gets its own release
	file and runs can be finished in any order -- which is what lets this test free
	one slot while another run is still going."""
	job_id = next(argument.split("=", 1)[1] for argument in argv if argument.startswith("job_id="))
	return _REAL_POPEN([sys.executable, "-c", _HOLD_SCRIPT, str(RELEASE_DIR / job_id)], **kwargs)


def _wait_until(predicate, timeout=10, what="condition"):
	deadline = time.time() + timeout
	while time.time() < deadline:
		if predicate():
			return True
		time.sleep(0.02)
	raise AssertionError(f"timed out waiting for {what}")


class NormalUserFlow(Base):
	def setUp(self):
		super().setUp()
		jobs.drain_flag_path().unlink(missing_ok=True)
		jobs.pipeline_queue_path().unlink(missing_ok=True)
		frontend.subprocess.Popen = _held_popen
		# Two slots, as in production (MAX_CONCURRENT_PIPELINES defaults to 2).
		self._real_max = frontend.MAX_CONCURRENT_PIPELINES
		frontend.MAX_CONCURRENT_PIPELINES = 2
		self.addCleanup(self._teardown)

	def _teardown(self):
		for job_id in list(frontend._pipeline_processes):
			self.release(job_id)
		for pipeline_process in list(frontend._pipeline_processes.values()):
			pipeline_process.kill()
		frontend._pipeline_processes.clear()
		frontend._pipeline_queue.clear()
		frontend.MAX_CONCURRENT_PIPELINES = self._real_max
		frontend.subprocess.Popen = _REAL_POPEN
		jobs.drain_flag_path().unlink(missing_ok=True)

	# User actions
	def release(self, job_id):
		"""Let a held run finish, the way a real run finishing frees its slot."""
		(RELEASE_DIR / job_id).touch()

	def cloud_import(self, sample_name, job_id=None):
		"""Pull a pair from a Google Drive share link, over the real code path."""
		r1, r2 = fastq_bytes(), fastq_bytes(2)
		drive = FakeDrive(
			{
				"FOLDER1": {"name": "Run", "mimeType": cloud_import._GOOGLE_FOLDER_MIME},
				"f1": {
					"name": f"{sample_name}_R1_001.fastq.gz",
					"mimeType": "application/gzip",
					"parent": "FOLDER1",
					"body": r1,
				},
				"f2": {
					"name": f"{sample_name}_R2_001.fastq.gz",
					"mimeType": "application/gzip",
					"parent": "FOLDER1",
					"body": r2,
				},
				"x1": {
					"name": "DNA Sequencing Stats.xlsx",
					"mimeType": "application/vnd.ms-excel",
					"parent": "FOLDER1",
					"body": stats_workbook([[sample_name, md5(r1), md5(r2)]]),
				},
			}
		)
		payload = {"share_url": SHARED_FOLDER}
		if job_id:
			payload["job_id"] = job_id
		with (
			mock.patch.object(cloud_import, "_SESSION", drive),
			mock.patch.object(cloud_import, "GOOGLE_API_KEY", "test-key"),
		):
			response = self.client.post("/cloud-import", data=payload)
			self.assertEqual(response.status_code, 202, response.get_data(as_text=True))
			imported_job_id = response.get_json()["job_id"]
			_wait_until(
				lambda: self.client.get(f"/cloud-import/status?job_id={imported_job_id}")
				.get_json()
				.get("state")
				!= "running",
				timeout=30,
				what="cloud import to finish",
			)
		record = self.client.get(f"/cloud-import/status?job_id={imported_job_id}").get_json()
		self.assertEqual(record["state"], "done", record)
		return imported_job_id

	def runnable(self, job_id):
		token_for(job_id)
		return job_id

	def start(self, job_id):
		return self.client.post("/run", data={"job_id": job_id})

	def seed_run_history(self, seconds_per_run):
		"""Pin what a one-sample run "costs", so the estimates a test reads are its
		own and not whatever the runs in every other test happened to take. None
		leaves this instance with no history at all, as a freshly deployed one has."""
		jobs.run_history_path().unlink(missing_ok=True)
		self.addCleanup(jobs.run_history_path().unlink, missing_ok=True)
		if seconds_per_run is not None:
			run_estimates.record(
				1, frontend.BVBRC_MAX_IN_FLIGHT, frontend.PIPELINE_CORES, seconds_per_run
			)

	def complete_sample_on_disk(self, job_id, isolate_id):
		"""Leave behind the outputs a finished sample leaves, which are exactly the
		outputs Snakemake looks for before deciding it can skip one."""
		results_dir = jobs.job_results_dir(job_id)
		for relative_path in run_estimates.SAMPLE_FINAL_OUTPUTS:
			output_path = results_dir / relative_path.format(sample=isolate_id)
			output_path.parent.mkdir(parents=True, exist_ok=True)
			output_path.write_text("")

	def health(self):
		return self.client.get("/api/health").get_json()["pipelines"]

	# Tests
	@unittest.skip("Cloud import is disabled; this test needs the /cloud-import route.")
	def test_every_input_method_produces_a_runnable_job(self):
		"""Each of the three doors into the app, on its own."""
		by_pair = self.runnable(self.submit_pair("SOLO_PAIR").get_json()["job_id"])
		by_folder = self.runnable(
			self.import_folder(["SOLO_FOLD_S1", "SOLO_FOLD_S2"]).get_json()["job_id"]
		)
		by_cloud = self.runnable(self.cloud_import("SOLO_CLOUD_S3"))

		self.assertEqual(self.isolates(by_pair), ["SOLO_PAIR"])
		self.assertEqual(self.isolates(by_folder), ["SOLO_FOLD_S1", "SOLO_FOLD_S2"])
		self.assertEqual(self.isolates(by_cloud), ["SOLO_CLOUD_S3"])

		# One at a time, so each starts into a free slot rather than queueing behind
		# the previous one -- releasing a run does not free its slot instantly, the
		# process still has to exit and the watcher still has to reap it.
		for job_id in (by_pair, by_folder, by_cloud):
			self.assertEqual(self.start(job_id).status_code, 200)
			self.release(job_id)
			_wait_until(
				lambda job_id=job_id: job_id not in frontend._pipeline_processes,
				what=f"{job_id} to release its slot",
			)

	@unittest.skip("Cloud import is disabled; this test needs the /cloud-import route.")
	def test_all_three_methods_fill_one_job_and_it_runs(self):
		"""The combination: one batch assembled from a folder import, a stray pair and
		a cloud pull, then run as a single job."""
		job_id = self.import_folder(["MIX_FOLD_S1"]).get_json()["job_id"]
		self.submit_pair("MIX_PAIR", job_id=job_id)
		self.cloud_import("MIX_CLOUD_S2", job_id=job_id)
		self.runnable(job_id)

		self.assertEqual(self.isolates(job_id), ["MIX_CLOUD_S2", "MIX_FOLD_S1", "MIX_PAIR"])
		# Every upload is recorded, with the method that brought it in.
		methods = sorted(entry["method"] for entry in frontend._read_uploads(job_id))
		self.assertEqual(methods, ["cloud", "folder", "pair"])

		self.assertEqual(self.start(job_id).status_code, 200)
		self.assertIn(job_id, frontend._pipeline_processes)
		self.release(job_id)

	def test_slots_fill_then_further_runs_queue_in_order(self):
		jobs_by_letter = {
			letter: self.runnable(self.submit_pair(f"LOAD_{letter}").get_json()["job_id"])
			for letter in "ABCD"
		}

		# Two slots, so A and B start...
		self.assertEqual(self.start(jobs_by_letter["A"]).status_code, 200)
		self.assertEqual(self.start(jobs_by_letter["B"]).status_code, 200)
		# ...and C and D queue, FIFO, reporting their place in line.
		for expected_position, letter in ((1, "C"), (2, "D")):
			response = self.start(jobs_by_letter[letter])
			self.assertEqual(response.status_code, 202)
			payload = response.get_json()
			self.assertTrue(payload["queued"])
			self.assertEqual(payload["queue_position"], expected_position)

		self.assertEqual(self.health(), {"draining": False, "running": 2, "queued": 2})
		# Starting a job that is already in flight is refused, running or queued.
		self.assertEqual(self.start(jobs_by_letter["A"]).status_code, 409)
		self.assertEqual(self.start(jobs_by_letter["C"]).status_code, 409)

		# A finishes -> its slot goes to C, the head of the queue, not to D.
		self.release(jobs_by_letter["A"])
		_wait_until(
			lambda: jobs_by_letter["C"] in frontend._pipeline_processes,
			what="C to be promoted into A's slot",
		)
		self.assertEqual(list(frontend._pipeline_queue), [jobs_by_letter["D"]])
		self.assertEqual(self.health()["running"], 2)

	def test_a_queued_run_can_be_cancelled_before_it_ever_starts(self):
		first = self.runnable(self.submit_pair("CANCEL_A").get_json()["job_id"])
		second = self.runnable(self.submit_pair("CANCEL_B").get_json()["job_id"])
		waiting = self.runnable(self.submit_pair("CANCEL_C").get_json()["job_id"])
		self.start(first)
		self.start(second)
		self.assertEqual(self.start(waiting).status_code, 202)

		self.assertEqual(self.client.post("/abort", data={"job_id": waiting}).status_code, 200)

		self.assertNotIn(waiting, frontend._pipeline_queue)
		status = self.client.get(f"/status?job_id={waiting}").get_json()
		self.assertTrue(status["done"])
		self.assertFalse(status["success"])
		self.assertIn("aborted", status["error"].lower())

	def test_a_waiting_run_is_told_when_it_starts_and_a_going_one_how_long_it_has(self):
		"""A run behind two others is a run someone is waiting on. Both numbers the
		page shows come from the server: when the queue lets this job start, and how
		long the job itself should take once it does."""
		self.seed_run_history(seconds_per_run=600)

		running_first = self.runnable(self.submit_pair("ETA_A").get_json()["job_id"])
		running_second = self.runnable(self.submit_pair("ETA_B").get_json()["job_id"])
		queued_first = self.runnable(self.submit_pair("ETA_C").get_json()["job_id"])
		queued_second = self.runnable(self.submit_pair("ETA_D").get_json()["job_id"])
		for job_id in (running_first, running_second, queued_first, queued_second):
			self.start(job_id)

		# A run that is going reports its estimated total; the page subtracts the
		# elapsed time it already has to count down what is left.
		going = self.client.get(f"/status?job_id={running_first}").get_json()
		self.assertFalse(going["queued"])
		self.assertAlmostEqual(going["estimated_seconds"], 600)
		self.assertIsNotNone(going["started_at"])

		# A run that is waiting reports the wait, which cannot exceed the runs it is
		# waiting on, and is longer the further back in the queue it sits.
		waiting = self.client.get(f"/status?job_id={queued_first}").get_json()
		behind_it = self.client.get(f"/status?job_id={queued_second}").get_json()
		self.assertTrue(waiting["queued"])
		self.assertAlmostEqual(waiting["estimated_seconds"], 600)
		self.assertGreater(waiting["queue_wait_seconds"], 0)
		self.assertLessEqual(waiting["queue_wait_seconds"], 600)
		self.assertGreaterEqual(behind_it["queue_wait_seconds"], waiting["queue_wait_seconds"])

		# Nothing starts during a database refresh, so there is no honest number to
		# give -- and the app says nothing rather than promising a start time.
		jobs.drain_flag_path().write_text("")
		self.assertIsNone(
			self.client.get(f"/status?job_id={queued_first}").get_json()["queue_wait_seconds"]
		)

	def test_the_estimate_is_learned_only_from_runs_that_really_ran(self):
		"""The first run on a new instance is estimated from the README's figures;
		after that, from what this instance's own hardware and BV-BRC queue actually
		did. Three kinds of run teach it nothing, and each would teach it a lie: one
		that was aborted, one that crashed, and one that "succeeded" in two seconds
		because Snakemake found every output already on disk."""
		self.seed_run_history(seconds_per_run=None)  # a fresh instance, no history
		# Nothing has contradicted the model yet, so it is trusted as written.
		self.assertEqual(run_estimates.calibration(), 1.0)

		# The runs in this test finish in milliseconds, which is exactly the case the
		# floor throws away -- so the run that is meant to teach it something is
		# recorded by hand, at a length a real one has.
		aborted = self.runnable(self.submit_pair("LEARN_ABORTED").get_json()["job_id"])
		self.start(aborted)
		self.client.post("/abort", data={"job_id": aborted})
		instant = self.runnable(self.submit_pair("LEARN_INSTANT").get_json()["job_id"])
		self.start(instant)
		self.release(instant)
		for job_id in (aborted, instant):
			_wait_until(
				lambda job_id=job_id: job_id not in frontend._pipeline_processes,
				what=f"{job_id} to be reaped",
			)
		self.assertTrue(self.client.get(f"/status?job_id={instant}").get_json()["success"])
		self.assertEqual(run_estimates.calibration(), 1.0)

		# A run of real length is what moves the estimate: this instance took 3000s
		# over a one-sample run the model priced at 6300s, so it is running at a bit
		# under half the model's pace, and every later job is quoted at that pace.
		self.seed_run_history(seconds_per_run=3000)
		one_sample_baseline = run_estimates.baseline_seconds(
			1, frontend.BVBRC_MAX_IN_FLIGHT, frontend.PIPELINE_CORES
		)
		self.assertAlmostEqual(run_estimates.calibration(), 3000 / one_sample_baseline)
		next_job = self.runnable(self.submit_pair("LEARN_NEXT").get_json()["job_id"])
		self.start(next_job)
		self.assertAlmostEqual(
			self.client.get(f"/status?job_id={next_job}").get_json()["estimated_seconds"], 3000
		)

	def record_runs(self, shapes):
		"""Write a history by hand, one entry per (samples, assemblies, factor).

		By hand because the runs a test can actually perform finish in milliseconds,
		which is the length ``record`` throws away -- and because a test about what
		the history teaches needs to say what is in it."""
		jobs.run_history_path().unlink(missing_ok=True)
		self.addCleanup(jobs.run_history_path().unlink, missing_ok=True)
		in_flight, cores = frontend.BVBRC_MAX_IN_FLIGHT, frontend.PIPELINE_CORES
		for samples, assemblies, factor in shapes:
			baseline = run_estimates.baseline_seconds(samples, in_flight, cores, assemblies)
			run_estimates.record(
				samples, in_flight, cores, baseline * factor, assembly_count=assemblies
			)

	def test_the_network_stays_out_of_the_way_until_it_has_runs_to_learn_from(self):
		"""Most of a run's length is BV-BRC's queue, which is not ours to predict, and
		a network fitted to three runs of it does not estimate -- it memorises. So
		below MIN_TRAIN_RUNS it declines to answer and the estimate is the median it
		always was. A new instance must not get *worse* estimates for having grown the
		capacity to learn better ones."""
		in_flight, cores = frontend.BVBRC_MAX_IN_FLIGHT, frontend.PIPELINE_CORES
		self.record_runs([(1, 1, 0.5)] * (run_estimate_net.MIN_TRAIN_RUNS - 1))

		self.assertIsNone(run_estimates._correction_model())
		self.assertAlmostEqual(
			run_estimates.correction_for(1, in_flight, cores, 1), run_estimates.calibration()
		)

		# One more run is the threshold, and now there is something to fit.
		self.record_runs([(1, 1, 0.5)] * run_estimate_net.MIN_TRAIN_RUNS)
		self.assertIsNotNone(run_estimates._correction_model())

	def test_the_network_learns_what_one_median_cannot_say(self):
		"""A re-run that already holds its assemblies owes BV-BRC nothing, so it lands
		much closer to the arithmetic than a cold run of the same size does. One median
		has to split the difference and be wrong about both. The network is given the
		shape of the run, so it can be right about each.

		The history here says exactly that: cold runs come in at half the model's
		price, re-runs at very nearly it."""
		in_flight, cores = frontend.BVBRC_MAX_IN_FLIGHT, frontend.PIPELINE_CORES
		self.record_runs(
			[(samples, samples, 0.5) for samples in (1, 2, 4, 8, 12, 24)]
			+ [(samples, 0, 1.0) for samples in (1, 2, 4, 8, 12, 24)]
		)

		cold = run_estimates.correction_for(8, in_flight, cores, 8)
		rerun = run_estimates.correction_for(8, in_flight, cores, 0)
		self.assertGreater(rerun, cold * 1.25)
		self.assertAlmostEqual(cold, 0.5, delta=0.15)
		self.assertAlmostEqual(rerun, 1.0, delta=0.2)

	def test_two_workers_estimate_the_same_run_identically(self):
		"""Gunicorn runs several workers and the page polls whichever answers. If two
		of them fitted the same history and disagreed, a countdown would jump about as
		polls landed on different ones, and the user would end up watching the estimate
		rather than the run. Training is seeded and full-batch precisely so it cannot."""
		in_flight, cores = frontend.BVBRC_MAX_IN_FLIGHT, frontend.PIPELINE_CORES
		self.record_runs([(samples, samples, 0.4 + samples / 100) for samples in range(1, 13)])

		estimates = []
		for _ in range(3):
			# What a worker that has never seen this history starts from.
			run_estimates._cached_net_key = object()
			run_estimates._cached_net = None
			estimates.append(run_estimates.estimate_seconds(6, in_flight, cores, 3))
		self.assertEqual(len(set(estimates)), 1)

	def test_a_wild_history_cannot_make_the_estimate_absurd(self):
		"""A BV-BRC outage, or a box that was thrashing, can leave runs in the history
		that took twenty times what they should. The network is allowed to learn that
		this instance is slow; it is not allowed to quote the next run at twenty times
		the arithmetic on the strength of it. Beyond CORRECTION_LIMITS the arithmetic
		itself is what is wrong, and that is a thing to go and fix rather than to paper
		over here."""
		in_flight, cores = frontend.BVBRC_MAX_IN_FLIGHT, frontend.PIPELINE_CORES
		lower, upper = run_estimate_net.CORRECTION_LIMITS

		self.record_runs([(4, 4, 50.0)] * run_estimate_net.MIN_TRAIN_RUNS)
		self.assertLessEqual(run_estimates.correction_for(4, in_flight, cores, 4), upper)

		self.record_runs([(4, 4, 0.001)] * run_estimate_net.MIN_TRAIN_RUNS)
		self.assertGreaterEqual(run_estimates.correction_for(4, in_flight, cores, 4), lower)

	def test_admission_records_when_a_run_began_waiting_for_a_slot(self):
		"""Queue time is measured from admission, so admission has to be written down --
		and written to disk, since a queued job can outlive the restart that will
		eventually start it. For a run that takes a free slot at once, that moment is
		essentially its start: a wait of nothing."""
		manager = frontend._pipeline_manager
		job_id = self.runnable(self.submit_pair("Q_ADMIT").get_json()["job_id"])

		self.assertIsNone(manager.job_store.read_run_admitted(job_id))
		self.assertEqual(self.start(job_id).status_code, 200)

		admitted = manager.job_store.read_run_admitted(job_id)
		started = manager.job_store.read_run_started(job_id)
		self.assertIsNotNone(admitted)
		self.assertLessEqual(admitted, started)
		self.assertLess(started - admitted, 5)
		self.release(job_id)

	def test_a_run_records_its_queue_time_beside_its_runtime(self):
		"""Every recorded run carries both what it waited and what it ran. A test run
		finishes in milliseconds -- below the floor ``record`` keeps -- so a run that
		waited 40s for a slot and then worked for 100s is staged by hand and recorded
		through the real ``_record_duration``.

		The two are kept apart on purpose: the wait is contention, not pipeline cost,
		so it is stored next to the runtime but the ``factor`` every estimate is built
		on stays the runtime over the baseline, owing nothing to how busy the box was."""
		self.seed_run_history(seconds_per_run=None)
		manager = frontend._pipeline_manager
		job_id = self.runnable(self.submit_pair("Q_REC").get_json()["job_id"])

		now = time.time()
		jobs.job_results_dir(job_id).mkdir(parents=True, exist_ok=True)
		jobs.job_run_admitted_path(job_id).write_text(str(now - 140))
		jobs.job_run_started_path(job_id).write_text(str(now - 100))
		manager.pending_at_start[job_id] = (1, 1)

		manager._record_duration(job_id)

		entry = json.loads(jobs.run_history_path().read_text())[-1]
		self.assertAlmostEqual(entry["queue_seconds"], 40, delta=3)
		self.assertAlmostEqual(entry["seconds"], 100, delta=3)
		# The factor is the runtime over the baseline, not the runtime plus the wait:
		# had the 40s wait leaked into it, this would be off by nearly half.
		self.assertAlmostEqual(
			entry["factor"], entry["seconds"] / entry["baseline_seconds"], places=3
		)
		self.assertLess(
			entry["factor"], (entry["seconds"] + entry["queue_seconds"]) / entry["baseline_seconds"]
		)

	def test_a_queue_time_that_was_never_measured_is_stored_as_unknown_not_zero(self):
		"""A run from before admission was tracked has no wait on record. Unknown is
		stored as null, not as a zero: a false zero would tell the history the box was
		idle when nobody knows that it was, and the whole point of separating the wait
		from the runtime is not to feed the estimate a number it did not earn."""
		self.seed_run_history(seconds_per_run=None)
		manager = frontend._pipeline_manager
		job_id = self.runnable(self.submit_pair("Q_UNKNOWN").get_json()["job_id"])

		jobs.job_results_dir(job_id).mkdir(parents=True, exist_ok=True)
		jobs.job_run_started_path(job_id).write_text(str(time.time() - 100))
		manager.pending_at_start[job_id] = (1, 1)

		manager._record_duration(job_id)

		entry = json.loads(jobs.run_history_path().read_text())[-1]
		self.assertIsNone(entry["queue_seconds"])

	def test_a_rerun_is_measured_by_the_work_it_has_left_not_by_its_manifest(self):
		"""Re-running is how a user recovers from a failed run, and a re-run keeps
		every sample that already finished: admission clears the run markers but not
		the results, so Snakemake finds those outputs and skips them.

		A re-run quoted the whole job's runtime is wrong by however much it skips.
		A re-run that *records* its length against the whole manifest is worse: it
		divides a short run by a full job's waves and teaches every later estimate
		that a wave costs a fraction of what it does."""
		in_flight = frontend.BVBRC_MAX_IN_FLIGHT
		cores = frontend.PIPELINE_CORES
		one_sample = run_estimates.baseline_seconds(1, in_flight, cores)
		five_samples = run_estimates.baseline_seconds(5, in_flight, cores)
		# An instance that runs at exactly the model's pace, so an estimate here is
		# the model's own arithmetic and the test can name what it should say.
		self.seed_run_history(seconds_per_run=one_sample)
		self.assertEqual(run_estimates.calibration(), 1.0)

		sample_names = [f"RERUN_S{index}" for index in range(5)]
		job_id = self.submit_pair(sample_names[0]).get_json()["job_id"]
		for sample_name in sample_names[1:]:
			self.submit_pair(sample_name, job_id=job_id)
		self.runnable(job_id)

		manager = frontend._pipeline_manager
		self.assertEqual(manager.pending_sample_count(job_id), 5)
		self.assertAlmostEqual(manager.estimated_seconds(job_id), five_samples)

		# The run dies with one sample to go. What is left is one sample's work, and
		# the manifest still says five.
		for sample_name in sample_names[:-1]:
			self.complete_sample_on_disk(job_id, sample_name)
		self.assertEqual(manager.pending_sample_count(job_id), 1)
		self.assertAlmostEqual(manager.estimated_seconds(job_id), one_sample)

		# The re-run is quoted that one sample, and stays quoted it: the estimate is
		# the size of the run, and a run does not shrink as its samples land. The work
		# is two numbers -- the sample still owes local analysis, and it still owes an
		# assembly -- because the stages are skipped independently.
		self.start(job_id)
		self.assertEqual(manager.pending_at_start[job_id], (1, 1))
		self.assertAlmostEqual(
			self.client.get(f"/status?job_id={job_id}").get_json()["estimated_seconds"],
			one_sample,
		)
		self.complete_sample_on_disk(job_id, sample_names[-1])
		self.assertAlmostEqual(manager.estimated_seconds(job_id), one_sample)

		# And it is folded into the history as the one sample it did. Charged instead
		# to the five-sample manifest, a run that took exactly as long as the model
		# said a single sample should would have "proved" the instance runs
		# five_samples/one_sample times faster than it does, and every later estimate
		# would have been cut by that much.
		samples, assemblies = manager.pending_at_start[job_id]
		run_estimates.record(samples, in_flight, cores, one_sample, assemblies)
		self.assertEqual(run_estimates.calibration(), 1.0)

		# A job with nothing left to do is a Snakemake no-op, not a run of work.
		self.assertEqual(manager.pending_sample_count(job_id), 0)
		self.assertEqual(run_estimates.estimate_seconds(0, in_flight, cores), 0)

	def test_a_finished_sample_is_recognised_at_the_path_the_pipeline_writes_it_to(self):
		"""The outputs are looked for where Snakemake actually puts them.

		They live under results/<job>/<sample>/, and the lists here once left the
		<sample>/ out -- so every path checked was one that never exists, every sample
		looked unfinished forever, and a re-run was quoted (and recorded) as though it
		had its whole manifest still to do. It passed its tests, because the helper
		that faked a finished sample wrote the same wrong paths. So this test builds
		the outputs from the Snakefile's own layout instead."""
		job_id = self.runnable(self.submit_pair("PATHS").get_json()["job_id"])
		results_dir = jobs.job_results_dir(job_id)
		manager = frontend._pipeline_manager

		self.assertEqual(manager.pending_work(job_id), (1, 1))

		# Exactly what workflow/Snakefile's get_all_outputs() asks for, written to the
		# per-sample directory the rules write to.
		for relative_path in (
			"PATHS/01_raw_qc/validation.txt",
			"PATHS/02_assembly/assembly_contigs.fasta",
			"PATHS/02_assembly/genome_report.json",
			"PATHS/03_resistance/rgi_results.json",
			"PATHS/04_blast/rgi_proteins.fasta",
			"PATHS/04_blast/blast_results.csv",
			"PATHS/04_blast/blast_results_full.tsv",
			"PATHS/05_mlst/mlst_results.txt",
			"PATHS/06_mobile_elements/me_summary.csv",
			"PATHS/06_mobile_elements/PATHS_arg_mge_colocation.csv",
			"PATHS/summary/report.html",
		):
			output_path = results_dir / relative_path
			output_path.parent.mkdir(parents=True, exist_ok=True)
			output_path.write_text("")

		# Seen as done -- which is the whole point, and what the missing <sample>/ broke.
		self.assertTrue(run_estimates.sample_is_complete(results_dir, "PATHS"))
		self.assertFalse(run_estimates.sample_needs_assembly(results_dir, "PATHS"))
		self.assertEqual(manager.pending_work(job_id), (0, 0))

	def test_an_assembled_sample_is_not_charged_another_trip_to_bv_brc(self):
		"""The usual re-run: BV-BRC is done, the local analysis is not.

		Such a sample is nowhere near complete, but it owes the expensive stage
		nothing. Quoting it another assembly is how a re-run reads hours too long --
		and then finishes in a fraction of that and teaches the history that a cold
		run is cheap."""
		in_flight = frontend.BVBRC_MAX_IN_FLIGHT
		cores = frontend.PIPELINE_CORES
		job_id = self.runnable(self.submit_pair("ASM").get_json()["job_id"])
		results_dir = jobs.job_results_dir(job_id)
		manager = frontend._pipeline_manager

		for relative_path in run_estimates.SAMPLE_ASSEMBLY_OUTPUTS:
			output_path = results_dir / relative_path.format(sample="ASM")
			output_path.parent.mkdir(parents=True, exist_ok=True)
			output_path.write_text("")

		# Still a sample's worth of local work, but no assembly left to wait on.
		self.assertEqual(manager.pending_work(job_id), (1, 0))
		self.assertFalse(run_estimates.sample_is_complete(results_dir, "ASM"))

		local_only = run_estimates.baseline_seconds(1, in_flight, cores, 0)
		cold = run_estimates.baseline_seconds(1, in_flight, cores, 1)
		self.assertAlmostEqual(manager.estimated_seconds(job_id), local_only)
		self.assertLess(local_only, cold)
		# The difference is exactly the assembly it no longer has to wait for.
		self.assertAlmostEqual(cold - local_only, run_estimates.REMOTE_SECONDS)

	def test_bvbrc_waits_are_bounded_by_their_own_pool_and_not_by_this_box_s_cores(self):
		"""The 40-60 minutes a sample spends assembling is spent waiting on BV-BRC's
		cluster, not working on this one. It used to sit in a Snakemake core slot
		anyway, so a ten-sample batch could only wait on four samples at a time and
		took three rounds of assembly to do what one round could.

		So --cores is handed a job-slot budget, wide enough that the waiting is never
		what it gates, and the three things that are genuinely scarce get pools of
		their own: the box's cores, the samples BV-BRC will hold, and the samples
		whose reads are on local disk."""
		captured_argv = []

		def recording_popen(argv, **kwargs):
			captured_argv.append(argv)
			return _held_popen(argv, **kwargs)

		frontend.subprocess.Popen = recording_popen
		job_id = self.runnable(self.submit_pair("POOLS").get_json()["job_id"])
		self.assertEqual(self.start(job_id).status_code, 200)
		argv = captured_argv[0]

		# Each pool is passed, and each is the knob it says it is.
		self.assertIn("--resources", argv)
		self.assertIn(f"cpu={frontend.PIPELINE_CORES}", argv)
		self.assertIn(f"bvbrc={frontend.BVBRC_MAX_IN_FLIGHT}", argv)
		self.assertIn(f"uploads={frontend.BVBRC_UPLOAD_BATCH}", argv)
		# A rule pays a core of the box unless it declares otherwise, so raising the
		# slot count below cannot let the local work oversubscribe the machine.
		self.assertEqual(argv[argv.index("--default-resources") + 1], "cpu=1")

		# And the slot budget is not the core count: it is big enough to hold every
		# BV-BRC wait at once, which is the entire point.
		job_slots = int(argv[argv.index("--cores") + 1])
		self.assertGreaterEqual(job_slots, frontend.BVBRC_MAX_IN_FLIGHT + frontend.PIPELINE_CORES)
		self.assertGreater(job_slots, frontend.PIPELINE_CORES)

	def test_a_database_refresh_lands_in_the_middle_of_a_busy_queue(self):
		"""The scenario the whole drain design exists for.

		Two runs in flight, two queued, and the weekly refresh starts. Nothing in
		flight may be killed; nothing queued may be lost; nothing new may start into
		the restart. Afterwards the queue picks up by itself.
		"""
		running_a = self.runnable(self.submit_pair("REF_A").get_json()["job_id"])
		running_b = self.runnable(self.import_folder(["REF_B_S1"]).get_json()["job_id"])
		# Was a cloud import; that feature is disabled, and what this test is about is
		# the drain, not the door the job came in through.
		queued_c = self.runnable(self.import_folder(["REF_C_S2"]).get_json()["job_id"])
		queued_d = self.runnable(self.submit_pair("REF_D").get_json()["job_id"])

		self.assertEqual(self.start(running_a).status_code, 200)
		self.assertEqual(self.start(running_b).status_code, 200)
		self.assertEqual(self.start(queued_c).status_code, 202)
		self.assertEqual(self.start(queued_d).status_code, 202)

		# --- refresh-databases.sh begins: drain, then build the image (slow), then wait.
		jobs.drain_flag_path().touch()
		self.assertEqual(self.health()["draining"], True)

		# A user submits during the refresh. It must be accepted and parked, not refused.
		queued_e = self.runnable(self.submit_pair("REF_E").get_json()["job_id"])
		response = self.start(queued_e)
		self.assertEqual(response.status_code, 202)
		self.assertIn("database refresh", response.get_json()["message"])

		# The first in-flight run finishes. Its slot must stay EMPTY: promoting the
		# queue now would feed a fresh run straight into the restart that is coming.
		self.release(running_a)
		_wait_until(
			lambda: running_a not in frontend._pipeline_processes,
			what="the running job to finish",
		)
		self.assertNotIn(queued_c, frontend._pipeline_processes)
		self.assertEqual(self.health()["running"], 1)
		self.assertEqual(list(frontend._pipeline_queue), [queued_c, queued_d, queued_e])

		# The second finishes too. The host now sees running == 0 and restarts.
		self.release(running_b)
		_wait_until(lambda: self.health()["running"] == 0, what="the app to go idle")

		# Both in-flight runs completed normally -- the refresh killed neither.
		for job_id in (running_a, running_b):
			self.assertTrue(self.client.get(f"/status?job_id={job_id}").get_json()["success"])

		# --- the restart itself: a new process, with no memory of either structure.
		frontend._pipeline_processes.clear()
		frontend._pipeline_queue.clear()
		frontend.run_startup_recovery()

		# The queue survived on disk, the drain lifted, and the parked runs started
		# themselves -- in order, up to the slot limit.
		self.assertFalse(jobs.drain_flag_path().is_file())
		self.assertIn(queued_c, frontend._pipeline_processes)
		self.assertIn(queued_d, frontend._pipeline_processes)
		self.assertEqual(list(frontend._pipeline_queue), [queued_e])
		self.assertEqual(self.health(), {"draining": False, "running": 2, "queued": 1})

		# And the run that only ever waited was not mistaken for one the restart killed.
		self.assertIsNone(frontend._read_job_status(queued_e))

	def test_a_run_killed_by_the_restart_surfaces_as_failed_not_as_silence(self):
		"""If the refresh does not drain -- or the box simply dies -- the run that was
		executing must still tell the user something."""
		victim = self.runnable(self.submit_pair("VICTIM").get_json()["job_id"])
		self.assertEqual(self.start(victim).status_code, 200)
		_wait_until(lambda: victim in frontend._pipeline_processes, what="the run to start")

		# The container dies. In production the watcher thread dies with the process and
		# nothing is ever recorded -- but here it is alive in this very interpreter and
		# will happily write a status of its own. So let it finish first, and only then
		# clear the status, which is the state a real restart actually leaves behind:
		# a run that began, and no record of how it ended.
		frontend._pipeline_processes[victim].kill()
		_wait_until(
			lambda: jobs.job_status_path(victim).is_file(),
			what="the watcher to finish, so it cannot race the recovery",
		)
		frontend._pipeline_processes.clear()
		frontend._pipeline_queue.clear()
		jobs.job_status_path(victim).unlink()

		frontend.run_startup_recovery()

		status = self.client.get(f"/status?job_id={victim}").get_json()
		self.assertTrue(status["done"])
		self.assertFalse(status["success"])
		self.assertIn("restarted", status["error"])


if __name__ == "__main__":
	unittest.main()
