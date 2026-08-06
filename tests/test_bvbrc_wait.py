"""Waiting on a BV-BRC job: what ends the wait, and what happens next.

Nothing here is on a clock. A job's runtime is BV-BRC's queue plus BV-BRC's
compute, and every deadline this pipeline has tried only ever managed to throw
away jobs that were still working -- a sample failed with hours of remote compute
already spent on it. So what is pinned here is the rule that replaced it: a job is
waited on until BV-BRC says it finished, and a job BV-BRC says it failed is
resubmitted rather than losing the sample to a failure that was probably theirs.

No network and no waiting: the task query is supplied by the test, and time is a
counter that only advances when the poll loop sleeps.
"""

import inspect
import unittest
from unittest import mock

from tests._isolation import TMP_ROOT  # noqa: F401  (must import first)
from workflow.helpers import bvbrc_client  # noqa: E402
from workflow.helpers.bvbrc_client import BVBRCClient  # noqa: E402

OLD_IDLE_LIMIT = 3 * 60 * 60
OLD_TOTAL_CEILING = 12 * 60 * 60


class FakeClock:
	"""Stands in for the `time` module: sleeping is the only thing that passes."""

	def __init__(self):
		self.now = 1_000_000.0

	def time(self):
		return self.now

	def sleep(self, seconds):
		self.now += seconds


class Poller:
	"""One scripted poll loop: what each status check comes back with, in order.

	An entry is a status string, None for "BV-BRC has no such task", or an
	exception to raise as if the service could not be reached. The last entry
	repeats forever, so a test that means "and then nothing ever changes again"
	says exactly that.
	"""

	def __init__(self, script):
		self.script = list(script)
		self.polls = 0

	def query(self, job_id):
		entry = self.script[min(self.polls, len(self.script) - 1)]
		self.polls += 1
		if isinstance(entry, Exception):
			raise entry
		return None if entry is None else {"status": entry}


class WaitBase(unittest.TestCase):
	def setUp(self):
		self.clock = FakeClock()
		time_patch = mock.patch.object(bvbrc_client, "time", self.clock)
		time_patch.start()
		self.addCleanup(time_patch.stop)
		self.client = BVBRCClient(token_file=str(TMP_ROOT / "no-such-token"))
		self.client.token = "test-token"

	def wait(self, script, **kwargs):
		poller = Poller(script)
		self.client._query_task = poller.query
		kwargs.setdefault("poll_interval", 30)
		started_at = self.clock.now
		result = self.client.wait_for_job("42", **kwargs)
		return result, self.clock.now - started_at


class TestTheWaitHasNoLimit(WaitBase):
	def test_the_wait_takes_no_deadline_at_all(self):
		"""Not a generous default -- there is no knob. Any figure here is a guess
		at BV-BRC's queue depth, and being wrong costs a finished assembly."""
		parameters = set(inspect.signature(BVBRCClient.wait_for_job).parameters)
		self.assertEqual(parameters, {"self", "job_id", "poll_interval"})

	def test_a_job_is_waited_on_however_long_it_takes(self):
		"""Eight hours queued and eight hours running, returning nothing at all
		until it finishes. Both of the limits this replaced would have failed it:
		the idle one at three hours, the absolute one at twelve."""
		script = ["queued"] * 1000 + ["in-progress"] * 1000 + ["completed"]
		(complete, status), elapsed = self.wait(script)
		self.assertTrue(complete)
		self.assertEqual(status, "completed")
		self.assertGreater(elapsed, OLD_TOTAL_CEILING)

	def test_a_silent_job_is_waited_on_too(self):
		"""A job returning no files is no longer evidence of anything: what the
		old idle clock actually caught, most of the time, was a long queue."""
		script = ["in-progress"] * 500 + ["completed"]
		(complete, _), elapsed = self.wait(script)
		self.assertTrue(complete)
		self.assertGreater(elapsed, OLD_IDLE_LIMIT)

	def test_an_unrecognised_status_is_not_a_failure(self):
		"""BV-BRC's statuses are theirs to add to. Anything that is not a failure
		is a job that has not finished, and is waited on."""
		(complete, status), _ = self.wait(["init", "pending", "running", "completed"])
		self.assertTrue(complete)
		self.assertEqual(status, "completed")

	def test_an_unreachable_service_does_not_end_the_wait(self):
		"""Not an answer about the job. Giving up here would fail every sample in
		flight for the duration of someone else's outage."""
		script = [RuntimeError("service unavailable")] * 100 + ["completed"]
		(complete, status), _ = self.wait(script)
		self.assertTrue(complete)
		self.assertEqual(status, "completed")


class TestWhatEndsTheWait(WaitBase):
	def test_a_failed_job_ends_the_wait_at_once(self):
		(complete, status), elapsed = self.wait(["failed"])
		self.assertFalse(complete)
		self.assertEqual(status, "failed")
		self.assertEqual(elapsed, 0)

	def test_the_other_ways_bvbrc_ends_a_job_also_end_the_wait(self):
		for status_name in ("deleted", "terminated", "cancelled", "killed"):
			with self.subTest(status=status_name):
				(complete, status), elapsed = self.wait([status_name])
				self.assertFalse(complete)
				self.assertEqual(status, status_name)
				self.assertEqual(elapsed, 0)

	def test_a_job_bvbrc_has_lost_ends_the_wait(self):
		"""The one stopping condition that is not a status. With no clock behind
		it, this is all that stands between a job BV-BRC has forgotten and a poll
		loop that runs until someone kills the pipeline."""
		(complete, status), elapsed = self.wait([None])
		self.assertFalse(complete)
		self.assertEqual(status, "missing")
		self.assertLess(elapsed, 5 * 30, "a lost job is not waited out")

	def test_one_odd_answer_is_not_a_lost_job(self):
		"""A single response that omits the task is not a job's obituary."""
		(complete, status), _ = self.wait([None, "in-progress", "completed"])
		self.assertTrue(complete)
		self.assertEqual(status, "completed")


class Attempts:
	"""A scripted run of run_job: what each submission hands back, and how each
	wait on it ends."""

	def __init__(self, waits, submissions=None):
		self.waits = list(waits)
		self.submissions = submissions
		self.submitted = []
		self.announced = []
		self.waited = []

	def submit(self):
		index = len(self.submitted)
		job_id = self.submissions[index] if self.submissions else f"job-{index + 1}"
		self.submitted.append(job_id)
		return job_id

	def announce(self, job_id):
		self.announced.append(job_id)

	def wait(self, job_id, poll_interval=30):
		status = self.waits[min(len(self.waited), len(self.waits) - 1)]
		self.waited.append(job_id)
		return status == "completed", status


class TestResubmittingAFailedJob(WaitBase):
	def run_job(self, attempts, **kwargs):
		self.client.wait_for_job = attempts.wait
		kwargs.setdefault("retry_delay", 60)
		kwargs.setdefault("max_attempts", 3)
		return self.client.run_job(attempts.submit, on_submit=attempts.announce, **kwargs)

	def test_a_failed_job_is_resubmitted(self):
		"""The trade the removed timeout was paying for: with nothing cut short
		for being slow, a reported failure is the only thing that loses a sample --
		and BV-BRC's failures are often BV-BRC's, not the sample's."""
		attempts = Attempts(["failed", "completed"])
		self.assertEqual(self.run_job(attempts), "job-2")
		self.assertEqual(attempts.submitted, ["job-1", "job-2"])

	def test_a_lost_job_is_resubmitted_the_same_way(self):
		attempts = Attempts(["missing", "completed"])
		self.assertEqual(self.run_job(attempts), "job-2")

	def test_a_job_that_completes_is_not_resubmitted(self):
		attempts = Attempts(["completed"])
		self.assertEqual(self.run_job(attempts), "job-1")
		self.assertEqual(attempts.submitted, ["job-1"])

	def test_the_caller_is_told_every_job_id_as_it_is_submitted(self):
		"""How the CGA rule's cached job ID follows the live job. Left pointing at
		the failed attempt, a restart would rejoin a job that is already dead."""
		attempts = Attempts(["failed", "completed"])
		self.run_job(attempts)
		self.assertEqual(attempts.announced, ["job-1", "job-2"])

	def test_the_same_failure_three_times_running_is_the_answer(self):
		"""Retrying is for BV-BRC's failures. A job that fails every time it is
		submitted is the sample's problem, and the sample fails."""
		attempts = Attempts(["failed"])
		with self.assertRaises(RuntimeError) as raised:
			self.run_job(attempts, max_attempts=3)
		self.assertIn("failed", str(raised.exception))
		self.assertEqual(len(attempts.submitted), 3)

	def test_a_resubmission_waits_first(self):
		"""Straight back into a queue that just failed the job is not a retry."""
		attempts = Attempts(["failed"])
		started_at = self.clock.now
		with self.assertRaises(RuntimeError):
			self.run_job(attempts, max_attempts=3, retry_delay=60)
		self.assertEqual(self.clock.now - started_at, 120, "a delay between attempts, not before")

	def test_a_refused_submission_counts_as_an_attempt(self):
		"""BV-BRC declining to take the job at all is a failure like any other,
		and must not become a submission loop with nothing to wait on."""
		attempts = Attempts(["completed"], submissions=[None, None, None])
		with self.assertRaises(RuntimeError):
			self.run_job(attempts, max_attempts=3)
		self.assertEqual(len(attempts.submitted), 3)
		self.assertEqual(attempts.waited, [])

	def test_a_resumed_job_is_waited_on_rather_than_duplicated(self):
		"""A restarted pipeline rejoins the assembly BV-BRC is already running."""
		attempts = Attempts(["completed"])
		self.assertEqual(self.run_job(attempts, resume_job_id="cached-7"), "cached-7")
		self.assertEqual(attempts.submitted, [])
		self.assertEqual(attempts.waited, ["cached-7"])

	def test_a_resumed_job_that_fails_is_resubmitted(self):
		attempts = Attempts(["failed", "completed"])
		self.assertEqual(self.run_job(attempts, resume_job_id="cached-7"), "job-1")
		self.assertEqual(attempts.waited, ["cached-7", "job-1"])


if __name__ == "__main__":
	unittest.main()
