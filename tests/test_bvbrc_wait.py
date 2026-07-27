"""Waiting on a BV-BRC job: when to keep waiting, and when to give up.

The timeout is what decides whether a sample that BV-BRC is still working on
comes back as an assembly or as a failure, and the sample has hours of remote
compute behind it by the time the question is asked. So what is pinned here is
the rule itself: the clock measures time since the last file BV-BRC returned, not
time since submission, and a job still delivering output is never cut off.

No network and no waiting: get_job_status and the workspace listing are supplied
by the test, and time is a counter that only advances when the poll loop sleeps.
"""

import inspect
import unittest
from unittest import mock

from tests._isolation import TMP_ROOT  # noqa: F401  (must import first)
from workflow.helpers import bvbrc_client  # noqa: E402
from workflow.helpers.bvbrc_client import BVBRCClient  # noqa: E402

OUTPUT_PATH = "/user@bvbrc/home/ws/cga_SAMPLE"


def _files_so_far(count):
	"""The job's output after it has written `count` files, as (name, size) pairs."""
	return [(f"f{index}.fa", 1) for index in range(count)]


class FakeClock:
	"""Stands in for the `time` module: sleeping is the only thing that passes."""

	def __init__(self):
		self.now = 1_000_000.0

	def time(self):
		return self.now

	def sleep(self, seconds):
		self.now += seconds


class Poller:
	"""One scripted poll loop: what each poll sees, in order.

	Each entry is (status, workspace_files); the last entry repeats forever, so a
	test that means "and then nothing ever changes again" says exactly that.
	"""

	def __init__(self, script):
		self.script = script
		self.polls = 0

	def _current(self):
		return self.script[min(self.polls, len(self.script) - 1)]

	def status(self, job_id):
		self.polls += 1
		return {"status": self._current()[0]}

	def files(self, base_path, max_depth=3):
		return [
			{"name": name, "type": "unspecified", "path": f"{base_path}/{name}", "size": size}
			for name, size in self._current()[1]
		]


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
		self.client.get_job_status = poller.status
		self.client.walk_workspace = poller.files
		kwargs.setdefault("output_paths", [OUTPUT_PATH])
		kwargs.setdefault("max_wait_seconds", 3600)
		kwargs.setdefault("poll_interval", 30)
		started_at = self.clock.now
		result = self.client.wait_for_job("42", **kwargs)
		return result, self.clock.now - started_at


class TestWaitResetsOnReturnedFiles(WaitBase):
	def test_the_default_wait_is_three_hours(self):
		"""Not a preference: it is the figure config/config.yaml and the README
		both quote, and the poll loop is where it actually takes effect."""
		default_wait = inspect.signature(BVBRCClient.wait_for_job).parameters["max_wait_seconds"]
		self.assertEqual(default_wait.default, 3 * 60 * 60)

	def test_a_job_that_returns_nothing_times_out_on_the_idle_limit(self):
		(complete, status), elapsed = self.wait(
			[("in-progress", [])], max_wait_seconds=600, poll_interval=30
		)
		self.assertFalse(complete)
		self.assertEqual(status, "in-progress")
		self.assertLess(elapsed, 700, "the wait must end shortly after the idle limit")

	def test_a_file_arriving_restarts_the_clock(self):
		"""The whole point: at 20 polls the job has been running for twice the
		limit, and every one of those polls brought back another file. The old
		fixed timeout failed this sample with the assembly nearly done."""
		script = [("in-progress", [(f"contig_{index}.fa", 100)]) for index in range(20)]
		script.append(("completed", [("contigs.fa", 100)]))
		(complete, status), elapsed = self.wait(script, max_wait_seconds=300, poll_interval=30)
		self.assertTrue(complete)
		self.assertEqual(status, "completed")
		self.assertGreater(elapsed, 300, "this job outlived the idle limit several times over")

	def test_a_file_that_is_still_growing_counts_as_progress(self):
		"""One file, written slowly, is a job that is working -- and the only sign
		of it is the size. Without that, a long single-file write reads as a stall."""
		script = [("in-progress", [("assembly.fa", size)]) for size in range(1, 30)]
		script.append(("completed", [("assembly.fa", 30)]))
		(complete, _), _ = self.wait(script, max_wait_seconds=300, poll_interval=30)
		self.assertTrue(complete)

	def test_a_job_that_stops_delivering_still_times_out(self):
		"""Progress buys time; it does not buy forever. Ten files arrive, then the
		job stalls, and the wait ends an idle limit after the last of them."""
		script = [("in-progress", _files_so_far(count)) for count in range(1, 11)]
		script.append(("in-progress", _files_so_far(10)))  # and nothing more, ever
		(complete, status), elapsed = self.wait(script, max_wait_seconds=300, poll_interval=30)
		self.assertFalse(complete)
		self.assertEqual(status, "in-progress")
		self.assertLess(elapsed, 10 * 30 + 400)

	def test_the_absolute_ceiling_ends_a_job_that_never_stops_writing(self):
		"""A job touching a log file every 30 seconds would otherwise hold a BV-BRC
		slot for as long as BV-BRC let it."""
		script = [("in-progress", _files_so_far(count)) for count in range(1, 501)]
		(complete, _), elapsed = self.wait(
			script, max_wait_seconds=300, poll_interval=30, max_total_seconds=1800
		)
		self.assertFalse(complete)
		self.assertLessEqual(elapsed, 1800 + 30)

	def test_a_long_wait_does_not_relist_the_workspace_on_every_poll(self):
		"""Each listing is an RPC per folder in the result tree, against a shared
		public service, for a dozen samples at once. A three-hour deadline does not
		need answering every thirty seconds, so the check runs at a tenth of it."""
		listings = []
		poller = Poller([("in-progress", [("f.fa", 1)])])
		self.client.get_job_status = poller.status
		self.client.walk_workspace = lambda path, max_depth=3: (
			listings.append(path) or poller.files(path, max_depth)
		)
		self.client.wait_for_job(
			"42",
			max_wait_seconds=10800,
			poll_interval=30,
			output_paths=[OUTPUT_PATH],
			max_total_seconds=3600,
		)
		self.assertGreaterEqual(poller.polls, 120, "an hour of polling, every 30 seconds")
		self.assertLessEqual(len(listings), 15, "an hour of listings, every 5 minutes")

	def test_a_failed_job_is_not_waited_on(self):
		(complete, status), elapsed = self.wait([("failed", [])])
		self.assertFalse(complete)
		self.assertEqual(status, "failed")
		self.assertEqual(elapsed, 0)

	def test_an_unreadable_workspace_neither_starts_nor_stops_the_clock(self):
		"""A listing that errors says nothing about the job: it is not progress,
		and it is not a stall either. The wait ends on the idle limit exactly as
		if the workspace had been readable and empty."""

		def unreadable(base_path, max_depth=3):
			raise RuntimeError("workspace unreachable")

		poller = Poller([("in-progress", [])])
		self.client.get_job_status = poller.status
		self.client.walk_workspace = unreadable
		started_at = self.clock.now
		complete, _ = self.client.wait_for_job(
			"42", max_wait_seconds=600, poll_interval=30, output_paths=[OUTPUT_PATH]
		)
		self.assertFalse(complete)
		self.assertLess(self.clock.now - started_at, 700)

	def test_without_output_paths_it_is_the_old_fixed_timeout(self):
		script = [("in-progress", [(f"f{index}.fa", 1)]) for index in range(100)]
		(complete, _), elapsed = self.wait(
			script, output_paths=None, max_wait_seconds=600, poll_interval=30
		)
		self.assertFalse(complete)
		self.assertLess(elapsed, 700)


if __name__ == "__main__":
	unittest.main()
