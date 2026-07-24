"""Two things a user does while an upload is still going, and what each one has
to leave behind.

Pressing Run mid-upload, and starting a second upload on top of one already in
flight, are both split across the two halves of this app, so they are tested in
both. The server decides what a run freezes and whether two overlapping uploads
can share a job; the browser decides whether pressing Run mid-upload reaches the
server at all, and which job the second upload joins. Testing only the server
would miss a deferral that stopped working, and testing only the browser would
take its word for what the server does with the request when it finally arrives.

The browser half runs the real static/app.js under Node -- see tests/app_driver.js
for the stub DOM and stub network it runs against. app.js is a page script, not a
module, so the driver reports what the app *did* (which requests, in which order,
carrying which job) and the assertions are made here.
"""

import contextlib
import json
import shutil
import subprocess
import sys
import threading
import time
import unittest
from pathlib import Path

import frontend  # noqa: E402
from tests._isolation import REAL_ROOT  # noqa: F401  (must import first)
from tests.test_batching import _REAL_POPEN, Base, token_for  # noqa: E402
from workflow.helpers import import_service, jobs  # noqa: E402

NODE = shutil.which("node")


@contextlib.contextmanager
def held(target, attribute):
	"""Freeze one call inside the request that makes it.

	An upload runs entirely inside its own request (/submit and /import stage,
	verify and offload before they answer), so "while an upload is in flight" is
	only reachable by stopping one partway and acting from another thread. Yields
	the event that says the call has been entered and the one that lets it finish.
	"""
	started = threading.Event()
	release = threading.Event()
	original = getattr(target, attribute)

	def blocking(*args, **kwargs):
		started.set()
		if not release.wait(timeout=30):  # pragma: no cover -- a hung test, not a result
			raise AssertionError(f"{attribute} was held but never released")
		return original(*args, **kwargs)

	setattr(target, attribute, blocking)
	try:
		yield started, release
	finally:
		setattr(target, attribute, original)


class ServerBase(Base):
	def held_run(self, job_id):
		"""Start a real /run that stays running until the test lets it go."""
		token_for(job_id)
		frontend.subprocess.Popen = lambda argv, **kw: _REAL_POPEN(
			[sys.executable, "-c", "import time; time.sleep(30)"], **kw
		)
		try:
			return self.client.post("/run", data={"job_id": job_id})
		finally:
			frontend.subprocess.Popen = _REAL_POPEN

	def tearDown(self):
		for pipeline_process in list(frontend._pipeline_manager.processes.values()):
			try:
				pipeline_process.kill()
			except Exception:  # pragma: no cover
				pass
		frontend._pipeline_manager.processes.clear()
		frontend._pipeline_manager.queue.clear()
		frontend.subprocess.Popen = _REAL_POPEN


# What the server does with a run that arrives mid-upload -- which is the reason
# the browser does not send one.
class TestARunThatArrivesMidUpload(ServerBase):
	def test_a_run_between_batches_freezes_the_job_around_what_landed(self):
		"""The whole case for deferring the press, stated as a server behaviour.

		Nothing here refuses the run: batch one's reads are all present, so from
		the server's side this is an ordinary runnable job. It is the batches
		after it that pay -- they are refused, and the job keeps a subset of the
		folder the user chose, permanently."""
		job_id = self.import_folder(["BATCH1_S1"]).get_json()["job_id"]

		self.assertEqual(self.held_run(job_id).status_code, 200)

		second_batch = self.import_folder(["BATCH2_S2"], job_id=job_id)
		self.assertEqual(second_batch.status_code, 409)
		self.assertIn("running", second_batch.get_json()["error"].lower())
		third_batch = self.import_folder(["BATCH3_S3"], job_id=job_id)
		self.assertEqual(third_batch.status_code, 409)

		# Not partially applied, and not recoverable: the job is now the first
		# batch and stays that way, because a job that has run is frozen.
		self.assertEqual(self.isolates(job_id), ["BATCH1_S1"])

	def test_a_run_after_the_last_batch_carries_every_sample(self):
		"""The same three batches with the press deferred to the end -- which is
		what app.js does with it."""
		job_id = self.import_folder(["EVERY1_S1"]).get_json()["job_id"]
		for isolate_id in ("EVERY2_S2", "EVERY3_S3"):
			self.assertEqual(self.import_folder([isolate_id], job_id=job_id).status_code, 200)

		self.assertEqual(self.isolates(job_id), ["EVERY1_S1", "EVERY2_S2", "EVERY3_S3"])
		self.assertEqual(self.held_run(job_id).status_code, 200)

		samples, _ = frontend._job_store.read_samples(jobs.job_samples_csv(job_id))
		self.assertEqual(len(samples), 3, "the run has to cover the whole folder, not a prefix of it")

	def test_a_run_is_refused_while_one_of_its_reads_is_still_missing(self):
		"""A manifest row whose read is in neither place is an upload still
		landing, not a job ready to run. Belt to the browser's braces: the
		deferral is a courtesy the page extends, and a request can arrive without
		one -- a second tab, a stale page, curl."""
		job_id = self.submit_pair("HALFTHERE").get_json()["job_id"]
		token_for(job_id)
		samples, _ = frontend._job_store.read_samples(jobs.job_samples_csv(job_id))
		(frontend.PROJECT_ROOT / samples[0]["R1_path"]).unlink()

		response = self.client.post("/run", data={"job_id": job_id})
		self.assertEqual(response.status_code, 409)
		self.assertIn("upload still in progress", response.get_json()["error"].lower())
		self.assertEqual(frontend._pipeline_manager.processes, {})


# A second upload started before the first has answered.
class TestASecondUploadWhileOneIsInFlight(ServerBase):
	def test_a_pair_added_during_a_folder_import_lands_in_the_same_job(self):
		"""Both uploads are read-modify-writes of one samples.csv, so the one that
		arrives second has to wait rather than write over the copy the first is
		holding -- otherwise a folder's samples vanish when a pair lands on top of
		them, with both requests reporting success.

		Held inside ImportService's per-job lock, which is where they contend."""
		job_id = self.client.post("/job/new").get_json()["job_id"]
		responses = {}

		with held(import_service.import_samples, "import_directory") as (started, release):
			folder = threading.Thread(
				target=lambda: responses.update(
					folder=self.import_folder(["FOLDER_S1", "FOLDER_S2"], job_id=job_id)
				)
			)
			folder.start()
			self.assertTrue(started.wait(timeout=10), "the folder import never reached the server")
			self.assertTrue(frontend._job_lock(job_id).locked())

			# The folder is mid-manifest and has not answered. This is the moment
			# the user picks two files and presses Add pair.
			pair = threading.Thread(
				target=lambda: responses.update(pair=self.submit_pair("PAIRED", job_id=job_id))
			)
			pair.start()
			pair.join(timeout=0.5)
			self.assertTrue(pair.is_alive(), "the pair did not wait for the manifest")
			self.assertNotIn("pair", responses)

			release.set()
			folder.join(timeout=30)
			pair.join(timeout=30)

		self.assertEqual(responses["folder"].status_code, 200)
		self.assertEqual(responses["pair"].status_code, 200)
		self.assertEqual(responses["pair"].get_json()["job_id"], job_id)
		self.assertEqual(self.isolates(job_id), ["FOLDER_S1", "FOLDER_S2", "PAIRED"])

	def test_overlapping_uploads_are_both_counted_so_a_restart_waits_for_both(self):
		"""deploy/refresh-databases.sh restarts when /api/health says nothing is in
		flight, and a restart mid-upload loses that upload outright. Two at once
		have to read as two, or the second is restarted out from under the user."""
		job_id = self.client.post("/job/new").get_json()["job_id"]

		with held(frontend._import_service, "import_directory") as (folder_started, folder_release):
			with held(frontend._import_service, "save_upload") as (pair_started, pair_release):
				folder = threading.Thread(
					target=lambda: self.import_folder(["INFLIGHT_S1"], job_id=job_id)
				)
				pair = threading.Thread(target=lambda: self.submit_pair("INFLIGHT_PAIR", job_id=job_id))
				folder.start()
				pair.start()
				self.assertTrue(folder_started.wait(timeout=10))
				self.assertTrue(pair_started.wait(timeout=10))

				in_flight = self.client.get("/api/health").get_json()["uploads"]["in_flight"]

				pair_release.set()
				folder_release.set()
			folder.join(timeout=30)
			pair.join(timeout=30)

		self.assertEqual(in_flight, 2)
		# And back to nothing once they answer, or the refresh never restarts.
		deadline = time.time() + 10
		while time.time() < deadline:
			if self.client.get("/api/health").get_json()["uploads"]["in_flight"] == 0:
				break
			time.sleep(0.05)
		self.assertEqual(self.client.get("/api/health").get_json()["uploads"]["in_flight"], 0)
		self.assertEqual(self.isolates(job_id), ["INFLIGHT_PAIR", "INFLIGHT_S1"])

	def test_a_second_upload_that_names_no_job_opens_its_own(self):
		"""Which is why the browser reserves the job before the first byte moves:
		the server cannot guess that two uploads belong together, so an upload
		that arrives without a job ID is a new batch, mid-upload or not."""
		job_id = self.client.post("/job/new").get_json()["job_id"]

		with held(frontend._import_service, "import_directory") as (started, release):
			folder = threading.Thread(target=lambda: self.import_folder(["LONELY_S1"], job_id=job_id))
			folder.start()
			self.assertTrue(started.wait(timeout=10))

			stray = self.submit_pair("STRAY").get_json()

			release.set()
			folder.join(timeout=30)

		self.assertNotEqual(stray["job_id"], job_id)
		self.assertEqual(self.isolates(job_id), ["LONELY_S1"])
		self.assertEqual(self.isolates(stray["job_id"]), ["STRAY"])


@unittest.skipUnless(NODE, "node is not installed; the browser half of these cases cannot run")
class BrowserCase(unittest.TestCase):
	"""Runs one tests/app_driver.js scenario against the real static/app.js.

	Once per class, not once per test: the scenario is the expensive part (a Node
	process) and every test in a class is a different question about the same run.
	"""

	scenario = None

	@classmethod
	def setUpClass(cls):
		completed = subprocess.run(
			[NODE, str(REAL_ROOT / "tests" / "app_driver.js"), cls.scenario],
			capture_output=True,
			text=True,
			timeout=120,
			cwd=REAL_ROOT,
		)
		if completed.returncode != 0:
			raise AssertionError(
				f"the {cls.scenario} scenario failed to run:\n{completed.stderr or completed.stdout}"
			)
		cls.result = json.loads(completed.stdout)


class TestPressingRunMidUpload(BrowserCase):
	"""Three 3 GB pairs, so app.js's own 2 GB batching splits them into three
	POSTs, and Run pressed while the first one is still going."""

	scenario = "runPressedMidImport"

	def test_the_folder_really_did_go_up_in_several_batches(self):
		"""Otherwise the rest of this class is a one-batch upload wearing a
		three-batch name, and it would pass with the deferral deleted."""
		self.assertEqual(self.result["batchesSent"], 3)

	def test_the_press_is_recorded_rather_than_sent(self):
		pressed = self.result["whilePressed"]
		self.assertEqual(pressed["runRequests"], 0, "Run reached the server mid-upload")
		self.assertEqual(pressed["batchesStillOpen"], 1, "the upload was not actually in flight")
		self.assertTrue(pressed["runButtonDisabled"], "a second press would queue a second run")
		self.assertIn("queued", pressed["runStatusText"].lower())
		self.assertIn("once this upload finishes", pressed["runStatusText"].lower())

	def test_no_batch_boundary_is_mistaken_for_the_end_of_the_upload(self):
		"""A batch answering 200 is the tempting moment to honour the press, and
		it is the wrong one: two thirds of the folder is still in the browser."""
		self.assertEqual(self.result["runRequestsAfterEachBatch"], [0, 0])

	def test_the_run_starts_once_the_last_batch_lands(self):
		self.assertEqual(self.result["runRequests"], 1, "the deferred run was dropped or doubled")
		self.assertEqual(
			self.result["trail"],
			["/job/new", "/import", "/import", "/import", "/run"],
			"the run has to be the last thing sent, after every batch",
		)

	def test_every_batch_and_the_run_name_the_one_reserved_job(self):
		self.assertEqual(self.result["jobReservations"], 1)
		self.assertEqual(len(set(self.result["batchJobIds"])), 1)


class TestPressingRunMidUploadThatFails(BrowserCase):
	"""The same press, over an upload that dies on its second batch."""

	scenario = "runPressedMidImportThatFails"

	def test_the_queued_run_is_dropped_rather_than_started(self):
		"""Starting it would freeze the job around the batches that did land, and
		the samples still in the browser could never be added afterwards."""
		self.assertEqual(self.result["runRequests"], 0)
		self.assertEqual(self.result["trail"], ["/job/new", "/import", "/import"])

	def test_the_user_is_told_the_run_is_not_happening(self):
		final = self.result["final"]
		self.assertIn("was not started", final["runStatusText"])
		self.assertFalse(final["runButtonDisabled"], "there is no way back to Run from here")

	def test_a_rejected_batch_is_not_retried(self):
		"""400 is the server's verdict on these reads. Re-sending them would dress
		a checksum mismatch up as a flaky link."""
		self.assertEqual(self.result["batchesSent"], 2)
		self.assertIn("checksum mismatch", self.result["final"]["importStatusText"])


class TestAddingAPairWhileAnUploadIsInProgress(BrowserCase):
	"""A two-batch folder going up, and the user picks two files and presses Add
	pair without waiting for it."""

	scenario = "pairAddedDuringImport"

	def test_the_pair_is_sent_while_the_folder_is_still_going(self):
		during = self.result["whileImportOpen"]
		self.assertEqual(during["batchesStillOpen"], 1, "the folder had already finished")
		self.assertEqual(during["batchesSent"], 1)

	def test_the_second_upload_joins_the_job_instead_of_opening_one(self):
		"""ensureUploadJob() reserves once and every later upload reuses it. Two
		reservations would leave the user with half a batch in each of two jobs
		and no way to merge them."""
		self.assertEqual(self.result["jobReservations"], 1)
		self.assertEqual(self.result["whileImportOpen"]["jobReservations"], 1)

	def test_both_uploads_name_the_same_job(self):
		batch_job_ids = set(self.result["batchJobIds"])
		self.assertEqual(len(batch_job_ids), 1)
		self.assertEqual(self.result["whileImportOpen"]["submitJobId"], batch_job_ids.pop())

	def test_the_folder_finishes_its_remaining_batches_afterwards(self):
		"""The pair must not cost the folder its place: the batch after it still
		has to go, and go to the same job."""
		self.assertEqual(self.result["batchesSent"], 2)
		self.assertEqual(self.result["trail"], ["/job/new", "/import", "/submit", "/import"])

	def test_neither_upload_starts_a_run_on_its_own(self):
		self.assertEqual(self.result["runRequests"], 0)


class TestAPairAddedOnTopOfARunPressedMidUpload(BrowserCase):
	"""Both cases at once: Run pressed during a three-batch folder, then a pair
	added while that folder is still going."""

	scenario = "runPressedThenPairAddedDuringImport"

	def test_the_pair_landing_is_not_mistaken_for_the_upload_finishing(self):
		"""The bug this pins: uploadInProgress was a boolean, so the pair's
		completion handler cleared the flag the folder had set and fired the run
		queued against the folder -- one batch of three registered. The server
		then froze the job around it (see
		TestARunThatArrivesMidUpload.test_a_run_between_batches_freezes_the_job_around_what_landed)
		and refused the last two batches, which is the outcome deferring the press
		exists to prevent, reached by pressing the buttons in this order."""
		after_pair = self.result["afterPairLanded"]
		self.assertEqual(
			after_pair["runRequests"],
			0,
			f"the run started with only {after_pair['isolatesSentSoFar']} of the folder uploaded",
		)
		self.assertEqual(after_pair["batchesStillOpen"], 1, "the folder was not still going")

	def test_the_run_waits_for_the_last_batch_of_the_folder(self):
		self.assertEqual(self.result["runRequests"], 1)
		self.assertEqual(
			self.result["trail"],
			["/job/new", "/import", "/submit", "/import", "/import", "/run"],
		)

	def test_the_queued_line_survives_the_pair_re_rendering_the_panel(self):
		"""A finished upload re-renders the batch panel, and the re-render used to
		clear the run line and re-enable the button -- leaving the app silently
		waiting on a press with nothing on screen to say so."""
		after_pair = self.result["afterPairLanded"]
		self.assertTrue(after_pair["runButtonDisabled"])
		self.assertIn("queued", after_pair["runStatusText"].lower())


class TestAPairThatFailsOnTopOfARunPressedMidUpload(BrowserCase):
	"""The counterpart: of two uploads the run is waiting on, the second one is
	refused while the first is still going."""

	scenario = "runPressedThenAPairThatFailsDuringImport"

	def test_the_queued_run_is_dropped_even_though_the_folder_is_still_going(self):
		"""One upload failing is enough to make the batch not the batch the user
		asked to run -- the pair they meant to include is not in it."""
		after_failure = self.result["afterPairFailed"]
		self.assertEqual(after_failure["runRequests"], 0)
		self.assertEqual(after_failure["batchesStillOpen"], 1, "the folder had already finished")
		self.assertFalse(after_failure["runButtonDisabled"], "there is no way back to Run from here")
		self.assertIn("upload failed", after_failure["runStatusText"].lower())

	def test_the_folder_finishing_does_not_resurrect_the_cancelled_run(self):
		"""The counting fix must not turn "the last upload landed" into a reason
		to start a run that was already called off."""
		self.assertEqual(self.result["batchesSent"], 3)
		self.assertEqual(self.result["runRequests"], 0)
		self.assertNotIn("/run", self.result["trail"])


if __name__ == "__main__":
	unittest.main(verbosity=2)
