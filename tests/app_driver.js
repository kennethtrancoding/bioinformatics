"use strict";

// Runs the real static/app.js under Node against a stub DOM and a stub network,
// and reports what it did as JSON on stdout. tests/test_upload_concurrency.py is
// what makes the assertions.
//
// The two things being tested here have no server side to test: pressing Run
// while a folder is still going up records the intent in the browser rather than
// POSTing /run, and a second upload started on top of the first is folded into
// the same job by the browser's one job reservation. Neither is visible from
// Flask -- to the server a deferred run is simply a /run that arrives later, and
// a shared job is simply a job_id it was handed. So the assertions have to be
// made where the decision is: in app.js. The alternative, re-implementing the
// deferral in a test, would only ever prove the copy right.
//
// Nothing here is a browser. The DOM stub answers the handful of properties
// app.js touches, and the network stub hands back canned replies -- but the
// batching, the pairing, the retry loop, the deferral and the job reservation
// are the shipped code, running as written.

const fs = require("fs");
const path = require("path");
const vm = require("vm");

const REPO_ROOT = path.resolve(__dirname, "..");
const JOB_ID = "ABCDEFGHJKMN";
const GIGABYTE = 1024 * 1024 * 1024;
// app.js splits a folder at IMPORT_BATCH_BYTES (2 GB). That constant is a
// top-level `let` in its own script scope, so a test cannot lower it; instead
// each read claims to be 1.5 GB, which puts every 3 GB pair in a batch of its
// own. No bytes exist -- size is the only thing the batching reads.
const READ_BYTES = 1.5 * GIGABYTE;

// How long a scenario waits for the app to reach a state before calling it hung.
// Everything here resolves on the microtask queue, so a real wait is milliseconds;
// this bound only exists so a broken driver fails with a sentence rather than
// hanging a test run.
const WAIT_TIMEOUT_MS = 5000;

// ---------------------------------------------------------------- the DOM stub

class FakeElement {
	constructor(name) {
		this.name = name;
		this.textContent = "";
		this.value = "";
		this.href = "";
		this.hidden = false;
		this.disabled = false;
		this.files = [];
		this.children = [];
		this.classes = new Set();
		this.handlers = {};
		this.tbody = null;
		this.generatedChild = null;
		this.html = "";
		this.classList = {
			add: (name) => this.classes.add(name),
			remove: (name) => this.classes.delete(name),
			contains: (name) => this.classes.has(name),
		};
	}

	// renderJob builds rows as HTML and then hangs a Delete button off the last
	// cell, so a written-through innerHTML has to leave something to append to.
	get innerHTML() {
		return this.html;
	}

	set innerHTML(value) {
		this.html = value;
		this.generatedChild = null;
	}

	get lastElementChild() {
		if (!this.generatedChild) this.generatedChild = new FakeElement("generated");
		return this.generatedChild;
	}

	addEventListener(type, handler) {
		(this.handlers[type] = this.handlers[type] || []).push(handler);
	}

	click() {
		(this.handlers.click || []).forEach((handler) => handler.call(this, { preventDefault() {} }));
	}

	appendChild(child) {
		this.children.push(child);
		return child;
	}

	querySelector() {
		if (!this.tbody) this.tbody = new FakeElement("tbody");
		return this.tbody;
	}
}

const elementsById = new Map();
const elementsBySelector = new Map();

function byId(id) {
	// Cloud import ships off, so index.html does not render its fieldset and
	// app.js's null check is live code. Mirror that rather than inventing a
	// button the page does not have.
	if (id === "cloud-import-btn") return null;
	if (!elementsById.has(id)) elementsById.set(id, new FakeElement(id));
	return elementsById.get(id);
}

function bySelector(selector) {
	if (!elementsBySelector.has(selector)) elementsBySelector.set(selector, new FakeElement(selector));
	return elementsBySelector.get(selector);
}

// ------------------------------------------------------------ the network stub

// Every request app.js makes, in order. The order is half the point: a deferred
// run has to land after the last batch, not before it.
const requests = [];
// Import batches that have been sent and not yet answered -- the scenario decides
// when each one comes back, which is what "mid upload" means here.
const pendingImports = [];
// Isolates the fake server has registered, so /job/<id> answers the way a real
// one would and trimAlreadyRegistered() sees the truth on a retry.
const registeredIsolates = [];
// Set by a scenario that needs the pair upload refused. /submit reports a bad
// pair in the body rather than by status, which is what app.js reads.
let submitError = null;

function record(method, url, extra) {
	const entry = Object.assign({ method, url, index: requests.length }, extra || {});
	requests.push(entry);
	return entry;
}

function countRequests(method, url) {
	return requests.filter((entry) => entry.method === method && entry.url.split("?")[0] === url).length;
}

class FakeFormData {
	constructor() {
		this.entries = [];
	}

	append(name, value, fileName) {
		this.entries.push({ name, value, fileName });
	}

	get(name) {
		const entry = this.entries.find((candidate) => candidate.name === name);
		return entry ? entry.value : null;
	}

	names(name) {
		return this.entries.filter((entry) => entry.name === name).map((entry) => entry.fileName);
	}
}

function jsonResponse(status, body) {
	return {
		ok: status >= 200 && status < 300,
		status,
		json: () => Promise.resolve(body),
	};
}

function jobSnapshot() {
	return {
		job_id: JOB_ID,
		samples: registeredIsolates.map((isolateId) => ({
			isolate_id: isolateId,
			R1_path: `data/raw_fastq/${JOB_ID}/${isolateId}_R1_001.fastq.gz`,
			R2_path: `data/raw_fastq/${JOB_ID}/${isolateId}_R2_001.fastq.gz`,
		})),
		uploads: [],
		results: [],
		run_status: null,
		has_master_report: false,
	};
}

function fakeFetch(url, options) {
	const method = ((options || {}).method || "GET").toUpperCase();
	const routePath = url.split("?")[0];

	if (routePath === "/api/health") {
		record(method, url);
		return Promise.resolve(jsonResponse(200, { services: [], pipelines: {}, uploads: {} }));
	}
	if (routePath === "/job/new") {
		record(method, url);
		return Promise.resolve(jsonResponse(201, { job_id: JOB_ID }));
	}
	if (routePath.startsWith("/job/")) {
		record(method, url);
		return Promise.resolve(jsonResponse(200, jobSnapshot()));
	}
	if (routePath === "/submit") {
		const body = options.body;
		record(method, url, { jobId: body.get("job_id"), files: body.names("fastq_file_1") });
		if (submitError) return Promise.resolve(jsonResponse(200, { error: submitError }));
		const isolateId = "PAIRED";
		registeredIsolates.push(isolateId);
		return Promise.resolve(
			jsonResponse(200, {
				job_id: JOB_ID,
				isolate_id: isolateId,
				added: [isolateId],
				updated: [],
				upload: { method: "pair", label: "paired upload", seconds: 2.0 },
				auto_run: null,
			})
		);
	}
	if (routePath === "/run") {
		record(method, url, { jobId: options.body.get("job_id") });
		return Promise.resolve(
			jsonResponse(200, { message: "Pipeline started", job_id: JOB_ID, queued: false })
		);
	}
	if (routePath === "/status") {
		record(method, url);
		return Promise.resolve(
			jsonResponse(200, { done: false, queued: false, started_at: null, estimated_seconds: null })
		);
	}
	throw new Error(`the driver has no route for ${method} ${url}`);
}

// /import is the one route app.js sends over XHR rather than fetch, because it
// wants upload-progress events. Held open by default: the scenario answers each
// batch when it wants the next one to start.
class FakeXMLHttpRequest {
	constructor() {
		this.handlers = {};
		this.uploadHandlers = {};
		this.status = 0;
		this.responseText = "";
		this.upload = {
			addEventListener: (type, handler) => {
				this.uploadHandlers[type] = handler;
			},
		};
	}

	open(method, url) {
		this.method = method.toUpperCase();
		this.url = url;
	}

	addEventListener(type, handler) {
		this.handlers[type] = handler;
	}

	send(formData) {
		const isolateIds = formData
			.names("files")
			.filter((fileName) => /\.(fastq|fq)(\.gz)?$/i.test(fileName))
			.map((fileName) => path.basename(fileName).replace(/_R[12][_.].*$/, ""));
		const entry = record(this.method, this.url, {
			jobId: formData.get("job_id"),
			files: formData.names("files"),
			isolates: Array.from(new Set(isolateIds)),
		});
		pendingImports.push({
			entry,
			isolates: entry.isolates,
			// Answer this batch the way the server would have.
			succeed: () => {
				entry.isolates.forEach((isolateId) => registeredIsolates.push(isolateId));
				this.finish(200, {
					job_id: JOB_ID,
					added: entry.isolates,
					updated: [],
					verified: entry.isolates,
					failed: [],
					warnings: [],
					skipped: 0,
					checksum_source: null,
					upload: { method: "folder", label: "folder import", seconds: 1.5 },
				});
			},
			refuse: (status, error) => this.finish(status, { error }),
			drop: () => this.handlers.error(),
		});
	}

	finish(status, body) {
		// The bytes-are-up-but-the-server-is-still-working branch of the progress
		// handler is what the user stares at for most of a real batch, so drive it.
		const total = 1;
		if (this.uploadHandlers.progress) {
			this.uploadHandlers.progress({ lengthComputable: true, loaded: total, total });
		}
		this.status = status;
		this.responseText = JSON.stringify(body);
		this.handlers.load();
	}
}

// ------------------------------------------------------------------- the files

function fakeFile(relativePath, size) {
	return { name: path.posix.basename(relativePath), webkitRelativePath: relativePath, size };
}

function folderOf(isolateIds) {
	const files = [];
	isolateIds.forEach((isolateId) => {
		files.push(fakeFile(`Run/${isolateId}_R1_001.fastq.gz`, READ_BYTES));
		files.push(fakeFile(`Run/${isolateId}_R2_001.fastq.gz`, READ_BYTES));
	});
	return files;
}

function chooseFolder(isolateIds) {
	byId("import-folder").files = folderOf(isolateIds);
}

function choosePair(isolateId) {
	bySelector('input[name="fastq_file_1"]').files = [
		fakeFile(`${isolateId}_R1_001.fastq.gz`, READ_BYTES),
	];
	bySelector('input[name="fastq_file_2"]').files = [
		fakeFile(`${isolateId}_R2_001.fastq.gz`, READ_BYTES),
	];
}

// ----------------------------------------------------------------- the harness

function sleep(milliseconds) {
	return new Promise((resolve) => setTimeout(resolve, milliseconds));
}

async function waitFor(predicate, what) {
	const deadline = Date.now() + WAIT_TIMEOUT_MS;
	while (Date.now() < deadline) {
		if (predicate()) return;
		await sleep(5);
	}
	throw new Error(`timed out waiting for ${what}`);
}

// The app is quiet when it has stopped making requests and stopped waiting on
// one: used to prove a run did NOT start, which no positive event can announce.
async function settle() {
	const before = requests.length;
	await sleep(60);
	if (requests.length !== before) await settle();
}

function snapshot() {
	return {
		runButtonDisabled: byId("run").disabled,
		runStatusText: byId("run-status").textContent,
		importStatusText: byId("import-status").textContent,
		jobBannerText: byId("job-banner").textContent,
	};
}

function requestTrail() {
	return requests
		.filter((entry) => entry.url !== "/api/health" && !entry.url.startsWith("/job/" + JOB_ID))
		.map((entry) => entry.url.split("?")[0]);
}

function loadApp() {
	global.document = { getElementById: byId, createElement: (tag) => new FakeElement(tag), querySelector: bySelector };
	global.window = { location: { search: "" } };
	global.fetch = fakeFetch;
	global.XMLHttpRequest = FakeXMLHttpRequest;
	global.FormData = FakeFormData;
	global.alert = (message) => {
		byId("job-banner").textContent = "ALERT: " + message;
	};
	const source = fs.readFileSync(path.join(REPO_ROOT, "static", "app.js"), "utf8");
	vm.runInThisContext(source, { filename: "static/app.js" });
}

// ----------------------------------------------------------------- the scenarios

const scenarios = {
	// Run pressed while a three-batch folder is going up. The press must not
	// reach /run: it has to wait for the last batch, or the pipeline runs on
	// whatever landed by then and the job freezes around it.
	async runPressedMidImport() {
		chooseFolder(["MIDA", "MIDB", "MIDC"]);
		byId("import-btn").click();
		await waitFor(() => pendingImports.length === 1, "the first batch to be in flight");

		byId("run").click();
		await settle();
		const whilePressed = Object.assign(snapshot(), {
			runRequests: countRequests("POST", "/run"),
			batchesSent: countRequests("POST", "/import"),
			batchesStillOpen: pendingImports.length,
		});

		// Release the batches one at a time, checking after each that the run the
		// user asked for has still not been sent.
		const runRequestsPerBatch = [];
		for (let batchNumber = 1; batchNumber <= 3; batchNumber++) {
			await waitFor(() => pendingImports.length === batchNumber, `batch ${batchNumber} to be in flight`);
			pendingImports[batchNumber - 1].succeed();
			if (batchNumber < 3) {
				await waitFor(
					() => pendingImports.length === batchNumber + 1,
					`batch ${batchNumber + 1} to start`
				);
				runRequestsPerBatch.push(countRequests("POST", "/run"));
			}
		}

		await waitFor(() => countRequests("POST", "/run") === 1, "the deferred run to start");
		await settle();
		return {
			whilePressed,
			runRequestsAfterEachBatch: runRequestsPerBatch,
			batchesSent: countRequests("POST", "/import"),
			batchJobIds: requests.filter((entry) => entry.url === "/import").map((entry) => entry.jobId),
			jobReservations: countRequests("POST", "/job/new"),
			runRequests: countRequests("POST", "/run"),
			trail: requestTrail(),
			final: snapshot(),
		};
	},

	// Same press, but the folder never finishes. The queued run has to be dropped:
	// honouring it would freeze the job around a batch that is missing samples.
	async runPressedMidImportThatFails() {
		chooseFolder(["FAILA", "FAILB"]);
		byId("import-btn").click();
		await waitFor(() => pendingImports.length === 1, "the first batch to be in flight");

		byId("run").click();
		await settle();

		pendingImports[0].succeed();
		await waitFor(() => pendingImports.length === 2, "the second batch to start");
		// 400, not 500: a verdict about the data, which app.js must not retry.
		pendingImports[1].refuse(400, "R1 checksum mismatch: FAILB");
		await settle();

		return {
			batchesSent: countRequests("POST", "/import"),
			runRequests: countRequests("POST", "/run"),
			trail: requestTrail(),
			final: snapshot(),
		};
	},

	// A pair chosen and added while the folder is still going up. Both uploads
	// have to land in the one job the first of them reserved.
	async pairAddedDuringImport() {
		chooseFolder(["FOLDA", "FOLDB"]);
		byId("import-btn").click();
		await waitFor(() => pendingImports.length === 1, "the first batch to be in flight");

		choosePair("PAIRED");
		byId("submit").click();
		await waitFor(() => countRequests("POST", "/submit") === 1, "the pair to be uploaded");
		await settle();
		const whileImportOpen = {
			batchesStillOpen: pendingImports.length,
			batchesSent: countRequests("POST", "/import"),
			submitJobId: requests.find((entry) => entry.url === "/submit").jobId,
			jobReservations: countRequests("POST", "/job/new"),
		};

		for (let batchNumber = 1; batchNumber <= 2; batchNumber++) {
			await waitFor(() => pendingImports.length === batchNumber, `batch ${batchNumber} to be in flight`);
			pendingImports[batchNumber - 1].succeed();
		}
		await settle();

		return {
			whileImportOpen,
			batchesSent: countRequests("POST", "/import"),
			batchJobIds: requests.filter((entry) => entry.url === "/import").map((entry) => entry.jobId),
			jobReservations: countRequests("POST", "/job/new"),
			runRequests: countRequests("POST", "/run"),
			trail: requestTrail(),
			final: snapshot(),
		};
	},

	// Both at once: Run pressed during the folder upload, then a pair added on
	// top of it. The pair finishing is not the folder finishing, so the run must
	// still wait for the last batch.
	async runPressedThenPairAddedDuringImport() {
		chooseFolder(["BOTHA", "BOTHB", "BOTHC"]);
		byId("import-btn").click();
		await waitFor(() => pendingImports.length === 1, "the first batch to be in flight");

		byId("run").click();
		await settle();

		choosePair("PAIRED");
		byId("submit").click();
		await waitFor(() => countRequests("POST", "/submit") === 1, "the pair to be uploaded");
		await settle();
		const afterPairLanded = Object.assign(snapshot(), {
			runRequests: countRequests("POST", "/run"),
			batchesSent: countRequests("POST", "/import"),
			batchesStillOpen: pendingImports.length,
			isolatesSentSoFar: requests
				.filter((entry) => entry.url === "/import")
				.reduce((all, entry) => all.concat(entry.isolates), []),
		});

		for (let batchNumber = 1; batchNumber <= 3; batchNumber++) {
			await waitFor(() => pendingImports.length === batchNumber, `batch ${batchNumber} to be in flight`);
			pendingImports[batchNumber - 1].succeed();
		}
		await settle();

		return {
			afterPairLanded,
			batchesSent: countRequests("POST", "/import"),
			runRequests: countRequests("POST", "/run"),
			trail: requestTrail(),
			final: snapshot(),
		};
	},
	// Run pressed during the folder, then a pair added on top of it that the
	// server refuses. The batch the user asked to run is now missing that pair,
	// so the run has to be dropped -- and must not come back when the folder,
	// which is still going, finishes successfully.
	async runPressedThenAPairThatFailsDuringImport() {
		submitError = "R1 checksum mismatch: PAIRED";
		chooseFolder(["DROPA", "DROPB", "DROPC"]);
		byId("import-btn").click();
		await waitFor(() => pendingImports.length === 1, "the first batch to be in flight");

		byId("run").click();
		await settle();

		choosePair("PAIRED");
		byId("submit").click();
		await waitFor(() => countRequests("POST", "/submit") === 1, "the pair to be refused");
		await settle();
		const afterPairFailed = Object.assign(snapshot(), {
			runRequests: countRequests("POST", "/run"),
			batchesStillOpen: pendingImports.length,
		});

		for (let batchNumber = 1; batchNumber <= 3; batchNumber++) {
			await waitFor(() => pendingImports.length === batchNumber, `batch ${batchNumber} to be in flight`);
			pendingImports[batchNumber - 1].succeed();
		}
		await settle();

		return {
			afterPairFailed,
			batchesSent: countRequests("POST", "/import"),
			runRequests: countRequests("POST", "/run"),
			trail: requestTrail(),
			final: snapshot(),
		};
	},
};

async function main() {
	const name = process.argv[2];
	if (!scenarios[name]) {
		process.stderr.write(`unknown scenario: ${name}\n`);
		process.exit(2);
	}
	loadApp();
	// loadHealth() fires on load and the page is not ready until it has answered.
	await settle();
	const result = await scenarios[name]();
	process.stdout.write(JSON.stringify(result));
	// app.js leaves a status poll and a run ticker on setInterval; nothing here
	// waits on them, so end rather than idling until they are noticed.
	process.exit(0);
}

main().catch((error) => {
	process.stderr.write((error && error.stack) || String(error));
	process.exit(1);
});
