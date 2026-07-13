var STATUS_LABEL = { up: "● Operational", degraded: "● Degraded", down: "● Down" };

function escapeHtml(value) {
	return String(value === null || value === undefined ? "" : value)
		.replaceAll("&", "&amp;")
		.replaceAll("<", "&lt;")
		.replaceAll(">", "&gt;")
		.replaceAll('"', "&quot;")
		.replaceAll("'", "&#39;");
}

function loadHealth() {
	var status = document.getElementById("health-status");
	status.textContent = "Checking services…";
	var healthUrl = "/api/health";
	if (currentJobId) healthUrl += "?job_id=" + encodeURIComponent(currentJobId);
	return fetch(healthUrl)
		.then(function (r) {
			return r.json();
		})
		.then(function (data) {
			var table = document.getElementById("health-table");
			var body = table.querySelector("tbody");
			body.innerHTML = "";
			data.services.forEach(function (s) {
				var row = document.createElement("tr");
				row.innerHTML =
					"<td><b>" +
					escapeHtml(s.name) +
					"</b> <small>(" +
					escapeHtml(s.group) +
					")</small><br><small>" +
					escapeHtml(s.url) +
					"</small></td>" +
					'<td><span class="badge ' +
					(s.status === "up" || s.status === "down" || s.status === "degraded"
						? s.status
						: "down") +
					'">' +
					escapeHtml(STATUS_LABEL[s.status] || s.status) +
					"</span></td>" +
					"<td>" +
					escapeHtml(s.code === null ? "—" : s.code) +
					"</td>" +
					"<td>" +
					escapeHtml(s.latency_ms) +
					" ms</td>" +
					"<td>" +
					escapeHtml(s.detail) +
					"</td>";
				body.appendChild(row);
			});
			table.hidden = false;
			status.textContent = "Last checked just now.";
		})
		.catch(function () {
			status.textContent = "Health check failed to run.";
		});
}

document.getElementById("refresh-health").addEventListener("click", loadHealth);
loadHealth();

function basename(p) {
	return p ? p.split("/").pop() : "";
}

function formatDuration(seconds) {
	if (seconds === null || seconds === undefined || !isFinite(seconds) || seconds < 0) {
		return "";
	}

	const hours = Math.floor(seconds / 3600);
	const minutes = Math.floor((seconds % 3600) / 60);
	const secs = Math.floor(seconds % 60);

	return `${String(hours).padStart(2, "0")}:${String(minutes).padStart(2, "0")}:${String(secs).padStart(2, "0")}`;
}

// A run's elapsed time has to keep moving between the 3s status polls,
// so it is recomputed locally each second from the start time the
// server gave us.
var currentRunStatus = null;
var runTicker = null;

function runningRunText(runStatus) {
	var elapsed = runStatus.started_at ? Date.now() / 1000 - runStatus.started_at : null;
	var elapsedText = formatDuration(elapsed);
	return "Pipeline running…" + (elapsedText ? " " + elapsedText + " elapsed" : "");
}

function startRunTicker() {
	if (runTicker) return;
	runTicker = setInterval(function () {
		if (!currentRunStatus || currentRunStatus.done || currentRunStatus.queued) return;
		document.getElementById("run-status").textContent = runningRunText(currentRunStatus);
	}, 1000);
}

function stopRunTicker() {
	if (runTicker) {
		clearInterval(runTicker);
		runTicker = null;
	}
}

// Renders a run_status object ({done, success, error, started_at,
// finished_at}) into a status element, making a crash impossible to
// mistake for "still running" or for success -- flagged in red, with
// whatever detail the server could pull from the pipeline log. A run
// that is going shows how long it has been going; one that finished
// shows how long it took.
function showRunStatus(el, runStatus) {
	let runPipelineButton = document.getElementById("run");
	currentRunStatus = runStatus;
	el.classList.remove("error-text");
	runPipelineButton.hidden = false;

	if (!runStatus.done) {
		// A queued run is admitted but waiting for a free slot -- say
		// so, rather than claiming it is running when it has not started.
		// It has no start time yet, so it has no elapsed time either.
		el.textContent = runStatus.queued
			? "Queued (position " +
				runStatus.queue_position +
				"); waiting for a free pipeline slot…"
			: runningRunText(runStatus);
		el.classList.remove("error-text");

		runPipelineButton.hidden = true;
		startRunTicker();
		return;
	}

	stopRunTicker();
	var took =
		runStatus.started_at && runStatus.finished_at
			? " Took " + formatDuration(runStatus.finished_at - runStatus.started_at) + "."
			: "";
	if (runStatus.success) {
		el.textContent = "Pipeline complete." + took;
		el.classList.remove("error-text");

		runPipelineButton.hidden = true;
		return;
	}
	el.textContent = "Pipeline CRASHED at " + took;
	el.classList.add("error-text");
}

// Every upload that fed this job, and what each one cost. A job can be
// filled by several uploads through several methods, so this is a list.
function renderUploads(uploads) {
	var el = document.getElementById("upload-summary");
	if (!uploads || !uploads.length) {
		el.textContent = "";
		return;
	}
	var totalSeconds = uploads.reduce(function (sum, upload) {
		return sum + (upload.seconds || 0);
	}, 0);
	var descriptions = uploads.map(function (upload) {
		var sampleCount = (upload.added || []).length + (upload.updated || []).length;
		return (
			upload.label +
			" (" +
			sampleCount +
			" sample" +
			(sampleCount === 1 ? "" : "s") +
			", " +
			formatDuration(upload.seconds) +
			")"
		);
	});
	el.textContent =
		"Uploads: " +
		uploads.length +
		", " +
		formatDuration(totalSeconds) +
		" total: " +
		descriptions.join("; ");
}

// Reserve the batch before uploading so simultaneous Add actions through
// different methods all receive the same job ID.
var currentJobId = null;
var pendingJobReservation = null;

function ensureUploadJob() {
	if (currentJobId) return Promise.resolve(currentJobId);
	if (!pendingJobReservation) {
		pendingJobReservation = fetch("/job/new", { method: "POST" })
			.then(function (response) {
				return response.json().then(function (data) {
					if (!response.ok) throw new Error(data.error || "Could not create job.");
					currentJobId = data.job_id;
					return currentJobId;
				});
			})
			.finally(function () {
				pendingJobReservation = null;
			});
	}
	return pendingJobReservation;
}

function addBatchFields(formData) {
	return ensureUploadJob().then(function (jobId) {
		formData.append("job_id", jobId);
		return formData;
	});
}

// The job currently shown in the "Your Batch" panel -- whatever was
// last submitted/imported/looked up. All actions in that panel
// (Run, Download All, per-sample Delete) act on this job.
var runPollInterval = null;

function renderJob(jobId, data) {
	currentJobId = jobId;
	var settingsUrl = "/settings?job_id=" + encodeURIComponent(jobId);
	document.getElementById("settings-link").href = settingsUrl;
	document.getElementById("health-settings-link").href = settingsUrl;
	document.getElementById("job-view").hidden = false;
	document.getElementById("job-view-id").textContent = jobId;
	renderUploads(data.uploads);

	var sTable = document.getElementById("samples-table");
	var sBody = sTable.querySelector("tbody");
	var sEmpty = document.getElementById("samples-empty");
	sBody.innerHTML = "";
	if (!data.samples.length) {
		sTable.hidden = true;
		sEmpty.hidden = false;
	} else {
		data.samples.forEach(function (s) {
			var row = document.createElement("tr");
			var f1 = basename(s.R1_path);
			var f2 = basename(s.R2_path);
			row.innerHTML =
				"<td><b>" +
				escapeHtml(s.isolate_id) +
				"</b></td>" +
				"<td><small>" +
				escapeHtml(f1) +
				"</small></td>" +
				"<td><small>" +
				escapeHtml(f2) +
				"</small></td><td></td>";
			var deleteButton = document.createElement("button");
			deleteButton.textContent = "Delete";
			deleteButton.addEventListener("click", function () {
				deletePair(f1, f2, deleteButton);
			});
			row.lastElementChild.appendChild(deleteButton);
			sBody.appendChild(row);
		});
		sTable.hidden = false;
		sEmpty.hidden = true;
	}

	var rTable = document.getElementById("results-table");
	var rBody = rTable.querySelector("tbody");
	var rEmpty = document.getElementById("results-empty");
	rBody.innerHTML = "";
	if (!data.results.length) {
		rTable.hidden = true;
		rEmpty.hidden = false;
	} else {
		data.results.forEach(function (res) {
			var row = document.createElement("tr");
			var viewCell = res.has_report
				? '<a href="/results/' +
					jobId +
					"/" +
					encodeURIComponent(res.isolate_id) +
					'/view" target="_blank">View Report</a>'
				: "<span><small>No report yet</small></span>";
			row.innerHTML =
				"<td><b>" +
				escapeHtml(res.isolate_id) +
				"</b></td>" +
				"<td>" +
				viewCell +
				' &nbsp;|&nbsp; <a href="/results/' +
				jobId +
				"/" +
				encodeURIComponent(res.isolate_id) +
				'/download">Download</a></td>';
			rBody.appendChild(row);
		});
		rTable.hidden = false;
		rEmpty.hidden = true;
	}

	document.getElementById("download-master-report").hidden = !data.has_master_report;

	var runStatus = document.getElementById("run-status");
	var runBtn = document.getElementById("run");
	var abortBtn = document.getElementById("abort");
	if (runPollInterval) {
		clearInterval(runPollInterval);
		runPollInterval = null;
	}
	if (data.run_status) {
		runBtn.disabled = !data.run_status.done;
		abortBtn.hidden = data.run_status.done;
		abortBtn.disabled = false;
		showRunStatus(runStatus, data.run_status);
		if (!data.run_status.done) {
			pollRunStatus(jobId);
		}
	} else {
		stopRunTicker();
		currentRunStatus = null;
		runBtn.disabled = false;
		abortBtn.hidden = true;
		runStatus.textContent = "";
		runStatus.classList.remove("error-text");
	}
}

function fetchJob(jobId) {
	return fetch("/job/" + encodeURIComponent(jobId)).then(function (r) {
		return r.json().then(function (d) {
			return { ok: r.ok, status: r.status, d: d };
		});
	});
}

document.getElementById("lookup-btn").addEventListener("click", function () {
	var jobId = document.getElementById("lookup-job-id").value.trim().toUpperCase();
	var statusText = document.getElementById("lookup-status");
	if (!jobId) {
		status.textContent = "Enter a job ID first.";
		return;
	}
	statusText.textContent = "Looking up…";

	fetchJob(jobId)
		.then(function (res) {
			if (res.status === 429) {
				statusText.textContent = "Too many lookups; wait a moment and try again.";
			} else if (res.status === 400 || res.status === 404) {
				statusText.textContent = "Job not found.";
			} else if (!res.ok) {
				statusText.textContent = "Lookup failed to run.";
			} else {
				statusText.textContent = "";
				renderJob(jobId, res.d);
			}
		})
		.catch(function (error) {
			statusText.textContent = "Lookup failed to run.";
		});
});

document.getElementById("download-all-results").addEventListener("click", function () {
	if (currentJobId) {
		window.location = "/results/" + currentJobId + "/download-all";
	}
});

document.getElementById("download-master-report").addEventListener("click", function () {
	if (currentJobId) {
		window.location = "/results/" + currentJobId + "/master-report/download";
	}
});

// The outcome of an import, however the files got here: what landed in
// the manifest, what was verified against the sequencing company's
// checksums, and what was left out and why.
function importSummary(d) {
	var msg =
		"Job ID: " +
		d.job_id +
		". Save this to check your results later. (" +
		d.added.length +
		" added, " +
		d.updated.length +
		" updated.";
	if (d.upload) {
		msg += " Uploaded in " + formatDuration(d.upload.seconds) + ".";
	}
	if (d.checksum_source) {
		msg +=
			" Checksums (" +
			d.checksum_source +
			"): " +
			d.verified.length +
			" verified" +
			(d.failed.length ? ", " + d.failed.length + " FAILED" : "") +
			".";
	}
	if (d.skipped) {
		msg += " " + d.skipped + " skipped (" + d.warnings.join("; ") + ")";
	}
	return msg + ")";
}

// After any upload, point the batch options at the job it landed in, so
// the next upload -- by any method -- adds to it rather than opening a
// second job the user then has to reconcile.
function afterUpload(jobId) {
	currentJobId = jobId;
	return fetchJob(jobId).then(function (res) {
		if (res.ok) renderJob(jobId, res.d);
	});
}

document.getElementById("import-btn").addEventListener("click", async function () {
	var status = document.getElementById("import-status");
	var files = document.getElementById("import-folder").files;
	if (!files.length) {
		status.textContent = "Choose a folder first.";
		return;
	}
	var formData = new FormData();
	for (var i = 0; i < files.length; i++) {
		var f = files[i];
		formData.append("files", f, f.webkitRelativePath || f.name);
	}
	try {
		formData = await addBatchFields(formData);
	} catch (error) {
		status.textContent = error.message;
		return;
	}

	status.textContent =
		"Uploading " + files.length + " file(s)… this can take a while for large folders.";
	fetch("/import", { method: "POST", body: formData })
		.then(function (r) {
			return r.json().then(function (d) {
				return { ok: r.ok, d: d };
			});
		})
		.then(function (res) {
			if (!res.ok) {
				status.textContent = "Error: " + res.d.error;
				return;
			}
			status.textContent = importSummary(res.d);
			afterUpload(res.d.job_id);
		})
		.catch(function () {
			status.textContent = "Import failed to run.";
		});
});

/* Cloud import is disabled; the fieldset it drives is commented out in
			   templates/index.html and the /cloud-import routes are commented out in
			   frontend.py. Restore all three together.

			// A cloud pull can run for a long time (it is the server, not the
			// browser, doing the downloading), so /cloud-import hands back a job ID
			// immediately and the progress arrives by polling.
			function pollCloudImport(jobId, button) {
				var status = document.getElementById("cloud-status");
				var poll = setInterval(function () {
					fetch("/cloud-import/status?job_id=" + encodeURIComponent(jobId))
						.then(function (r) {
							return r.json();
						})
						.then(function (data) {
							if (data.state === "running") {
								status.textContent = data.message || "Importing…";
								return;
							}
							clearInterval(poll);
							button.disabled = false;
							if (data.state !== "done") {
								status.textContent =
									"Error: " + (data.error || "The import failed.");
								status.classList.add("error-text");
								return;
							}
							status.classList.remove("error-text");
							status.textContent = importSummary(data.result);
							afterUpload(jobId);
						})
						.catch(function () {
							clearInterval(poll);
							button.disabled = false;
							status.textContent = "Lost contact with the server during the import.";
							status.classList.add("error-text");
						});
				}, 2000);
			}

			document
				.getElementById("cloud-import-btn")
				.addEventListener("click", async function () {
					var status = document.getElementById("cloud-status");
					var button = this;
					var shareUrl = document.getElementById("cloud-url").value.trim();
					if (!shareUrl) {
						status.textContent = "Paste a share link first.";
						return;
					}

					var formData = new FormData();
					formData.append("share_url", shareUrl);
					try {
						formData = await addBatchFields(formData);
					} catch (error) {
						status.textContent = error.message;
						return;
					}

					button.disabled = true;
					status.classList.remove("error-text");
					status.textContent = "Reading the shared folder…";

					fetch("/cloud-import", { method: "POST", body: formData })
						.then(function (r) {
							return r.json().then(function (d) {
								return { ok: r.ok, d: d };
							});
						})
						.then(function (res) {
							if (!res.ok) {
								status.textContent = "Error: " + res.d.error;
								status.classList.add("error-text");
								button.disabled = false;
								return;
							}
							pollCloudImport(res.d.job_id, button);
						})
						.catch(function () {
							status.textContent = "Import failed to start.";
							status.classList.add("error-text");
							button.disabled = false;
						});
				});
			*/

document.getElementById("submit").addEventListener("click", async function (event) {
	event.preventDefault();

	var fileInput1 = document.querySelector('input[name="fastq_file_1"]');
	var fileInput2 = document.querySelector('input[name="fastq_file_2"]');

	var checksum1 = document.querySelector('input[name="fastq_file_1_checksum"]').value.trim();
	var checksum2 = document.querySelector('input[name="fastq_file_2_checksum"]').value.trim();

	if (fileInput1.files.length > 0 && fileInput2.files.length > 0) {
		var file1 = fileInput1.files[0];
		var file2 = fileInput2.files[0];

		var formData = new FormData();
		formData.append("fastq_file_1", file1);
		formData.append("fastq_file_2", file2);
		formData.append("fastq_file_1_checksum", checksum1);
		formData.append("fastq_file_2_checksum", checksum2);
		try {
			formData = await addBatchFields(formData);
		} catch (error) {
			alert(error.message);
			return;
		}

		fetch("/submit", { method: "POST", body: formData })
			.then((res) => res.json())
			.then((data) => {
				if (data.error) {
					alert(data.error);
					return;
				}

				document.getElementById("job-banner").textContent =
					"Job ID: " +
					data.job_id +
					". Save this to check your results later. Uploaded " +
					data.isolate_id +
					" in " +
					formatDuration(data.upload.seconds) +
					".";
				afterUpload(data.job_id);

				fileInput1.value = "";
				fileInput2.value = "";
			});
	} else {
		alert("Please select both FASTQ files before submitting.");
	}
});

function deletePair(f1, f2, btn) {
	if (!currentJobId) return;
	fetch("/delete", {
		method: "DELETE",
		headers: { "Content-Type": "application/json" },
		body: JSON.stringify({ job_id: currentJobId, files: [f1, f2] }),
	}).then(function () {
		fetchJob(currentJobId).then(function (res) {
			if (res.ok) renderJob(currentJobId, res.d);
		});
	});
}

function pollRunStatus(jobId) {
	runPollInterval = setInterval(function () {
		fetch("/status?job_id=" + encodeURIComponent(jobId))
			.then(function (r) {
				return r.json();
			})
			.then(function (data) {
				if (data.error) {
					clearInterval(runPollInterval);
					runPollInterval = null;
					return;
				}
				// Re-render every tick, not just at the end, so a queued
				// run's position counts down live as slots free up.
				showRunStatus(document.getElementById("run-status"), data);
				if (data.done) {
					clearInterval(runPollInterval);
					runPollInterval = null;
					document.getElementById("run").disabled = false;
					document.getElementById("abort").hidden = true;
					loadHealth();
					fetchJob(jobId).then(function (res) {
						if (res.ok) renderJob(jobId, res.d);
					});
				}
			});
	}, 3000);
}

document.getElementById("run").addEventListener("click", function () {
	if (!currentJobId) return;
	var btn = this;
	var abortBtn = document.getElementById("abort");
	var status = document.getElementById("run-status");

	btn.disabled = true;

	status.textContent = "Starting pipeline…";

	var formData = new FormData();
	formData.append("job_id", currentJobId);
	formData.append("username", document.getElementById("username").value);
	formData.append("password", document.getElementById("password").value);

	fetch("/run", { method: "POST", body: formData })
		.then((res) => res.json())
		.then((data) => {
			if (data.error) {
				status.textContent = "Error: " + data.error;
				btn.disabled = false;
				return;
			}
			// /run returns 202 + queued when every slot is busy.
			status.textContent = data.queued
				? "Queued (position " + data.queue_position + "); waiting for a free pipeline slot…"
				: "Pipeline running…";
			abortBtn.hidden = false;
			abortBtn.disabled = false;
			pollRunStatus(currentJobId);
		})
		.catch(() => {
			status.textContent = "Failed to contact server.";
			btn.disabled = false;
		});
});

document.getElementById("abort").addEventListener("click", function () {
	if (!currentJobId) return;
	var btn = this;
	var status = document.getElementById("run-status");

	btn.disabled = true;
	status.textContent = "Aborting…";

	var formData = new FormData();
	formData.append("job_id", currentJobId);

	fetch("/abort", { method: "POST", body: formData })
		.then((res) => res.json())
		.then((data) => {
			if (data.error) {
				status.textContent = "Error: " + data.error;
				btn.disabled = false;
			}
			// On success, leave the poll loop (already running from
			// the Run click) to notice run_status.done and hide this
			// button/re-enable Run once the process actually exits.
		})
		.catch(() => {
			status.textContent = "Failed to contact server.";
			btn.disabled = false;
		});
});

// Coming back from the settings page. The batch panel lives in memory, so
// without this a trip to API Settings and back would land on an empty page
// and the user would have to re-enter the job ID they just came from.
// /?job_id=XXXX reopens that batch exactly as a lookup would.
(function reopenJobFromUrl() {
	var requestedJobId = (new URLSearchParams(window.location.search).get("job_id") || "")
		.trim()
		.toUpperCase();
	if (!requestedJobId) return;
	fetchJob(requestedJobId).then(function (res) {
		if (!res.ok) return; // expired or bogus: leave the page as it is
		document.getElementById("lookup-job-id").value = requestedJobId;
		renderJob(requestedJobId, res.d);
	});
})();
