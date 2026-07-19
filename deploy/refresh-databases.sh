#!/bin/bash
# Refresh the pipeline's reference databases on the EC2 host: CARD (RGI), MGEdb
# (MobileElementFinder), and AMRProt (the local AMR BLAST catalog).
#
# Run weekly from deploy/bioinformatics-db-refresh.timer (install instructions in
# that unit, and in DATABASE_UPDATES.md). Not a crontab line: this host's clock is
# UTC, so `0 3 * * 0` fires at 20:00 Saturday Pacific -- a peak upload window --
# and the cron here has no CRON_TZ to correct it. The timer names the zone.
#
# WHY THIS REBUILDS THE IMAGE INSTEAD OF PIP-UPGRADING THE PACKAGES:
#
# Snakemake runs RGI and MobileElementFinder inside its OWN per-rule conda
# environments (.snakemake/conda/<hash>/, built from workflow/envs/rgi.yml and
# mefinder.yml). A `pip install --upgrade rgi` in the container's base or
# `bioinformatics` env installs into an interpreter the pipeline never invokes,
# so it would report success while changing nothing the analysis actually uses.
#
# Rebuilding the image re-solves those per-rule envs from the .yml files, which
# is what genuinely pulls the current released rgi + MobileElementFinder (and
# with them, the current CARD and MGEdb). Pin versions in workflow/envs/*.yml if
# you need reproducible databases across rebuilds instead of latest-on-rebuild.
#
# The rebuild also re-runs build_amr_blastdb.py, which re-downloads NCBI's
# AMRFinderPlus reference catalog (the local BLAST database blast_ncbi.py searches
# before it will consider a remote NCBI query). All three databases therefore refresh
# in this one pass.
#
# .snakemake/ and resources/blastdb/ are not on volumes, so all of this lives in the
# image itself: the image *is* the database. That is why a refresh necessarily means
# a new image, and a new image necessarily means a restart.
#
# WHY IT DRAINS FIRST:
#
# Snakemake runs as a child of the container, so the restart at the end kills any
# run in flight -- and a 33-step run with BV-BRC assembly can be hours long. This
# script therefore does not pick a quiet-looking window and hope. It puts the app
# into drain mode (new runs queue to disk instead of starting), lets the running
# ones finish, and only then restarts. If they do not finish within
# DRAIN_TIMEOUT_SECONDS it leaves the databases alone and exits: a weekly database
# refresh is never worth killing a multi-hour assembly, and skipping costs one
# cycle. The queued runs start by themselves once the new container boots.
#
# A run is not the only thing a restart can destroy. /submit and /import do their
# work inside the request that carries the reads -- staging, checksum verification
# and the S3 push all happen before the response -- so a restart under an upload
# loses it, and the user sees a dropped connection with only part of their batch
# registered. The wait below therefore covers uploads as well as runs. New uploads
# are still accepted while draining: refusing one would cost the user exactly what
# the drain is meant to protect, and if they keep arriving the timeout skips the
# refresh, which is the safe end.
#
# The build happens *during* the drain, not after it, so the wait is mostly free:
# the conda solve is slow, and in-flight runs use that time to finish.

set -euo pipefail

REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_DIR"

CONTAINER="${CONTAINER:-bioinformatics}"
SERVICE="${SERVICE:-bioinformatics}"
IMAGE_TAG="${IMAGE_TAG:-bioinformatics-pipeline:latest}"
HEALTH_URL="${HEALTH_URL:-http://127.0.0.1:5001/api/health}"
DRAIN_FLAG="/app/config/jobs/.drain"

# A run deep in a BV-BRC assembly can legitimately take hours. Past this, skip.
DRAIN_TIMEOUT_SECONDS="${DRAIN_TIMEOUT_SECONDS:-14400}"  # 4h
DRAIN_POLL_SECONDS="${DRAIN_POLL_SECONDS:-60}"

log() { echo "[$(date -u +%FT%TZ)] $*"; }

container_is_running() {
	[ "$(docker inspect -f '{{.State.Running}}' "$CONTAINER" 2>/dev/null || echo false)" = "true" ]
}

# What the app has in flight right now, as "<runs> <uploads>". Prints nothing when
# the app cannot be reached, which the caller treats as "unknown", never as
# "idle" -- guessing idle is exactly how you kill a run.
#
# Uploads count for the same reason runs do: /submit and /import stage, verify and
# push their reads to S3 inside the request, so restarting under one loses that
# upload outright. Waiting only for runs left that window wide open.
#
# `uploads` is read with a default because this script always drains the container
# it is about to replace -- which, on the deploy that first ships the field, is an
# older image whose /api/health does not report it. Missing then means 0, and the
# script behaves exactly as it did before.
in_flight_counts() {
	curl -fsS --max-time 10 "$HEALTH_URL" 2>/dev/null \
		| python3 -c 'import json,sys; d=json.load(sys.stdin); print(d["pipelines"]["running"], d.get("uploads",{}).get("in_flight",0))' 2>/dev/null
}

undrain() {
	docker exec "$CONTAINER" rm -f "$DRAIN_FLAG" >/dev/null 2>&1 || true
}

if ! container_is_running; then
	# Nothing to drain and nothing to kill: just refresh and let systemd bring it up.
	log "container '$CONTAINER' is not running; rebuilding without a drain."
	docker build -t "$IMAGE_TAG" .
	log "restarting service onto the new image..."
	sudo systemctl restart "$SERVICE"
	log "done."
	exit 0
fi

log "entering drain mode: new runs will queue instead of starting."
docker exec "$CONTAINER" touch "$DRAIN_FLAG"
# From here on, any exit that is not a successful restart has to lift the drain,
# or the app would queue runs forever and never start them.
trap 'undrain' EXIT

log "rebuilding image to refresh CARD/MGEdb (in-flight runs continue during the build)..."
docker build -t "$IMAGE_TAG" .

log "waiting for in-flight runs and uploads to finish (timeout ${DRAIN_TIMEOUT_SECONDS}s)..."
deadline=$(( $(date +%s) + DRAIN_TIMEOUT_SECONDS ))
while :; do
	counts="$(in_flight_counts || true)"
	running="${counts%% *}"
	uploading="${counts##* }"

	if [ "$running" = "0" ] && [ "$uploading" = "0" ]; then
		log "no runs or uploads in flight; safe to restart."
		break
	fi

	if [ "$(date +%s)" -ge "$deadline" ]; then
		log "SKIPPING refresh: ${running:-an unknown number of} run(s) and ${uploading:-an unknown number of} upload(s) still in flight after ${DRAIN_TIMEOUT_SECONDS}s."
		log "Databases left unchanged; the new image is built and the next run will reuse it."
		exit 0  # trap lifts the drain
	fi

	if [ -z "$counts" ]; then
		# Unreachable app. Do not assume idle -- wait and re-ask; if it stays
		# unreachable we hit the deadline above and skip, which is the safe end.
		log "app is not answering /api/health; will re-check."
	else
		log "waiting for $running run(s) and $uploading upload(s) to finish..."
	fi
	sleep "$DRAIN_POLL_SECONDS"
done

log "restarting service onto the new image..."
# The new container clears the drain flag on boot and starts whatever queued up
# while we were draining (see _reconcile_interrupted_runs in frontend.py).
trap - EXIT
sudo systemctl restart "$SERVICE"

log "done."
