#!/bin/bash
# Refresh the pipeline's reference databases on the EC2 host: CARD (RGI), MGEdb
# (MobileElementFinder), and AMRProt (the local AMR BLAST catalog).
#
# Install as a weekly cron job:
#   sudo crontab -e
#   0 3 * * 0  /home/ec2-user/bioinformatics/deploy/refresh-databases.sh >> /var/log/bioinformatics-db-refresh.log 2>&1
#
# WHY THIS REBUILDS THE IMAGE INSTEAD OF RUNNING daily_update_databases.py:
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

# Number of pipeline runs currently executing, per the app itself. Prints nothing
# when the app cannot be reached, which the caller treats as "unknown", never as
# "idle" -- guessing idle is exactly how you kill a run.
running_pipelines() {
	curl -fsS --max-time 10 "$HEALTH_URL" 2>/dev/null \
		| python3 -c 'import json,sys; print(json.load(sys.stdin)["pipelines"]["running"])' 2>/dev/null
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

log "waiting for in-flight runs to finish (timeout ${DRAIN_TIMEOUT_SECONDS}s)..."
deadline=$(( $(date +%s) + DRAIN_TIMEOUT_SECONDS ))
while :; do
	running="$(running_pipelines || true)"

	if [ "$running" = "0" ]; then
		log "no runs in flight; safe to restart."
		break
	fi

	if [ "$(date +%s)" -ge "$deadline" ]; then
		log "SKIPPING refresh: ${running:-an unknown number of} run(s) still in flight after ${DRAIN_TIMEOUT_SECONDS}s."
		log "Databases left unchanged; the new image is built and the next run will reuse it."
		exit 0  # trap lifts the drain
	fi

	if [ -z "$running" ]; then
		# Unreachable app. Do not assume idle -- wait and re-ask; if it stays
		# unreachable we hit the deadline above and skip, which is the safe end.
		log "app is not answering /api/health; will re-check."
	else
		log "waiting for $running run(s) to finish..."
	fi
	sleep "$DRAIN_POLL_SECONDS"
done

log "restarting service onto the new image..."
# The new container clears the drain flag on boot and starts whatever queued up
# while we were draining (see _reconcile_interrupted_runs in frontend.py).
trap - EXIT
sudo systemctl restart "$SERVICE"

log "done."
