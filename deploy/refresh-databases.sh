#!/bin/bash
# Refresh the pipeline's reference databases (CARD for RGI, MGEdb for
# MobileElementFinder) on the EC2 host.
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
# The rebuild is slow (conda solves) but happens off to the side: the running
# container keeps serving until the restart at the end.

set -euo pipefail

REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_DIR"

echo "[$(date -u +%FT%TZ)] rebuilding image to refresh CARD/MGEdb..."
docker build -t bioinformatics-pipeline:latest .

echo "[$(date -u +%FT%TZ)] restarting service onto the new image..."
# NOTE: this drops any pipeline run currently in flight -- the Snakemake process
# is a child of the container. Scheduled for Sunday 03:00 for that reason;
# move it to a window when your queue is reliably empty.
sudo systemctl restart bioinformatics

echo "[$(date -u +%FT%TZ)] done."
