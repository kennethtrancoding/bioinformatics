#!/bin/bash
set -euo pipefail

source /opt/conda/etc/profile.d/conda.sh
conda activate bioinformatics

mkdir -p data/raw_fastq results logs config/jobs

exec gunicorn -c gunicorn.conf.py frontend:app
