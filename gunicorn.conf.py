# Gunicorn config for serving the pipeline frontend in production.
#
# Run with:  gunicorn -c gunicorn.conf.py frontend:app
#
# Pipeline admission state and rate-limit counters are process-local, so this
# application must use one worker. Threads still allow concurrent HTTP requests.

import os

bind = f"0.0.0.0:{os.environ.get('PORT', '5001')}"
workers = 1
threads = 4

# Large FASTQ uploads (hundreds of MB) and long-polled status calls need a
# generous timeout so gunicorn does not kill the worker mid-upload.
timeout = 1800

accesslog = "-"  # log to stdout (captured by the container/service manager)
errorlog = "-"
loglevel = "info"


def post_worker_init(worker):
	"""Recover the job state the previous process died holding: mark runs killed
	mid-flight as failed, and restart runs that were queued when it went down.

	This runs in the worker, not the arbiter, because the queue and the running set
	it rebuilds are worker-local (which is also why `workers` above must stay 1)."""
	from frontend import run_startup_recovery

	run_startup_recovery()
