# SINGLE worker: Background worker and webhook run in exactly one process.
workers = 1
worker_class = "gthread"
threads = 8
import os
bind = "0.0.0.0:" + os.environ.get("PORT", "10000")
timeout = 180
graceful_timeout = 30
accesslog = "-"
errorlog = "-"
