"""Render/Gunicorn entry point: must run exactly ONE Gunicorn worker."""
import os
os.environ.setdefault("PUBLIC_URL", os.getenv("RENDER_EXTERNAL_URL", "").rstrip("/"))
from bot import app, startup
startup()
