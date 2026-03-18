#!/bin/sh
set -eu

PORT_VALUE="${PORT:-8000}"
echo "[startup] launching uvicorn on port ${PORT_VALUE}"
exec python -m uvicorn web_ui:app --host 0.0.0.0 --port "${PORT_VALUE}"
