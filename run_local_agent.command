#!/bin/bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$ROOT_DIR"

if ! command -v python3 >/dev/null 2>&1; then
  echo "[ERROR] python3 is not installed."
  echo "Install Python 3, then re-run this file."
  read -r -p "Press Enter to exit..." _
  exit 1
fi

PY_BIN=".venv/bin/python"
PIP_BIN=".venv/bin/pip"

echo "[setup] checking virtual environment..."
if [ ! -x "$PY_BIN" ]; then
  echo "[setup] rebuilding .venv..."
  rm -rf .venv
  python3 -m venv .venv
fi

if [ ! -x "$PY_BIN" ]; then
  echo "[ERROR] failed to create virtual environment."
  read -r -p "Press Enter to exit..." _
  exit 1
fi

echo "[setup] installing/updating dependencies..."
"$PY_BIN" -m pip install -q --upgrade pip
"$PIP_BIN" install -q -r requirements.txt

export LOCAL_AGENT_PORT="${LOCAL_AGENT_PORT:-8765}"
export LOCAL_AGENT_ALLOWED_ORIGINS="${LOCAL_AGENT_ALLOWED_ORIGINS:-*}"
CHROME_DEBUG_PORT="${CHROME_DEBUG_PORT:-9223}"

if [ "${LAUNCH_CHROME_ON_START:-1}" = "1" ]; then
  echo "[start] launching Chrome debug on port ${CHROME_DEBUG_PORT}..."
  open -na "Google Chrome" --args --remote-debugging-port="${CHROME_DEBUG_PORT}" --user-data-dir="$HOME/.chrome-debug-evidence" >/dev/null 2>&1 || true
fi

echo "[start] local agent => http://127.0.0.1:${LOCAL_AGENT_PORT}"
echo "[hint] keep this window open while using web deploy"
"$PY_BIN" -m uvicorn local_agent:app --host 127.0.0.1 --port "$LOCAL_AGENT_PORT"
