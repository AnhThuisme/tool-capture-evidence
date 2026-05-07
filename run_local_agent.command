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

if [ ! -d ".venv" ]; then
  echo "[setup] creating virtual environment..."
  python3 -m venv .venv
fi

PY_BIN=".venv/bin/python"
PIP_BIN=".venv/bin/pip"

if [ ! -x "$PY_BIN" ]; then
  echo "[ERROR] virtual environment is broken (.venv/bin/python missing)."
  read -r -p "Press Enter to exit..." _
  exit 1
fi

echo "[setup] installing/updating dependencies..."
"$PIP_BIN" install -q --upgrade pip
"$PIP_BIN" install -q -r requirements.txt

export LOCAL_AGENT_PORT="${LOCAL_AGENT_PORT:-8765}"
export LOCAL_AGENT_ALLOWED_ORIGINS="${LOCAL_AGENT_ALLOWED_ORIGINS:-*}"

echo "[start] local agent => http://127.0.0.1:${LOCAL_AGENT_PORT}"
echo "[hint] keep this window open while using web deploy"
"$PY_BIN" -m uvicorn local_agent:app --host 127.0.0.1 --port "$LOCAL_AGENT_PORT"
