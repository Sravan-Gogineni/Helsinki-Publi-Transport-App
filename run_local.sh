#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")" && pwd)"

if [ ! -f "$ROOT/.venv/bin/uvicorn" ]; then
  echo "Dependencies missing — run: python3 -m venv .venv --clear && .venv/bin/pip install -r backend/requirements.txt"
  exit 1
fi

source "$ROOT/.venv/bin/activate"

if [ -f "$ROOT/backend/.env" ]; then
  set -o allexport
  source "$ROOT/backend/.env"
  set +o allexport
fi

echo "Starting HFP dashboard at http://localhost:8080"

cd "$ROOT"
"$ROOT/.venv/bin/uvicorn" backend.app.main:app --host 0.0.0.0 --port 8080 --reload
