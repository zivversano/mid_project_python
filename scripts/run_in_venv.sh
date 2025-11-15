#!/usr/bin/env bash
# Run a command using the project's .venv python interpreter.
# Usage: ./scripts/run_in_venv.sh path/to/script.py [args...]
set -euo pipefail
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
VENV_PY="$ROOT_DIR/.venv/bin/python"
if [ ! -x "$VENV_PY" ]; then
  echo "Virtualenv python not found at $VENV_PY"
  echo "Create the venv or adjust the path in this script." >&2
  exit 1
fi

"$VENV_PY" "$@"
