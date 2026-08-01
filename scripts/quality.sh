#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

if [[ $# -gt 1 ]]; then
  printf 'usage: %s [--print-env]\n' "$0" >&2
  exit 2
fi
case "${1:-}" in
  "")
    QUALITY_PRINT_ENV_ONLY=0
    ;;
  --print-env)
    QUALITY_PRINT_ENV_ONLY=1
    ;;
  *)
    printf 'usage: %s [--print-env]\n' "$0" >&2
    exit 2
    ;;
esac

PYTHON_BIN="${PYTHON_BIN:-python3}"
CONSTRAINTS_FILE="$ROOT_DIR/constraints/ci.txt"
export PIP_CONSTRAINT="${PIP_CONSTRAINT:-$CONSTRAINTS_FILE}"

if [[ -n "${MAL_UPDATER_QUALITY_TMP:-}" ]]; then
  QUALITY_TMP="$MAL_UPDATER_QUALITY_TMP"
else
  QUALITY_TMP="$(mktemp -d /tmp/mal-updater-quality.XXXXXX)"
fi
if [[ "${MAL_UPDATER_QUALITY_ALLOW_AMBIENT:-0}" == "1" ]]; then
  QUALITY_RUNTIME="${MAL_UPDATER_RUNTIME_ROOT:-${MAL_UPDATER_RUNTIME_DIR:-$QUALITY_TMP/runtime}}"
  QUALITY_SETTINGS="${MAL_UPDATER_SETTINGS_PATH:-${MAL_UPDATER_CONFIG:-$QUALITY_RUNTIME/config/settings.toml}}"
else
  QUALITY_RUNTIME="$QUALITY_TMP/runtime"
  QUALITY_SETTINGS="$QUALITY_RUNTIME/config/settings.toml"
fi

export MAL_UPDATER_RUNTIME_DIR="$QUALITY_RUNTIME"
export MAL_UPDATER_RUNTIME_ROOT="$QUALITY_RUNTIME"
export MAL_UPDATER_CONFIG="$QUALITY_SETTINGS"
export MAL_UPDATER_SETTINGS_PATH="$QUALITY_SETTINGS"
export TMPDIR="$QUALITY_TMP/tmp"

mkdir -p "$(dirname "$MAL_UPDATER_SETTINGS_PATH")" "$TMPDIR"
: > "$MAL_UPDATER_SETTINGS_PATH"

printf 'quality_runtime=%s\n' "$MAL_UPDATER_RUNTIME_ROOT"
printf 'quality_settings=%s\n' "$MAL_UPDATER_SETTINGS_PATH"
printf 'quality_tmp=%s\n' "$TMPDIR"

if [[ "$QUALITY_PRINT_ENV_ONLY" == "1" ]]; then
  exit 0
fi

"$PYTHON_BIN" -m ruff check conftest.py src tests scripts/check_distribution.py
"$PYTHON_BIN" -m mypy
"$PYTHON_BIN" -m coverage erase
"$PYTHON_BIN" -m coverage run -m pytest -q
"$PYTHON_BIN" -m coverage report

rm -rf dist
"$PYTHON_BIN" -m build --sdist --wheel --outdir dist
"$PYTHON_BIN" scripts/check_distribution.py dist

SMOKE_VENV="$QUALITY_TMP/wheel-smoke-venv"
SMOKE_PROJECT="$QUALITY_TMP/wheel-smoke-project"
SMOKE_RUNTIME="$QUALITY_TMP/wheel-smoke-runtime"
SMOKE_SETTINGS="$SMOKE_RUNTIME/config/settings.toml"
"$PYTHON_BIN" -m venv "$SMOKE_VENV"
"$SMOKE_VENV/bin/python" -m pip install -c "$CONSTRAINTS_FILE" dist/*.whl
mkdir -p "$(dirname "$SMOKE_SETTINGS")" "$SMOKE_PROJECT"
: > "$SMOKE_SETTINGS"
env \
  MAL_UPDATER_RUNTIME_DIR="$SMOKE_RUNTIME" \
  MAL_UPDATER_RUNTIME_ROOT="$SMOKE_RUNTIME" \
  MAL_UPDATER_CONFIG="$SMOKE_SETTINGS" \
  MAL_UPDATER_SETTINGS_PATH="$SMOKE_SETTINGS" \
  "$SMOKE_VENV/bin/mal-updater" --help > "$QUALITY_TMP/mal-updater-help.txt"
env \
  MAL_UPDATER_RUNTIME_DIR="$SMOKE_RUNTIME" \
  MAL_UPDATER_RUNTIME_ROOT="$SMOKE_RUNTIME" \
  MAL_UPDATER_CONFIG="$SMOKE_SETTINGS" \
  MAL_UPDATER_SETTINGS_PATH="$SMOKE_SETTINGS" \
  "$SMOKE_VENV/bin/mal-updater" --project-root "$SMOKE_PROJECT" init > "$QUALITY_TMP/mal-updater-init.txt"
"$SMOKE_VENV/bin/python" - "$SMOKE_RUNTIME/data/mal_updater.sqlite3" <<'PY'
import sqlite3
import sys
from pathlib import Path

from mal_updater.db import MIGRATION_FILENAMES

db_path = Path(sys.argv[1])
with sqlite3.connect(db_path) as conn:
    rows = [row[0] for row in conn.execute("SELECT version FROM schema_migrations ORDER BY rowid")]
if tuple(rows) != MIGRATION_FILENAMES:
    raise SystemExit(f"wheel migration smoke mismatch: {rows!r}")
print(f"wheel_migration_rows={len(rows)}")
PY
printf 'wheel_smoke_help=%s\n' "$QUALITY_TMP/mal-updater-help.txt"
printf 'wheel_smoke_init=%s\n' "$QUALITY_TMP/mal-updater-init.txt"
