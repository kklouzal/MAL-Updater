#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

ARGS=("$@")
exec env PYTHONPATH=src python3 -m mal_updater.cli --project-root "$ROOT_DIR" exact-approved-sync-cycle "${ARGS[@]}"
