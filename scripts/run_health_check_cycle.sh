#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

ARGS=("$@")

if [[ -n "${MAL_UPDATER_HEALTH_STALE_HOURS:-}" ]]; then
  ARGS+=("--stale-hours" "$MAL_UPDATER_HEALTH_STALE_HOURS")
fi
if [[ "${MAL_UPDATER_HEALTH_STRICT:-0}" == "1" ]]; then
  ARGS+=("--strict")
fi
if [[ "${MAL_UPDATER_HEALTH_AUTO_RUN_RECOMMENDED:-0}" == "1" ]]; then
  ARGS+=("--auto-run-recommended")
fi
if [[ -n "${MAL_UPDATER_HEALTH_AUTO_RUN_REASON_CODES:-}" ]]; then
  ARGS+=("--auto-run-reason-codes" "$MAL_UPDATER_HEALTH_AUTO_RUN_REASON_CODES")
fi

exec env PYTHONPATH=src python3 -m mal_updater.cli --project-root "$ROOT_DIR" health-check-cycle "${ARGS[@]}"
