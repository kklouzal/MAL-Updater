#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LOCK_DIR="$ROOT_DIR/state/locks"
LOG_DIR="$ROOT_DIR/state/logs"
LOCK_FILE="$LOCK_DIR/exact-approved-sync.lock"
SNAPSHOT_PATH="$ROOT_DIR/cache/live-crunchyroll-snapshot.json"
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
RUN_LOG="$LOG_DIR/exact-approved-sync-$STAMP.log"

mkdir -p "$LOCK_DIR" "$LOG_DIR" "$ROOT_DIR/cache"

exec 9>"$LOCK_FILE"
if ! flock -n 9; then
  echo "[$(date -Is)] exact-approved sync already running; skipping overlap"
  exit 0
fi

exec > >(tee -a "$RUN_LOG") 2>&1

echo "[$(date -Is)] starting exact-approved MAL sync cycle"
echo "root=$ROOT_DIR"
echo "log=$RUN_LOG"
echo "snapshot=$SNAPSHOT_PATH"

cd "$ROOT_DIR"
PYTHONPATH=src python3 -m mal_updater.cli init >/dev/null

if PYTHONPATH=src python3 -m mal_updater.cli crunchyroll-fetch-snapshot --out "$SNAPSHOT_PATH" --ingest; then
  echo "[$(date -Is)] fresh Crunchyroll fetch+ingest completed"
else
  echo "[$(date -Is)] WARNING: fresh Crunchyroll fetch+ingest failed; continuing with the most recent already-ingested Crunchyroll state"
fi

PYTHONPATH=src python3 -m mal_updater.cli apply-sync --limit 0 --exact-approved-only --execute

echo "[$(date -Is)] exact-approved MAL sync cycle completed"
