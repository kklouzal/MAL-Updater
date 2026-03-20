#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SOURCE_DIR="$ROOT_DIR/ops/systemd-user"
TARGET_DIR="${XDG_CONFIG_HOME:-$HOME/.config}/systemd/user"
HEALTH_ENV_SOURCE="$SOURCE_DIR/mal-updater-health-check.env.example"
HEALTH_ENV_TARGET="${XDG_CONFIG_HOME:-$HOME/.config}/mal-updater-health-check.env"
ENABLE_TIMERS=1
RELOAD_DAEMON=1
START_SERVICES=0
COPY_HEALTH_ENV=1
DRY_RUN=0

usage() {
  cat <<'EOF'
Usage: scripts/install_user_systemd_units.sh [options]

Render and install the repo-owned user-level systemd units for MAL-Updater.

Options:
  --target-dir PATH           Override the systemd user unit target directory.
  --health-env-target PATH    Override where the optional health-check env file is copied.
  --no-enable                 Copy/update unit files but do not enable timers.
  --start-services            After install/reload, start both services once immediately.
  --no-daemon-reload          Skip `systemctl --user daemon-reload`.
  --no-health-env             Do not copy the example health-check env file.
  --dry-run                   Print planned actions without changing anything.
  -h, --help                  Show this help.
EOF
}

log() {
  printf '%s\n' "$*"
}

join_by_comma_space() {
  local first=1
  for item in "$@"; do
    if [[ "$first" == "1" ]]; then
      printf '%s' "$item"
      first=0
    else
      printf ', %s' "$item"
    fi
  done
}

run_cmd() {
  if [[ "$DRY_RUN" == "1" ]]; then
    printf '[dry-run]'
    for arg in "$@"; do
      printf ' %q' "$arg"
    done
    printf '\n'
    return 0
  fi
  "$@"
}

copy_file() {
  local source_path="$1"
  local target_path="$2"
  local mode="${3:-644}"
  if [[ "$DRY_RUN" == "1" ]]; then
    printf '[dry-run] install -D -m %s %q %q\n' "$mode" "$source_path" "$target_path"
    return 0
  fi
  install -D -m "$mode" "$source_path" "$target_path"
}

render_unit() {
  local source_path="$1"
  local target_path="$2"
  if [[ "$DRY_RUN" == "1" ]]; then
    printf '[dry-run] render %q -> %q\n' "$source_path" "$target_path"
    return 0
  fi
  python3 - "$source_path" "$target_path" "$ROOT_DIR" "$HEALTH_ENV_TARGET" <<'PY'
from pathlib import Path
import sys
source_path = Path(sys.argv[1])
target_path = Path(sys.argv[2])
repo_root = sys.argv[3]
health_env_target = sys.argv[4]
text = source_path.read_text(encoding='utf-8')
text = text.replace('__MAL_UPDATER_REPO_ROOT__', repo_root)
text = text.replace('__MAL_UPDATER_HEALTH_ENV_FILE__', health_env_target)
target_path.parent.mkdir(parents=True, exist_ok=True)
target_path.write_text(text, encoding='utf-8')
PY
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --target-dir)
      [[ $# -ge 2 ]] || { echo "missing value for $1" >&2; exit 2; }
      TARGET_DIR="$2"
      shift 2
      ;;
    --health-env-target)
      [[ $# -ge 2 ]] || { echo "missing value for $1" >&2; exit 2; }
      HEALTH_ENV_TARGET="$2"
      shift 2
      ;;
    --no-enable)
      ENABLE_TIMERS=0
      shift
      ;;
    --start-services)
      START_SERVICES=1
      shift
      ;;
    --no-daemon-reload)
      RELOAD_DAEMON=0
      shift
      ;;
    --no-health-env)
      COPY_HEALTH_ENV=0
      shift
      ;;
    --dry-run)
      DRY_RUN=1
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "unknown option: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

if [[ ! -d "$SOURCE_DIR" ]]; then
  echo "missing source unit directory: $SOURCE_DIR" >&2
  exit 1
fi

UNITS=(
  mal-updater-exact-approved-sync.service
  mal-updater-exact-approved-sync.timer
  mal-updater-health-check.service
  mal-updater-health-check.timer
)

log "repo_root=$ROOT_DIR"
log "source_dir=$SOURCE_DIR"
log "target_dir=$TARGET_DIR"
log "health_env_target=$HEALTH_ENV_TARGET"

installed_units=()
updated_units=()
unchanged_units=()
for unit in "${UNITS[@]}"; do
  source_path="$SOURCE_DIR/$unit"
  target_path="$TARGET_DIR/$unit"

  rendered_content="$(python3 - "$source_path" "$ROOT_DIR" "$HEALTH_ENV_TARGET" <<'PY'
from pathlib import Path
import sys
source_path = Path(sys.argv[1])
repo_root = sys.argv[2]
health_env_target = sys.argv[3]
text = source_path.read_text(encoding='utf-8')
text = text.replace('__MAL_UPDATER_REPO_ROOT__', repo_root)
text = text.replace('__MAL_UPDATER_HEALTH_ENV_FILE__', health_env_target)
print(text, end='')
PY
)"

  if [[ ! -e "$target_path" ]]; then
    installed_units+=("$unit")
  elif [[ "$(cat "$target_path")" == "$rendered_content" ]]; then
    unchanged_units+=("$unit")
  else
    updated_units+=("$unit")
  fi
  render_unit "$source_path" "$target_path"
done

if [[ ${#installed_units[@]} -gt 0 ]]; then
  log "installed_units=$(join_by_comma_space "${installed_units[@]}")"
fi
if [[ ${#updated_units[@]} -gt 0 ]]; then
  log "updated_units=$(join_by_comma_space "${updated_units[@]}")"
fi
if [[ ${#unchanged_units[@]} -gt 0 ]]; then
  log "unchanged_units=$(join_by_comma_space "${unchanged_units[@]}")"
fi

health_env_action="skipped"
if [[ "$COPY_HEALTH_ENV" == "1" ]]; then
  if [[ -e "$HEALTH_ENV_TARGET" ]]; then
    health_env_action="preserved"
    log "health env already exists; leaving it untouched: $HEALTH_ENV_TARGET"
  else
    health_env_action="installed"
    copy_file "$HEALTH_ENV_SOURCE" "$HEALTH_ENV_TARGET"
  fi
fi
log "health_env_action=$health_env_action"

if [[ "$RELOAD_DAEMON" == "1" ]]; then
  run_cmd systemctl --user daemon-reload
fi

if [[ "$ENABLE_TIMERS" == "1" ]]; then
  run_cmd systemctl --user enable --now \
    mal-updater-exact-approved-sync.timer \
    mal-updater-health-check.timer
else
  log "timer enable/start skipped (--no-enable)"
fi

if [[ "$START_SERVICES" == "1" ]]; then
  run_cmd systemctl --user start \
    mal-updater-exact-approved-sync.service \
    mal-updater-health-check.service
fi

run_cmd systemctl --user list-timers \
  mal-updater-exact-approved-sync.timer \
  mal-updater-health-check.timer

log "user-level MAL-Updater systemd unit install completed"
