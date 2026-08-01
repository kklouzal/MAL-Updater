#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SOURCE_DIR="$ROOT_DIR/ops/systemd-user"
TARGET_DIR="${XDG_CONFIG_HOME:-$HOME/.config}/systemd/user"
SERVICE_ENV_SOURCE="$SOURCE_DIR/mal-updater-service.env.example"
SERVICE_ENV_TARGET="${XDG_CONFIG_HOME:-$HOME/.config}/mal-updater-service.env"
ENABLE_SERVICE=1
ENABLE_DASHBOARD=0
INSTALL_DASHBOARD=0
RELOAD_DAEMON=1
START_SERVICE=0
COPY_SERVICE_ENV=1
DRY_RUN=0
SERVICE_PYTHON_BIN="${MAL_UPDATER_SERVICE_PYTHON_BIN:-$ROOT_DIR/.venv/bin/python}"
UNITS=("mal-updater.service")
DASHBOARD_UNIT_NAME="mal-updater-dashboard.service"

usage() {
  cat <<'EOF'
Usage: scripts/install_user_systemd_units.sh [options]

Render and install the repo-owned user-level MAL-Updater daemon service.
The optional local dashboard unit is source-tracked but installed only with an
explicit opt-in flag; it binds to 127.0.0.1 by template default.

Options:
  --target-dir PATH             Override the systemd user unit target directory.
  --service-env-target PATH     Override where the optional service env file is copied.
  --no-enable                   Copy/update selected service units but do not enable the main daemon.
  --install-dashboard           Also render/install the optional dashboard service unit without enabling it.
  --enable-dashboard            Render/install and enable the optional dashboard service unit.
  --start-service               After install/reload, restart the main daemon service immediately.
  --no-daemon-reload            Skip `systemctl --user daemon-reload`.
  --no-service-env              Do not copy the example service env file.
  --service-python-bin PATH     Python executable for rendered services (default: repo .venv).
  --dry-run                     Print planned actions without changing anything.
  -h, --help                    Show this help.
EOF
}

log() {
  printf '%s\n' "$*"
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

service_env_permission_status() {
  local target_path="$1"
  python3 - "$target_path" <<'PY'
import stat
import sys
from pathlib import Path

path = Path(sys.argv[1])
try:
    mode = stat.S_IMODE(path.stat().st_mode)
except OSError:
    print("service_env_mode=unknown")
    print("service_env_restrictive=false")
else:
    print(f"service_env_mode=0o{mode:03o}")
    print(f"service_env_restrictive={str(mode == 0o600).lower()}")
PY
}

render_unit_python() {
  local mode="$1"
  local source_path="$2"
  local target_path="${3:-}"
  PYTHONPATH="$ROOT_DIR/src${PYTHONPATH:+:$PYTHONPATH}" python3 - "$mode" "$source_path" "$target_path" "$ROOT_DIR" "$SERVICE_ENV_TARGET" "$SERVICE_PYTHON_BIN" <<'PY'
from pathlib import Path
import sys

mode = sys.argv[1]
source_path = Path(sys.argv[2])
target_path = Path(sys.argv[3]) if sys.argv[3] else None
repo_root = Path(sys.argv[4]).resolve()
src_dir = str(repo_root / "src")
if src_dir not in sys.path:
    sys.path.insert(0, src_dir)

from mal_updater.config import load_config
from mal_updater.service_units import render_systemd_unit_template_file, systemd_unit_path_context

config = load_config(repo_root)
rendered = render_systemd_unit_template_file(
    source_path,
    repo_root,
    sys.argv[5],
    sys.argv[6],
    path_context=systemd_unit_path_context(config),
)
if mode == "stdout":
    print(rendered, end="")
elif mode == "write":
    if target_path is None:
        raise SystemExit("write mode requires a target path")
    target_path.parent.mkdir(parents=True, exist_ok=True)
    target_path.write_text(rendered, encoding="utf-8")
else:
    raise SystemExit(f"unknown render mode: {mode}")
PY
}

render_unit_content() {
  local source_path="$1"
  render_unit_python stdout "$source_path"
}

render_unit() {
  local source_path="$1"
  local target_path="$2"
  if [[ "$DRY_RUN" == "1" ]]; then
    printf '[dry-run] render %q -> %q\n' "$source_path" "$target_path"
    return 0
  fi
  render_unit_python write "$source_path" "$target_path"
}

append_unique_unit() {
  local unit_name="$1"
  local existing
  for existing in "${UNITS[@]}"; do
    if [[ "$existing" == "$unit_name" ]]; then
      return 0
    fi
  done
  UNITS+=("$unit_name")
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --target-dir)
      [[ $# -ge 2 ]] || { echo "missing value for $1" >&2; exit 2; }
      TARGET_DIR="$2"
      shift 2
      ;;
    --service-env-target)
      [[ $# -ge 2 ]] || { echo "missing value for $1" >&2; exit 2; }
      SERVICE_ENV_TARGET="$2"
      shift 2
      ;;
    --no-enable)
      ENABLE_SERVICE=0
      shift
      ;;
    --install-dashboard)
      INSTALL_DASHBOARD=1
      shift
      ;;
    --enable-dashboard)
      INSTALL_DASHBOARD=1
      ENABLE_DASHBOARD=1
      shift
      ;;
    --start-service)
      START_SERVICE=1
      shift
      ;;
    --no-daemon-reload)
      RELOAD_DAEMON=0
      shift
      ;;
    --service-python-bin)
      [[ $# -ge 2 ]] || { echo "missing value for $1" >&2; exit 2; }
      SERVICE_PYTHON_BIN="$2"
      shift 2
      ;;
    --no-service-env)
      COPY_SERVICE_ENV=0
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

if [[ "$INSTALL_DASHBOARD" == "1" ]]; then
  append_unique_unit "$DASHBOARD_UNIT_NAME"
fi

for unit_name in "${UNITS[@]}"; do
  source_path="$SOURCE_DIR/$unit_name"
  if [[ ! -f "$source_path" ]]; then
    echo "missing source unit file: $source_path" >&2
    exit 1
  fi
done

log "repo_root=$ROOT_DIR"
log "source_dir=$SOURCE_DIR"
log "target_dir=$TARGET_DIR"
log "service_env_target=$SERVICE_ENV_TARGET"
log "service_python_bin=$SERVICE_PYTHON_BIN"
log "selected_units=$(IFS=,; echo "${UNITS[*]}")"
if [[ "$INSTALL_DASHBOARD" != "1" ]]; then
  log "dashboard_unit_action=skipped (use --install-dashboard or --enable-dashboard to install optional dashboard)"
fi

installed_units=()
updated_units=()
unchanged_units=()
for unit_name in "${UNITS[@]}"; do
  source_path="$SOURCE_DIR/$unit_name"
  target_path="$TARGET_DIR/$unit_name"
  rendered_content="$(render_unit_content "$source_path")"
  if [[ ! -e "$target_path" ]]; then
    installed_units+=("$unit_name")
  elif [[ "$(cat "$target_path")" == "$rendered_content" ]]; then
    unchanged_units+=("$unit_name")
  else
    updated_units+=("$unit_name")
  fi
  render_unit "$source_path" "$target_path"
done

if [[ ${#installed_units[@]} -gt 0 ]]; then
  log "installed_units=$(IFS=,; echo "${installed_units[*]}")"
fi
if [[ ${#updated_units[@]} -gt 0 ]]; then
  log "updated_units=$(IFS=,; echo "${updated_units[*]}")"
fi
if [[ ${#unchanged_units[@]} -gt 0 ]]; then
  log "unchanged_units=$(IFS=,; echo "${unchanged_units[*]}")"
fi

service_env_action="skipped"
if [[ "$COPY_SERVICE_ENV" == "1" ]]; then
  if [[ -e "$SERVICE_ENV_TARGET" ]]; then
    service_env_action="preserved"
    log "service env already exists; leaving it untouched: $SERVICE_ENV_TARGET"
  else
    service_env_action="installed"
    copy_file "$SERVICE_ENV_SOURCE" "$SERVICE_ENV_TARGET" 600
  fi
fi
log "service_env_action=$service_env_action"
if [[ "$DRY_RUN" != "1" && -e "$SERVICE_ENV_TARGET" ]]; then
  service_env_status="$(service_env_permission_status "$SERVICE_ENV_TARGET")"
  log "$service_env_status"
  if grep -q '^service_env_restrictive=false$' <<<"$service_env_status"; then
    log "WARNING: service env file is not mode 0600; leaving existing file untouched: $SERVICE_ENV_TARGET"
  fi
fi

if [[ "$RELOAD_DAEMON" == "1" ]]; then
  run_cmd systemctl --user daemon-reload
fi

if [[ "$ENABLE_SERVICE" == "1" ]]; then
  run_cmd systemctl --user enable "mal-updater.service"
else
  log "service enable skipped (--no-enable)"
fi

if [[ "$ENABLE_DASHBOARD" == "1" ]]; then
  run_cmd systemctl --user enable "$DASHBOARD_UNIT_NAME"
elif [[ "$INSTALL_DASHBOARD" == "1" ]]; then
  log "dashboard enable skipped (use --enable-dashboard to opt in)"
fi

if [[ "$START_SERVICE" == "1" ]]; then
  run_cmd systemctl --user restart "mal-updater.service"
fi

for unit_name in "${UNITS[@]}"; do
  if ! run_cmd systemctl --user status "$unit_name" --no-pager; then
    log "service status probe failed for $unit_name; continuing"
  fi
done

log "user-level MAL-Updater systemd service install completed"
