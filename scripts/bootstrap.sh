#!/usr/bin/env bash
set -euo pipefail

SCRIPT_PATH="${BASH_SOURCE[0]}"
while [ -L "$SCRIPT_PATH" ]; do
  SCRIPT_DIR="$(cd -- "$(dirname -- "$SCRIPT_PATH")" && pwd -P)"
  SCRIPT_PATH="$(readlink -- "$SCRIPT_PATH")"
  case "$SCRIPT_PATH" in
    /*) ;;
    *) SCRIPT_PATH="$SCRIPT_DIR/$SCRIPT_PATH" ;;
  esac
done
SCRIPT_DIR="$(cd -- "$(dirname -- "$SCRIPT_PATH")" && pwd -P)"
REPO_ROOT="$(cd -- "$SCRIPT_DIR/.." && pwd -P)"
PYTHON_BOOTSTRAP_BIN="${PYTHON_BIN:-python3}"
BOOTSTRAP_VENV="${MAL_UPDATER_BOOTSTRAP_VENV:-$REPO_ROOT/.venv}"
PYTHON_BIN="$BOOTSTRAP_VENV/bin/python"
PIP_INSTALL_DEFAULT="${MAL_UPDATER_BOOTSTRAP_INSTALL_DEPS:-yes}"

cd "$REPO_ROOT"

say() {
  printf '\n==> %s\n' "$*"
}

warn() {
  printf 'WARNING: %s\n' "$*" >&2
}

run_cli() {
  PYTHONPATH="$REPO_ROOT/src${PYTHONPATH:+:$PYTHONPATH}" "$PYTHON_BIN" -m mal_updater.cli "$@"
}

ensure_venv() {
  if [ -x "$PYTHON_BIN" ]; then
    return 0
  fi

  say "Creating Python virtual environment"
  printf 'Path: %s\n' "$BOOTSTRAP_VENV"
  if ! "$PYTHON_BOOTSTRAP_BIN" -m venv "$BOOTSTRAP_VENV"; then
    cat >&2 <<EOF
Failed to create virtualenv at: $BOOTSTRAP_VENV

Install Python venv support for $PYTHON_BOOTSTRAP_BIN and retry.
On Debian/Ubuntu this is commonly: sudo apt install python3-venv
EOF
    exit 1
  fi

  if [ ! -x "$PYTHON_BIN" ]; then
    printf 'Virtualenv was created but Python is not executable at: %s\n' "$PYTHON_BIN" >&2
    exit 1
  fi
}

prompt_yes_no() {
  local prompt="$1"
  local default_answer="$2"
  local suffix answer
  case "$default_answer" in
    [Yy]*) suffix="Y/n" ;;
    [Nn]*) suffix="y/N" ;;
    *) suffix="y/n" ;;
  esac
  while true; do
    read -r -p "$prompt [$suffix] " answer || return 1
    if [ -z "$answer" ]; then
      answer="$default_answer"
    fi
    case "$answer" in
      [Yy]|[Yy][Ee][Ss]) return 0 ;;
      [Nn]|[Nn][Oo]) return 1 ;;
      *) printf 'Please answer yes or no.\n' ;;
    esac
  done
}

prompt_value() {
  local label="$1"
  local path="$2"
  local secret="$3"
  local value=""

  if [ -f "$path" ]; then
    if prompt_yes_no "$label is already staged at $path. Keep existing value without showing it?" yes; then
      return 0
    fi
  fi

  while [ -z "$value" ]; do
    if [ "$secret" = "yes" ]; then
      read -r -s -p "Enter $label: " value || return 1
      printf '\n'
    else
      read -r -p "Enter $label: " value || return 1
    fi
    if [ -z "$value" ]; then
      printf '%s cannot be empty. Press Ctrl-C to abort or enter a value.\n' "$label"
    fi
  done

  umask 077
  mkdir -p -- "$(dirname -- "$path")"
  printf '%s\n' "$value" > "$path"
  chmod 600 -- "$path"
}

resolve_paths() {
  PYTHONPATH="$REPO_ROOT/src${PYTHONPATH:+:$PYTHONPATH}" "$PYTHON_BIN" - "$REPO_ROOT" <<'PY'
import sys
from pathlib import Path
from mal_updater.config import load_config, load_mal_secrets
from mal_updater.crunchyroll_auth import load_crunchyroll_credentials
from mal_updater.hidive_auth import load_hidive_credentials

config = load_config(Path(sys.argv[1]))
mal = load_mal_secrets(config)
cr = load_crunchyroll_credentials(config)
hidive = load_hidive_credentials(config)
for key, value in {
    "RUNTIME_ROOT": config.runtime_root,
    "SETTINGS_PATH": config.settings_path,
    "SECRETS_DIR": config.secrets_dir,
    "MAL_CLIENT_ID_PATH": mal.client_id_path,
    "MAL_CLIENT_SECRET_PATH": mal.client_secret_path,
    "CRUNCHYROLL_USERNAME_PATH": cr.username_path,
    "CRUNCHYROLL_PASSWORD_PATH": cr.password_path,
    "HIDIVE_USERNAME_PATH": hidive.username_path,
    "HIDIVE_PASSWORD_PATH": hidive.password_path,
}.items():
    print(f"{key}={value}")
PY
}

detect_host_ip() {
  local detected=""
  if command -v ip >/dev/null 2>&1; then
    detected="$(ip route get 1.1.1.1 2>/dev/null | awk '{for (i=1; i<=NF; i++) if ($i == "src") {print $(i+1); exit}}')"
  fi
  if [ -z "$detected" ] && command -v hostname >/dev/null 2>&1; then
    detected="$(hostname -I 2>/dev/null | tr ' ' '\n' | awk '/^[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+$/ && $0 !~ /^127\./ {print; exit}')"
  fi
  printf '%s\n' "$detected"
}

prompt_redirect_host() {
  local detected="$1"
  local existing="$2"
  local selected answer

  if [ -n "${MAL_UPDATER_BOOTSTRAP_REDIRECT_HOST:-}" ]; then
    printf '%s\n' "$MAL_UPDATER_BOOTSTRAP_REDIRECT_HOST"
    return 0
  fi

  selected="$detected"
  if [ -z "$selected" ]; then
    selected="$existing"
    warn "Could not detect a non-loopback LAN host IP; falling back to existing/default redirect_host: $selected"
  fi

  if [ ! -t 0 ]; then
    printf '%s\n' "$selected"
    return 0
  fi

  printf 'Detected MAL OAuth redirect host: %s\n' "$selected" >&2
  read -r -p "Use this redirect host? [Y/n or enter override IP/host] " answer || return 1
  case "$answer" in
    ""|[Yy]|[Yy][Ee][Ss]) printf '%s\n' "$selected" ;;
    [Nn]|[Nn][Oo])
      while [ -z "$answer" ] || [ "$answer" = "n" ] || [ "$answer" = "N" ] || [ "$answer" = "no" ] || [ "$answer" = "No" ] || [ "$answer" = "NO" ]; do
        read -r -p "Enter redirect host IP/hostname: " answer || return 1
      done
      printf '%s\n' "$answer"
      ;;
    *) printf '%s\n' "$answer" ;;
  esac
}

update_mal_runtime_settings() {
  local settings_path="$1"
  local redirect_host="$2"
  "$PYTHON_BIN" - "$settings_path" "$redirect_host" <<'PY'
import re
import sys
from pathlib import Path

settings_path = Path(sys.argv[1])
redirect_host = sys.argv[2]
updates = {
    "bind_host": "0.0.0.0",
    "redirect_host": redirect_host,
    "redirect_port": 8765,
}

settings_path.parent.mkdir(parents=True, exist_ok=True)
text = settings_path.read_text(encoding="utf-8") if settings_path.exists() else ""
lines = text.splitlines(keepends=True)
section_re = re.compile(r"^\s*\[([^\]]+)\]\s*(?:#.*)?$")
mal_start = None
mal_end = len(lines)
for index, line in enumerate(lines):
    match = section_re.match(line)
    if not match:
        continue
    if match.group(1).strip() == "mal":
        mal_start = index
        mal_end = len(lines)
        continue
    if mal_start is not None and index > mal_start:
        mal_end = index
        break

def toml_value(value):
    if isinstance(value, int):
        return str(value)
    return '"' + str(value).replace('\\', '\\\\').replace('"', '\\"') + '"'

if mal_start is None:
    prefix = "" if not lines or lines[-1].endswith("\n") else "\n"
    block = [prefix, "[mal]\n"] + [f"{key} = {toml_value(value)}\n" for key, value in updates.items()]
    lines.extend(block)
else:
    seen = set()
    key_re = re.compile(r"^(\s*)([A-Za-z0-9_-]+)(\s*=\s*)(.*?)(\s*(?:#.*)?)$")
    for index in range(mal_start + 1, mal_end):
        match = key_re.match(lines[index].rstrip("\n"))
        if not match:
            continue
        key = match.group(2)
        if key in updates:
            newline = "\n" if lines[index].endswith("\n") else ""
            lines[index] = f"{match.group(1)}{key}{match.group(3)}{toml_value(updates[key])}{match.group(5)}{newline}"
            seen.add(key)
    insert_at = mal_end
    additions = [f"{key} = {toml_value(value)}\n" for key, value in updates.items() if key not in seen]
    if additions:
        lines[insert_at:insert_at] = additions

settings_path.write_text("".join(lines), encoding="utf-8")
PY
}

run_auth_step() {
  local description="$1"
  shift
  say "$description"
  printf 'This may require network access, browser interaction, or a local callback depending on the provider.\n'
  if prompt_yes_no "Run now?" yes; then
    run_cli "$@"
  else
    printf 'Skipped: %s\n' "$description"
  fi
}

say "MAL-Updater production bootstrap"
printf 'Repository: %s\n' "$REPO_ROOT"
printf 'This script stages credential files, initializes runtime state, runs auth bootstraps, audits health, and installs the user systemd service.\n'
printf 'It will not run apply-sync --execute or perform live MAL writes beyond auth/token exchange.\n'
printf 'Virtualenv: %s\n' "$BOOTSTRAP_VENV"
printf 'Python:     %s\n' "$PYTHON_BIN"

ensure_venv

if prompt_yes_no "Install/update Python package dependencies with: $PYTHON_BIN -m pip install -e .?" "$PIP_INSTALL_DEFAULT"; then
  "$PYTHON_BIN" -m pip install -e .
else
  printf 'Skipped dependency install. Existing environment must already provide required packages.\n'
fi

say "Initializing runtime layout"
run_cli init

RUNTIME_ROOT=""
SETTINGS_PATH=""
SECRETS_DIR=""
MAL_CLIENT_ID_PATH=""
MAL_CLIENT_SECRET_PATH=""
CRUNCHYROLL_USERNAME_PATH=""
CRUNCHYROLL_PASSWORD_PATH=""
HIDIVE_USERNAME_PATH=""
HIDIVE_PASSWORD_PATH=""
while IFS='=' read -r key value; do
  case "$key" in
    RUNTIME_ROOT) RUNTIME_ROOT="$value" ;;
    SETTINGS_PATH) SETTINGS_PATH="$value" ;;
    SECRETS_DIR) SECRETS_DIR="$value" ;;
    MAL_CLIENT_ID_PATH) MAL_CLIENT_ID_PATH="$value" ;;
    MAL_CLIENT_SECRET_PATH) MAL_CLIENT_SECRET_PATH="$value" ;;
    CRUNCHYROLL_USERNAME_PATH) CRUNCHYROLL_USERNAME_PATH="$value" ;;
    CRUNCHYROLL_PASSWORD_PATH) CRUNCHYROLL_PASSWORD_PATH="$value" ;;
    HIDIVE_USERNAME_PATH) HIDIVE_USERNAME_PATH="$value" ;;
    HIDIVE_PASSWORD_PATH) HIDIVE_PASSWORD_PATH="$value" ;;
  esac
done < <(resolve_paths)
: "${RUNTIME_ROOT:?}"
: "${SETTINGS_PATH:?}"
: "${SECRETS_DIR:?}"
: "${MAL_CLIENT_ID_PATH:?}"
: "${MAL_CLIENT_SECRET_PATH:?}"
: "${CRUNCHYROLL_USERNAME_PATH:?}"
: "${CRUNCHYROLL_PASSWORD_PATH:?}"
: "${HIDIVE_USERNAME_PATH:?}"
: "${HIDIVE_PASSWORD_PATH:?}"

say "Resolved runtime paths"
printf 'Runtime root: %s\n' "$RUNTIME_ROOT"
printf 'Settings:     %s\n' "$SETTINGS_PATH"
printf 'Secrets dir:  %s\n' "$SECRETS_DIR"
umask 077
mkdir -p -- "$SECRETS_DIR"
chmod 700 -- "$SECRETS_DIR"

say "Configure MAL OAuth callback listener"
EXISTING_REDIRECT_HOST="$(run_cli status 2>/dev/null | awk -F= '/^mal.redirect_uri=/{gsub(/^http:\/\//, "", $2); sub(/:[0-9]+\/callback$/, "", $2); print $2; exit}')"
if [ -z "$EXISTING_REDIRECT_HOST" ]; then
  EXISTING_REDIRECT_HOST="127.0.0.1"
fi
DETECTED_REDIRECT_HOST="$(detect_host_ip)"
REDIRECT_HOST="$(prompt_redirect_host "$DETECTED_REDIRECT_HOST" "$EXISTING_REDIRECT_HOST")"
update_mal_runtime_settings "$SETTINGS_PATH" "$REDIRECT_HOST"
printf 'Updated MAL runtime settings:\n'
printf '  bind_host = 0.0.0.0\n'
printf '  redirect_host = %s\n' "$REDIRECT_HOST"
printf '  redirect_port = 8765\n'
printf 'Register this exact MyAnimeList API callback URI before running MAL OAuth login:\n'
printf '  http://%s:8765/callback\n' "$REDIRECT_HOST"

say "Stage credentials"
prompt_value "MAL client id" "$MAL_CLIENT_ID_PATH" no
prompt_value "MAL client secret" "$MAL_CLIENT_SECRET_PATH" yes
prompt_value "Crunchyroll username/email" "$CRUNCHYROLL_USERNAME_PATH" no
prompt_value "Crunchyroll password" "$CRUNCHYROLL_PASSWORD_PATH" yes
prompt_value "HIDIVE username/email" "$HIDIVE_USERNAME_PATH" no
prompt_value "HIDIVE password" "$HIDIVE_PASSWORD_PATH" yes
chmod 700 -- "$SECRETS_DIR"
find "$SECRETS_DIR" -type f -exec chmod 600 {} +

run_auth_step "MyAnimeList OAuth login (mal-auth-login)" mal-auth-login
run_auth_step "Crunchyroll provider auth login" provider-auth-login --provider crunchyroll
run_auth_step "HIDIVE provider auth login" provider-auth-login --provider hidive

say "Read-only bootstrap audit"
run_cli bootstrap-audit --summary

say "Read-only health check"
run_cli health-check --format summary

say "Install/update user systemd service"
if [ -x "$REPO_ROOT/scripts/install_user_systemd_units.sh" ]; then
  MAL_UPDATER_SERVICE_PYTHON_BIN="$PYTHON_BIN" "$REPO_ROOT/scripts/install_user_systemd_units.sh"
else
  MAL_UPDATER_SERVICE_PYTHON_BIN="$PYTHON_BIN" bash "$REPO_ROOT/scripts/install_user_systemd_units.sh"
fi

if command -v systemctl >/dev/null 2>&1; then
  if prompt_yes_no "Start or restart the mal-updater user service now?" no; then
    systemctl --user restart mal-updater.service
    systemctl --user status --no-pager --lines=20 mal-updater.service || true
  else
    printf 'Service start skipped. You can start it later with: systemctl --user start mal-updater.service\n'
  fi
else
  warn "systemctl not found; service install script may have reported host-specific guidance."
fi

say "Bootstrap complete"
printf 'Review audit/health output above before enabling unattended production use.\n'
