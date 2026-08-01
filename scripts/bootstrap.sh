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

first_env_value() {
  local name
  for name in "$@"; do
    if [ -n "${!name:-}" ]; then
      printf '%s\n' "${!name}"
      return 0
    fi
  done
  return 1
}

env_names_label() {
  local joined=""
  local name
  for name in "$@"; do
    if [ -z "$joined" ]; then
      joined="$name"
    else
      joined="$joined, $name"
    fi
  done
  printf '%s\n' "$joined"
}

stage_value_file() {
  local path="$1"
  local value="$2"
  umask 077
  mkdir -p -- "$(dirname -- "$path")"
  printf '%s\n' "$value" > "$path"
  chmod 600 -- "$path"
}

prompt_value() {
  local label="$1"
  local path="$2"
  local secret="$3"
  shift 3
  local value=""
  local env_value=""
  local env_label=""

  if [ -f "$path" ] && [ -s "$path" ]; then
    if [ ! -t 0 ]; then
      printf '%s is already staged at %s. Keeping existing value without showing it.\n' "$label" "$path"
      return 0
    fi
    if prompt_yes_no "$label is already staged at $path. Keep existing value without showing it?" yes; then
      return 0
    fi
  elif [ -f "$path" ]; then
    printf '%s exists at %s but is empty; treating it as missing.\n' "$label" "$path"
  fi

  if [ "$#" -gt 0 ] && env_value="$(first_env_value "$@")"; then
    stage_value_file "$path" "$env_value"
    printf 'Staged %s from environment-provided value at %s.\n' "$label" "$path"
    return 0
  fi

  if [ ! -t 0 ]; then
    env_label="$(env_names_label "$@")"
    if [ -n "$env_label" ]; then
      printf 'Missing %s at %s and no environment value (%s) was provided in non-interactive mode.\n' "$label" "$path" "$env_label" >&2
    else
      printf 'Missing %s at %s in non-interactive mode.\n' "$label" "$path" >&2
    fi
    return 1
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

  stage_value_file "$path" "$value"
}

env_truthy() {
  local name="$1"
  local value="${!name:-}"
  local normalized="${value,,}"
  case "$normalized" in
    1|true|yes|on) return 0 ;;
    0|false|no|off|"") return 1 ;;
    *)
      printf 'Invalid boolean value for %s: %s\n' "$name" "$value" >&2
      exit 2
      ;;
  esac
}

SELECTED_PROVIDERS=()

append_selected_provider() {
  local provider="$1"
  local existing
  for existing in "${SELECTED_PROVIDERS[@]}"; do
    if [ "$existing" = "$provider" ]; then
      return 0
    fi
  done
  SELECTED_PROVIDERS+=("$provider")
}

provider_has_staged_input() {
  local provider="$1"
  case "$provider" in
    crunchyroll)
      [ -s "$CRUNCHYROLL_USERNAME_PATH" ] || [ -s "$CRUNCHYROLL_PASSWORD_PATH" ] || \
        [ -n "${MAL_UPDATER_CRUNCHYROLL_USERNAME:-}" ] || [ -n "${MAL_UPDATER_CRUNCHYROLL_PASSWORD:-}" ]
      ;;
    hidive)
      [ -s "$HIDIVE_USERNAME_PATH" ] || [ -s "$HIDIVE_PASSWORD_PATH" ] || \
        [ -n "${MAL_UPDATER_HIDIVE_USERNAME:-}" ] || [ -n "${MAL_UPDATER_HIDIVE_PASSWORD:-}" ]
      ;;
    *) return 1 ;;
  esac
}

provider_default_answer() {
  local provider="$1"
  if provider_has_staged_input "$provider"; then
    printf 'yes\n'
  else
    printf 'no\n'
  fi
}

parse_provider_selection() {
  local raw="$1"
  local normalized token
  local provider_tokens=()
  normalized="${raw,,}"
  normalized="${normalized// /,}"
  normalized="${normalized//;/,}"
  IFS=',' read -r -a provider_tokens <<< "$normalized"
  for token in "${provider_tokens[@]}"; do
    token="${token//$'\t'/}"
    token="${token//$'\r'/}"
    token="${token//$'\n'/}"
    case "$token" in
      "") ;;
      all|both)
        append_selected_provider crunchyroll
        append_selected_provider hidive
        ;;
      none|no|disabled|disable) ;;
      crunchyroll|cr)
        append_selected_provider crunchyroll
        ;;
      hidive|hi)
        append_selected_provider hidive
        ;;
      *)
        printf 'Unknown MAL_UPDATER_BOOTSTRAP_PROVIDERS entry: %s\n' "$token" >&2
        printf 'Use comma-separated provider slugs (crunchyroll,hidive), all, or none.\n' >&2
        exit 2
        ;;
    esac
  done
}

resolve_selected_providers() {
  local raw="${MAL_UPDATER_BOOTSTRAP_PROVIDERS:-${MAL_UPDATER_BOOTSTRAP_SOURCE_PROVIDERS:-}}"
  local legacy_seen=0

  SELECTED_PROVIDERS=()
  if [ -n "$raw" ]; then
    parse_provider_selection "$raw"
    return 0
  fi

  if [ -n "${MAL_UPDATER_BOOTSTRAP_ENABLE_CRUNCHYROLL:-}" ]; then
    legacy_seen=1
    if env_truthy MAL_UPDATER_BOOTSTRAP_ENABLE_CRUNCHYROLL; then
      append_selected_provider crunchyroll
    fi
  fi
  if [ -n "${MAL_UPDATER_BOOTSTRAP_ENABLE_HIDIVE:-}" ]; then
    legacy_seen=1
    if env_truthy MAL_UPDATER_BOOTSTRAP_ENABLE_HIDIVE; then
      append_selected_provider hidive
    fi
  fi
  if [ "$legacy_seen" = "1" ]; then
    return 0
  fi

  if [ -t 0 ]; then
    if prompt_yes_no "Enable Crunchyroll provider bootstrap in this run?" "$(provider_default_answer crunchyroll)"; then
      append_selected_provider crunchyroll
    fi
    if prompt_yes_no "Enable HIDIVE provider bootstrap in this run?" "$(provider_default_answer hidive)"; then
      append_selected_provider hidive
    fi
    return 0
  fi

  if provider_has_staged_input crunchyroll; then
    append_selected_provider crunchyroll
  fi
  if provider_has_staged_input hidive; then
    append_selected_provider hidive
  fi
}

provider_is_selected() {
  local provider="$1"
  local existing
  for existing in "${SELECTED_PROVIDERS[@]}"; do
    if [ "$existing" = "$provider" ]; then
      return 0
    fi
  done
  return 1
}

print_selected_providers() {
  if [ "${#SELECTED_PROVIDERS[@]}" -eq 0 ]; then
    printf 'Source provider bootstraps selected: none\n'
  else
    printf 'Source provider bootstraps selected: %s\n' "${SELECTED_PROVIDERS[*]}"
  fi
}

resolve_service_start() {
  local policy="${MAL_UPDATER_BOOTSTRAP_SERVICE_START:-prompt}"
  policy="${policy,,}"
  case "$policy" in
    1|true|yes|on)
      return 0
      ;;
    0|false|no|off|skip)
      return 1
      ;;
    prompt|"")
      if [ -t 0 ] && prompt_yes_no "Start or restart the mal-updater user service now?" no; then
        return 0
      fi
      return 1
      ;;
    *)
      printf 'Invalid MAL_UPDATER_BOOTSTRAP_SERVICE_START value: %s\n' "$MAL_UPDATER_BOOTSTRAP_SERVICE_START" >&2
      printf 'Use yes, no, or prompt.\n' >&2
      exit 2
      ;;
  esac
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

  selected="$existing"
  if [ -z "$selected" ]; then
    selected="127.0.0.1"
  fi

  if [ ! -t 0 ]; then
    printf '%s\n' "$selected"
    return 0
  fi

  printf 'Default MAL OAuth redirect host: %s\n' "$selected" >&2
  if [ -n "$detected" ]; then
    printf 'Detected non-loopback LAN host if you explicitly need another device/browser: %s\n' "$detected" >&2
  fi
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

is_loopback_host() {
  "$PYTHON_BIN" - "$1" <<'PY'
import ipaddress
import sys

host = sys.argv[1].strip().lower()
if host in {"localhost", "ip6-localhost", "ip6-loopback"}:
    raise SystemExit(0)
try:
    raise SystemExit(0 if ipaddress.ip_address(host).is_loopback else 1)
except ValueError:
    raise SystemExit(1)
PY
}

update_mal_runtime_settings() {
  local settings_path="$1"
  local redirect_host="$2"
  local bind_host="$3"
  local non_loopback_ack="$4"
  "$PYTHON_BIN" - "$settings_path" "$redirect_host" "$bind_host" "$non_loopback_ack" <<'PY'
import re
import sys
from pathlib import Path

settings_path = Path(sys.argv[1])
redirect_host = sys.argv[2]
bind_host = sys.argv[3]
non_loopback_ack = sys.argv[4].lower() in {"1", "true", "yes", "on"}
updates = {
    "bind_host": bind_host,
    "non_loopback_callback_ack": non_loopback_ack,
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
    if isinstance(value, bool):
        return "true" if value else "false"
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
  local policy="${MAL_UPDATER_BOOTSTRAP_RUN_AUTH_STEPS:-prompt}"
  policy="${policy,,}"
  case "$policy" in
    1|true|yes|on)
      run_cli "$@"
      ;;
    0|false|no|off|skip)
      printf 'Skipped: %s\n' "$description"
      ;;
    prompt|"")
      if [ -t 0 ] && prompt_yes_no "Run now?" yes; then
        run_cli "$@"
      else
        printf 'Skipped: %s\n' "$description"
      fi
      ;;
    *)
      printf 'Invalid MAL_UPDATER_BOOTSTRAP_RUN_AUTH_STEPS value: %s\n' "$MAL_UPDATER_BOOTSTRAP_RUN_AUTH_STEPS" >&2
      printf 'Use yes, no, or prompt.\n' >&2
      exit 2
      ;;
  esac
}
say "MAL-Updater production bootstrap"
printf 'Repository: %s\n' "$REPO_ROOT"
printf 'This script stages selected credential files, initializes runtime state, runs selected auth bootstraps, audits health, and installs the user systemd service.\n'
printf 'It will not run apply-sync --execute or perform live MAL writes beyond auth/token exchange.\n'
printf 'Provider selection: set MAL_UPDATER_BOOTSTRAP_PROVIDERS=crunchyroll,hidive, all, or none; when unset, interactive runs ask and non-interactive runs infer only already staged/env-provided provider credentials.\n'
printf 'Non-interactive controls: set MAL_UPDATER_BOOTSTRAP_RUN_AUTH_STEPS=yes/no/prompt and MAL_UPDATER_BOOTSTRAP_SERVICE_START=yes/no/prompt to control live auth/service start; otherwise auth/service start prompts only when there is a TTY.\n'
printf 'Virtualenv: %s\n' "$BOOTSTRAP_VENV"
printf 'Python:     %s\n' "$PYTHON_BIN"

ensure_venv

INSTALL_DEPS=0
if [ -t 0 ]; then
  if prompt_yes_no "Install/update Python package dependencies with: $PYTHON_BIN -m pip install -e .?" "$PIP_INSTALL_DEFAULT"; then
    INSTALL_DEPS=1
  fi
else
  case "${PIP_INSTALL_DEFAULT,,}" in
    1|true|yes|on) INSTALL_DEPS=1 ;;
    0|false|no|off|"") INSTALL_DEPS=0 ;;
    *)
      printf 'Invalid MAL_UPDATER_BOOTSTRAP_INSTALL_DEPS value: %s\n' "$PIP_INSTALL_DEFAULT" >&2
      exit 2
      ;;
  esac
fi
if [ "$INSTALL_DEPS" = "1" ]; then
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
BIND_HOST="127.0.0.1"
NON_LOOPBACK_CALLBACK_ACK="false"
if ! is_loopback_host "$REDIRECT_HOST"; then
  if [ -n "${MAL_UPDATER_BOOTSTRAP_BIND_HOST:-}" ]; then
    BIND_HOST="$MAL_UPDATER_BOOTSTRAP_BIND_HOST"
  else
    BIND_HOST="0.0.0.0"
  fi
  if [ -t 0 ]; then
    warn "Non-loopback MAL OAuth callback requested; this exposes the temporary callback listener beyond loopback."
    if prompt_yes_no "Acknowledge non-loopback callback listener exposure for this bootstrap?" no; then
      NON_LOOPBACK_CALLBACK_ACK="true"
    else
      printf 'Non-loopback callback configuration requires acknowledgement. Aborting before writing settings.\n' >&2
      exit 2
    fi
  elif [ "${MAL_UPDATER_BOOTSTRAP_NON_LOOPBACK_CALLBACK_ACK:-}" = "true" ] || [ "${MAL_UPDATER_BOOTSTRAP_NON_LOOPBACK_CALLBACK_ACK:-}" = "1" ]; then
    NON_LOOPBACK_CALLBACK_ACK="true"
  else
    printf 'Non-loopback callback configuration requires MAL_UPDATER_BOOTSTRAP_NON_LOOPBACK_CALLBACK_ACK=true in non-interactive mode.\n' >&2
    exit 2
  fi
fi
update_mal_runtime_settings "$SETTINGS_PATH" "$REDIRECT_HOST" "$BIND_HOST" "$NON_LOOPBACK_CALLBACK_ACK"
printf 'Updated MAL runtime settings:\n'
printf '  bind_host = %s\n' "$BIND_HOST"
printf '  non_loopback_callback_ack = %s\n' "$NON_LOOPBACK_CALLBACK_ACK"
printf '  redirect_host = %s\n' "$REDIRECT_HOST"
printf '  redirect_port = 8765\n'
printf 'Register this exact MyAnimeList API callback URI before running MAL OAuth login:\n'
printf '  http://%s:8765/callback\n' "$REDIRECT_HOST"

say "Select source provider bootstraps"
resolve_selected_providers
print_selected_providers

say "Stage credentials"
prompt_value "MAL client id" "$MAL_CLIENT_ID_PATH" no MAL_UPDATER_MAL_CLIENT_ID
prompt_value "MAL client secret" "$MAL_CLIENT_SECRET_PATH" yes MAL_UPDATER_MAL_CLIENT_SECRET
if provider_is_selected crunchyroll; then
  prompt_value "Crunchyroll username/email" "$CRUNCHYROLL_USERNAME_PATH" no MAL_UPDATER_CRUNCHYROLL_USERNAME
  prompt_value "Crunchyroll password" "$CRUNCHYROLL_PASSWORD_PATH" yes MAL_UPDATER_CRUNCHYROLL_PASSWORD
else
  printf 'Skipping Crunchyroll credential prompts/auth because Crunchyroll was not selected for this bootstrap run.\n'
fi
if provider_is_selected hidive; then
  prompt_value "HIDIVE username/email" "$HIDIVE_USERNAME_PATH" no MAL_UPDATER_HIDIVE_USERNAME
  prompt_value "HIDIVE password" "$HIDIVE_PASSWORD_PATH" yes MAL_UPDATER_HIDIVE_PASSWORD
else
  printf 'Skipping HIDIVE credential prompts/auth because HIDIVE was not selected for this bootstrap run.\n'
fi
chmod 700 -- "$SECRETS_DIR"
find "$SECRETS_DIR" -type f -exec chmod 600 {} +

run_auth_step "MyAnimeList OAuth login (mal-auth-login)" mal-auth-login
if provider_is_selected crunchyroll; then
  run_auth_step "Crunchyroll provider auth login" provider-auth-login --provider crunchyroll
fi
if provider_is_selected hidive; then
  run_auth_step "HIDIVE provider auth login" provider-auth-login --provider hidive
fi

say "Read-only bootstrap audit"
run_cli bootstrap-audit --summary

say "Read-only health check"
run_cli health-check --format summary

say "Install/update user systemd service"
INSTALL_ARGS=()
START_SERVICE=0
if resolve_service_start; then
  START_SERVICE=1
  INSTALL_ARGS+=(--start-service)
fi
if [ -x "$REPO_ROOT/scripts/install_user_systemd_units.sh" ]; then
  MAL_UPDATER_SERVICE_PYTHON_BIN="$PYTHON_BIN" "$REPO_ROOT/scripts/install_user_systemd_units.sh" "${INSTALL_ARGS[@]}"
else
  MAL_UPDATER_SERVICE_PYTHON_BIN="$PYTHON_BIN" bash "$REPO_ROOT/scripts/install_user_systemd_units.sh" "${INSTALL_ARGS[@]}"
fi

if command -v systemctl >/dev/null 2>&1; then
  if [ "$START_SERVICE" = "1" ]; then
    systemctl --user status --no-pager --lines=20 mal-updater.service || true
  else
    printf 'Service start skipped. You can start it later with: systemctl --user start mal-updater.service\n'
  fi
else
  warn "systemctl not found; service install script may have reported host-specific guidance."
fi
say "Bootstrap complete"
printf 'Review audit/health output above before enabling unattended production use.\n'
