"""Trusted-LAN container control-plane state."""
from __future__ import annotations

import json, math, os, re, secrets, threading, time
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import urlencode, urlparse

from .auth import write_secret_file
from .config import (
    AppConfig,
    DEFAULT_SERVICE_PROVIDER_AUTH_FAILURE_BACKOFF_FLOORS,
    DEFAULT_SERVICE_PROVIDER_CRITICAL_BACKOFF_FLOORS,
    DEFAULT_SERVICE_PROVIDER_HOURLY_LIMITS,
    DEFAULT_SERVICE_PROVIDER_PROJECTED_REQUEST_HISTORY_WINDOWS,
    DEFAULT_SERVICE_PROVIDER_PROJECTED_REQUEST_PERCENTILES,
    DEFAULT_SERVICE_PROVIDER_WARN_BACKOFF_FLOORS,
    DEFAULT_SERVICE_TASK_AUTH_FAILURE_BACKOFF_FLOORS,
    DEFAULT_SERVICE_TASK_CRITICAL_BACKOFF_FLOORS,
    DEFAULT_SERVICE_TASK_EXECUTE_LIMITS,
    DEFAULT_SERVICE_TASK_HOURLY_LIMITS,
    DEFAULT_SERVICE_TASK_PROJECTED_REQUEST_HISTORY_WINDOWS,
    DEFAULT_SERVICE_TASK_PROJECTED_REQUEST_PERCENTILES,
    DEFAULT_SERVICE_TASK_WARN_BACKOFF_FLOORS,
    DEFAULT_SERVICE_CRITICAL_RATIO,
    DEFAULT_SERVICE_WARN_RATIO,
    _toml_parser,
    load_config,
    load_mal_secrets,
)
from .crunchyroll_auth import load_crunchyroll_credentials
from .hidive_auth import load_hidive_credentials
from .persistence import atomic_write_text

MAX_BODY = 64 * 1024


@dataclass(frozen=True, slots=True)
class SettingSpec:
    kind: str
    minimum: float
    maximum: float
    group: str
    label: str
    help: str
    unit: str = ""
    step: str = "1"


def _label(key: str) -> str:
    words = {"mal": "MAL", "hidive": "HIDIVE", "crunchyroll": "Crunchyroll"}
    rendered = " ".join(words.get(part, part) for part in key.split("_"))
    return rendered[:1].upper() + rendered[1:]


def _int_spec(group: str, key: str, *, minimum: int = 0, maximum: int = 100_000, help: str, unit: str = "") -> SettingSpec:
    return SettingSpec("int", minimum, maximum, group, _label(key), help, unit)


SETTINGS_SCHEMA: dict[str, SettingSpec] = {
    "request_timeout_seconds": SettingSpec("float", 1, 300, "Request pacing and retries", "Request timeout", "Per-request network timeout.", "seconds", "0.1"),
    "service.sync_every_seconds": _int_spec("Scheduler cadence", "sync_every_seconds", minimum=60, maximum=2_592_000, help="Base provider synchronization interval.", unit="seconds"),
    "service.health_every_seconds": _int_spec("Scheduler cadence", "health_every_seconds", minimum=60, maximum=2_592_000, help="Health-check interval.", unit="seconds"),
    "service.crunchyroll_hourly_limit": _int_spec("Hourly request budgets", "crunchyroll_hourly_limit", help="Legacy Crunchyroll request budget per rolling hour; 0 blocks requests.", unit="requests/hour"),
    "service.source_provider_hourly_limit": _int_spec("Hourly request budgets", "source_provider_hourly_limit", help="Fallback source-provider request budget per rolling hour; 0 blocks requests.", unit="requests/hour"),
    "service.mal_hourly_limit": _int_spec("Hourly request budgets", "mal_hourly_limit", help="Global MAL request budget per rolling hour; 0 blocks requests.", unit="requests/hour"),
    "service.crunchyroll_provider_max_history_pages": _int_spec("Provider pagination", "crunchyroll_provider_max_history_pages", minimum=1, maximum=1_000, help="Maximum Crunchyroll history pages fetched in one provider run.", unit="pages/run"),
    "service.crunchyroll_provider_max_watchlist_pages": _int_spec("Provider pagination", "crunchyroll_provider_max_watchlist_pages", minimum=1, maximum=1_000, help="Maximum Crunchyroll watchlist pages fetched in one provider run.", unit="pages/run"),
    "service.warn_ratio": SettingSpec("float", 0.01, 0.99, "Budget learning", "Warning threshold", "Fraction of an hourly budget that triggers warning backoff; must be below the critical threshold.", "ratio", "0.01"),
    "service.critical_ratio": SettingSpec("float", 0.02, 1.0, "Budget learning", "Critical threshold", "Fraction of an hourly budget that triggers critical backoff; must exceed the warning threshold.", "ratio", "0.01"),
}

for provider in DEFAULT_SERVICE_PROVIDER_HOURLY_LIMITS:
    SETTINGS_SCHEMA[f"service.provider_hourly_limits.{provider}"] = _int_spec(
        "Hourly request budgets", provider, help=f"{provider.upper()} request budget per rolling hour; 0 blocks requests.", unit="requests/hour"
    )
for task in DEFAULT_SERVICE_TASK_HOURLY_LIMITS:
    SETTINGS_SCHEMA[f"service.task_hourly_limits.{task}"] = _int_spec(
        "Hourly request budgets", task, help="Task-scoped request budget per rolling hour; 0 blocks this task's requests.", unit="requests/hour"
    )
for task in DEFAULT_SERVICE_TASK_EXECUTE_LIMITS:
    SETTINGS_SCHEMA[f"service.task_execute_limits.{task}"] = _int_spec(
        "Per-run execution bounds", task, help="Maximum items, candidates, queries, or pages processed by this task per run; 0 disables or makes the task no-op.", unit="per run"
    )
for section in ("mal", "crunchyroll", "hidive"):
    display = "MAL" if section == "mal" else section.upper() if section == "hidive" else section.capitalize()
    SETTINGS_SCHEMA[f"{section}.request_spacing_seconds"] = SettingSpec("float", 0, 3_600, "Request pacing and retries", f"{display} request spacing", "Minimum delay between request starts on this host.", "seconds", "0.01")
    SETTINGS_SCHEMA[f"{section}.request_spacing_jitter_seconds"] = SettingSpec("float", 0, 3_600, "Request pacing and retries", f"{display} spacing jitter", "Maximum random delay added to request spacing.", "seconds", "0.01")
    SETTINGS_SCHEMA[f"{section}.retry_max_attempts"] = SettingSpec("int", 1, 10, "Request pacing and retries", f"{display} retry attempts", "Maximum attempts for safe read requests; login, token, and MAL write requests remain single-attempt.", "attempts")
    SETTINGS_SCHEMA[f"{section}.retry_backoff_base_seconds"] = SettingSpec("float", 0, 3_600, "Request pacing and retries", f"{display} retry backoff base", "Base delay used for safe-read retry backoff.", "seconds", "0.01")
    SETTINGS_SCHEMA[f"{section}.retry_backoff_jitter_seconds"] = SettingSpec("float", 0, 3_600, "Request pacing and retries", f"{display} retry backoff jitter", "Maximum random delay added to retry backoff.", "seconds", "0.01")
    SETTINGS_SCHEMA[f"{section}.retry_after_cap_seconds"] = SettingSpec("float", 0, 86_400, "Request pacing and retries", f"{display} Retry-After cap", "Maximum server-requested Retry-After delay honored by a single retry.", "seconds", "0.01")

for section, defaults, value_kind in (
    ("provider_projected_request_history_windows", DEFAULT_SERVICE_PROVIDER_PROJECTED_REQUEST_HISTORY_WINDOWS, "history"),
    ("task_projected_request_history_windows", DEFAULT_SERVICE_TASK_PROJECTED_REQUEST_HISTORY_WINDOWS, "history"),
    ("provider_projected_request_percentiles", DEFAULT_SERVICE_PROVIDER_PROJECTED_REQUEST_PERCENTILES, "percentile"),
    ("task_projected_request_percentiles", DEFAULT_SERVICE_TASK_PROJECTED_REQUEST_PERCENTILES, "percentile"),
):
    for key in defaults:
        path = f"service.{section}.{key}"
        if value_kind == "history":
            SETTINGS_SCHEMA[path] = SettingSpec("int", 1, 20, "Budget learning", _label(key) + " history", "Recent successful request-count samples used for learned budget projections.", "runs")
        else:
            SETTINGS_SCHEMA[path] = SettingSpec("float", 0.01, 1.0, "Budget learning", _label(key) + " percentile", "Conservative percentile selected from learned request-count history.", "ratio", "0.01")

for section, defaults, level in (
    ("provider_warn_backoff_floor_seconds", DEFAULT_SERVICE_PROVIDER_WARN_BACKOFF_FLOORS, "warning"),
    ("task_warn_backoff_floor_seconds", DEFAULT_SERVICE_TASK_WARN_BACKOFF_FLOORS, "warning"),
    ("provider_critical_backoff_floor_seconds", DEFAULT_SERVICE_PROVIDER_CRITICAL_BACKOFF_FLOORS, "critical"),
    ("task_critical_backoff_floor_seconds", DEFAULT_SERVICE_TASK_CRITICAL_BACKOFF_FLOORS, "critical"),
    ("provider_auth_failure_backoff_floor_seconds", DEFAULT_SERVICE_PROVIDER_AUTH_FAILURE_BACKOFF_FLOORS, "authentication-failure"),
    ("task_auth_failure_backoff_floor_seconds", DEFAULT_SERVICE_TASK_AUTH_FAILURE_BACKOFF_FLOORS, "authentication-failure"),
):
    for key in defaults:
        SETTINGS_SCHEMA[f"service.{section}.{key}"] = _int_spec(
            "Budget backoff floors", f"{key} {level}", maximum=604_800, help=f"Minimum {level} backoff for this budget scope.", unit="seconds"
        )


_TABLE_RE = re.compile(r"^\s*\[([^\[\]]+)]\s*(?:#.*)?(?:\r?\n)?$")


def _flatten_settings(data: dict[str, Any], prefix: str = "") -> dict[str, Any]:
    flat: dict[str, Any] = {}
    for key, value in data.items():
        if not isinstance(key, str) or not key or "." in key:
            raise ValueError("unknown setting")
        path = f"{prefix}.{key}" if prefix else key
        if isinstance(value, dict):
            flat.update(_flatten_settings(value, path))
        else:
            flat[path] = value
    return flat


def _replace_toml_number(text: str, path: str, value: int | float) -> str:
    table, key = path.rsplit(".", 1) if "." in path else ("", path)
    lines = text.splitlines(keepends=True)
    section_start, section_end = (0, len(lines)) if not table else (-1, -1)
    current_table = ""
    for index, line in enumerate(lines):
        match = _TABLE_RE.match(line)
        if not match:
            continue
        if not table and section_end == len(lines):
            section_end = index
        next_table = match.group(1).strip()
        if current_table == table and section_start >= 0 and section_end < 0:
            section_end = index
        current_table = next_table
        if next_table == table:
            section_start, section_end = index + 1, -1
    if section_start >= 0 and section_end < 0:
        section_end = len(lines)
    rendered = str(value).lower()
    if section_start >= 0:
        assignment = re.compile(rf"^(\s*{re.escape(key)}\s*=\s*)([^#\r\n]*?)([ \t]*(?:#.*)?)(\r?\n)?$")
        for index in range(section_start, section_end):
            match = assignment.match(lines[index])
            if match:
                newline = match.group(4) or ("\n" if lines[index].endswith("\n") else "")
                lines[index] = f"{match.group(1)}{rendered}{match.group(3)}{newline}"
                return "".join(lines)
        lines.insert(section_end, f"{key} = {rendered}\n")
        return "".join(lines)
    if lines and not lines[-1].endswith(("\n", "\r")):
        lines[-1] += "\n"
    if lines and lines[-1].strip():
        lines.append("\n")
    lines.extend((f"[{table}]\n", f"{key} = {rendered}\n"))
    return "".join(lines)


def _setting_value(config: AppConfig, path: str) -> int | float:
    value: Any = config
    for component in path.split("."):
        value = value.get(component) if isinstance(value, dict) else getattr(value, component)
    return value


def _nested_setting_values(config: AppConfig) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for path in SETTINGS_SCHEMA:
        target = result
        components = path.split(".")
        for component in components[:-1]:
            target = target.setdefault(component, {})
        target[components[-1]] = _setting_value(config, path)
    return result


class RateLimiter:
    def __init__(self, limit: int = 6, window: float = 300):
        self.limit, self.window, self._hits, self._lock = limit, window, {}, threading.Lock()
    def allow(self, key: str, now: float | None = None) -> bool:
        now = time.monotonic() if now is None else now
        with self._lock:
            hits = [x for x in self._hits.get(key, []) if x > now - self.window]
            if len(hits) >= self.limit:
                self._hits[key] = hits
                return False
            hits.append(now); self._hits[key] = hits
            return True
    def clear(self, key: str) -> None:
        with self._lock:
            self._hits.pop(key, None)

class ControlStore:
    SECRET_NAMES = {
        "mal_client_id": "mal_client_id.txt", "mal_client_secret": "mal_client_secret.txt",
        "crunchyroll_username": "crunchyroll_username.txt", "crunchyroll_password": "crunchyroll_password.txt",
        "hidive_username": "hidive_username.txt", "hidive_password": "hidive_password.txt",
    }
    def __init__(self, config: AppConfig):
        self.config = config
        self.state_path = config.state_dir / "container-control.json"
        self.audit_path = config.state_dir / "container-audit.jsonl"
        # A process-local synchronizer token protects credential-free mutations.
        # It is readable only through same-origin fetch under the browser SOP and
        # is intentionally neither a credential nor persisted installation state.
        self.csrf_token = secrets.token_urlsafe(32)
        self.oauth: dict[str, dict[str, Any]] = {}
        self.connection_results: dict[str, dict[str, Any]] = {}
        self.rate = RateLimiter()
        self.lock = threading.RLock()
    def _secret_paths(self) -> dict[str, Path]:
        mal = load_mal_secrets(self.config)
        crunchyroll = load_crunchyroll_credentials(self.config)
        hidive = load_hidive_credentials(self.config)
        return {
            "mal_client_id": mal.client_id_path,
            "mal_client_secret": mal.client_secret_path,
            "crunchyroll_username": crunchyroll.username_path,
            "crunchyroll_password": crunchyroll.password_path,
            "hidive_username": hidive.username_path,
            "hidive_password": hidive.password_path,
        }
    def status(self) -> dict[str, Any]:
        with self.lock:
            connection_results = {kind: dict(result) for kind, result in self.connection_results.items()}
            try:
                effective_config = load_config(self.config.project_root)
                settings_error = None
            except ValueError:
                # Keep health/readiness and credential controls available if an
                # operator independently leaves settings.toml malformed.
                effective_config = self.config
                settings_error = "Persisted settings are invalid; showing the running configuration."
            effective_settings = _nested_setting_values(effective_config)
            runtime_settings = _nested_setting_values(self.config)
        mal = load_mal_secrets(self.config)
        crunchyroll = load_crunchyroll_credentials(self.config)
        hidive = load_hidive_credentials(self.config)
        present = {
            "mal_client_id": bool(mal.client_id),
            "mal_client_secret": bool(mal.client_secret),
            "crunchyroll_username": bool(crunchyroll.username),
            "crunchyroll_password": bool(crunchyroll.password),
            "hidive_username": bool(hidive.username),
            "hidive_password": bool(hidive.password),
        }
        complete = bool(mal.client_id) and bool(mal.access_token and mal.refresh_token)
        blockers = []
        if not mal.client_id:
            blockers.append("mal_client_id")
        if not (mal.access_token and mal.refresh_token):
            blockers.append("mal_oauth_tokens")
        return {
            "setup_complete": complete,
            "automation_desired": True,
            "automation_prerequisites_satisfied": complete,
            "automation_state": "ready" if complete else "blocked",
            "automation_blockers": blockers,
            "mal_oauth_complete": bool(mal.access_token and mal.refresh_token),
            "secrets_present": present,
            "providers": {
                "crunchyroll": {
                    "scheduler_eligible": bool(crunchyroll.username and crunchyroll.password),
                    "scheduling_basis": "credentials_present",
                },
                "hidive": {
                    "scheduler_eligible": bool(hidive.username and hidive.password),
                    "scheduling_basis": "credentials_present",
                },
            },
            "connection_tests": connection_results,
            "settings": effective_settings,
            "settings_schema": [
                {
                    "path": path,
                    "kind": spec.kind,
                    "min": spec.minimum,
                    "max": spec.maximum,
                    "group": spec.group,
                    "label": spec.label,
                    "help": spec.help,
                    "unit": spec.unit,
                    "step": spec.step,
                }
                for path, spec in SETTINGS_SCHEMA.items()
            ],
            "restart_required": effective_settings != runtime_settings,
            "settings_application": "Changes apply after the scheduler/container restarts; the running scheduler is not live-reloaded.",
            "settings_error": settings_error,
            "write_posture": "conservative; onboarding does not approve MAL writes",
        }
    def save_settings(self, data: dict[str, Any]) -> None:
        if not isinstance(data, dict):
            raise ValueError("settings object required")
        # Retain the two pre-form API spellings while keeping all new names
        # explicit dotted or nested allowlisted paths.
        aliases = {
            "sync_every_seconds": "service.sync_every_seconds",
            "health_every_seconds": "service.health_every_seconds",
        }
        flat: dict[str, Any] = {}
        nested: dict[str, Any] = {}
        for key, value in data.items():
            if key in aliases:
                if aliases[key] in flat:
                    raise ValueError("duplicate setting")
                flat[aliases[key]] = value
            elif isinstance(key, str) and "." in key:
                flat[key] = value
            else:
                nested[key] = value
        nested_flat = _flatten_settings(nested)
        if set(flat) & set(nested_flat):
            raise ValueError("duplicate setting")
        flat.update(nested_flat)
        if not flat or set(flat) - set(SETTINGS_SCHEMA):
            raise ValueError("unknown setting")
        normalized: dict[str, int | float] = {}
        for path, value in flat.items():
            spec = SETTINGS_SCHEMA[path]
            if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(float(value)):
                raise ValueError(f"invalid setting: {path}")
            if spec.kind == "int" and (not isinstance(value, int) or isinstance(value, bool)):
                raise ValueError(f"invalid setting: {path}")
            if not spec.minimum <= value <= spec.maximum:
                raise ValueError(f"invalid setting: {path}")
            normalized[path] = int(value) if spec.kind == "int" else float(value)
        with self.lock:
            try:
                original = self.config.settings_path.read_text(encoding="utf-8")
            except FileNotFoundError:
                original = ""
            candidate = original
            for path, value in normalized.items():
                candidate = _replace_toml_number(candidate, path, value)
            # Enforce the cross-field budget relationship against the complete
            # effective candidate, not merely this request's partial values.
            try:
                parsed = _toml_parser.loads(candidate)
            except ValueError:
                raise ValueError("invalid settings file") from None
            service = parsed.get("service", {})
            if not isinstance(service, dict):
                raise ValueError("invalid service settings")
            warn_ratio = service.get("warn_ratio", DEFAULT_SERVICE_WARN_RATIO)
            critical_ratio = service.get("critical_ratio", DEFAULT_SERVICE_CRITICAL_RATIO)
            if (
                isinstance(warn_ratio, bool)
                or isinstance(critical_ratio, bool)
                or not isinstance(warn_ratio, (int, float))
                or not isinstance(critical_ratio, (int, float))
                or float(warn_ratio) >= float(critical_ratio)
            ):
                raise ValueError("warning threshold must be below critical threshold")
            atomic_write_text(self.config.settings_path, candidate, mode=0o600)
            try:
                persisted = load_config(self.config.project_root)
                if persisted.service.warn_ratio >= persisted.service.critical_ratio:
                    raise ValueError("warning threshold must be below critical threshold")
            except Exception:
                atomic_write_text(self.config.settings_path, original, mode=0o600)
                raise
        self.audit("settings_updated", fields=sorted(normalized))
    def save_secrets(self, data: dict[str, Any], remove: list[str] | None = None) -> None:
        if not isinstance(data, dict) or not isinstance(remove or [], list) or set(data) - set(self.SECRET_NAMES) or set(remove or []) - set(self.SECRET_NAMES): raise ValueError("unknown secret")
        if set(data) & set(remove or []): raise ValueError("secret cannot be replaced and removed together")
        with self.lock:
            paths = self._secret_paths()
            for name, value in data.items():
                if not isinstance(value, str) or not value.strip() or len(value.encode()) > 4096: raise ValueError("invalid secret")
                write_secret_file(paths[name], value)
            for name in remove or []:
                paths[name].unlink(missing_ok=True)
        self.audit("secrets_changed", replaced=sorted(data), removed=sorted(remove or []))
    def record_connection_test(self, kind: str, *, ok: bool, message: str) -> None:
        if kind not in {"mal", "crunchyroll", "hidive"}: raise ValueError("unknown connection")
        with self.lock:
            self.connection_results[kind] = {
                "ok": bool(ok),
                "message": str(message),
                "tested_at": int(time.time()),
            }
    def begin_oauth(self, redirect_uri: str) -> dict[str, str]:
        parsed = urlparse(redirect_uri)
        if parsed.scheme not in {"http", "https"} or parsed.username or parsed.password or parsed.path != "/oauth/mal/callback" or parsed.query or parsed.fragment:
            raise ValueError("invalid OAuth redirect")
        mal = load_mal_secrets(self.config)
        if not mal.client_id: raise ValueError("MAL client ID required")
        state, verifier = secrets.token_urlsafe(32), secrets.token_urlsafe(64)[:96]
        with self.lock:
            now = time.monotonic()
            self.oauth = {k: v for k, v in self.oauth.items() if v["expires"] >= now}
            self.oauth[state] = {"verifier": verifier, "expires": now + 600, "redirect_uri": redirect_uri}
        query = {"response_type": "code", "client_id": mal.client_id, "redirect_uri": redirect_uri, "code_challenge": verifier, "code_challenge_method": "plain", "state": state}
        return {"authorization_url": f"{self.config.mal.auth_url}?{urlencode(query)}"}
    def consume_oauth(self, state: str) -> dict[str, Any]:
        with self.lock: item = self.oauth.pop(state, None)
        if not item or item["expires"] < time.monotonic(): raise ValueError("invalid OAuth state")
        return item
    def audit(self, event: str, **fields: Any) -> None:
        safe = {"time": int(time.time()), "event": event, **fields}
        line = json.dumps(safe, separators=(",", ":")) + "\n"
        with self.lock:
            self.audit_path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
            fd = os.open(self.audit_path, os.O_WRONLY | os.O_APPEND | os.O_CREAT | os.O_NOFOLLOW, 0o600)
            with os.fdopen(fd, "a", encoding="utf-8") as f: f.write(line)
            os.chmod(self.audit_path, 0o600)
