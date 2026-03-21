from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

try:
    import tomllib
except ModuleNotFoundError:  # pragma: no cover - Python < 3.11
    tomllib = None

DEFAULT_COMPLETION_THRESHOLD = 0.95
DEFAULT_CREDITS_SKIP_WINDOW_SECONDS = 120
DEFAULT_CONTRACT_VERSION = "1.0"
DEFAULT_REQUEST_TIMEOUT_SECONDS = 20.0
DEFAULT_MAL_BASE_URL = "https://api.myanimelist.net/v2"
DEFAULT_MAL_AUTH_URL = "https://myanimelist.net/v1/oauth2/authorize"
DEFAULT_MAL_TOKEN_URL = "https://myanimelist.net/v1/oauth2/token"
DEFAULT_MAL_BIND_HOST = "0.0.0.0"
DEFAULT_MAL_REDIRECT_HOST = "127.0.0.1"
DEFAULT_MAL_REDIRECT_PORT = 8765
DEFAULT_MAL_REQUEST_SPACING_SECONDS = 1.0
DEFAULT_MAL_REQUEST_SPACING_JITTER_SECONDS = 0.2
DEFAULT_CRUNCHYROLL_LOCALE = "en-US"
DEFAULT_CRUNCHYROLL_REQUEST_SPACING_SECONDS = 22.5
DEFAULT_CRUNCHYROLL_REQUEST_SPACING_JITTER_SECONDS = 7.5
DEFAULT_MAL_CLIENT_ID_FILE = "mal_client_id.txt"
DEFAULT_MAL_CLIENT_SECRET_FILE = "mal_client_secret.txt"
DEFAULT_MAL_ACCESS_TOKEN_FILE = "mal_access_token.txt"
DEFAULT_MAL_REFRESH_TOKEN_FILE = "mal_refresh_token.txt"
DEFAULT_DB_FILE = "mal_updater.sqlite3"
DEFAULT_RUNTIME_DIR_NAME = ".MAL-Updater"
DEFAULT_SERVICE_SYNC_EVERY_SECONDS = 6 * 60 * 60
DEFAULT_SERVICE_HEALTH_EVERY_SECONDS = 12 * 60 * 60
DEFAULT_SERVICE_MAL_REFRESH_EVERY_SECONDS = 6 * 60 * 60
DEFAULT_SERVICE_LOOP_SLEEP_SECONDS = 30
DEFAULT_SERVICE_CRUNCHYROLL_HOURLY_LIMIT = 180
DEFAULT_SERVICE_MAL_HOURLY_LIMIT = 120
DEFAULT_SERVICE_WARN_RATIO = 0.8
DEFAULT_SERVICE_CRITICAL_RATIO = 0.95
WORKSPACE_MARKER_FILES = ("AGENTS.md", "SOUL.md", "USER.md")


@dataclass(slots=True)
class MalSettings:
    base_url: str = DEFAULT_MAL_BASE_URL
    auth_url: str = DEFAULT_MAL_AUTH_URL
    token_url: str = DEFAULT_MAL_TOKEN_URL
    bind_host: str = DEFAULT_MAL_BIND_HOST
    redirect_host: str = DEFAULT_MAL_REDIRECT_HOST
    redirect_port: int = DEFAULT_MAL_REDIRECT_PORT
    request_spacing_seconds: float = DEFAULT_MAL_REQUEST_SPACING_SECONDS
    request_spacing_jitter_seconds: float = DEFAULT_MAL_REQUEST_SPACING_JITTER_SECONDS

    @property
    def redirect_uri(self) -> str:
        return f"http://{self.redirect_host}:{self.redirect_port}/callback"


@dataclass(slots=True)
class CrunchyrollSettings:
    locale: str = DEFAULT_CRUNCHYROLL_LOCALE
    request_spacing_seconds: float = DEFAULT_CRUNCHYROLL_REQUEST_SPACING_SECONDS
    request_spacing_jitter_seconds: float = DEFAULT_CRUNCHYROLL_REQUEST_SPACING_JITTER_SECONDS


@dataclass(slots=True)
class ServiceSettings:
    sync_every_seconds: int = DEFAULT_SERVICE_SYNC_EVERY_SECONDS
    health_every_seconds: int = DEFAULT_SERVICE_HEALTH_EVERY_SECONDS
    mal_refresh_every_seconds: int = DEFAULT_SERVICE_MAL_REFRESH_EVERY_SECONDS
    loop_sleep_seconds: int = DEFAULT_SERVICE_LOOP_SLEEP_SECONDS
    crunchyroll_hourly_limit: int = DEFAULT_SERVICE_CRUNCHYROLL_HOURLY_LIMIT
    mal_hourly_limit: int = DEFAULT_SERVICE_MAL_HOURLY_LIMIT
    provider_hourly_limits: dict[str, int] = field(default_factory=dict)
    warn_ratio: float = DEFAULT_SERVICE_WARN_RATIO
    critical_ratio: float = DEFAULT_SERVICE_CRITICAL_RATIO

    def hourly_limit_for(self, provider: str) -> int:
        if provider == "mal":
            return self.mal_hourly_limit
        if provider == "crunchyroll":
            return self.crunchyroll_hourly_limit
        value = self.provider_hourly_limits.get(provider)
        return int(value) if isinstance(value, int) else self.crunchyroll_hourly_limit


@dataclass(slots=True)
class MalSecrets:
    client_id: str | None
    client_secret: str | None
    access_token: str | None
    refresh_token: str | None
    client_id_path: Path
    client_secret_path: Path
    access_token_path: Path
    refresh_token_path: Path


@dataclass(slots=True)
class AppConfig:
    project_root: Path
    workspace_root: Path
    runtime_root: Path
    settings_path: Path
    config_dir: Path
    secrets_dir: Path
    data_dir: Path
    state_dir: Path
    cache_dir: Path
    db_path: Path
    secret_files: dict[str, Any] = field(default_factory=dict)
    completion_threshold: float = DEFAULT_COMPLETION_THRESHOLD
    credits_skip_window_seconds: int = DEFAULT_CREDITS_SKIP_WINDOW_SECONDS
    contract_version: str = DEFAULT_CONTRACT_VERSION
    request_timeout_seconds: float = DEFAULT_REQUEST_TIMEOUT_SECONDS
    mal: MalSettings = field(default_factory=MalSettings)
    crunchyroll: CrunchyrollSettings = field(default_factory=CrunchyrollSettings)
    service: ServiceSettings = field(default_factory=ServiceSettings)

    @property
    def service_log_path(self) -> Path:
        return self.state_dir / "logs" / "service.log"

    @property
    def service_state_path(self) -> Path:
        return self.state_dir / "service-state.json"

    @property
    def api_request_events_path(self) -> Path:
        return self.state_dir / "api-request-events.jsonl"

    @property
    def health_latest_json_path(self) -> Path:
        return self.state_dir / "health" / "latest-health-check.json"


def _resolve_from(base_dir: Path, raw_value: str | Path) -> Path:
    path = Path(raw_value).expanduser()
    if not path.is_absolute():
        path = base_dir / path
    return path.resolve()


def _discover_workspace_root(project_root: Path) -> Path:
    env_override = os.getenv("MAL_UPDATER_WORKSPACE_DIR") or os.getenv("OPENCLAW_WORKSPACE_DIR")
    if env_override:
        return _resolve_from(Path.cwd(), env_override)

    for candidate in (project_root, *project_root.parents):
        for marker in WORKSPACE_MARKER_FILES:
            if (candidate / marker).exists():
                return candidate.resolve()

    if project_root.parent.name == "skills" and project_root.parent.parent.exists():
        return project_root.parent.parent.resolve()

    return project_root.resolve()


def _default_runtime_root(workspace_root: Path) -> Path:
    env_override = os.getenv("MAL_UPDATER_RUNTIME_ROOT")
    if env_override:
        return _resolve_from(Path.cwd(), env_override)
    return (workspace_root / DEFAULT_RUNTIME_DIR_NAME).resolve()


def _get_table(data: dict[str, Any], name: str) -> dict[str, Any]:
    value = data.get(name)
    return value if isinstance(value, dict) else {}


def _get_str(data: dict[str, Any], key: str, default: str) -> str:
    value = data.get(key, default)
    return str(value)


def _get_float(data: dict[str, Any], key: str, default: float) -> float:
    value = data.get(key, default)
    return float(value)


def _get_int(data: dict[str, Any], key: str, default: int) -> int:
    value = data.get(key, default)
    return int(value)


def _resolve_path_setting(
    env_name: str,
    settings: dict[str, Any],
    key: str,
    *,
    base_dir: Path,
    default: Path,
) -> Path:
    env_value = os.getenv(env_name)
    if env_value:
        return _resolve_from(Path.cwd(), env_value)
    raw_value = settings.get(key)
    if raw_value is None:
        return _resolve_from(base_dir, default)
    return _resolve_from(base_dir, str(raw_value))


def _resolve_secret_path(
    env_name: str,
    settings: dict[str, Any],
    key: str,
    *,
    secrets_dir: Path,
    default_file: str,
) -> Path:
    env_value = os.getenv(env_name)
    if env_value:
        return _resolve_from(Path.cwd(), env_value)
    raw_value = settings.get(key)
    if raw_value is None:
        return (secrets_dir / default_file).resolve()
    return _resolve_from(secrets_dir, str(raw_value))


def _parse_toml_scalar(raw_value: str) -> Any:
    value = raw_value.strip()
    if value.startswith('"') and value.endswith('"'):
        return value[1:-1]
    lowered = value.lower()
    if lowered in {"true", "false"}:
        return lowered == "true"
    try:
        if "." in value:
            return float(value)
        return int(value)
    except ValueError:
        return value


def _simple_toml_loads(text: str) -> dict[str, Any]:
    root: dict[str, Any] = {}
    current = root
    for line in text.splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        if stripped.startswith("[") and stripped.endswith("]"):
            section_name = stripped[1:-1].strip()
            current = root.setdefault(section_name, {})
            if not isinstance(current, dict):
                raise ValueError(f"TOML section conflict for [{section_name}]")
            continue
        if "=" not in stripped:
            raise ValueError(f"Unsupported TOML line: {line}")
        key, raw_value = stripped.split("=", 1)
        current[key.strip()] = _parse_toml_scalar(raw_value)
    return root


def _read_toml_file(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    if tomllib is not None:
        with path.open("rb") as fh:
            data = tomllib.load(fh)
        if not isinstance(data, dict):
            raise ValueError(f"Expected top-level TOML table in {path}")
        return data
    data = _simple_toml_loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise ValueError(f"Expected top-level TOML table in {path}")
    return data


def _read_secret_file(path: Path) -> str | None:
    if not path.exists():
        return None
    value = path.read_text(encoding="utf-8").strip()
    return value or None


def load_config(project_root: Path | None = None) -> AppConfig:
    root = (project_root or Path(__file__).resolve().parents[2]).resolve()
    workspace_root = _discover_workspace_root(root)
    runtime_root = _default_runtime_root(workspace_root)
    default_config_dir = (runtime_root / "config").resolve()
    settings_path = _resolve_from(
        Path.cwd(),
        os.getenv("MAL_UPDATER_SETTINGS_PATH", str(default_config_dir / "settings.toml")),
    )
    settings = _read_toml_file(settings_path)

    paths_section = _get_table(settings, "paths")
    mal_section = _get_table(settings, "mal")
    crunchyroll_section = _get_table(settings, "crunchyroll")
    service_section = _get_table(settings, "service")
    service_provider_limits_section = _get_table(service_section, "provider_hourly_limits")
    secret_files_section = _get_table(settings, "secret_files")
    settings_dir = settings_path.parent

    config_dir = _resolve_path_setting(
        "MAL_UPDATER_CONFIG_DIR",
        paths_section,
        "config_dir",
        base_dir=settings_dir,
        default=runtime_root / "config",
    )
    secrets_dir = _resolve_path_setting(
        "MAL_UPDATER_SECRETS_DIR",
        paths_section,
        "secrets_dir",
        base_dir=settings_dir,
        default=runtime_root / "secrets",
    )
    data_dir = _resolve_path_setting(
        "MAL_UPDATER_DATA_DIR",
        paths_section,
        "data_dir",
        base_dir=settings_dir,
        default=runtime_root / "data",
    )
    state_dir = _resolve_path_setting(
        "MAL_UPDATER_STATE_DIR",
        paths_section,
        "state_dir",
        base_dir=settings_dir,
        default=runtime_root / "state",
    )
    cache_dir = _resolve_path_setting(
        "MAL_UPDATER_CACHE_DIR",
        paths_section,
        "cache_dir",
        base_dir=settings_dir,
        default=runtime_root / "cache",
    )
    db_path = _resolve_path_setting(
        "MAL_UPDATER_DB_PATH",
        paths_section,
        "db_path",
        base_dir=settings_dir,
        default=data_dir / DEFAULT_DB_FILE,
    )
    app_config = AppConfig(
        project_root=root,
        workspace_root=workspace_root,
        runtime_root=runtime_root,
        settings_path=settings_path,
        config_dir=config_dir,
        secrets_dir=secrets_dir,
        data_dir=data_dir,
        state_dir=state_dir,
        cache_dir=cache_dir,
        db_path=db_path,
        secret_files=secret_files_section,
        completion_threshold=float(os.getenv("MAL_UPDATER_COMPLETION_THRESHOLD", _get_float(settings, "completion_threshold", DEFAULT_COMPLETION_THRESHOLD))),
        credits_skip_window_seconds=int(
            os.getenv(
                "MAL_UPDATER_CREDITS_SKIP_WINDOW_SECONDS",
                _get_int(settings, "credits_skip_window_seconds", DEFAULT_CREDITS_SKIP_WINDOW_SECONDS),
            )
        ),
        contract_version=os.getenv("MAL_UPDATER_CONTRACT_VERSION", _get_str(settings, "contract_version", DEFAULT_CONTRACT_VERSION)),
        request_timeout_seconds=float(
            os.getenv("MAL_UPDATER_REQUEST_TIMEOUT_SECONDS", _get_float(settings, "request_timeout_seconds", DEFAULT_REQUEST_TIMEOUT_SECONDS))
        ),
        mal=MalSettings(
            base_url=os.getenv("MAL_UPDATER_MAL_BASE_URL", _get_str(mal_section, "base_url", DEFAULT_MAL_BASE_URL)),
            auth_url=os.getenv("MAL_UPDATER_MAL_AUTH_URL", _get_str(mal_section, "auth_url", DEFAULT_MAL_AUTH_URL)),
            token_url=os.getenv("MAL_UPDATER_MAL_TOKEN_URL", _get_str(mal_section, "token_url", DEFAULT_MAL_TOKEN_URL)),
            bind_host=os.getenv("MAL_UPDATER_MAL_BIND_HOST", _get_str(mal_section, "bind_host", DEFAULT_MAL_BIND_HOST)),
            redirect_host=os.getenv("MAL_UPDATER_MAL_REDIRECT_HOST", _get_str(mal_section, "redirect_host", DEFAULT_MAL_REDIRECT_HOST)),
            redirect_port=int(os.getenv("MAL_UPDATER_MAL_REDIRECT_PORT", _get_int(mal_section, "redirect_port", DEFAULT_MAL_REDIRECT_PORT))),
            request_spacing_seconds=float(
                os.getenv(
                    "MAL_UPDATER_MAL_REQUEST_SPACING_SECONDS",
                    _get_float(mal_section, "request_spacing_seconds", DEFAULT_MAL_REQUEST_SPACING_SECONDS),
                )
            ),
            request_spacing_jitter_seconds=float(
                os.getenv(
                    "MAL_UPDATER_MAL_REQUEST_SPACING_JITTER_SECONDS",
                    _get_float(mal_section, "request_spacing_jitter_seconds", DEFAULT_MAL_REQUEST_SPACING_JITTER_SECONDS),
                )
            ),
        ),
        crunchyroll=CrunchyrollSettings(
            locale=os.getenv("MAL_UPDATER_CRUNCHYROLL_LOCALE", _get_str(crunchyroll_section, "locale", DEFAULT_CRUNCHYROLL_LOCALE)),
            request_spacing_seconds=float(
                os.getenv(
                    "MAL_UPDATER_CRUNCHYROLL_REQUEST_SPACING_SECONDS",
                    _get_float(crunchyroll_section, "request_spacing_seconds", DEFAULT_CRUNCHYROLL_REQUEST_SPACING_SECONDS),
                )
            ),
            request_spacing_jitter_seconds=float(
                os.getenv(
                    "MAL_UPDATER_CRUNCHYROLL_REQUEST_SPACING_JITTER_SECONDS",
                    _get_float(
                        crunchyroll_section,
                        "request_spacing_jitter_seconds",
                        DEFAULT_CRUNCHYROLL_REQUEST_SPACING_JITTER_SECONDS,
                    ),
                )
            ),
        ),
        service=ServiceSettings(
            sync_every_seconds=int(os.getenv("MAL_UPDATER_SERVICE_SYNC_EVERY_SECONDS", _get_int(service_section, "sync_every_seconds", DEFAULT_SERVICE_SYNC_EVERY_SECONDS))),
            health_every_seconds=int(os.getenv("MAL_UPDATER_SERVICE_HEALTH_EVERY_SECONDS", _get_int(service_section, "health_every_seconds", DEFAULT_SERVICE_HEALTH_EVERY_SECONDS))),
            mal_refresh_every_seconds=int(os.getenv("MAL_UPDATER_SERVICE_MAL_REFRESH_EVERY_SECONDS", _get_int(service_section, "mal_refresh_every_seconds", DEFAULT_SERVICE_MAL_REFRESH_EVERY_SECONDS))),
            loop_sleep_seconds=int(os.getenv("MAL_UPDATER_SERVICE_LOOP_SLEEP_SECONDS", _get_int(service_section, "loop_sleep_seconds", DEFAULT_SERVICE_LOOP_SLEEP_SECONDS))),
            crunchyroll_hourly_limit=int(os.getenv("MAL_UPDATER_SERVICE_CRUNCHYROLL_HOURLY_LIMIT", _get_int(service_section, "crunchyroll_hourly_limit", DEFAULT_SERVICE_CRUNCHYROLL_HOURLY_LIMIT))),
            mal_hourly_limit=int(os.getenv("MAL_UPDATER_SERVICE_MAL_HOURLY_LIMIT", _get_int(service_section, "mal_hourly_limit", DEFAULT_SERVICE_MAL_HOURLY_LIMIT))),
            provider_hourly_limits={
                str(key): int(value)
                for key, value in service_provider_limits_section.items()
                if isinstance(key, str) and isinstance(value, (int, float))
            },
            warn_ratio=float(os.getenv("MAL_UPDATER_SERVICE_WARN_RATIO", _get_float(service_section, "warn_ratio", DEFAULT_SERVICE_WARN_RATIO))),
            critical_ratio=float(os.getenv("MAL_UPDATER_SERVICE_CRITICAL_RATIO", _get_float(service_section, "critical_ratio", DEFAULT_SERVICE_CRITICAL_RATIO))),
        ),
    )
    return app_config


def load_mal_secrets(config: AppConfig) -> MalSecrets:
    secret_files_section = config.secret_files
    client_id_path = _resolve_secret_path(
        "MAL_UPDATER_MAL_CLIENT_ID_FILE",
        secret_files_section,
        "mal_client_id",
        secrets_dir=config.secrets_dir,
        default_file=DEFAULT_MAL_CLIENT_ID_FILE,
    )
    client_secret_path = _resolve_secret_path(
        "MAL_UPDATER_MAL_CLIENT_SECRET_FILE",
        secret_files_section,
        "mal_client_secret",
        secrets_dir=config.secrets_dir,
        default_file=DEFAULT_MAL_CLIENT_SECRET_FILE,
    )
    access_token_path = _resolve_secret_path(
        "MAL_UPDATER_MAL_ACCESS_TOKEN_FILE",
        secret_files_section,
        "mal_access_token",
        secrets_dir=config.secrets_dir,
        default_file=DEFAULT_MAL_ACCESS_TOKEN_FILE,
    )
    refresh_token_path = _resolve_secret_path(
        "MAL_UPDATER_MAL_REFRESH_TOKEN_FILE",
        secret_files_section,
        "mal_refresh_token",
        secrets_dir=config.secrets_dir,
        default_file=DEFAULT_MAL_REFRESH_TOKEN_FILE,
    )

    return MalSecrets(
        client_id=os.getenv("MAL_UPDATER_MAL_CLIENT_ID") or _read_secret_file(client_id_path),
        client_secret=os.getenv("MAL_UPDATER_MAL_CLIENT_SECRET") or _read_secret_file(client_secret_path),
        access_token=os.getenv("MAL_UPDATER_MAL_ACCESS_TOKEN") or _read_secret_file(access_token_path),
        refresh_token=os.getenv("MAL_UPDATER_MAL_REFRESH_TOKEN") or _read_secret_file(refresh_token_path),
        client_id_path=client_id_path,
        client_secret_path=client_secret_path,
        access_token_path=access_token_path,
        refresh_token_path=refresh_token_path,
    )


def ensure_directories(config: AppConfig) -> None:
    for path in (
        config.runtime_root,
        config.config_dir,
        config.secrets_dir,
        config.data_dir,
        config.state_dir,
        config.cache_dir,
        config.service_log_path.parent,
        config.health_latest_json_path.parent,
    ):
        path.mkdir(parents=True, exist_ok=True)
