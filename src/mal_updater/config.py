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
DEFAULT_CRUNCHYROLL_LOCALE = "en-US"
DEFAULT_CRUNCHYROLL_REQUEST_SPACING_SECONDS = 10.0
DEFAULT_CRUNCHYROLL_REQUEST_SPACING_JITTER_SECONDS = 3.0
DEFAULT_MAL_CLIENT_ID_FILE = "mal_client_id.txt"
DEFAULT_MAL_CLIENT_SECRET_FILE = "mal_client_secret.txt"
DEFAULT_MAL_ACCESS_TOKEN_FILE = "mal_access_token.txt"
DEFAULT_MAL_REFRESH_TOKEN_FILE = "mal_refresh_token.txt"
DEFAULT_DB_FILE = "mal_updater.sqlite3"


@dataclass(slots=True)
class MalSettings:
    base_url: str = DEFAULT_MAL_BASE_URL
    auth_url: str = DEFAULT_MAL_AUTH_URL
    token_url: str = DEFAULT_MAL_TOKEN_URL
    bind_host: str = DEFAULT_MAL_BIND_HOST
    redirect_host: str = DEFAULT_MAL_REDIRECT_HOST
    redirect_port: int = DEFAULT_MAL_REDIRECT_PORT

    @property
    def redirect_uri(self) -> str:
        return f"http://{self.redirect_host}:{self.redirect_port}/callback"


@dataclass(slots=True)
class CrunchyrollSettings:
    locale: str = DEFAULT_CRUNCHYROLL_LOCALE
    request_spacing_seconds: float = DEFAULT_CRUNCHYROLL_REQUEST_SPACING_SECONDS


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


def _resolve_from(base_dir: Path, raw_value: str | Path) -> Path:
    path = Path(raw_value).expanduser()
    if not path.is_absolute():
        path = base_dir / path
    return path.resolve()


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

    default_config_dir = (root / "config").resolve()
    settings_path = _resolve_from(
        Path.cwd(),
        os.getenv("MAL_UPDATER_SETTINGS_PATH", str(default_config_dir / "settings.toml")),
    )
    settings = _read_toml_file(settings_path)

    paths_section = _get_table(settings, "paths")
    mal_section = _get_table(settings, "mal")
    crunchyroll_section = _get_table(settings, "crunchyroll")
    secret_files_section = _get_table(settings, "secret_files")
    settings_dir = settings_path.parent

    config_dir = _resolve_path_setting(
        "MAL_UPDATER_CONFIG_DIR",
        paths_section,
        "config_dir",
        base_dir=settings_dir,
        default=root / "config",
    )
    secrets_dir = _resolve_path_setting(
        "MAL_UPDATER_SECRETS_DIR",
        paths_section,
        "secrets_dir",
        base_dir=settings_dir,
        default=root / "secrets",
    )
    data_dir = _resolve_path_setting(
        "MAL_UPDATER_DATA_DIR",
        paths_section,
        "data_dir",
        base_dir=settings_dir,
        default=root / "data",
    )
    state_dir = _resolve_path_setting(
        "MAL_UPDATER_STATE_DIR",
        paths_section,
        "state_dir",
        base_dir=settings_dir,
        default=root / "state",
    )
    cache_dir = _resolve_path_setting(
        "MAL_UPDATER_CACHE_DIR",
        paths_section,
        "cache_dir",
        base_dir=settings_dir,
        default=root / "cache",
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
        ),
        crunchyroll=CrunchyrollSettings(
            locale=os.getenv("MAL_UPDATER_CRUNCHYROLL_LOCALE", _get_str(crunchyroll_section, "locale", DEFAULT_CRUNCHYROLL_LOCALE)),
            request_spacing_seconds=float(
                os.getenv(
                    "MAL_UPDATER_CRUNCHYROLL_REQUEST_SPACING_SECONDS",
                    _get_float(crunchyroll_section, "request_spacing_seconds", DEFAULT_CRUNCHYROLL_REQUEST_SPACING_SECONDS),
                )
            ),
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
    for path in (config.config_dir, config.secrets_dir, config.data_dir, config.state_dir, config.cache_dir):
        path.mkdir(parents=True, exist_ok=True)
