from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

try:
    import tomllib
except ModuleNotFoundError:  # pragma: no cover - Python < 3.11
    tomllib = None

DEFAULT_COMPLETION_THRESHOLD = 0.90
DEFAULT_CONTRACT_VERSION = "1.0"
DEFAULT_MAL_BASE_URL = "https://api.myanimelist.net/v2"
DEFAULT_MAL_AUTH_URL = "https://myanimelist.net/v1/oauth2/authorize"
DEFAULT_MAL_TOKEN_URL = "https://myanimelist.net/v1/oauth2/token"
DEFAULT_MAL_REDIRECT_HOST = "127.0.0.1"
DEFAULT_MAL_REDIRECT_PORT = 8765
DEFAULT_CRUNCHYROLL_LOCALE = "en-US"
DEFAULT_MAL_CLIENT_ID_FILE = "mal_client_id.txt"
DEFAULT_MAL_CLIENT_SECRET_FILE = "mal_client_secret.txt"
DEFAULT_MAL_ACCESS_TOKEN_FILE = "mal_access_token.txt"
DEFAULT_MAL_REFRESH_TOKEN_FILE = "mal_refresh_token.txt"


@dataclass(slots=True)
class MalSettings:
    base_url: str = DEFAULT_MAL_BASE_URL
    auth_url: str = DEFAULT_MAL_AUTH_URL
    token_url: str = DEFAULT_MAL_TOKEN_URL
    redirect_host: str = DEFAULT_MAL_REDIRECT_HOST
    redirect_port: int = DEFAULT_MAL_REDIRECT_PORT

    @property
    def redirect_uri(self) -> str:
        return f"http://{self.redirect_host}:{self.redirect_port}/callback"


@dataclass(slots=True)
class CrunchyrollSettings:
    locale: str = DEFAULT_CRUNCHYROLL_LOCALE


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
    config_dir: Path
    secrets_dir: Path
    data_dir: Path
    state_dir: Path
    cache_dir: Path
    db_path: Path
    crunchyroll_adapter_bin: Path
    completion_threshold: float = DEFAULT_COMPLETION_THRESHOLD
    contract_version: str = DEFAULT_CONTRACT_VERSION
    mal: MalSettings = field(default_factory=MalSettings)
    crunchyroll: CrunchyrollSettings = field(default_factory=CrunchyrollSettings)


def _resolve_dir(env_name: str, default: Path) -> Path:
    value = os.getenv(env_name)
    return Path(value).expanduser().resolve() if value else default.resolve()


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
    config_dir = _resolve_dir("MAL_UPDATER_CONFIG_DIR", root / "config")
    secrets_dir = _resolve_dir("MAL_UPDATER_SECRETS_DIR", root / "secrets")
    data_dir = _resolve_dir("MAL_UPDATER_DATA_DIR", root / "data")
    state_dir = _resolve_dir("MAL_UPDATER_STATE_DIR", root / "state")
    cache_dir = _resolve_dir("MAL_UPDATER_CACHE_DIR", root / "cache")
    settings_path = Path(os.getenv("MAL_UPDATER_SETTINGS_PATH", str(config_dir / "settings.toml"))).expanduser().resolve()
    settings = _read_toml_file(settings_path)

    mal_section = settings.get("mal") if isinstance(settings.get("mal"), dict) else {}
    crunchyroll_section = settings.get("crunchyroll") if isinstance(settings.get("crunchyroll"), dict) else {}

    db_path = Path(
        os.getenv("MAL_UPDATER_DB_PATH", str(data_dir / "mal_updater.sqlite3"))
    ).expanduser().resolve()
    crunchyroll_adapter_bin = Path(
        os.getenv(
            "MAL_UPDATER_CRUNCHYROLL_ADAPTER",
            str(root / "rust" / "crunchyroll_adapter" / "target" / "debug" / "crunchyroll-adapter"),
        )
    ).expanduser().resolve()

    return AppConfig(
        project_root=root,
        config_dir=config_dir,
        secrets_dir=secrets_dir,
        data_dir=data_dir,
        state_dir=state_dir,
        cache_dir=cache_dir,
        db_path=db_path,
        crunchyroll_adapter_bin=crunchyroll_adapter_bin,
        completion_threshold=float(os.getenv("MAL_UPDATER_COMPLETION_THRESHOLD", settings.get("completion_threshold", DEFAULT_COMPLETION_THRESHOLD))),
        contract_version=str(os.getenv("MAL_UPDATER_CONTRACT_VERSION", settings.get("contract_version", DEFAULT_CONTRACT_VERSION))),
        mal=MalSettings(
            base_url=str(os.getenv("MAL_UPDATER_MAL_BASE_URL", mal_section.get("base_url", DEFAULT_MAL_BASE_URL))),
            auth_url=str(os.getenv("MAL_UPDATER_MAL_AUTH_URL", mal_section.get("auth_url", DEFAULT_MAL_AUTH_URL))),
            token_url=str(os.getenv("MAL_UPDATER_MAL_TOKEN_URL", mal_section.get("token_url", DEFAULT_MAL_TOKEN_URL))),
            redirect_host=str(os.getenv("MAL_UPDATER_MAL_REDIRECT_HOST", mal_section.get("redirect_host", DEFAULT_MAL_REDIRECT_HOST))),
            redirect_port=int(os.getenv("MAL_UPDATER_MAL_REDIRECT_PORT", mal_section.get("redirect_port", DEFAULT_MAL_REDIRECT_PORT))),
        ),
        crunchyroll=CrunchyrollSettings(
            locale=str(os.getenv("MAL_UPDATER_CRUNCHYROLL_LOCALE", crunchyroll_section.get("locale", DEFAULT_CRUNCHYROLL_LOCALE)))
        ),
    )


def load_mal_secrets(config: AppConfig) -> MalSecrets:
    client_id_path = Path(os.getenv("MAL_UPDATER_MAL_CLIENT_ID_FILE", str(config.secrets_dir / DEFAULT_MAL_CLIENT_ID_FILE))).expanduser().resolve()
    client_secret_path = Path(os.getenv("MAL_UPDATER_MAL_CLIENT_SECRET_FILE", str(config.secrets_dir / DEFAULT_MAL_CLIENT_SECRET_FILE))).expanduser().resolve()
    access_token_path = Path(os.getenv("MAL_UPDATER_MAL_ACCESS_TOKEN_FILE", str(config.secrets_dir / DEFAULT_MAL_ACCESS_TOKEN_FILE))).expanduser().resolve()
    refresh_token_path = Path(os.getenv("MAL_UPDATER_MAL_REFRESH_TOKEN_FILE", str(config.secrets_dir / DEFAULT_MAL_REFRESH_TOKEN_FILE))).expanduser().resolve()

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
