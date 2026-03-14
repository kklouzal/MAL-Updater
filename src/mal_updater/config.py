from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

DEFAULT_COMPLETION_THRESHOLD = 0.90
DEFAULT_CONTRACT_VERSION = "1.0"


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



def _resolve_dir(env_name: str, default: Path) -> Path:
    value = os.getenv(env_name)
    return Path(value).expanduser().resolve() if value else default.resolve()



def load_config(project_root: Path | None = None) -> AppConfig:
    root = (project_root or Path(__file__).resolve().parents[2]).resolve()
    config_dir = _resolve_dir("MAL_UPDATER_CONFIG_DIR", root / "config")
    secrets_dir = _resolve_dir("MAL_UPDATER_SECRETS_DIR", root / "secrets")
    data_dir = _resolve_dir("MAL_UPDATER_DATA_DIR", root / "data")
    state_dir = _resolve_dir("MAL_UPDATER_STATE_DIR", root / "state")
    cache_dir = _resolve_dir("MAL_UPDATER_CACHE_DIR", root / "cache")
    db_path = Path(os.getenv("MAL_UPDATER_DB_PATH", str(data_dir / "mal_updater.sqlite3"))).expanduser().resolve()
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
        completion_threshold=float(os.getenv("MAL_UPDATER_COMPLETION_THRESHOLD", DEFAULT_COMPLETION_THRESHOLD)),
        contract_version=os.getenv("MAL_UPDATER_CONTRACT_VERSION", DEFAULT_CONTRACT_VERSION),
    )



def ensure_directories(config: AppConfig) -> None:
    for path in (config.config_dir, config.secrets_dir, config.data_dir, config.state_dir, config.cache_dir):
        path.mkdir(parents=True, exist_ok=True)
