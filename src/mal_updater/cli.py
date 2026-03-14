from __future__ import annotations

import argparse
from pathlib import Path

from .config import ensure_directories, load_config
from .db import apply_migrations, connect



def _cmd_init(project_root: Path | None) -> int:
    config = load_config(project_root)
    ensure_directories(config)
    with connect(config.db_path) as conn:
        apply_migrations(conn)
    print(f"Initialized MAL-Updater scaffold at {config.project_root}")
    print(f"SQLite database: {config.db_path}")
    return 0



def _cmd_status(project_root: Path | None) -> int:
    config = load_config(project_root)
    print(f"project_root={config.project_root}")
    print(f"config_dir={config.config_dir}")
    print(f"secrets_dir={config.secrets_dir}")
    print(f"data_dir={config.data_dir}")
    print(f"state_dir={config.state_dir}")
    print(f"cache_dir={config.cache_dir}")
    print(f"db_path={config.db_path}")
    print(f"contract_version={config.contract_version}")
    print(f"completion_threshold={config.completion_threshold}")
    print(f"crunchyroll_adapter_bin={config.crunchyroll_adapter_bin}")
    return 0



def _cmd_sync(_: Path | None) -> int:
    raise SystemExit(
        "Sync pipeline not implemented yet. This scaffold only provides project layout, config loading, and DB bootstrap."
    )



def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="mal-updater")
    parser.add_argument("--project-root", type=Path, default=None, help="Override project root")
    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser("init", help="Create local dirs and initialize SQLite schema")
    subparsers.add_parser("status", help="Print resolved config and paths")
    subparsers.add_parser("sync", help="Reserved for future sync orchestration")
    return parser



def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    if args.command == "init":
        return _cmd_init(args.project_root)
    if args.command == "status":
        return _cmd_status(args.project_root)
    if args.command == "sync":
        return _cmd_sync(args.project_root)
    parser.error(f"Unknown command: {args.command}")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
