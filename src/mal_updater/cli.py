from __future__ import annotations

import argparse
import json
from pathlib import Path

from .config import ensure_directories, load_config, load_mal_secrets
from .db import bootstrap_database
from .mal_client import MalClient


def _cmd_init(project_root: Path | None) -> int:
    config = load_config(project_root)
    ensure_directories(config)
    bootstrap_database(config.db_path)
    print(f"Initialized MAL-Updater scaffold at {config.project_root}")
    print(f"SQLite database: {config.db_path}")
    return 0


def _cmd_status(project_root: Path | None) -> int:
    config = load_config(project_root)
    secrets = load_mal_secrets(config)
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
    print(f"mal.base_url={config.mal.base_url}")
    print(f"mal.auth_url={config.mal.auth_url}")
    print(f"mal.token_url={config.mal.token_url}")
    print(f"mal.redirect_uri={config.mal.redirect_uri}")
    print(f"crunchyroll.locale={config.crunchyroll.locale}")
    print(f"mal.client_id_present={bool(secrets.client_id)}")
    print(f"mal.client_secret_present={bool(secrets.client_secret)}")
    print(f"mal.access_token_present={bool(secrets.access_token)}")
    print(f"mal.refresh_token_present={bool(secrets.refresh_token)}")
    print(f"mal.client_id_path={secrets.client_id_path}")
    print(f"mal.client_secret_path={secrets.client_secret_path}")
    print(f"mal.access_token_path={secrets.access_token_path}")
    print(f"mal.refresh_token_path={secrets.refresh_token_path}")
    return 0


def _cmd_mal_auth_url(project_root: Path | None, emit_json: bool) -> int:
    config = load_config(project_root)
    ensure_directories(config)
    client = MalClient(config, load_mal_secrets(config))
    pkce = client.generate_pkce_pair()
    auth_url = client.build_authorization_url(code_challenge=pkce.code_challenge)
    if emit_json:
        print(
            json.dumps(
                {
                    "authorization_url": auth_url,
                    "redirect_uri": config.mal.redirect_uri,
                    "code_verifier": pkce.code_verifier,
                    "code_challenge": pkce.code_challenge,
                },
                indent=2,
            )
        )
        return 0
    print("Open this URL in a browser after writing down the code verifier:")
    print(auth_url)
    print()
    print("code_verifier=")
    print(pkce.code_verifier)
    print()
    print(f"redirect_uri={config.mal.redirect_uri}")
    return 0


def _cmd_sync(_: Path | None) -> int:
    raise SystemExit(
        "Sync pipeline not implemented yet. This scaffold provides config loading, MAL OAuth prep, and DB bootstrap only."
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="mal-updater")
    parser.add_argument("--project-root", type=Path, default=None, help="Override project root")
    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser("init", help="Create local dirs and initialize SQLite schema")
    subparsers.add_parser("status", help="Print resolved config, paths, and secret presence")
    mal_auth = subparsers.add_parser("mal-auth-url", help="Generate a MAL OAuth authorization URL + PKCE verifier")
    mal_auth.add_argument("--json", action="store_true", help="Emit machine-readable JSON")
    subparsers.add_parser("sync", help="Reserved for future sync orchestration")
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    if args.command == "init":
        return _cmd_init(args.project_root)
    if args.command == "status":
        return _cmd_status(args.project_root)
    if args.command == "mal-auth-url":
        return _cmd_mal_auth_url(args.project_root, args.json)
    if args.command == "sync":
        return _cmd_sync(args.project_root)
    parser.error(f"Unknown command: {args.command}")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
