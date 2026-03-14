from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from .auth import OAuthCallbackError, format_auth_flow_prompt, persist_token_response, wait_for_oauth_callback
from .config import ensure_directories, load_config, load_mal_secrets
from .crunchyroll_auth import (
    CrunchyrollAuthError,
    crunchyroll_login_with_credentials,
    load_crunchyroll_credentials,
    resolve_crunchyroll_state_paths,
)
from .db import bootstrap_database
from .ingestion import ingest_snapshot_file, ingest_snapshot_payload
from .mal_client import MalApiError, MalClient
from .validation import SnapshotValidationError, validate_snapshot_payload


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
    crunchyroll_credentials = load_crunchyroll_credentials(config)
    crunchyroll_state = resolve_crunchyroll_state_paths(config)
    print(f"project_root={config.project_root}")
    print(f"settings_path={config.settings_path}")
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
    print(f"mal.bind_host={config.mal.bind_host}")
    print(f"mal.redirect_uri={config.mal.redirect_uri}")
    print(f"crunchyroll.locale={config.crunchyroll.locale}")
    print(f"crunchyroll.username_present={bool(crunchyroll_credentials.username)}")
    print(f"crunchyroll.password_present={bool(crunchyroll_credentials.password)}")
    print(f"crunchyroll.username_path={crunchyroll_credentials.username_path}")
    print(f"crunchyroll.password_path={crunchyroll_credentials.password_path}")
    print(f"crunchyroll.state_root={crunchyroll_state.root}")
    print(f"crunchyroll.refresh_token_path={crunchyroll_state.refresh_token_path}")
    print(f"crunchyroll.device_id_path={crunchyroll_state.device_id_path}")
    print(f"crunchyroll.session_state_path={crunchyroll_state.session_state_path}")
    print(f"crunchyroll.refresh_token_present={crunchyroll_state.refresh_token_path.exists()}")
    print(f"crunchyroll.device_id_present={crunchyroll_state.device_id_path.exists()}")
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
    try:
        auth_url = client.build_authorization_url(code_challenge=pkce.code_challenge)
    except MalApiError as exc:
        print(str(exc), file=sys.stderr)
        return 1
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


def _cmd_mal_auth_login(project_root: Path | None, timeout_seconds: float, verify_whoami: bool) -> int:
    config = load_config(project_root)
    ensure_directories(config)
    secrets = load_mal_secrets(config)
    client = MalClient(config, secrets)
    pkce = client.generate_pkce_pair()
    state = client.generate_state()
    try:
        auth_url = client.build_authorization_url(code_challenge=pkce.code_challenge, state=state)
    except MalApiError as exc:
        print(str(exc), file=sys.stderr)
        return 1

    print(format_auth_flow_prompt(config, auth_url, timeout_seconds))
    try:
        callback = wait_for_oauth_callback(
            config.mal.bind_host,
            config.mal.redirect_port,
            expected_state=state,
            timeout_seconds=timeout_seconds,
        )
        token = client.exchange_code(callback.code, pkce.code_verifier)
        persisted = persist_token_response(token, secrets)
    except OSError as exc:
        print(f"Unable to start MAL callback listener on {config.mal.redirect_uri}: {exc}", file=sys.stderr)
        return 1
    except TimeoutError as exc:
        print(str(exc), file=sys.stderr)
        return 1
    except (OAuthCallbackError, MalApiError) as exc:
        print(str(exc), file=sys.stderr)
        return 1

    print()
    print(f"Persisted access token to {persisted.access_token_path}")
    if token.refresh_token:
        print(f"Persisted refresh token to {persisted.refresh_token_path}")
    else:
        print("No refresh token returned by MAL; existing refresh token file left untouched")

    if not verify_whoami:
        return 0

    try:
        whoami = client.get_my_user(access_token=token.access_token)
    except MalApiError as exc:
        print(f"Token exchange succeeded, but /users/@me verification failed: {exc}", file=sys.stderr)
        return 1

    print(f"Authenticated MAL user: {json.dumps(whoami, indent=2)}")
    return 0


def _cmd_mal_refresh(project_root: Path | None, verify_whoami: bool) -> int:
    config = load_config(project_root)
    ensure_directories(config)
    secrets = load_mal_secrets(config)
    client = MalClient(config, secrets)
    try:
        token = client.refresh_access_token()
        persisted = persist_token_response(token, secrets)
    except MalApiError as exc:
        print(str(exc), file=sys.stderr)
        return 1

    print(f"Persisted access token to {persisted.access_token_path}")
    if token.refresh_token:
        print(f"Persisted refresh token to {persisted.refresh_token_path}")

    if not verify_whoami:
        return 0

    try:
        whoami = client.get_my_user(access_token=token.access_token)
    except MalApiError as exc:
        print(f"Refresh succeeded, but /users/@me verification failed: {exc}", file=sys.stderr)
        return 1

    print(f"Authenticated MAL user: {json.dumps(whoami, indent=2)}")
    return 0


def _cmd_mal_whoami(project_root: Path | None) -> int:
    config = load_config(project_root)
    secrets = load_mal_secrets(config)
    client = MalClient(config, secrets)
    try:
        whoami = client.get_my_user()
    except MalApiError as exc:
        print(str(exc), file=sys.stderr)
        return 1
    print(json.dumps(whoami, indent=2))
    return 0


def _cmd_crunchyroll_auth_login(project_root: Path | None, profile: str, no_verify: bool) -> int:
    config = load_config(project_root)
    ensure_directories(config)
    try:
        result = crunchyroll_login_with_credentials(
            config,
            profile=profile,
            verify_account=not no_verify,
        )
    except CrunchyrollAuthError as exc:
        print(str(exc), file=sys.stderr)
        return 1

    print(f"Staged Crunchyroll refresh token to {result.refresh_token_path}")
    print(f"Staged Crunchyroll device id to {result.device_id_path}")
    print(f"Updated session state at {result.session_state_path}")
    if result.account_id:
        print(f"Crunchyroll account_id={result.account_id}")
    if result.account_email:
        print(f"Crunchyroll account_email={result.account_email}")
    print(f"profile={result.profile}")
    print(f"locale={result.locale}")
    print(f"device_type={result.device_type}")
    return 0


def _cmd_validate_snapshot(project_root: Path | None, snapshot_path: Path | None) -> int:
    config = load_config(project_root)
    if snapshot_path is None:
        payload = json.load(sys.stdin)
        source = "stdin"
    else:
        payload = json.loads(snapshot_path.read_text(encoding="utf-8"))
        source = str(snapshot_path)
    try:
        validate_snapshot_payload(payload)
    except SnapshotValidationError as exc:
        print(f"INVALID: {exc}", file=sys.stderr)
        return 1
    print(f"VALID: {source}")
    return 0


def _cmd_ingest_snapshot(project_root: Path | None, snapshot_path: Path | None) -> int:
    config = load_config(project_root)
    ensure_directories(config)
    if snapshot_path is None:
        payload = json.load(sys.stdin)
        summary = ingest_snapshot_payload(payload, config)
    else:
        summary = ingest_snapshot_file(snapshot_path, config)
    print(json.dumps(summary.as_dict(), indent=2))
    return 0


def _cmd_sync(_: Path | None) -> int:
    raise SystemExit(
        "Sync pipeline not implemented yet. This scaffold provides config loading, validation, ingestion, MAL OAuth prep, and DB bootstrap only."
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="mal-updater")
    parser.add_argument("--project-root", type=Path, default=None, help="Override project root")
    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser("init", help="Create local dirs and initialize SQLite schema")
    subparsers.add_parser("status", help="Print resolved config, paths, and secret presence")
    mal_auth = subparsers.add_parser("mal-auth-url", help="Generate a MAL OAuth authorization URL + PKCE verifier")
    mal_auth.add_argument("--json", action="store_true", help="Emit machine-readable JSON")
    mal_auth_login = subparsers.add_parser("mal-auth-login", help="Run a local loopback MAL OAuth flow and persist returned tokens")
    mal_auth_login.add_argument("--timeout-seconds", type=float, default=300.0, help="How long to wait for the local callback before failing")
    mal_auth_login.add_argument("--no-verify", action="store_true", help="Skip the follow-up GET /users/@me token check")
    mal_refresh = subparsers.add_parser("mal-refresh", help="Refresh the persisted MAL access token using the local refresh token")
    mal_refresh.add_argument("--no-verify", action="store_true", help="Skip the follow-up GET /users/@me token check")
    subparsers.add_parser("mal-whoami", help="Call MAL GET /users/@me with the currently configured access token")
    crunchyroll_auth_login = subparsers.add_parser(
        "crunchyroll-auth-login",
        help="Use local Crunchyroll username/password secrets to stage adapter refresh-token auth material",
    )
    crunchyroll_auth_login.add_argument("--profile", default="default", help="Crunchyroll adapter profile name")
    crunchyroll_auth_login.add_argument("--no-verify", action="store_true", help="Skip the follow-up GET /accounts/v1/me token check")
    validate_snapshot = subparsers.add_parser("validate-snapshot", help="Validate a Crunchyroll snapshot JSON payload")
    validate_snapshot.add_argument("snapshot", nargs="?", type=Path, help="Snapshot JSON file path (defaults to stdin)")
    ingest_snapshot = subparsers.add_parser("ingest-snapshot", help="Validate and ingest a Crunchyroll snapshot into SQLite")
    ingest_snapshot.add_argument("snapshot", nargs="?", type=Path, help="Snapshot JSON file path (defaults to stdin)")
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
    if args.command == "mal-auth-login":
        return _cmd_mal_auth_login(args.project_root, args.timeout_seconds, verify_whoami=not args.no_verify)
    if args.command == "mal-refresh":
        return _cmd_mal_refresh(args.project_root, verify_whoami=not args.no_verify)
    if args.command == "mal-whoami":
        return _cmd_mal_whoami(args.project_root)
    if args.command == "crunchyroll-auth-login":
        return _cmd_crunchyroll_auth_login(args.project_root, args.profile, args.no_verify)
    if args.command == "validate-snapshot":
        return _cmd_validate_snapshot(args.project_root, args.snapshot)
    if args.command == "ingest-snapshot":
        return _cmd_ingest_snapshot(args.project_root, args.snapshot)
    if args.command == "sync":
        return _cmd_sync(args.project_root)
    parser.error(f"Unknown command: {args.command}")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
