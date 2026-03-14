# MAL-Updater

Local Crunchyroll → MyAnimeList sync and recommendation integration for OpenClaw on the Orin.

## Current status

This repo now has an initial scaffold for:
- a Python worker/orchestrator
- a Rust Crunchyroll adapter boundary
- a versioned JSON contract between them
- SQLite bootstrap migrations
- local config/secrets/data directory conventions
- MAL OAuth + API client scaffolding on the Python side

What it does **not** do yet:
- complete end-to-end Crunchyroll -> MAL sync behavior
- write sync updates back to MAL
- blindly auto-resolve mappings or perform live MAL mutations

What it can do now beyond ingestion:
- search MAL for conservative mapping candidates from the ingested Crunchyroll SQLite dataset
- generate guarded read-only dry-run sync proposals
- explicitly skip any proposal that would decrease MAL progress or downgrade a completed entry

That line is intentional. The scaffold is real, and the remaining live integration gaps are not being faked.

## Repository layout

- `src/mal_updater/` — Python worker package
- `migrations/` — SQLite schema migrations
- `docs/JSON_CONTRACT.md` — Rust ↔ Python boundary contract
- `docs/MAL_OAUTH.md` — MAL OAuth flow, callback model, and secret conventions
- `docs/PYTHON_WORKER.md` — worker config/bootstrap notes
- `docs/DECISIONS.md` — durable architectural and policy decisions
- `docs/CURRENT_STATUS.md` — current implemented state vs missing pieces
- `docs/OPERATIONS.md` — local operational commands and expectations
- `docs/CRUNCHYROLL_ADAPTER.md` — Rust adapter state/auth-material notes and current blocker
- `docs/contracts/` — JSON schema for adapter payloads
- `rust/crunchyroll_adapter/` — Rust adapter crate scaffold
- `config/` — non-secret local config examples
- `secrets/` — local-only secret material (ignored by git)
- `examples/` — environment override examples

## Local directory conventions

Safe defaults are relative to the repo root:

- `config/` — committed examples and optional local non-secret config
- `secrets/` — credentials/tokens only, gitignored
- `data/` — SQLite DB and durable application data, gitignored
- `state/` — transient run state, checkpoints, gitignored
- `cache/` — fetch/cache scratch space, gitignored

Optional environment overrides are documented in `examples/env.example`.

## Python worker scaffold

### Requirements

- Python 3.11+

### Commands

Run from the repo root:

```bash
PYTHONPATH=src python3 -m mal_updater.cli status
PYTHONPATH=src python3 -m mal_updater.cli init
PYTHONPATH=src python3 -m mal_updater.cli mal-auth-url
PYTHONPATH=src python3 -m mal_updater.cli mal-auth-login
PYTHONPATH=src python3 -m mal_updater.cli mal-refresh
PYTHONPATH=src python3 -m mal_updater.cli mal-whoami
PYTHONPATH=src python3 -m mal_updater.cli crunchyroll-auth-login
PYTHONPATH=src python3 -m mal_updater.cli crunchyroll-fetch-snapshot --out cache/live-crunchyroll-snapshot.json
PYTHONPATH=src python3 -m mal_updater.cli crunchyroll-fetch-snapshot --out cache/live-crunchyroll-snapshot.json --ingest
PYTHONPATH=src python3 -m mal_updater.cli validate-snapshot path/to/snapshot.json
PYTHONPATH=src python3 -m mal_updater.cli ingest-snapshot path/to/snapshot.json
PYTHONPATH=src python3 -m mal_updater.cli map-series --limit 20 --mapping-limit 5
PYTHONPATH=src python3 -m mal_updater.cli dry-run-sync --limit 20 --mapping-limit 5
```

Or install editable and use the console script:

```bash
pip install -e .
# optional but currently practical for Crunchyroll auth on this host:
pip install -e '.[crunchyroll]'
mal-updater status
mal-updater init
mal-updater mal-auth-url
```

### Tests

Run the current stdlib smoke/integration tests with:

```bash
PYTHONPATH=src python3 -m unittest discover -s tests -v
```

This repo currently relies on the built-in `unittest` runner for local verification; `pytest` is not required or assumed to be installed on the host.

### Current behavior

- `status` prints resolved paths/config plus MAL and Crunchyroll secret/state presence
- `config/settings.toml` can define path layout, MAL endpoint settings, Crunchyroll locale, and secret file locations
- `validate-snapshot` checks adapter JSON shape and cross-reference sanity before ingestion work touches SQLite
- `init` creates local directories and applies SQLite migrations
- `mal-auth-url` generates a real PKCE pair plus MAL authorization URL
- `mal-auth-login` starts a local callback listener bound to the configured host, exchanges the returned code for tokens, persists them under `secrets/`, and verifies the token with `GET /users/@me`
- `mal-refresh` refreshes a persisted MAL token pair and writes updated token files back to `secrets/`
- `mal-whoami` exercises the current access token against MAL `GET /users/@me`
- `crunchyroll-auth-login` uses local Crunchyroll username/password secrets to fetch a real refresh token + device id and stage them into `state/crunchyroll/<profile>/`; if optional `curl_cffi` support is installed, it uses browser-TLS impersonation to get through Crunchyroll's Cloudflare layer
- `crunchyroll-fetch-snapshot` is now the practical live Crunchyroll path: it refreshes auth through the Python impersonated transport, fetches account/history/watchlist data, normalizes it into the JSON contract, and can write a snapshot file and/or ingest it directly
- a real live run on this host now succeeds through the Python path and ingests into SQLite (`series_count=219`, `progress_count=4311`, `watchlist_count=10`)
- `validate-snapshot` strictly validates a Crunchyroll snapshot payload against the current Python-side contract rules
- `ingest-snapshot` validates then upserts normalized snapshot data into SQLite, recording a summary row in `sync_runs`
- `map-series` searches MAL for conservative candidate matches for recently seen Crunchyroll series and reports confidence / ambiguity instead of silently persisting guesses
- `dry-run-sync` builds read-only MAL list proposals from ingested Crunchyroll progress plus MAL list state, and only suggests forward-safe updates
- `sync` exists as a reserved entrypoint and exits with a clear message pointing at `dry-run-sync`; live MAL writes are still disabled on purpose

## Rust adapter scaffold

### Requirements

- Rust `1.88.0` for the adapter crate
- `rustup` recommended on the Orin; the repo pins the adapter locally with `rust/crunchyroll_adapter/rust-toolchain.toml`

### Commands

```bash
cd rust/crunchyroll_adapter
cargo run -- auth status
cargo run -- auth save-refresh-token --refresh-token-file /path/to/refresh_token.txt [--device-id-file /path/to/device_id.txt]
cargo run -- snapshot --contract-version 1.0
```

Current adapter behavior:
- `auth status` reports the resolved adapter state paths plus refresh-token / device-id presence
- `auth save-refresh-token` stages a local Crunchyroll refresh token (and optional device id) into the adapter state directory
- the adapter has a concrete local state convention under `state/crunchyroll/<profile>/`
- `snapshot` now attempts a real `crunchyroll-rs` refresh-token login when auth material is present
- `snapshot` honestly returns `auth_material_missing`, `auth_failed`, or `ok` in `raw.status`
- on success, the adapter fetches account/watch-history/watchlist data and normalizes it into the current JSON contract
- direct credential login is supported by `crunchyroll-rs`, but on this host the practical live bootstrap path is currently Python-side `crunchyroll-auth-login` with optional `curl_cffi`; plain `requests` gets a Cloudflare 403 interstitial, while `curl_cffi` succeeds in minting a refresh token
- the currently built Rust adapter still fails its follow-up refresh-token login on this host, so the remaining blocker looks transport/anti-bot related rather than missing credentials alone; see `docs/CRUNCHYROLL_ADAPTER.md`

## SQLite schema

Initial migration: `migrations/001_initial.sql`

Current tables:
- `provider_series`
- `provider_episode_progress`
- `provider_watchlist`
- `mal_series_mapping`
- `review_queue`
- `sync_runs`
- `schema_migrations`

This is enough to start read-only ingestion and later add mapping + review workflows without redesigning the repo layout.

## JSON contract

Current version: `1.0`

See:
- `docs/JSON_CONTRACT.md`
- `docs/contracts/crunchyroll_snapshot.schema.json`

Boundary rule:
- Rust produces normalized provider snapshots
- Python validates/persists them and owns all MAL-side decision making

## Security notes

- Do not commit real credentials
- Keep secrets under `secrets/`
- `config/settings.toml` is treated as local-only
- The repo is intended to remain private for now

## Planned architecture

- Python orchestration worker
- Rust Crunchyroll adapter (`crunchyroll-rs` later)
- SQLite state database
- MAL official OAuth/API client
- Safe dry-run sync before live writes
