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
- log into Crunchyroll
- complete a local MAL OAuth callback exchange end-to-end
- ingest real watch data
- write anything to MAL

That line is intentional. The scaffold is real; the sync functionality is not being faked.

## Repository layout

- `src/mal_updater/` — Python worker package
- `migrations/` — SQLite schema migrations
- `docs/JSON_CONTRACT.md` — Rust ↔ Python boundary contract
- `docs/MAL_OAUTH.md` — local OAuth assumptions and secret conventions
- `docs/PYTHON_WORKER.md` — worker config/bootstrap notes
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
```

Or install editable and use the console script:

```bash
pip install -e .
mal-updater status
mal-updater init
mal-updater mal-auth-url
```

### Current behavior

- `status` prints resolved paths/config plus MAL secret presence
- `init` creates local directories and applies SQLite migrations
- `mal-auth-url` generates a real PKCE pair plus MAL authorization URL
- `sync` exists as a reserved entrypoint and exits with a clear "not implemented yet" message

## Rust adapter scaffold

### Requirements

- Rust stable toolchain

### Commands

```bash
cd rust/crunchyroll_adapter
cargo run -- snapshot --contract-version 1.0
cargo run -- auth status
cargo run -- auth login
```

Current adapter behavior:
- `snapshot` emits a valid `1.0` JSON snapshot scaffold
- `auth status` exposes the intended auth CLI shape without pretending auth exists
- `auth login` is a deliberate placeholder that exits non-zero with a clear message
- no command pretends to fetch live Crunchyroll data yet

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
