# MAL-Updater

Local Crunchyroll → MyAnimeList sync and recommendation groundwork for OpenClaw on the Orin.

## Current status

This repo is now a **Python-only** implementation.

What it does today:
- real MAL OAuth and MAL API reads/writes
- real Crunchyroll auth bootstrap from local username/password secrets
- real live Crunchyroll snapshot fetches through the Python transport
- strict snapshot validation
- SQLite ingestion of Crunchyroll watch history/watchlist data
- conservative MAL mapping review workflows
- guarded dry-run sync planning
- a first live MAL executor that only applies approved, forward-safe updates

What it does **not** do yet:
- complete unattended end-to-end sync behavior
- auto-resolve ambiguous mappings
- perform broad or reckless MAL mutations
- ship recommendation features yet

That line is intentional. The working path is real, and the remaining gaps are not being faked.

## Repository layout

- `src/mal_updater/` — Python application package
- `migrations/` — SQLite schema migrations
- `docs/JSON_CONTRACT.md` — normalized Crunchyroll snapshot contract
- `docs/MAL_OAUTH.md` — MAL OAuth flow, callback model, and secret conventions
- `docs/PYTHON_WORKER.md` — worker/config/bootstrap notes
- `docs/DECISIONS.md` — durable architectural and policy decisions
- `docs/CURRENT_STATUS.md` — current implemented state vs missing pieces
- `docs/OPERATIONS.md` — local operational commands and expectations
- `docs/contracts/` — JSON schema for snapshot payloads
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

## Python application

### Requirements

- Python 3.10+

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
PYTHONPATH=src python3 -m mal_updater.cli review-mappings --limit 20 --mapping-limit 5 --persist-review-queue
PYTHONPATH=src python3 -m mal_updater.cli list-mappings --approved-only
PYTHONPATH=src python3 -m mal_updater.cli approve-mapping series-123 16498 --confidence 0.995 --notes "manual approval"
PYTHONPATH=src python3 -m mal_updater.cli dry-run-sync --limit 20 --mapping-limit 5 --persist-review-queue
PYTHONPATH=src python3 -m mal_updater.cli dry-run-sync --limit 20 --approved-mappings-only
PYTHONPATH=src python3 -m mal_updater.cli list-review-queue --issue-type mapping_review
PYTHONPATH=src python3 -m mal_updater.cli apply-sync --limit 20
PYTHONPATH=src python3 -m mal_updater.cli apply-sync --limit 20 --execute
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
- `validate-snapshot` checks normalized Crunchyroll JSON shape and cross-reference sanity before ingestion work touches SQLite
- `init` creates local directories and applies SQLite migrations
- `mal-auth-url` generates a real PKCE pair plus MAL authorization URL
- `mal-auth-login` starts a local callback listener bound to the configured host, exchanges the returned code for tokens, persists them under `secrets/`, and verifies the token with `GET /users/@me`
- `mal-refresh` refreshes a persisted MAL token pair and writes updated token files back to `secrets/`
- `mal-whoami` exercises the current access token against MAL `GET /users/@me`
- `crunchyroll-auth-login` uses local Crunchyroll username/password secrets to fetch a real refresh token + device id and stage them into `state/crunchyroll/<profile>/`; if optional `curl_cffi` support is installed, it uses browser-TLS impersonation to get through Crunchyroll's Cloudflare layer
- `crunchyroll-fetch-snapshot` is the live Crunchyroll path: it refreshes auth through the Python impersonated transport, fetches account/history/watchlist data, normalizes it into the JSON contract, and can write a snapshot file and/or ingest it directly
- a real live run on this host succeeds through the Python path and ingests into SQLite (`series_count=219`, `progress_count=4311`, `watchlist_count=10`)
- `ingest-snapshot` validates then upserts normalized snapshot data into SQLite, recording a summary row in `sync_runs`
- `map-series` searches MAL for conservative candidate matches for recently seen Crunchyroll series and reports confidence / ambiguity instead of silently persisting guesses
- `review-mappings` turns those candidates into an operator-facing review list, preserves already approved mappings, and can replace open `mapping_review` rows in `review_queue`
- `list-mappings` shows the durable Crunchyroll -> MAL mappings already stored in SQLite
- `approve-mapping` persists an explicit user-approved mapping into `mal_series_mapping`
- `dry-run-sync` prefers approved persisted mappings first, can optionally require approved mappings only, only suggests forward-safe updates, applies explicit missing-data-only merge rules, and can replace open `sync_review` rows in `review_queue`
- `list-review-queue` exposes the durable unresolved review backlog stored in SQLite
- `apply-sync` is the first guarded live executor: it revalidates live MAL state, only consumes approved mappings, only submits forward-safe updates, and only fills MAL fields that are still genuinely missing
- `sync` remains a reserved umbrella entrypoint and points at the explicit review/apply commands

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

This is enough to support read-only ingestion, durable mapping review, guarded sync planning, and the first safe MAL apply path.

## JSON contract

Current version: `1.0`

See:
- `docs/JSON_CONTRACT.md`
- `docs/contracts/crunchyroll_snapshot.schema.json`

Boundary rule:
- the Python Crunchyroll fetch path produces normalized provider snapshots
- the rest of the Python app validates, persists, maps, and decides MAL-side actions

## Security notes

- Do not commit real credentials
- Keep secrets under `secrets/`
- `config/settings.toml` is treated as local-only
- The repo is intended to remain private for now

## Planned architecture

- Python Crunchyroll auth + live fetch path
- Python orchestration worker
- SQLite state database
- MAL official OAuth/API client
- Safe dry-run sync before live writes
