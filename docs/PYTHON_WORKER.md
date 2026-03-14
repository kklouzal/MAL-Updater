# Python worker notes

## What exists now

- directory/bootstrap handling
- TOML settings loading from `config/settings.toml`
- environment override support
- secret file conventions under `secrets/`
- SQLite bootstrap helper
- MAL API client scaffold for:
  - PKCE generation
  - authorization URL building
  - local loopback callback capture
  - token exchange
  - refresh flow
  - `GET /users/@me`
  - atomic token persistence back into `secrets/`

## Important boundary

The MAL client scaffold is **real transport/config structure**, but the project still does **not** claim end-to-end sync is finished.
Nothing here fakes Crunchyroll ingestion or live MAL writes.

## Config precedence

1. explicit environment variables
2. local `config/settings.toml`
3. code defaults

## `settings.toml` coverage

The loader now understands all current non-secret knobs from `config/settings.toml`:

- top-level: `completion_threshold`, `contract_version`
- `[paths]`: config/secrets/data/state/cache dirs, `db_path`, `crunchyroll_adapter_bin`
- `[mal]`: MAL API/auth endpoints plus redirect host/port
- `[crunchyroll]`: locale
- `[secret_files]`: filenames or paths for MAL client/token files

Path values in `settings.toml` may be absolute or relative to the settings file location.

## DB bootstrap path

Use:

```python
from mal_updater.db import bootstrap_database
```

This is the clean entrypoint used by the CLI and should stay the main schema-init path unless migrations become more sophisticated.

## Snapshot validation

Use the CLI to validate a Crunchyroll adapter payload before ingestion work lands:

```bash
PYTHONPATH=src python3 -m mal_updater.cli validate-snapshot path/to/snapshot.json
# or
cat path/to/snapshot.json | PYTHONPATH=src python3 -m mal_updater.cli validate-snapshot
```

Current validation is Python-side and intentionally strict:
- enforces the current contract version
- rejects unexpected keys
- checks basic field types and datetime strings
- verifies progress/watchlist entries reference known series ids
- rejects duplicate series, episode, and watchlist identifiers

## Snapshot ingestion

Use the CLI to validate and persist a normalized adapter snapshot:

```bash
PYTHONPATH=src python3 -m mal_updater.cli ingest-snapshot path/to/snapshot.json
# or
cat path/to/snapshot.json | PYTHONPATH=src python3 -m mal_updater.cli ingest-snapshot
```

Current ingestion behavior:
- validates the payload before any DB writes
- bootstraps the SQLite schema if needed
- upserts `provider_series`, `provider_episode_progress`, and `provider_watchlist`
- stores per-row normalized `raw_json` payloads for audit/debugging
- records an `ingest_snapshot` row in `sync_runs` with a JSON summary
- does **not** delete missing rows or infer MAL-side mutations yet

## MAL mapping and dry-run planning

Use:

```bash
PYTHONPATH=src python3 -m mal_updater.cli map-series --limit 20 --mapping-limit 5
PYTHONPATH=src python3 -m mal_updater.cli review-mappings --limit 20 --mapping-limit 5
PYTHONPATH=src python3 -m mal_updater.cli list-mappings --approved-only
PYTHONPATH=src python3 -m mal_updater.cli approve-mapping series-123 16498 --confidence 0.995 --notes "manual approval"
PYTHONPATH=src python3 -m mal_updater.cli dry-run-sync --limit 20 --mapping-limit 5
PYTHONPATH=src python3 -m mal_updater.cli dry-run-sync --limit 20 --approved-mappings-only
```

Current behavior:
- `map-series` reads recently seen Crunchyroll series from SQLite and searches MAL for candidate matches
- title scoring is intentionally conservative and reports `exact`, `strong`, `ambiguous`, `weak`, or `no_candidates`
- `review-mappings` produces an approval queue: approved mappings are preserved, strong matches are marked `ready_for_approval`, and ambiguous/weak cases stay review-only
- `approve-mapping` persists an explicit Crunchyroll -> MAL choice into `mal_series_mapping`
- `list-mappings` shows the durable mapping cache already saved in SQLite
- `dry-run-sync` prefers approved persisted mappings before doing fresh search work
- `dry-run-sync --approved-mappings-only` is the safe future-executor gate: it refuses to plan anything that lacks explicit approval
- dry-run planning only suggests forward-safe list changes (`watching` / `completed` / `plan_to_watch` with episode counts)
- it refuses to decrease MAL episode counts or downgrade a `completed` MAL entry
- live MAL writes are still out of scope for this pass

## MAL auth commands

```bash
PYTHONPATH=src python3 -m mal_updater.cli mal-auth-url
PYTHONPATH=src python3 -m mal_updater.cli mal-auth-login
PYTHONPATH=src python3 -m mal_updater.cli mal-refresh
PYTHONPATH=src python3 -m mal_updater.cli mal-whoami
```

Notes:
- `mal-auth-login` is the real local loopback flow and waits for one callback hit on the configured redirect port
- token files are written to the configured `secret_files` targets with private `0600` permissions
- `mal-refresh` reuses the persisted refresh token and overwrites the access token file with the new token
- `mal-whoami` is the cheapest honest sanity check after auth succeeds

## Live Crunchyroll fetch path

The fastest honest live path is now Python-side rather than Rust-side.

Use:

```bash
PYTHONPATH=src python3 -m mal_updater.cli crunchyroll-fetch-snapshot --out cache/live-crunchyroll-snapshot.json
PYTHONPATH=src python3 -m mal_updater.cli crunchyroll-fetch-snapshot --out cache/live-crunchyroll-snapshot.json --ingest
```

Behavior:
- refreshes the staged Crunchyroll refresh token from `state/crunchyroll/<profile>/`
- uses `curl_cffi` browser-TLS impersonation when available
- fetches real account / watch-history / watchlist data
- normalizes the result into the existing `1.0` snapshot contract
- can immediately validate + ingest into SQLite

## Near-term next steps

- conflict/review queue generation from ingestion + mapping passes
- MAL-side write planning/dry-run sync pipeline
- optional future Rust transport recovery if it becomes worth the effort
