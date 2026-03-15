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

- top-level: `completion_threshold`, `credits_skip_window_seconds`, `contract_version`
- `[paths]`: config/secrets/data/state/cache dirs and `db_path`
- `[mal]`: MAL API/auth endpoints plus redirect host/port
- `[crunchyroll]`: locale, request_spacing_seconds
- `[secret_files]`: filenames or paths for MAL client/token files

Path values in `settings.toml` may be absolute or relative to the settings file location.

## DB bootstrap path

Use:

```python
from mal_updater.db import bootstrap_database
```

This is the clean entrypoint used by the CLI and should stay the main schema-init path unless migrations become more sophisticated.

## Snapshot validation

Use the CLI to validate a normalized Crunchyroll snapshot payload before ingestion work lands:

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

Use the CLI to validate and persist a normalized Crunchyroll snapshot:

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
PYTHONPATH=src python3 -m mal_updater.cli review-mappings --limit 20 --mapping-limit 5 --persist-review-queue
PYTHONPATH=src python3 -m mal_updater.cli list-mappings --approved-only
PYTHONPATH=src python3 -m mal_updater.cli approve-mapping series-123 16498 --confidence 0.995 --notes "manual approval"
PYTHONPATH=src python3 -m mal_updater.cli dry-run-sync --limit 20 --mapping-limit 5 --persist-review-queue
PYTHONPATH=src python3 -m mal_updater.cli dry-run-sync --limit 20 --approved-mappings-only
PYTHONPATH=src python3 -m mal_updater.cli list-review-queue --issue-type mapping_review
PYTHONPATH=src python3 -m mal_updater.cli list-review-queue --issue-type sync_review
PYTHONPATH=src python3 -m mal_updater.cli apply-sync --limit 20
PYTHONPATH=src python3 -m mal_updater.cli apply-sync --limit 20 --execute
PYTHONPATH=src python3 -m mal_updater.cli apply-sync --limit 0 --exact-approved-only --execute
```

Current behavior:
- `map-series` reads recently seen Crunchyroll series from SQLite and searches MAL for candidate matches
- title scoring is intentionally conservative and reports `exact`, `strong`, `ambiguous`, `weak`, or `no_candidates`; generic Crunchyroll labels like `Season 2`, `Part 2`, `2nd Cour`, or `Final Season` are expanded into `Title ...` search queries, and explicit installment hints (season numbers, ordinal seasons, roman numerals, parts, cours, split indexes, `Final Season`) are scored as explainable evidence instead of opaque confidence bumps
- exact-title checks are stricter than the broader similarity score so `Part 1` and `Part 2` are no longer treated as the same exact title just because the base show name matches after cleanup
- noisy provider `season_number` metadata is no longer blindly trusted when `season_title` contains a conflicting explicit season number; the title cue wins and the conflict is surfaced in rationale
- exact movie-title matches are allowed even when Crunchyroll grouped the item under a movie/collection shell, but movies are still penalized by default when that exact title evidence is absent
- `review-mappings` produces an approval queue: approved mappings are preserved, truly exact/unique season-consistent matches are auto-approved into durable `auto_exact` rows, strong-but-not-exact matches are marked `ready_for_approval`, explicit installment-hint conflicts block auto-approval, ambiguous/weak cases stay review-only, and `--persist-review-queue` replaces open `mapping_review` rows in SQLite with the unresolved items from the latest pass
- `approve-mapping` persists an explicit Crunchyroll -> MAL choice into `mal_series_mapping`
- `list-mappings` shows the durable mapping cache already saved in SQLite
- `dry-run-sync` prefers approved persisted mappings before doing fresh search work and can auto-promote the same safe exact matches into durable `auto_exact` approvals
- `dry-run-sync --approved-mappings-only` is the executor safety gate: it refuses to plan anything that lacks durable approval
- `dry-run-sync --persist-review-queue` replaces open `sync_review` rows in SQLite with the latest non-actionable review/skip items
- `list-review-queue` surfaces the durable review backlog from `review_queue`
- dry-run planning only suggests forward-safe list changes (`watching` / `completed` / `plan_to_watch` with episode counts)
- Crunchyroll completion is now derived conservatively from the real dataset instead of a blind ratio-only rule:
  - `completion_ratio >= 0.95`, or
  - `duration_ms - playback_position_ms <= credits_skip_window_seconds` (default `120`), or
  - `completion_ratio >= 0.85` plus later watched progress for a higher episode number in the same series
- progress counting deduplicates alternate provider episode variants by `episode_number` when available so dub/sub variants do not inflate MAL watched counts
- `apply-sync` re-fetches live MAL state, only considers approved mappings, and only writes still-safe missing-data merges (`status`, `num_watched_episodes`, and when justified `finish_date`)
- `apply-sync --exact-approved-only` narrows execution further to exact approved mappings only (`auto_exact` / `user_exact`) for the first unattended cadence
- explicit merge rules are now encoded in the planner/executor:
  - `status` is missing only when absent/null
  - progress is missing only when list status is absent
  - `score` is missing only when null/absent/`0`, but Crunchyroll does not supply a trustworthy score yet so it is preserved
  - `start_date` is preserved because Crunchyroll currently exposes only a last-watch timestamp, not a proven first-watch date
  - `finish_date` can be filled from Crunchyroll `last_watched_at` only when completion is otherwise safely inferred
- the planner/executor refuses to decrease MAL episode counts or downgrade a `completed` MAL entry
- meaningful existing MAL metadata is preserved because the executor only submits the fields it explicitly intends to change

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

The live Crunchyroll path is Python-side.

Use:

```bash
PYTHONPATH=src python3 -m mal_updater.cli crunchyroll-fetch-snapshot --out cache/live-crunchyroll-snapshot.json
PYTHONPATH=src python3 -m mal_updater.cli crunchyroll-fetch-snapshot --out cache/live-crunchyroll-snapshot.json --ingest
```

Behavior:
- refreshes the staged Crunchyroll refresh token from `state/crunchyroll/<profile>/`
- uses `curl_cffi` browser-TLS impersonation when available
- fetches real account / watch-history / watchlist data
- spaces individual Crunchyroll requests by `crunchyroll.request_spacing_seconds` (default `10.0`)
- paginates Crunchyroll watchlists with the provider's `n` + `start` parameters so large libraries are ingested completely
- normalizes the result into the existing `1.0` snapshot contract
- can immediately validate + ingest into SQLite

## Near-term next steps

- persist richer audit/debug output if we want to surface *why* a specific episode counted as completed (strict threshold vs credits window vs follow-on evidence)
- missing-data-only merge rules / richer field policy if score/start/finish dates are ever brought into scope
