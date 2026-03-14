# Current Status

## Working today

- Python worker/application exists
- versioned normalized snapshot contract exists
- SQLite bootstrap + initial schema exist
- local config/secrets/data directory conventions exist
- MAL OAuth flow is working end-to-end:
  - auth URL generation
  - callback listener
  - LAN callback reception on the Orin
  - token exchange
  - token persistence
  - `/users/@me` verification
- Python-side Crunchyroll credential bootstrap exists:
  - reads local `secrets/crunchyroll_username.txt` and `secrets/crunchyroll_password.txt`
  - stages `state/crunchyroll/<profile>/refresh_token.txt`
  - stages matching `device_id.txt`
  - updates `session.json`
  - exposes `crunchyroll-auth-login` in the CLI
- Python-side live Crunchyroll snapshot fetching exists:
  - refreshes the staged Crunchyroll token through the Python transport
  - uses browser-TLS impersonation when optional `curl_cffi` is installed
  - fetches `GET /accounts/v1/me`, `GET /content/v2/{account_id}/watch-history`, and `GET /content/v2/discover/{account_id}/watchlist`
  - normalizes the live responses into the existing `1.0` snapshot contract
  - exposes `crunchyroll-fetch-snapshot` in the CLI, with optional direct ingestion
- the Python fetch path has produced real live Crunchyroll data on this host
- a live snapshot was validated and ingested into SQLite with:
  - `series_count=219`
  - `progress_count=4311`
  - `watchlist_count=10`

## Not implemented yet

- unattended/full-fidelity MAL sync beyond the first guarded executor
- recommendation engine
- OpenClaw skill wrapper for the integration

## Newly added downstream progress

- MAL-side search + candidate scoring exists on top of the ingested Crunchyroll SQLite dataset
- the Python CLI exposes:
  - `map-series`
  - `review-mappings`
  - `list-mappings`
  - `approve-mapping`
  - `dry-run-sync`
  - `list-review-queue`
  - `apply-sync`
- `map-series` reports conservative mapping confidence (`exact`, `strong`, `ambiguous`, `weak`, `no_candidates`) instead of silently persisting guesses
- `review-mappings` provides the first durable operator workflow: preserved approved mappings stay fixed, strong suggestions are surfaced for explicit approval, weaker cases remain review-only, and unresolved items can be persisted into `review_queue`
- `approve-mapping` persists a user-approved Crunchyroll -> MAL mapping into `mal_series_mapping`
- `dry-run-sync` prefers approved persisted mappings before falling back to live MAL search, `--approved-mappings-only` gives the safe gate for execution, and unresolved review/skip results can be persisted into `review_queue`
- `list-review-queue` exposes the durable backlog from SQLite for operator follow-up
- `apply-sync` is the first guarded live-write path:
  - only consumes user-approved mappings
  - revalidates live MAL state immediately before acting
  - only applies proposals that remain forward-safe
  - never decreases MAL watched-episode counts
  - never downgrades a `completed` MAL entry
  - now uses explicit missing-data-only field rules:
    - `status` is only filled when MAL has no status yet
    - progress is only filled when MAL has no status yet, so `plan_to_watch` + `0` is preserved as meaningful
    - `score` is treated as meaningful whenever it is non-zero and is never overwritten by Crunchyroll today
    - `start_date` is preserved because the current Crunchyroll snapshot does not prove the true first-watch date
    - `finish_date` may be filled only when Crunchyroll safely implies completion and MAL does not already have one
  - only sends the fields it intends to change, so meaningful existing MAL metadata is left alone
- live Crunchyroll evidence is now feeding the completion policy directly:
  - strict ratio completion defaults to `0.95`
  - episodes with `<= 120s` remaining count as watched to cover the dominant credits-skip pattern seen in the dataset
  - episodes in the `0.85-0.95` band also count when a later episode in the same series was watched afterwards
  - progress is deduplicated by `episode_number` when available so alternate dub/sub variants do not inflate MAL watched counts

## Current Crunchyroll state

The practical live Crunchyroll path is working on this machine.

What was verified locally:

- Crunchyroll username/password secrets are staged locally
- Python credential login can mint a real refresh token when using browser-like TLS impersonation (`curl_cffi`)
- the minted refresh token also refreshes successfully through the same Python impersonated transport
- the Python fetch path can retrieve real live Crunchyroll data on this host and normalize it honestly
- validated live snapshots ingest cleanly into SQLite

That means the active blocker is no longer Crunchyroll access itself. The remaining work is downstream sync policy hardening, review UX, and recommendation work.

## Next practical milestone

Harden the guarded MAL executor further on top of the now-live local Crunchyroll dataset:
- richer audit/debug surfacing for why an episode counted as completed
- missing-data-only merge policy refinement
- better review resolution UX
