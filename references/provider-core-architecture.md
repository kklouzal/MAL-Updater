# MAL-Updater provider/core architecture

Date: 2026-03-20 (currentized 2026-07-24)

This note describes the initial provider-agnostic refactor that separates MAL-Updater's common core from provider-specific source integration code.

## Goals

- keep mapping, review queue, ingestion, sync planning, recommendations, and MAL writes in the common core
- isolate provider-specific auth/session/fetch/normalization code behind a provider module boundary
- preserve existing Crunchyroll behavior and operator commands while introducing a provider API shared by Crunchyroll and HIDIVE
- keep HIDIVE auth, account snapshot, and title-search behavior isolated behind the same provider boundary

## New core seams

### Provider-neutral snapshot contract

Common normalized contract now lives under:
- `src/mal_updater/contracts/provider.py`

Key exported dataclasses:
- `ProviderSnapshot`
- `SeriesRef`
- `EpisodeProgress`
- `WatchlistEntry`

Notes:
- `contracts/crunchyroll.py` now aliases the generic contract for backward compatibility
- `validation.py` now validates a generic provider snapshot instead of enforcing `provider == "crunchyroll"`

### Provider registry

New registry module:
- `src/mal_updater/provider_registry.py`

Purpose:
- register provider modules by slug
- resolve providers from generic CLI entrypoints
- list available provider slugs for argument parsing

### Provider interface types

New type definitions:
- `src/mal_updater/provider_types.py`

Important types:
- `ProviderCapabilities`
- `ProviderFetchResult`
- `ProviderModule` protocol

These describe the minimal contract expected from provider modules:
- provider identity
- capability declaration
- normalized snapshot fetching
- normalized snapshot file writing

## Provider modules

### Crunchyroll provider module

New module:
- `src/mal_updater/providers/crunchyroll.py`

Current behavior:
- wraps the existing Crunchyroll auth/fetch implementation
- exposes the provider via the new provider registry
- returns normalized provider-neutral snapshot results
- preserves existing incremental boundary behavior through the provider wrapper

### HIDIVE provider module

New module:
- `src/mal_updater/providers/hidive.py`

Current behavior:
- registers the provider slug
- implements `provider-auth-login --provider hidive` through the HIDIVE credential login flow
- implements `provider-fetch-snapshot --provider hidive` through normalized account-scoped snapshot fetching
- writes snapshots through the shared provider snapshot serializer
- exposes bounded read-only title search for recommendation enrichment via HIDIVE frontend Algolia `VOD_SERIES` results; episode/video hits are intentionally ignored for conservative provider-to-MAL matching

Supported HIDIVE snapshot surfaces today are account watch history, continue-watching progress, and favourites represented as normalized watchlist entries. `--full-refresh` fetches those account-scoped surfaces without crawling the full HIDIVE catalog. The incremental/hot path uses the HIDIVE sync boundary, keeps the newest history page current, and can skip continue/favourites until a full account refresh is due. Crunchyroll page chunk controls remain Crunchyroll-only; HIDIVE rejects them rather than pretending partial page resume exists.

## CLI changes

### New generic commands

Added:
- `provider-auth-login --provider <slug>`
- `provider-fetch-snapshot --provider <slug>`

Current behavior:
- `provider-fetch-snapshot` dispatches through the provider registry
- `provider-auth-login` supports both registered source providers, Crunchyroll and HIDIVE

### Compatibility preserved

Existing Crunchyroll commands still exist and behave the same:
- `crunchyroll-auth-login`
- `crunchyroll-fetch-snapshot`

Implementation note:
- `crunchyroll-fetch-snapshot` now delegates to the generic provider-fetch path under the hood

## What remains intentionally unchanged

These common-core areas still operate on normalized provider data and did not need a behavioral rewrite in this slice:
- `db.py`
- `ingestion.py`
- `mapping.py`
- `sync_planner.py`
- `recommendations.py`
- MAL auth/client/write flows

Some naming still reflects the original Crunchyroll-first evolution (for example a few planner/review labels), but the data model already keys everything by `provider`, which keeps the architecture transition safe.

## Why this refactor shape fits both Crunchyroll and HIDIVE

### Crunchyroll characteristics
- stateful auth bootstrap
- provider-local runtime artifacts (refresh token, device id, session state, sync boundary)
- incremental history/watchlist fetching
- transport-specific pacing and retry behavior

### HIDIVE characteristics
- bearer-token login + refresh model
- provider-local runtime artifacts (authorisation token, refresh token, session state, sync boundary)
- multiple useful account data surfaces (history, continue-watching, favourites-as-watchlist)
- bounded title search through public frontend search metadata, used only for specific recommendation/mapping candidates

The provider boundary intentionally does **not** assume one auth style or one fetch shape.
It only requires the provider to emit a normalized snapshot contract the core understands.

## Recommended next implementation slice

1. move remaining provider-specific status/bootstrap reporting behind provider-aware helpers
2. keep refining lane-specific daemon budgeting/cost modeling on top of the generic source-provider defaults
3. decide whether HIDIVE needs additional explicit service-budget tuning beyond the current built-in/provider-service budget tables
4. add richer watchlist handling only if HIDIVE exposes more than the current favourites surface

## Current state

After this refactor slice:
- MAL-Updater has a real provider/core architecture spine
- Crunchyroll is now represented as a provider module instead of the only implicit source model
- HIDIVE has implemented auth bootstrap, account-scoped snapshot fetch, snapshot serialization, and bounded title search behind the provider module
- existing test suite remains green
