# Crunchyroll snapshot JSON contract

The live Crunchyroll fetch path produces a single normalized JSON snapshot.
The rest of the Python application owns validation, persistence, mapping, sync policy, and review-queue generation.

## Contract boundary

Current producer shape:

```bash
PYTHONPATH=src python3 -m mal_updater.cli provider-fetch-snapshot --provider crunchyroll --out path/to/snapshot.json
```

- output JSON matches `references/contracts/crunchyroll_snapshot.schema.json`
- the snapshot is normalized provider data, not direct MAL mutation intent
- any future producer must emit the same contract if it wants to plug into the existing ingestion path

## Contract versioning

- Current version: `1.0`
- Any breaking change requires a new contract version string and coordinated validation/ingestion updates.

## Snapshot semantics

- `series`: deduplicated per-provider series/season records known from Crunchyroll data
- `progress`: per-episode playback observations with timestamps, completion ratio, and raw timing needed for conservative completion inference
- `watchlist`: explicit watchlist/library entries if Crunchyroll exposes them
- `raw`: optional provider-specific passthrough/debug object

## Required safety expectations

- Secrets must never be written into the snapshot.
- Missing/unknown fields must be treated as incomplete data, not proof of absence.
- MAL mutations must only be inferred from normalized persisted state, never directly from raw passthrough blobs.
- `playback_position_ms`, `duration_ms`, `episode_number`, and `last_watched_at` are part of the conservative completion policy; do not drop them if credits-skipped behavior still matters.

## API request telemetry contract

`.MAL-Updater/state/api-request-events.jsonl` is append-only request-attempt telemetry. New records use `schema_version: 2` and retain the legacy fields (`at`, `provider`, `operation`, `url`, `method`, `outcome`, `status_code`, `error`) while adding:

- `event_id`: unique event identity used for monotonic run-boundary deltas
- `task` / `run_id`: daemon or direct-CLI attribution when known
- `attempt_sequence`: sequence within the propagated request context; retries/timeouts are separate events

URLs retain scheme/host/path and query-key shape, but credentials, fragments, and every query value are redacted. Error text is bounded and redacts URLs, auth material, and sensitive key/value forms. Request headers/bodies are never written. Readers remain tolerant of legacy records without the v2 fields; those records count toward provider-global budgets and conservatively count in task gates where attribution is unavailable.

## Service status niceness contract

`service-status` and `health-check` JSON include `niceness_policy`, an effective loaded-policy object rather than a copy of example settings. Stable top-level children are:

- `policy_kind`: explicitly identifies local niceness controls as non-provider-limit claims
- `cadences`: effective hot/cold, MAL user-list, recommendation metadata, provider eligibility, snapshot, and health intervals in seconds
- `thresholds`: warn/critical ratios and the task-plus-provider-global enforcement posture
- `provider_hourly_budgets`, `request_start_spacing_seconds`, and `retry_policy`: effective local headroom/pacing/retry controls (including the non-retried MAL-write posture)
- `execute_limits`: bounded per-task page/candidate/seed/snapshot limits
- `task_policies`: credential-sensitive effective daemon lanes with cadence, initial stagger, provider/task budgets, and cold-start projection
- `cold_refresh_bounds`: Crunchyroll page caps and HIDIVE unattended-full disabled posture
- `cache_horizons_days`: effective MAL/provider/recommendation cache and freshness horizons

Fields may be added compatibly. Consumers must tolerate task-lane absence when credentials are not configured and must not reinterpret the local budgets as external rate-limit declarations.

## Example payload

```json
{
  "contract_version": "1.0",
  "generated_at": "2026-03-14T18:00:00Z",
  "provider": "crunchyroll",
  "account_id_hint": null,
  "series": [
    {
      "provider_series_id": "series-123",
      "title": "Example Show",
      "season_title": "Example Show Season 1",
      "season_number": 1
    }
  ],
  "progress": [
    {
      "provider_episode_id": "episode-456",
      "provider_series_id": "series-123",
      "episode_number": 3,
      "episode_title": "Example Episode",
      "playback_position_ms": 1300000,
      "duration_ms": 1440000,
      "completion_ratio": 0.95,
      "last_watched_at": "2026-03-14T17:55:00Z",
      "audio_locale": "en-US",
      "subtitle_locale": null,
      "rating": null
    }
  ],
  "watchlist": [
    {
      "provider_series_id": "series-123",
      "added_at": "2026-03-10T12:00:00Z",
      "status": "watching"
    }
  ],
  "raw": {}
}
```
