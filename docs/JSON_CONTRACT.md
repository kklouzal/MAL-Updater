# Rust ↔ Python JSON contract

The Rust Crunchyroll adapter is responsible for collecting provider data and emitting a single normalized JSON snapshot to stdout.
The Python worker owns validation, persistence, mapping, sync policy, and review-queue generation.

## Command boundary

Planned invocation shape:

```bash
crunchyroll-adapter snapshot --contract-version 1.0
```

- stdout: JSON payload matching `docs/contracts/crunchyroll_snapshot.schema.json`
- stderr: human-readable logs/errors only
- exit code `0`: snapshot produced
- non-zero exit: adapter failure, no snapshot should be trusted

## Contract versioning

- Current version: `1.0`
- Any breaking change requires a new contract version string and coordinated Python/Rust updates.

## Snapshot semantics

- `series`: deduplicated per-provider series/season records known to the adapter
- `progress`: per-episode playback observations with timestamps and completion ratio
- `watchlist`: explicit watchlist/library entries if the provider exposes them
- `raw`: optional provider-specific passthrough object for debugging/auditing

## Required safety expectations

- Rust must not write secrets into the snapshot.
- Python must treat missing/unknown fields as incomplete data, not as proof of absence.
- Python must not infer MAL mutations directly from raw payloads; only from normalized persisted state.

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
      "completion_ratio": 0.90,
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
