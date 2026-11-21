# JSON contracts

## Provider snapshot

Every provider fetcher (`provider-fetch-snapshot --provider crunchyroll|hidive`) emits one normalized snapshot. The field definitions are the dataclasses in `src/mal_updater/contracts/provider.py`; `validate_snapshot_payload` in `src/mal_updater/validation.py` is the authoritative validator (run it with `validate-snapshot <file>`). The snapshot is normalized provider data, not MAL mutation intent; a new producer plugs into ingestion by emitting the same contract.

- Version: `contract_version` must be `1.0`. Fields are added compatibly; a breaking change needs a new version and coordinated validation/ingestion updates.
- `provider`: the provider slug (`crunchyroll`, `hidive`).
- `series`: deduplicated provider series/season records.
- `progress`: per-episode playback observations with timestamps, completion ratio and raw timing for conservative completion inference. Optional additive fields: observation identity/time (`observation_id`, `observed_at`, `effective_at`), provenance (`progress_source_surface`, `progress_observation_kind`, `completion_assertion`, `normalization_logic_version`), season identity (`provider_season_key`, the canonical dub-collapsed season, plus `season_number`, `season_title`), `episode_label`, the in-season position `season_episode_ordinal`, and the provider's explicit `fully_watched` flag. A missing optional field means "not observed", never "none".
- `watchlist`: watchlist/library entries with optional list identity (`list_id`, `list_name`, `list_kind`, `list_memberships`, item identity and position).
- `fetch_provenance`: optional per-surface completeness evidence; absent means completeness is unknown.
- `raw`: optional provider-specific passthrough/debug object.

## Required safety expectations

- Secrets must never be written into the snapshot.
- Missing/unknown fields must be treated as incomplete data, not proof of absence.
- MAL mutations must only be inferred from normalized persisted state, never directly from raw passthrough blobs.
- `playback_position_ms`, `duration_ms`, `episode_number`, and `last_watched_at` are part of the conservative completion policy; do not drop them if credits-skipped behavior still matters.

## Example snapshot

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

## API request telemetry contract

`.MAL-Updater/state/api-request-events.jsonl` is append-only request-attempt telemetry. New records use `schema_version: 2` and retain the legacy fields (`at`, `provider`, `operation`, `url`, `method`, `outcome`, `status_code`, `error`) while adding:

- `event_id`: unique event identity used for monotonic run-boundary deltas
- `task` / `run_id`: daemon or direct-CLI attribution when known
- `attempt_sequence`: sequence within the propagated request context; retries/timeouts are separate events

URLs retain scheme/host/path and query-key shape, but credentials, fragments, and every query value are redacted. Error text is bounded and redacts URLs, auth material, and sensitive key/value forms. Request headers/bodies are never written. Readers remain tolerant of legacy records without the v2 fields; those records count toward provider-global budgets and conservatively count in task gates where attribution is unavailable.

## Service status niceness contract

`service-status` and `health-check` JSON include `niceness_policy`, an effective loaded-policy object rather than a copy of example settings. Stable top-level children are:

- `policy_kind`: explicitly identifies local niceness controls as non-provider-limit claims
- `cadences`: effective hot/cold, MAL user-list, recommendation metadata, complete public MAL userrecs harvest, provider eligibility, snapshot, and health intervals in seconds
- `thresholds`: warn/critical ratios and the task-plus-provider-global enforcement posture
- `provider_hourly_budgets`, `request_start_spacing_seconds`, and `retry_policy`: effective local headroom/pacing/retry controls (including the non-retried MAL-write posture)
- `execute_limits`: bounded per-task page/candidate/seed/snapshot limits
- `task_policies`: credential-sensitive effective daemon lanes with cadence, initial stagger, provider/task budgets, and cold-start projection
- `cold_refresh_bounds`: Crunchyroll page caps and HIDIVE unattended-full disabled posture
- `cache_horizons_days`: effective MAL/provider/recommendation cache and freshness horizons, including the public userrecs full-harvest stale horizon

Fields may be added compatibly. Consumers must tolerate task-lane absence when credentials are not configured and must not reinterpret the local budgets as external rate-limit declarations.

## Public MAL full-userrecs harvest summary

Periodic-evidence observability is additive. `service-status`/niceness policy includes `periodic_evidence_refresh_policy` (120-day target, 15-day deterministic jitter, 105-135-day window, stale-while-revalidate posture, short negative-claim posture, identity-only provider-title horizon, and the unattended apply gate `fresh_provider_fetch_since_last_apply_required_for_unattended_apply`). `cache_horizons_days` reports the configured nominal targets; retention horizons remain separate.

`recommend-refresh-full-userrecs` emits a JSON object with stable counters: `status` (`ok`, `partial`, or `failed`), `seed_count`, `considered`, `harvested`, `failed`, `paused`, `drift_restarted`, `quarantined`, `skipped_fresh`, `total_edges`, `forced`, `stale_after_days`, `max_pages`, `queue_counts`, `selected_classes`, `failures`, `harvested_sources`, `paused_sources`, `restarted_sources`, `quarantined_sources`, and `semantics`. `max_pages` is a per-source hard network-attempt budget including continuation and revalidation of every staged page; zero means no fetch, queue synchronization/claim, fairness, outcome, or generation mutation. Unfinished or insufficiently revalidated generations pause without publication. `partial` means at least one source paused or at least one source was preserved because completeness was not proven; existing edges for paused/failed/drift-restarted/quarantined sources are not replaced unless a terminal coherent generation publishes atomically. `selected_classes` follows strict `never_started`, `resumable`, `retry_due`, `refresh_due` ordering. Persisted recommendation edges from this path retain only `target_mal_anime_id`, `target_title`, and `num_recommendations`; recommendation prose and usernames are not part of the durable contract.

Dashboard/status health JSON `coverage.public_userrecs.queue` exposes `counts_by_class`, oldest never-started/open timestamps, `last_selected_at`, monotonic selection sequence, `fairness_lag_seconds`, active claim count, retry-scheduled count, quarantine count, and the stable class-priority/fairness descriptions. `open_generations` continues to expose staged page/edge progress; generation records durably retain logic/generation identity, attempts/restarts/drift, retry class/time, validation times, claim, quarantine, and terminal-empty evidence.

## `mal-list-refresh` durable traversal additions

The existing refresh summary is preserved and gains additive `traversal`. The compatible `max_pages` input is a request-attempt budget that includes account identity calls and all page/revalidation calls; zero is a no-network/no-mutation read-only result. Fields include `generation`, `account_key`, `query_identity`, `logic_version`, `revision`, `claimed`, `claim_expires_at`, `queue_classes` (`never_started`, `resumable`, `retry_due`, `terminal`), oldest queue ages, `fairness_sequence`, request `attempted`/`succeeded`/`failed`, restart/drift/quarantine fields, `validated_at`, `terminal_empty_proof`, and per-partition status/page/cursor/terminal/retry/fairness-lag fields. `partial` means staging/LKG preservation, never publication. Empty publication is represented by `terminal_empty_proof.final_validation=true` and `explicit_empty=true`.

## Offline evaluation bundles

`eval resume|validate|labels|score` builds and scores immutable temporal replay bundles from the append-only evaluation events (migration 027). Bundle record schemas are in `references/contracts/evaluation/` (`mal-eval-manifest/v1`, `mal-eval-event/v1`, `mal-eval-candidate/v1`, `mal-eval-prediction/v1`); `eval validate --bundle <dir>` checks a bundle and its leakage guards. Reports are secret-free.
