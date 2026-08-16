-- Append-only, privacy-safe temporal observations for offline replay. Operational
-- latest-state tables are deliberately untouched and no legacy rows are backfilled.
CREATE TABLE evaluation_events (
    event_id TEXT PRIMARY KEY,
    schema_version TEXT NOT NULL CHECK (schema_version = 'mal-eval-event/v1'),
    user_id TEXT NOT NULL,
    event_type TEXT NOT NULL CHECK (event_type IN (
        'provider_series_observed', 'provider_episode_observed',
        'provider_play', 'provider_watchlist_state'
    )),
    source TEXT NOT NULL CHECK (source IN ('crunchyroll', 'hidive', 'system', 'fixture')),
    source_event_id TEXT NOT NULL,
    source_revision INTEGER NOT NULL DEFAULT 1 CHECK (source_revision >= 1),
    occurred_at TEXT NOT NULL,
    observed_at TEXT NOT NULL,
    effective_from TEXT NOT NULL,
    effective_to TEXT,
    supersedes_event_id TEXT,
    entity_type TEXT NOT NULL CHECK (entity_type IN ('anime', 'episode')),
    entity_id TEXT NOT NULL,
    provider TEXT NOT NULL,
    provider_series_id TEXT,
    provider_episode_id TEXT,
    payload_json TEXT NOT NULL,
    payload_sha256 TEXT NOT NULL CHECK (length(payload_sha256) = 64),
    normalization_version TEXT NOT NULL,
    sync_run_id INTEGER,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (sync_run_id) REFERENCES sync_runs(id) ON DELETE SET NULL
);
CREATE UNIQUE INDEX evaluation_events_source_identity
    ON evaluation_events(source, event_type, source_event_id, source_revision, payload_sha256);
CREATE INDEX evaluation_events_temporal
    ON evaluation_events(observed_at, occurred_at, effective_from, effective_to);
CREATE INDEX evaluation_events_resume
    ON evaluation_events(provider, provider_series_id, provider_episode_id, event_type, occurred_at);
