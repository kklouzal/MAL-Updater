-- Append-only provider-neutral progress/activity observations. The canonical
-- provider_episode_progress table remains the precedence-ranked projection.
-- Existing operational rows are intentionally not backfilled or changed.
CREATE TABLE provider_progress_observations (
    observation_id TEXT PRIMARY KEY,
    provider TEXT NOT NULL,
    provider_episode_id TEXT NOT NULL,
    provider_series_id TEXT NOT NULL,
    source_surface TEXT,
    observation_kind TEXT,
    completion_assertion TEXT,
    normalization_logic_version TEXT,
    observed_at TEXT NOT NULL,
    effective_at TEXT,
    episode_number INTEGER,
    episode_title TEXT,
    playback_position_ms INTEGER,
    duration_ms INTEGER,
    completion_ratio REAL,
    audio_locale TEXT,
    subtitle_locale TEXT,
    rating TEXT,
    raw_json TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CHECK (observation_kind IS NULL OR observation_kind IN (
        'position', 'ratio', 'history_membership', 'explicit_completed', 'inferred_later_episode'
    )),
    CHECK (completion_assertion IS NULL OR completion_assertion IN ('confirmed', 'inferred', 'unknown')),
    CHECK (playback_position_ms IS NULL OR playback_position_ms >= 0),
    CHECK (duration_ms IS NULL OR duration_ms >= 0),
    CHECK (completion_ratio IS NULL OR (completion_ratio >= 0.0 AND completion_ratio <= 1.0))
);

CREATE INDEX idx_provider_progress_observations_series_time
    ON provider_progress_observations(provider, provider_series_id, effective_at, observed_at);
CREATE INDEX idx_provider_progress_observations_episode_surface
    ON provider_progress_observations(provider, provider_episode_id, source_surface, observed_at);
