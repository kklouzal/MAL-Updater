CREATE TABLE IF NOT EXISTS watch_confirmation_provenance (
    provider TEXT NOT NULL,
    provider_series_id TEXT NOT NULL,
    identity_key TEXT NOT NULL DEFAULT '',
    mal_anime_id INTEGER,
    source_title TEXT NOT NULL,
    season_title TEXT,
    mapped_mal_title TEXT,
    progress_rows INTEGER NOT NULL DEFAULT 0,
    completed_episode_count INTEGER NOT NULL DEFAULT 0,
    max_episode_number INTEGER,
    max_completed_episode_number INTEGER,
    provider_watched_episodes INTEGER NOT NULL DEFAULT 0,
    mal_num_episodes INTEGER,
    confirmed_complete INTEGER NOT NULL DEFAULT 0,
    completion_decision TEXT NOT NULL,
    completion_status TEXT NOT NULL,
    completion_threshold REAL,
    credits_skip_window_seconds INTEGER,
    last_watched_at TEXT,
    last_progress_seen_at TEXT,
    last_series_seen_at TEXT,
    last_evidence_at TEXT,
    mapping_source TEXT,
    mapping_confidence REAL,
    mapping_approved INTEGER NOT NULL DEFAULT 0,
    verified_identity_kind TEXT,
    verified_identity_json TEXT,
    completed_by_json TEXT NOT NULL DEFAULT '{}',
    completed_examples_json TEXT NOT NULL DEFAULT '{}',
    incomplete_examples_json TEXT NOT NULL DEFAULT '[]',
    thresholds_json TEXT NOT NULL DEFAULT '{}',
    progress_audit_json TEXT NOT NULL DEFAULT '{}',
    mapping_audit_json TEXT NOT NULL DEFAULT '{}',
    decision_audit_json TEXT NOT NULL DEFAULT '{}',
    generated_at TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (provider, provider_series_id)
);

CREATE INDEX IF NOT EXISTS idx_watch_confirmation_provenance_identity
    ON watch_confirmation_provenance(provider, identity_key, provider_series_id);

CREATE INDEX IF NOT EXISTS idx_watch_confirmation_provenance_mal
    ON watch_confirmation_provenance(mal_anime_id, provider, provider_series_id)
    WHERE mal_anime_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_watch_confirmation_provenance_decision
    ON watch_confirmation_provenance(confirmed_complete, completion_decision, generated_at);

CREATE INDEX IF NOT EXISTS idx_watch_confirmation_provenance_evidence
    ON watch_confirmation_provenance(last_evidence_at, last_watched_at);
