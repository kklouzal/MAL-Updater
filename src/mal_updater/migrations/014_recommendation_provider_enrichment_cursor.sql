CREATE TABLE IF NOT EXISTS recommendation_provider_enrichment_cursor (
    provider TEXT PRIMARY KEY,
    cursor_mal_anime_id INTEGER,
    cursor_rank_key_json TEXT,
    cursor_generation INTEGER NOT NULL DEFAULT 0,
    wrapped_at TEXT,
    last_attempted_mal_anime_id INTEGER,
    last_attempted_rank_key_json TEXT,
    last_attempted_at TEXT,
    last_selection_class TEXT,
    last_outcome TEXT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS recommendation_provider_enrichment_attempts (
    provider TEXT NOT NULL,
    mal_anime_id INTEGER NOT NULL,
    rank_key_json TEXT NOT NULL DEFAULT '{}',
    selection_class TEXT NOT NULL,
    attempted_at TEXT NOT NULL,
    attempt_count INTEGER NOT NULL DEFAULT 0,
    last_outcome TEXT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (provider, mal_anime_id)
);

CREATE INDEX IF NOT EXISTS idx_recommendation_provider_enrichment_attempts_provider_time
    ON recommendation_provider_enrichment_attempts(provider, attempted_at, mal_anime_id);
