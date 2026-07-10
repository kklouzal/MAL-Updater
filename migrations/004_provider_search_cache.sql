CREATE TABLE IF NOT EXISTS provider_title_search_cache (
    provider TEXT NOT NULL,
    normalized_query TEXT NOT NULL,
    query TEXT NOT NULL,
    candidate_mal_anime_id INTEGER,
    candidate_title TEXT,
    matches_json TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'ok',
    fetched_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    expires_at TEXT NOT NULL,
    PRIMARY KEY (provider, normalized_query)
);

CREATE INDEX IF NOT EXISTS idx_provider_title_search_cache_expires
    ON provider_title_search_cache(expires_at);
