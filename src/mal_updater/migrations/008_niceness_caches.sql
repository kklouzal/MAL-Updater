CREATE TABLE IF NOT EXISTS mal_anime_search_cache (
    cache_key TEXT PRIMARY KEY,
    normalized_query TEXT NOT NULL,
    result_limit INTEGER NOT NULL,
    fields TEXT NOT NULL,
    logic_version TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('ok', 'negative')),
    response_json TEXT NOT NULL,
    fetched_at TEXT NOT NULL,
    expires_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_mal_anime_search_cache_expiry
    ON mal_anime_search_cache(expires_at, logic_version);

CREATE TABLE IF NOT EXISTS mal_anime_detail_cache (
    mal_anime_id INTEGER NOT NULL,
    fields_key TEXT NOT NULL,
    logic_version TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('ok', 'failed')),
    response_json TEXT NOT NULL,
    fetched_at TEXT NOT NULL,
    expires_at TEXT NOT NULL,
    failure_count INTEGER NOT NULL DEFAULT 0,
    next_retry_at TEXT,
    PRIMARY KEY (mal_anime_id, fields_key, logic_version)
);
CREATE INDEX IF NOT EXISTS idx_mal_anime_detail_cache_expiry
    ON mal_anime_detail_cache(expires_at, status);

CREATE TABLE IF NOT EXISTS provider_enriched_detail_cache (
    provider TEXT NOT NULL,
    provider_series_id TEXT NOT NULL,
    logic_version TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('ok', 'failed')),
    detail_json TEXT NOT NULL,
    fetched_at TEXT NOT NULL,
    expires_at TEXT NOT NULL,
    failure_count INTEGER NOT NULL DEFAULT 0,
    next_retry_at TEXT,
    PRIMARY KEY (provider, provider_series_id, logic_version)
);
CREATE INDEX IF NOT EXISTS idx_provider_enriched_detail_cache_expiry
    ON provider_enriched_detail_cache(expires_at, status);

ALTER TABLE provider_title_search_cache ADD COLUMN logic_version TEXT NOT NULL DEFAULT 'legacy-v1';
ALTER TABLE provider_title_search_cache ADD COLUMN search_limit INTEGER NOT NULL DEFAULT 10;
ALTER TABLE provider_title_search_cache ADD COLUMN identity_key TEXT NOT NULL DEFAULT '';

ALTER TABLE recommendation_provider_eligibility_evidence ADD COLUMN refresh_status TEXT NOT NULL DEFAULT 'ok';
ALTER TABLE recommendation_provider_eligibility_evidence ADD COLUMN failure_count INTEGER NOT NULL DEFAULT 0;
ALTER TABLE recommendation_provider_eligibility_evidence ADD COLUMN next_retry_at TEXT;
ALTER TABLE recommendation_provider_eligibility_evidence ADD COLUMN logic_version TEXT NOT NULL DEFAULT 'legacy-v1';
CREATE INDEX IF NOT EXISTS idx_recommendation_eligibility_retry
    ON recommendation_provider_eligibility_evidence(refresh_status, next_retry_at, expires_at);
