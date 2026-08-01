CREATE TABLE provider_title_search_cache_v2 (
    provider TEXT NOT NULL,
    normalized_query TEXT NOT NULL,
    query TEXT NOT NULL,
    candidate_mal_anime_id INTEGER,
    candidate_title TEXT,
    matches_json TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'ok',
    fetched_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    expires_at TEXT NOT NULL,
    logic_version TEXT NOT NULL DEFAULT 'legacy-v1',
    search_limit INTEGER NOT NULL DEFAULT 10,
    identity_key TEXT NOT NULL DEFAULT '',
    PRIMARY KEY (provider, normalized_query, logic_version, search_limit, identity_key)
);

INSERT INTO provider_title_search_cache_v2 (
    provider,
    normalized_query,
    query,
    candidate_mal_anime_id,
    candidate_title,
    matches_json,
    status,
    fetched_at,
    expires_at,
    logic_version,
    search_limit,
    identity_key
)
SELECT
    provider,
    normalized_query,
    query,
    candidate_mal_anime_id,
    candidate_title,
    matches_json,
    status,
    fetched_at,
    expires_at,
    COALESCE(logic_version, 'legacy-v1'),
    COALESCE(search_limit, 10),
    COALESCE(identity_key, '')
FROM provider_title_search_cache;

DROP TABLE provider_title_search_cache;
ALTER TABLE provider_title_search_cache_v2 RENAME TO provider_title_search_cache;

CREATE INDEX IF NOT EXISTS idx_provider_title_search_cache_expires
    ON provider_title_search_cache(expires_at);
