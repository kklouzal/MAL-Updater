-- MAL lists each season/cour as its own entry while providers group seasons
-- under one series. A series whose progress spans several canonical provider
-- seasons is planned per season; this table records which MAL entry each
-- season resolves to. Unresolved attempts are kept (mal_anime_id may be set to
-- the best candidate, approved = 0) so they are retried on a cadence rather
-- than re-searched every run, and can be approved explicitly.
CREATE TABLE IF NOT EXISTS provider_season_mappings (
    provider TEXT NOT NULL,
    provider_series_id TEXT NOT NULL,
    provider_season_key TEXT NOT NULL,
    mal_anime_id INTEGER,
    mapping_source TEXT NOT NULL CHECK (mapping_source IN ('auto_exact', 'user_exact', 'unresolved')),
    confidence REAL,
    approved INTEGER NOT NULL DEFAULT 0 CHECK (approved IN (0, 1)),
    season_title TEXT,
    season_number INTEGER,
    resolver_version TEXT NOT NULL,
    detail_json TEXT,
    resolved_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (provider, provider_series_id, provider_season_key),
    CHECK (approved = 0 OR mal_anime_id IS NOT NULL),
    CHECK ((mapping_source = 'unresolved') = (approved = 0))
);
