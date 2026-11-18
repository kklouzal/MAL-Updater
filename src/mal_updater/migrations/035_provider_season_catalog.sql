-- Provider season catalog facts used to match a provider season to its MAL
-- entry: episode counts and the season's date window. date_kind says what the
-- dates mean: 'original_air' (Crunchyroll episode_air_date, the original
-- broadcast) or 'provider_publish' (HIDIVE publishedDate, the provider's own
-- release, never earlier than the original broadcast).
CREATE TABLE IF NOT EXISTS provider_season_catalog (
    provider TEXT NOT NULL,
    provider_season_key TEXT NOT NULL,
    title TEXT,
    numbered_episode_count INTEGER CHECK (numbered_episode_count IS NULL OR numbered_episode_count >= 0),
    total_episode_count INTEGER CHECK (total_episode_count IS NULL OR total_episode_count >= 0),
    first_date TEXT,
    last_date TEXT,
    date_kind TEXT NOT NULL CHECK (date_kind IN ('original_air', 'provider_publish')),
    complete INTEGER NOT NULL DEFAULT 1 CHECK (complete IN (0, 1)),
    fetched_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (provider, provider_season_key)
);
