CREATE TABLE IF NOT EXISTS mal_user_anime_list_cache (
    mal_anime_id INTEGER PRIMARY KEY,
    title TEXT NOT NULL,
    list_status TEXT CHECK (list_status IS NULL OR list_status IN ('completed', 'watching', 'on_hold', 'dropped', 'plan_to_watch')),
    user_score INTEGER CHECK (user_score IS NULL OR (user_score >= 0 AND user_score <= 10)),
    num_episodes_watched INTEGER CHECK (num_episodes_watched IS NULL OR num_episodes_watched >= 0),
    start_date TEXT,
    finish_date TEXT,
    list_updated_at TEXT,
    node_json TEXT NOT NULL DEFAULT '{}',
    list_status_json TEXT NOT NULL DEFAULT '{}',
    raw_json TEXT NOT NULL,
    refresh_run_id TEXT NOT NULL,
    refresh_generation INTEGER NOT NULL,
    fetched_at TEXT NOT NULL,
    last_seen_at TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_mal_user_anime_list_cache_status
    ON mal_user_anime_list_cache(list_status, mal_anime_id);

CREATE INDEX IF NOT EXISTS idx_mal_user_anime_list_cache_score
    ON mal_user_anime_list_cache(user_score DESC, list_status, mal_anime_id);

CREATE INDEX IF NOT EXISTS idx_mal_user_anime_list_cache_freshness
    ON mal_user_anime_list_cache(last_seen_at, fetched_at, refresh_generation);

CREATE INDEX IF NOT EXISTS idx_mal_user_anime_list_cache_generation
    ON mal_user_anime_list_cache(refresh_generation, refresh_run_id, mal_anime_id);
