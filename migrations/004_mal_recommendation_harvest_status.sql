CREATE TABLE IF NOT EXISTS mal_recommendation_harvest_status (
    source_mal_anime_id INTEGER PRIMARY KEY,
    status TEXT NOT NULL DEFAULT 'fetched',
    num_edges INTEGER NOT NULL DEFAULT 0,
    fetched_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_mal_recommendation_harvest_status_fetched_at ON mal_recommendation_harvest_status(fetched_at);
