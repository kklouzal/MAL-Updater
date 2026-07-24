CREATE TABLE IF NOT EXISTS recommendation_score_snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT NOT NULL,
    generated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    kind TEXT NOT NULL,
    provider TEXT,
    title TEXT NOT NULL,
    provider_series_id TEXT,
    mal_anime_id INTEGER,
    score REAL,
    priority INTEGER,
    reasons_json TEXT,
    scorecard_json TEXT,
    context_json TEXT,
    availability_providers_json TEXT,
    dub_signal TEXT,
    availability_confidence REAL
);

CREATE INDEX IF NOT EXISTS idx_recommendation_score_snapshots_run
    ON recommendation_score_snapshots(run_id, id);
CREATE INDEX IF NOT EXISTS idx_recommendation_score_snapshots_latest
    ON recommendation_score_snapshots(generated_at DESC, id DESC);
CREATE INDEX IF NOT EXISTS idx_recommendation_score_snapshots_identity
    ON recommendation_score_snapshots(kind, provider, provider_series_id, mal_anime_id);
