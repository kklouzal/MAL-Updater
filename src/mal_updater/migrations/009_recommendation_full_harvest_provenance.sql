ALTER TABLE mal_anime_recommendations ADD COLUMN harvest_source TEXT NOT NULL DEFAULT 'official_detail';
ALTER TABLE mal_anime_recommendations ADD COLUMN complete_harvest INTEGER NOT NULL DEFAULT 0;
ALTER TABLE mal_anime_recommendations ADD COLUMN provenance_json TEXT NOT NULL DEFAULT '{}';

ALTER TABLE mal_recommendation_harvest_status ADD COLUMN source_type TEXT NOT NULL DEFAULT 'official_detail';
ALTER TABLE mal_recommendation_harvest_status ADD COLUMN is_complete INTEGER NOT NULL DEFAULT 0;
ALTER TABLE mal_recommendation_harvest_status ADD COLUMN pages_fetched INTEGER NOT NULL DEFAULT 0;
ALTER TABLE mal_recommendation_harvest_status ADD COLUMN source_url TEXT;
ALTER TABLE mal_recommendation_harvest_status ADD COLUMN last_attempted_at TEXT;
ALTER TABLE mal_recommendation_harvest_status ADD COLUMN last_error TEXT;
ALTER TABLE mal_recommendation_harvest_status ADD COLUMN failure_count INTEGER NOT NULL DEFAULT 0;
ALTER TABLE mal_recommendation_harvest_status ADD COLUMN updated_at TEXT NOT NULL DEFAULT '1970-01-01 00:00:00';

UPDATE mal_recommendation_harvest_status
SET last_attempted_at = COALESCE(last_attempted_at, fetched_at),
    updated_at = COALESCE(fetched_at, CURRENT_TIMESTAMP);

CREATE INDEX IF NOT EXISTS idx_mal_recommendation_harvest_status_source_complete
    ON mal_recommendation_harvest_status(source_type, is_complete, fetched_at);
CREATE INDEX IF NOT EXISTS idx_mal_recommendations_harvest_source
    ON mal_anime_recommendations(harvest_source, complete_harvest, source_mal_anime_id);
