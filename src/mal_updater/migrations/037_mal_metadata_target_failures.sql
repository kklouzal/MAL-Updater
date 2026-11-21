-- MAL detail fetch failures for metadata-refresh targets (discovery and sequel
-- titles). Targets are picked missing-metadata-first, so without this memory a
-- permanently failing entry (for example one deleted on MAL) would be retried
-- at the front of the queue on every run and starve the rest.
CREATE TABLE IF NOT EXISTS mal_metadata_target_failures (
    mal_anime_id INTEGER PRIMARY KEY,
    consecutive_failures INTEGER NOT NULL CHECK (consecutive_failures >= 1),
    last_failed_at TEXT NOT NULL,
    retry_after_at TEXT NOT NULL,
    last_error TEXT
);
CREATE INDEX IF NOT EXISTS idx_mal_metadata_target_failures_retry
    ON mal_metadata_target_failures(retry_after_at);
