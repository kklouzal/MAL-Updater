CREATE TABLE IF NOT EXISTS provider_watchlist_v2 (
    provider TEXT NOT NULL,
    provider_series_id TEXT NOT NULL,
    added_at TEXT,
    status TEXT,
    list_id TEXT NOT NULL DEFAULT 'default',
    list_name TEXT,
    list_kind TEXT,
    provider_item_id TEXT NOT NULL DEFAULT '',
    provider_item_type TEXT NOT NULL DEFAULT 'series',
    position INTEGER,
    raw_json TEXT,
    first_seen_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_seen_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (provider, list_id, provider_series_id, provider_item_type, provider_item_id),
    FOREIGN KEY (provider, provider_series_id)
        REFERENCES provider_series(provider, provider_series_id)
        ON DELETE CASCADE
);

INSERT OR IGNORE INTO provider_watchlist_v2 (
    provider,
    provider_series_id,
    added_at,
    status,
    list_id,
    list_name,
    list_kind,
    provider_item_id,
    provider_item_type,
    position,
    raw_json,
    first_seen_at,
    last_seen_at
)
SELECT
    provider,
    provider_series_id,
    added_at,
    status,
    COALESCE(NULLIF(TRIM(CAST(CASE WHEN raw_json IS NOT NULL AND json_valid(raw_json) THEN json_extract(raw_json, '$.list_id') ELSE NULL END AS TEXT)), ''), 'default'),
    NULLIF(TRIM(CAST(CASE WHEN raw_json IS NOT NULL AND json_valid(raw_json) THEN json_extract(raw_json, '$.list_name') ELSE NULL END AS TEXT)), ''),
    NULLIF(TRIM(CAST(CASE WHEN raw_json IS NOT NULL AND json_valid(raw_json) THEN json_extract(raw_json, '$.list_kind') ELSE NULL END AS TEXT)), ''),
    COALESCE(NULLIF(TRIM(CAST(CASE WHEN raw_json IS NOT NULL AND json_valid(raw_json) THEN json_extract(raw_json, '$.provider_item_id') ELSE NULL END AS TEXT)), ''), provider_series_id),
    COALESCE(NULLIF(TRIM(CAST(CASE WHEN raw_json IS NOT NULL AND json_valid(raw_json) THEN json_extract(raw_json, '$.provider_item_type') ELSE NULL END AS TEXT)), ''), 'series'),
    CASE WHEN raw_json IS NOT NULL AND json_valid(raw_json) THEN json_extract(raw_json, '$.position') ELSE NULL END,
    raw_json,
    first_seen_at,
    last_seen_at
FROM provider_watchlist;

DROP TABLE provider_watchlist;
ALTER TABLE provider_watchlist_v2 RENAME TO provider_watchlist;

CREATE INDEX IF NOT EXISTS idx_provider_watchlist_series
    ON provider_watchlist(provider, provider_series_id);

CREATE INDEX IF NOT EXISTS idx_provider_watchlist_list
    ON provider_watchlist(provider, list_id);
