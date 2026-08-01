ALTER TABLE provider_series ADD COLUMN account_observed_at TEXT;
ALTER TABLE provider_series ADD COLUMN catalog_observed_at TEXT;

UPDATE provider_series
SET account_observed_at = COALESCE(
    (
        SELECT MAX(linked_observation.observed_at)
        FROM (
            SELECT provider_episode_progress.last_seen_at AS observed_at
            FROM provider_episode_progress
            WHERE provider_episode_progress.provider = provider_series.provider
              AND provider_episode_progress.provider_series_id = provider_series.provider_series_id
              AND provider_episode_progress.last_seen_at IS NOT NULL
            UNION ALL
            SELECT provider_watchlist.last_seen_at AS observed_at
            FROM provider_watchlist
            WHERE provider_watchlist.provider = provider_series.provider
              AND provider_watchlist.provider_series_id = provider_series.provider_series_id
              AND provider_watchlist.last_seen_at IS NOT NULL
        ) AS linked_observation
    ),
    provider_series.last_seen_at
)
WHERE account_observed_at IS NULL;
