-- Additive per-episode season identity for provider progress. Existing rows
-- receive NULL (unknown) and are backfilled when a later observation of the
-- same episode carries the values; ingestion never nulls a known value out.
--   provider_season_key: canonical, dub-collapsed season identity string
--   season_number: provider season ordinal for that canonical season
--   season_title: provider season display title, verbatim
--   episode_label: provider display episode string (e.g. '12.5', 'SP')
--   fully_watched: provider explicit fully-watched flag (1/0), NULL if absent
ALTER TABLE provider_episode_progress ADD COLUMN provider_season_key TEXT;
ALTER TABLE provider_episode_progress ADD COLUMN season_number INTEGER;
ALTER TABLE provider_episode_progress ADD COLUMN season_title TEXT;
ALTER TABLE provider_episode_progress ADD COLUMN episode_label TEXT;
ALTER TABLE provider_episode_progress ADD COLUMN fully_watched INTEGER
    CHECK (fully_watched IS NULL OR fully_watched IN (0, 1));

ALTER TABLE provider_progress_observations ADD COLUMN provider_season_key TEXT;
ALTER TABLE provider_progress_observations ADD COLUMN season_number INTEGER;
ALTER TABLE provider_progress_observations ADD COLUMN season_title TEXT;
ALTER TABLE provider_progress_observations ADD COLUMN episode_label TEXT;
ALTER TABLE provider_progress_observations ADD COLUMN fully_watched INTEGER
    CHECK (fully_watched IS NULL OR fully_watched IN (0, 1));

CREATE INDEX IF NOT EXISTS idx_progress_provider_series_season
    ON provider_episode_progress(provider, provider_series_id, provider_season_key);
