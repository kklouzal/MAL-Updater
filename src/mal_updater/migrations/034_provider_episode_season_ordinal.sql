-- Position of an episode within its provider season (Crunchyroll
-- sequence_number; HIDIVE per-season episode number). Provider episode_number
-- can be absolute across a series (Attack on Titan Final Season is 60-87) and
-- specials reuse low numbers (SP1 -> 1), so per-season planning needs this.
ALTER TABLE provider_episode_progress ADD COLUMN season_episode_ordinal INTEGER
    CHECK (season_episode_ordinal IS NULL OR season_episode_ordinal >= 0);
ALTER TABLE provider_progress_observations ADD COLUMN season_episode_ordinal INTEGER
    CHECK (season_episode_ordinal IS NULL OR season_episode_ordinal >= 0);
