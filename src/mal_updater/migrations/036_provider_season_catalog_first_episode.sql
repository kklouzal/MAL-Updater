-- Lowest numbered episode of the season. Provider numbering can be absolute
-- across a series (Slime season 2 is 25-48) and sequence numbers also count
-- interleaved recaps/specials, so the in-season position of a numbered
-- episode is episode_number - first_episode_number + 1.
ALTER TABLE provider_season_catalog ADD COLUMN first_episode_number INTEGER
    CHECK (first_episode_number IS NULL OR first_episode_number >= 0);
