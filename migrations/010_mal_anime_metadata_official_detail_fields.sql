ALTER TABLE mal_anime_metadata ADD COLUMN rank INTEGER;
ALTER TABLE mal_anime_metadata ADD COLUMN num_list_users INTEGER;
ALTER TABLE mal_anime_metadata ADD COLUMN num_scoring_users INTEGER;
ALTER TABLE mal_anime_metadata ADD COLUMN rating TEXT;
ALTER TABLE mal_anime_metadata ADD COLUMN average_episode_duration INTEGER;
ALTER TABLE mal_anime_metadata ADD COLUMN start_date TEXT;
ALTER TABLE mal_anime_metadata ADD COLUMN end_date TEXT;
ALTER TABLE mal_anime_metadata ADD COLUMN broadcast_day TEXT;
ALTER TABLE mal_anime_metadata ADD COLUMN broadcast_time TEXT;
ALTER TABLE mal_anime_metadata ADD COLUMN broadcast_timezone TEXT;
ALTER TABLE mal_anime_metadata ADD COLUMN nsfw TEXT;

UPDATE mal_anime_metadata
SET
    rank = CASE
        WHEN json_type(raw_json, '$.rank') IN ('integer', 'real') THEN CAST(json_extract(raw_json, '$.rank') AS INTEGER)
        WHEN json_type(raw_json, '$.rank') = 'text'
             AND TRIM(CAST(json_extract(raw_json, '$.rank') AS TEXT)) <> ''
             AND TRIM(CAST(json_extract(raw_json, '$.rank') AS TEXT)) NOT GLOB '*[^0-9]*'
            THEN CAST(json_extract(raw_json, '$.rank') AS INTEGER)
        ELSE rank
    END,
    num_list_users = CASE
        WHEN json_type(raw_json, '$.num_list_users') IN ('integer', 'real') THEN CAST(json_extract(raw_json, '$.num_list_users') AS INTEGER)
        WHEN json_type(raw_json, '$.num_list_users') = 'text'
             AND TRIM(CAST(json_extract(raw_json, '$.num_list_users') AS TEXT)) <> ''
             AND TRIM(CAST(json_extract(raw_json, '$.num_list_users') AS TEXT)) NOT GLOB '*[^0-9]*'
            THEN CAST(json_extract(raw_json, '$.num_list_users') AS INTEGER)
        ELSE num_list_users
    END,
    num_scoring_users = CASE
        WHEN json_type(raw_json, '$.num_scoring_users') IN ('integer', 'real') THEN CAST(json_extract(raw_json, '$.num_scoring_users') AS INTEGER)
        WHEN json_type(raw_json, '$.num_scoring_users') = 'text'
             AND TRIM(CAST(json_extract(raw_json, '$.num_scoring_users') AS TEXT)) <> ''
             AND TRIM(CAST(json_extract(raw_json, '$.num_scoring_users') AS TEXT)) NOT GLOB '*[^0-9]*'
            THEN CAST(json_extract(raw_json, '$.num_scoring_users') AS INTEGER)
        ELSE num_scoring_users
    END,
    rating = CASE
        WHEN json_type(raw_json, '$.rating') = 'text' AND TRIM(CAST(json_extract(raw_json, '$.rating') AS TEXT)) <> ''
            THEN LOWER(TRIM(CAST(json_extract(raw_json, '$.rating') AS TEXT)))
        ELSE rating
    END,
    average_episode_duration = CASE
        WHEN json_type(raw_json, '$.average_episode_duration') IN ('integer', 'real') THEN CAST(json_extract(raw_json, '$.average_episode_duration') AS INTEGER)
        WHEN json_type(raw_json, '$.average_episode_duration') = 'text'
             AND TRIM(CAST(json_extract(raw_json, '$.average_episode_duration') AS TEXT)) <> ''
             AND TRIM(CAST(json_extract(raw_json, '$.average_episode_duration') AS TEXT)) NOT GLOB '*[^0-9]*'
            THEN CAST(json_extract(raw_json, '$.average_episode_duration') AS INTEGER)
        ELSE average_episode_duration
    END,
    start_date = CASE
        WHEN json_type(raw_json, '$.start_date') = 'text' AND TRIM(CAST(json_extract(raw_json, '$.start_date') AS TEXT)) <> ''
            THEN TRIM(CAST(json_extract(raw_json, '$.start_date') AS TEXT))
        ELSE start_date
    END,
    end_date = CASE
        WHEN json_type(raw_json, '$.end_date') = 'text' AND TRIM(CAST(json_extract(raw_json, '$.end_date') AS TEXT)) <> ''
            THEN TRIM(CAST(json_extract(raw_json, '$.end_date') AS TEXT))
        ELSE end_date
    END,
    broadcast_day = CASE
        WHEN json_type(raw_json, '$.broadcast.day_of_the_week') = 'text' AND TRIM(CAST(json_extract(raw_json, '$.broadcast.day_of_the_week') AS TEXT)) <> ''
            THEN LOWER(TRIM(CAST(json_extract(raw_json, '$.broadcast.day_of_the_week') AS TEXT)))
        ELSE broadcast_day
    END,
    broadcast_time = CASE
        WHEN json_type(raw_json, '$.broadcast.start_time') = 'text' AND TRIM(CAST(json_extract(raw_json, '$.broadcast.start_time') AS TEXT)) <> ''
            THEN TRIM(CAST(json_extract(raw_json, '$.broadcast.start_time') AS TEXT))
        ELSE broadcast_time
    END,
    broadcast_timezone = CASE
        WHEN json_type(raw_json, '$.broadcast.timezone') = 'text' AND TRIM(CAST(json_extract(raw_json, '$.broadcast.timezone') AS TEXT)) <> ''
            THEN TRIM(CAST(json_extract(raw_json, '$.broadcast.timezone') AS TEXT))
        ELSE broadcast_timezone
    END,
    nsfw = CASE
        WHEN json_type(raw_json, '$.nsfw') = 'text' AND TRIM(CAST(json_extract(raw_json, '$.nsfw') AS TEXT)) <> ''
            THEN LOWER(TRIM(CAST(json_extract(raw_json, '$.nsfw') AS TEXT)))
        ELSE nsfw
    END
WHERE json_valid(raw_json);

CREATE INDEX IF NOT EXISTS idx_mal_anime_metadata_rank
    ON mal_anime_metadata(rank)
    WHERE rank IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_mal_anime_metadata_list_users
    ON mal_anime_metadata(num_list_users)
    WHERE num_list_users IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_mal_anime_metadata_scoring_users
    ON mal_anime_metadata(num_scoring_users)
    WHERE num_scoring_users IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_mal_anime_metadata_dates
    ON mal_anime_metadata(start_date, end_date);
CREATE INDEX IF NOT EXISTS idx_mal_anime_metadata_rating_nsfw
    ON mal_anime_metadata(rating, nsfw);
