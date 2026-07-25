ALTER TABLE mal_user_anime_list_cache ADD COLUMN priority INTEGER CHECK (priority IS NULL OR (priority >= 0 AND priority <= 2));
ALTER TABLE mal_user_anime_list_cache ADD COLUMN is_rewatching INTEGER CHECK (is_rewatching IS NULL OR is_rewatching IN (0, 1));
ALTER TABLE mal_user_anime_list_cache ADD COLUMN num_times_rewatched INTEGER CHECK (num_times_rewatched IS NULL OR num_times_rewatched >= 0);
ALTER TABLE mal_user_anime_list_cache ADD COLUMN rewatch_value INTEGER CHECK (rewatch_value IS NULL OR (rewatch_value >= 0 AND rewatch_value <= 5));
ALTER TABLE mal_user_anime_list_cache ADD COLUMN tag_count INTEGER NOT NULL DEFAULT 0 CHECK (tag_count >= 0);
ALTER TABLE mal_user_anime_list_cache ADD COLUMN has_comments INTEGER NOT NULL DEFAULT 0 CHECK (has_comments IN (0, 1));

UPDATE mal_user_anime_list_cache
SET
    priority = CASE
        WHEN json_valid(list_status_json) AND json_type(list_status_json, '$.priority') IN ('integer', 'real')
             AND CAST(json_extract(list_status_json, '$.priority') AS INTEGER) BETWEEN 0 AND 2
            THEN CAST(json_extract(list_status_json, '$.priority') AS INTEGER)
        WHEN json_valid(list_status_json) AND json_type(list_status_json, '$.priority') = 'text'
             AND TRIM(CAST(json_extract(list_status_json, '$.priority') AS TEXT)) <> ''
             AND TRIM(CAST(json_extract(list_status_json, '$.priority') AS TEXT)) NOT GLOB '*[^0-9]*'
             AND CAST(json_extract(list_status_json, '$.priority') AS INTEGER) BETWEEN 0 AND 2
            THEN CAST(json_extract(list_status_json, '$.priority') AS INTEGER)
        ELSE priority
    END,
    is_rewatching = CASE
        WHEN json_valid(list_status_json) AND json_type(list_status_json, '$.is_rewatching') = 'true' THEN 1
        WHEN json_valid(list_status_json) AND json_type(list_status_json, '$.is_rewatching') = 'false' THEN 0
        WHEN json_valid(list_status_json) AND json_type(list_status_json, '$.is_rewatching') IN ('integer', 'real')
             AND CAST(json_extract(list_status_json, '$.is_rewatching') AS INTEGER) IN (0, 1)
            THEN CAST(json_extract(list_status_json, '$.is_rewatching') AS INTEGER)
        WHEN json_valid(list_status_json) AND json_type(list_status_json, '$.is_rewatching') = 'text'
             AND LOWER(TRIM(CAST(json_extract(list_status_json, '$.is_rewatching') AS TEXT))) IN ('true', 'false', '1', '0')
            THEN CASE
                WHEN LOWER(TRIM(CAST(json_extract(list_status_json, '$.is_rewatching') AS TEXT))) IN ('true', '1') THEN 1
                ELSE 0
            END
        ELSE is_rewatching
    END,
    num_times_rewatched = CASE
        WHEN json_valid(list_status_json) AND json_type(list_status_json, '$.num_times_rewatched') IN ('integer', 'real')
             AND CAST(json_extract(list_status_json, '$.num_times_rewatched') AS INTEGER) >= 0
            THEN CAST(json_extract(list_status_json, '$.num_times_rewatched') AS INTEGER)
        WHEN json_valid(list_status_json) AND json_type(list_status_json, '$.num_times_rewatched') = 'text'
             AND TRIM(CAST(json_extract(list_status_json, '$.num_times_rewatched') AS TEXT)) <> ''
             AND TRIM(CAST(json_extract(list_status_json, '$.num_times_rewatched') AS TEXT)) NOT GLOB '*[^0-9]*'
            THEN CAST(json_extract(list_status_json, '$.num_times_rewatched') AS INTEGER)
        ELSE num_times_rewatched
    END,
    rewatch_value = CASE
        WHEN json_valid(list_status_json) AND json_type(list_status_json, '$.rewatch_value') IN ('integer', 'real')
             AND CAST(json_extract(list_status_json, '$.rewatch_value') AS INTEGER) BETWEEN 0 AND 5
            THEN CAST(json_extract(list_status_json, '$.rewatch_value') AS INTEGER)
        WHEN json_valid(list_status_json) AND json_type(list_status_json, '$.rewatch_value') = 'text'
             AND TRIM(CAST(json_extract(list_status_json, '$.rewatch_value') AS TEXT)) <> ''
             AND TRIM(CAST(json_extract(list_status_json, '$.rewatch_value') AS TEXT)) NOT GLOB '*[^0-9]*'
             AND CAST(json_extract(list_status_json, '$.rewatch_value') AS INTEGER) BETWEEN 0 AND 5
            THEN CAST(json_extract(list_status_json, '$.rewatch_value') AS INTEGER)
        ELSE rewatch_value
    END,
    tag_count = CASE
        WHEN json_valid(list_status_json) AND json_type(list_status_json, '$.tags') = 'array'
            THEN COALESCE(json_array_length(json_extract(list_status_json, '$.tags')), 0)
        WHEN json_valid(list_status_json) AND json_type(list_status_json, '$.tags') = 'text'
             AND TRIM(CAST(json_extract(list_status_json, '$.tags') AS TEXT)) <> ''
            THEN 1
        ELSE tag_count
    END,
    has_comments = CASE
        WHEN json_valid(list_status_json) AND json_type(list_status_json, '$.comments') = 'text'
             AND TRIM(CAST(json_extract(list_status_json, '$.comments') AS TEXT)) <> ''
            THEN 1
        ELSE has_comments
    END;

CREATE INDEX IF NOT EXISTS idx_mal_user_anime_list_cache_priority_pref
    ON mal_user_anime_list_cache(priority DESC, list_status, mal_anime_id)
    WHERE priority IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_mal_user_anime_list_cache_rewatch_pref
    ON mal_user_anime_list_cache(is_rewatching, num_times_rewatched DESC, rewatch_value DESC, mal_anime_id)
    WHERE is_rewatching IS NOT NULL OR num_times_rewatched IS NOT NULL OR rewatch_value IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_mal_user_anime_list_cache_private_text_presence
    ON mal_user_anime_list_cache(tag_count, has_comments, mal_anime_id)
    WHERE tag_count > 0 OR has_comments = 1;
