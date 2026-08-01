CREATE TABLE IF NOT EXISTS mal_user_anime_list_refresh_generations (
    generation INTEGER PRIMARY KEY AUTOINCREMENT,
    refresh_run_id TEXT NOT NULL UNIQUE,
    status TEXT NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'completed', 'partial', 'failed')),
    fetched_at TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    completed_at TEXT,
    error TEXT CHECK (error IS NULL OR length(error) <= 2000),
    pages INTEGER NOT NULL DEFAULT 0 CHECK (pages >= 0),
    items INTEGER NOT NULL DEFAULT 0 CHECK (items >= 0),
    upserted INTEGER NOT NULL DEFAULT 0 CHECK (upserted >= 0),
    pruned INTEGER NOT NULL DEFAULT 0 CHECK (pruned >= 0),
    preserved_absent INTEGER NOT NULL DEFAULT 0 CHECK (preserved_absent >= 0)
);

CREATE TEMP TABLE _mal_user_anime_list_refresh_generation_map AS
WITH groups AS (
    SELECT
        refresh_generation AS old_generation,
        refresh_run_id,
        MIN(fetched_at) AS fetched_at,
        COALESCE(MIN(created_at), CURRENT_TIMESTAMP) AS created_at,
        COALESCE(MAX(updated_at), CURRENT_TIMESTAMP) AS updated_at,
        COUNT(*) AS items,
        COUNT(*) AS upserted,
        MIN(mal_anime_id) AS first_mal_anime_id
    FROM mal_user_anime_list_cache
    GROUP BY refresh_generation, refresh_run_id
),
ranked AS (
    SELECT
        groups.*,
        ROW_NUMBER() OVER (
            PARTITION BY old_generation
            ORDER BY fetched_at ASC, created_at ASC, updated_at ASC, refresh_run_id ASC, first_mal_anime_id ASC
        ) AS old_generation_rank
    FROM groups
),
old_max AS (
    SELECT COALESCE(MAX(old_generation), 0) AS max_generation FROM groups
),
preserved AS (
    SELECT
        old_generation,
        refresh_run_id,
        old_generation AS generation,
        fetched_at,
        created_at,
        updated_at,
        items,
        upserted,
        first_mal_anime_id
    FROM ranked
    WHERE old_generation_rank = 1
),
remapped AS (
    SELECT
        old_generation,
        refresh_run_id,
        (SELECT max_generation FROM old_max) + ROW_NUMBER() OVER (
            ORDER BY old_generation ASC, old_generation_rank ASC, fetched_at ASC, created_at ASC, updated_at ASC, refresh_run_id ASC, first_mal_anime_id ASC
        ) AS generation,
        fetched_at,
        created_at,
        updated_at,
        items,
        upserted,
        first_mal_anime_id
    FROM ranked
    WHERE old_generation_rank > 1
)
SELECT * FROM preserved
UNION ALL
SELECT * FROM remapped;

UPDATE mal_user_anime_list_cache
SET refresh_generation = (
    SELECT generation
    FROM _mal_user_anime_list_refresh_generation_map AS map
    WHERE map.old_generation = mal_user_anime_list_cache.refresh_generation
      AND map.refresh_run_id = mal_user_anime_list_cache.refresh_run_id
)
WHERE EXISTS (
    SELECT 1
    FROM _mal_user_anime_list_refresh_generation_map AS map
    WHERE map.old_generation = mal_user_anime_list_cache.refresh_generation
      AND map.refresh_run_id = mal_user_anime_list_cache.refresh_run_id
      AND map.generation <> mal_user_anime_list_cache.refresh_generation
);

INSERT INTO mal_user_anime_list_refresh_generations (
    generation,
    refresh_run_id,
    status,
    fetched_at,
    created_at,
    updated_at,
    completed_at,
    items,
    upserted
)
SELECT
    generation,
    refresh_run_id,
    'completed',
    fetched_at,
    created_at,
    updated_at,
    updated_at,
    items,
    upserted
FROM _mal_user_anime_list_refresh_generation_map
ORDER BY generation ASC;

UPDATE sqlite_sequence
SET seq = (SELECT COALESCE(MAX(generation), 0) FROM mal_user_anime_list_refresh_generations)
WHERE name = 'mal_user_anime_list_refresh_generations'
  AND seq < (SELECT COALESCE(MAX(generation), 0) FROM mal_user_anime_list_refresh_generations);

INSERT INTO sqlite_sequence (name, seq)
SELECT 'mal_user_anime_list_refresh_generations', max_generation
FROM (SELECT COALESCE(MAX(generation), 0) AS max_generation FROM mal_user_anime_list_refresh_generations)
WHERE max_generation > 0
  AND NOT EXISTS (
      SELECT 1 FROM sqlite_sequence WHERE name = 'mal_user_anime_list_refresh_generations'
  );

DROP TABLE _mal_user_anime_list_refresh_generation_map;

CREATE INDEX IF NOT EXISTS idx_mal_user_anime_list_refresh_generations_status
    ON mal_user_anime_list_refresh_generations(status, generation);

CREATE UNIQUE INDEX IF NOT EXISTS uq_mal_user_anime_list_refresh_generations_active
    ON mal_user_anime_list_refresh_generations(status)
    WHERE status = 'active';
