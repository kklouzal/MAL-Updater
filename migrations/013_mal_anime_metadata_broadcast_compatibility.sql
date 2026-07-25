-- Historical compatibility marker for mal_anime_metadata broadcast columns.
-- The idempotent repair is implemented in mal_updater.db.apply_migrations so it can
-- inspect existing SQLite columns before adding/backfilling canonical names.
SELECT 1;
