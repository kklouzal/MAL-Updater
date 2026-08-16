from __future__ import annotations

import json
import shutil
import sqlite3
import subprocess
import tempfile
import unittest
from unittest import mock
import venv
from contextlib import closing
from pathlib import Path
from zipfile import ZipFile

from mal_updater import db
from mal_updater.db import bootstrap_database, connect, validate_migration_catalog


class MigrationCatalogTests(unittest.TestCase):
    def test_catalog_preserves_historical_order_and_schema_versions(self) -> None:
        self.assertEqual(
            (
                "001_initial.sql",
                "002_mal_metadata_cache.sql",
                "003_mal_recommendation_edges.sql",
                "004_provider_search_cache.sql",
                "004_mal_recommendation_harvest_status.sql",
                "005_recommendation_score_snapshots.sql",
                "006_recommendation_eligibility_evidence.sql",
                "007_mal_user_anime_list_cache.sql",
                "008_niceness_caches.sql",
                "009_recommendation_full_harvest_provenance.sql",
                "010_mal_anime_metadata_official_detail_fields.sql",
                "011_mal_user_anime_list_preference_fields.sql",
                "012_watch_confirmation_provenance.sql",
                "013_mal_anime_metadata_broadcast_compatibility.sql",
                "014_recommendation_provider_enrichment_cursor.sql",
                "015_public_userrecs_resumable_staging.sql",
                "016_provider_watchlist_membership_keys.sql",
                "017_provider_series_observation_provenance.sql",
                "018_provider_title_search_cache_full_key.sql",
                "019_mal_user_anime_list_refresh_generations.sql",
                "020_provider_eligibility_refresh_lifecycle.sql",
                "021_public_userrecs_durable_queue_and_snapshot_guards.sql",
                "022_mal_user_list_durable_pagination.sql",
                "023_public_userrecs_incremental_validation.sql",
                "024_public_userrecs_final_anchor_validation.sql",
                "025_provider_watchlist_current_membership.sql",
                "026_provider_episode_progress_provenance.sql",
                "027_evaluation_events.sql",
                "028_provider_progress_observations.sql",
                "029_provider_fetch_provenance.sql",
                "030_recommendation_decision_ledger.sql",
            ),
            db.MIGRATION_FILENAMES,
        )
        self.assertEqual(
            db.MIGRATION_FILENAMES,
            tuple(migration.name for migration in db.MIGRATIONS),
        )

    def test_catalog_guard_allows_only_historical_duplicate_004_prefix(self) -> None:
        validate_migration_catalog(
            db.MIGRATION_FILENAMES,
            packaged_filenames=db.MIGRATION_FILENAMES,
        )
        first_after_012 = db.MIGRATION_FILENAMES.index("013_mal_anime_metadata_broadcast_compatibility.sql")
        invalid_filenames = (
            db.MIGRATION_FILENAMES[:first_after_012]
            + ("012_future_duplicate.sql",)
            + db.MIGRATION_FILENAMES[first_after_012:]
        )
        with self.assertRaisesRegex(RuntimeError, "duplicate numeric prefix"):
            validate_migration_catalog(
                invalid_filenames,
                packaged_filenames=invalid_filenames,
            )
        with self.assertRaisesRegex(RuntimeError, "historical duplicate migration order changed"):
            validate_migration_catalog(
                (
                    "001_initial.sql",
                    "002_mal_metadata_cache.sql",
                    "003_mal_recommendation_edges.sql",
                    "004_mal_recommendation_harvest_status.sql",
                    "004_provider_search_cache.sql",
                    "005_recommendation_score_snapshots.sql",
                    "006_recommendation_eligibility_evidence.sql",
                    "007_mal_user_anime_list_cache.sql",
                    "008_niceness_caches.sql",
                    "009_recommendation_full_harvest_provenance.sql",
                    "010_mal_anime_metadata_official_detail_fields.sql",
                    "011_mal_user_anime_list_preference_fields.sql",
                    "012_watch_confirmation_provenance.sql",
                    "013_mal_anime_metadata_broadcast_compatibility.sql",
                    "014_recommendation_provider_enrichment_cursor.sql",
                    "015_public_userrecs_resumable_staging.sql",
                    "016_provider_watchlist_membership_keys.sql",
                    "017_provider_series_observation_provenance.sql",
                    "018_provider_title_search_cache_full_key.sql",
                    "019_mal_user_anime_list_refresh_generations.sql",
                    "020_provider_eligibility_refresh_lifecycle.sql",
                    "021_public_userrecs_durable_queue_and_snapshot_guards.sql",
                    "022_mal_user_list_durable_pagination.sql",
                    "023_public_userrecs_incremental_validation.sql",
                    "024_public_userrecs_final_anchor_validation.sql",
                    "025_provider_watchlist_current_membership.sql",
                    "026_provider_episode_progress_provenance.sql",
                    "027_evaluation_events.sql",
                    "028_provider_progress_observations.sql",
                    "029_provider_fetch_provenance.sql",
                    "030_recommendation_decision_ledger.sql",
                ),
                packaged_filenames=db.MIGRATION_FILENAMES,
            )
        with self.assertRaisesRegex(RuntimeError, "order drifted"):
            validate_migration_catalog(
                (
                    "001_initial.sql",
                    "003_mal_recommendation_edges.sql",
                    "002_mal_metadata_cache.sql",
                    "004_provider_search_cache.sql",
                    "004_mal_recommendation_harvest_status.sql",
                    "005_recommendation_score_snapshots.sql",
                    "006_recommendation_eligibility_evidence.sql",
                    "007_mal_user_anime_list_cache.sql",
                    "008_niceness_caches.sql",
                    "009_recommendation_full_harvest_provenance.sql",
                    "010_mal_anime_metadata_official_detail_fields.sql",
                    "011_mal_user_anime_list_preference_fields.sql",
                    "012_watch_confirmation_provenance.sql",
                    "013_mal_anime_metadata_broadcast_compatibility.sql",
                    "014_recommendation_provider_enrichment_cursor.sql",
                    "015_public_userrecs_resumable_staging.sql",
                    "016_provider_watchlist_membership_keys.sql",
                    "017_provider_series_observation_provenance.sql",
                    "018_provider_title_search_cache_full_key.sql",
                    "019_mal_user_anime_list_refresh_generations.sql",
                    "020_provider_eligibility_refresh_lifecycle.sql",
                    "021_public_userrecs_durable_queue_and_snapshot_guards.sql",
                    "022_mal_user_list_durable_pagination.sql",
                    "023_public_userrecs_incremental_validation.sql",
                    "024_public_userrecs_final_anchor_validation.sql",
                    "025_provider_watchlist_current_membership.sql",
                    "026_provider_episode_progress_provenance.sql",
                    "027_evaluation_events.sql",
                    "028_provider_progress_observations.sql",
                    "029_provider_fetch_provenance.sql",
                    "030_recommendation_decision_ledger.sql",
                ),
                packaged_filenames=db.MIGRATION_FILENAMES,
            )

    def test_source_bootstrap_uses_packaged_resources_and_records_filenames(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "source-bootstrap.sqlite3"
            bootstrap_database(db_path)
            with connect(db_path) as conn:
                rows = conn.execute(
                    "SELECT version FROM schema_migrations ORDER BY rowid"
                ).fetchall()

        self.assertEqual(db.MIGRATION_FILENAMES, tuple(row["version"] for row in rows))

    def test_repository_compatibility_files_match_packaged_resources(self) -> None:
        root_migrations = Path(__file__).resolve().parents[1] / "migrations"
        self.assertTrue(root_migrations.is_dir())
        for migration in db.MIGRATIONS:
            root_file = root_migrations / migration.name
            self.assertTrue(root_file.is_file(), root_file)
            self.assertEqual(
                root_file.read_text(encoding="utf-8"),
                migration.read_text(encoding="utf-8"),
            )

    def test_023_to_current_upgrade_applies_024_and_025_once_with_exact_constraints(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "023-upgrade.sqlite3"
            original = db.MIGRATIONS
            first_024 = db.MIGRATION_FILENAMES.index("024_public_userrecs_final_anchor_validation.sql")
            try:
                db.MIGRATIONS = original[:first_024]
                bootstrap_database(db_path)
            finally:
                db.MIGRATIONS = original

            bootstrap_database(db_path)
            bootstrap_database(db_path)
            with connect(db_path) as conn:
                generation_columns = {
                    row["name"]: row for row in conn.execute("PRAGMA table_info(mal_public_userrecs_crawl_generations)")
                }
                watchlist_columns = {
                    row["name"]: row for row in conn.execute("PRAGMA table_info(provider_watchlist)")
                }
                generation_sql = conn.execute(
                    "SELECT sql FROM sqlite_master WHERE type='table' AND name='mal_public_userrecs_crawl_generations'"
                ).fetchone()["sql"]
                watchlist_sql = conn.execute(
                    "SELECT sql FROM sqlite_master WHERE type='table' AND name='provider_watchlist'"
                ).fetchone()["sql"]
                indexes = {row["name"] for row in conn.execute("PRAGMA index_list(provider_watchlist)")}
                markers = {
                    row["version"]: row["n"]
                    for row in conn.execute(
                        "SELECT version, COUNT(*) AS n FROM schema_migrations WHERE version IN (?, ?) GROUP BY version",
                        ("024_public_userrecs_final_anchor_validation.sql", "025_provider_watchlist_current_membership.sql"),
                    )
                }
                quick_check = conn.execute("PRAGMA quick_check").fetchone()[0]

        self.assertEqual(1, generation_columns["final_anchor_step"]["notnull"])
        self.assertEqual("0", generation_columns["final_anchor_step"]["dflt_value"])
        self.assertIn("final_anchor_step BETWEEN 0 AND 2", generation_sql)
        self.assertEqual(1, watchlist_columns["is_active"]["notnull"])
        self.assertEqual("1", watchlist_columns["is_active"]["dflt_value"])
        self.assertIn("is_active IN (0, 1)", watchlist_sql)
        self.assertTrue({"idx_provider_watchlist_active_series", "idx_provider_watchlist_account_generation"} <= indexes)
        self.assertEqual({
            "024_public_userrecs_final_anchor_validation.sql": 1,
            "025_provider_watchlist_current_membership.sql": 1,
        }, markers)
        self.assertEqual("ok", quick_check)

    def test_provider_title_search_cache_uses_full_semantic_primary_key(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "provider-title-search-cache-schema.sqlite3"
            bootstrap_database(db_path)
            with connect(db_path) as conn:
                table_info = conn.execute("PRAGMA table_info(provider_title_search_cache)").fetchall()
                index_rows = conn.execute("PRAGMA index_list(provider_title_search_cache)").fetchall()
                expiry_index_columns = conn.execute(
                    "PRAGMA index_info(idx_provider_title_search_cache_expires)"
                ).fetchall()

        self.assertEqual(
            [
                "provider",
                "normalized_query",
                "query",
                "candidate_mal_anime_id",
                "candidate_title",
                "matches_json",
                "status",
                "fetched_at",
                "expires_at",
                "logic_version",
                "search_limit",
                "identity_key",
            ],
            [row["name"] for row in table_info],
        )
        self.assertEqual(
            [
                (1, "provider"),
                (2, "normalized_query"),
                (3, "logic_version"),
                (4, "search_limit"),
                (5, "identity_key"),
            ],
            [(row["pk"], row["name"]) for row in table_info if row["pk"]],
        )
        self.assertIn("idx_provider_title_search_cache_expires", {row["name"] for row in index_rows})
        self.assertEqual(["expires_at"], [row["name"] for row in expiry_index_columns])

    def test_provider_title_search_cache_full_key_upgrade_preserves_legacy_row_defaults(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "provider-title-search-cache-upgrade.sqlite3"
            original = db.MIGRATIONS
            try:
                migration_index = db.MIGRATION_FILENAMES.index(db.PROVIDER_TITLE_SEARCH_CACHE_FULL_KEY_MIGRATION)
                db.MIGRATIONS = original[:migration_index]
                bootstrap_database(db_path)
            finally:
                db.MIGRATIONS = original

            with connect(db_path) as conn:
                conn.execute(
                    """
                    INSERT INTO provider_title_search_cache(
                        provider, normalized_query, query, candidate_mal_anime_id,
                        candidate_title, matches_json, status, fetched_at, expires_at
                    ) VALUES (
                        'hidive', 'legacy query', 'Legacy Query', 101,
                        'Legacy Candidate', '[{"provider_series_id":"2312"}]',
                        'ok', '2026-07-30T00:00:00Z', '2027-07-30T00:00:00Z'
                    )
                    """
                )
                conn.commit()

            bootstrap_database(db_path)
            with connect(db_path) as conn:
                row = conn.execute(
                    """
                    SELECT provider, normalized_query, query, candidate_mal_anime_id,
                           candidate_title, matches_json, status, fetched_at, expires_at,
                           logic_version, search_limit, identity_key
                    FROM provider_title_search_cache
                    WHERE provider = 'hidive' AND normalized_query = 'legacy query'
                    """
                ).fetchone()
                marker_count = conn.execute(
                    "SELECT COUNT(*) FROM schema_migrations WHERE version = ?",
                    (db.PROVIDER_TITLE_SEARCH_CACHE_FULL_KEY_MIGRATION,),
                ).fetchone()[0]
                integrity = conn.execute("PRAGMA integrity_check").fetchone()[0]

        self.assertIsNotNone(row)
        assert row is not None
        self.assertEqual("Legacy Query", row["query"])
        self.assertEqual(101, row["candidate_mal_anime_id"])
        self.assertEqual('[{"provider_series_id":"2312"}]', row["matches_json"])
        self.assertEqual("legacy-v1", row["logic_version"])
        self.assertEqual(10, row["search_limit"])
        self.assertEqual("", row["identity_key"])
        self.assertEqual(1, marker_count)
        self.assertEqual("ok", integrity)

    def test_provider_watchlist_membership_migration_preserves_fk_and_cascade(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "watchlist-fk.sqlite3"
            bootstrap_database(db_path)
            with connect(db_path) as conn:
                fk_rows = conn.execute("PRAGMA foreign_key_list(provider_watchlist)").fetchall()
                fk_groups: dict[int, list[sqlite3.Row]] = {}
                for row in fk_rows:
                    fk_groups.setdefault(int(row["id"]), []).append(row)
                self.assertTrue(
                    any(
                        group
                        and group[0]["table"] == "provider_series"
                        and group[0]["on_delete"].upper() == "CASCADE"
                        and {(row["from"], row["to"]) for row in group}
                        == {("provider", "provider"), ("provider_series_id", "provider_series_id")}
                        for group in fk_groups.values()
                    ),
                    [dict(row) for row in fk_rows],
                )
                conn.execute(
                    """
                    INSERT INTO provider_series(provider, provider_series_id, title)
                    VALUES ('hidive', '2312', 'Dungeon People')
                    """
                )
                conn.execute(
                    """
                    INSERT INTO provider_watchlist(provider, provider_series_id, list_id, provider_item_id)
                    VALUES ('hidive', '2312', 'favorites', 'item-2312')
                    """
                )
                conn.execute("DELETE FROM provider_series WHERE provider = 'hidive' AND provider_series_id = '2312'")
                remaining = conn.execute(
                    "SELECT COUNT(*) FROM provider_watchlist WHERE provider = 'hidive' AND provider_series_id = '2312'"
                ).fetchone()[0]
                self.assertEqual(0, remaining)

    def test_provider_series_observation_provenance_current_columns(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "provider-provenance-current.sqlite3"
            bootstrap_database(db_path)

            with connect(db_path) as conn:
                columns = {row["name"] for row in conn.execute("PRAGMA table_info(provider_series)")}
                self.assertTrue({"account_observed_at", "catalog_observed_at"} <= columns)
                self.assertEqual(
                    1,
                    conn.execute(
                        "SELECT COUNT(*) FROM schema_migrations WHERE version = ?",
                        (db.PROVIDER_SERIES_OBSERVATION_PROVENANCE_MIGRATION,),
                    ).fetchone()[0],
                )
                self.assertEqual("ok", conn.execute("PRAGMA integrity_check").fetchone()[0])

    def test_provider_series_observation_provenance_upgrade_backfills_linked_and_ambiguous_legacy_rows(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "provider-provenance-upgrade.sqlite3"
            original = db.MIGRATIONS
            migration_index = db.MIGRATION_FILENAMES.index(db.PROVIDER_SERIES_OBSERVATION_PROVENANCE_MIGRATION)
            try:
                db.MIGRATIONS = original[:migration_index]
                bootstrap_database(db_path)
            finally:
                db.MIGRATIONS = original

            with connect(db_path) as conn:
                conn.execute(
                    """
                    INSERT INTO provider_series(provider, provider_series_id, title, last_seen_at)
                    VALUES
                        ('crunchyroll', 'linked-progress', 'Linked Progress', '2026-01-01T00:00:00Z'),
                        ('crunchyroll', 'linked-watchlist', 'Linked Watchlist', '2026-01-02T00:00:00Z'),
                        ('crunchyroll', 'ambiguous-legacy', 'Ambiguous Legacy', '2026-01-03T00:00:00Z')
                    """
                )
                conn.execute(
                    """
                    INSERT INTO provider_episode_progress(
                        provider, provider_episode_id, provider_series_id, raw_json, last_seen_at
                    ) VALUES
                        ('crunchyroll', 'progress-old', 'linked-progress', '{}', '2026-01-04T00:00:00Z'),
                        ('crunchyroll', 'progress-new', 'linked-progress', '{}', '2026-01-05T00:00:00Z')
                    """
                )
                conn.execute(
                    """
                    INSERT INTO provider_watchlist(
                        provider, provider_series_id, list_id, provider_item_id, raw_json, last_seen_at
                    ) VALUES ('crunchyroll', 'linked-watchlist', 'default', 'linked-watchlist', '{}', '2026-01-06T00:00:00Z')
                    """
                )
                conn.commit()

            bootstrap_database(db_path)
            bootstrap_database(db_path)

            with connect(db_path) as conn:
                rows = {
                    row["provider_series_id"]: row
                    for row in conn.execute(
                        """
                        SELECT provider_series_id, last_seen_at, account_observed_at, catalog_observed_at
                        FROM provider_series
                        ORDER BY provider_series_id
                        """
                    )
                }
                self.assertEqual("2026-01-05T00:00:00Z", rows["linked-progress"]["account_observed_at"])
                self.assertEqual("2026-01-06T00:00:00Z", rows["linked-watchlist"]["account_observed_at"])
                self.assertEqual("2026-01-03T00:00:00Z", rows["ambiguous-legacy"]["account_observed_at"])
                self.assertIsNone(rows["linked-progress"]["catalog_observed_at"])
                self.assertIsNone(rows["linked-watchlist"]["catalog_observed_at"])
                self.assertIsNone(rows["ambiguous-legacy"]["catalog_observed_at"])
                self.assertEqual(
                    1,
                    conn.execute(
                        "SELECT COUNT(*) FROM schema_migrations WHERE version = ?",
                        (db.PROVIDER_SERIES_OBSERVATION_PROVENANCE_MIGRATION,),
                    ).fetchone()[0],
                )
                self.assertEqual("ok", conn.execute("PRAGMA integrity_check").fetchone()[0])

    def test_provider_series_observation_provenance_bootstrap_accepts_already_marked_deployed_shape(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "provider-provenance-deployed.sqlite3"
            original = db.MIGRATIONS
            migration_index = db.MIGRATION_FILENAMES.index(db.PROVIDER_SERIES_OBSERVATION_PROVENANCE_MIGRATION)
            try:
                db.MIGRATIONS = original[:migration_index]
                bootstrap_database(db_path)
            finally:
                db.MIGRATIONS = original

            with connect(db_path) as conn:
                conn.execute("ALTER TABLE provider_series ADD COLUMN account_observed_at TEXT")
                conn.execute("ALTER TABLE provider_series ADD COLUMN catalog_observed_at TEXT")
                conn.execute(
                    """
                    INSERT INTO provider_series(provider, provider_series_id, title, last_seen_at, account_observed_at)
                    VALUES ('crunchyroll', 'legacy-live-row', 'Legacy Live Row', '2026-01-07T00:00:00Z', '2026-01-07T00:00:00Z')
                    """
                )
                conn.execute(
                    "INSERT INTO schema_migrations(version) VALUES (?)",
                    (db.PROVIDER_SERIES_OBSERVATION_PROVENANCE_MIGRATION,),
                )
                conn.commit()

            bootstrap_database(db_path)

            with connect(db_path) as conn:
                row = conn.execute(
                    """
                    SELECT account_observed_at, catalog_observed_at
                    FROM provider_series
                    WHERE provider = 'crunchyroll' AND provider_series_id = 'legacy-live-row'
                    """
                ).fetchone()
                self.assertEqual("2026-01-07T00:00:00Z", row["account_observed_at"])
                self.assertIsNone(row["catalog_observed_at"])
                self.assertEqual("ok", conn.execute("PRAGMA integrity_check").fetchone()[0])

    def test_v7_to_v8_failure_rolls_back_and_retry_is_clean(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "v7.sqlite3"
            original = db.MIGRATIONS
            try:
                db.MIGRATIONS = original[: db.MIGRATION_FILENAMES.index("008_niceness_caches.sql")]
                bootstrap_database(db_path)
            finally:
                db.MIGRATIONS = original

            real_execute = db._execute_migration_statement
            calls = 0

            def fail_midway(conn: sqlite3.Connection, statement: str) -> None:
                nonlocal calls
                calls += 1
                if calls == 9:
                    raise RuntimeError("injected migration failure")
                real_execute(conn, statement)

            with connect(db_path) as conn:
                with mock.patch.object(db, "_execute_migration_statement", side_effect=fail_midway):
                    with self.assertRaisesRegex(RuntimeError, "injected migration failure"):
                        db.apply_migrations(conn)
            with connect(db_path) as conn:
                self.assertIsNone(conn.execute("SELECT 1 FROM schema_migrations WHERE version = '008_niceness_caches.sql'").fetchone())
                self.assertIsNone(conn.execute("SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'mal_anime_search_cache'").fetchone())
                provider_columns = {row["name"] for row in conn.execute("PRAGMA table_info(provider_title_search_cache)")}
                self.assertTrue({"logic_version", "search_limit", "identity_key"}.isdisjoint(provider_columns))

            bootstrap_database(db_path)
            with connect(db_path) as conn:
                self.assertIsNotNone(conn.execute("SELECT 1 FROM schema_migrations WHERE version = '008_niceness_caches.sql'").fetchone())
                self.assertIsNotNone(conn.execute("SELECT 1 FROM schema_migrations WHERE version = '009_recommendation_full_harvest_provenance.sql'").fetchone())
                self.assertIsNotNone(conn.execute("SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'mal_anime_search_cache'").fetchone())
                eligibility_columns = {row["name"] for row in conn.execute("PRAGMA table_info(recommendation_provider_eligibility_evidence)")}
                self.assertTrue({"fetched_at", "expires_at", "last_verified_at"} <= eligibility_columns)
                self.assertTrue({"refresh_status", "failure_count", "next_retry_at", "logic_version"} <= eligibility_columns)
                rec_columns = {row["name"] for row in conn.execute("PRAGMA table_info(mal_anime_recommendations)")}
                self.assertTrue({"harvest_source", "complete_harvest", "provenance_json"} <= rec_columns)
                status_columns = {row["name"] for row in conn.execute("PRAGMA table_info(mal_recommendation_harvest_status)")}
                self.assertTrue({"source_type", "is_complete", "pages_fetched", "source_url", "last_attempted_at", "last_error", "failure_count", "updated_at"} <= status_columns)
                cursor_columns = {row["name"] for row in conn.execute("PRAGMA table_info(recommendation_provider_enrichment_cursor)")}
                self.assertTrue({"provider", "cursor_mal_anime_id", "cursor_rank_key_json", "cursor_generation", "last_selection_class"} <= cursor_columns)
                attempt_columns = {row["name"] for row in conn.execute("PRAGMA table_info(recommendation_provider_enrichment_attempts)")}
                self.assertTrue({"provider", "mal_anime_id", "rank_key_json", "selection_class", "attempted_at", "attempt_count", "last_outcome"} <= attempt_columns)
                userrecs_generation_columns = {row["name"] for row in conn.execute("PRAGMA table_info(mal_public_userrecs_crawl_generations)")}
                self.assertTrue({"generation_id", "source_mal_anime_id", "status", "cursor_url", "pages_fetched", "staged_edge_count"} <= userrecs_generation_columns)
                userrecs_page_columns = {row["name"] for row in conn.execute("PRAGMA table_info(mal_public_userrecs_staged_pages)")}
                self.assertTrue({"generation_id", "page_number", "page_url", "page_fingerprint", "anchor_json", "next_url"} <= userrecs_page_columns)
                userrecs_edge_columns = {row["name"] for row in conn.execute("PRAGMA table_info(mal_public_userrecs_staged_edges)")}
                self.assertTrue({"generation_id", "page_number", "target_mal_anime_id", "target_title", "num_recommendations"} <= userrecs_edge_columns)
                userrecs_event_columns = {row["name"] for row in conn.execute("PRAGMA table_info(mal_public_userrecs_crawl_events)")}
                self.assertTrue({"generation_id", "source_mal_anime_id", "event_type", "page_number", "page_url", "error"} <= userrecs_event_columns)
                mal_list_generation_columns = {row["name"] for row in conn.execute("PRAGMA table_info(mal_user_anime_list_refresh_generations)")}
                self.assertTrue({"publication_epoch", "identity_assertion_nonce", "identity_asserted_revision"} <= mal_list_generation_columns)
                mal_list_page_columns = {row["name"] for row in conn.execute("PRAGMA table_info(mal_user_anime_list_staged_pages)")}
                self.assertTrue({"page_offset", "expected_page_size", "validated_at"} <= mal_list_page_columns)
                mal_list_row_columns = {row["name"] for row in conn.execute("PRAGMA table_info(mal_user_anime_list_staged_rows)")}
                self.assertIn("mal_status", mal_list_row_columns)
                watchlist_columns = {row["name"] for row in conn.execute("PRAGMA table_info(provider_watchlist)")}
                self.assertTrue({"list_id", "list_name", "list_kind", "provider_item_id", "provider_item_type", "position"} <= watchlist_columns)
                self.assertTrue({"is_active", "membership_generation", "account_id_hint", "deactivated_at"} <= watchlist_columns)
                self.assertEqual("ok", conn.execute("PRAGMA integrity_check").fetchone()[0])

            bootstrap_database(db_path)
            with connect(db_path) as conn:
                marker_count = conn.execute(
                    "SELECT COUNT(*) FROM schema_migrations WHERE version = '009_recommendation_full_harvest_provenance.sql'"
                ).fetchone()[0]
                self.assertEqual(1, marker_count)
                self.assertEqual("ok", conn.execute("PRAGMA integrity_check").fetchone()[0])

    def test_v8_to_v9_provenance_upgrade_backfills_and_is_idempotent(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "v8.sqlite3"
            original = db.MIGRATIONS
            try:
                db.MIGRATIONS = original[: db.MIGRATION_FILENAMES.index("009_recommendation_full_harvest_provenance.sql")]
                bootstrap_database(db_path)
            finally:
                db.MIGRATIONS = original

            with connect(db_path) as conn:
                conn.execute(
                    """
                    INSERT INTO mal_anime_recommendations (
                        source_mal_anime_id,
                        target_mal_anime_id,
                        target_title,
                        num_recommendations,
                        hop_distance,
                        source_kind,
                        raw_json,
                        fetched_at
                    ) VALUES (1, 2, 'Two', 3, 1, 'mal_recommendation', '{}', '2026-01-01 00:00:00')
                    """
                )
                conn.execute(
                    """
                    INSERT INTO mal_recommendation_harvest_status (source_mal_anime_id, status, num_edges, fetched_at)
                    VALUES (1, 'fetched', 1, '2026-01-01 00:00:00')
                    """
                )
                conn.commit()

            bootstrap_database(db_path)
            bootstrap_database(db_path)
            with connect(db_path) as conn:
                edge = conn.execute("SELECT harvest_source, complete_harvest, provenance_json FROM mal_anime_recommendations WHERE source_mal_anime_id = 1").fetchone()
                self.assertEqual("official_detail", edge["harvest_source"])
                self.assertEqual(0, edge["complete_harvest"])
                self.assertEqual("{}", edge["provenance_json"])
                status = conn.execute("SELECT source_type, is_complete, pages_fetched, last_attempted_at, failure_count FROM mal_recommendation_harvest_status WHERE source_mal_anime_id = 1").fetchone()
                self.assertEqual("official_detail", status["source_type"])
                self.assertEqual(0, status["is_complete"])
                self.assertEqual(0, status["pages_fetched"])
                self.assertEqual("2026-01-01 00:00:00", status["last_attempted_at"])
                self.assertEqual(0, status["failure_count"])
                marker_count = conn.execute(
                    "SELECT COUNT(*) FROM schema_migrations WHERE version = '009_recommendation_full_harvest_provenance.sql'"
                ).fetchone()[0]
                self.assertEqual(1, marker_count)
                self.assertEqual("ok", conn.execute("PRAGMA integrity_check").fetchone()[0])

    def test_v9_to_current_recommendation_data_upgrades_backfill_privacy_and_current_tables(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "v9.sqlite3"
            original = db.MIGRATIONS
            first_upgrade = db.MIGRATION_FILENAMES.index(
                "010_mal_anime_metadata_official_detail_fields.sql"
            )
            try:
                db.MIGRATIONS = original[:first_upgrade]
                bootstrap_database(db_path)
            finally:
                db.MIGRATIONS = original

            with connect(db_path) as conn:
                conn.execute(
                    """
                    INSERT INTO mal_anime_metadata (
                        mal_anime_id, title, alternative_titles_json, raw_json, fetched_at, updated_at
                    ) VALUES (?, ?, '[]', ?, '2026-01-01 00:00:00', '2026-01-01 00:00:00')
                    """,
                    (
                        10,
                        "Backfill Detail",
                        json.dumps(
                            {
                                "rank": "123",
                                "num_list_users": 200000,
                                "num_scoring_users": 50000,
                                "rating": "PG_13",
                                "average_episode_duration": "1440",
                                "start_date": "2024-01-01",
                                "broadcast": {"day_of_the_week": "Friday", "start_time": "23:30"},
                                "nsfw": "white",
                            }
                        ),
                    ),
                )
                conn.execute(
                    """
                    INSERT INTO mal_user_anime_list_cache (
                        mal_anime_id, title, list_status, user_score, num_episodes_watched,
                        node_json, list_status_json, raw_json, refresh_run_id, refresh_generation,
                        fetched_at, last_seen_at
                    ) VALUES (20, 'Preference Seed', 'completed', 9, 12, '{}', ?, ?, 'run', 7, '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')
                    """,
                    (
                        json.dumps(
                            {
                                "priority": 2,
                                "is_rewatching": True,
                                "num_times_rewatched": 1,
                                "rewatch_value": 5,
                                "tags": ["private tag"],
                                "comments": "private comment",
                            }
                        ),
                        json.dumps({"node": {"id": 20, "title": "Preference Seed"}}),
                    ),
                )
                conn.commit()

            bootstrap_database(db_path)
            bootstrap_database(db_path)
            with connect(db_path) as conn:
                metadata = conn.execute("SELECT rank, num_list_users, num_scoring_users, rating, average_episode_duration, start_date, broadcast_day, broadcast_time, nsfw FROM mal_anime_metadata WHERE mal_anime_id = 10").fetchone()
                self.assertEqual(123, metadata["rank"])
                self.assertEqual(200000, metadata["num_list_users"])
                self.assertEqual(50000, metadata["num_scoring_users"])
                self.assertEqual("pg_13", metadata["rating"])
                self.assertEqual(1440, metadata["average_episode_duration"])
                self.assertEqual("2024-01-01", metadata["start_date"])
                self.assertEqual("friday", metadata["broadcast_day"])
                self.assertEqual("23:30", metadata["broadcast_time"])
                self.assertEqual("white", metadata["nsfw"])
                prefs = conn.execute("SELECT priority, is_rewatching, num_times_rewatched, rewatch_value, tag_count, has_comments, refresh_generation FROM mal_user_anime_list_cache WHERE mal_anime_id = 20").fetchone()
                self.assertEqual(2, prefs["priority"])
                self.assertEqual(1, prefs["is_rewatching"])
                self.assertEqual(1, prefs["num_times_rewatched"])
                self.assertEqual(5, prefs["rewatch_value"])
                self.assertEqual(1, prefs["tag_count"])
                self.assertEqual(1, prefs["has_comments"])
                self.assertEqual(7, prefs["refresh_generation"])
                lifecycle = conn.execute(
                    "SELECT refresh_run_id, status, fetched_at, items, upserted FROM mal_user_anime_list_refresh_generations WHERE generation = 7"
                ).fetchone()
                self.assertEqual("run", lifecycle["refresh_run_id"])
                self.assertEqual("completed", lifecycle["status"])
                self.assertEqual("2026-01-01T00:00:00Z", lifecycle["fetched_at"])
                self.assertEqual(1, lifecycle["items"])
                self.assertEqual(1, lifecycle["upserted"])
                columns = {row["name"] for row in conn.execute("PRAGMA table_info(mal_user_anime_list_cache)")}
                self.assertNotIn("comments", columns)
                self.assertNotIn("tags_json", columns)
                self.assertIsNotNone(conn.execute("SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'watch_confirmation_provenance'").fetchone())
                for version in db.MIGRATION_FILENAMES[first_upgrade:]:
                    marker_count = conn.execute("SELECT COUNT(*) AS n FROM schema_migrations WHERE version = ?", (version,)).fetchone()["n"]
                    self.assertEqual(1, marker_count)
                self.assertEqual("ok", conn.execute("PRAGMA integrity_check").fetchone()[0])

            next_refresh = db.begin_mal_user_anime_list_cache_refresh(
                db_path,
                refresh_run_id="post-upgrade-run",
                fetched_at="2026-01-02T00:00:00Z",
            )
            self.assertEqual(8, next_refresh.generation)

    def test_user_list_lifecycle_upgrade_preserves_colliding_legacy_generation_groups(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "colliding-user-list-generations.sqlite3"
            original = db.MIGRATIONS
            migration_index = db.MIGRATION_FILENAMES.index(
                db.MAL_USER_ANIME_LIST_REFRESH_GENERATIONS_MIGRATION
            )
            try:
                db.MIGRATIONS = original[:migration_index]
                bootstrap_database(db_path)
            finally:
                db.MIGRATIONS = original

            legacy_rows = (
                (101, "Run Seven B First", "run-seven-b", 7, "2026-01-01T00:00:00Z"),
                (102, "Run Seven B Second", "run-seven-b", 7, "2026-01-01T00:01:00Z"),
                (103, "Run Seven A", "run-seven-a", 7, "2026-01-01T00:02:00Z"),
                (104, "Run Nine", "run-nine", 9, "2026-01-01T00:03:00Z"),
            )
            with connect(db_path) as conn:
                conn.executemany(
                    """
                    INSERT INTO mal_user_anime_list_cache (
                        mal_anime_id, title, list_status, user_score, num_episodes_watched,
                        node_json, list_status_json, raw_json, refresh_run_id,
                        refresh_generation, fetched_at, last_seen_at
                    ) VALUES (?, ?, 'completed', 8, 12, '{}', '{}', '{}', ?, ?, ?, ?)
                    """,
                    [(*row, row[-1]) for row in legacy_rows],
                )
                conn.commit()

            bootstrap_database(db_path)
            with connect(db_path) as conn:
                cache_rows = conn.execute(
                    """
                    SELECT mal_anime_id, refresh_run_id, refresh_generation
                    FROM mal_user_anime_list_cache
                    ORDER BY mal_anime_id
                    """
                ).fetchall()
                lifecycle_rows = conn.execute(
                    """
                    SELECT generation, refresh_run_id, status, items, upserted
                    FROM mal_user_anime_list_refresh_generations
                    ORDER BY generation
                    """
                ).fetchall()
                sequence = conn.execute(
                    "SELECT seq FROM sqlite_sequence WHERE name = 'mal_user_anime_list_refresh_generations'"
                ).fetchone()["seq"]

            self.assertEqual(4, len(cache_rows))
            self.assertEqual(3, len(lifecycle_rows))
            self.assertEqual(
                {"run-seven-a", "run-seven-b", "run-nine"},
                {row["refresh_run_id"] for row in lifecycle_rows},
            )
            self.assertEqual(3, len({row["generation"] for row in lifecycle_rows}))
            generation_by_run = {
                row["refresh_run_id"]: row["generation"] for row in lifecycle_rows
            }
            self.assertEqual(7, generation_by_run["run-seven-b"])
            self.assertGreater(generation_by_run["run-seven-a"], 9)
            self.assertEqual(9, generation_by_run["run-nine"])
            self.assertTrue(all(row["status"] == "completed" for row in lifecycle_rows))
            self.assertEqual(
                {"run-seven-a": 1, "run-seven-b": 2, "run-nine": 1},
                {row["refresh_run_id"]: row["items"] for row in lifecycle_rows},
            )
            self.assertEqual(
                {"run-seven-a": 1, "run-seven-b": 2, "run-nine": 1},
                {row["refresh_run_id"]: row["upserted"] for row in lifecycle_rows},
            )
            for cache_row in cache_rows:
                self.assertEqual(
                    generation_by_run[cache_row["refresh_run_id"]],
                    cache_row["refresh_generation"],
                )
            highest_mapped_generation = max(generation_by_run.values())
            self.assertEqual(highest_mapped_generation, sequence)

            next_refresh = db.begin_mal_user_anime_list_cache_refresh(
                db_path,
                refresh_run_id="post-collision-upgrade",
                fetched_at="2026-01-02T00:00:00Z",
            )
            self.assertGreater(next_refresh.generation, highest_mapped_generation)

    def test_recorded_legacy_010_schema_gets_canonical_broadcast_columns(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "legacy-010.sqlite3"
            original = db.MIGRATIONS
            try:
                db.MIGRATIONS = original[: db.MIGRATION_FILENAMES.index("010_mal_anime_metadata_official_detail_fields.sql")]
                bootstrap_database(db_path)
            finally:
                db.MIGRATIONS = original

            with connect(db_path) as conn:
                conn.execute("ALTER TABLE mal_anime_metadata ADD COLUMN rank INTEGER")
                conn.execute("ALTER TABLE mal_anime_metadata ADD COLUMN num_list_users INTEGER")
                conn.execute("ALTER TABLE mal_anime_metadata ADD COLUMN num_scoring_users INTEGER")
                conn.execute("ALTER TABLE mal_anime_metadata ADD COLUMN rating TEXT")
                conn.execute("ALTER TABLE mal_anime_metadata ADD COLUMN average_episode_duration INTEGER")
                conn.execute("ALTER TABLE mal_anime_metadata ADD COLUMN start_date TEXT")
                conn.execute("ALTER TABLE mal_anime_metadata ADD COLUMN end_date TEXT")
                conn.execute("ALTER TABLE mal_anime_metadata ADD COLUMN broadcast_day_of_the_week TEXT")
                conn.execute("ALTER TABLE mal_anime_metadata ADD COLUMN broadcast_start_time TEXT")
                conn.execute("ALTER TABLE mal_anime_metadata ADD COLUMN broadcast_timezone TEXT")
                conn.execute("ALTER TABLE mal_anime_metadata ADD COLUMN nsfw TEXT")
                conn.execute(
                    """
                    INSERT INTO mal_anime_metadata (
                        mal_anime_id, title, alternative_titles_json, raw_json,
                        broadcast_day_of_the_week, broadcast_start_time, broadcast_timezone,
                        fetched_at, updated_at
                    ) VALUES (30, 'Legacy Broadcast', '[]', ?, 'Saturday', '01:05', 'Asia/Tokyo',
                              '2026-01-01 00:00:00', '2026-01-01 00:00:00')
                    """,
                    (json.dumps({"broadcast": {"day_of_the_week": "Tuesday", "start_time": "09:30"}}),),
                )
                conn.execute("INSERT INTO schema_migrations(version) VALUES ('010_mal_anime_metadata_official_detail_fields.sql')")
                conn.execute("INSERT INTO schema_migrations(version) VALUES ('011_mal_user_anime_list_preference_fields.sql')")
                conn.execute("INSERT INTO schema_migrations(version) VALUES ('012_watch_confirmation_provenance.sql')")
                conn.commit()

            bootstrap_database(db_path)
            bootstrap_database(db_path)
            with connect(db_path) as conn:
                columns = {row["name"] for row in conn.execute("PRAGMA table_info(mal_anime_metadata)")}
                self.assertTrue({"broadcast_day", "broadcast_time"} <= columns)
                self.assertTrue({"broadcast_day_of_the_week", "broadcast_start_time"} <= columns)
                row = conn.execute(
                    """
                    SELECT broadcast_day, broadcast_time, broadcast_day_of_the_week, broadcast_start_time
                    FROM mal_anime_metadata
                    WHERE mal_anime_id = 30
                    """
                ).fetchone()
                self.assertEqual("saturday", row["broadcast_day"])
                self.assertEqual("01:05", row["broadcast_time"])
                self.assertEqual("Saturday", row["broadcast_day_of_the_week"])
                self.assertEqual("01:05", row["broadcast_start_time"])
                marker_count = conn.execute(
                    "SELECT COUNT(*) AS n FROM schema_migrations WHERE version = ?",
                    (db.BROADCAST_COMPATIBILITY_MIGRATION,),
                ).fetchone()["n"]
                self.assertEqual(1, marker_count)
                metadata = db.get_mal_anime_metadata_map(db_path)[30]
                self.assertEqual("saturday", metadata.broadcast_day)
                self.assertEqual("01:05", metadata.broadcast_time)
                self.assertEqual("ok", conn.execute("PRAGMA integrity_check").fetchone()[0])

    def test_fresh_schema_keeps_current_broadcast_columns_after_compatibility_migration(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "fresh.sqlite3"
            bootstrap_database(db_path)
            bootstrap_database(db_path)
            with connect(db_path) as conn:
                columns = {row["name"] for row in conn.execute("PRAGMA table_info(mal_anime_metadata)")}
                self.assertTrue({"broadcast_day", "broadcast_time", "broadcast_timezone"} <= columns)
                self.assertFalse({"broadcast_day_of_the_week", "broadcast_start_time"} & columns)
                marker_count = conn.execute(
                    "SELECT COUNT(*) AS n FROM schema_migrations WHERE version = ?",
                    (db.BROADCAST_COMPATIBILITY_MIGRATION,),
                ).fetchone()["n"]
                self.assertEqual(1, marker_count)
                self.assertEqual(db.MIGRATION_FILENAMES, tuple(row["version"] for row in conn.execute("SELECT version FROM schema_migrations ORDER BY rowid")))
                self.assertEqual("ok", conn.execute("PRAGMA integrity_check").fetchone()[0])


class WheelMigrationPackagingTests(unittest.TestCase):
    def test_wheel_contains_migrations_and_bootstraps_outside_repo(self) -> None:
        if shutil.which("uv") is None:
            self.skipTest("uv is required for the installed-wheel migration smoke test")
        repo_root = Path(__file__).resolve().parents[1]
        build_dir = repo_root / "build"
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            dist_dir = temp_path / "dist"
            venv_dir = temp_path / "venv"
            db_path = temp_path / "wheel-bootstrap.sqlite3"

            try:
                subprocess.run(
                    ["uv", "build", "--wheel", "--out-dir", str(dist_dir)],
                    cwd=repo_root,
                    check=True,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                )
            finally:
                shutil.rmtree(build_dir, ignore_errors=True)
            wheels = sorted(dist_dir.glob("mal_updater-*.whl"))
            self.assertEqual(1, len(wheels), wheels)
            with ZipFile(wheels[0]) as wheel:
                migration_members = sorted(
                    member.rsplit("/", 1)[-1]
                    for member in wheel.namelist()
                    if member.startswith("mal_updater/migrations/") and member.endswith(".sql")
                )
            self.assertEqual(sorted(db.MIGRATION_FILENAMES), migration_members)

            venv.EnvBuilder(with_pip=True).create(venv_dir)
            python = venv_dir / "bin" / "python"
            subprocess.run(
                [
                    str(python),
                    "-m",
                    "pip",
                    "install",
                    "--no-deps",
                    "--no-index",
                    "--find-links",
                    str(dist_dir),
                    str(wheels[0]),
                ],
                cwd=temp_path,
                check=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            script = """
import sys
from pathlib import Path
from mal_updater.db import MIGRATION_FILENAMES, bootstrap_database, connect
path = Path(sys.argv[1])
bootstrap_database(path)
with connect(path) as conn:
    rows = [
        row['version']
        for row in conn.execute('SELECT version FROM schema_migrations ORDER BY rowid')
    ]
assert tuple(rows) == MIGRATION_FILENAMES, rows
"""
            subprocess.run(
                [str(python), "-c", script, str(db_path)],
                cwd=temp_path,
                check=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            self.assertTrue(db_path.exists())
            with closing(sqlite3.connect(db_path)) as conn:
                rows = [
                    row[0]
                    for row in conn.execute("SELECT version FROM schema_migrations ORDER BY rowid")
                ]
            self.assertEqual(list(db.MIGRATION_FILENAMES), rows)


if __name__ == "__main__":
    unittest.main()
