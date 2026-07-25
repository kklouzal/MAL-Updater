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
        invalid_filenames = (
            db.MIGRATION_FILENAMES[:-1]
            + ("012_future_duplicate.sql",)
            + db.MIGRATION_FILENAMES[-1:]
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
                    ) VALUES (20, 'Preference Seed', 'completed', 9, 12, '{}', ?, ?, 'run', 1, '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')
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
                prefs = conn.execute("SELECT priority, is_rewatching, num_times_rewatched, rewatch_value, tag_count, has_comments FROM mal_user_anime_list_cache WHERE mal_anime_id = 20").fetchone()
                self.assertEqual(2, prefs["priority"])
                self.assertEqual(1, prefs["is_rewatching"])
                self.assertEqual(1, prefs["num_times_rewatched"])
                self.assertEqual(5, prefs["rewatch_value"])
                self.assertEqual(1, prefs["tag_count"])
                self.assertEqual(1, prefs["has_comments"])
                columns = {row["name"] for row in conn.execute("PRAGMA table_info(mal_user_anime_list_cache)")}
                self.assertNotIn("comments", columns)
                self.assertNotIn("tags_json", columns)
                self.assertIsNotNone(conn.execute("SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'watch_confirmation_provenance'").fetchone())
                for version in db.MIGRATION_FILENAMES[first_upgrade:]:
                    marker_count = conn.execute("SELECT COUNT(*) AS n FROM schema_migrations WHERE version = ?", (version,)).fetchone()["n"]
                    self.assertEqual(1, marker_count)
                self.assertEqual("ok", conn.execute("PRAGMA integrity_check").fetchone()[0])

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
