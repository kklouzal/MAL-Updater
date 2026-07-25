from __future__ import annotations

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
        with self.assertRaisesRegex(RuntimeError, "duplicate numeric prefix"):
            validate_migration_catalog(
                db.MIGRATION_FILENAMES + ("009_future_duplicate.sql",),
                packaged_filenames=db.MIGRATION_FILENAMES + ("009_future_duplicate.sql",),
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
                db.MIGRATIONS = original[:-2]
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
                db.MIGRATIONS = original[:-1]
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
