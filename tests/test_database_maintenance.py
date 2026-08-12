from __future__ import annotations

import json
import sqlite3
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch

from mal_updater.config import ensure_directories, load_config
from mal_updater.database_maintenance import acquire_database_lock, compact_database_if_due, release_database_lock
from mal_updater.db import bootstrap_database, connect
from mal_updater.service_manager import doctor_service
from mal_updater.service_runtime import run_pending_tasks


class DatabaseMaintenanceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory(prefix="mal-db-maint-test-", dir="/tmp")
        self.addCleanup(self.temp_dir.cleanup)
        self.root = Path(self.temp_dir.name)
        self.config = load_config(self.root)
        ensure_directories(self.config)
        bootstrap_database(self.config.db_path)
        self.config.service.db_compaction_min_interval_seconds = 0
        self.config.service.db_compaction_min_freelist_bytes = 1
        self.config.service.db_compaction_min_freelist_ratio = 0.000001

    def _make_freelist(self) -> int:
        with connect(self.config.db_path) as conn:
            conn.execute("CREATE TABLE compact_probe(payload BLOB NOT NULL)")
            for _ in range(16):
                conn.execute("INSERT INTO compact_probe(payload) VALUES (randomblob(65536))")
            conn.commit()
            before = self.config.db_path.stat().st_size
            conn.execute("DELETE FROM compact_probe")
            conn.commit()
        return before

    def test_threshold_skip_records_clear_reason_without_backup(self) -> None:
        self.config.service.db_compaction_min_freelist_bytes = 10**12
        with patch("mal_updater.database_maintenance.create_backup", side_effect=AssertionError("no backup below thresholds")):
            report = compact_database_if_due(self.config).as_dict()
        self.assertEqual("skipped", report["status"])
        self.assertEqual("freelist_bytes_below_threshold", report["reason"])

    def test_min_interval_skip_uses_previous_success(self) -> None:
        self._make_freelist()
        previous = {"last_success_epoch": time.time(), "last_success_at": "2026-08-12T00:00:00Z"}
        self.config.service.db_compaction_min_interval_seconds = 3600
        report = compact_database_if_due(self.config, previous=previous).as_dict()
        self.assertEqual("skipped", report["status"])
        self.assertEqual("min_interval_not_elapsed", report["reason"])
        self.assertEqual("2026-08-12T00:00:00Z", report["last_success_at"])

    def test_backup_verify_failure_blocks_before_vacuum(self) -> None:
        self._make_freelist()
        with patch("mal_updater.database_maintenance.create_backup", side_effect=RuntimeError("boom")):
            report = compact_database_if_due(self.config).as_dict()
        self.assertEqual("blocked", report["status"])
        self.assertEqual("backup_verify_failed", report["reason"])
        self.assertGreater(report["freelist_bytes"], 0)

    def test_insufficient_space_blocks_before_backup(self) -> None:
        self._make_freelist()
        with patch("mal_updater.database_maintenance.shutil.disk_usage") as disk_usage, patch(
            "mal_updater.database_maintenance.create_backup", side_effect=AssertionError("backup must not run")
        ):
            disk_usage.return_value.free = 1
            report = compact_database_if_due(self.config).as_dict()
        self.assertEqual("blocked", report["status"])
        self.assertEqual("insufficient_database_volume_space", report["reason"])

    def test_post_backup_space_recheck_blocks_and_retains_verified_backup(self) -> None:
        self._make_freelist()
        real_disk_usage = __import__("shutil").disk_usage
        first_free = real_disk_usage(self.config.db_path.parent).free
        usage_type = type(real_disk_usage(self.config.db_path.parent))
        high = usage_type(10**12, 0, first_free)
        low = usage_type(10**12, 10**12 - 1, 1)
        with patch(
            "mal_updater.database_maintenance.shutil.disk_usage",
            side_effect=[high, high, low],
        ):
            report = compact_database_if_due(self.config).as_dict()
        self.assertEqual("blocked", report["status"])
        self.assertEqual("insufficient_database_volume_space_after_backup", report["reason"])
        self.assertEqual(first_free, report["initial_available_free_bytes"])
        self.assertEqual(1, report["post_backup_available_free_bytes"])
        self.assertTrue(Path(report["backup_archive"]).exists())
        self.assertTrue(report["backup_archive_sha256"])
        with sqlite3.connect(f"file:{self.config.db_path}?mode=ro", uri=True) as conn:
            self.assertGreater(int(conn.execute("PRAGMA freelist_count").fetchone()[0]), 0)

    def test_lease_contention_skips_without_vacuum(self) -> None:
        lock = acquire_database_lock(self.config.db_path, exclusive=False, blocking=True)
        self.addCleanup(lambda: release_database_lock(lock))
        report = compact_database_if_due(self.config).as_dict()
        self.assertEqual("skipped", report["status"])
        self.assertEqual("database_writer_lease_busy", report["reason"])

    def test_successful_compaction_writes_verified_backup_and_reclaims_bytes(self) -> None:
        before = self._make_freelist()
        report = compact_database_if_due(self.config).as_dict()
        self.assertEqual("compacted", report["status"])
        self.assertTrue(report["backup_archive"])
        self.assertTrue(Path(report["backup_archive"]).exists())
        self.assertGreater(report["backup_file_count"], 0)
        self.assertLess(report["db_size_after"], before)
        self.assertGreater(report["bytes_reclaimed"], 0)
        self.assertIsInstance(report["last_success_epoch"], float)

    def test_compaction_backup_manifest_excludes_prior_backup_archives(self) -> None:
        self._make_freelist()
        backup_dir = self.config.state_dir / "backups"
        backup_dir.mkdir(parents=True, exist_ok=True)
        prior = backup_dir / "prior-backup.tar.gz"
        prior.write_bytes(b"historical-backup-must-not-be-embedded")
        report = compact_database_if_due(self.config).as_dict()
        self.assertEqual("compacted", report["status"])
        from mal_updater.container_lifecycle import inspect

        verified = inspect(Path(report["backup_archive"]), verify=True)
        manifest_paths = [item["path"] for item in verified["manifest"]["files"]]
        self.assertFalse(any(path.startswith("state/backups/") for path in manifest_paths), manifest_paths)
        self.assertTrue(prior.exists())

    def test_scheduler_projects_compaction_status_and_next_due(self) -> None:
        self.config.service.db_compaction_every_seconds = 60
        self.config.service.sync_every_seconds = 0
        self.config.service.health_every_seconds = 3600
        self.config.service_state_path.write_text(json.dumps({"tasks": {"db_compaction": {"last_run_epoch": 1}}}), encoding="utf-8")
        with patch("mal_updater.service_runtime._available_source_providers", return_value=[]), patch(
            "mal_updater.service_runtime.load_mal_secrets"
        ) as secrets, patch("mal_updater.service_runtime.prune_recommendation_score_snapshots") as prune:
            secrets.return_value.access_token = None
            prune.return_value.as_dict.return_value = {"status": "no_change", "deleted_rows": 0}
            result = run_pending_tasks(self.config)
        self.assertEqual("ok", result["status"])
        state = json.loads(self.config.service_state_path.read_text(encoding="utf-8"))
        task = state["tasks"]["db_compaction"]
        self.assertEqual("skipped", task["last_status"])
        self.assertIn("next_due_at", task)
        self.assertIn("last_result", task)
        status = doctor_service(self.config)
        self.assertIn("db_compaction", status["task_state"])
        self.assertIn("last_result", status["task_state"]["db_compaction"])


if __name__ == "__main__":
    unittest.main()
