from __future__ import annotations

import json
import os
import tempfile
import time
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

from mal_updater.config import ensure_directories, load_config
from mal_updater.runtime_housekeeping import inspect_service_log, prune_health_history, rotate_service_log
from mal_updater.service_manager import doctor_service
from mal_updater.service_runtime import TaskSpec, _append_log, run_pending_tasks


class RuntimeHousekeepingTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory(prefix="mal-runtime-housekeeping-", dir="/tmp")
        self.addCleanup(self.temp_dir.cleanup)
        self.root = Path(self.temp_dir.name)
        self.config = load_config(self.root)
        ensure_directories(self.config)

    def _health(self, stamp: str, payload: str = "{}") -> Path:
        path = self.config.health_latest_json_path.parent / f"health-check-{stamp}.json"
        path.write_text(payload, encoding="utf-8")
        return path

    def test_health_retention_preserves_latest_and_newest_floor_while_converging(self) -> None:
        self.config.service.health_history_retention_days = 30
        self.config.service.health_history_min_count = 2
        self.config.service.health_history_prune_batch_size = 2
        self.config.health_latest_json_path.write_text('{"latest":true}', encoding="utf-8")
        for stamp in ("20260101T000000Z", "20260102T000000Z", "20260103T000000Z", "20260104T000000Z", "20260105T000000Z"):
            self._health(stamp, stamp)
        now = datetime(2026, 8, 12, tzinfo=timezone.utc).timestamp()

        first = prune_health_history(self.config, now=now).as_dict()
        second = prune_health_history(self.config, now=now).as_dict()

        self.assertEqual("pruned", first["status"])
        self.assertEqual(2, first["deleted_count"])
        self.assertEqual(3, first["remaining_count"])
        self.assertEqual("pruned", second["status"])
        self.assertEqual(1, second["deleted_count"])
        self.assertEqual(2, second["remaining_count"])
        self.assertEqual('{"latest":true}', self.config.health_latest_json_path.read_text(encoding="utf-8"))
        self.assertTrue(self._health("20260104T000000Z").exists())
        self.assertTrue(self._health("20260105T000000Z").exists())

    def test_health_retention_fails_closed_on_unsafe_name_or_symlink(self) -> None:
        latest = self.config.health_latest_json_path
        latest.write_text("latest", encoding="utf-8")
        old = self._health("20260101T000000Z", "old")
        (latest.parent / "notes.txt").write_text("ambiguous", encoding="utf-8")
        report = prune_health_history(self.config, now=time.time()).as_dict()
        self.assertEqual("blocked", report["status"])
        self.assertEqual("unsafe_health_history_name", report["reason"])
        self.assertTrue(old.exists())
        (latest.parent / "notes.txt").unlink()
        (latest.parent / "health-check-20260102T000000Z.json").symlink_to(old)
        report = prune_health_history(self.config, now=time.time()).as_dict()
        self.assertEqual("blocked", report["status"])
        self.assertEqual("unsafe_health_history_entry", report["reason"])
        self.assertTrue(old.exists())

    def test_service_log_rotates_by_size_and_caps_generations(self) -> None:
        self.config.service.service_log_max_bytes = 20
        self.config.service.service_log_retained_generations = 2
        self.config.service_log_path.write_text("current-current\n", encoding="utf-8")
        self.config.service_log_path.with_name("service.log.1").write_text("generation-one\n", encoding="utf-8")
        self.config.service_log_path.with_name("service.log.2").write_text("generation-two\n", encoding="utf-8")

        report = rotate_service_log(self.config, incoming_bytes=10).as_dict()

        self.assertEqual("rotated", report["status"])
        self.assertFalse(self.config.service_log_path.exists())
        self.assertEqual("current-current\n", self.config.service_log_path.with_name("service.log.1").read_text(encoding="utf-8"))
        self.assertEqual("generation-one\n", self.config.service_log_path.with_name("service.log.2").read_text(encoding="utf-8"))
        self.assertFalse(self.config.service_log_path.with_name("service.log.3").exists())

    def test_append_log_remains_bounded_and_symlink_fails_closed_without_recursion(self) -> None:
        self.config.service.service_log_max_bytes = 80
        self.config.service.service_log_retained_generations = 2
        for index in range(20):
            _append_log(self.config, f"event-{index}")
        diagnostics = inspect_service_log(self.config).as_dict()
        self.assertLessEqual(diagnostics["current_bytes"], 80)
        self.assertLessEqual(diagnostics["generation_count"], 2)

        target = self.root / "external.log"
        target.write_text("sentinel", encoding="utf-8")
        self.config.service_log_path.unlink(missing_ok=True)
        self.config.service_log_path.symlink_to(target)
        _append_log(self.config, "must-not-follow")
        self.assertEqual("sentinel", target.read_text(encoding="utf-8"))
        self.assertEqual("blocked", inspect_service_log(self.config).as_dict()["status"])

    def test_runtime_retention_audit_scheduler_projects_status_and_never_deletes_backup(self) -> None:
        backup_dir = self.config.state_dir / "backups"
        backup_dir.mkdir(parents=True)
        backup = backup_dir / "verified-backup.tar.gz"
        backup.write_bytes(b"backup")
        self.config.service.runtime_retention_audit_every_seconds = 60
        self.config.service_state_path.write_text(json.dumps({"tasks": {"runtime_retention_audit": {"last_run_epoch": 1}}}), encoding="utf-8")

        with patch("mal_updater.service_runtime._task_specs", return_value=[TaskSpec("runtime_retention_audit", 60, None)]), patch(
            "mal_updater.service_runtime.prune_api_request_events_with_diagnostics"
        ) as prune, patch("mal_updater.service_runtime.prune_recommendation_score_snapshots") as snapshot_prune:
            prune.return_value.blocked = False
            prune.return_value.actual_removed = 0
            prune.return_value.as_dict.return_value = {"status": "no_change", "pruned_records": 0, "kept_records": 0, "scanned_records": 0}
            snapshot_prune.return_value.as_dict.return_value = {"status": "no_change", "deleted_rows": 0}
            result = run_pending_tasks(self.config)

        self.assertEqual("ok", result["status"])
        self.assertTrue(backup.exists())
        state = json.loads(self.config.service_state_path.read_text(encoding="utf-8"))
        task = state["tasks"]["runtime_retention_audit"]
        self.assertIn(task["last_status"], {"ok", "warning"})
        self.assertIn("next_due_at", task)
        self.assertTrue(task["last_result"]["read_only"])
        self.assertFalse(task["last_result"]["backup_deletion_performed"])
        status = doctor_service(self.config)
        self.assertIn("runtime_retention_audit", status["task_state"])
        self.assertFalse(status["task_state"]["runtime_retention_audit"]["last_result"]["backup_deletion_performed"])
