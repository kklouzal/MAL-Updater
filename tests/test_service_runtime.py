from __future__ import annotations

import json
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

from mal_updater.config import ensure_directories, load_config
from mal_updater.request_tracking import estimate_budget_recovery_seconds
from mal_updater.service_runtime import run_pending_tasks


class ServiceRuntimeBudgetBackoffTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)
        self.project_root = Path(self.temp_dir.name)
        (self.project_root / ".MAL-Updater" / "config").mkdir(parents=True)
        (self.project_root / ".MAL-Updater" / "secrets").mkdir(parents=True)
        (self.project_root / ".MAL-Updater" / "secrets" / "crunchyroll_username.txt").write_text("user@example.com\n", encoding="utf-8")
        (self.project_root / ".MAL-Updater" / "secrets" / "crunchyroll_password.txt").write_text("secret\n", encoding="utf-8")
        self.config = load_config(self.project_root)
        ensure_directories(self.config)

    def _write_request_events(self, provider: str, offsets_seconds: list[int]) -> None:
        now = datetime.now(timezone.utc).replace(microsecond=0)
        events: list[str] = []
        for offset in offsets_seconds:
            at = (now - timedelta(seconds=offset)).isoformat().replace("+00:00", "Z")
            events.append(
                json.dumps(
                    {
                        "at": at,
                        "provider": provider,
                        "operation": "test-op",
                        "url": "https://example.invalid/api",
                        "method": "GET",
                        "outcome": "ok",
                        "status_code": 200,
                        "error": None,
                    },
                    sort_keys=True,
                )
            )
        self.config.api_request_events_path.write_text("\n".join(events) + "\n", encoding="utf-8")

    def test_estimate_budget_recovery_seconds_waits_until_enough_events_age_out(self) -> None:
        self._write_request_events("crunchyroll", [50, 100, 200])
        recovery = estimate_budget_recovery_seconds(provider="crunchyroll", limit=3, critical_ratio=0.95, config=self.config)
        self.assertGreaterEqual(recovery, 3500)
        self.assertLessEqual(recovery, 3555)

    def test_run_pending_tasks_records_budget_backoff_and_skips_rechecks_until_expiry(self) -> None:
        self._write_request_events("crunchyroll", [50, 100, 200])
        self.config.service.crunchyroll_hourly_limit = 3

        with patch("mal_updater.service_runtime._refresh_mal_tokens", return_value={"status": "ok"}), patch(
            "mal_updater.service_runtime._run_subprocess",
            return_value={"status": "ok", "label": "health", "returncode": 0, "stdout": "", "stderr": ""},
        ):
            result = run_pending_tasks(self.config)

        sync_result = next(item for item in result["results"] if item["task"] == "sync_fetch_crunchyroll")
        self.assertEqual("skipped", sync_result["status"])
        self.assertIn("crunchyroll_budget_critical", sync_result["reason"])
        self.assertGreater(sync_result["budget_backoff_remaining_seconds"], 0)

        state = json.loads(self.config.service_state_path.read_text(encoding="utf-8"))
        sync_state = state["tasks"]["sync_fetch_crunchyroll"]
        self.assertIn("budget_backoff_until", sync_state)
        self.assertIn("budget_backoff_until_epoch", sync_state)
        self.assertEqual("skipped", sync_state["last_status"])
        self.assertEqual("crunchyroll", sync_state["budget_provider"])
        self.assertEqual(self.config.service.sync_every_seconds, sync_state["every_seconds"])
        self.assertIn("next_due_at", sync_state)

        with patch("mal_updater.service_runtime._refresh_mal_tokens", return_value={"status": "ok"}), patch(
            "mal_updater.service_runtime._run_subprocess",
            side_effect=AssertionError("budget-backed-off sync should not re-run subprocesses"),
        ):
            result_second = run_pending_tasks(self.config)

        sync_result_second = next(item for item in result_second["results"] if item["task"] == "sync_fetch_crunchyroll")
        self.assertEqual("skipped", sync_result_second["status"])
        self.assertIn("budget_backoff_active", sync_result_second["reason"])

        state_second = json.loads(self.config.service_state_path.read_text(encoding="utf-8"))
        sync_state_second = state_second["tasks"]["sync_fetch_crunchyroll"]
        self.assertEqual("skipped", sync_state_second["last_status"])
        self.assertIn("budget_backoff_active", sync_state_second["last_skip_reason"])
        self.assertGreater(sync_state_second["budget_backoff_remaining_seconds"], 0)

    def test_run_pending_tasks_clears_budget_backoff_after_successful_run(self) -> None:
        state = {
            "started_at": "2026-03-20T20:00:00Z",
            "tasks": {
                "sync_fetch_crunchyroll": {
                    "budget_backoff_until_epoch": 1,
                    "budget_backoff_until": "2026-03-20T21:00:00Z",
                }
            },
        }
        self.config.service_state_path.write_text(json.dumps(state, indent=2), encoding="utf-8")

        with patch("mal_updater.service_runtime._budget_gate", side_effect=[(True, None, {"provider": "mal"}), (True, None, {"provider": "crunchyroll"}), (True, None, {"provider": "mal"}), (True, None, None)]), patch(
            "mal_updater.service_runtime._refresh_mal_tokens",
            return_value={"status": "ok"},
        ), patch(
            "mal_updater.service_runtime._run_subprocess",
            side_effect=[
                {"status": "ok", "label": "sync_fetch_crunchyroll", "returncode": 0, "stdout": "", "stderr": ""},
                {"status": "ok", "label": "sync_apply", "returncode": 0, "stdout": "", "stderr": ""},
                {"status": "ok", "label": "health", "returncode": 0, "stdout": "", "stderr": ""},
            ],
        ):
            run_pending_tasks(self.config)

        saved = json.loads(self.config.service_state_path.read_text(encoding="utf-8"))
        self.assertNotIn("budget_backoff_until", saved["tasks"]["sync_fetch_crunchyroll"])
        self.assertNotIn("budget_backoff_until_epoch", saved["tasks"]["sync_fetch_crunchyroll"])
        self.assertEqual("ok", saved["tasks"]["sync_fetch_crunchyroll"]["last_status"])
        self.assertIn("next_due_at", saved["tasks"]["sync_fetch_crunchyroll"])
        self.assertNotIn("last_skip_reason", saved["tasks"]["sync_fetch_crunchyroll"])
        self.assertIn("last_started_at", saved["tasks"]["sync_fetch_crunchyroll"])
        self.assertIn("last_finished_at", saved["tasks"]["sync_fetch_crunchyroll"])
        self.assertIn("last_decision_at", saved["tasks"]["sync_fetch_crunchyroll"])
        self.assertIn("last_duration_seconds", saved["tasks"]["sync_fetch_crunchyroll"])
