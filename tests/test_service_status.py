from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

from mal_updater.config import ensure_directories, load_config
from mal_updater.service_manager import doctor_service


class ServiceStatusTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)
        self.project_root = Path(self.temp_dir.name)
        (self.project_root / ".MAL-Updater" / "config").mkdir(parents=True)
        self.config = load_config(self.project_root)
        ensure_directories(self.config)

    def test_doctor_service_includes_recent_task_state_and_log_tail(self) -> None:
        self.config.service_state_path.write_text(
            json.dumps(
                {
                    "last_loop_at": "2026-03-20T21:55:00Z",
                    "api_usage": {
                        "mal": {"request_count": 4},
                        "crunchyroll": {"request_count": 2},
                    },
                    "tasks": {
                        "sync": {
                            "last_run_epoch": 123.0,
                            "last_run_at": "2026-03-20T21:54:00Z",
                            "last_status": "ok",
                            "last_result": {
                                "label": "sync",
                                "returncode": 0,
                                "stdout": "sync completed\nwith useful detail",
                                "stderr": "",
                            },
                        },
                        "health": {
                            "last_skipped_at": "2026-03-20T21:53:00Z",
                            "last_skip_reason": "crunchyroll_budget_critical ratio=1.000",
                        },
                    },
                },
                indent=2,
            ),
            encoding="utf-8",
        )
        self.config.service_log_path.write_text(
            "\n".join(["line-1", "line-2", "line-3"]),
            encoding="utf-8",
        )
        self.config.health_latest_json_path.parent.mkdir(parents=True, exist_ok=True)
        self.config.health_latest_json_path.write_text(
            json.dumps({"healthy": True, "warning_count": 0}),
            encoding="utf-8",
        )

        def fake_run(command: list[str], check: bool = True):
            if command[-2:] == ["is-enabled", "mal-updater.service"]:
                return Mock(returncode=0, stdout="enabled\n", stderr="")
            if command[-2:] == ["is-active", "mal-updater.service"]:
                return Mock(returncode=0, stdout="active\n", stderr="")
            raise AssertionError(f"unexpected command: {command}")

        fake_home = self.project_root / "fake-home"
        with (
            patch("mal_updater.service_manager._run", side_effect=fake_run),
            patch.dict("os.environ", {"HOME": str(fake_home)}, clear=False),
        ):
            payload = doctor_service(self.config)

        self.assertTrue(payload["enabled"])
        self.assertTrue(payload["active"])
        self.assertEqual("2026-03-20T21:55:00Z", payload["last_loop_at"])
        self.assertEqual({"request_count": 4}, payload["api_usage"]["mal"])
        self.assertEqual(["line-1", "line-2", "line-3"], payload["service_log_tail"])
        self.assertEqual({"healthy": True, "warning_count": 0}, payload["health_latest_summary"])
        self.assertIsNone(payload["service_state_parse_error"])
        self.assertIsNone(payload["health_latest_parse_error"])
        self.assertEqual(
            {
                "last_run_epoch": 123.0,
                "last_run_at": "2026-03-20T21:54:00Z",
                "last_status": "ok",
                "last_result": {
                    "label": "sync",
                    "returncode": 0,
                    "stdout_snippet": "sync completed\nwith useful detail",
                },
            },
            payload["task_state"]["sync"],
        )
        self.assertEqual(
            {
                "last_skipped_at": "2026-03-20T21:53:00Z",
                "last_skip_reason": "crunchyroll_budget_critical ratio=1.000",
            },
            payload["task_state"]["health"],
        )

    def test_doctor_service_reports_state_parse_errors_without_crashing(self) -> None:
        self.config.service_state_path.write_text("{not-json", encoding="utf-8")
        self.config.health_latest_json_path.parent.mkdir(parents=True, exist_ok=True)
        self.config.health_latest_json_path.write_text("[]", encoding="utf-8")

        def fake_run(command: list[str], check: bool = True):
            return Mock(returncode=1, stdout="", stderr="not-found\n")

        fake_home = self.project_root / "fake-home"
        with (
            patch("mal_updater.service_manager._run", side_effect=fake_run),
            patch.dict("os.environ", {"HOME": str(fake_home)}, clear=False),
        ):
            payload = doctor_service(self.config)

        self.assertFalse(payload["enabled"])
        self.assertFalse(payload["active"])
        self.assertIn("JSONDecodeError", payload["service_state_parse_error"])
        self.assertEqual(f"Expected top-level object in {self.config.health_latest_json_path.name}", payload["health_latest_parse_error"])
        self.assertEqual({}, payload["task_state"])
        self.assertIsNone(payload["last_loop_at"])
        self.assertNotIn("api_usage", payload)
