from __future__ import annotations

import io
import json
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import Mock, patch

from mal_updater.cli import main as cli_main
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

    def _run_service_status_raw(self, *args: str) -> tuple[int, str]:
        argv = [
            "mal-updater",
            "--project-root",
            str(self.project_root),
            "service-status",
            *args,
        ]
        with (
            patch("sys.argv", argv),
            patch("sys.stdout", new_callable=io.StringIO) as stdout,
            patch.dict("os.environ", {"HOME": str(self.project_root / "fake-home")}, clear=False),
        ):
            exit_code = cli_main()
        return exit_code, stdout.getvalue()

    def test_doctor_service_includes_recent_task_state_and_log_tail(self) -> None:
        now = datetime.now(timezone.utc)
        sync_next_due = (now + timedelta(hours=6)).replace(microsecond=0).isoformat().replace("+00:00", "Z")
        health_budget_backoff_until = (now + timedelta(minutes=30)).replace(microsecond=0).isoformat().replace("+00:00", "Z")
        health_next_due = (now + timedelta(hours=12)).replace(microsecond=0).isoformat().replace("+00:00", "Z")
        fetch_failure_backoff_until = (now + timedelta(minutes=10)).replace(microsecond=0).isoformat().replace("+00:00", "Z")
        fetch_next_due = (now + timedelta(hours=6)).replace(microsecond=0).isoformat().replace("+00:00", "Z")
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
                            "every_seconds": 21600,
                            "budget_provider": "mal",
                            "budget_scope": "task",
                            "projected_request_source": "configured",
                            "projected_request_count": 4,
                            "projected_request_total": 8,
                            "projected_request_history_window": 3,
                            "projected_request_history_sample_count": 2,
                            "projected_request_percentile": 0.75,
                            "projected_request_percentile_source": "configured",
                            "projected_ratio": 0.2,
                            "last_request_delta": 3,
                            "next_due_at": sync_next_due,
                            "last_result": {
                                "label": "sync",
                                "returncode": 0,
                                "stdout": "sync completed\nwith useful detail",
                                "stderr": "",
                            },
                        },
                        "health": {
                            "last_skipped_at": "2026-03-20T21:53:00Z",
                            "last_skip_reason": "crunchyroll_budget_critical ratio=1.000 cooldown=1800s",
                            "budget_backoff_level": "critical",
                            "budget_backoff_until": health_budget_backoff_until,
                            "budget_backoff_remaining_seconds": 1800,
                            "budget_backoff_floor_seconds": 1800,
                            "budget_backoff_cooldown_source": "provider_floor",
                            "every_seconds": 43200,
                            "next_due_at": health_next_due,
                        },
                        "sync_fetch_crunchyroll": {
                            "last_run_at": "2026-03-20T21:52:00Z",
                            "last_status": "error",
                            "last_error": "HTTP 401 from Crunchyroll",
                            "failure_backoff_until": fetch_failure_backoff_until,
                            "failure_backoff_remaining_seconds": 600,
                            "failure_backoff_reason": "HTTP 401 from Crunchyroll",
                            "failure_backoff_class": "auth",
                            "failure_backoff_floor_seconds": 7200,
                            "failure_backoff_consecutive_failures": 2,
                            "every_seconds": 21600,
                            "budget_provider": "crunchyroll",
                            "budget_scope": "provider",
                            "full_refresh_anchor_epoch": 0,
                            "next_due_at": fetch_next_due
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
        sync_summary = payload["task_state"]["sync"]
        self.assertEqual(123.0, sync_summary["last_run_epoch"])
        self.assertEqual("2026-03-20T21:54:00Z", sync_summary["last_run_at"])
        self.assertEqual("ok", sync_summary["last_status"])
        self.assertEqual(21600, sync_summary["every_seconds"])
        self.assertEqual("mal", sync_summary["budget_provider"])
        self.assertEqual("task", sync_summary["budget_scope"])
        self.assertEqual("configured", sync_summary["projected_request_source"])
        self.assertEqual(4, sync_summary["projected_request_count"])
        self.assertEqual(8, sync_summary["projected_request_total"])
        self.assertEqual(3, sync_summary["projected_request_history_window"])
        self.assertEqual(2, sync_summary["projected_request_history_sample_count"])
        self.assertEqual(0.75, sync_summary["projected_request_percentile"])
        self.assertEqual("configured", sync_summary["projected_request_percentile_source"])
        self.assertEqual(0.2, sync_summary["projected_ratio"])
        self.assertEqual(3, sync_summary["last_request_delta"])
        self.assertEqual(sync_next_due, sync_summary["next_due_at"])
        self.assertIn("next_due_in_seconds", sync_summary)
        self.assertEqual("waiting_until_due", sync_summary["execution_state"])
        self.assertEqual("next_due_pending", sync_summary["execution_state_reason"])
        self.assertEqual(
            {
                "label": "sync",
                "returncode": 0,
                "stdout_snippet": "sync completed\nwith useful detail",
            },
            sync_summary["last_result"],
        )
        health_summary = payload["task_state"]["health"]
        self.assertEqual("2026-03-20T21:53:00Z", health_summary["last_skipped_at"])
        self.assertEqual("crunchyroll_budget_critical ratio=1.000 cooldown=1800s", health_summary["last_skip_reason"])
        self.assertEqual("critical", health_summary["budget_backoff_level"])
        self.assertEqual(health_budget_backoff_until, health_summary["budget_backoff_until"])
        self.assertEqual("cooling_down_for_budget", health_summary["execution_state"])
        self.assertEqual("budget_backoff_active", health_summary["execution_state_reason"])
        self.assertEqual("critical", health_summary["execution_state_detail"])
        self.assertEqual(1800, health_summary["budget_backoff_floor_seconds"])
        self.assertEqual("provider_floor", health_summary["budget_backoff_cooldown_source"])
        self.assertEqual(43200, health_summary["every_seconds"])
        self.assertEqual(health_next_due, health_summary["next_due_at"])
        self.assertIn("next_due_in_seconds", health_summary)
        self.assertIn("budget_backoff_remaining_seconds", health_summary)
        fetch_summary = payload["task_state"]["sync_fetch_crunchyroll"]
        self.assertEqual("error", fetch_summary["last_status"])
        self.assertEqual("HTTP 401 from Crunchyroll", fetch_summary["last_error"])
        self.assertEqual(fetch_failure_backoff_until, fetch_summary["failure_backoff_until"])
        self.assertEqual("HTTP 401 from Crunchyroll", fetch_summary["failure_backoff_reason"])
        self.assertEqual("auth", fetch_summary["failure_backoff_class"])
        self.assertEqual(7200, fetch_summary["failure_backoff_floor_seconds"])
        self.assertEqual(2, fetch_summary["failure_backoff_consecutive_failures"])
        self.assertEqual("cooling_down_after_failure", fetch_summary["execution_state"])
        self.assertEqual("failure_backoff_active", fetch_summary["execution_state_reason"])
        self.assertEqual("auth", fetch_summary["execution_state_detail"])
        self.assertIn("execution_state_remaining_seconds", fetch_summary)
        self.assertEqual("incremental", fetch_summary["planned_fetch_mode"])
        self.assertIn("failure_backoff_remaining_seconds", fetch_summary)

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

    def test_service_status_summary_format_emits_operator_lines(self) -> None:
        now = datetime.now(timezone.utc)
        sync_next_due = (now + timedelta(hours=6)).replace(microsecond=0).isoformat().replace("+00:00", "Z")
        health_budget_backoff_until = (now + timedelta(minutes=20)).replace(microsecond=0).isoformat().replace("+00:00", "Z")
        health_next_due = (now + timedelta(hours=12)).replace(microsecond=0).isoformat().replace("+00:00", "Z")
        fetch_failure_backoff_until = (now + timedelta(minutes=10)).replace(microsecond=0).isoformat().replace("+00:00", "Z")
        fetch_next_due = (now + timedelta(hours=6)).replace(microsecond=0).isoformat().replace("+00:00", "Z")
        self.config.service_state_path.write_text(
            json.dumps(
                {
                    "last_loop_at": "2026-03-20T21:55:00Z",
                    "api_usage": {
                        "mal": {
                            "request_count": 4,
                            "success_count": 3,
                            "error_count": 1,
                            "last_event_at": "2026-03-20T21:54:30Z",
                        },
                        "crunchyroll": {
                            "request_count": 2,
                            "success_count": 2,
                            "error_count": 0,
                        },
                    },
                    "tasks": {
                        "sync": {
                            "last_run_at": "2026-03-20T21:54:00Z",
                            "last_status": "ok",
                            "last_decision_at": "2026-03-20T21:54:02Z",
                            "last_started_at": "2026-03-20T21:54:00Z",
                            "last_finished_at": "2026-03-20T21:54:02Z",
                            "last_duration_seconds": 2.0,
                            "every_seconds": 21600,
                            "budget_provider": "mal",
                            "budget_scope": "task",
                            "projected_request_source": "configured",
                            "projected_request_count": 4,
                            "projected_request_total": 8,
                            "projected_request_history_window": 3,
                            "projected_request_history_sample_count": 2,
                            "projected_request_percentile": 0.75,
                            "projected_request_percentile_source": "configured",
                            "projected_ratio": 0.2,
                            "last_request_delta": 3,
                            "next_due_at": sync_next_due,
                        },
                        "health": {
                            "last_skipped_at": "2026-03-20T21:53:00Z",
                            "last_skip_reason": "budget_guard",
                            "last_decision_at": "2026-03-20T21:53:00Z",
                            "budget_backoff_level": "warn",
                            "budget_backoff_until": health_budget_backoff_until,
                            "budget_backoff_remaining_seconds": 1200,
                            "budget_backoff_floor_seconds": 900,
                            "budget_backoff_cooldown_source": "provider_floor",
                            "every_seconds": 43200,
                            "next_due_at": health_next_due,
                        },
                        "sync_fetch_crunchyroll": {
                            "last_run_at": "2026-03-20T21:52:00Z",
                            "last_status": "error",
                            "last_error": "HTTP 401 from Crunchyroll",
                            "failure_backoff_until": fetch_failure_backoff_until,
                            "failure_backoff_remaining_seconds": 600,
                            "failure_backoff_reason": "HTTP 401 from Crunchyroll",
                            "failure_backoff_class": "auth",
                            "failure_backoff_floor_seconds": 7200,
                            "failure_backoff_consecutive_failures": 2,
                            "every_seconds": 21600,
                            "budget_provider": "crunchyroll",
                            "budget_scope": "provider",
                            "projected_request_source": "observed_incremental_auto_p90",
                            "projected_request_count": 20,
                            "projected_request_history_window": 7,
                            "projected_request_history_mode": "incremental",
                            "projected_request_history_sample_count": 4,
                            "projected_request_percentile": 0.9,
                            "projected_request_percentile_source": "auto",
                            "full_refresh_anchor_epoch": 1,
                            "full_refresh_anchor_at": "1970-01-01T00:00:01Z",
                            "last_fetch_mode": "incremental",
                            "last_result": {
                                "status": "ok",
                                "label": "sync_fetch_crunchyroll",
                                "returncode": 0,
                                "reason": "completed",
                                "fetch_mode": "incremental",
                                "deferred_full_refresh_reason": "periodic_cadence",
                                "stdout": "incremental fetch completed\nwith operator detail"
                            },
                            "next_due_at": fetch_next_due
                        },
                    },
                },
                indent=2,
            ),
            encoding="utf-8",
        )
        self.config.service_log_path.write_text("line-1\nline-2", encoding="utf-8")
        self.config.health_latest_json_path.parent.mkdir(parents=True, exist_ok=True)
        self.config.health_latest_json_path.write_text(
            json.dumps(
                {
                    "healthy": False,
                    "warnings": [{"code": "open_review_queue"}],
                    "maintenance": {
                        "recommended_command": {
                            "command": "PYTHONPATH=src python3 -m mal_updater.cli review-queue-next",
                            "reason_code": "review_queue_backlog",
                            "automation_safe": True,
                            "requires_auth_interaction": False,
                        },
                        "recommended_automation_command": {
                            "command": "PYTHONPATH=src python3 -m mal_updater.cli review-queue-apply-worklist --limit 1",
                            "reason_code": "apply_review_queue_worklist",
                            "automation_safe": True,
                            "requires_auth_interaction": False,
                        },
                    },
                }
            ),
            encoding="utf-8",
        )

        def fake_run(command: list[str], check: bool = True):
            if command[-2:] == ["is-enabled", "mal-updater.service"]:
                return Mock(returncode=0, stdout="enabled\n", stderr="")
            if command[-2:] == ["is-active", "mal-updater.service"]:
                return Mock(returncode=0, stdout="active\n", stderr="")
            raise AssertionError(f"unexpected command: {command}")

        with patch("mal_updater.service_manager._run", side_effect=fake_run):
            exit_code, stdout = self._run_service_status_raw("--format", "summary")

        self.assertEqual(0, exit_code)
        self.assertIn("unit_exists=False", stdout)
        self.assertIn("enabled=True", stdout)
        self.assertIn("active=True", stdout)
        self.assertIn("last_loop_at=2026-03-20T21:55:00Z", stdout)
        self.assertIn("health_healthy=False", stdout)
        self.assertIn("health_warning_count=1", stdout)
        self.assertIn("health_warnings=open_review_queue", stdout)
        self.assertIn("maintenance_recommended_command=PYTHONPATH=src python3 -m mal_updater.cli review-queue-next", stdout)
        self.assertIn("maintenance_recommended_reason_code=review_queue_backlog", stdout)
        self.assertIn("maintenance_recommended_automation_safe=True", stdout)
        self.assertIn("maintenance_recommended_requires_auth_interaction=False", stdout)
        self.assertIn("maintenance_recommended_auto_command=PYTHONPATH=src python3 -m mal_updater.cli review-queue-apply-worklist --limit 1", stdout)
        self.assertIn("maintenance_recommended_auto_reason_code=apply_review_queue_worklist", stdout)
        self.assertIn("maintenance_recommended_auto_automation_safe=True", stdout)
        self.assertIn("maintenance_recommended_auto_requires_auth_interaction=False", stdout)
        self.assertIn("api_mal_request_count=4", stdout)
        self.assertIn("api_crunchyroll_success_count=2", stdout)
        self.assertIn("task_sync_last_status=ok", stdout)
        self.assertIn("task_sync_every_seconds=21600", stdout)
        self.assertIn("task_sync_budget_provider=mal", stdout)
        self.assertIn("task_sync_budget_scope=task", stdout)
        self.assertIn("task_sync_projected_request_source=configured", stdout)
        self.assertIn("task_sync_projected_request_count=4", stdout)
        self.assertIn("task_sync_projected_request_total=8", stdout)
        self.assertIn("task_sync_projected_request_history_window=3", stdout)
        self.assertIn("task_sync_projected_request_history_sample_count=2", stdout)
        self.assertIn("task_sync_projected_request_percentile=0.75", stdout)
        self.assertIn("task_sync_projected_request_percentile_source=configured", stdout)
        self.assertIn("task_sync_projected_ratio=0.2", stdout)
        self.assertIn("task_sync_last_request_delta=3", stdout)
        self.assertIn("task_sync_last_decision_at=2026-03-20T21:54:02Z", stdout)
        self.assertIn("task_sync_last_started_at=2026-03-20T21:54:00Z", stdout)
        self.assertIn("task_sync_last_finished_at=2026-03-20T21:54:02Z", stdout)
        self.assertIn("task_sync_last_duration_seconds=2.0", stdout)
        self.assertIn(f"task_sync_next_due_at={sync_next_due}", stdout)
        self.assertIn("task_sync_execution_state=waiting_until_due", stdout)
        self.assertIn("task_sync_execution_state_reason=next_due_pending", stdout)
        self.assertIn("task_sync_execution_state_remaining_seconds=", stdout)
        self.assertIn("task_health_last_skip_reason=budget_guard", stdout)
        self.assertIn("task_health_last_decision_at=2026-03-20T21:53:00Z", stdout)
        self.assertIn("task_health_budget_backoff_level=warn", stdout)
        self.assertIn(f"task_health_budget_backoff_until={health_budget_backoff_until}", stdout)
        self.assertIn("task_health_budget_backoff_remaining_seconds=", stdout)
        self.assertIn("task_health_execution_state=cooling_down_for_budget", stdout)
        self.assertIn("task_health_execution_state_reason=budget_backoff_active", stdout)
        self.assertIn("task_health_execution_state_detail=warn", stdout)
        self.assertIn("task_health_execution_state_remaining_seconds=", stdout)
        self.assertIn("task_health_budget_backoff_floor_seconds=900", stdout)
        self.assertIn("task_health_budget_backoff_cooldown_source=provider_floor", stdout)
        self.assertIn(f"task_health_next_due_at={health_next_due}", stdout)
        self.assertIn("task_sync_fetch_crunchyroll_last_status=error", stdout)
        self.assertIn("task_sync_fetch_crunchyroll_last_error=HTTP 401 from Crunchyroll", stdout)
        self.assertIn(f"task_sync_fetch_crunchyroll_failure_backoff_until={fetch_failure_backoff_until}", stdout)
        self.assertIn("task_sync_fetch_crunchyroll_failure_backoff_remaining_seconds=", stdout)
        self.assertIn("task_sync_fetch_crunchyroll_execution_state=cooling_down_after_failure", stdout)
        self.assertIn("task_sync_fetch_crunchyroll_execution_state_reason=failure_backoff_active", stdout)
        self.assertIn("task_sync_fetch_crunchyroll_execution_state_detail=auth", stdout)
        self.assertIn("task_sync_fetch_crunchyroll_execution_state_remaining_seconds=", stdout)
        self.assertIn("task_sync_fetch_crunchyroll_failure_backoff_reason=HTTP 401 from Crunchyroll", stdout)
        self.assertIn("task_sync_fetch_crunchyroll_failure_backoff_class=auth", stdout)
        self.assertIn("task_sync_fetch_crunchyroll_failure_backoff_floor_seconds=7200", stdout)
        self.assertIn("task_sync_fetch_crunchyroll_failure_backoff_consecutive_failures=2", stdout)
        self.assertIn("task_sync_fetch_crunchyroll_budget_scope=provider", stdout)
        self.assertIn("task_sync_fetch_crunchyroll_projected_request_source=observed_incremental_auto_p90", stdout)
        self.assertIn("task_sync_fetch_crunchyroll_projected_request_count=20", stdout)
        self.assertIn("task_sync_fetch_crunchyroll_projected_request_history_window=7", stdout)
        self.assertIn("task_sync_fetch_crunchyroll_projected_request_history_mode=incremental", stdout)
        self.assertIn("task_sync_fetch_crunchyroll_projected_request_history_sample_count=4", stdout)
        self.assertIn("task_sync_fetch_crunchyroll_projected_request_percentile=0.9", stdout)
        self.assertIn("task_sync_fetch_crunchyroll_projected_request_percentile_source=auto", stdout)
        self.assertIn("task_sync_fetch_crunchyroll_last_result_status=ok", stdout)
        self.assertIn("task_sync_fetch_crunchyroll_last_result_label=sync_fetch_crunchyroll", stdout)
        self.assertIn("task_sync_fetch_crunchyroll_last_result_returncode=0", stdout)
        self.assertIn("task_sync_fetch_crunchyroll_last_result_reason=completed", stdout)
        self.assertIn("task_sync_fetch_crunchyroll_last_result_fetch_mode=incremental", stdout)
        self.assertIn("task_sync_fetch_crunchyroll_last_result_deferred_full_refresh_reason=periodic_cadence", stdout)
        self.assertIn("task_sync_fetch_crunchyroll_last_result_stdout_snippet=incremental fetch completed", stdout)
        self.assertIn("task_sync_fetch_crunchyroll_planned_fetch_mode=full_refresh", stdout)
        self.assertIn("task_sync_fetch_crunchyroll_planned_full_refresh_reason=periodic_cadence", stdout)
        self.assertIn("task_sync_fetch_crunchyroll_planned_full_refresh_due_at=1970-01-02T00:00:01Z", stdout)
        self.assertRegex(stdout, r"task_sync_fetch_crunchyroll_planned_full_refresh_overdue_seconds=\d+")
        self.assertIn("task_sync_fetch_crunchyroll_planned_full_refresh_budget_deferred=True", stdout)
        self.assertIn("task_sync_fetch_crunchyroll_planned_full_refresh_deferred_reason=periodic_cadence", stdout)
        self.assertIn("service_log_last_line=line-2", stdout)

    def test_doctor_service_surfaces_planned_full_refresh_reason_for_overdue_fetch_lane(self) -> None:
        stale_anchor = 1
        self.config.service_state_path.write_text(
            json.dumps(
                {
                    "tasks": {
                        "sync_fetch_crunchyroll": {
                            "last_run_at": "2026-03-20T21:52:00Z",
                            "last_status": "ok",
                            "every_seconds": 21600,
                            "budget_provider": "crunchyroll",
                            "budget_scope": "provider",
                            "full_refresh_anchor_epoch": stale_anchor,
                            "full_refresh_anchor_at": "1970-01-01T00:00:01Z",
                            "next_due_at": "2026-03-21T03:52:00Z"
                        }
                    }
                },
                indent=2,
            ),
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

        fetch_summary = payload["task_state"]["sync_fetch_crunchyroll"]
        self.assertEqual("full_refresh", fetch_summary["planned_fetch_mode"])
        self.assertEqual("periodic_cadence", fetch_summary["planned_full_refresh_reason"])
        self.assertEqual("1970-01-02T00:00:01Z", fetch_summary["planned_full_refresh_due_at"])
        self.assertGreater(fetch_summary["planned_full_refresh_overdue_seconds"], 0)
        self.assertNotIn("planned_full_refresh_budget_deferred", fetch_summary)

    def test_doctor_service_surfaces_budget_deferred_full_refresh_pressure(self) -> None:
        stale_anchor = 1
        self.config.service_state_path.write_text(
            json.dumps(
                {
                    "tasks": {
                        "sync_fetch_crunchyroll": {
                            "last_run_at": "2026-03-20T21:52:00Z",
                            "last_status": "ok",
                            "last_fetch_mode": "incremental",
                            "every_seconds": 21600,
                            "budget_provider": "crunchyroll",
                            "budget_scope": "provider",
                            "full_refresh_anchor_epoch": stale_anchor,
                            "full_refresh_anchor_at": "1970-01-01T00:00:01Z",
                            "last_result": {
                                "status": "ok",
                                "label": "sync_fetch_crunchyroll",
                                "returncode": 0,
                                "fetch_mode": "incremental",
                                "deferred_full_refresh_reason": "periodic_cadence"
                            },
                            "next_due_at": "2026-03-21T03:52:00Z"
                        }
                    }
                },
                indent=2,
            ),
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

        fetch_summary = payload["task_state"]["sync_fetch_crunchyroll"]
        self.assertEqual("full_refresh", fetch_summary["planned_fetch_mode"])
        self.assertEqual("periodic_cadence", fetch_summary["planned_full_refresh_reason"])
        self.assertEqual("1970-01-02T00:00:01Z", fetch_summary["planned_full_refresh_due_at"])
        self.assertGreater(fetch_summary["planned_full_refresh_overdue_seconds"], 0)
        self.assertTrue(fetch_summary["planned_full_refresh_budget_deferred"])
        self.assertEqual("periodic_cadence", fetch_summary["planned_full_refresh_deferred_reason"])
        self.assertEqual("periodic_cadence", fetch_summary["last_result"]["deferred_full_refresh_reason"])

    def test_service_status_summary_surfaces_parse_errors(self) -> None:
        self.config.service_state_path.write_text("{not-json", encoding="utf-8")
        self.config.health_latest_json_path.parent.mkdir(parents=True, exist_ok=True)
        self.config.health_latest_json_path.write_text("[]", encoding="utf-8")

        def fake_run(command: list[str], check: bool = True):
            return Mock(returncode=1, stdout="", stderr="not-found\n")

        with patch("mal_updater.service_manager._run", side_effect=fake_run):
            exit_code, stdout = self._run_service_status_raw("--format", "summary")

        self.assertEqual(0, exit_code)
        self.assertIn("enabled=False", stdout)
        self.assertIn("active=False", stdout)
        self.assertIn("service_state_parse_error=JSONDecodeError", stdout)
        self.assertIn(f"health_latest_parse_error=Expected top-level object in {self.config.health_latest_json_path.name}", stdout)
