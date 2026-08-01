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
from mal_updater.service_manager import doctor_service, service_status, unit_contents
from mal_updater.service_systemd_status import build_automation_installation_status, read_systemd_user_unit_runtime
from mal_updater.service_units import render_repo_systemd_unit_template


class ServiceStatusTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)
        self.project_root = Path(self.temp_dir.name)
        (self.project_root / ".MAL-Updater" / "config").mkdir(parents=True)
        (self.project_root / ".MAL-Updater" / "config" / "settings.toml").write_text("[service]\nfull_refresh_every_seconds = 86400\n", encoding="utf-8")
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

    def test_unit_contents_renders_repo_owned_template_with_service_manager_inputs(self) -> None:
        template_source = Path(__file__).resolve().parents[1] / "ops" / "systemd-user" / "mal-updater.service"
        template_target = self.project_root / "ops" / "systemd-user" / "mal-updater.service"
        template_target.parent.mkdir(parents=True, exist_ok=True)
        template_target.write_text(template_source.read_text(encoding="utf-8"), encoding="utf-8")
        fake_home = self.project_root / "fake-home"
        fake_python = self.project_root / "venv" / "bin" / "python"

        def fake_python_probe(command: list[str], **kwargs: object) -> Mock:
            self.assertEqual(["python3", "-c", "import sys; print(sys.executable)"], command)
            return Mock(stdout=f"{fake_python}\n")

        with (
            patch("mal_updater.service_manager.subprocess.run", side_effect=fake_python_probe),
            patch.dict("os.environ", {"HOME": str(fake_home)}, clear=False),
        ):
            rendered = unit_contents(self.config)

        expected = render_repo_systemd_unit_template(
            self.config.project_root,
            fake_home / ".config" / "mal-updater-service.env",
            fake_python,
        )
        self.assertEqual(expected, rendered)
        self.assertIn(f"WorkingDirectory={self.config.project_root}", rendered)
        self.assertIn(f"Environment=PYTHONPATH={self.config.project_root}/src", rendered)
        self.assertIn(f"EnvironmentFile=-{fake_home}/.config/mal-updater-service.env", rendered)
        self.assertIn(
            f"ExecStart={fake_python} -m mal_updater.cli --project-root {self.config.project_root} service-run",
            rendered,
        )

    def test_doctor_service_includes_recent_task_state_and_log_tail(self) -> None:
        now = datetime.now(timezone.utc)
        sync_next_due = (now + timedelta(hours=1)).replace(microsecond=0).isoformat().replace("+00:00", "Z")
        health_budget_backoff_until = (now + timedelta(minutes=30)).replace(microsecond=0).isoformat().replace("+00:00", "Z")
        health_next_due = (now + timedelta(hours=1)).replace(microsecond=0).isoformat().replace("+00:00", "Z")
        fetch_failure_backoff_until = (now + timedelta(minutes=10)).replace(microsecond=0).isoformat().replace("+00:00", "Z")
        fetch_next_due = (now + timedelta(hours=1)).replace(microsecond=0).isoformat().replace("+00:00", "Z")
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
                            "every_seconds": 3600,
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
                            "every_seconds": 3600,
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
                            "every_seconds": 3600,
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
        self.assertEqual(3600, sync_summary["every_seconds"])
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
        self.assertEqual(3600, health_summary["every_seconds"])
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
        self.assertEqual("hot", fetch_summary["planned_fetch_mode"])
        self.assertIn("failure_backoff_remaining_seconds", fetch_summary)

    def test_doctor_service_sanitizes_contaminated_state_log_and_health(self) -> None:
        sentinel = "SENTINEL-doctor-credential-123456789"
        self.config.service_state_path.write_text(
            json.dumps(
                {
                    "last_loop_at": "2026-03-20T21:55:00Z",
                    "tasks": {
                        "sync_fetch_crunchyroll": {
                            "last_status": "error",
                            "last_error": f"HTTP 401 invalid_grant access_token={sentinel}",
                            "failure_backoff_reason": f"Bearer {sentinel} HTTP 401 invalid_grant",
                            "last_result": {
                                "status": "error",
                                "request_url": f"https://user:{sentinel}@example.invalid/hook?token={sentinel}&page=2#private",
                                "stderr": f"password={sentinel} useful failure",
                            },
                        }
                    },
                }
            ),
            encoding="utf-8",
        )
        self.config.service_log_path.write_text(
            f"authorization: Bearer {sentinel} HTTP 401 invalid_grant\n" + ("x" * 5000),
            encoding="utf-8",
        )
        self.config.health_latest_json_path.parent.mkdir(parents=True, exist_ok=True)
        self.config.health_latest_json_path.write_text(
            json.dumps(
                {
                    "healthy": False,
                    "warning_count": 1,
                    "warnings": [{"code": "auth_degraded", "detail": f"refresh_token={sentinel}"}],
                    "maintenance": {
                        "recommended_command": {"command": f"tool --api-key={sentinel}"}
                    },
                }
            ),
            encoding="utf-8",
        )

        with patch("mal_updater.service_manager._run", return_value=Mock(returncode=1, stdout="", stderr=f"Basic {sentinel}")):
            payload = doctor_service(self.config)

        rendered = json.dumps(payload)
        self.assertNotIn(sentinel, rendered)
        self.assertIn("HTTP 401 invalid_grant", rendered)
        self.assertIn("auth_degraded", rendered)
        warning = payload["health_latest_summary"]["warnings"][0]
        self.assertEqual("auth_degraded", warning["code"])
        self.assertIn("https://example.invalid/hook?token=%3Credacted%3E&page=%3Cvalue%3E", rendered)
        self.assertTrue(all(len(line) <= 1000 for line in payload["service_log_tail"]))

    def test_doctor_service_reports_running_subprocess_state(self) -> None:
        started_epoch = datetime.now(timezone.utc).timestamp() - 12
        self.config.service_state_path.write_text(
            json.dumps(
                {
                    "tasks": {
                        "sync_fetch_crunchyroll": {
                            "execution_state": "running",
                            "running_started_epoch": started_epoch,
                            "running_started_at": "2026-03-20T21:54:00Z",
                            "running_command": "python -m mal_updater.cli --project-root /tmp/project provider-refresh crunchyroll",
                            "running_timeout_seconds": 1200,
                        }
                    }
                }
            ),
            encoding="utf-8",
        )

        with patch("mal_updater.service_manager._run", return_value=Mock(returncode=1, stdout="", stderr="")):
            payload = doctor_service(self.config)

        task = payload["task_state"]["sync_fetch_crunchyroll"]
        self.assertEqual("running", task["execution_state"])
        self.assertEqual("subprocess_active", task["execution_state_reason"])
        self.assertEqual("2026-03-20T21:54:00Z", task["running_started_at"])
        self.assertIn("provider-refresh crunchyroll", task["running_command"])
        self.assertEqual(1200, task["running_timeout_seconds"])
        self.assertGreaterEqual(task["running_duration_seconds"], 0)
        self.assertIn("execution_state_elapsed_seconds", task)

    def test_service_status_raw_prints_running_subprocess_fields(self) -> None:
        self.config.service_state_path.write_text(
            json.dumps(
                {
                    "tasks": {
                        "sync_fetch_crunchyroll": {
                            "execution_state": "running",
                            "running_started_epoch": datetime.now(timezone.utc).timestamp(),
                            "running_started_at": "2026-03-20T21:54:00Z",
                            "running_command": "python -m mal_updater.cli provider-refresh crunchyroll --password <redacted>",
                            "running_timeout_seconds": 1200,
                        }
                    }
                }
            ),
            encoding="utf-8",
        )
        with patch("mal_updater.service_manager._run", return_value=Mock(returncode=1, stdout="", stderr="")):
            exit_code, stdout = self._run_service_status_raw("--format", "summary")

        self.assertEqual(0, exit_code)
        self.assertIn("task_sync_fetch_crunchyroll_execution_state=running", stdout)
        self.assertIn("task_sync_fetch_crunchyroll_running_started_at=2026-03-20T21:54:00Z", stdout)
        self.assertIn("task_sync_fetch_crunchyroll_running_command=python -m mal_updater.cli provider-refresh crunchyroll --password <redacted>", stdout)
        self.assertIn("task_sync_fetch_crunchyroll_running_timeout_seconds=1200", stdout)
        self.assertIn("task_sync_fetch_crunchyroll_running_duration_seconds=", stdout)

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
        self.assertEqual(f"type=UnexpectedJsonType file={self.config.health_latest_json_path.name} expected=object", payload["health_latest_parse_error"])
        self.assertEqual({}, payload["task_state"])
        self.assertIsNone(payload["last_loop_at"])
        self.assertNotIn("api_usage", payload)

    def test_service_status_returns_structured_unavailable_when_systemctl_oserror(self) -> None:
        fake_home = self.project_root / "fake-home"
        with (
            patch("mal_updater.service_manager._run", side_effect=OSError("systemctl unavailable")),
            patch.dict("os.environ", {"HOME": str(fake_home)}, clear=False),
        ):
            payload = service_status()

        self.assertFalse(payload["systemctl_available"])
        self.assertEqual("unavailable", payload["systemctl_status"])
        self.assertFalse(payload["enabled"])
        self.assertFalse(payload["active"])
        self.assertIn("OSError: systemctl unavailable", payload["systemctl_error"])
        self.assertIn("is_enabled", payload["systemctl_errors"])
        self.assertIn("is_active", payload["systemctl_errors"])

    def test_service_status_summary_surfaces_systemctl_oserror(self) -> None:
        with patch("mal_updater.service_manager._run", side_effect=OSError("systemctl unavailable")):
            exit_code, stdout = self._run_service_status_raw("--format", "summary")

        self.assertEqual(0, exit_code)
        self.assertIn("systemctl_status=unavailable", stdout)
        self.assertIn("systemctl_available=False", stdout)
        self.assertIn("systemctl_error=is_enabled: OSError: systemctl unavailable", stdout)
        self.assertIn("enabled=False", stdout)
        self.assertIn("active=False", stdout)

    def test_systemd_runtime_reader_reports_unavailable_without_crashing_on_oserror(self) -> None:
        with patch("mal_updater.service_systemd_status.subprocess.run", side_effect=OSError("systemctl unavailable")):
            runtime_state = read_systemd_user_unit_runtime("mal-updater.service")

        self.assertFalse(runtime_state["available"])
        self.assertEqual("systemctl unavailable", runtime_state["error"])

    def test_automation_installation_status_distinguishes_current_and_outdated_units(self) -> None:
        source_path = self.project_root / "ops" / "systemd-user" / "mal-updater.service"
        source_path.parent.mkdir(parents=True, exist_ok=True)
        source_path.write_text(
            "\n".join(
                [
                    "[Unit]",
                    "Description=MAL-Updater",
                    "",
                    "[Service]",
                    "WorkingDirectory=__MAL_UPDATER_REPO_ROOT__",
                    "EnvironmentFile=-__MAL_UPDATER_SERVICE_ENV_FILE__",
                    "ExecStart=__MAL_UPDATER_PYTHON_BIN__ -m mal_updater.cli --project-root __MAL_UPDATER_REPO_ROOT__ service-run",
                    "",
                ]
            ),
            encoding="utf-8",
        )
        script_path = self.project_root / "scripts" / "install_user_systemd_units.sh"
        script_path.parent.mkdir(parents=True, exist_ok=True)
        script_path.write_text("#!/usr/bin/env bash\n", encoding="utf-8")
        fake_home = self.project_root / "fake-home"
        target_dir = fake_home / ".config" / "systemd" / "user"
        target_dir.mkdir(parents=True, exist_ok=True)
        env_path = fake_home / ".config" / "mal-updater-service.env"
        rendered = render_repo_systemd_unit_template(self.project_root, env_path)
        target_path = target_dir / "mal-updater.service"
        target_path.write_text(rendered, encoding="utf-8")
        runtime_state = {
            "available": True,
            "active_state": "active",
            "sub_state": "running",
            "unit_file_state": "enabled",
            "next_elapse_at": None,
            "last_trigger_at": None,
            "result": "success",
        }

        with patch.dict("os.environ", {"HOME": str(fake_home)}, clear=False):
            current = build_automation_installation_status(
                self.project_root,
                runtime_reader=Mock(return_value=runtime_state),
            )
            target_path.write_text("[Unit]\nDescription=stale\n", encoding="utf-8")
            outdated = build_automation_installation_status(
                self.project_root,
                runtime_reader=Mock(return_value=runtime_state),
            )

        self.assertIsNotNone(current)
        self.assertTrue(current["all_units_installed"])
        self.assertTrue(current["all_units_current"])
        self.assertEqual([], current["outdated_units"])
        self.assertTrue(current["service_enabled"])
        self.assertTrue(current["service_active"])
        self.assertIsNotNone(outdated)
        self.assertTrue(outdated["all_units_installed"])
        self.assertFalse(outdated["all_units_current"])
        self.assertEqual(["mal-updater.service"], outdated["outdated_units"])
        self.assertFalse(outdated["unit"]["content_matches_repo"])

    def test_service_status_summary_format_emits_operator_lines(self) -> None:
        now = datetime.now(timezone.utc)
        sync_next_due = (now + timedelta(hours=1)).replace(microsecond=0).isoformat().replace("+00:00", "Z")
        health_budget_backoff_until = (now + timedelta(minutes=20)).replace(microsecond=0).isoformat().replace("+00:00", "Z")
        health_next_due = (now + timedelta(hours=1)).replace(microsecond=0).isoformat().replace("+00:00", "Z")
        fetch_failure_backoff_until = (now + timedelta(minutes=10)).replace(microsecond=0).isoformat().replace("+00:00", "Z")
        fetch_next_due = (now + timedelta(hours=1)).replace(microsecond=0).isoformat().replace("+00:00", "Z")
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
                            "every_seconds": 3600,
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
                            "every_seconds": 3600,
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
                            "every_seconds": 3600,
                            "budget_provider": "crunchyroll",
                            "budget_scope": "provider",
                            "projected_request_source": "observed_hot_auto_p90",
                            "projected_request_count": 20,
                            "projected_request_history_window": 7,
                            "projected_request_history_mode": "hot",
                            "projected_request_history_sample_count": 4,
                            "projected_request_percentile": 0.9,
                            "projected_request_percentile_source": "auto",
                            "full_refresh_anchor_epoch": 1,
                            "full_refresh_anchor_at": "1970-01-01T00:00:01Z",
                            "planned_full_refresh_budget_deferred": True,
                            "planned_full_refresh_deferred_reason": "periodic_cadence",
                            "last_fetch_mode": "hot",
                            "last_result": {
                                "status": "ok",
                                "label": "sync_fetch_crunchyroll",
                                "returncode": 0,
                                "reason": "completed",
                                "fetch_mode": "hot",
                                "deferred_full_refresh_reason": "periodic_cadence",
                                "stdout": "hot fetch completed\nwith operator detail"
                            },
                            "next_due_at": fetch_next_due
                        },
                        "push_recommendations_webhook": {
                            "last_run_at": "2026-03-20T21:54:30Z",
                            "last_status": "ok",
                            "every_seconds": 3600,
                            "budget_scope": "none",
                            "next_due_at": "2026-03-21T03:54:30Z",
                            "last_result": {
                                "status": "ok",
                                "label": "push_recommendations_webhook",
                                "delivery_status": "delivered",
                                "request_id": "abc123",
                                "request_url": "http://127.0.0.1:18789/hooks/agent",
                                "http_status": 200
                            }
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
        self.assertIn("task_sync_every_seconds=3600", stdout)
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
        self.assertIn("task_sync_fetch_crunchyroll_projected_request_source=observed_hot_auto_p90", stdout)
        self.assertIn("task_sync_fetch_crunchyroll_projected_request_count=20", stdout)
        self.assertIn("task_sync_fetch_crunchyroll_projected_request_history_window=7", stdout)
        self.assertIn("task_sync_fetch_crunchyroll_projected_request_history_mode=hot", stdout)
        self.assertIn("task_sync_fetch_crunchyroll_projected_request_history_sample_count=4", stdout)
        self.assertIn("task_sync_fetch_crunchyroll_projected_request_percentile=0.9", stdout)
        self.assertIn("task_sync_fetch_crunchyroll_projected_request_percentile_source=auto", stdout)
        self.assertIn("task_sync_fetch_crunchyroll_last_result_status=ok", stdout)
        self.assertIn("task_sync_fetch_crunchyroll_last_result_label=sync_fetch_crunchyroll", stdout)
        self.assertIn("task_sync_fetch_crunchyroll_last_result_returncode=0", stdout)
        self.assertIn("task_sync_fetch_crunchyroll_last_result_reason=completed", stdout)
        self.assertIn("task_sync_fetch_crunchyroll_last_result_fetch_mode=hot", stdout)
        self.assertIn("task_sync_fetch_crunchyroll_last_result_deferred_full_refresh_reason=periodic_cadence", stdout)
        self.assertIn("task_sync_fetch_crunchyroll_last_result_stdout_snippet=hot fetch completed", stdout)
        self.assertIn("task_sync_fetch_crunchyroll_planned_fetch_mode=full_refresh", stdout)
        self.assertIn("task_sync_fetch_crunchyroll_planned_full_refresh_reason=periodic_cadence", stdout)
        self.assertIn("task_sync_fetch_crunchyroll_planned_full_refresh_due_at=1970-01-02T00:00:01Z", stdout)
        self.assertRegex(stdout, r"task_sync_fetch_crunchyroll_planned_full_refresh_overdue_seconds=\d+")
        self.assertIn("task_sync_fetch_crunchyroll_planned_full_refresh_budget_deferred=True", stdout)
        self.assertIn("task_sync_fetch_crunchyroll_planned_full_refresh_deferred_reason=periodic_cadence", stdout)
        self.assertIn("task_push_recommendations_webhook_last_status=ok", stdout)
        self.assertIn("task_push_recommendations_webhook_last_result_delivery_status=delivered", stdout)
        self.assertIn("task_push_recommendations_webhook_last_result_request_id=abc123", stdout)
        self.assertIn("task_push_recommendations_webhook_last_result_request_url=http://127.0.0.1:18789/hooks/agent", stdout)
        self.assertIn("task_push_recommendations_webhook_last_result_http_status=200", stdout)
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
                            "every_seconds": 3600,
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
                            "last_fetch_mode": "hot",
                            "every_seconds": 3600,
                            "budget_provider": "crunchyroll",
                            "budget_scope": "provider",
                            "full_refresh_anchor_epoch": stale_anchor,
                            "full_refresh_anchor_at": "1970-01-01T00:00:01Z",
                            "planned_full_refresh_budget_deferred": True,
                            "planned_full_refresh_deferred_reason": "periodic_cadence",
                            "last_result": {
                                "status": "ok",
                                "label": "sync_fetch_crunchyroll",
                                "returncode": 0,
                                "fetch_mode": "hot",
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
        self.assertIn("service_state_parse_error=type=JSONDecodeError", stdout)
        self.assertIn(f"health_latest_parse_error=type=UnexpectedJsonType file={self.config.health_latest_json_path.name} expected=object", stdout)

    def test_service_status_exposes_effective_niceness_policy_and_cache_horizons(self) -> None:
        with patch("mal_updater.service_manager._run", return_value=Mock(returncode=1, stdout="", stderr="not-found\n")):
            payload = doctor_service(self.config)

        policy = payload["niceness_policy"]
        self.assertEqual(86400, policy["cadences"]["provider_cold_full_seconds"])
        self.assertEqual(28800, policy["cadences"]["mal_user_list_refresh_seconds"])
        self.assertEqual(43200, policy["cadences"]["recommendation_metadata_refresh_seconds"])
        self.assertEqual(3600, policy["cadences"]["provider_eligibility_refresh_seconds"])
        self.assertEqual(14, policy["cache_horizons_days"]["mal_search_positive"])
        self.assertEqual(7, policy["cache_horizons_days"]["provider_eligibility_evidence"])
        self.assertTrue(policy["thresholds"]["task_and_provider_global_budgets_enforced"])
