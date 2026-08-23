from __future__ import annotations

import json
import os
import tempfile
import sys
import unittest
from datetime import datetime, timedelta, timezone
import time
from pathlib import Path
from unittest.mock import patch

from mal_updater.config import ensure_directories, load_config
from mal_updater.openclaw_delivery import OpenClawRecommendationDeliveryResult
from mal_updater.request_tracking import begin_api_request_context, end_api_request_context, estimate_budget_recovery_seconds, estimate_budget_recovery_seconds_for_ratio, record_api_request_event
from mal_updater.service_runtime import TaskSpec, _ProcessLease, _apply_sync_command, _budget_gate, _mal_list_refresh_command, _projected_request_count, _recommendation_full_harvest_command, _recommendation_metadata_refresh_command, _run_subprocess, _save_state, _task_execution_signature, effective_niceness_policy, run_pending_tasks, run_service_loop


class ServiceRuntimeLeaseTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)
        self.project_root = Path(self.temp_dir.name)
        self.config = load_config(self.project_root)
        ensure_directories(self.config)

    def test_overlapping_scheduler_pass_is_suppressed_with_holder_evidence(self) -> None:
        holder = _ProcessLease(self.config, "scheduler")
        self.assertTrue(holder.try_acquire(phase="test-holder"))
        self.addCleanup(holder.release)

        with patch("mal_updater.service_runtime._run_pending_tasks_unlocked") as unlocked:
            result = run_pending_tasks(self.config)

        self.assertEqual("skipped", result["status"])
        self.assertEqual("lease_busy", result["reason"])
        self.assertEqual(os.getpid(), result["lease"]["holder"]["pid"])
        unlocked.assert_not_called()

    def test_dead_stale_lease_metadata_is_recovered_under_new_kernel_lock(self) -> None:
        self.config.service.lease_stale_after_seconds = 10
        status_path = self.config.service_leases_dir / "scheduler.json"
        status_path.write_text(
            json.dumps({"status": "running", "pid": 999999, "run_id": "dead-run", "started_epoch": time.time() - 60}),
            encoding="utf-8",
        )

        lease = _ProcessLease(self.config, "scheduler")
        self.assertTrue(lease.try_acquire(phase="recovery"))
        self.addCleanup(lease.release)

        self.assertTrue(lease.status["recovered_previous_lease"])
        self.assertTrue(lease.status["previous_was_stale"])
        self.assertEqual("dead-run", lease.status["previous_run_id"])

    def test_overlapping_task_subprocess_is_suppressed(self) -> None:
        holder = _ProcessLease(self.config, "task-diagnostic")
        self.assertTrue(holder.try_acquire(phase="test-task-holder"))
        self.addCleanup(holder.release)

        with patch("mal_updater.service_runtime.subprocess.run") as subprocess_run:
            result = _run_subprocess(self.config, [sys.executable, "-c", "print('never')"], label="diagnostic")

        self.assertEqual("skipped", result["status"])
        self.assertEqual("lease_busy", result["reason"])
        subprocess_run.assert_not_called()

    def test_run_subprocess_propagates_request_task_and_run_context(self) -> None:
        token = begin_api_request_context(task="sync_fetch_crunchyroll", run_id="run-context-123")
        try:
            result = _run_subprocess(
                self.config,
                [sys.executable, "-c", "import os; print(os.environ['MAL_UPDATER_REQUEST_TASK'] + ':' + os.environ['MAL_UPDATER_REQUEST_RUN_ID'])"],
                label="sync_fetch_crunchyroll",
            )
        finally:
            end_api_request_context(token)

        self.assertEqual("ok", result["status"])
        self.assertEqual("sync_fetch_crunchyroll:run-context-123", result["stdout"].strip())

    def test_daemon_startup_grace_precedes_first_task_pass(self) -> None:
        self.config.service.startup_grace_seconds = 17
        with patch("mal_updater.service_runtime.time.sleep", side_effect=[None, KeyboardInterrupt]) as sleep, patch(
            "mal_updater.service_runtime.run_pending_tasks", return_value={"status": "ok"}
        ) as run_tasks:
            with self.assertRaises(KeyboardInterrupt):
                run_service_loop(self.config)

        self.assertEqual(17, sleep.call_args_list[0].args[0])
        run_tasks.assert_called_once_with(self.config)

    def test_manual_one_shot_has_no_startup_grace(self) -> None:
        self.config.service.startup_grace_seconds = 99
        with patch("mal_updater.service_runtime.time.sleep") as sleep, patch(
            "mal_updater.service_runtime._run_pending_tasks_unlocked", return_value={"status": "ok", "results": []}
        ):
            result = run_pending_tasks(self.config)

        self.assertEqual("ok", result["status"])
        sleep.assert_not_called()

    def test_run_pending_tasks_fails_closed_on_corrupt_service_state(self) -> None:
        sentinel = "SENTINEL-service-state-credential-123456789"
        self.config.service_state_path.write_text(f'{{"tasks": {{}}, "refresh_token": "{sentinel}"', encoding="utf-8")

        with patch("mal_updater.service_runtime._run_subprocess") as run_subprocess:
            result = run_pending_tasks(self.config)

        rendered = json.dumps(result)
        log_text = self.config.service_log_path.read_text(encoding="utf-8")
        self.assertEqual("error", result["status"])
        self.assertEqual("service_state_unavailable", result["reason"])
        self.assertIn("JSONDecodeError", result["service_state_parse_error"])
        self.assertNotIn(sentinel, rendered)
        self.assertNotIn(sentinel, log_text)
        self.assertIn(sentinel, self.config.service_state_path.read_text(encoding="utf-8"))
        run_subprocess.assert_not_called()

    def test_run_pending_tasks_fails_closed_on_corrupt_api_request_events(self) -> None:
        sentinel = "SENTINEL-api-events-credential-123456789"
        current_at = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        valid = {"event_id": "valid", "at": current_at, "provider": "mal", "operation": "current", "outcome": "ok"}
        original_events = json.dumps(valid) + "\n" + f'{{"at":"{current_at}","refresh_token":"{sentinel}"' + "\n"
        self.config.api_request_events_path.write_text(original_events, encoding="utf-8")

        with patch("mal_updater.service_runtime._run_subprocess") as run_subprocess:
            result = run_pending_tasks(self.config)

        rendered = json.dumps(result)
        log_text = self.config.service_log_path.read_text(encoding="utf-8")
        self.assertEqual("error", result["status"])
        self.assertEqual("api_request_events_unavailable", result["reason"])
        self.assertEqual("blocked_corrupt", result["api_request_events_prune"]["status"])
        self.assertEqual(1, result["api_request_events_prune"]["corrupt_records"])
        self.assertEqual(original_events, self.config.api_request_events_path.read_text(encoding="utf-8"))
        self.assertFalse(self.config.service_state_path.exists())
        self.assertNotIn(sentinel, rendered)
        self.assertNotIn(sentinel, log_text)
        run_subprocess.assert_not_called()


class ServiceRuntimeFullRefreshCadenceTests(unittest.TestCase):
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
        self.config.service.sync_every_seconds = 0
        self.config.service.task_execute_limits["sync_apply"] = 8
        self.config.service.health_every_seconds = 3600
        self.config.service.mal_refresh_every_seconds = 3600
        self.config.service.full_refresh_every_seconds = 86400
        now = time.time()
        self.config.service_state_path.write_text(
            json.dumps(
                {
                    "started_at": "2026-03-20T20:00:00Z",
                    "tasks": {
                        "mal_refresh": {"last_run_epoch": now, "last_run_at": "2026-03-20T20:00:00Z"},
                        "health": {"last_run_epoch": now, "last_run_at": "2026-03-20T20:00:00Z"},
                    },
                },
                indent=2,
            ),
            encoding="utf-8",
        )

    def test_run_pending_tasks_seeds_full_refresh_anchor_from_first_hot_fetch(self) -> None:
        with patch("mal_updater.service_runtime._budget_gate", side_effect=[(True, None, {"provider": "crunchyroll"}), (True, None, {"provider": "mal"}), (True, None, None)]), patch(
            "mal_updater.service_runtime._run_subprocess",
            side_effect=[
                {"status": "ok", "label": "sync_fetch_crunchyroll", "returncode": 0, "stdout": "", "stderr": ""},
                {"status": "ok", "label": "sync_apply", "returncode": 0, "stdout": "", "stderr": ""},
                {"status": "ok", "label": "health", "returncode": 0, "stdout": "", "stderr": ""},
            ],
        ) as run_subprocess:
            result = run_pending_tasks(self.config)

        sync_result = next(item for item in result["results"] if item["task"] == "sync_fetch_crunchyroll")
        self.assertEqual("hot", sync_result["fetch_mode"])
        sync_args = run_subprocess.call_args_list[0].args[1]
        self.assertIn("provider-fetch-snapshot", sync_args)
        self.assertEqual(sync_args[sync_args.index("--provider") + 1], "crunchyroll")
        self.assertNotIn("--full-refresh", sync_args)

        state = json.loads(self.config.service_state_path.read_text(encoding="utf-8"))
        sync_state = state["tasks"]["sync_fetch_crunchyroll"]
        self.assertEqual("hot", sync_state["last_fetch_mode"])
        self.assertIn("full_refresh_anchor_epoch", sync_state)
        self.assertNotIn("last_successful_full_refresh_epoch", sync_state)

    def test_run_pending_tasks_persists_and_clears_running_subprocess_state(self) -> None:
        observed_state: dict[str, object] = {}

        def fake_run(config, args, *, label):
            nonlocal observed_state
            if label == "sync_fetch_crunchyroll":
                observed_state = json.loads(config.service_state_path.read_text(encoding="utf-8"))["tasks"]["sync_fetch_crunchyroll"]
            return {"status": "ok", "label": label, "returncode": 0, "stdout": "", "stderr": "", "command": "safe"}

        with patch("mal_updater.service_runtime._run_subprocess", side_effect=fake_run):
            result = run_pending_tasks(self.config)

        self.assertEqual("ok", result["status"])
        self.assertEqual("running", observed_state["execution_state"])
        self.assertIn("running_started_at", observed_state)
        self.assertIn("provider-fetch-snapshot", str(observed_state["running_command"]))
        self.assertIn("--provider crunchyroll", str(observed_state["running_command"]))
        self.assertNotIn("secret", str(observed_state["running_command"]))
        self.assertEqual(self.config.service.task_timeout_seconds, observed_state["running_timeout_seconds"])

        final_state = json.loads(self.config.service_state_path.read_text(encoding="utf-8"))["tasks"]["sync_fetch_crunchyroll"]
        self.assertEqual("idle", final_state["execution_state"])
        self.assertNotIn("running_started_at", final_state)
        self.assertNotIn("running_command", final_state)
        self.assertNotIn("running_timeout_seconds", final_state)

    def test_run_pending_tasks_requests_periodic_provider_full_refresh_when_anchor_is_stale(self) -> None:
        stale_anchor = datetime.now(timezone.utc).timestamp() - 90000
        state = {
            "started_at": "2026-03-20T20:00:00Z",
            "tasks": {
                "mal_refresh": {"last_run_epoch": time.time(), "last_run_at": "2026-03-20T20:00:00Z"},
                "health": {"last_run_epoch": time.time(), "last_run_at": "2026-03-20T20:00:00Z"},
                "sync_fetch_crunchyroll": {
                    "full_refresh_anchor_epoch": stale_anchor,
                    "full_refresh_anchor_at": "2026-03-20T20:00:00Z",
                    "last_run_epoch": 0,
                }
            },
        }
        self.config.service_state_path.write_text(json.dumps(state, indent=2), encoding="utf-8")

        with patch("mal_updater.service_runtime._budget_gate", side_effect=[(True, None, {"provider": "crunchyroll"}), (True, None, {"provider": "mal"}), (True, None, None)]), patch(
            "mal_updater.service_runtime._run_subprocess",
            side_effect=[
                {"status": "ok", "label": "sync_fetch_crunchyroll", "returncode": 0, "stdout": "", "stderr": ""},
                {"status": "ok", "label": "sync_apply", "returncode": 0, "stdout": "", "stderr": ""},
                {"status": "ok", "label": "health", "returncode": 0, "stdout": "", "stderr": ""},
            ],
        ) as run_subprocess:
            result = run_pending_tasks(self.config)

        sync_result = next(item for item in result["results"] if item["task"] == "sync_fetch_crunchyroll")
        self.assertEqual("full_refresh", sync_result["fetch_mode"])
        self.assertEqual("periodic_cadence", sync_result["full_refresh_reason"])
        sync_args = run_subprocess.call_args_list[0].args[1]
        self.assertIn("--full-refresh", sync_args)

        saved = json.loads(self.config.service_state_path.read_text(encoding="utf-8"))
        sync_state = saved["tasks"]["sync_fetch_crunchyroll"]
        self.assertEqual("full_refresh", sync_state["last_fetch_mode"])
        self.assertEqual("periodic_cadence", sync_state["last_full_refresh_reason"])
        self.assertIn("last_successful_full_refresh_epoch", sync_state)
        self.assertGreater(sync_state["full_refresh_anchor_epoch"], stale_anchor)

    def test_run_pending_tasks_downgrades_budget_blocked_full_refresh_to_hot_fetch(self) -> None:
        stale_anchor = datetime.now(timezone.utc).timestamp() - 90000
        state = {
            "started_at": "2026-03-20T20:00:00Z",
            "tasks": {
                "mal_refresh": {"last_run_epoch": time.time(), "last_run_at": "2026-03-20T20:00:00Z"},
                "health": {"last_run_epoch": time.time(), "last_run_at": "2026-03-20T20:00:00Z"},
                "sync_fetch_crunchyroll": {
                    "full_refresh_anchor_epoch": stale_anchor,
                    "full_refresh_anchor_at": "2026-03-20T20:00:00Z",
                    "last_run_epoch": 0,
                }
            },
        }
        self.config.service_state_path.write_text(json.dumps(state, indent=2), encoding="utf-8")

        with patch(
            "mal_updater.service_runtime._budget_gate",
            side_effect=[
                (False, "crunchyroll_budget_projected_critical ratio=0.000 projected_ratio=1.000 projected_requests=55 cooldown=1800s", {"provider": "crunchyroll", "projected_request_source": "configured_full_refresh", "projected_request_count": 55}),
                (True, None, {"provider": "crunchyroll", "projected_request_source": "observed_hot_smoothed", "projected_request_count": 4}),
                (True, None, {"provider": "mal"}),
                (True, None, None),
            ],
        ), patch(
            "mal_updater.service_runtime._run_subprocess",
            side_effect=[
                {"status": "ok", "label": "sync_fetch_crunchyroll", "returncode": 0, "stdout": "", "stderr": ""},
                {"status": "ok", "label": "sync_apply", "returncode": 0, "stdout": "", "stderr": ""},
                {"status": "ok", "label": "health", "returncode": 0, "stdout": "", "stderr": ""},
            ],
        ) as run_subprocess:
            result = run_pending_tasks(self.config)

        sync_result = next(item for item in result["results"] if item["task"] == "sync_fetch_crunchyroll")
        self.assertEqual("hot", sync_result["fetch_mode"])
        self.assertEqual("periodic_cadence", sync_result["deferred_full_refresh_reason"])
        sync_args = run_subprocess.call_args_list[0].args[1]
        self.assertNotIn("--full-refresh", sync_args)

        saved = json.loads(self.config.service_state_path.read_text(encoding="utf-8"))
        sync_state = saved["tasks"]["sync_fetch_crunchyroll"]
        self.assertEqual("hot", sync_state["last_fetch_mode"])
        self.assertEqual(stale_anchor, sync_state["full_refresh_anchor_epoch"])
        self.assertNotIn("last_successful_full_refresh_epoch", sync_state)

    def test_run_pending_tasks_requests_health_recommended_full_refresh(self) -> None:
        self.config.health_latest_json_path.parent.mkdir(parents=True, exist_ok=True)
        self.config.health_latest_json_path.write_text(
            json.dumps(
                {
                    "maintenance": {
                        "recommended_commands": [
                            {
                                "reason_code": "refresh_full_snapshot",
                                "command_args": [
                                    "crunchyroll-fetch-snapshot",
                                    "--full-refresh",
                                    "--out",
                                    ".MAL-Updater/cache/live-crunchyroll-snapshot.json",
                                    "--ingest",
                                ],
                            }
                        ]
                    }
                },
                indent=2,
            ),
            encoding="utf-8",
        )

        with patch("mal_updater.service_runtime._budget_gate", side_effect=[(True, None, {"provider": "crunchyroll"}), (True, None, {"provider": "mal"}), (True, None, None)]), patch(
            "mal_updater.service_runtime._run_subprocess",
            side_effect=[
                {"status": "ok", "label": "sync_fetch_crunchyroll", "returncode": 0, "stdout": "", "stderr": ""},
                {"status": "ok", "label": "sync_apply", "returncode": 0, "stdout": "", "stderr": ""},
                {"status": "ok", "label": "health", "returncode": 0, "stdout": "", "stderr": ""},
            ],
        ) as run_subprocess:
            result = run_pending_tasks(self.config)

        sync_result = next(item for item in result["results"] if item["task"] == "sync_fetch_crunchyroll")
        self.assertEqual("full_refresh", sync_result["fetch_mode"])
        self.assertEqual("health_recommended", sync_result["full_refresh_reason"])
        sync_args = run_subprocess.call_args_list[0].args[1]
        self.assertIn("--full-refresh", sync_args)

        saved = json.loads(self.config.service_state_path.read_text(encoding="utf-8"))
        sync_state = saved["tasks"]["sync_fetch_crunchyroll"]
        self.assertEqual("health_recommended", sync_state["last_full_refresh_reason"])

    def test_lease_busy_child_preserves_due_full_refresh_and_health_request(self) -> None:
        self.config.health_latest_json_path.parent.mkdir(parents=True, exist_ok=True)
        self.config.health_latest_json_path.write_text(json.dumps({"maintenance": {"recommended_commands": [{
            "reason_code": "refresh_full_snapshot",
            "command_args": ["crunchyroll-fetch-snapshot", "--full-refresh"],
        }]}}), encoding="utf-8")
        health_mtime = self.config.health_latest_json_path.stat().st_mtime
        stale_anchor = time.time() - 90000
        state = json.loads(self.config.service_state_path.read_text(encoding="utf-8"))
        state["tasks"]["sync_fetch_crunchyroll"] = {
            "last_run_epoch": 0,
            "full_refresh_anchor_epoch": stale_anchor,
            "execution_signature": "sync_fetch_crunchyroll:mode=full_refresh",
            "last_request_delta_by_mode": {"full_refresh": 44},
        }
        self.config.service_state_path.write_text(json.dumps(state), encoding="utf-8")

        with patch("mal_updater.service_runtime._budget_gate", side_effect=[(True, None, {"provider": "crunchyroll"}), (True, None, {"provider": "mal"}), (True, None, None)]), patch(
            "mal_updater.service_runtime._run_subprocess",
            side_effect=[
                {"status": "skipped", "reason": "lease_busy", "label": "sync_fetch_crunchyroll"},
                {"status": "ok", "label": "sync_apply", "returncode": 0, "stdout": "", "stderr": ""},
                {"status": "ok", "label": "health", "returncode": 0, "stdout": "", "stderr": ""},
            ],
        ):
            result = run_pending_tasks(self.config)

        fetch = next(item for item in result["results"] if item["task"] == "sync_fetch_crunchyroll")
        self.assertEqual("skipped", fetch["status"])
        saved = json.loads(self.config.service_state_path.read_text(encoding="utf-8"))["tasks"]["sync_fetch_crunchyroll"]
        self.assertEqual(stale_anchor, saved["full_refresh_anchor_epoch"])
        self.assertEqual({"full_refresh": 44}, saved["last_request_delta_by_mode"])
        self.assertNotIn("last_successful_full_refresh_epoch", saved)
        self.assertNotIn("last_health_request_handled_mtime", saved)
        self.assertLessEqual(saved["next_due_epoch"], time.time() + 61)
        self.assertGreater(saved["next_due_epoch"], time.time())
        self.assertGreater(health_mtime, 0)

    def test_run_pending_tasks_does_not_repeat_health_recommended_full_refresh_after_newer_success(self) -> None:
        self.config.health_latest_json_path.parent.mkdir(parents=True, exist_ok=True)
        self.config.health_latest_json_path.write_text(
            json.dumps(
                {
                    "maintenance": {
                        "recommended_commands": [
                            {
                                "reason_code": "refresh_full_snapshot",
                                "command_args": [
                                    "crunchyroll-fetch-snapshot",
                                    "--full-refresh",
                                    "--out",
                                    ".MAL-Updater/cache/live-crunchyroll-snapshot.json",
                                    "--ingest",
                                ],
                            }
                        ]
                    }
                },
                indent=2,
            ),
            encoding="utf-8",
        )
        stale_health_mtime = time.time() - 600
        os.utime(self.config.health_latest_json_path, (stale_health_mtime, stale_health_mtime))
        self.config.service_state_path.write_text(
            json.dumps(
                {
                    "started_at": "2026-03-20T20:00:00Z",
                    "tasks": {
                        "mal_refresh": {"last_run_epoch": time.time(), "last_run_at": "2026-03-20T20:00:00Z"},
                        "health": {"last_run_epoch": time.time(), "last_run_at": "2026-03-20T20:00:00Z"},
                        "sync_fetch_crunchyroll": {
                            "last_successful_full_refresh_epoch": time.time(),
                            "full_refresh_anchor_epoch": time.time(),
                            "full_refresh_anchor_at": "2026-03-20T20:00:00Z",
                            "last_run_epoch": 0,
                        },
                    },
                },
                indent=2,
            ),
            encoding="utf-8",
        )

        with patch("mal_updater.service_runtime._budget_gate", side_effect=[(True, None, {"provider": "crunchyroll"}), (True, None, {"provider": "mal"}), (True, None, None)]), patch(
            "mal_updater.service_runtime._run_subprocess",
            side_effect=[
                {"status": "ok", "label": "sync_fetch_crunchyroll", "returncode": 0, "stdout": "", "stderr": ""},
                {"status": "ok", "label": "sync_apply", "returncode": 0, "stdout": "", "stderr": ""},
                {"status": "ok", "label": "health", "returncode": 0, "stdout": "", "stderr": ""},
            ],
        ) as run_subprocess:
            result = run_pending_tasks(self.config)

        sync_result = next(item for item in result["results"] if item["task"] == "sync_fetch_crunchyroll")
        self.assertEqual("hot", sync_result["fetch_mode"])
        self.assertNotIn("full_refresh_reason", sync_result)
        sync_args = run_subprocess.call_args_list[0].args[1]
        self.assertNotIn("--full-refresh", sync_args)

    def test_run_pending_tasks_honors_health_requested_incremental_fetch_even_before_cadence_due(self) -> None:
        now = time.time()
        self.config.service.sync_every_seconds = 3600
        self.config.service.health_every_seconds = 3600
        self.config.service.mal_refresh_every_seconds = 3600
        self.config.health_latest_json_path.parent.mkdir(parents=True, exist_ok=True)
        self.config.health_latest_json_path.write_text(
            json.dumps(
                {
                    "healthy": False,
                    "maintenance": {
                        "recommended_commands": [
                            {
                                "reason_code": "refresh_ingested_snapshot",
                                "command_args": ["sync-source", "crunchyroll"],
                            }
                        ]
                    },
                },
                indent=2,
            ),
            encoding="utf-8",
        )
        self.config.service_state_path.write_text(
            json.dumps(
                {
                    "started_at": "2026-03-20T20:00:00Z",
                    "tasks": {
                        "mal_refresh": {"last_run_epoch": now, "last_run_at": "2026-03-20T20:00:00Z"},
                        "sync_fetch_crunchyroll": {"last_run_epoch": now, "last_run_at": "2026-03-20T20:00:00Z"},
                        "sync_apply": {"last_run_epoch": now, "last_run_at": "2026-03-20T20:00:00Z"},
                        "health": {"last_run_epoch": now, "last_run_at": "2026-03-20T20:00:00Z"},
                    },
                },
                indent=2,
            ),
            encoding="utf-8",
        )

        with patch("mal_updater.service_runtime._budget_gate", side_effect=[(True, None, {"provider": "crunchyroll"}), (True, None, {"provider": "mal"}), (True, None, None)]), patch(
            "mal_updater.service_runtime._run_subprocess",
            side_effect=[
                {"status": "ok", "label": "sync_fetch_crunchyroll", "returncode": 0, "stdout": "", "stderr": ""},
                {"status": "ok", "label": "sync_apply", "returncode": 0, "stdout": "", "stderr": ""},
                {"status": "ok", "label": "health", "returncode": 0, "stdout": "", "stderr": ""},
            ],
        ) as run_subprocess:
            result = run_pending_tasks(self.config)

        sync_result = next(item for item in result["results"] if item["task"] == "sync_fetch_crunchyroll")
        self.assertEqual("hot", sync_result["fetch_mode"])
        self.assertEqual("refresh_ingested_snapshot", sync_result["health_request_reason_code"])
        self.assertEqual("health_recommended_hot", sync_result["full_refresh_reason"])
        sync_args = run_subprocess.call_args_list[0].args[1]
        self.assertNotIn("--full-refresh", sync_args)

        saved = json.loads(self.config.service_state_path.read_text(encoding="utf-8"))
        sync_state = saved["tasks"]["sync_fetch_crunchyroll"]
        self.assertEqual("refresh_ingested_snapshot", sync_state["last_health_request_reason_code"])
        self.assertIn("last_health_request_handled_mtime", sync_state)

    def test_run_pending_tasks_does_not_mark_failed_full_refresh_as_successful(self) -> None:
        stale_anchor = datetime.now(timezone.utc).timestamp() - 90000
        previous_success = stale_anchor - 120
        state = {
            "started_at": "2026-03-20T20:00:00Z",
            "tasks": {
                "mal_refresh": {"last_run_epoch": time.time(), "last_run_at": "2026-03-20T20:00:00Z"},
                "health": {"last_run_epoch": time.time(), "last_run_at": "2026-03-20T20:00:00Z"},
                "sync_fetch_crunchyroll": {
                    "last_fetch_mode": "hot",
                    "last_fetch_mode_at": "2026-03-20T18:00:00Z",
                    "last_successful_full_refresh_epoch": previous_success,
                    "last_successful_full_refresh_at": "2026-03-20T17:58:00Z",
                    "full_refresh_anchor_epoch": stale_anchor,
                    "full_refresh_anchor_at": "2026-03-20T18:00:00Z",
                    "last_run_epoch": 0,
                },
            },
        }
        self.config.service_state_path.write_text(json.dumps(state, indent=2), encoding="utf-8")

        with patch("mal_updater.service_runtime._budget_gate", side_effect=[(True, None, {"provider": "crunchyroll"}), (True, None, {"provider": "mal"}), (True, None, None)]), patch(
            "mal_updater.service_runtime._run_subprocess",
            side_effect=[
                {"status": "error", "label": "sync_fetch_crunchyroll", "returncode": 1, "stdout": "", "stderr": "HTTP 401 from Crunchyroll\n"},
                {"status": "ok", "label": "sync_apply", "returncode": 0, "stdout": "", "stderr": ""},
                {"status": "ok", "label": "health", "returncode": 0, "stdout": "", "stderr": ""},
            ],
        ) as run_subprocess:
            result = run_pending_tasks(self.config)

        sync_result = next(item for item in result["results"] if item["task"] == "sync_fetch_crunchyroll")
        self.assertEqual("error", sync_result["status"])
        sync_args = run_subprocess.call_args_list[0].args[1]
        self.assertIn("--full-refresh", sync_args)

        saved = json.loads(self.config.service_state_path.read_text(encoding="utf-8"))
        sync_state = saved["tasks"]["sync_fetch_crunchyroll"]
        self.assertEqual("hot", sync_state["last_fetch_mode"])
        self.assertEqual(previous_success, sync_state["last_successful_full_refresh_epoch"])
        self.assertEqual(stale_anchor, sync_state["full_refresh_anchor_epoch"])
        self.assertEqual("auth", sync_state["failure_backoff_class"])


class ServiceRuntimeHealthCommandTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)
        self.project_root = Path(self.temp_dir.name)
        (self.project_root / ".MAL-Updater" / "config").mkdir(parents=True)
        self.config = load_config(self.project_root)
        ensure_directories(self.config)
        self.config.service.sync_every_seconds = 3600
        self.config.service.health_every_seconds = 0
        self.config.service.mal_refresh_every_seconds = 3600

    def test_run_pending_tasks_uses_repo_native_health_check_cycle_command(self) -> None:
        with patch(
            "mal_updater.service_runtime._run_subprocess",
            return_value={"status": "ok", "label": "health", "returncode": 0, "stdout": "", "stderr": ""},
        ) as run_subprocess:
            result = run_pending_tasks(self.config)

        self.assertEqual(
            [
                sys.executable,
                "-m",
                "mal_updater.cli",
                "--project-root",
                str(self.project_root),
                "health-check-cycle",
            ],
            run_subprocess.call_args.args[1],
        )
        health_result = next(item for item in result["results"] if item["task"] == "health")
        self.assertEqual("ok", health_result["status"])


class ServiceRuntimeApplyBatchingTests(unittest.TestCase):
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

    def test_apply_sync_command_uses_bounded_service_limit(self) -> None:
        self.config.service.task_execute_limits["sync_apply"] = 6
        self.assertEqual(
            [
                sys.executable,
                "-m",
                "mal_updater.cli",
                "apply-sync",
                "--limit",
                "6",
                "--exact-approved-only",
                "--execute",
            ],
            _apply_sync_command(self.config),
        )

    def test_successful_not_due_sync_apply_preserves_status_and_cadence(self) -> None:
        self.config.service.sync_every_seconds = 3600
        self.config.service.task_execute_limits["sync_apply"] = 2
        last_run = time.time() - 60
        self.config.service_state_path.write_text(
            json.dumps({"started_at": "2026-03-20T20:00:00Z", "tasks": {
                "sync_apply": {
                    "last_run_epoch": last_run,
                    "last_run_at": "2026-03-20T20:00:00Z",
                    "last_status": "ok",
                },
            }}), encoding="utf-8",
        )

        with patch(
            "mal_updater.service_runtime._task_specs",
            return_value=[TaskSpec("sync_apply", 3600, "mal")],
        ), patch("mal_updater.service_runtime._run_subprocess") as run_subprocess:
            result = run_pending_tasks(self.config)

        self.assertFalse(any(item["task"] == "sync_apply" for item in result["results"]))
        run_subprocess.assert_not_called()
        apply_state = json.loads(self.config.service_state_path.read_text(encoding="utf-8"))["tasks"]["sync_apply"]
        self.assertEqual("ok", apply_state["last_status"])
        self.assertEqual(last_run, apply_state["last_run_epoch"])
        self.assertEqual(last_run + 3600, apply_state["next_due_epoch"])
        self.assertNotIn("last_skip_reason", apply_state)

    def test_due_sync_apply_is_skipped_when_required_provider_fetch_fails(self) -> None:
        self.config.service.sync_every_seconds = 0
        self.config.service.health_every_seconds = 3600
        self.config.service.mal_refresh_every_seconds = 3600
        self.config.service.recommendation_metadata_refresh_every_seconds = 0
        self.config.service.recommendation_full_harvest_every_seconds = 0
        self.config.service.recommend_maintain_every_seconds = 0
        self.config.service.provider_eligibility_refresh_every_seconds = 0
        self.config.service.task_execute_limits["sync_apply"] = 2
        now = time.time()
        self.config.service_state_path.write_text(
            json.dumps({"started_at": "2026-03-20T20:00:00Z", "tasks": {
                "mal_refresh": {"last_run_epoch": now},
                "health": {"last_run_epoch": now},
                "sync_fetch_crunchyroll": {"last_run_epoch": 0},
                "sync_apply": {"last_run_epoch": 0},
            }}), encoding="utf-8",
        )
        with patch(
            "mal_updater.service_runtime._run_subprocess",
            side_effect=[
                {"status": "error", "returncode": 1, "stdout": "", "stderr": "fetch failed"},
            ],
        ) as run_subprocess:
            result = run_pending_tasks(self.config)
        apply_result = next(item for item in result["results"] if item["task"] == "sync_apply")
        self.assertEqual("skipped", apply_result["status"])
        self.assertEqual("same_cycle_provider_fetch_required", apply_result["reason"])
        self.assertEqual(["crunchyroll"], apply_result["missing_providers"])
        self.assertFalse(any(call.kwargs.get("label") == "sync_apply" for call in run_subprocess.call_args_list))

    def test_due_sync_apply_runs_after_required_provider_fetch_succeeds(self) -> None:
        self.config.service.sync_every_seconds = 60
        self.config.service.task_execute_limits["sync_apply"] = 2
        self.config.service_state_path.write_text(
            json.dumps({"started_at": "2026-03-20T20:00:00Z", "tasks": {
                "sync_fetch_crunchyroll": {"last_run_epoch": 0},
                "sync_apply": {"last_run_epoch": 0},
            }}), encoding="utf-8",
        )

        with patch(
            "mal_updater.service_runtime._task_specs",
            return_value=[
                TaskSpec("sync_fetch_crunchyroll", 60, "crunchyroll"),
                TaskSpec("sync_apply", 60, "mal"),
            ],
        ), patch(
            "mal_updater.service_runtime._budget_gate",
            side_effect=[
                (True, None, {"provider": "crunchyroll"}),
                (True, None, {"provider": "mal"}),
            ],
        ), patch(
            "mal_updater.service_runtime._run_subprocess",
            side_effect=[
                {"status": "ok", "label": "sync_fetch_crunchyroll", "returncode": 0, "stdout": "", "stderr": ""},
                {"status": "ok", "label": "sync_apply", "returncode": 0, "stdout": "", "stderr": ""},
            ],
        ) as run_subprocess:
            result = run_pending_tasks(self.config)

        apply_result = next(item for item in result["results"] if item["task"] == "sync_apply")
        self.assertEqual("ok", apply_result["status"])
        self.assertEqual(2, apply_result["apply_limit"])
        self.assertEqual(
            ["sync_fetch_crunchyroll", "sync_apply"],
            [call.kwargs.get("label") for call in run_subprocess.call_args_list],
        )

    def test_zero_sync_apply_limit_disables_unattended_execution(self) -> None:
        self.config.service.sync_every_seconds = 0
        self.config.service.health_every_seconds = 3600
        self.config.service.mal_refresh_every_seconds = 3600
        self.config.service.task_execute_limits["sync_apply"] = 0
        now = time.time()
        self.config.service_state_path.write_text(
            json.dumps({"started_at": "2026-03-20T20:00:00Z", "tasks": {
                "mal_refresh": {"last_run_epoch": now},
                "health": {"last_run_epoch": now},
                "sync_apply": {"last_run_epoch": 0},
            }}), encoding="utf-8",
        )
        with patch(
            "mal_updater.service_runtime._run_subprocess",
            return_value={"status": "ok", "returncode": 0, "stdout": "", "stderr": ""},
        ) as run_subprocess:
            result = run_pending_tasks(self.config)
        disabled = next(item for item in result["results"] if item["task"] == "sync_apply")
        self.assertEqual({"task": "sync_apply", "status": "skipped", "reason": "execute_limit_zero"}, disabled)
        self.assertFalse(any(call.kwargs.get("label") == "sync_apply" for call in run_subprocess.call_args_list))

    def test_zero_provider_eligibility_candidate_limit_disables_unattended_lane(self) -> None:
        self.config.service.health_every_seconds = 3600
        self.config.service.mal_refresh_every_seconds = 3600
        self.config.service.recommendation_metadata_refresh_every_seconds = 0
        self.config.service.recommendation_full_harvest_every_seconds = 0
        self.config.service.recommend_maintain_every_seconds = 0
        self.config.service.provider_eligibility_refresh_every_seconds = 1
        self.config.service.task_execute_limits["recommend_provider_eligibility_candidates"] = 0
        now = time.time()
        self.config.service_state_path.write_text(
            json.dumps(
                {
                    "started_at": "2026-03-20T20:00:00Z",
                    "tasks": {
                        "mal_refresh": {"last_run_epoch": now},
                        "sync_fetch_crunchyroll": {"last_run_epoch": now},
                        "sync_apply": {"last_run_epoch": now},
                        "recommend_provider_eligibility_crunchyroll": {"last_run_epoch": 0},
                        "health": {"last_run_epoch": now},
                    },
                }
            ),
            encoding="utf-8",
        )
        with patch(
            "mal_updater.service_runtime._run_subprocess",
            return_value={"status": "ok", "returncode": 0, "stdout": "", "stderr": ""},
        ) as run_subprocess:
            result = run_pending_tasks(self.config)
        disabled = next(item for item in result["results"] if item["task"] == "recommend_provider_eligibility_crunchyroll")
        self.assertEqual(
            {"task": "recommend_provider_eligibility_crunchyroll", "status": "skipped", "reason": "execute_limit_zero"},
            disabled,
        )
        self.assertFalse(any(call.kwargs.get("label") == "recommend_provider_eligibility_crunchyroll" for call in run_subprocess.call_args_list))

    def test_recommendation_metadata_refresh_command_uses_bounded_service_limits(self) -> None:
        self.config.service.task_execute_limits["recommend_metadata_refresh"] = 4
        self.config.service.task_execute_limits["recommend_metadata_discovery_targets"] = 7
        self.assertEqual(
            [
                sys.executable,
                "-m",
                "mal_updater.cli",
                "recommend-refresh-metadata",
                "--limit",
                "4",
                "--include-discovery-targets",
                "--discovery-target-limit",
                "7",
            ],
            _recommendation_metadata_refresh_command(self.config),
        )

    def test_mal_list_refresh_default_command_and_signature_use_ten_attempts(self) -> None:
        self.assertEqual(
            [sys.executable, "-m", "mal_updater.cli", "mal-list-refresh", "--max-pages", "10"],
            _mal_list_refresh_command(self.config),
        )
        self.assertEqual(
            "mal_list_refresh:max_pages=10",
            _task_execution_signature(
                self.config,
                TaskSpec("mal_list_refresh", self.config.service.mal_list_refresh_every_seconds, budget_provider="mal"),
            ),
        )

    def test_recommendation_full_harvest_default_cadence_command_and_policy_are_bounded(self) -> None:
        self.assertEqual(3600, self.config.service.recommendation_full_harvest_every_seconds)

        self.assertEqual(
            [
                sys.executable,
                "-m",
                "mal_updater.cli",
                "recommend-refresh-full-userrecs",
                "--limit",
                "3",
                "--stale-after-days",
                "120",
                "--max-pages",
                "10",
            ],
            _recommendation_full_harvest_command(self.config),
        )
        policy = effective_niceness_policy(self.config)
        self.assertEqual(3600, policy["cadences"]["recommendation_full_harvest_seconds"])
        self.assertEqual(120, policy["cache_horizons_days"]["recommendation_full_userrecs_harvest"])
        self.assertEqual(3, policy["execute_limits"]["recommend_full_harvest"])
        self.assertEqual(10, policy["execute_limits"]["recommend_full_harvest_pages"])
        self.assertIn("recommend_full_harvest", policy["task_policies"])
        self.assertEqual(
            "recommend_full_harvest:limit=3:stale_after_days=120:max_pages=10",
            _task_execution_signature(
                self.config,
                TaskSpec(
                    "recommend_full_harvest",
                    self.config.service.recommendation_full_harvest_every_seconds,
                    budget_provider="mal",
                ),
            ),
        )

    def test_recommendation_full_harvest_command_honors_explicit_bounded_overrides(self) -> None:
        self.config.service.task_execute_limits["recommend_full_harvest"] = 3
        self.config.service.task_execute_limits["recommend_full_harvest_pages"] = 4
        self.config.service.recommendation_full_harvest_stale_after_days = 60

        self.assertEqual(
            [
                sys.executable,
                "-m",
                "mal_updater.cli",
                "recommend-refresh-full-userrecs",
                "--limit",
                "3",
                "--stale-after-days",
                "60",
                "--max-pages",
                "4",
            ],
            _recommendation_full_harvest_command(self.config),
        )

    def test_run_pending_tasks_executes_recommendation_full_harvest_slow_lane(self) -> None:
        now = time.time()
        self.config.service.sync_every_seconds = 3600
        self.config.service.health_every_seconds = 3600
        self.config.service.mal_refresh_every_seconds = 3600
        self.config.service.recommendation_metadata_refresh_every_seconds = 0
        self.config.service.recommendation_full_harvest_every_seconds = 1
        self.config.service.task_execute_limits["recommend_full_harvest"] = 3
        self.config.service.task_execute_limits["recommend_full_harvest_pages"] = 4
        self.config.service.recommendation_full_harvest_stale_after_days = 60
        self.config.service_state_path.write_text(
            json.dumps(
                {
                    "started_at": "2026-03-20T20:00:00Z",
                    "tasks": {
                        "mal_refresh": {"last_run_epoch": now, "last_run_at": "2026-03-20T20:00:00Z"},
                        "sync_apply": {"last_run_epoch": now, "last_run_at": "2026-03-20T20:00:00Z"},
                        "recommend_full_harvest": {"last_run_epoch": 0, "last_run_at": "2026-03-19T20:00:00Z"},
                        "health": {"last_run_epoch": now, "last_run_at": "2026-03-20T20:00:00Z"},
                    },
                },
                indent=2,
            ),
            encoding="utf-8",
        )

        with patch("mal_updater.service_runtime._budget_gate", return_value=(True, None, {"provider": "mal", "request_count": 0})), patch(
            "mal_updater.service_runtime._run_subprocess",
            return_value={
                "status": "ok",
                "label": "recommend_full_harvest",
                "returncode": 0,
                "stdout": json.dumps({"seed_count": 10, "considered": 3, "harvested": 2, "failed": 1, "skipped_fresh": 7, "total_edges": 80}),
                "stderr": "",
            },
        ) as run_subprocess:
            result = run_pending_tasks(self.config)

        harvest_result = next(item for item in result["results"] if item["task"] == "recommend_full_harvest")
        self.assertEqual("ok", harvest_result["status"])
        self.assertEqual(3, harvest_result["refresh_limit"])
        self.assertEqual(4, harvest_result["max_pages"])
        self.assertEqual(10, harvest_result["seed_count"])
        self.assertEqual(2, harvest_result["harvested"])
        self.assertEqual(1, harvest_result["failed"])
        self.assertEqual(_recommendation_full_harvest_command(self.config), run_subprocess.call_args.args[1])

        state = json.loads(self.config.service_state_path.read_text(encoding="utf-8"))
        harvest_state = state["tasks"]["recommend_full_harvest"]
        self.assertEqual("recommend_full_harvest:limit=3:stale_after_days=60:max_pages=4", harvest_state["execution_signature"])

    def test_run_pending_tasks_executes_recommendation_metadata_refresh_slow_lane(self) -> None:
        now = time.time()
        self.config.service.sync_every_seconds = 3600
        self.config.service.health_every_seconds = 3600
        self.config.service.mal_refresh_every_seconds = 3600
        self.config.service.recommendation_metadata_refresh_every_seconds = 1
        self.config.service.task_execute_limits["recommend_metadata_refresh"] = 4
        self.config.service.task_execute_limits["recommend_metadata_discovery_targets"] = 7
        self.config.service_state_path.write_text(
            json.dumps(
                {
                    "started_at": "2026-03-20T20:00:00Z",
                    "tasks": {
                        "mal_refresh": {"last_run_epoch": now, "last_run_at": "2026-03-20T20:00:00Z"},
                        "sync_fetch_crunchyroll": {"last_run_epoch": now, "last_run_at": "2026-03-20T20:00:00Z"},
                        "sync_apply": {"last_run_epoch": now, "last_run_at": "2026-03-20T20:00:00Z"},
                        "health": {"last_run_epoch": now, "last_run_at": "2026-03-20T20:00:00Z"},
                    },
                },
                indent=2,
            ),
            encoding="utf-8",
        )

        with patch("mal_updater.service_runtime._budget_gate", return_value=(True, None, {"provider": "mal", "request_count": 0})), patch(
            "mal_updater.service_runtime._run_subprocess",
            return_value={
                "status": "ok",
                "label": "recommend_metadata_refresh",
                "returncode": 0,
                "stdout": json.dumps({"considered": 4, "refreshed": 4, "discovery_considered": 7, "discovery_refreshed": 5}),
                "stderr": "",
            },
        ) as run_subprocess:
            result = run_pending_tasks(self.config)

        metadata_result = next(item for item in result["results"] if item["task"] == "recommend_metadata_refresh")
        self.assertEqual("ok", metadata_result["status"])
        self.assertEqual(4, metadata_result["refresh_limit"])
        self.assertEqual(7, metadata_result["discovery_target_limit"])
        self.assertEqual(4, metadata_result["considered"])
        self.assertEqual(4, metadata_result["refreshed"])
        self.assertEqual(7, metadata_result["discovery_considered"])
        self.assertEqual(5, metadata_result["discovery_refreshed"])
        self.assertEqual(_recommendation_metadata_refresh_command(self.config), run_subprocess.call_args.args[1])

        state = json.loads(self.config.service_state_path.read_text(encoding="utf-8"))
        metadata_state = state["tasks"]["recommend_metadata_refresh"]
        self.assertEqual("recommend_metadata_refresh:limit=4:discovery_target_limit=7", metadata_state["execution_signature"])

    def test_run_pending_tasks_executes_recommendations_webhook_push_lane(self) -> None:
        now = time.time()
        self.config.service.sync_every_seconds = 3600
        self.config.service.health_every_seconds = 3600
        self.config.service.mal_refresh_every_seconds = 3600
        self.config.service.recommendation_metadata_refresh_every_seconds = 0
        self.config.service.recommendations_webhook_push_every_seconds = 1
        self.config.service.task_execute_limits["push_recommendations_webhook"] = 6
        self.config.openclaw.recommendations_webhook_enabled = True
        self.config.service_state_path.write_text(
            json.dumps(
                {
                    "started_at": "2026-03-20T20:00:00Z",
                    "tasks": {
                        "mal_refresh": {"last_run_epoch": now, "last_run_at": "2026-03-20T20:00:00Z"},
                        "sync_fetch_crunchyroll": {"last_run_epoch": now, "last_run_at": "2026-03-20T20:00:00Z"},
                        "sync_apply": {"last_run_epoch": now, "last_run_at": "2026-03-20T20:00:00Z"},
                        "health": {"last_run_epoch": now, "last_run_at": "2026-03-20T20:00:00Z"},
                    },
                },
                indent=2,
            ),
            encoding="utf-8",
        )

        preview = OpenClawRecommendationDeliveryResult(
            status="dry_run",
            request_url="http://127.0.0.1:18789/hooks/agent",
            payload={"structured_payload": {"item_fingerprints": ["fp-1"]}},
            request_id="abc123",
        )
        delivered = OpenClawRecommendationDeliveryResult(
            status="delivered",
            request_url="http://127.0.0.1:18789/hooks/agent",
            payload={"structured_payload": {"item_fingerprints": ["fp-1"]}},
            http_status=200,
            request_id="abc123",
        )
        with patch("mal_updater.service_runtime.deliver_recommendations_via_openclaw", side_effect=[preview, delivered]):
            result = run_pending_tasks(self.config)

        webhook_result = next(item for item in result["results"] if item["task"] == "push_recommendations_webhook")
        self.assertEqual("ok", webhook_result["status"])
        self.assertEqual("delivered", webhook_result["delivery_status"])
        self.assertEqual(6, webhook_result["delivery_limit"])
        self.assertEqual("abc123", webhook_result["request_id"])

        state = json.loads(self.config.service_state_path.read_text(encoding="utf-8"))
        webhook_state = state["tasks"]["push_recommendations_webhook"]
        self.assertEqual("push_recommendations_webhook:limit=6:mode=fresh", webhook_state["execution_signature"])
        self.assertEqual("abc123", webhook_state["last_delivery_request_id"])
        self.assertEqual(["fp-1"], webhook_state["last_delivery_item_fingerprints"])
        self.assertIn("fp-1", webhook_state["delivery_item_fingerprint_history"])
        self.assertIsInstance(webhook_state["last_delivery_epoch"], float)

    def test_run_pending_tasks_skips_unchanged_recommendations_webhook_push_lane(self) -> None:
        now = time.time()
        self.config.service.sync_every_seconds = 3600
        self.config.service.health_every_seconds = 3600
        self.config.service.mal_refresh_every_seconds = 3600
        self.config.service.recommendation_metadata_refresh_every_seconds = 0
        self.config.service.recommendations_webhook_push_every_seconds = 1
        self.config.service.task_execute_limits["push_recommendations_webhook"] = 6
        self.config.openclaw.recommendations_webhook_enabled = True
        self.config.service_state_path.write_text(
            json.dumps(
                {
                    "started_at": "2026-03-20T20:00:00Z",
                    "tasks": {
                        "mal_refresh": {"last_run_epoch": now, "last_run_at": "2026-03-20T20:00:00Z"},
                        "sync_fetch_crunchyroll": {"last_run_epoch": now, "last_run_at": "2026-03-20T20:00:00Z"},
                        "sync_apply": {"last_run_epoch": now, "last_run_at": "2026-03-20T20:00:00Z"},
                        "health": {"last_run_epoch": now, "last_run_at": "2026-03-20T20:00:00Z"},
                        "push_recommendations_webhook": {"last_delivery_request_id": "abc123", "last_delivery_item_fingerprints": ["fp-1"]},
                    },
                },
                indent=2,
            ),
            encoding="utf-8",
        )

        preview = OpenClawRecommendationDeliveryResult(
            status="dry_run",
            request_url="http://127.0.0.1:18789/hooks/agent",
            payload={"structured_payload": {"item_fingerprints": ["fp-1"]}},
            request_id="abc123",
        )
        with patch("mal_updater.service_runtime.deliver_recommendations_via_openclaw", return_value=preview) as deliver:
            result = run_pending_tasks(self.config)

        webhook_result = next(item for item in result["results"] if item["task"] == "push_recommendations_webhook")
        self.assertEqual("ok", webhook_result["status"])
        self.assertEqual("unchanged", webhook_result["delivery_status"])
        self.assertEqual(1, deliver.call_count)

    def test_run_pending_tasks_uses_recent_delivery_history_as_item_cooldown(self) -> None:
        now = time.time()
        self.config.service.sync_every_seconds = 3600
        self.config.service.health_every_seconds = 3600
        self.config.service.mal_refresh_every_seconds = 3600
        self.config.service.recommendation_metadata_refresh_every_seconds = 0
        self.config.service.recommendations_webhook_push_every_seconds = 1
        self.config.service.task_execute_limits["push_recommendations_webhook"] = 6
        self.config.openclaw.recommendations_webhook_enabled = True
        self.config.service_state_path.write_text(
            json.dumps(
                {
                    "started_at": "2026-03-20T20:00:00Z",
                    "tasks": {
                        "mal_refresh": {"last_run_epoch": now, "last_run_at": "2026-03-20T20:00:00Z"},
                        "sync_fetch_crunchyroll": {"last_run_epoch": now, "last_run_at": "2026-03-20T20:00:00Z"},
                        "sync_apply": {"last_run_epoch": now, "last_run_at": "2026-03-20T20:00:00Z"},
                        "health": {"last_run_epoch": now, "last_run_at": "2026-03-20T20:00:00Z"},
                        "push_recommendations_webhook": {
                            "delivery_item_fingerprint_history": {
                                "recent-fp": now - 60,
                                "expired-fp": now - (91 * 24 * 60 * 60),
                            }
                        },
                    },
                },
                indent=2,
            ),
            encoding="utf-8",
        )

        preview = OpenClawRecommendationDeliveryResult(
            status="dry_run",
            request_url="http://127.0.0.1:18789/hooks/agent",
            payload={"structured_payload": {"item_fingerprints": ["new-fp"]}},
            request_id="new-request",
        )
        delivered = OpenClawRecommendationDeliveryResult(
            status="delivered",
            request_url="http://127.0.0.1:18789/hooks/agent",
            payload={"structured_payload": {"item_fingerprints": ["new-fp"]}},
            http_status=200,
            request_id="new-request",
        )
        with patch("mal_updater.service_runtime.deliver_recommendations_via_openclaw", side_effect=[preview, delivered]) as deliver:
            result = run_pending_tasks(self.config)

        webhook_result = next(item for item in result["results"] if item["task"] == "push_recommendations_webhook")
        self.assertEqual("delivered", webhook_result["delivery_status"])
        self.assertEqual(1, webhook_result["suppressed_recent_item_count"])
        for call in deliver.call_args_list:
            self.assertFalse(call.kwargs["include_dormant"])
            self.assertEqual({"recent-fp"}, call.kwargs["suppress_item_fingerprints"])
            self.assertNotIn("max_dormant_discovery_items", call.kwargs)

        state = json.loads(self.config.service_state_path.read_text(encoding="utf-8"))
        history = state["tasks"]["push_recommendations_webhook"]["delivery_item_fingerprint_history"]
        self.assertIn("recent-fp", history)
        self.assertIn("new-fp", history)
        self.assertNotIn("expired-fp", history)

    def test_run_pending_tasks_resets_stale_sync_apply_projection_when_execution_signature_changes(self) -> None:
        now = time.time()
        self.config.service.sync_every_seconds = 0
        self.config.service.health_every_seconds = 3600
        self.config.service.mal_refresh_every_seconds = 3600
        self.config.service.task_execute_limits["sync_apply"] = 6
        self.config.service_state_path.write_text(
            json.dumps(
                {
                    "started_at": "2026-03-20T20:00:00Z",
                    "tasks": {
                        "mal_refresh": {"last_run_epoch": now, "last_run_at": "2026-03-20T20:00:00Z"},
                        "health": {"last_run_epoch": now, "last_run_at": "2026-03-20T20:00:00Z"},
                        "sync_apply": {
                            "last_run_epoch": 0,
                            "execution_signature": "sync_apply:limit=0",
                            "last_request_delta": 250,
                            "last_request_delta_history": [250, 250, 250],
                            "projected_request_count": 250,
                            "projected_request_source": "observed_p90",
                            "budget_backoff_until_epoch": now + 7200,
                            "budget_backoff_until": "2099-01-01T00:00:00Z",
                            "budget_backoff_remaining_seconds": 7200,
                            "last_skip_reason": "mal_budget_projected_critical ratio=0.0 projected_ratio=5.2 projected_requests=250 cooldown=1800s",
                        },
                    },
                },
                indent=2,
            ),
            encoding="utf-8",
        )

        with patch(
            "mal_updater.service_runtime._budget_gate",
            side_effect=[(True, None, {"provider": "crunchyroll"}), (True, None, {"provider": "mal"}), (True, None, None)],
        ), patch(
            "mal_updater.service_runtime._run_subprocess",
            side_effect=[
                {"status": "ok", "label": "sync_fetch_crunchyroll", "returncode": 0, "stdout": "", "stderr": ""},
                {"status": "ok", "label": "sync_apply", "returncode": 0, "stdout": "", "stderr": ""},
                {"status": "ok", "label": "health", "returncode": 0, "stdout": "", "stderr": ""},
            ],
        ):
            result = run_pending_tasks(self.config)

        sync_apply_result = next(item for item in result["results"] if item["task"] == "sync_apply")
        self.assertEqual("ok", sync_apply_result["status"])
        self.assertEqual(6, sync_apply_result["apply_limit"])

        state = json.loads(self.config.service_state_path.read_text(encoding="utf-8"))
        apply_state = state["tasks"]["sync_apply"]
        self.assertEqual("sync_apply:limit=6", apply_state["execution_signature"])
        self.assertNotIn("budget_backoff_until_epoch", apply_state)
        self.assertNotIn("last_skip_reason", apply_state)

    def test_zero_unattended_sync_apply_limit_disables_lane_without_subprocess(self) -> None:
        now = time.time()
        self.config.service.sync_every_seconds = 0
        self.config.service.health_every_seconds = 3600
        self.config.service.mal_refresh_every_seconds = 3600
        self.config.service.task_execute_limits["sync_apply"] = 0
        self.config.service_state_path.write_text(json.dumps({"tasks": {
            "mal_refresh": {"last_run_epoch": now},
            "health": {"last_run_epoch": now},
        }}), encoding="utf-8")

        with patch("mal_updater.service_runtime._task_specs", return_value=[TaskSpec("sync_apply", 60, "mal")]), patch(
            "mal_updater.service_runtime._run_subprocess"
        ) as run_subprocess:
            result = run_pending_tasks(self.config)

        apply_result = next(item for item in result["results"] if item["task"] == "sync_apply")
        self.assertEqual({"task": "sync_apply", "status": "skipped", "reason": "execute_limit_zero"}, apply_result)
        run_subprocess.assert_not_called()
        state = json.loads(self.config.service_state_path.read_text(encoding="utf-8"))["tasks"]["sync_apply"]
        self.assertEqual("disabled", state["last_status"])
        self.assertNotIn("last_run_epoch", state)


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
        self.config.service.recommendation_metadata_refresh_every_seconds = 0

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

    def test_estimate_budget_recovery_seconds_for_warn_ratio(self) -> None:
        self._write_request_events("crunchyroll", [50, 100, 200, 300, 400, 500, 600, 700])
        recovery = estimate_budget_recovery_seconds_for_ratio(provider="crunchyroll", limit=10, target_ratio=0.8, config=self.config)
        self.assertGreaterEqual(recovery, 2850)
        self.assertLessEqual(recovery, 2955)

    def test_task_budget_and_provider_global_budget_are_both_enforced(self) -> None:
        self.config.service.mal_hourly_limit = 5
        self.config.service.task_hourly_limits["sync_apply"] = 10
        self.config.service.task_projected_request_counts["sync_apply"] = 1
        token = begin_api_request_context(task="mal_refresh", run_id="other-run")
        try:
            for index in range(4):
                record_api_request_event("mal", "other", url=f"https://example.invalid/{index}", method="POST", outcome="ok", config=self.config)
        finally:
            end_api_request_context(token)

        allowed, reason, usage = _budget_gate(self.config, TaskSpec("sync_apply", 60, "mal"), {}, fetch_mode=None)
        self.assertFalse(allowed)
        self.assertIn("mal_global_budget_critical", reason or "")
        self.assertEqual(4, usage["global_request_count"])
        self.assertEqual(0, usage["task_request_count"])

        self.config.api_request_events_path.unlink()
        self.config.service.mal_hourly_limit = 100
        self.config.service.task_hourly_limits["sync_apply"] = 2
        self.config.service.task_projected_request_counts["sync_apply"] = 0
        token = begin_api_request_context(task="sync_apply", run_id="task-run")
        try:
            for index in range(2):
                record_api_request_event("mal", "apply", url=f"https://example.invalid/{index}", method="PUT", outcome="ok", config=self.config)
        finally:
            end_api_request_context(token)

        allowed, reason, usage = _budget_gate(self.config, TaskSpec("sync_apply", 60, "mal"), {}, fetch_mode=None)
        self.assertFalse(allowed)
        self.assertIn("mal_budget_critical", reason or "")
        self.assertEqual(2, usage["task_request_count"])
        self.assertEqual(2, usage["global_request_count"])

    def test_recommend_full_harvest_cold_start_projection_fits_default_task_budget(self) -> None:
        spec = TaskSpec("recommend_full_harvest", self.config.service.recommendation_full_harvest_every_seconds, budget_provider="mal")

        allowed, reason, usage = _budget_gate(self.config, spec, {}, fetch_mode=None)

        self.assertTrue(allowed, reason)
        self.assertIsNone(reason)
        self.assertEqual(40, usage["task_limit"])
        self.assertEqual(0.95, usage["critical_ratio"])
        self.assertEqual(30, usage["projected_request_count"])
        self.assertEqual(30, usage["projected_request_total"])
        self.assertAlmostEqual(0.75, usage["projected_ratio"])
        self.assertEqual("configured", usage["projected_request_source"])
        self.assertEqual(0, usage["task_request_count"])
        self.assertEqual(0, usage["global_request_count"])

        token = begin_api_request_context(task="recommend_full_harvest", run_id="harvest-critical")
        try:
            for index in range(4):
                record_api_request_event(
                    "mal",
                    "recommend-full-harvest",
                    url=f"https://example.invalid/userrecs/{index}",
                    method="GET",
                    outcome="ok",
                    status_code=200,
                    config=self.config,
                )
        finally:
            end_api_request_context(token)

        allowed, reason, usage = _budget_gate(self.config, spec, {}, fetch_mode=None)

        self.assertFalse(allowed)
        self.assertIn("mal_budget_projected_warn", reason or "")
        self.assertEqual(40, usage["task_limit"])
        self.assertEqual(4, usage["task_request_count"])
        self.assertEqual(4, usage["global_request_count"])
        self.assertEqual(30, usage["projected_request_count"])
        self.assertEqual(34, usage["projected_request_total"])
        self.assertAlmostEqual(0.85, usage["projected_ratio"])
        self.assertEqual("warn", usage["backoff_level"])

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
        self.assertEqual("provider", sync_state["budget_scope"])
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

    def test_run_pending_tasks_warn_paces_provider_before_critical_budget(self) -> None:
        self._write_request_events("crunchyroll", [50, 100, 200, 300, 400, 500, 600, 700])
        self.config.service.crunchyroll_hourly_limit = 10
        self.config.service.task_projected_request_counts["sync_fetch_crunchyroll"] = 1

        with patch("mal_updater.service_runtime._refresh_mal_tokens", return_value={"status": "ok"}), patch(
            "mal_updater.service_runtime._run_subprocess",
            return_value={"status": "ok", "label": "health", "returncode": 0, "stdout": "", "stderr": ""},
        ):
            result = run_pending_tasks(self.config)

        sync_result = next(item for item in result["results"] if item["task"] == "sync_fetch_crunchyroll")
        self.assertEqual("skipped", sync_result["status"])
        self.assertIn("crunchyroll_budget_warn", sync_result["reason"])
        self.assertEqual("warn", sync_result["budget_backoff_level"])
        self.assertGreater(sync_result["budget_backoff_remaining_seconds"], 0)

        state = json.loads(self.config.service_state_path.read_text(encoding="utf-8"))
        sync_state = state["tasks"]["sync_fetch_crunchyroll"]
        self.assertEqual("warn", sync_state["budget_backoff_level"])
        self.assertIn("budget_backoff_until", sync_state)
        self.assertIn("next_due_at", sync_state)

    def test_run_pending_tasks_uses_provider_warn_backoff_floor_when_larger_than_recovery(self) -> None:
        self._write_request_events("crunchyroll", [2810, 2820, 2830, 2840, 2850, 2860, 2870, 2880])
        self.config.service.crunchyroll_hourly_limit = 10
        self.config.service.provider_warn_backoff_floor_seconds["crunchyroll"] = 900
        self.config.service.task_projected_request_counts["sync_fetch_crunchyroll"] = 1

        with patch("mal_updater.service_runtime._refresh_mal_tokens", return_value={"status": "ok"}), patch(
            "mal_updater.service_runtime._run_subprocess",
            return_value={"status": "ok", "label": "health", "returncode": 0, "stdout": "", "stderr": ""},
        ):
            result = run_pending_tasks(self.config)

        sync_result = next(item for item in result["results"] if item["task"] == "sync_fetch_crunchyroll")
        self.assertEqual("skipped", sync_result["status"])
        self.assertEqual("warn", sync_result["budget_backoff_level"])
        self.assertEqual(900, sync_result["budget_backoff_remaining_seconds"])
        self.assertEqual(900, sync_result["budget_backoff_floor_seconds"])
        self.assertEqual("provider_floor", sync_result["budget_backoff_cooldown_source"])
        self.assertIn("cooldown=900s", sync_result["reason"])

        state = json.loads(self.config.service_state_path.read_text(encoding="utf-8"))
        sync_state = state["tasks"]["sync_fetch_crunchyroll"]
        self.assertEqual(900, sync_state["budget_backoff_floor_seconds"])
        self.assertEqual("provider_floor", sync_state["budget_backoff_cooldown_source"])

    def test_run_pending_tasks_uses_task_specific_budget_limit_and_warn_floor(self) -> None:
        self.config.service.task_execute_limits["sync_apply"] = 8
        self._write_request_events("mal", [2810, 2820, 2830, 2840])
        self.config.service.mal_hourly_limit = 120
        self.config.service.task_hourly_limits["sync_apply"] = 5
        self.config.service.task_warn_backoff_floor_seconds["sync_apply"] = 900
        self.config.service.task_projected_request_counts.pop("sync_apply", None)

        with patch("mal_updater.service_runtime._refresh_mal_tokens", return_value={"status": "ok"}), patch(
            "mal_updater.service_runtime._run_subprocess",
            side_effect=[
                {"status": "ok", "label": "sync_fetch_crunchyroll", "returncode": 0, "stdout": "", "stderr": ""},
                {"status": "ok", "label": "health", "returncode": 0, "stdout": "", "stderr": ""},
            ],
        ):
            result = run_pending_tasks(self.config)

        sync_apply_result = next(item for item in result["results"] if item["task"] == "sync_apply")
        self.assertEqual("skipped", sync_apply_result["status"])
        self.assertEqual("warn", sync_apply_result["budget_backoff_level"])
        self.assertEqual("task", sync_apply_result["budget_scope"])
        self.assertEqual(900, sync_apply_result["budget_backoff_remaining_seconds"])
        self.assertEqual(900, sync_apply_result["budget_backoff_floor_seconds"])
        self.assertEqual("task_floor", sync_apply_result["budget_backoff_cooldown_source"])

        state = json.loads(self.config.service_state_path.read_text(encoding="utf-8"))
        apply_state = state["tasks"]["sync_apply"]
        self.assertEqual("mal", apply_state["budget_provider"])
        self.assertEqual("task", apply_state["budget_scope"])
        self.assertEqual(900, apply_state["budget_backoff_floor_seconds"])
        self.assertEqual("task_floor", apply_state["budget_backoff_cooldown_source"])

    def test_run_pending_tasks_projects_warn_budget_from_configured_request_cost(self) -> None:
        self._write_request_events("crunchyroll", [50, 100, 200, 300, 400, 500])
        self.config.service.crunchyroll_hourly_limit = 10
        self.config.service.task_projected_request_counts["sync_fetch_crunchyroll"] = 2

        with patch("mal_updater.service_runtime._refresh_mal_tokens", return_value={"status": "ok"}), patch(
            "mal_updater.service_runtime._run_subprocess",
            return_value={"status": "ok", "label": "health", "returncode": 0, "stdout": "", "stderr": ""},
        ):
            result = run_pending_tasks(self.config)

        sync_result = next(item for item in result["results"] if item["task"] == "sync_fetch_crunchyroll")
        self.assertEqual("skipped", sync_result["status"])
        self.assertIn("crunchyroll_budget_projected_warn", sync_result["reason"])
        self.assertIn("projected_requests=2", sync_result["reason"])
        self.assertEqual("configured", sync_result["api_usage"]["projected_request_source"])
        self.assertEqual(2, sync_result["api_usage"]["projected_request_count"])
        self.assertEqual(8, sync_result["api_usage"]["projected_request_total"])
        self.assertAlmostEqual(0.8, sync_result["api_usage"]["projected_ratio"])

        state = json.loads(self.config.service_state_path.read_text(encoding="utf-8"))
        sync_state = state["tasks"]["sync_fetch_crunchyroll"]
        self.assertEqual(2, sync_state["projected_request_count"])
        self.assertEqual(8, sync_state["projected_request_total"])
        self.assertAlmostEqual(0.8, sync_state["projected_ratio"])
        self.assertEqual("configured", sync_state["projected_request_source"])

    def test_hidive_periodic_full_refresh_stays_manual_and_uses_hot_projection(self) -> None:
        self._write_request_events("hidive", [50, 100])
        (self.project_root / ".MAL-Updater" / "secrets" / "hidive_username.txt").write_text("user@example.com\n", encoding="utf-8")
        (self.project_root / ".MAL-Updater" / "secrets" / "hidive_password.txt").write_text("secret\n", encoding="utf-8")
        self.config.service.provider_hourly_limits["hidive"] = 72
        self.config.service.full_refresh_every_seconds = 86400
        self.config.service.task_projected_request_counts["sync_fetch_hidive"] = 14
        self.config.service.task_projected_request_counts_by_mode["sync_fetch_hidive"] = {"full_refresh": 70, "hot": 5}
        stale_anchor = datetime.now(timezone.utc).timestamp() - 90000
        self.config.service_state_path.write_text(
            json.dumps(
                {
                    "started_at": "2026-03-20T20:00:00Z",
                    "tasks": {
                        "mal_refresh": {"last_run_epoch": time.time(), "last_run_at": "2026-03-20T20:00:00Z"},
                        "health": {"last_run_epoch": time.time(), "last_run_at": "2026-03-20T20:00:00Z"},
                        "sync_fetch_hidive": {
                            "full_refresh_anchor_epoch": stale_anchor,
                            "full_refresh_anchor_at": "2026-03-20T20:00:00Z",
                            "last_run_epoch": 0,
                        },
                    },
                },
                indent=2,
            ),
            encoding="utf-8",
        )

        def fake_run_subprocess(config, args, *, label):
            if label == "sync_fetch_hidive":
                for index in range(5):
                    record_api_request_event(
                        "hidive",
                        "sync-fetch",
                        url=f"https://example.invalid/api/{index}",
                        method="GET",
                        outcome="ok",
                        status_code=200,
                        config=config,
                    )
            return {"status": "ok", "label": label, "returncode": 0, "stdout": "", "stderr": ""}

        with patch("mal_updater.service_runtime._refresh_mal_tokens", return_value={"status": "ok"}), patch(
            "mal_updater.service_runtime._run_subprocess",
            side_effect=fake_run_subprocess,
        ):
            result = run_pending_tasks(self.config)

        sync_result = next(item for item in result["results"] if item["task"] == "sync_fetch_hidive")
        self.assertEqual("ok", sync_result["status"])
        self.assertEqual("hot", sync_result["fetch_mode"])
        self.assertNotIn("deferred_full_refresh_reason", sync_result)
        self.assertEqual(5, sync_result["next_projected_request_count"])
        self.assertEqual("configured_hot", sync_result["next_projected_request_source"])

        state = json.loads(self.config.service_state_path.read_text(encoding="utf-8"))
        sync_state = state["tasks"]["sync_fetch_hidive"]
        self.assertEqual("hot", sync_state["last_fetch_mode"])
        self.assertEqual(stale_anchor, sync_state["full_refresh_anchor_epoch"])
        self.assertEqual(5, sync_state["projected_request_count"])
        self.assertEqual("configured_hot", sync_state["projected_request_source"])


    def test_run_pending_tasks_learns_observed_request_delta_for_future_budgeting(self) -> None:
        self.config.service.sync_every_seconds = 0
        self.config.service.health_every_seconds = 3600
        self.config.service.mal_refresh_every_seconds = 3600
        self.config.service.provider_projected_request_percentiles.pop("crunchyroll", None)

        def fake_run_subprocess(config, args, *, label):
            if label == "sync_fetch_crunchyroll":
                record_api_request_event(
                    "crunchyroll",
                    "sync-fetch",
                    url="https://example.invalid/api/1",
                    method="GET",
                    outcome="ok",
                    status_code=200,
                    config=config,
                )
                record_api_request_event(
                    "crunchyroll",
                    "sync-fetch",
                    url="https://example.invalid/api/2",
                    method="GET",
                    outcome="ok",
                    status_code=200,
                    config=config,
                )
            return {"status": "ok", "label": label, "returncode": 0, "stdout": "", "stderr": ""}

        with patch("mal_updater.service_runtime._refresh_mal_tokens", return_value={"status": "ok"}), patch(
            "mal_updater.service_runtime._run_subprocess",
            side_effect=fake_run_subprocess,
        ):
            run_pending_tasks(self.config)
            run_pending_tasks(self.config)

        saved = json.loads(self.config.service_state_path.read_text(encoding="utf-8"))
        sync_state = saved["tasks"]["sync_fetch_crunchyroll"]
        self.assertEqual(2, sync_state["last_request_delta"])
        self.assertEqual({"hot": 2}, sync_state["last_request_delta_by_mode"])
        self.assertEqual(2, sync_state["projected_request_count"])
        self.assertEqual("observed_hot_smoothed", sync_state["projected_request_source"])

    def test_run_pending_tasks_smooths_observed_request_delta_history_for_budgeting(self) -> None:
        self.config.service.sync_every_seconds = 0
        self.config.service.health_every_seconds = 3600
        self.config.service.mal_refresh_every_seconds = 3600
        self.config.service.provider_projected_request_percentiles.pop("crunchyroll", None)
        planned_deltas = iter([2, 8, 2])

        def fake_run_subprocess(config, args, *, label):
            if label == "sync_fetch_crunchyroll":
                for index in range(next(planned_deltas)):
                    record_api_request_event(
                        "crunchyroll",
                        "sync-fetch",
                        url=f"https://example.invalid/api/{index}",
                        method="GET",
                        outcome="ok",
                        status_code=200,
                        config=config,
                    )
            return {"status": "ok", "label": label, "returncode": 0, "stdout": "", "stderr": ""}

        with patch("mal_updater.service_runtime._refresh_mal_tokens", return_value={"status": "ok"}), patch(
            "mal_updater.service_runtime._run_subprocess",
            side_effect=fake_run_subprocess,
        ):
            run_pending_tasks(self.config)
            run_pending_tasks(self.config)
            run_pending_tasks(self.config)

        saved = json.loads(self.config.service_state_path.read_text(encoding="utf-8"))
        sync_state = saved["tasks"]["sync_fetch_crunchyroll"]
        self.assertEqual([2, 8, 2], sync_state["last_request_delta_history"])
        self.assertEqual({"hot": [2, 8, 2]}, sync_state["last_request_delta_history_by_mode"])
        self.assertEqual(4, sync_state["projected_request_count"])
        self.assertEqual("observed_hot_smoothed", sync_state["projected_request_source"])

    def test_run_pending_tasks_uses_task_percentile_projection_and_history_window_for_budgeting(self) -> None:
        self.config.service.sync_every_seconds = 0
        self.config.service.health_every_seconds = 3600
        self.config.service.mal_refresh_every_seconds = 3600
        self.config.service.task_projected_request_history_windows["sync_fetch_crunchyroll"] = 3
        self.config.service.task_projected_request_percentiles["sync_fetch_crunchyroll"] = 0.75
        planned_deltas = iter([2, 8, 2, 20])

        def fake_run_subprocess(config, args, *, label):
            if label == "sync_fetch_crunchyroll":
                for index in range(next(planned_deltas)):
                    record_api_request_event(
                        "crunchyroll",
                        "sync-fetch",
                        url=f"https://example.invalid/api/{index}",
                        method="GET",
                        outcome="ok",
                        status_code=200,
                        config=config,
                    )
            return {"status": "ok", "label": label, "returncode": 0, "stdout": "", "stderr": ""}

        with patch("mal_updater.service_runtime._refresh_mal_tokens", return_value={"status": "ok"}), patch(
            "mal_updater.service_runtime._run_subprocess",
            side_effect=fake_run_subprocess,
        ):
            run_pending_tasks(self.config)
            run_pending_tasks(self.config)
            run_pending_tasks(self.config)
            run_pending_tasks(self.config)

        saved = json.loads(self.config.service_state_path.read_text(encoding="utf-8"))
        sync_state = saved["tasks"]["sync_fetch_crunchyroll"]
        self.assertEqual([8, 2, 20], sync_state["last_request_delta_history"])
        self.assertEqual({"hot": [8, 2, 20]}, sync_state["last_request_delta_history_by_mode"])
        projected_count, projected_source = _projected_request_count(
            self.config,
            TaskSpec("sync_fetch_crunchyroll", self.config.service.sync_every_seconds, budget_provider="crunchyroll"),
            sync_state,
            fetch_mode="hot",
        )
        self.assertEqual(20, projected_count)
        self.assertEqual("observed_hot_p75", projected_source)

    def test_run_pending_tasks_uses_provider_projection_defaults_when_task_override_absent(self) -> None:
        self.config.service.sync_every_seconds = 0
        self.config.service.health_every_seconds = 3600
        self.config.service.mal_refresh_every_seconds = 3600
        self.config.service.provider_projected_request_history_windows["crunchyroll"] = 3
        self.config.service.provider_projected_request_percentiles["crunchyroll"] = 0.75
        planned_deltas = iter([2, 8, 2, 20])

        def fake_run_subprocess(config, args, *, label):
            if label == "sync_fetch_crunchyroll":
                for index in range(next(planned_deltas)):
                    record_api_request_event(
                        "crunchyroll",
                        "sync-fetch",
                        url=f"https://example.invalid/api/{index}",
                        method="GET",
                        outcome="ok",
                        status_code=200,
                        config=config,
                    )
            return {"status": "ok", "label": label, "returncode": 0, "stdout": "", "stderr": ""}

        with patch("mal_updater.service_runtime._refresh_mal_tokens", return_value={"status": "ok"}), patch(
            "mal_updater.service_runtime._run_subprocess",
            side_effect=fake_run_subprocess,
        ):
            run_pending_tasks(self.config)
            run_pending_tasks(self.config)
            run_pending_tasks(self.config)
            run_pending_tasks(self.config)

        saved = json.loads(self.config.service_state_path.read_text(encoding="utf-8"))
        sync_state = saved["tasks"]["sync_fetch_crunchyroll"]
        self.assertEqual([8, 2, 20], sync_state["last_request_delta_history"])
        self.assertEqual({"hot": [8, 2, 20]}, sync_state["last_request_delta_history_by_mode"])

        projected_count, projected_source = _projected_request_count(
            self.config,
            TaskSpec("sync_fetch_crunchyroll", self.config.service.sync_every_seconds, budget_provider="crunchyroll"),
            sync_state,
            fetch_mode="hot",
        )
        self.assertEqual(20, projected_count)
        self.assertEqual("observed_hot_p75", projected_source)

    def test_projected_request_count_uses_built_in_mal_refresh_default(self) -> None:
        projected_count, projected_source = _projected_request_count(
            self.config,
            TaskSpec("mal_refresh", self.config.service.mal_refresh_every_seconds, budget_provider="mal"),
            {},
        )
        self.assertEqual(1, projected_count)
        self.assertEqual("configured", projected_source)

    def test_projected_request_count_uses_built_in_sync_apply_percentile(self) -> None:
        projected_count, projected_source = _projected_request_count(
            self.config,
            TaskSpec("sync_apply", self.config.service.sync_every_seconds, budget_provider="mal"),
            {"last_request_delta_history": [4, 10, 4]},
        )
        self.assertEqual(10, projected_count)
        self.assertEqual("observed_p90", projected_source)

    def test_projected_request_count_allows_task_percentile_override_for_sync_apply(self) -> None:
        self.config.service.task_projected_request_percentiles["sync_apply"] = 0.75
        projected_count, projected_source = _projected_request_count(
            self.config,
            TaskSpec("sync_apply", self.config.service.sync_every_seconds, budget_provider="mal"),
            {"last_request_delta_history": [4, 10, 4]},
        )
        self.assertEqual(10, projected_count)
        self.assertEqual("observed_p75", projected_source)

    def test_projected_request_count_uses_built_in_crunchyroll_incremental_default(self) -> None:
        projected_count, projected_source = _projected_request_count(
            self.config,
            TaskSpec("sync_fetch_crunchyroll", self.config.service.sync_every_seconds, budget_provider="crunchyroll"),
            {},
            fetch_mode="hot",
        )
        self.assertEqual(4, projected_count)
        self.assertEqual("configured_hot", projected_source)

    def test_projected_request_count_uses_built_in_crunchyroll_full_refresh_default(self) -> None:
        projected_count, projected_source = _projected_request_count(
            self.config,
            TaskSpec("sync_fetch_crunchyroll", self.config.service.sync_every_seconds, budget_provider="crunchyroll"),
            {},
            fetch_mode="full_refresh",
        )
        self.assertEqual(55, projected_count)
        self.assertEqual("configured_full_refresh", projected_source)

    def test_projected_request_count_uses_built_in_hidive_incremental_default(self) -> None:
        projected_count, projected_source = _projected_request_count(
            self.config,
            TaskSpec("sync_fetch_hidive", self.config.service.sync_every_seconds, budget_provider="hidive"),
            {},
            fetch_mode="hot",
        )
        self.assertEqual(4, projected_count)
        self.assertEqual("configured_hot", projected_source)

    def test_projected_request_count_treats_built_in_mal_refresh_default_as_cold_start_seed(self) -> None:
        projected_count, projected_source = _projected_request_count(
            self.config,
            TaskSpec("mal_refresh", self.config.service.mal_refresh_every_seconds, budget_provider="mal"),
            {"last_request_delta_history": [2, 3, 2]},
        )
        self.assertEqual(3, projected_count)
        self.assertEqual("observed_smoothed", projected_source)

    def test_projected_request_count_treats_built_in_full_refresh_default_as_cold_start_seed(self) -> None:
        projected_count, projected_source = _projected_request_count(
            self.config,
            TaskSpec("sync_fetch_crunchyroll", self.config.service.sync_every_seconds, budget_provider="crunchyroll"),
            {
                "last_request_delta_history_by_mode": {"full_refresh": [18, 22, 20]},
                "last_request_delta_by_mode": {"full_refresh": 20},
            },
            fetch_mode="full_refresh",
        )
        self.assertEqual(22, projected_count)
        self.assertEqual("observed_full_refresh_p90", projected_source)

    def test_projected_request_count_uses_built_in_hidive_percentile_for_observed_history(self) -> None:
        projected_count, projected_source = _projected_request_count(
            self.config,
            TaskSpec("sync_fetch_hidive", self.config.service.sync_every_seconds, budget_provider="hidive"),
            {
                "last_request_delta_history_by_mode": {"full_refresh": [18, 22, 20]},
                "last_request_delta_by_mode": {"full_refresh": 20},
            },
            fetch_mode="full_refresh",
        )
        self.assertEqual(22, projected_count)
        self.assertEqual("observed_full_refresh_p90", projected_source)

    def test_projected_request_count_lets_task_wide_override_beat_built_in_full_refresh_seed(self) -> None:
        self.config.service.task_projected_request_counts["sync_fetch_crunchyroll"] = 12
        projected_count, projected_source = _projected_request_count(
            self.config,
            TaskSpec("sync_fetch_crunchyroll", self.config.service.sync_every_seconds, budget_provider="crunchyroll"),
            {},
            fetch_mode="full_refresh",
        )
        self.assertEqual(12, projected_count)
        self.assertEqual("configured", projected_source)

    def test_run_pending_tasks_auto_uses_conservative_percentile_for_bursty_history(self) -> None:
        self.config.service.sync_every_seconds = 0
        self.config.service.health_every_seconds = 3600
        self.config.service.mal_refresh_every_seconds = 3600
        self.config.service.provider_projected_request_percentiles.pop("crunchyroll", None)
        planned_deltas = iter([2, 12, 2, 20])

        def fake_run_subprocess(config, args, *, label):
            if label == "sync_fetch_crunchyroll":
                for index in range(next(planned_deltas)):
                    record_api_request_event(
                        "crunchyroll",
                        "sync-fetch",
                        url=f"https://example.invalid/api/{index}",
                        method="GET",
                        outcome="ok",
                        status_code=200,
                        config=config,
                    )
            return {"status": "ok", "label": label, "returncode": 0, "stdout": "", "stderr": ""}

        with patch("mal_updater.service_runtime._refresh_mal_tokens", return_value={"status": "ok"}), patch(
            "mal_updater.service_runtime._run_subprocess",
            side_effect=fake_run_subprocess,
        ):
            run_pending_tasks(self.config)
            run_pending_tasks(self.config)
            run_pending_tasks(self.config)
            run_pending_tasks(self.config)

        saved = json.loads(self.config.service_state_path.read_text(encoding="utf-8"))
        sync_state = saved["tasks"]["sync_fetch_crunchyroll"]
        self.assertEqual([2, 12, 2, 20], sync_state["last_request_delta_history"])
        self.assertEqual({"hot": [2, 12, 2, 20]}, sync_state["last_request_delta_history_by_mode"])
        self.assertEqual(20, sync_state["projected_request_count"])
        self.assertEqual("observed_hot_auto_p90", sync_state["projected_request_source"])
        self.assertEqual(7, sync_state["projected_request_history_window"])
        self.assertEqual("hot", sync_state["projected_request_history_mode"])
        self.assertEqual(4, sync_state["projected_request_history_sample_count"])
        self.assertEqual(0.9, sync_state["projected_request_percentile"])
        self.assertEqual("auto", sync_state["projected_request_percentile_source"])

        projected_count, projected_source = _projected_request_count(
            self.config,
            TaskSpec("sync_fetch_crunchyroll", self.config.service.sync_every_seconds, budget_provider="crunchyroll"),
            sync_state,
            fetch_mode="hot",
        )
        self.assertEqual(20, projected_count)
        self.assertEqual("observed_hot_auto_p90", projected_source)

    def test_run_pending_tasks_clears_budget_backoff_after_successful_run(self) -> None:
        state = {
            "started_at": "2026-03-20T20:00:00Z",
            "tasks": {
                "sync_fetch_crunchyroll": {
                    "budget_backoff_level": "warn",
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
        self.assertNotIn("budget_backoff_level", saved["tasks"]["sync_fetch_crunchyroll"])
        self.assertNotIn("budget_backoff_until", saved["tasks"]["sync_fetch_crunchyroll"])
        self.assertNotIn("budget_backoff_until_epoch", saved["tasks"]["sync_fetch_crunchyroll"])
        self.assertEqual("ok", saved["tasks"]["sync_fetch_crunchyroll"]["last_status"])
        self.assertIn("next_due_at", saved["tasks"]["sync_fetch_crunchyroll"])
        self.assertNotIn("last_skip_reason", saved["tasks"]["sync_fetch_crunchyroll"])
        self.assertIn("last_started_at", saved["tasks"]["sync_fetch_crunchyroll"])
        self.assertIn("last_finished_at", saved["tasks"]["sync_fetch_crunchyroll"])
        self.assertIn("last_decision_at", saved["tasks"]["sync_fetch_crunchyroll"])
        self.assertIn("last_duration_seconds", saved["tasks"]["sync_fetch_crunchyroll"])

    def test_run_pending_tasks_records_failure_backoff_for_provider_errors(self) -> None:
        with patch("mal_updater.service_runtime._budget_gate", side_effect=[(True, None, {"provider": "mal"}), (True, None, {"provider": "crunchyroll"}), (True, None, {"provider": "mal"}), (True, None, None)]), patch(
            "mal_updater.service_runtime._refresh_mal_tokens",
            return_value={"status": "ok"},
        ), patch(
            "mal_updater.service_runtime._run_subprocess",
            side_effect=[
                {"status": "error", "label": "sync_fetch_crunchyroll", "returncode": 1, "stdout": "", "stderr": "HTTP 401 from Crunchyroll\n"},
                {"status": "ok", "label": "sync_apply", "returncode": 0, "stdout": "", "stderr": ""},
                {"status": "ok", "label": "health", "returncode": 0, "stdout": "", "stderr": ""},
            ],
        ):
            result = run_pending_tasks(self.config)

        sync_result = next(item for item in result["results"] if item["task"] == "sync_fetch_crunchyroll")
        self.assertEqual("error", sync_result["status"])
        self.assertGreaterEqual(sync_result["failure_backoff_remaining_seconds"], 300)
        self.assertEqual("HTTP 401 from Crunchyroll", sync_result["failure_backoff_reason"])
        self.assertEqual("auth", sync_result["failure_backoff_class"])
        self.assertEqual(7200, sync_result["failure_backoff_floor_seconds"])
        self.assertEqual(1, sync_result["failure_backoff_consecutive_failures"])

        state = json.loads(self.config.service_state_path.read_text(encoding="utf-8"))
        sync_state = state["tasks"]["sync_fetch_crunchyroll"]
        self.assertEqual("error", sync_state["last_status"])
        self.assertEqual("HTTP 401 from Crunchyroll", sync_state["last_error"])
        self.assertEqual("auth", sync_state["failure_backoff_class"])
        self.assertEqual(7200, sync_state["failure_backoff_floor_seconds"])
        self.assertIn("failure_backoff_until", sync_state)
        self.assertIn("failure_backoff_until_epoch", sync_state)
        self.assertGreaterEqual(sync_state["failure_backoff_remaining_seconds"], 300)
        self.assertEqual(1, sync_state["failure_backoff_consecutive_failures"])

    def test_run_pending_tasks_uses_provider_auth_failure_floor_for_auth_style_errors(self) -> None:
        self.config.service.provider_auth_failure_backoff_floor_seconds["crunchyroll"] = 1800

        with patch("mal_updater.service_runtime._budget_gate", side_effect=[(True, None, {"provider": "mal"}), (True, None, {"provider": "crunchyroll"}), (True, None, {"provider": "mal"}), (True, None, None)]), patch(
            "mal_updater.service_runtime._refresh_mal_tokens",
            return_value={"status": "ok"},
        ), patch(
            "mal_updater.service_runtime._run_subprocess",
            side_effect=[
                {"status": "error", "label": "sync_fetch_crunchyroll", "returncode": 1, "stdout": "", "stderr": "login failed for Crunchyroll refresh token\n"},
                {"status": "ok", "label": "sync_apply", "returncode": 0, "stdout": "", "stderr": ""},
                {"status": "ok", "label": "health", "returncode": 0, "stdout": "", "stderr": ""},
            ],
        ):
            result = run_pending_tasks(self.config)

        sync_result = next(item for item in result["results"] if item["task"] == "sync_fetch_crunchyroll")
        self.assertEqual("auth", sync_result["failure_backoff_class"])
        self.assertEqual(1800, sync_result["failure_backoff_floor_seconds"])
        self.assertEqual(1800, sync_result["failure_backoff_remaining_seconds"])

        state = json.loads(self.config.service_state_path.read_text(encoding="utf-8"))
        sync_state = state["tasks"]["sync_fetch_crunchyroll"]
        self.assertEqual("auth", sync_state["failure_backoff_class"])
        self.assertEqual(1800, sync_state["failure_backoff_floor_seconds"])

    def test_run_pending_tasks_treats_missing_refresh_token_as_auth_style_failure(self) -> None:
        self.config.service.provider_auth_failure_backoff_floor_seconds["crunchyroll"] = 2400

        with patch("mal_updater.service_runtime._budget_gate", side_effect=[(True, None, {"provider": "mal"}), (True, None, {"provider": "crunchyroll"}), (True, None, {"provider": "mal"}), (True, None, None)]), patch(
            "mal_updater.service_runtime._refresh_mal_tokens",
            return_value={"status": "ok"},
        ), patch(
            "mal_updater.service_runtime._run_subprocess",
            side_effect=[
                {"status": "error", "label": "sync_fetch_crunchyroll", "returncode": 1, "stdout": "", "stderr": "Missing Crunchyroll refresh token at /tmp/cr-token\n"},
                {"status": "ok", "label": "sync_apply", "returncode": 0, "stdout": "", "stderr": ""},
                {"status": "ok", "label": "health", "returncode": 0, "stdout": "", "stderr": ""},
            ],
        ):
            result = run_pending_tasks(self.config)

        sync_result = next(item for item in result["results"] if item["task"] == "sync_fetch_crunchyroll")
        self.assertEqual("auth", sync_result["failure_backoff_class"])
        self.assertEqual(2400, sync_result["failure_backoff_floor_seconds"])
        self.assertEqual("Missing Crunchyroll refresh token at /tmp/cr-token", sync_result["failure_backoff_reason"])

        state = json.loads(self.config.service_state_path.read_text(encoding="utf-8"))
        sync_state = state["tasks"]["sync_fetch_crunchyroll"]
        self.assertEqual("auth", sync_state["failure_backoff_class"])
        self.assertEqual(2400, sync_state["failure_backoff_floor_seconds"])

    def test_run_pending_tasks_skips_provider_retries_while_failure_backoff_is_active(self) -> None:
        state = {
            "started_at": "2026-03-20T20:00:00Z",
            "tasks": {
                "sync_fetch_crunchyroll": {
                    "failure_backoff_until_epoch": datetime.now(timezone.utc).timestamp() + 600,
                    "failure_backoff_until": "2026-03-20T21:10:00Z",
                    "failure_backoff_reason": "HTTP 401 from Crunchyroll",
                    "failure_backoff_class": "auth",
                    "failure_backoff_floor_seconds": 1800,
                    "failure_backoff_consecutive_failures": 2,
                }
            },
        }
        self.config.service_state_path.write_text(json.dumps(state, indent=2), encoding="utf-8")

        with patch("mal_updater.service_runtime._budget_gate", side_effect=[(True, None, {"provider": "mal"}), (True, None, {"provider": "mal"}), (True, None, None)]), patch(
            "mal_updater.service_runtime._refresh_mal_tokens",
            return_value={"status": "ok"},
        ), patch(
            "mal_updater.service_runtime._run_subprocess",
            side_effect=[
                {"status": "ok", "label": "sync_apply", "returncode": 0, "stdout": "", "stderr": ""},
                {"status": "ok", "label": "health", "returncode": 0, "stdout": "", "stderr": ""},
            ],
        ):
            result = run_pending_tasks(self.config)

        sync_result = next(item for item in result["results"] if item["task"] == "sync_fetch_crunchyroll")
        self.assertEqual("skipped", sync_result["status"])
        self.assertIn("failure_backoff_active", sync_result["reason"])
        self.assertEqual("HTTP 401 from Crunchyroll", sync_result["failure_backoff_reason"])
        self.assertEqual("auth", sync_result["failure_backoff_class"])
        self.assertEqual(1800, sync_result["failure_backoff_floor_seconds"])
        self.assertEqual(2, sync_result["failure_backoff_consecutive_failures"])

        saved = json.loads(self.config.service_state_path.read_text(encoding="utf-8"))
        sync_state = saved["tasks"]["sync_fetch_crunchyroll"]
        self.assertEqual("skipped", sync_state["last_status"])
        self.assertIn("failure_backoff_active", sync_state["last_skip_reason"])
        self.assertGreater(sync_state["failure_backoff_remaining_seconds"], 0)

    def test_run_pending_tasks_clears_failure_backoff_after_successful_run(self) -> None:
        state = {
            "started_at": "2026-03-20T20:00:00Z",
            "tasks": {
                "sync_fetch_crunchyroll": {
                    "failure_backoff_until_epoch": 1,
                    "failure_backoff_until": "2026-03-20T21:00:00Z",
                    "failure_backoff_reason": "HTTP 401 from Crunchyroll",
                    "failure_backoff_consecutive_failures": 2,
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
        sync_state = saved["tasks"]["sync_fetch_crunchyroll"]
        self.assertNotIn("failure_backoff_until", sync_state)
        self.assertNotIn("failure_backoff_until_epoch", sync_state)
        self.assertNotIn("failure_backoff_reason", sync_state)
        self.assertNotIn("failure_backoff_class", sync_state)
        self.assertNotIn("failure_backoff_floor_seconds", sync_state)
        self.assertNotIn("failure_backoff_consecutive_failures", sync_state)
        self.assertEqual("ok", sync_state["last_status"])

class ServiceRuntimeRecommendMaintainTaskTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)
        self.project_root = Path(self.temp_dir.name)
        self.config = load_config(self.project_root)
        ensure_directories(self.config)
        self.config.service.sync_every_seconds = 60 * 60
        self.config.service.health_every_seconds = 60 * 60
        self.config.service.mal_refresh_every_seconds = 60 * 60
        self.config.service.recommendation_metadata_refresh_every_seconds = 0
        self.config.service.recommendations_webhook_push_every_seconds = 0
        self.assertEqual(60 * 60, self.config.service.recommend_maintain_every_seconds)

    def test_recommend_maintain_runs_as_recurring_service_task_and_sets_next_due(self) -> None:
        self.config.service_state_path.write_text(
            json.dumps(
                {
                    "started_at": "2026-07-06T00:00:00Z",
                    "tasks": {
                        "mal_refresh": {"last_run_epoch": time.time()},
                        "sync_apply": {"last_run_epoch": time.time()},
                        "health": {"last_run_epoch": time.time()},
                        "recommend_maintain": {"last_status": "dry_run", "execution_state": "awaiting_schedule", "last_run_epoch": 0},
                    },
                },
                indent=2,
            ),
            encoding="utf-8",
        )

        with patch(
            "mal_updater.service_runtime._run_subprocess",
            return_value={"status": "ok", "label": "recommend_maintain", "returncode": 0, "stdout": "", "stderr": ""},
        ) as run_subprocess:
            result = run_pending_tasks(self.config)

        self.assertEqual(["recommend_maintain"], [item["task"] for item in result["results"]])
        command = run_subprocess.call_args.args[1]
        self.assertIn("recommend-maintain", command)
        self.assertIn("--local-only", command)
        self.assertNotIn("--metadata-limit", command)
        self.assertNotIn("--discovery-target-limit", command)
        self.assertNotIn("--dry-run", command)

        saved = json.loads(self.config.service_state_path.read_text(encoding="utf-8"))
        task_state = saved["tasks"]["recommend_maintain"]
        self.assertEqual("ok", task_state["last_status"])
        self.assertEqual("idle", task_state["execution_state"])
        self.assertEqual(60 * 60, task_state["every_seconds"])
        self.assertIn("next_due_epoch", task_state)
        self.assertIn("next_due_at", task_state)
        self.assertGreater(task_state["next_due_epoch"], task_state["last_run_epoch"])

    def test_recommend_maintain_initial_state_schedules_one_hour_due(self) -> None:
        before = time.time()
        self.config.service_state_path.write_text(
            json.dumps(
                {
                    "started_at": "2026-07-06T00:00:00Z",
                    "tasks": {
                        "mal_refresh": {"last_run_epoch": before},
                        "sync_apply": {"last_run_epoch": before},
                        "health": {"last_run_epoch": before},
                    },
                },
                indent=2,
            ),
            encoding="utf-8",
        )

        with patch("mal_updater.service_runtime._run_subprocess") as run_subprocess:
            result = run_pending_tasks(self.config)

        self.assertEqual([], result["results"])
        run_subprocess.assert_not_called()
        saved = json.loads(self.config.service_state_path.read_text(encoding="utf-8"))
        task_state = saved["tasks"]["recommend_maintain"]
        self.assertEqual("scheduled", task_state["last_status"])
        self.assertEqual("idle", task_state["execution_state"])
        self.assertEqual(60 * 60, task_state["every_seconds"])
        self.assertGreaterEqual(task_state["next_due_epoch"], before + 60 * 60)
        self.assertLessEqual(task_state["next_due_epoch"], time.time() + 60 * 60 + 5)

    def test_recommend_maintain_waits_until_next_interval(self) -> None:
        now = time.time()
        self.config.service_state_path.write_text(
            json.dumps(
                {
                    "started_at": "2026-07-06T00:00:00Z",
                    "tasks": {
                        "mal_refresh": {"last_run_epoch": now},
                        "sync_apply": {"last_run_epoch": now},
                        "health": {"last_run_epoch": now},
                        "recommend_maintain": {"last_status": "ok", "last_run_epoch": now},
                    },
                },
                indent=2,
            ),
            encoding="utf-8",
        )

        with patch("mal_updater.service_runtime._run_subprocess") as run_subprocess:
            result = run_pending_tasks(self.config)

        self.assertEqual([], result["results"])
        run_subprocess.assert_not_called()

    def test_env_configures_recommend_maintain_interval(self) -> None:
        with patch.dict(os.environ, {"MAL_UPDATER_SERVICE_RECOMMEND_MAINTAIN_EVERY_SECONDS": "1234"}):
            config = load_config(self.project_root)
        self.assertEqual(1234, config.service.recommend_maintain_every_seconds)

class ServiceRuntimeSubprocessTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)
        self.project_root = Path(self.temp_dir.name)
        (self.project_root / "src").mkdir(parents=True)
        self.config = load_config(self.project_root)
        ensure_directories(self.config)

    def test_run_subprocess_logs_observable_command_without_secret_value(self) -> None:
        self.config.service.task_timeout_seconds = 30
        result = _run_subprocess(
            self.config,
            [sys.executable, "-c", "print('ok')", "--client-secret", "super-secret"],
            label="diagnostic",
        )

        self.assertEqual("ok", result["status"])
        self.assertEqual(30, result["timeout_seconds"])
        self.assertFalse(result["timed_out"])
        self.assertIn("--client-secret '<redacted>'", result["command"])
        self.assertNotIn("super-secret", result["command"])
        log_text = self.config.service_log_path.read_text()
        self.assertIn("task=diagnostic status=started", log_text)
        self.assertIn("timeout_seconds=30", log_text)
        self.assertIn("status=ok", log_text)
        self.assertNotIn("super-secret", log_text)

    def test_run_subprocess_sanitizes_and_bounds_stdout_stderr_and_failure_log(self) -> None:
        sentinel = "SENTINEL-subprocess-credential-123456789"
        result = _run_subprocess(
            self.config,
            [
                sys.executable,
                "-c",
                (
                    "import sys; "
                    f"print('authorization: Bearer {sentinel} ' + 'x' * 12000); "
                    f"print('password={sentinel} HTTP 401 invalid_grant', file=sys.stderr); "
                    "raise SystemExit(7)"
                ),
            ],
            label="sensitive_failure",
        )

        rendered = json.dumps(result)
        self.assertEqual("error", result["status"])
        self.assertEqual(7, result["returncode"])
        self.assertNotIn(sentinel, rendered)
        self.assertIn("<redacted>", rendered)
        self.assertIn("HTTP 401 invalid_grant", result["stderr"])
        self.assertLessEqual(len(result["stdout"]), 4000)
        self.assertIn("truncated", result["stdout"])
        log_text = self.config.service_log_path.read_text(encoding="utf-8")
        self.assertNotIn(sentinel, log_text)
        self.assertIn("HTTP 401 invalid_grant", log_text)

    def test_run_subprocess_timeout_sanitizes_partial_streams(self) -> None:
        sentinel = "SENTINEL-timeout-credential-123456789"
        self.config.service.task_timeout_seconds = 1
        result = _run_subprocess(
            self.config,
            [
                sys.executable,
                "-c",
                f"import sys,time; print('access_token={sentinel}', flush=True); print('Bearer {sentinel}', file=sys.stderr, flush=True); time.sleep(2)",
            ],
            label="sensitive_timeout",
        )

        rendered = json.dumps(result)
        self.assertEqual("subprocess_timeout", result["reason"])
        self.assertNotIn(sentinel, rendered)
        self.assertIn("<redacted>", rendered)

    def test_save_state_persists_only_sanitized_bounded_stream_summaries(self) -> None:
        sentinel = "SENTINEL-state-credential-123456789"
        state = {
            "tasks": {
                "sensitive": {
                    "last_status": "error",
                    "last_error": f"HTTP 401 invalid_grant refresh_token={sentinel}",
                    "last_result": {
                        "status": "error",
                        "stderr": f"password={sentinel} HTTP 401 invalid_grant " + ("x" * 12_000),
                        "stdout": f'{{"access_token":"{sentinel}"}}',
                        "response_text": f"secret={sentinel}",
                    },
                }
            }
        }

        _save_state(self.config, state)
        persisted = json.loads(self.config.service_state_path.read_text(encoding="utf-8"))
        rendered = json.dumps(persisted)
        persisted_result = persisted["tasks"]["sensitive"]["last_result"]
        self.assertNotIn(sentinel, rendered)
        self.assertIn("HTTP 401 invalid_grant", rendered)
        self.assertNotIn("stdout", persisted_result)
        self.assertNotIn("stderr", persisted_result)
        self.assertNotIn("response_text", persisted_result)
        self.assertLessEqual(len(persisted_result["stderr_snippet"]), 500)
        self.assertIn("truncated", persisted_result["stderr_snippet"])

    def test_run_subprocess_timeout_returns_status_reason_and_log(self) -> None:
        self.config.service.task_timeout_seconds = 1
        result = _run_subprocess(
            self.config,
            [sys.executable, "-c", "import time; time.sleep(2)"],
            label="slow_crunchyroll_fetch",
        )

        self.assertEqual("error", result["status"])
        self.assertEqual("subprocess_timeout", result["reason"])
        self.assertTrue(result["timed_out"])
        self.assertEqual(1, result["timeout_seconds"])
        self.assertIsNone(result["returncode"])
        self.assertIn("time.sleep(2)", result["command"])
        log_text = self.config.service_log_path.read_text()
        self.assertIn("task=slow_crunchyroll_fetch status=started", log_text)
        self.assertIn("reason=subprocess_timeout", log_text)
        self.assertIn("timeout_seconds=1", log_text)
