from __future__ import annotations

import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

from mal_updater.config import ensure_directories, load_config
from mal_updater.container_runtime import SchedulerSupervisor, _status_payload, daemon_command, initialize_runtime
from mal_updater.container_web import ControlStore
from mal_updater.service_runtime import _task_specs


ROOT = Path(__file__).resolve().parents[1]


class ContainerRuntimeTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory(prefix="mal-updater-container-runtime-", dir="/tmp")
        self.runtime = Path(self.tmp.name) / "data"
        self.env = patch.dict(
            os.environ,
            {
                "MAL_UPDATER_RUNTIME_ROOT": str(self.runtime),
                "MAL_UPDATER_SETTINGS_PATH": str(self.runtime / "config" / "settings.toml"),
            },
            clear=False,
        )
        self.env.start()
        self.config = load_config(ROOT)
        ensure_directories(self.config)
        self.store = ControlStore(self.config)

    def tearDown(self) -> None:
        self.env.stop()
        self.tmp.cleanup()

    def satisfy_mal_prerequisites(self) -> None:
        self.store.save_secrets({"mal_client_id": "fake-client"})
        (self.config.secrets_dir / "mal_access_token.txt").write_text("access\n", encoding="utf-8")
        (self.config.secrets_dir / "mal_refresh_token.txt").write_text("refresh\n", encoding="utf-8")

    def test_initialize_runtime_creates_layout_and_migrated_database(self) -> None:
        config = initialize_runtime(ROOT)
        self.assertTrue(config.db_path.is_file())
        for name in ("config", "secrets", "data", "state", "cache"):
            self.assertTrue((self.runtime / name).is_dir())

    def test_daemon_command_uses_installed_cli_and_explicit_project_root(self) -> None:
        command = daemon_command(Path("/app"))
        self.assertEqual(["-m", "mal_updater.cli", "--project-root", "/app", "service-run"], command[1:])

    def test_startup_without_prerequisites_is_blocked_and_does_not_spawn(self) -> None:
        daemon_ref = [None]
        popen = Mock()
        SchedulerSupervisor(self.config, self.store, daemon_ref, popen=popen).reconcile(now=10)
        popen.assert_not_called()
        payload = _status_payload(config=self.config, daemon=None, store=self.store)
        self.assertEqual("blocked", payload["automation_state"])
        self.assertFalse(payload["ready"])
        self.assertEqual(["mal_client_id", "mal_oauth_tokens"], payload["automation_blockers"])

    def test_ready_startup_and_readiness_transition_start_automatically(self) -> None:
        daemon_ref = [None]
        child = Mock()
        child.poll.return_value = None
        popen = Mock(return_value=child)
        supervisor = SchedulerSupervisor(self.config, self.store, daemon_ref, popen=popen)

        supervisor.reconcile(now=10)
        popen.assert_not_called()
        self.satisfy_mal_prerequisites()
        supervisor.reconcile(now=11)

        popen.assert_called_once_with(daemon_command(self.config.project_root), start_new_session=False)
        payload = _status_payload(config=self.config, daemon=child, store=self.store)
        self.assertEqual("running", payload["automation_state"])
        self.assertTrue(payload["ready"])

    def test_prerequisite_loss_stops_and_restoration_restarts(self) -> None:
        self.satisfy_mal_prerequisites()
        first = Mock()
        first.poll.return_value = None
        second = Mock()
        second.poll.return_value = None
        popen = Mock(side_effect=[first, second])
        daemon_ref = [None]
        supervisor = SchedulerSupervisor(self.config, self.store, daemon_ref, popen=popen)

        supervisor.reconcile(now=10)
        self.store.save_secrets({}, ["mal_client_id"])
        supervisor.reconcile(now=11)
        first.terminate.assert_called_once_with()
        first.wait.assert_called_once_with(timeout=20)
        self.assertIsNone(daemon_ref[0])

        self.store.save_secrets({"mal_client_id": "replacement-client"})
        supervisor.reconcile(now=12)
        self.assertIs(second, daemon_ref[0])
        self.assertEqual(2, popen.call_count)

    def test_child_failure_keeps_bounded_restart_backoff(self) -> None:
        self.satisfy_mal_prerequisites()
        failed = Mock()
        failed.poll.return_value = 1
        replacement = Mock()
        replacement.poll.return_value = None
        daemon_ref = [failed]
        popen = Mock(return_value=replacement)
        supervisor = SchedulerSupervisor(self.config, self.store, daemon_ref, popen=popen)

        supervisor.reconcile(now=10)
        popen.assert_not_called()
        self.assertEqual(12, supervisor.next_start)
        supervisor.reconcile(now=11)
        popen.assert_not_called()
        supervisor.reconcile(now=12)
        popen.assert_called_once()

    def test_legacy_false_state_is_ignored(self) -> None:
        self.satisfy_mal_prerequisites()
        self.store.state_path.write_text('{"daemon_enabled": false}\n', encoding="utf-8")
        status = self.store.status()
        self.assertNotIn("daemon_enabled", status)
        self.assertTrue(status["automation_desired"])
        self.assertTrue(status["automation_prerequisites_satisfied"])

        child = Mock()
        child.poll.return_value = None
        popen = Mock(return_value=child)
        SchedulerSupervisor(self.config, self.store, [None], popen=popen).reconcile(now=10)
        popen.assert_called_once()

    def test_provider_lanes_remain_independently_credential_gated(self) -> None:
        self.satisfy_mal_prerequisites()
        baseline = {spec.name for spec in _task_specs(self.config)}
        self.assertNotIn("sync_fetch_crunchyroll", baseline)
        self.assertNotIn("sync_fetch_hidive", baseline)

        self.store.save_secrets({"crunchyroll_username": "user", "crunchyroll_password": "password"})
        crunchyroll_only = {spec.name for spec in _task_specs(self.config)}
        self.assertIn("sync_fetch_crunchyroll", crunchyroll_only)
        self.assertNotIn("sync_fetch_hidive", crunchyroll_only)


if __name__ == "__main__":
    unittest.main()
