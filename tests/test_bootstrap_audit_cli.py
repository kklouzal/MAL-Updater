from __future__ import annotations

import io
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from mal_updater.cli import main as cli_main


class BootstrapAuditCliTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)
        self.project_root = Path(self.temp_dir.name)
        (self.project_root / ".MAL-Updater" / "config").mkdir(parents=True)
        scripts_dir = self.project_root / "scripts"
        scripts_dir.mkdir(parents=True)
        (scripts_dir / "install_user_systemd_units.sh").write_text("#!/usr/bin/env bash\n", encoding="utf-8")
        ops_dir = self.project_root / "ops" / "systemd-user"
        ops_dir.mkdir(parents=True)
        (ops_dir / "mal-updater.service").write_text(
            "[Unit]\nDescription=MAL-Updater\n[Service]\nEnvironmentFile=__MAL_UPDATER_SERVICE_ENV_FILE__\nWorkingDirectory=__MAL_UPDATER_REPO_ROOT__\n",
            encoding="utf-8",
        )

    def _run_bootstrap_audit_raw(self, *args: str) -> tuple[int, str]:
        argv = [
            "mal-updater",
            "--project-root",
            str(self.project_root),
            "bootstrap-audit",
            *args,
        ]
        with (
            patch("sys.argv", argv),
            patch("sys.stdout", new_callable=io.StringIO) as stdout,
            patch.dict("os.environ", {"XDG_CONFIG_HOME": str(self.project_root / ".config")}, clear=False),
        ):
            exit_code = cli_main()
        return exit_code, stdout.getvalue()

    def test_bootstrap_audit_json_exposes_provider_readiness_and_recommended_commands(self) -> None:
        exit_code, stdout = self._run_bootstrap_audit_raw()
        payload = json.loads(stdout)

        self.assertEqual(0, exit_code)
        self.assertIn("providers", payload)
        self.assertIn("summary", payload)
        self.assertIn("recommended_commands", payload)
        self.assertIn("runtime_initialization", payload)
        self.assertIn("secrets_dir_permissions", payload)
        self.assertIn("automation_installation", payload["services"])
        self.assertIn("operation_modes", payload)
        self.assertFalse(payload["providers"]["crunchyroll"]["ready"])
        self.assertEqual("not-configured", payload["providers"]["crunchyroll"]["operation_mode"])
        self.assertIn("credentials", payload["providers"]["crunchyroll"]["missing"])
        self.assertIn("session", payload["providers"]["crunchyroll"]["missing"])
        self.assertFalse(payload["providers"]["hidive"]["ready"])
        self.assertEqual("not-configured", payload["providers"]["hidive"]["operation_mode"])
        self.assertEqual(2, payload["summary"]["provider_count"])
        self.assertFalse(payload["summary"]["runtime_initialized"])
        self.assertIn("db_path", payload["runtime_initialization"]["missing"])
        self.assertIsNone(payload["summary"]["secrets_dir_restrictive"])
        self.assertFalse(payload["summary"]["automation_installed"])
        self.assertFalse(payload["summary"]["automation_current"])
        self.assertIsNone(payload["summary"]["automation_enabled"])
        self.assertIsNone(payload["summary"]["automation_active"])
        self.assertEqual("bootstrap-manual-acceptable", payload["summary"]["operation_mode"])
        self.assertTrue(payload["summary"]["manual_foreground_acceptable"])
        self.assertFalse(payload["summary"]["daemon_expected"])
        self.assertEqual("bootstrap-manual-acceptable", payload["operation_modes"]["mode"])
        self.assertEqual(0, payload["summary"]["intended_provider_count"])
        self.assertEqual(0, payload["summary"]["partially_staged_provider_count"])
        self.assertGreaterEqual(payload["summary"]["actionable_command_count"], 1)
        commands = [item["command"] for item in payload["recommended_commands"] if item.get("command")]
        self.assertIn("PYTHONPATH=src python3 -m mal_updater.cli init", commands)
        self.assertIn("PYTHONPATH=src python3 -m mal_updater.cli mal-auth-login", commands)
        self.assertIn(
            "PYTHONPATH=src python3 -m mal_updater.cli provider-auth-login --provider crunchyroll",
            commands,
        )
        self.assertIn(str(self.project_root / "scripts" / "install_user_systemd_units.sh"), commands)

    def test_bootstrap_audit_summary_reports_provider_missing_state_and_next_commands(self) -> None:
        secrets_dir = self.project_root / ".MAL-Updater" / "secrets"
        secrets_dir.mkdir(parents=True, exist_ok=True)
        secrets_dir.chmod(0o755)
        (secrets_dir / "crunchyroll_username.txt").write_text("user@example.com\n", encoding="utf-8")
        (secrets_dir / "crunchyroll_password.txt").write_text("top-secret\n", encoding="utf-8")
        crunchyroll_state_root = self.project_root / ".MAL-Updater" / "state" / "crunchyroll" / "default"
        crunchyroll_state_root.mkdir(parents=True, exist_ok=True)
        (crunchyroll_state_root / "refresh_token.txt").write_text("refresh-token\n", encoding="utf-8")

        exit_code, stdout = self._run_bootstrap_audit_raw("--summary")

        self.assertEqual(0, exit_code)
        self.assertIn("runtime_initialized=False", stdout)
        self.assertIn("runtime_missing=data_dir, cache_dir, db_path", stdout)
        self.assertIn("secrets_dir_mode=0o755", stdout)
        self.assertIn("secrets_dir_restrictive=False", stdout)
        self.assertIn("automation_installed=False", stdout)
        self.assertIn("automation_current=False", stdout)
        self.assertIn("operation_mode=bootstrap-manual-acceptable", stdout)
        self.assertIn("manual_foreground_acceptable=True", stdout)
        self.assertIn("daemon_expected=False", stdout)
        self.assertIn("provider_crunchyroll_ready=False", stdout)
        self.assertIn("provider_crunchyroll_operation_mode=credentials-staged-awaiting-bootstrap", stdout)
        self.assertIn("provider_crunchyroll_missing=session", stdout)
        self.assertIn(
            "provider_crunchyroll_next_command=PYTHONPATH=src python3 -m mal_updater.cli provider-auth-login --provider crunchyroll",
            stdout,
        )
        self.assertIn("intended_provider_count=1", stdout)
        self.assertIn("partially_staged_provider_count=1", stdout)
        self.assertIn("next_command=PYTHONPATH=src python3 -m mal_updater.cli init", stdout)
        self.assertIn(
            "next_command=PYTHONPATH=src python3 -m mal_updater.cli provider-auth-login --provider crunchyroll",
            stdout,
        )
        self.assertIn(f"next_command={self.project_root / 'scripts' / 'install_user_systemd_units.sh'}", stdout)
        self.assertIn("next_command=chmod 700", stdout)
        self.assertIn("blocking_step_count=", stdout)
        self.assertIn("nonblocking_step_count=", stdout)

    def test_bootstrap_audit_marks_daemon_expected_once_runtime_and_auth_state_exist(self) -> None:
        runtime_root = self.project_root / ".MAL-Updater"
        for relative in ("config", "data", "cache", "state", "secrets"):
            (runtime_root / relative).mkdir(parents=True, exist_ok=True)
        (runtime_root / "data" / "mal_updater.sqlite3").write_text("", encoding="utf-8")
        (runtime_root / "secrets" / "mal_client_id.txt").write_text("client-id\n", encoding="utf-8")
        (runtime_root / "secrets" / "mal_access_token.txt").write_text("access-token\n", encoding="utf-8")
        (runtime_root / "secrets" / "mal_refresh_token.txt").write_text("refresh-token\n", encoding="utf-8")

        exit_code, stdout = self._run_bootstrap_audit_raw()
        payload = json.loads(stdout)

        self.assertEqual(0, exit_code)
        self.assertEqual("bootstrap-provider-staged", payload["summary"]["operation_mode"])
        self.assertTrue(payload["summary"]["manual_foreground_acceptable"])
        self.assertFalse(payload["summary"]["daemon_expected"])
        self.assertEqual("bootstrap-provider-staged", payload["operation_modes"]["mode"])
        self.assertEqual(0, payload["summary"]["intended_provider_count"])
        self.assertEqual(0, payload["summary"]["partially_staged_provider_count"])

    def test_bootstrap_audit_marks_partial_provider_bootstrap_as_staged_not_ready(self) -> None:
        runtime_root = self.project_root / ".MAL-Updater"
        for relative in ("config", "data", "cache", "state", "secrets"):
            (runtime_root / relative).mkdir(parents=True, exist_ok=True)
        (runtime_root / "data" / "mal_updater.sqlite3").write_text("", encoding="utf-8")
        (runtime_root / "secrets" / "crunchyroll_username.txt").write_text("user@example.com\n", encoding="utf-8")
        (runtime_root / "secrets" / "crunchyroll_password.txt").write_text("top-secret\n", encoding="utf-8")

        exit_code, stdout = self._run_bootstrap_audit_raw()
        payload = json.loads(stdout)

        self.assertEqual(0, exit_code)
        self.assertEqual("bootstrap-provider-staged", payload["summary"]["operation_mode"])
        self.assertFalse(payload["summary"]["daemon_expected"])
        self.assertEqual(1, payload["summary"]["intended_provider_count"])
        self.assertEqual(1, payload["summary"]["partially_staged_provider_count"])
        self.assertEqual("bootstrap-provider-staged", payload["operation_modes"]["mode"])
        self.assertEqual("credentials-staged-awaiting-bootstrap", payload["providers"]["crunchyroll"]["operation_mode"])
        self.assertEqual(
            "PYTHONPATH=src python3 -m mal_updater.cli provider-auth-login --provider crunchyroll",
            payload["providers"]["crunchyroll"]["operation_guidance"]["next_command"],
        )

    def test_bootstrap_audit_marks_daemon_expected_once_mal_and_intended_provider_state_exist(self) -> None:
        runtime_root = self.project_root / ".MAL-Updater"
        for relative in ("config", "data", "cache", "state", "secrets"):
            (runtime_root / relative).mkdir(parents=True, exist_ok=True)
        (runtime_root / "data" / "mal_updater.sqlite3").write_text("", encoding="utf-8")
        (runtime_root / "secrets" / "mal_client_id.txt").write_text("client-id\n", encoding="utf-8")
        (runtime_root / "secrets" / "mal_access_token.txt").write_text("access-token\n", encoding="utf-8")
        (runtime_root / "secrets" / "mal_refresh_token.txt").write_text("refresh-token\n", encoding="utf-8")
        (runtime_root / "secrets" / "crunchyroll_username.txt").write_text("user@example.com\n", encoding="utf-8")
        (runtime_root / "secrets" / "crunchyroll_password.txt").write_text("top-secret\n", encoding="utf-8")
        crunchyroll_state_root = runtime_root / "state" / "crunchyroll" / "default"
        crunchyroll_state_root.mkdir(parents=True, exist_ok=True)
        (crunchyroll_state_root / "refresh_token.txt").write_text("refresh-token\n", encoding="utf-8")
        (crunchyroll_state_root / "device_id.txt").write_text("device-id\n", encoding="utf-8")

        exit_code, stdout = self._run_bootstrap_audit_raw()
        payload = json.loads(stdout)

        self.assertEqual(0, exit_code)
        self.assertEqual("daemon-expected-for-unattended", payload["summary"]["operation_mode"])
        self.assertTrue(payload["summary"]["manual_foreground_acceptable"])
        self.assertTrue(payload["summary"]["daemon_expected"])
        self.assertEqual(1, payload["summary"]["intended_provider_count"])
        self.assertEqual(0, payload["summary"]["partially_staged_provider_count"])
        self.assertEqual("daemon-expected-for-unattended", payload["operation_modes"]["mode"])
        self.assertEqual("ready-for-unattended", payload["providers"]["crunchyroll"]["operation_mode"])

    def test_bootstrap_audit_marks_provider_auth_degraded_from_session_residue(self) -> None:
        runtime_root = self.project_root / ".MAL-Updater"
        for relative in ("config", "data", "cache", "state", "secrets"):
            (runtime_root / relative).mkdir(parents=True, exist_ok=True)
        (runtime_root / "data" / "mal_updater.sqlite3").write_text("", encoding="utf-8")
        (runtime_root / "secrets" / "mal_client_id.txt").write_text("client-id\n", encoding="utf-8")
        (runtime_root / "secrets" / "mal_access_token.txt").write_text("access-token\n", encoding="utf-8")
        (runtime_root / "secrets" / "mal_refresh_token.txt").write_text("refresh-token\n", encoding="utf-8")
        (runtime_root / "secrets" / "crunchyroll_username.txt").write_text("user@example.com\n", encoding="utf-8")
        (runtime_root / "secrets" / "crunchyroll_password.txt").write_text("top-secret\n", encoding="utf-8")
        crunchyroll_state_root = runtime_root / "state" / "crunchyroll" / "default"
        crunchyroll_state_root.mkdir(parents=True, exist_ok=True)
        (crunchyroll_state_root / "refresh_token.txt").write_text("refresh-token\n", encoding="utf-8")
        (crunchyroll_state_root / "device_id.txt").write_text("device-id\n", encoding="utf-8")
        (crunchyroll_state_root / "session.json").write_text(
            json.dumps({"crunchyroll_phase": "auth_failed", "last_error": "refresh token revoked"}, indent=2),
            encoding="utf-8",
        )

        exit_code, stdout = self._run_bootstrap_audit_raw()
        payload = json.loads(stdout)

        self.assertEqual(0, exit_code)
        self.assertFalse(payload["providers"]["crunchyroll"]["ready"])
        self.assertTrue(payload["providers"]["crunchyroll"]["auth_degraded"])
        self.assertIn("auth", payload["providers"]["crunchyroll"]["missing"])
        self.assertEqual("auth-degraded-needs-rebootstrap", payload["providers"]["crunchyroll"]["operation_mode"])
        self.assertEqual("session_state", payload["providers"]["crunchyroll"]["auth_degradation"]["source"])
        self.assertIn("refresh token revoked", payload["providers"]["crunchyroll"]["operation_guidance"]["details"])
        self.assertEqual("bootstrap-provider-staged", payload["summary"]["operation_mode"])
        self.assertEqual(1, payload["summary"]["intended_provider_count"])
        self.assertEqual(1, payload["summary"]["partially_staged_provider_count"])

    def test_bootstrap_audit_summary_surfaces_rebootstrap_command_for_repeated_auth_failures(self) -> None:
        runtime_root = self.project_root / ".MAL-Updater"
        for relative in ("config", "data", "cache", "state", "secrets"):
            (runtime_root / relative).mkdir(parents=True, exist_ok=True)
        (runtime_root / "data" / "mal_updater.sqlite3").write_text("", encoding="utf-8")
        (runtime_root / "secrets" / "mal_client_id.txt").write_text("client-id\n", encoding="utf-8")
        (runtime_root / "secrets" / "mal_access_token.txt").write_text("access-token\n", encoding="utf-8")
        (runtime_root / "secrets" / "mal_refresh_token.txt").write_text("refresh-token\n", encoding="utf-8")
        (runtime_root / "secrets" / "hidive_username.txt").write_text("user@example.com\n", encoding="utf-8")
        (runtime_root / "secrets" / "hidive_password.txt").write_text("top-secret\n", encoding="utf-8")
        hidive_state_root = runtime_root / "state" / "hidive" / "default"
        hidive_state_root.mkdir(parents=True, exist_ok=True)
        (hidive_state_root / "authorisation_token.txt").write_text("access-token\n", encoding="utf-8")
        (hidive_state_root / "refresh_token.txt").write_text("refresh-token\n", encoding="utf-8")
        (runtime_root / "state" / "service-state.json").write_text(
            json.dumps(
                {
                    "tasks": {
                        "sync_fetch_hidive": {
                            "last_status": "error",
                            "last_error": "HIDIVE login failed: refresh token expired",
                            "failure_backoff_reason": "HIDIVE login failed: refresh token expired",
                            "failure_backoff_consecutive_failures": 3,
                        }
                    }
                },
                indent=2,
            ),
            encoding="utf-8",
        )

        exit_code, stdout = self._run_bootstrap_audit_raw("--summary")

        self.assertEqual(0, exit_code)
        self.assertIn("provider_hidive_ready=False", stdout)
        self.assertIn("provider_hidive_operation_mode=auth-degraded-needs-rebootstrap", stdout)
        self.assertIn("provider_hidive_missing=auth", stdout)
        self.assertIn(
            "provider_hidive_next_command=PYTHONPATH=src python3 -m mal_updater.cli provider-auth-login --provider hidive",
            stdout,
        )
        self.assertIn(
            "next_command=PYTHONPATH=src python3 -m mal_updater.cli provider-auth-login --provider hidive",
            stdout,
        )
        self.assertIn("partially_staged_provider_count=1", stdout)


if __name__ == "__main__":
    unittest.main()
