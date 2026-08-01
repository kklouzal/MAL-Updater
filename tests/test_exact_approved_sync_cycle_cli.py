from __future__ import annotations

import io
import json
import sys
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from unittest.mock import patch

from mal_updater.cli import main


class ExactApprovedSyncCycleCliTests(unittest.TestCase):
    def test_exact_approved_sync_cycle_fetches_staged_providers_and_executes_apply(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / ".MAL-Updater" / "config").mkdir(parents=True)
            secrets_dir = root / ".MAL-Updater" / "secrets"
            secrets_dir.mkdir(parents=True)
            (secrets_dir / "crunchyroll_username.txt").write_text("user@example.com\n", encoding="utf-8")
            (secrets_dir / "crunchyroll_password.txt").write_text("secret\n", encoding="utf-8")
            (secrets_dir / "hidive_username.txt").write_text("user@example.com\n", encoding="utf-8")
            (secrets_dir / "hidive_password.txt").write_text("secret\n", encoding="utf-8")

            output = io.StringIO()
            with patch("mal_updater.cli._cmd_provider_fetch_snapshot", return_value=0) as fetch_mock, patch(
                "mal_updater.cli._run_apply_sync", return_value=[]
            ) as apply_mock, patch.dict(
                "os.environ", {"MAL_UPDATER_RUNTIME_ROOT": str(root / ".MAL-Updater")}, clear=False
            ), patch.object(
                sys,
                "argv",
                ["mal-updater", "--project-root", str(root), "exact-approved-sync-cycle"],
            ), redirect_stdout(output):
                rc = main()

            self.assertEqual(0, rc)
            self.assertEqual(2, fetch_mock.call_count)
            fetch_args = [call.args[1] for call in fetch_mock.call_args_list]
            self.assertEqual(["crunchyroll", "hidive"], fetch_args)
            apply_mock.assert_called_once()
            self.assertEqual(Path(root), apply_mock.call_args.args[0].project_root)
            self.assertEqual(0, apply_mock.call_args.kwargs["limit"])
            self.assertEqual(5, apply_mock.call_args.kwargs["mapping_limit"])
            self.assertTrue(apply_mock.call_args.kwargs["exact_approved_only"])
            self.assertTrue(apply_mock.call_args.kwargs["execute"])
            payload = json.loads(output.getvalue())
            self.assertEqual("ok", payload["status"])
            self.assertEqual(["crunchyroll", "hidive"], payload["providers_fetched"])

    def test_exact_approved_sync_cycle_aborts_on_fetch_failure_by_default(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / ".MAL-Updater" / "config").mkdir(parents=True)
            secrets_dir = root / ".MAL-Updater" / "secrets"
            secrets_dir.mkdir(parents=True)
            (secrets_dir / "crunchyroll_username.txt").write_text("user@example.com\n", encoding="utf-8")
            (secrets_dir / "crunchyroll_password.txt").write_text("secret\n", encoding="utf-8")

            output = io.StringIO()
            with patch("mal_updater.cli._cmd_provider_fetch_snapshot", return_value=1) as fetch_mock, patch(
                "mal_updater.cli._run_apply_sync", return_value=[]
            ) as apply_mock, patch.dict(
                "os.environ", {"MAL_UPDATER_RUNTIME_ROOT": str(root / ".MAL-Updater")}, clear=False
            ), patch.object(
                sys,
                "argv",
                ["mal-updater", "--project-root", str(root), "exact-approved-sync-cycle", "--full-refresh"],
            ), redirect_stdout(output):
                rc = main()

            self.assertEqual(1, rc)
            fetch_mock.assert_called_once()
            self.assertTrue(fetch_mock.call_args.args[5])
            apply_mock.assert_not_called()
            payload = json.loads(output.getvalue())
            self.assertEqual("aborted", payload["status"])
            self.assertEqual("provider_refresh_failed", payload["reason"])
            self.assertTrue(payload["apply_skipped"])
            self.assertEqual("provider_refresh_failed", payload["apply_skip_reason"])
            self.assertFalse(payload["stale_provider_apply_authorized"])
            self.assertEqual("failed", payload["fetches"][0]["status"])
            self.assertTrue(payload["fetches"][0]["full_refresh"])

    def test_exact_approved_sync_cycle_suppresses_sensitive_failed_fetch_output(self) -> None:
        sensitive_values = ["user@example.com", "Bearer secret-token", "password=value"]

        def noisy_failed_fetch(*_args: object, **_kwargs: object) -> int:
            print("provider stdout includes " + " ".join(sensitive_values))
            print("provider stderr includes " + " ".join(sensitive_values), file=sys.stderr)
            return 7

        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / ".MAL-Updater" / "config").mkdir(parents=True)
            secrets_dir = root / ".MAL-Updater" / "secrets"
            secrets_dir.mkdir(parents=True)
            (secrets_dir / "crunchyroll_username.txt").write_text("configured@example.com\n", encoding="utf-8")
            (secrets_dir / "crunchyroll_password.txt").write_text("configured-secret\n", encoding="utf-8")

            outer_stdout = io.StringIO()
            outer_stderr = io.StringIO()
            with patch("mal_updater.cli._cmd_provider_fetch_snapshot", side_effect=noisy_failed_fetch) as fetch_mock, patch(
                "mal_updater.cli._run_apply_sync", return_value=[]
            ) as apply_mock, patch.dict(
                "os.environ", {"MAL_UPDATER_RUNTIME_ROOT": str(root / ".MAL-Updater")}, clear=False
            ), patch.object(
                sys,
                "argv",
                ["mal-updater", "--project-root", str(root), "exact-approved-sync-cycle"],
            ), redirect_stdout(outer_stdout), redirect_stderr(outer_stderr):
                rc = main()

            self.assertNotEqual(0, rc)
            fetch_mock.assert_called_once()
            apply_mock.assert_not_called()
            stdout_text = outer_stdout.getvalue()
            stderr_text = outer_stderr.getvalue()
            payload = json.loads(stdout_text)
            self.assertEqual("aborted", payload["status"])
            self.assertEqual("provider_refresh_failed", payload["reason"])
            self.assertTrue(payload["apply_skipped"])
            self.assertEqual("provider_refresh_failed", payload["apply_skip_reason"])
            self.assertEqual(
                {
                    "status": "failed",
                    "reason": "provider_refresh_failed",
                    "target_count": 1,
                    "attempted_count": 1,
                    "succeeded_count": 0,
                    "failed_count": 1,
                    "failed_providers": ["crunchyroll"],
                },
                payload["provider_refresh"],
            )
            self.assertEqual(["crunchyroll"], payload["providers_considered"])
            self.assertEqual(["crunchyroll"], payload["providers_fetch_attempted"])
            self.assertEqual([], payload["providers_fetched"])
            self.assertEqual(["crunchyroll"], payload["providers_failed"])
            self.assertEqual(1, len(payload["fetches"]))
            self.assertEqual("crunchyroll", payload["fetches"][0]["provider"])
            self.assertEqual("failed", payload["fetches"][0]["status"])
            self.assertEqual(7, payload["fetches"][0]["exit_code"])
            self.assertTrue(payload["fetches"][0]["failed"])
            self.assertNotIn("stdout", payload["fetches"][0])
            self.assertNotIn("stderr", payload["fetches"][0])
            json_text = json.dumps(payload, sort_keys=True)
            for value in sensitive_values:
                self.assertNotIn(value, stdout_text)
                self.assertNotIn(value, json_text)
                self.assertNotIn(value, stderr_text)

    def test_exact_approved_sync_cycle_allows_stale_provider_apply_with_explicit_override(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / ".MAL-Updater" / "config").mkdir(parents=True)
            secrets_dir = root / ".MAL-Updater" / "secrets"
            secrets_dir.mkdir(parents=True)
            (secrets_dir / "crunchyroll_username.txt").write_text("user@example.com\n", encoding="utf-8")
            (secrets_dir / "crunchyroll_password.txt").write_text("secret\n", encoding="utf-8")

            output = io.StringIO()
            with patch("mal_updater.cli._cmd_provider_fetch_snapshot", return_value=1) as fetch_mock, patch(
                "mal_updater.cli._run_apply_sync", return_value=[]
            ) as apply_mock, patch.dict(
                "os.environ", {"MAL_UPDATER_RUNTIME_ROOT": str(root / ".MAL-Updater")}, clear=False
            ), patch.object(
                sys,
                "argv",
                [
                    "mal-updater",
                    "--project-root",
                    str(root),
                    "exact-approved-sync-cycle",
                    "--allow-stale-provider-apply",
                ],
            ), redirect_stdout(output):
                rc = main()

            self.assertEqual(0, rc)
            fetch_mock.assert_called_once()
            apply_mock.assert_called_once()
            payload = json.loads(output.getvalue())
            self.assertEqual("ok_with_warnings", payload["status"])
            self.assertEqual("stale_provider_apply_authorized", payload["reason"])
            self.assertTrue(payload["stale_provider_apply_authorized"])
            self.assertEqual("provider_refresh_failed", payload["stale_provider_apply_reason"])
            self.assertFalse(payload["apply_skipped"])
            self.assertEqual(["crunchyroll"], payload["providers_failed"])
            self.assertEqual({"provider_refresh_failed", "stale_provider_apply_authorized"}, {item["code"] for item in payload["warnings"]})

    def test_exact_approved_sync_cycle_aborts_without_provider_targets_by_default(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / ".MAL-Updater" / "config").mkdir(parents=True)

            output = io.StringIO()
            with patch("mal_updater.cli._cmd_provider_fetch_snapshot", return_value=0) as fetch_mock, patch(
                "mal_updater.cli._run_apply_sync", return_value=[]
            ) as apply_mock, patch.dict(
                "os.environ", {"MAL_UPDATER_RUNTIME_ROOT": str(root / ".MAL-Updater")}, clear=False
            ), patch.object(
                sys,
                "argv",
                ["mal-updater", "--project-root", str(root), "exact-approved-sync-cycle"],
            ), redirect_stdout(output):
                rc = main()

            self.assertEqual(1, rc)
            fetch_mock.assert_not_called()
            apply_mock.assert_not_called()
            payload = json.loads(output.getvalue())
            self.assertEqual("aborted", payload["status"])
            self.assertEqual("no_provider_targets", payload["reason"])
            self.assertEqual("not_configured", payload["provider_refresh"]["status"])
            self.assertTrue(payload["apply_skipped"])
            self.assertEqual([], payload["providers_considered"])
            self.assertEqual([], payload["providers_fetched"])
            self.assertFalse(payload["stale_provider_apply_authorized"])


if __name__ == "__main__":
    unittest.main()
