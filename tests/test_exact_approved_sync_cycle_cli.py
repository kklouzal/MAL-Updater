from __future__ import annotations

import io
import json
import sys
import tempfile
import unittest
from contextlib import redirect_stdout
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
            ) as apply_mock, patch.object(
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

    def test_exact_approved_sync_cycle_returns_success_with_fetch_warnings_when_apply_succeeds(self) -> None:
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
            ), patch.object(
                sys,
                "argv",
                ["mal-updater", "--project-root", str(root), "exact-approved-sync-cycle", "--full-refresh"],
            ), redirect_stdout(output):
                rc = main()

            self.assertEqual(0, rc)
            fetch_mock.assert_called_once()
            self.assertTrue(fetch_mock.call_args.args[5])
            payload = json.loads(output.getvalue())
            self.assertEqual("ok_with_warnings", payload["status"])
            self.assertEqual("warning", payload["fetches"][0]["status"])
            self.assertTrue(payload["fetches"][0]["full_refresh"])


if __name__ == "__main__":
    unittest.main()
