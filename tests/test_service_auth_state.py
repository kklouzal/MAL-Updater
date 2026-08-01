from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from mal_updater.config import load_config
from mal_updater.crunchyroll_auth import resolve_crunchyroll_state_paths
from mal_updater.hidive_auth import resolve_hidive_state_paths
from mal_updater.service_auth_state import (
    load_service_state,
    mal_bootstrap_auth_issue,
    provider_bootstrap_auth_issue,
    provider_service_auth_failure,
)


class ServiceAuthStateTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)
        self.project_root = Path(self.temp_dir.name)
        (self.project_root / ".MAL-Updater" / "config").mkdir(parents=True)
        self.config = load_config(self.project_root)
        self.config.state_dir.mkdir(parents=True, exist_ok=True)

    def test_import_does_not_pull_health_or_bootstrap_modules(self) -> None:
        env = dict(os.environ)
        env["PYTHONDONTWRITEBYTECODE"] = "1"
        env["PYTHONPATH"] = str(Path(__file__).resolve().parents[1] / "src")
        code = (
            "import importlib, sys; "
            "importlib.import_module('mal_updater.service_auth_state'); "
            "print('mal_updater.health_report' in sys.modules); "
            "print('mal_updater.bootstrap_guidance' in sys.modules)"
        )
        result = subprocess.run(
            [sys.executable, "-c", code],
            cwd=Path(__file__).resolve().parents[1],
            env=env,
            text=True,
            capture_output=True,
            check=True,
        )

        self.assertEqual(["False", "False"], result.stdout.strip().splitlines())

    def test_load_service_state_keeps_json_tolerance(self) -> None:
        self.assertIsNone(load_service_state(self.config))

        self.config.service_state_path.parent.mkdir(parents=True, exist_ok=True)
        self.config.service_state_path.write_text("not-json", encoding="utf-8")
        self.assertIsNone(load_service_state(self.config))

        self.config.service_state_path.write_text(json.dumps(["not", "a", "dict"]), encoding="utf-8")
        self.assertIsNone(load_service_state(self.config))

        self.config.service_state_path.write_text(json.dumps({"tasks": {}}), encoding="utf-8")
        self.assertEqual({"tasks": {}}, load_service_state(self.config))

    def test_service_failure_payload_preserves_threshold_and_metadata(self) -> None:
        service_state = {
            "tasks": {
                "mal_refresh": {
                    "last_error": "ignored because backoff reason wins",
                    "failure_backoff_reason": "MalApiError: invalid_grant from MAL token endpoint",
                    "failure_backoff_consecutive_failures": 3.0,
                    "failure_backoff_until": "2026-07-24T06:00:00Z",
                    "failure_backoff_remaining_seconds": 45.9,
                    "failure_backoff_class": "auth",
                    "failure_backoff_floor_seconds": 30.5,
                }
            }
        }

        issue = mal_bootstrap_auth_issue(service_state)

        self.assertEqual(
            {
                "provider": "mal",
                "reason": "MalApiError: invalid_grant from MAL token endpoint",
                "consecutive_failures": 3,
                "auth_failure_kind": "invalid_grant",
                "auth_failure_label": "revoked or invalid refresh/auth token",
                "auth_remediation_kind": "refresh-token-invalidated",
                "auth_remediation_detail": "stage fresh auth material because the existing refresh/auth token looks revoked, expired, or otherwise invalid",
                "failure_backoff_until": "2026-07-24T06:00:00Z",
                "failure_backoff_remaining_seconds": 45,
                "failure_backoff_class": "auth",
                "failure_backoff_floor_seconds": 30,
                "source": "service_state",
            },
            issue,
        )
        self.assertIsNone(mal_bootstrap_auth_issue(service_state, min_consecutive_failures=4))

    def test_auth_classification_retains_markers_but_redacts_credentials_and_account_fields(self) -> None:
        sentinel = "SENTINEL-auth-state-credential-123456789"
        session_path = resolve_hidive_state_paths(self.config).session_state_path
        session_path.parent.mkdir(parents=True, exist_ok=True)
        session_path.write_text(
            json.dumps(
                {
                    "hidive_phase": "auth_failed",
                    "last_error": f"HTTP 401 invalid_grant refresh_token={sentinel} username=user@example.invalid",
                }
            ),
            encoding="utf-8",
        )
        service_state = {
            "tasks": {
                "sync_fetch_hidive": {
                    "failure_backoff_reason": f"HTTP 401 invalid_grant access_token={sentinel}",
                    "failure_backoff_consecutive_failures": 2,
                }
            }
        }

        issue = provider_service_auth_failure(service_state, provider="hidive", config=self.config)
        self.assertIsNotNone(issue)
        rendered = json.dumps(issue)
        self.assertNotIn(sentinel, rendered)
        self.assertNotIn("user@example.invalid", rendered)
        self.assertIn("HTTP 401 invalid_grant", rendered)
        assert issue is not None
        self.assertEqual("invalid_grant", issue["auth_failure_kind"])

    def test_provider_issues_preserve_service_and_session_residue_semantics(self) -> None:
        hidive_session = resolve_hidive_state_paths(self.config).session_state_path
        hidive_session.parent.mkdir(parents=True, exist_ok=True)
        hidive_session.write_text(
            json.dumps(
                {
                    "hidive_phase": "auth_failed",
                    "last_error": "HIDIVE login did not return authorisationToken",
                }
            ),
            encoding="utf-8",
        )
        service_state = {
            "tasks": {
                "sync_fetch_hidive": {
                    "failure_backoff_reason": "provider snapshot failed after token churn",
                    "failure_backoff_consecutive_failures": 2,
                }
            }
        }

        service_failure = provider_service_auth_failure(service_state, provider="hidive", config=self.config)

        self.assertIsNotNone(service_failure)
        assert service_failure is not None
        self.assertEqual("hidive", service_failure["provider"])
        self.assertEqual("malformed_token_payload", service_failure["auth_failure_kind"])
        self.assertEqual("token-payload-malformed", service_failure["auth_remediation_kind"])
        self.assertEqual("auth_failed", service_failure["session_phase"])
        self.assertEqual("HIDIVE login did not return authorisationToken", service_failure["session_last_error"])
        self.assertNotIn("source", service_failure)

        crunchyroll_session = resolve_crunchyroll_state_paths(self.config).session_state_path
        crunchyroll_session.parent.mkdir(parents=True, exist_ok=True)
        crunchyroll_session.write_text(json.dumps({"crunchyroll_phase": "auth_failed"}), encoding="utf-8")

        session_issue = provider_bootstrap_auth_issue(provider="crunchyroll", config=self.config, service_state=None)

        self.assertIsNotNone(session_issue)
        assert session_issue is not None
        self.assertEqual("crunchyroll", session_issue["provider"])
        self.assertEqual("session phase auth_failed", session_issue["reason"])
        self.assertEqual("session_state", session_issue["source"])
        self.assertEqual("session_auth_failed", session_issue["auth_failure_kind"])
        self.assertEqual("session-auth-failed", session_issue["auth_remediation_kind"])
        self.assertEqual("auth_failed", session_issue["session_phase"])


if __name__ == "__main__":
    unittest.main()
