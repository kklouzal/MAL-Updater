from __future__ import annotations

import json
import tempfile
import textwrap
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

from mal_updater.auth_utils import current_utc_timestamp_z
from mal_updater.config import load_config
import mal_updater.crunchyroll_auth as crunchyroll_auth
from mal_updater.crunchyroll_auth import (
    CrunchyrollAuthError,
    crunchyroll_login_with_credentials,
    load_crunchyroll_credentials,
    resolve_crunchyroll_state_paths,
)


class _FakeResponse:
    def __init__(self, status_code: int, payload: dict[str, object]) -> None:
        self.status_code = status_code
        self._payload = payload

    def json(self) -> dict[str, object]:
        return self._payload


class CrunchyrollAuthTests(unittest.TestCase):
    def test_provider_timestamp_helper_preserves_utc_z_format(self) -> None:
        self.assertEqual(
            current_utc_timestamp_z(now=datetime(2026, 7, 23, 3, 1, 2, 345678, tzinfo=timezone.utc)),
            "2026-07-23T03:01:02Z",
        )

    def test_crunchyroll_http_requires_curl_cffi_transport(self) -> None:
        with patch.object(crunchyroll_auth, "curl_requests", None):
            with self.assertRaisesRegex(CrunchyrollAuthError, "requires curl_cffi"):
                crunchyroll_auth._http_post(
                    "https://example.invalid/token",
                    data={},
                    headers={},
                    timeout_seconds=1.0,
                )

    def test_load_crunchyroll_credentials_reads_secret_file_overrides(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / ".MAL-Updater" / "config").mkdir(parents=True)
            (root / ".MAL-Updater" / "secrets").mkdir(parents=True)
            (root / ".MAL-Updater" / "config" / "settings.toml").write_text(
                textwrap.dedent(
                    """
                    [secret_files]
                    crunchyroll_username = "../secrets/custom_username.txt"
                    crunchyroll_password = "../secrets/custom_password.txt"
                    """
                ),
                encoding="utf-8",
            )
            (root / ".MAL-Updater" / "secrets" / "custom_username.txt").write_text("user@example.com\n", encoding="utf-8")
            (root / ".MAL-Updater" / "secrets" / "custom_password.txt").write_text("hunter2\n", encoding="utf-8")

            credentials = load_crunchyroll_credentials(load_config(root))

            self.assertEqual(credentials.username, "user@example.com")
            self.assertEqual(credentials.password, "hunter2")
            self.assertEqual(credentials.username_path, (root / ".MAL-Updater" / "secrets" / "custom_username.txt").resolve())
            self.assertEqual(credentials.password_path, (root / ".MAL-Updater" / "secrets" / "custom_password.txt").resolve())

    def test_crunchyroll_session_state_json_contract(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            config = load_config(Path(td))
            state_paths = resolve_crunchyroll_state_paths(config, profile="family-room")
            state_paths.root.mkdir(parents=True)
            state_paths.refresh_token_path.write_text("refresh-token\n", encoding="utf-8")
            state_paths.device_id_path.write_text("device-id\n", encoding="utf-8")

            with patch("mal_updater.crunchyroll_auth._now_string", side_effect=["2026-07-23T03:01:01Z", "2026-07-23T03:01:02Z"]):
                crunchyroll_auth._write_session_state(
                    state_paths=state_paths,
                    profile="family-room",
                    locale="en-US",
                    device_type="ANDROIDTV",
                    account_id="acct-42",
                    last_error=None,
                    success=True,
                )

            self.assertEqual(
                state_paths.session_state_path.read_text(encoding="utf-8"),
                json.dumps(
                    {
                        "profile": "family-room",
                        "locale": "en-US",
                        "refresh_token_present": True,
                        "device_id_present": True,
                        "device_type_hint": "ANDROIDTV",
                        "last_login_attempt_at": "2026-07-23T03:01:01Z",
                        "last_login_success_at": "2026-07-23T03:01:02Z",
                        "last_account_id_hint": "acct-42",
                        "last_error": None,
                        "crunchyroll_phase": "ready",
                    },
                    indent=2,
                )
                + "\n",
            )

    def test_crunchyroll_login_with_credentials_persists_refresh_token_and_session_state(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / ".MAL-Updater" / "config").mkdir(parents=True)
            (root / ".MAL-Updater" / "secrets").mkdir(parents=True)
            (root / ".MAL-Updater" / "secrets" / "crunchyroll_username.txt").write_text("user@example.com\n", encoding="utf-8")
            (root / ".MAL-Updater" / "secrets" / "crunchyroll_password.txt").write_text("pw-123\n", encoding="utf-8")
            config = load_config(root)

            with patch("mal_updater.crunchyroll_auth._http_post") as mock_post, patch(
                "mal_updater.crunchyroll_auth._http_get"
            ) as mock_get:
                mock_post.return_value = _FakeResponse(
                    200,
                    {
                        "access_token": "access-abc",
                        "refresh_token": "refresh-xyz",
                        "account_id": "acct-42",
                    },
                )
                mock_get.return_value = _FakeResponse(
                    200,
                    {
                        "account_id": "acct-42",
                        "email": "user@example.com",
                    },
                )

                result = crunchyroll_login_with_credentials(config)

            state_paths = resolve_crunchyroll_state_paths(config)
            self.assertEqual(state_paths.refresh_token_path.read_text(encoding="utf-8"), "refresh-xyz\n")
            self.assertEqual(state_paths.device_id_path.read_text(encoding="utf-8").strip(), result.device_id)
            session_payload = json.loads(state_paths.session_state_path.read_text(encoding="utf-8"))
            self.assertEqual(session_payload["crunchyroll_phase"], "ready")
            self.assertEqual(session_payload["last_account_id_hint"], "acct-42")
            self.assertIsNone(session_payload["last_error"])
            self.assertEqual(result.account_email, "user@example.com")

            request_body = mock_post.call_args.kwargs["data"]
            self.assertEqual(request_body["grant_type"], "password")
            self.assertEqual(request_body["scope"], "offline_access")
            self.assertEqual(request_body["device_type"], "ANDROIDTV")

    def test_crunchyroll_login_with_credentials_records_failure_state(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / ".MAL-Updater" / "config").mkdir(parents=True)
            (root / ".MAL-Updater" / "secrets").mkdir(parents=True)
            (root / ".MAL-Updater" / "secrets" / "crunchyroll_username.txt").write_text("user@example.com\n", encoding="utf-8")
            (root / ".MAL-Updater" / "secrets" / "crunchyroll_password.txt").write_text("pw-123\n", encoding="utf-8")
            config = load_config(root)

            with patch("mal_updater.crunchyroll_auth._http_post") as mock_post:
                mock_post.return_value = _FakeResponse(
                    400,
                    {
                        "error": "invalid_grant",
                        "error_description": "bad credentials",
                    },
                )

                with self.assertRaises(CrunchyrollAuthError):
                    crunchyroll_login_with_credentials(config)

            state_paths = resolve_crunchyroll_state_paths(config)
            session_payload = json.loads(state_paths.session_state_path.read_text(encoding="utf-8"))
            self.assertEqual(session_payload["crunchyroll_phase"], "auth_failed")
            self.assertIn("invalid_grant", session_payload["last_error"])


if __name__ == "__main__":
    unittest.main()
