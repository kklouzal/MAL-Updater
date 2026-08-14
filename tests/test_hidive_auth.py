from __future__ import annotations

import json
import tempfile
import textwrap
import unittest
from pathlib import Path
from unittest.mock import patch

from mal_updater.auth_utils import decode_jwt_payload, jwt_expiry_epoch
from mal_updater.config import load_config
import mal_updater.hidive_auth as hidive_auth
from mal_updater.hidive_auth import (
    HIDIVE_REFRESH_WINDOW_SECONDS,
    HidiveAuthError,
    HidiveSession,
    HidiveStatePaths,
    HidiveTokenSet,
    _seconds_until_jwt_expiry,
    hidive_login_with_credentials,
    load_hidive_credentials,
    resolve_hidive_state_paths,
)


class _FakeResponse:
    def __init__(self, status_code: int, payload: dict[str, object]) -> None:
        self.status_code = status_code
        self._payload = payload
        self.text = json.dumps(payload)

    def json(self) -> dict[str, object]:
        return self._payload


class HidiveAuthTests(unittest.TestCase):
    def test_session_state_preserves_prior_account_and_redacts_provider_body(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            config = load_config(Path(td))
            paths = resolve_hidive_state_paths(config)
            hidive_auth._write_session_state(
                state_paths=paths, profile="default", account_id="acct-1", account_name="user",
                last_error=None, success=True, phase="ready",
            )
            hidive_auth._write_session_state(
                state_paths=paths, profile="default", account_id=None, account_name=None,
                last_error="HIDIVE GET /private failed: HTTP 403: secret provider body",
                success=False, phase="auth_failed",
            )
            payload = json.loads(paths.session_state_path.read_text(encoding="utf-8"))
            self.assertEqual(payload["last_account_id_hint"], "acct-1")
            self.assertEqual(payload["last_error"], "HIDIVE GET /private failed: HTTP 403: <redacted>")

    def test_json_request_rejects_untrusted_absolute_urls_before_request(self) -> None:
        rejected_urls = [
            "https://attacker.example/api/v2/page",
            "http://dce-frontoffice.imggaming.com/api/v2/page",
            "https://user:pass@dce-frontoffice.imggaming.com/api/v2/page",
            "https://dce-frontoffice.imggaming.com:444/api/v2/page",
            "https://dce-frontoffice.imggaming.com/not-api/page",
            "https://dce-frontoffice.imggaming.com/api/v2/page#fragment",
        ]
        with tempfile.TemporaryDirectory() as td:
            config = load_config(Path(td))
            config.hidive.request_spacing_seconds = 0.0
            config.hidive.request_spacing_jitter_seconds = 0.0
            for url in rejected_urls:
                with self.subTest(url=url), patch("mal_updater.hidive_auth.requests.request") as send:
                    with self.assertRaisesRegex(HidiveAuthError, "absolute URL is not allowed"):
                        hidive_auth._hidive_json_request(config, "GET", url, headers={"Authorization": "Bearer do-not-send"})
                    send.assert_not_called()

            self.assertFalse(config.api_request_events_path.exists())

    def test_json_request_allows_frontoffice_api_absolute_url_without_logging_secrets(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            config = load_config(Path(td))
            config.hidive.request_spacing_seconds = 0.0
            config.hidive.request_spacing_jitter_seconds = 0.0
            url = "https://DCE-FRONTOFFICE.IMGGAMING.COM/api/v2/page?token=query-secret&page=2"
            headers = {"Authorization": "Bearer authorization-secret"}
            with patch("mal_updater.hidive_auth.requests.request", return_value=_FakeResponse(200, {"ok": True})) as send:
                payload = hidive_auth._hidive_json_request(config, "GET", url, headers=headers)

            self.assertEqual({"ok": True}, payload)
            send.assert_called_once()
            self.assertEqual(
                "https://dce-frontoffice.imggaming.com/api/v2/page?token=query-secret&page=2",
                send.call_args.kwargs["url"] if "url" in send.call_args.kwargs else send.call_args.args[1],
            )
            events_json = config.api_request_events_path.read_text(encoding="utf-8")
            self.assertNotIn("authorization-secret", events_json)
            self.assertNotIn("query-secret", events_json)
            self.assertIn('"url": "https://dce-frontoffice.imggaming.com/api/v2/page?token=%3Credacted%3E&page=%3Cvalue%3E"', events_json)

    def test_login_and_refresh_post_failures_are_single_attempt(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            config = load_config(Path(td))
            config.hidive.retry_max_attempts = 5
            config.hidive.request_spacing_seconds = 0
            for path, effect in (("/login", _FakeResponse(503, {"error": "busy"})), ("/token/refresh", hidive_auth.requests.Timeout("ambiguous"))):
                with patch("mal_updater.hidive_auth.requests.request", side_effect=effect if isinstance(effect, BaseException) else None, return_value=None if isinstance(effect, BaseException) else effect) as send:
                    with self.assertRaises(HidiveAuthError):
                        hidive_auth._hidive_json_request(config, "POST", path, headers={}, json_body={})
                self.assertEqual(1, send.call_count)

    def test_ambiguous_refresh_failure_preserves_tokens_without_credential_rebootstrap(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            session = self._build_session(Path(td))
            with patch("mal_updater.hidive_auth._hidive_json_request", side_effect=HidiveAuthError("HIDIVE POST /token/refresh failed: HTTP 503")), patch(
                "mal_updater.hidive_auth.hidive_login_with_credentials"
            ) as login:
                with self.assertRaisesRegex(HidiveAuthError, "HTTP 503"):
                    session.refresh_tokens()
            login.assert_not_called()
            self.assertEqual("access-token", session.token.authorisation_token)
            self.assertEqual("refresh-token", session.token.refresh_token)

    def _build_session(self, root: Path) -> HidiveSession:
        config = load_config(root)
        state_paths = HidiveStatePaths(
            root=root / ".MAL-Updater" / "state" / "hidive" / "default",
            access_token_path=root / ".MAL-Updater" / "state" / "hidive" / "default" / "authorisation_token.txt",
            refresh_token_path=root / ".MAL-Updater" / "state" / "hidive" / "default" / "refresh_token.txt",
            session_state_path=root / ".MAL-Updater" / "state" / "hidive" / "default" / "session.json",
            sync_boundary_path=root / ".MAL-Updater" / "state" / "hidive" / "default" / "sync_boundary.json",
        )
        state_paths.root.mkdir(parents=True, exist_ok=True)
        return HidiveSession(
            config=config,
            profile="default",
            state_paths=state_paths,
            token=HidiveTokenSet(authorisation_token="access-token", refresh_token="refresh-token", account_id="acct-123"),
        )

    def test_json_request_records_http_and_transport_failures(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            config = load_config(Path(td))
            config.hidive.request_spacing_seconds = 0.0
            config.hidive.request_spacing_jitter_seconds = 0.0
            config.hidive.retry_backoff_base_seconds = 0.0
            config.hidive.retry_backoff_jitter_seconds = 0.0
            with patch("mal_updater.hidive_auth.requests.request", return_value=_FakeResponse(500, {"error": "failed"})):
                with self.assertRaisesRegex(HidiveAuthError, "HTTP 500"):
                    hidive_auth._hidive_json_request(config, "GET", "/test", headers={})
            with patch("mal_updater.hidive_auth.requests.request", side_effect=hidive_auth.requests.Timeout("token=do-not-log")):
                with self.assertRaisesRegex(HidiveAuthError, "request failed"):
                    hidive_auth._hidive_json_request(config, "GET", "/test", headers={})

            events = [json.loads(line) for line in config.api_request_events_path.read_text(encoding="utf-8").splitlines()]
            self.assertEqual(["http_error", "http_error", "request_error", "request_error"], [event["outcome"] for event in events])
            self.assertNotIn("do-not-log", json.dumps(events))

    def test_load_hidive_credentials_reads_secret_file_overrides(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / ".MAL-Updater" / "config").mkdir(parents=True)
            (root / ".MAL-Updater" / "secrets").mkdir(parents=True)
            (root / ".MAL-Updater" / "config" / "settings.toml").write_text(
                textwrap.dedent(
                    """
                    [secret_files]
                    hidive_username = "../secrets/custom_hidive_username.txt"
                    hidive_password = "../secrets/custom_hidive_password.txt"
                    """
                ),
                encoding="utf-8",
            )
            (root / ".MAL-Updater" / "secrets" / "custom_hidive_username.txt").write_text("user@example.com\n", encoding="utf-8")
            (root / ".MAL-Updater" / "secrets" / "custom_hidive_password.txt").write_text("hunter2\n", encoding="utf-8")

            credentials = load_hidive_credentials(load_config(root))

            self.assertEqual(credentials.username, "user@example.com")
            self.assertEqual(credentials.password, "hunter2")

    def test_hidive_jwt_helpers_decode_expiry_and_tolerate_invalid_tokens(self) -> None:
        token = (
            "eyJhbGciOiJIUzI1NiJ9."
            "eyJleHAiOjE3NzQwNjkyMzYsInN1YiI6IjU3MjQ2MnxkY2UuaGlkaXZlIn0."
            "signature"
        )
        self.assertEqual({"exp": 1774069236, "sub": "572462|dce.hidive"}, decode_jwt_payload(token))
        self.assertEqual(1774069236, jwt_expiry_epoch(token))
        self.assertEqual(1774069236 - 1774068636, _seconds_until_jwt_expiry(token, now_epoch=1774068636))
        self.assertIsNone(decode_jwt_payload("not-a-jwt"))
        self.assertIsNone(jwt_expiry_epoch("not-a-jwt"))
        self.assertIsNone(_seconds_until_jwt_expiry("not-a-jwt", now_epoch=1774068636))

    def test_hidive_session_state_json_contract_for_failure(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            config = load_config(Path(td))
            state_paths = resolve_hidive_state_paths(config, profile="family-room")
            with patch("mal_updater.hidive_auth._now_string", return_value="2026-07-23T03:01:01Z"):
                hidive_auth._write_session_state(
                    state_paths=state_paths,
                    profile="family-room",
                    account_id=None,
                    account_name=None,
                    last_error="HIDIVE POST /login failed: HTTP 401: bad credentials",
                    success=False,
                    phase="auth_failed",
                )

            self.assertEqual(
                state_paths.session_state_path.read_text(encoding="utf-8"),
                json.dumps(
                    {
                        "profile": "family-room",
                        "authorisation_token_present": False,
                        "refresh_token_present": False,
                        "last_login_attempt_at": "2026-07-23T03:01:01Z",
                        "last_login_success_at": None,
                        "last_account_id_hint": None,
                        "last_account_name_hint": None,
                        "last_error": "HIDIVE POST /login failed: HTTP 401: <redacted>",
                        "hidive_phase": "auth_failed",
                    },
                    indent=2,
                )
                + "\n",
            )

    def test_hidive_profile_loader_preserves_id_and_name_mapping(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            config = load_config(Path(td))
            with patch("mal_updater.hidive_auth._hidive_json_request") as mock_request:
                mock_request.return_value = {"id": 42, "username": "example-user", "email": "fallback@example.com"}
                account_id, account_name = hidive_auth._load_profile(config, "access-token", timeout_seconds=7.5)

            self.assertEqual((account_id, account_name), ("42", "example-user"))
            self.assertEqual(mock_request.call_args.args[:3], (config, "GET", "/user/profile"))
            self.assertEqual(mock_request.call_args.kwargs["headers"]["Authorization"], "Bearer access-token")
            self.assertEqual(mock_request.call_args.kwargs["timeout_seconds"], 7.5)

    def test_hidive_login_with_credentials_persists_tokens_and_session_state(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / ".MAL-Updater" / "config").mkdir(parents=True)
            (root / ".MAL-Updater" / "secrets").mkdir(parents=True)
            (root / ".MAL-Updater" / "secrets" / "hidive_username.txt").write_text("user@example.com\n", encoding="utf-8")
            (root / ".MAL-Updater" / "secrets" / "hidive_password.txt").write_text("pw-123\n", encoding="utf-8")
            config = load_config(root)

            with patch("mal_updater.hidive_auth.requests.request") as mock_request:
                mock_request.side_effect = [
                    _FakeResponse(
                        200,
                        {
                            "authorisationToken": "access-abc",
                            "refreshToken": "refresh-xyz",
                            "missingInformationStatus": "NONE",
                        },
                    ),
                    _FakeResponse(
                        200,
                        {
                            "id": "acct-42",
                            "displayName": "example-user",
                        },
                    ),
                ]

                result = hidive_login_with_credentials(config)

            state_paths = resolve_hidive_state_paths(config)
            self.assertEqual(state_paths.access_token_path.read_text(encoding="utf-8"), "access-abc\n")
            self.assertEqual(state_paths.refresh_token_path.read_text(encoding="utf-8"), "refresh-xyz\n")
            session_payload = json.loads(state_paths.session_state_path.read_text(encoding="utf-8"))
            self.assertEqual(session_payload["hidive_phase"], "ready")
            self.assertEqual(session_payload["last_account_id_hint"], "acct-42")
            self.assertEqual(result.account_name, "example-user")

    def test_hidive_login_with_credentials_records_failure_state(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / ".MAL-Updater" / "config").mkdir(parents=True)
            (root / ".MAL-Updater" / "secrets").mkdir(parents=True)
            (root / ".MAL-Updater" / "secrets" / "hidive_username.txt").write_text("user@example.com\n", encoding="utf-8")
            (root / ".MAL-Updater" / "secrets" / "hidive_password.txt").write_text("pw-123\n", encoding="utf-8")
            config = load_config(root)

            with patch("mal_updater.hidive_auth.requests.request") as mock_request:
                mock_request.return_value = _FakeResponse(
                    401,
                    {
                        "status": 401,
                        "code": "UNAUTHORIZED",
                        "messages": ["bad credentials"],
                    },
                )
                with self.assertRaises(HidiveAuthError):
                    hidive_login_with_credentials(config)

            state_paths = resolve_hidive_state_paths(config)
            session_payload = json.loads(state_paths.session_state_path.read_text(encoding="utf-8"))
            self.assertEqual(session_payload["hidive_phase"], "auth_failed")
            self.assertIn("HTTP 401", session_payload["last_error"])

    def test_hidive_session_refreshes_proactively_when_near_expiry(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / ".MAL-Updater" / "config").mkdir(parents=True)
            session = self._build_session(Path(td))
            session.token.authorisation_token = (
                "eyJhbGciOiJIUzI1NiJ9."
                "eyJleHAiOjEwMCwic3ViIjoiNTcyNDYyfGRjZS5oaWRpdmUifQ."
                "signature"
            )
            with patch("mal_updater.hidive_auth._seconds_until_jwt_expiry", return_value=HIDIVE_REFRESH_WINDOW_SECONDS), patch.object(
                HidiveSession, "refresh_tokens"
            ) as refresh_mock:
                session.ensure_fresh_tokens()
            refresh_mock.assert_called_once()

    def test_hidive_session_retries_401_via_refresh(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / ".MAL-Updater" / "config").mkdir(parents=True)
            session = self._build_session(Path(td))
            calls = []

            def fake_request(config, method, path, *, headers, params=None, json_body=None, timeout_seconds=None):
                calls.append((method, path, json_body, headers.get("Authorization")))
                if len(calls) == 1:
                    raise HidiveAuthError("HIDIVE GET /content/home failed: HTTP 401: expired")
                if path == "/token/refresh":
                    return {"authorisationToken": "refreshed-access", "refreshToken": "refreshed-refresh"}
                return {"ok": True}

            with patch("mal_updater.hidive_auth._hidive_json_request", side_effect=fake_request):
                payload = session.json_get("/content/home")

            self.assertEqual(payload, {"ok": True})
            self.assertEqual(session.token.authorisation_token, "refreshed-access")
            self.assertEqual(session.token.refresh_token, "refreshed-refresh")
            self.assertEqual([item[1] for item in calls], ["/content/home", "/token/refresh", "/content/home"])

    def test_hidive_session_falls_back_to_credentials_after_refresh_failure(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / ".MAL-Updater" / "config").mkdir(parents=True)
            session = self._build_session(Path(td))
            calls = []

            def fake_request(config, method, path, *, headers, params=None, json_body=None, timeout_seconds=None):
                calls.append((method, path))
                if path == "/content/home" and len(calls) == 1:
                    raise HidiveAuthError("HIDIVE GET /content/home failed: HTTP 401: expired")
                if path == "/token/refresh":
                    raise HidiveAuthError("refresh blocked: HTTP 401 invalid token")
                return {"ok": True}

            with patch("mal_updater.hidive_auth._hidive_json_request", side_effect=fake_request), patch(
                "mal_updater.hidive_auth.hidive_login_with_credentials"
            ) as mock_login:
                mock_login.return_value = type("Bootstrap", (), {
                    "authorisation_token": "credential-access",
                    "refresh_token": "credential-refresh",
                    "account_id": "acct-123",
                    "account_name": "example-user",
                })()
                payload = session.json_get("/content/home")

            self.assertEqual(payload, {"ok": True})
            self.assertEqual(session.token.authorisation_token, "credential-access")
            self.assertEqual(session.token.refresh_token, "credential-refresh")
            self.assertTrue(session.credential_rebootstrap_attempted)


if __name__ == "__main__":
    unittest.main()
