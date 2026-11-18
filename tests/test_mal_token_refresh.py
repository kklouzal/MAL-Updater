from __future__ import annotations

import io
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch
from urllib.error import HTTPError

from mal_updater import auth
from mal_updater.auth import persist_token_response, refresh_mal_tokens_if_due
from mal_updater.config import AppConfig, MalSecrets, MalSettings
from mal_updater.mal_client import (
    MalApiError,
    MalClient,
    TokenResponse,
    mal_access_token_state_path,
    read_mal_access_token_expiry,
)

NOW = datetime(2026, 11, 18, 12, 0, tzinfo=timezone.utc)


def _secrets(root: Path, *, access_token: str | None = None, refresh_token: str | None = "refresh-old") -> MalSecrets:
    return MalSecrets(
        client_id="client-id",
        client_secret=None,
        access_token=access_token,
        refresh_token=refresh_token,
        client_id_path=root / "mal_client_id.txt",
        client_secret_path=root / "mal_client_secret.txt",
        access_token_path=root / "mal_access_token.txt",
        refresh_token_path=root / "mal_refresh_token.txt",
    )


def _token(access: str = "access-new", refresh: str | None = "refresh-new", expires_in: int | None = 2678400) -> TokenResponse:
    return TokenResponse(access_token=access, token_type="Bearer", expires_in=expires_in, refresh_token=refresh, scope=None, raw={})


class MalTokenPersistenceTests(unittest.TestCase):
    def test_refresh_token_is_written_before_access_token_and_expiry_last(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            secrets = _secrets(Path(td))
            order: list[str] = []
            real_write = auth.write_secret_file

            def recording_write(path: Path, value: str) -> None:
                order.append(path.name)
                real_write(path, value)

            with patch("mal_updater.auth.write_secret_file", side_effect=recording_write), patch(
                "mal_updater.auth.write_mal_access_token_state",
                side_effect=lambda *a, **k: order.append("state"),
            ):
                persist_token_response(_token(), secrets, obtained_at=NOW)
            self.assertEqual(["mal_refresh_token.txt", "mal_access_token.txt", "state"], order)

    def test_crash_after_refresh_token_write_reads_as_unknown_expiry(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            persist_token_response(_token("access-1", "refresh-1"), _secrets(root), obtained_at=NOW)
            self.assertIsNotNone(read_mal_access_token_expiry(_secrets(root, access_token="access-1")))
            real_write = auth.write_secret_file

            def crash_on_access(path: Path, value: str) -> None:
                if path.name == "mal_access_token.txt":
                    raise OSError("simulated crash")
                real_write(path, value)

            with patch("mal_updater.auth.write_secret_file", side_effect=crash_on_access):
                with self.assertRaises(OSError):
                    persist_token_response(_token("access-2", "refresh-2"), _secrets(root), obtained_at=NOW)
            # New refresh token is durable; the old access token no longer has a
            # recorded expiry, so the next run refreshes with the new refresh token.
            self.assertEqual("refresh-2\n", (root / "mal_refresh_token.txt").read_text(encoding="utf-8"))
            self.assertIsNone(read_mal_access_token_expiry(_secrets(root, access_token="access-1")))

    def test_expiry_record_is_bound_to_the_access_token(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            persist_token_response(_token("access-1"), _secrets(root), obtained_at=NOW)
            self.assertEqual(NOW + timedelta(seconds=2678400), read_mal_access_token_expiry(_secrets(root, access_token="access-1")))
            self.assertIsNone(read_mal_access_token_expiry(_secrets(root, access_token="operator-replaced")))
            self.assertNotIn("access-1", mal_access_token_state_path(_secrets(root)).read_text(encoding="utf-8"))


class MalTokenRefreshPolicyTests(unittest.TestCase):
    def _client(self, token: TokenResponse | None = None) -> MagicMock:
        client = MagicMock(spec=MalClient)
        client.refresh_access_token.return_value = token or _token()
        return client

    def test_skips_network_refresh_when_access_token_is_not_near_expiry(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            persist_token_response(_token("access-1"), _secrets(root), obtained_at=NOW)
            client = self._client()
            outcome = refresh_mal_tokens_if_due(client, _secrets(root, access_token="access-1"), now=NOW + timedelta(days=1))
            client.refresh_access_token.assert_not_called()
            self.assertFalse(outcome.refreshed)
            self.assertEqual("ok", outcome.as_dict()["status"])
            self.assertEqual("access_token_not_near_expiry", outcome.reason)

    def test_refreshes_inside_margin_when_forced_and_when_expiry_unknown(self) -> None:
        cases = {
            "near": (dict(now=NOW + timedelta(days=30)), "access_token_near_expiry"),
            "forced": (dict(now=NOW + timedelta(days=1), force=True), "forced"),
        }
        for name, (kwargs, reason) in cases.items():
            with self.subTest(name), tempfile.TemporaryDirectory() as td:
                root = Path(td)
                persist_token_response(_token("access-1"), _secrets(root), obtained_at=NOW)
                client = self._client(_token("access-2", "refresh-2"))
                outcome = refresh_mal_tokens_if_due(client, _secrets(root, access_token="access-1"), **kwargs)
                client.refresh_access_token.assert_called_once()
                self.assertTrue(outcome.refreshed)
                self.assertEqual(reason, outcome.reason)
                self.assertEqual("refresh-2\n", (root / "mal_refresh_token.txt").read_text(encoding="utf-8"))
                self.assertIsNotNone(read_mal_access_token_expiry(_secrets(root, access_token="access-2")))
        with tempfile.TemporaryDirectory() as td:
            client = self._client()
            outcome = refresh_mal_tokens_if_due(client, _secrets(Path(td), access_token="legacy-access"), now=NOW)
            client.refresh_access_token.assert_called_once()
            self.assertEqual("expiry_unknown", outcome.reason)

    def test_api_401_for_current_bearer_voids_expiry_so_next_run_refreshes(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            config = AppConfig(
                project_root=root, workspace_root=root, runtime_root=root, settings_path=root / "settings.toml",
                config_dir=root, secrets_dir=root, data_dir=root, state_dir=root, cache_dir=root,
                db_path=root / "db.sqlite3",
                mal=MalSettings(request_spacing_seconds=0.0, request_spacing_jitter_seconds=0.0),
            )
            persist_token_response(_token("access-1"), _secrets(root), obtained_at=NOW)
            secrets = _secrets(root, access_token="access-1")
            client = MalClient(config, secrets)

            def unauthorized(request, timeout):  # type: ignore[no-untyped-def]
                raise HTTPError(request.full_url, 401, "Unauthorized", {}, io.BytesIO(b"{}"))

            with patch("mal_updater.mal_client.urlopen", unauthorized):
                with self.assertRaisesRegex(MalApiError, "HTTP 401"):
                    client.get_my_user(max_attempts=1)
            self.assertIsNone(read_mal_access_token_expiry(secrets))
            refresher = self._client()
            outcome = refresh_mal_tokens_if_due(refresher, secrets, now=NOW + timedelta(days=1))
            self.assertTrue(outcome.refreshed)
            self.assertEqual("expiry_unknown", outcome.reason)


if __name__ == "__main__":
    unittest.main()
