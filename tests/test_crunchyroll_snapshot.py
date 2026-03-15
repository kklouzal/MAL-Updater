from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import call, patch

from mal_updater.config import load_config
from mal_updater import crunchyroll_snapshot as crunchyroll_snapshot_module
from mal_updater.crunchyroll_snapshot import fetch_snapshot, refresh_access_token, snapshot_to_dict


class _FakeResponse:
    def __init__(self, status_code: int, payload):
        self.status_code = status_code
        self._payload = payload

    def json(self):
        return self._payload


class CrunchyrollSnapshotTests(unittest.TestCase):
    def test_refresh_access_token_persists_rotated_refresh_token(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / "config").mkdir()
            (root / "state" / "crunchyroll" / "default").mkdir(parents=True)
            (root / "state" / "crunchyroll" / "default" / "refresh_token.txt").write_text("refresh-old\n", encoding="utf-8")
            (root / "state" / "crunchyroll" / "default" / "device_id.txt").write_text("device-123\n", encoding="utf-8")
            config = load_config(root)

            with patch("mal_updater.crunchyroll_snapshot._http_post") as mock_post:
                mock_post.return_value = _FakeResponse(
                    200,
                    {
                        "access_token": "access-1",
                        "refresh_token": "refresh-new",
                        "account_id": "acct-1",
                    },
                )
                token, state_paths = refresh_access_token(config)

            self.assertEqual(token.access_token, "access-1")
            self.assertEqual(token.refresh_token, "refresh-new")
            self.assertEqual(state_paths.refresh_token_path.read_text(encoding="utf-8"), "refresh-new\n")

    def test_fetch_snapshot_normalizes_history_and_watchlist(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / "config").mkdir()
            (root / "config" / "settings.toml").write_text("[crunchyroll]\nrequest_spacing_seconds = 0\n", encoding="utf-8")
            (root / "state" / "crunchyroll" / "default").mkdir(parents=True)
            (root / "state" / "crunchyroll" / "default" / "refresh_token.txt").write_text("refresh-old\n", encoding="utf-8")
            (root / "state" / "crunchyroll" / "default" / "device_id.txt").write_text("device-123\n", encoding="utf-8")
            config = load_config(root)

            history_payload = {
                "total": 1,
                "data": [
                    {
                        "id": "ep-1",
                        "date_played": "2026-03-14T22:10:00Z",
                        "playhead": 1200000,
                        "fully_watched": False,
                        "panel": {
                            "type": "episode",
                            "id": "ep-1",
                            "series_id": "series-1",
                            "series_title": "Example Show",
                            "season_title": "Season 1",
                            "season_number": 1,
                            "episode_number": 3,
                            "title": "Episode 3",
                            "duration_ms": 1440000,
                            "audio_locale": "en-US",
                            "subtitle_locales": ["en-US"],
                        },
                    }
                ],
            }
            watchlist_payload = {
                "total": 1,
                "data": [
                    {
                        "never_watched": False,
                        "fully_watched": False,
                        "date_added": "2026-03-10T12:00:00Z",
                        "panel": {
                            "type": "series",
                            "id": "series-1",
                            "title": "Example Show",
                        },
                    }
                ]
            }

            with patch("mal_updater.crunchyroll_snapshot._http_post") as mock_post, patch(
                "mal_updater.crunchyroll_snapshot._http_get"
            ) as mock_get:
                mock_post.return_value = _FakeResponse(
                    200,
                    {
                        "access_token": "access-1",
                        "refresh_token": "refresh-new",
                        "account_id": "acct-1",
                    },
                )
                mock_get.side_effect = [
                    _FakeResponse(200, {"account_id": "acct-1", "email": "user@example.com"}),
                    _FakeResponse(200, history_payload),
                    _FakeResponse(200, watchlist_payload),
                ]

                result = fetch_snapshot(config)

            payload = snapshot_to_dict(result.snapshot)
            self.assertEqual(payload["provider"], "crunchyroll")
            self.assertEqual(payload["account_id_hint"], "acct-1")
            self.assertEqual(len(payload["series"]), 1)
            self.assertEqual(payload["series"][0]["provider_series_id"], "series-1")
            self.assertEqual(len(payload["progress"]), 1)
            self.assertAlmostEqual(payload["progress"][0]["completion_ratio"], 1200000 / 1440000)
            self.assertEqual(len(payload["watchlist"]), 1)
            self.assertEqual(payload["watchlist"][0]["status"], "in_progress")
            self.assertEqual(payload["raw"]["history_count"], 1)
            self.assertEqual(payload["raw"]["watchlist_count"], 1)

    def test_fetch_snapshot_paginates_watchlist_with_n_and_start(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / "config").mkdir()
            (root / "config" / "settings.toml").write_text("[crunchyroll]\nrequest_spacing_seconds = 0\n", encoding="utf-8")
            (root / "state" / "crunchyroll" / "default").mkdir(parents=True)
            (root / "state" / "crunchyroll" / "default" / "refresh_token.txt").write_text("refresh-old\n", encoding="utf-8")
            (root / "state" / "crunchyroll" / "default" / "device_id.txt").write_text("device-123\n", encoding="utf-8")
            config = load_config(root)

            history_payload = {"total": 0, "data": []}
            watchlist_page_1 = {
                "total": 197,
                "data": [
                    {"never_watched": False, "fully_watched": False, "panel": {"type": "series", "id": f"series-{index}", "title": f"Show {index}"}}
                    for index in range(100)
                ],
            }
            watchlist_page_2 = {
                "total": 197,
                "data": [
                    {"never_watched": False, "fully_watched": False, "panel": {"type": "series", "id": f"series-{index}", "title": f"Show {index}"}}
                    for index in range(100, 197)
                ],
            }

            with patch("mal_updater.crunchyroll_snapshot._http_post") as mock_post, patch(
                "mal_updater.crunchyroll_snapshot._http_get"
            ) as mock_get:
                mock_post.return_value = _FakeResponse(
                    200,
                    {"access_token": "access-1", "refresh_token": "refresh-new", "account_id": "acct-1"},
                )
                mock_get.side_effect = [
                    _FakeResponse(200, {"account_id": "acct-1", "email": "user@example.com"}),
                    _FakeResponse(200, history_payload),
                    _FakeResponse(200, watchlist_page_1),
                    _FakeResponse(200, watchlist_page_2),
                ]

                result = fetch_snapshot(config)

            payload = snapshot_to_dict(result.snapshot)
            self.assertEqual(len(payload["watchlist"]), 197)
            self.assertEqual(payload["raw"]["watchlist_count"], 197)
            self.assertEqual(mock_get.call_args_list[2].kwargs["params"]["n"], 100)
            self.assertEqual(mock_get.call_args_list[2].kwargs["params"]["start"], 0)
            self.assertEqual(mock_get.call_args_list[3].kwargs["params"]["start"], 100)



    def test_fetch_snapshot_rebootstraps_after_watch_history_401(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / "config").mkdir()
            (root / "config" / "settings.toml").write_text("[crunchyroll]\nrequest_spacing_seconds = 0\n", encoding="utf-8")
            (root / "state" / "crunchyroll" / "default").mkdir(parents=True)
            (root / "state" / "crunchyroll" / "default" / "refresh_token.txt").write_text("refresh-old\n", encoding="utf-8")
            (root / "state" / "crunchyroll" / "default" / "device_id.txt").write_text("device-123\n", encoding="utf-8")
            (root / "secrets").mkdir()
            (root / "secrets" / "crunchyroll_username.txt").write_text("user@example.com\n", encoding="utf-8")
            (root / "secrets" / "crunchyroll_password.txt").write_text("super-secret\n", encoding="utf-8")
            config = load_config(root)

            state_paths = crunchyroll_snapshot_module.resolve_crunchyroll_state_paths(config)
            (root / "state" / "crunchyroll" / "default" / "refresh_token.txt").write_text("refresh-2\n", encoding="utf-8")
            with patch(
                "mal_updater.crunchyroll_snapshot.refresh_access_token",
                return_value=(
                    crunchyroll_snapshot_module.CrunchyrollAccessToken(
                        access_token="access-refresh",
                        refresh_token="refresh-1",
                        account_id="acct-1",
                        device_id="device-123",
                        device_type="ANDROIDTV",
                    ),
                    state_paths,
                ),
            ) as refresh_mock, patch(
                "mal_updater.crunchyroll_snapshot.crunchyroll_login_with_credentials",
                return_value=SimpleNamespace(
                    access_token="access-bootstrap",
                    refresh_token="refresh-2",
                    account_id="acct-1",
                    account_email="user@example.com",
                    device_id="device-123",
                    device_type="ANDROIDTV",
                ),
            ) as bootstrap_mock, patch("mal_updater.crunchyroll_snapshot._http_get") as mock_get:
                mock_get.side_effect = [
                    _FakeResponse(200, {"account_id": "acct-1", "email": "user@example.com"}),
                    _FakeResponse(401, {"message": "Unauthorized"}),
                    _FakeResponse(200, {"total": 0, "data": []}),
                    _FakeResponse(200, {"total": 0, "data": []}),
                ]

                result = fetch_snapshot(config)

            self.assertEqual(result.snapshot.raw["auth_source"], "credential_rebootstrap")
            refresh_mock.assert_called_once()
            bootstrap_mock.assert_called_once()
            self.assertEqual((root / "state" / "crunchyroll" / "default" / "refresh_token.txt").read_text(encoding="utf-8"), "refresh-2\n")

    def test_fetch_snapshot_resumes_same_history_page_after_midrun_401(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / "config").mkdir()
            (root / "config" / "settings.toml").write_text("[crunchyroll]\nrequest_spacing_seconds = 0\n", encoding="utf-8")
            (root / "state" / "crunchyroll" / "default").mkdir(parents=True)
            (root / "state" / "crunchyroll" / "default" / "refresh_token.txt").write_text("refresh-old\n", encoding="utf-8")
            (root / "state" / "crunchyroll" / "default" / "device_id.txt").write_text("device-123\n", encoding="utf-8")
            (root / "secrets").mkdir()
            (root / "secrets" / "crunchyroll_username.txt").write_text("user@example.com\n", encoding="utf-8")
            (root / "secrets" / "crunchyroll_password.txt").write_text("super-secret\n", encoding="utf-8")
            config = load_config(root)

            history_page_1 = {
                "total": 150,
                "data": [
                    {
                        "id": f"ep-{index}",
                        "date_played": "2026-03-14T22:10:00Z",
                        "playhead": 1200000,
                        "fully_watched": True,
                        "panel": {
                            "type": "episode",
                            "id": f"ep-{index}",
                            "series_id": "series-1",
                            "series_title": "Example Show",
                            "season_title": "Season 1",
                            "season_number": 1,
                            "episode_number": index,
                            "title": f"Episode {index}",
                            "duration_ms": 1440000,
                            "audio_locale": "en-US",
                            "subtitle_locales": ["en-US"],
                        },
                    }
                    for index in range(1, 101)
                ],
            }
            history_page_2 = {
                "total": 150,
                "data": [
                    {
                        "id": f"ep-{index}",
                        "date_played": "2026-03-15T00:10:00Z",
                        "playhead": 1200000,
                        "fully_watched": True,
                        "panel": {
                            "type": "episode",
                            "id": f"ep-{index}",
                            "series_id": "series-1",
                            "series_title": "Example Show",
                            "season_title": "Season 1",
                            "season_number": 1,
                            "episode_number": index,
                            "title": f"Episode {index}",
                            "duration_ms": 1440000,
                            "audio_locale": "en-US",
                            "subtitle_locales": ["en-US"],
                        },
                    }
                    for index in range(101, 151)
                ],
            }

            state_paths = crunchyroll_snapshot_module.resolve_crunchyroll_state_paths(config)
            with patch(
                "mal_updater.crunchyroll_snapshot.refresh_access_token",
                return_value=(
                    crunchyroll_snapshot_module.CrunchyrollAccessToken(
                        access_token="access-refresh",
                        refresh_token="refresh-1",
                        account_id="acct-1",
                        device_id="device-123",
                        device_type="ANDROIDTV",
                    ),
                    state_paths,
                ),
            ) as refresh_mock, patch(
                "mal_updater.crunchyroll_snapshot.crunchyroll_login_with_credentials",
                return_value=SimpleNamespace(
                    access_token="access-bootstrap",
                    refresh_token="refresh-2",
                    account_id="acct-1",
                    account_email="user@example.com",
                    device_id="device-123",
                    device_type="ANDROIDTV",
                ),
            ) as bootstrap_mock, patch("mal_updater.crunchyroll_snapshot._http_get") as mock_get:
                mock_get.side_effect = [
                    _FakeResponse(200, {"account_id": "acct-1", "email": "user@example.com"}),
                    _FakeResponse(200, history_page_1),
                    _FakeResponse(401, {"message": "Unauthorized"}),
                    _FakeResponse(200, history_page_2),
                    _FakeResponse(200, {"total": 0, "data": []}),
                ]

                result = fetch_snapshot(config)

            payload = snapshot_to_dict(result.snapshot)
            self.assertEqual(payload["raw"]["auth_source"], "credential_rebootstrap")
            self.assertEqual(payload["raw"]["history_count"], 150)
            self.assertEqual(len(payload["progress"]), 150)
            refresh_mock.assert_called_once()
            bootstrap_mock.assert_called_once()
            self.assertEqual(mock_get.call_args_list[2].kwargs["params"]["page"], 2)
            self.assertEqual(mock_get.call_args_list[3].kwargs["params"]["page"], 2)

    def test_fetch_snapshot_rebootstraps_after_refresh_token_failure(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / "config").mkdir()
            (root / "config" / "settings.toml").write_text("[crunchyroll]\nrequest_spacing_seconds = 0\n", encoding="utf-8")
            (root / "state" / "crunchyroll" / "default").mkdir(parents=True)
            (root / "state" / "crunchyroll" / "default" / "refresh_token.txt").write_text("refresh-old\n", encoding="utf-8")
            (root / "state" / "crunchyroll" / "default" / "device_id.txt").write_text("device-123\n", encoding="utf-8")
            (root / "secrets").mkdir()
            (root / "secrets" / "crunchyroll_username.txt").write_text("user@example.com\n", encoding="utf-8")
            (root / "secrets" / "crunchyroll_password.txt").write_text("super-secret\n", encoding="utf-8")
            config = load_config(root)

            with patch(
                "mal_updater.crunchyroll_snapshot.refresh_access_token",
                side_effect=crunchyroll_snapshot_module.CrunchyrollAuthError("expired refresh token"),
            ) as refresh_mock, patch(
                "mal_updater.crunchyroll_snapshot.crunchyroll_login_with_credentials",
                return_value=SimpleNamespace(
                    access_token="access-bootstrap",
                    refresh_token="refresh-2",
                    account_id="acct-1",
                    account_email="user@example.com",
                    device_id="device-123",
                    device_type="ANDROIDTV",
                ),
            ) as bootstrap_mock, patch("mal_updater.crunchyroll_snapshot._http_get") as mock_get:
                mock_get.side_effect = [
                    _FakeResponse(200, {"account_id": "acct-1", "email": "user@example.com"}),
                    _FakeResponse(200, {"total": 0, "data": []}),
                    _FakeResponse(200, {"total": 0, "data": []}),
                ]

                result = fetch_snapshot(config)

            self.assertEqual(result.snapshot.raw["auth_source"], "credential_rebootstrap")
            refresh_mock.assert_called_once()
            bootstrap_mock.assert_called_once()

    def test_fetch_snapshot_respects_configured_request_spacing_with_jitter(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / "config").mkdir()
            (root / "config" / "settings.toml").write_text(
                "[crunchyroll]\nrequest_spacing_seconds = 10\nrequest_spacing_jitter_seconds = 3\n",
                encoding="utf-8",
            )
            (root / "state" / "crunchyroll" / "default").mkdir(parents=True)
            (root / "state" / "crunchyroll" / "default" / "refresh_token.txt").write_text("refresh-old\n", encoding="utf-8")
            (root / "state" / "crunchyroll" / "default" / "device_id.txt").write_text("device-123\n", encoding="utf-8")
            config = load_config(root)

            with patch("mal_updater.crunchyroll_snapshot._http_post") as mock_post, patch(
                "mal_updater.crunchyroll_snapshot._http_get"
            ) as mock_get, patch.object(crunchyroll_snapshot_module.random, "uniform", side_effect=[13.0, 9.0, 12.0, 7.0]), patch(
                "mal_updater.crunchyroll_snapshot.time.sleep"
            ) as sleep_mock, patch(
                "mal_updater.crunchyroll_snapshot.time.monotonic",
                side_effect=[0.0, 1.0, 13.0, 15.0, 24.0, 26.0, 33.0],
            ):
                mock_post.return_value = _FakeResponse(
                    200,
                    {"access_token": "access-1", "refresh_token": "refresh-new", "account_id": "acct-1"},
                )
                mock_get.side_effect = [
                    _FakeResponse(200, {"account_id": "acct-1", "email": "user@example.com"}),
                    _FakeResponse(200, {"total": 0, "data": []}),
                    _FakeResponse(200, {"total": 0, "data": []}),
                ]

                result = fetch_snapshot(config)

            self.assertEqual(result.snapshot.raw["request_spacing_seconds"], 10.0)
            self.assertEqual(result.snapshot.raw["request_spacing_jitter_seconds"], 3.0)
            self.assertEqual(sleep_mock.call_args_list, [call(8.0), call(10.0), call(5.0)])


if __name__ == "__main__":
    unittest.main()
