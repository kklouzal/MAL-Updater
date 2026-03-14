from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from mal_updater.config import load_config
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


if __name__ == "__main__":
    unittest.main()
