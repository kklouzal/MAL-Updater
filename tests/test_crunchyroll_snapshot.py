from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from mal_updater.config import load_config
from mal_updater.crunchyroll_auth import resolve_crunchyroll_state_paths
from mal_updater.crunchyroll_snapshot import (
    CRUNCHYROLL_ME_URL,
    CrunchyrollAccessToken,
    _CrunchyrollAuthSession,
    _CrunchyrollRequestPacer,
    _fetch_snapshot_once,
    _write_sync_boundary,
)


class CrunchyrollSnapshotBoundaryTests(unittest.TestCase):
    def _build_session(self, root: Path) -> _CrunchyrollAuthSession:
        config = load_config(root)
        state_paths = resolve_crunchyroll_state_paths(config)
        return _CrunchyrollAuthSession(
            config=config,
            profile="default",
            timeout_seconds=5.0,
            pacer=_CrunchyrollRequestPacer(0.0, 0.0),
            state_paths=state_paths,
            token=CrunchyrollAccessToken(
                access_token="access-token",
                refresh_token="refresh-token",
                account_id="acct-123",
                device_id="device-123",
                device_type="ANDROIDTV",
            ),
            auth_source="refresh_token",
        )

    def _history_entry(self, idx: int, *, played_at: str | None = None) -> dict[str, object]:
        return {
            "date_played": played_at or f"2026-03-14T20:{idx % 60:02d}:00Z",
            "playhead": 1_440_000,
            "fully_watched": True,
            "panel": {
                "type": "episode",
                "id": f"episode-{idx}",
                "title": f"Episode {idx}",
                "episode_metadata": {
                    "series_id": "series-123",
                    "series_title": "Example Show",
                    "season_title": "Example Show",
                    "season_number": 1,
                    "episode_number": idx,
                    "duration_ms": 1_440_000,
                    "audio_locale": "en-US",
                    "subtitle_locales": ["en-US"],
                },
            },
        }

    def _watchlist_entry(self, idx: int, *, added_at: str | None = None) -> dict[str, object]:
        return {
            "date_added": added_at or f"2026-03-14T18:{idx % 60:02d}:00Z",
            "never_watched": idx % 2 == 0,
            "fully_watched": False,
            "panel": {
                "type": "series",
                "id": f"watch-{idx}",
                "title": f"Watchlist Show {idx}",
            },
        }

    def test_fetch_snapshot_uses_incremental_boundary_to_stop_history_and_watchlist_early(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / "config").mkdir()
            session = self._build_session(root)

            previous_history = [self._history_entry(500), self._history_entry(499)]
            previous_watchlist = [self._watchlist_entry(700), self._watchlist_entry(699)]
            _write_sync_boundary(
                state_paths=session.state_paths,
                generated_at="2026-03-14T19:00:00Z",
                account_id_hint="acct-123",
                history_entries=previous_history,
                watchlist_entries=previous_watchlist,
            )

            history_page = [self._history_entry(idx) for idx in range(1, 100)] + [previous_history[0]]
            watchlist_page = [self._watchlist_entry(idx) for idx in range(1, 100)] + [previous_watchlist[0]]
            calls: list[tuple[str, dict[str, object] | None]] = []

            def fake_get(url: str, *, params: dict[str, object] | None = None):
                calls.append((url, params))
                if url == CRUNCHYROLL_ME_URL:
                    return {"account_id": "acct-123", "email": "user@example.com"}
                if url.endswith("/watch-history"):
                    return {"total": 350, "data": history_page}
                if url.endswith("/watchlist"):
                    return {"total": 250, "data": watchlist_page}
                raise AssertionError(url)

            with patch.object(_CrunchyrollAuthSession, "authorized_json_get", side_effect=fake_get):
                result = _fetch_snapshot_once(session, use_incremental_boundary=True)

            watch_history_calls = [item for item in calls if item[0].endswith("/watch-history")]
            watchlist_calls = [item for item in calls if item[0].endswith("/watchlist")]
            self.assertEqual(len(watch_history_calls), 1)
            self.assertEqual(len(watchlist_calls), 1)
            self.assertTrue(result.snapshot.raw["history_stopped_early"])
            self.assertTrue(result.snapshot.raw["watchlist_stopped_early"])
            self.assertEqual(result.snapshot.raw["sync_boundary_mode"], "incremental")
            self.assertTrue(result.state_paths.sync_boundary_path.exists())
            saved = json.loads(result.state_paths.sync_boundary_path.read_text(encoding="utf-8"))
            self.assertEqual(saved["account_id_hint"], "acct-123")
            self.assertGreaterEqual(saved["history"]["retained_count"], 1)
            self.assertGreaterEqual(saved["watchlist"]["retained_count"], 1)

    def test_fetch_snapshot_full_refresh_ignores_existing_boundary(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / "config").mkdir()
            session = self._build_session(root)

            previous_history = [self._history_entry(500)]
            previous_watchlist = [self._watchlist_entry(700)]
            _write_sync_boundary(
                state_paths=session.state_paths,
                generated_at="2026-03-14T19:00:00Z",
                account_id_hint="acct-123",
                history_entries=previous_history,
                watchlist_entries=previous_watchlist,
            )

            history_page_1 = [self._history_entry(idx) for idx in range(1, 100)] + [previous_history[0]]
            history_page_2 = [self._history_entry(idx) for idx in range(101, 151)]
            watchlist_page_1 = [self._watchlist_entry(idx) for idx in range(1, 100)] + [previous_watchlist[0]]
            watchlist_page_2 = [self._watchlist_entry(idx) for idx in range(101, 121)]
            calls: list[tuple[str, dict[str, object] | None]] = []

            def fake_get(url: str, *, params: dict[str, object] | None = None):
                calls.append((url, params))
                if url == CRUNCHYROLL_ME_URL:
                    return {"account_id": "acct-123", "email": "user@example.com"}
                if url.endswith("/watch-history"):
                    if params and params.get("page") == 1:
                        return {"total": 150, "data": history_page_1}
                    if params and params.get("page") == 2:
                        return {"total": 150, "data": history_page_2}
                if url.endswith("/watchlist"):
                    if params and params.get("start") == 0:
                        return {"total": 120, "data": watchlist_page_1}
                    if params and params.get("start") == 100:
                        return {"total": 120, "data": watchlist_page_2}
                raise AssertionError((url, params))

            with patch.object(_CrunchyrollAuthSession, "authorized_json_get", side_effect=fake_get):
                result = _fetch_snapshot_once(session, use_incremental_boundary=False)

            watch_history_calls = [item for item in calls if item[0].endswith("/watch-history")]
            watchlist_calls = [item for item in calls if item[0].endswith("/watchlist")]
            self.assertEqual(len(watch_history_calls), 2)
            self.assertEqual(len(watchlist_calls), 2)
            self.assertFalse(result.snapshot.raw["history_stopped_early"])
            self.assertFalse(result.snapshot.raw["watchlist_stopped_early"])
            self.assertEqual(result.snapshot.raw["sync_boundary_mode"], "full_refresh")


if __name__ == "__main__":
    unittest.main()
