from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from mal_updater.config import load_config
from mal_updater.contracts import EpisodeProgress, ProviderSnapshot, SeriesRef, WatchlistEntry
import mal_updater.crunchyroll_snapshot as crunchyroll_snapshot
from mal_updater.crunchyroll_auth import resolve_crunchyroll_state_paths
from mal_updater.crunchyroll_auth import CrunchyrollAuthError, CrunchyrollBootstrapResult
from mal_updater.provider_snapshot import snapshot_to_dict as shared_snapshot_to_dict
from mal_updater.provider_snapshot import write_snapshot_file as shared_write_snapshot_file
from mal_updater.crunchyroll_snapshot import (
    CRUNCHYROLL_ME_URL,
    CrunchyrollAccessToken,
    CrunchyrollSnapshotError,
    CrunchyrollUnauthorizedError,
    _CrunchyrollAuthSession,
    _authorized_json_get,
    _CrunchyrollRequestPacer,
    _fetch_snapshot_once,
    _write_sync_boundary,
)


class CrunchyrollAuthRecoveryTests(unittest.TestCase):
    def test_crunchyroll_snapshot_http_requires_curl_cffi_transport(self) -> None:
        with patch.object(crunchyroll_snapshot, "curl_requests", None):
            with self.assertRaisesRegex(CrunchyrollSnapshotError, "requires curl_cffi"):
                crunchyroll_snapshot._http_get(
                    "https://example.invalid/history",
                    headers={},
                    timeout_seconds=1.0,
                )

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

    def _sample_snapshot(self) -> ProviderSnapshot:
        return ProviderSnapshot(
            contract_version="1.0",
            generated_at="2026-07-23T00:00:00Z",
            provider="crunchyroll",
            account_id_hint="acct-123",
            series=[SeriesRef(provider_series_id="SERIES1", title="Example", season_title="Example Season", season_number=1)],
            progress=[
                EpisodeProgress(
                    provider_episode_id="EP1",
                    provider_series_id="SERIES1",
                    episode_number=1,
                    episode_title="Episode One",
                    playback_position_ms=120000,
                    duration_ms=1440000,
                    completion_ratio=0.5,
                    last_watched_at="2026-07-22T00:00:00Z",
                    audio_locale="ja-JP",
                    subtitle_locale="en-US",
                    rating="TV-14",
                )
            ],
            watchlist=[WatchlistEntry(provider_series_id="SERIES1", added_at="2026-07-21T00:00:00Z", status="current")],
            raw={"supports": {"history": True}, "nested": {"value": 1}},
        )

    def test_module_snapshot_serializer_matches_shared_exact_shape(self) -> None:
        snapshot = self._sample_snapshot()

        payload = crunchyroll_snapshot.snapshot_to_dict(snapshot)

        self.assertEqual(shared_snapshot_to_dict(snapshot), payload)
        self.assertEqual(
            ["contract_version", "generated_at", "provider", "account_id_hint", "series", "progress", "watchlist", "raw"],
            list(payload.keys()),
        )
        self.assertEqual("crunchyroll", payload["provider"])
        self.assertEqual("SERIES1", payload["series"][0]["provider_series_id"])
        self.assertEqual("EP1", payload["progress"][0]["provider_episode_id"])

    def test_module_snapshot_writer_matches_shared_output(self) -> None:
        snapshot = self._sample_snapshot()
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            module_path = root / "module" / "snapshot.json"
            shared_path = root / "shared" / "snapshot.json"

            returned_path = crunchyroll_snapshot.write_snapshot_file(module_path, snapshot)
            shared_write_snapshot_file(shared_path, snapshot)

            self.assertEqual(module_path, returned_path)
            self.assertEqual(shared_path.read_text(encoding="utf-8"), module_path.read_text(encoding="utf-8"))
            self.assertTrue(module_path.read_text(encoding="utf-8").endswith("\n"))

    def test_authorized_json_get_recovers_401_by_refreshing_access_token(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / ".MAL-Updater" / "config").mkdir(parents=True)
            session = self._build_session(root)
            calls: list[str] = []

            def fake_authorized_get(url: str, *, access_token: str, timeout_seconds: float, params=None, pacer=None, phase=None):
                calls.append(access_token)
                if len(calls) == 1:
                    raise CrunchyrollUnauthorizedError(url, 401)
                return {"ok": True, "token_used": access_token}

            with patch("mal_updater.crunchyroll_snapshot._authorized_json_get", side_effect=fake_authorized_get), patch(
                "mal_updater.crunchyroll_snapshot.refresh_access_token"
            ) as mock_refresh, patch("mal_updater.crunchyroll_snapshot.crunchyroll_login_with_credentials") as mock_login:
                mock_refresh.return_value = (
                    CrunchyrollAccessToken(
                        access_token="refreshed-access-token",
                        refresh_token="refreshed-refresh-token",
                        account_id="acct-123",
                        device_id="device-123",
                        device_type="ANDROIDTV",
                    ),
                    session.state_paths,
                )

                payload = session.authorized_json_get("https://example.invalid/history")

            self.assertEqual(payload["token_used"], "refreshed-access-token")
            self.assertEqual(calls, ["access-token", "refreshed-access-token"])
            self.assertEqual(session.auth_source, "refresh_token_recovery")
            self.assertFalse(session.credential_rebootstrap_attempted)
            mock_refresh.assert_called_once()
            mock_login.assert_not_called()

    def test_authorized_json_get_falls_back_to_credentials_when_refresh_recovery_fails(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / ".MAL-Updater" / "config").mkdir(parents=True)
            session = self._build_session(root)
            calls: list[str] = []

            def fake_authorized_get(url: str, *, access_token: str, timeout_seconds: float, params=None, pacer=None, phase=None):
                calls.append(access_token)
                if len(calls) == 1:
                    raise CrunchyrollUnauthorizedError(url, 401)
                return {"ok": True, "token_used": access_token}

            with patch("mal_updater.crunchyroll_snapshot._authorized_json_get", side_effect=fake_authorized_get), patch(
                "mal_updater.crunchyroll_snapshot.refresh_access_token",
                side_effect=CrunchyrollAuthError("refresh failed"),
            ) as mock_refresh, patch(
                "mal_updater.crunchyroll_snapshot.crunchyroll_login_with_credentials"
            ) as mock_login:
                mock_login.return_value = CrunchyrollBootstrapResult(
                    profile="default",
                    locale="en-US",
                    username_path=root / ".MAL-Updater" / "secrets" / "crunchyroll_username.txt",
                    password_path=root / ".MAL-Updater" / "secrets" / "crunchyroll_password.txt",
                    refresh_token_path=session.state_paths.refresh_token_path,
                    device_id_path=session.state_paths.device_id_path,
                    session_state_path=session.state_paths.session_state_path,
                    account_id="acct-123",
                    account_email="user@example.com",
                    access_token="credential-access-token",
                    refresh_token="credential-refresh-token",
                    device_id="device-123",
                    device_type="ANDROIDTV",
                )

                payload = session.authorized_json_get("https://example.invalid/history")

            self.assertEqual(payload["token_used"], "credential-access-token")
            self.assertEqual(calls, ["access-token", "credential-access-token"])
            self.assertEqual(session.auth_source, "credential_rebootstrap")
            self.assertTrue(session.credential_rebootstrap_attempted)
            mock_refresh.assert_called_once()
            mock_login.assert_called_once()

    def test_authorized_json_get_stops_cleanly_after_refresh_and_credential_retry_are_exhausted(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / ".MAL-Updater" / "config").mkdir(parents=True)
            session = self._build_session(root)
            calls: list[tuple[str, dict[str, object] | None]] = []

            def fake_authorized_get(url: str, *, access_token: str, timeout_seconds: float, params=None, pacer=None, phase=None):
                calls.append((access_token, params))
                raise CrunchyrollUnauthorizedError(url, 401)

            with patch("mal_updater.crunchyroll_snapshot._authorized_json_get", side_effect=fake_authorized_get), patch(
                "mal_updater.crunchyroll_snapshot.refresh_access_token"
            ) as mock_refresh, patch("mal_updater.crunchyroll_snapshot.crunchyroll_login_with_credentials") as mock_login:
                mock_refresh.return_value = (
                    CrunchyrollAccessToken(
                        access_token="refreshed-access-token",
                        refresh_token="refreshed-refresh-token",
                        account_id="acct-123",
                        device_id="device-123",
                        device_type="ANDROIDTV",
                    ),
                    session.state_paths,
                )
                mock_login.return_value = CrunchyrollBootstrapResult(
                    profile="default",
                    locale="en-US",
                    username_path=root / ".MAL-Updater" / "secrets" / "crunchyroll_username.txt",
                    password_path=root / ".MAL-Updater" / "secrets" / "crunchyroll_password.txt",
                    refresh_token_path=session.state_paths.refresh_token_path,
                    device_id_path=session.state_paths.device_id_path,
                    session_state_path=session.state_paths.session_state_path,
                    account_id="acct-123",
                    account_email="user@example.com",
                    access_token="credential-access-token",
                    refresh_token="credential-refresh-token",
                    device_id="device-123",
                    device_type="ANDROIDTV",
                )

                with self.assertRaises(CrunchyrollUnauthorizedError):
                    session.authorized_json_get(
                        "https://example.invalid/history",
                        params={"page": 7, "page_size": 100},
                    )

            self.assertEqual(
                calls,
                [
                    ("access-token", {"page": 7, "page_size": 100}),
                    ("refreshed-access-token", {"page": 7, "page_size": 100}),
                    ("credential-access-token", {"page": 7, "page_size": 100}),
                ],
            )
            self.assertEqual(session.auth_source, "credential_rebootstrap")
            self.assertTrue(session.credential_rebootstrap_attempted)
            mock_refresh.assert_called_once()
            mock_login.assert_called_once()

            session_state = json.loads(session.state_paths.session_state_path.read_text(encoding="utf-8"))
            self.assertEqual(session_state["crunchyroll_phase"], "auth_failed")
            self.assertIn("credential rebootstrap already used", session_state["last_error"])


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

    def test_fetch_snapshot_recovers_watch_history_401_via_refresh_then_completes(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / ".MAL-Updater" / "config").mkdir(parents=True)
            session = self._build_session(root)
            calls: list[tuple[str, str, dict[str, object] | None]] = []
            history_page = [self._history_entry(1)]
            watchlist_page = [self._watchlist_entry(1)]

            def fake_authorized_get(
                url: str,
                *,
                access_token: str,
                timeout_seconds: float,
                params=None,
                pacer=None,
                phase=None,
            ):
                calls.append((url, access_token, params))
                if url == CRUNCHYROLL_ME_URL:
                    self.assertEqual(access_token, "access-token")
                    return {"account_id": "acct-123", "email": "user@example.com"}
                if url.endswith("/watch-history"):
                    if access_token == "access-token":
                        raise CrunchyrollUnauthorizedError(url, 401)
                    self.assertEqual(access_token, "refreshed-access-token")
                    return {"total": 1, "data": history_page}
                if url.endswith("/watchlist"):
                    self.assertEqual(access_token, "refreshed-access-token")
                    return {"total": 1, "data": watchlist_page}
                raise AssertionError(url)

            with patch("mal_updater.crunchyroll_snapshot._authorized_json_get", side_effect=fake_authorized_get), patch(
                "mal_updater.crunchyroll_snapshot.refresh_access_token"
            ) as mock_refresh, patch("mal_updater.crunchyroll_snapshot.crunchyroll_login_with_credentials") as mock_login:
                mock_refresh.return_value = (
                    CrunchyrollAccessToken(
                        access_token="refreshed-access-token",
                        refresh_token="refreshed-refresh-token",
                        account_id="acct-123",
                        device_id="device-123",
                        device_type="ANDROIDTV",
                    ),
                    session.state_paths,
                )

                result = _fetch_snapshot_once(session, use_incremental_boundary=True)

            self.assertEqual(
                calls,
                [
                    (CRUNCHYROLL_ME_URL, "access-token", None),
                    (
                        "https://www.crunchyroll.com/content/v2/acct-123/watch-history",
                        "access-token",
                        {"page": 1, "page_size": 100, "locale": "en-US"},
                    ),
                    (
                        "https://www.crunchyroll.com/content/v2/acct-123/watch-history",
                        "refreshed-access-token",
                        {"page": 1, "page_size": 100, "locale": "en-US"},
                    ),
                ],
            )
            self.assertEqual(session.auth_source, "refresh_token_recovery")
            self.assertEqual(session.token.access_token, "refreshed-access-token")
            self.assertEqual(session.token.refresh_token, "refreshed-refresh-token")
            self.assertEqual(session.account_email, "user@example.com")
            self.assertFalse(session.credential_rebootstrap_attempted)
            self.assertEqual(result.account_email, "user@example.com")
            self.assertEqual(result.snapshot.account_id_hint, "acct-123")
            self.assertEqual(result.snapshot.raw["auth_source"], "refresh_token_recovery")
            self.assertEqual(result.snapshot.raw["history_count"], 1)
            self.assertEqual(result.snapshot.raw["watchlist_count"], 0)
            self.assertFalse(result.snapshot.raw["history_stopped_early"])
            self.assertFalse(result.snapshot.raw["watchlist_stopped_early"])
            self.assertEqual(result.snapshot.raw["sync_boundary_mode"], "hot")
            self.assertTrue(result.snapshot.raw["hot_surface_only"])
            mock_refresh.assert_called_once()
            mock_login.assert_not_called()

            session_state = json.loads(session.state_paths.session_state_path.read_text(encoding="utf-8"))
            self.assertEqual(session_state["crunchyroll_phase"], "python_live_snapshot")
            self.assertIsNone(session_state["last_error"])
            self.assertEqual(session_state["last_account_id_hint"], "acct-123")

    def test_fetch_snapshot_uses_incremental_boundary_to_stop_history_and_watchlist_early(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / ".MAL-Updater" / "config").mkdir(parents=True)
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
            history_backfill_page = [self._history_entry(idx) for idx in range(101, 151)]
            watchlist_page = [self._watchlist_entry(idx) for idx in range(1, 100)] + [previous_watchlist[0]]
            watchlist_backfill_page = [self._watchlist_entry(idx) for idx in range(101, 121)]
            calls: list[tuple[str, dict[str, object] | None]] = []

            def fake_get(url: str, *, params: dict[str, object] | None = None, phase: str | None = None):
                calls.append((url, params))
                if url == CRUNCHYROLL_ME_URL:
                    return {"account_id": "acct-123", "email": "user@example.com"}
                if url.endswith("/watch-history"):
                    if params and params.get("page") == 1:
                        return {"total": 150, "data": history_page}
                    if params and params.get("page") == 2:
                        return {"total": 150, "data": history_backfill_page}
                if url.endswith("/watchlist"):
                    if params and params.get("start") == 0:
                        return {"total": 120, "data": watchlist_page}
                    if params and params.get("start") == 100:
                        return {"total": 120, "data": watchlist_backfill_page}
                raise AssertionError(url)

            with patch.object(_CrunchyrollAuthSession, "authorized_json_get", side_effect=fake_get):
                result = _fetch_snapshot_once(session, use_incremental_boundary=True)

            watch_history_calls = [item for item in calls if item[0].endswith("/watch-history")]
            watchlist_calls = [item for item in calls if item[0].endswith("/watchlist")]
            self.assertEqual(len(watch_history_calls), 1)
            self.assertEqual(len(watchlist_calls), 0)
            self.assertFalse(result.snapshot.raw["history_stopped_early"])
            self.assertFalse(result.snapshot.raw["watchlist_stopped_early"])
            self.assertEqual(result.snapshot.raw["sync_boundary_mode"], "hot")
            self.assertTrue(result.snapshot.raw["hot_surface_only"])
            self.assertTrue(result.state_paths.sync_boundary_path.exists())
            saved = json.loads(result.state_paths.sync_boundary_path.read_text(encoding="utf-8"))
            self.assertEqual(saved["account_id_hint"], "acct-123")
            self.assertIn("history", saved)
            self.assertIn("watchlist", saved)

    def test_fetch_snapshot_full_refresh_ignores_existing_boundary(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / ".MAL-Updater" / "config").mkdir(parents=True)
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

            def fake_get(url: str, *, params: dict[str, object] | None = None, phase: str | None = None):
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

    def test_authorized_json_get_wraps_request_error_with_phase_and_endpoint(self) -> None:
        class FakeTransport:
            class exceptions:
                RequestException = TimeoutError

            def get(self, url, *, headers, timeout, params=None, impersonate=None):
                self.last_call = {"url": url, "timeout": timeout, "impersonate": impersonate}
                raise TimeoutError("simulated timeout")

        fake_transport = FakeTransport()
        with patch("mal_updater.crunchyroll_snapshot.curl_requests", fake_transport):
            with self.assertRaises(CrunchyrollSnapshotError) as ctx:
                _authorized_json_get(
                    "https://www.crunchyroll.com/content/v2/acct-123/watch-history",
                    access_token="access-token",
                    timeout_seconds=7.0,
                    params={"page": 3},
                    phase="watch-history page 3",
                )

        self.assertIn("watch-history page 3", str(ctx.exception))
        self.assertIn("/watch-history", str(ctx.exception))
        self.assertEqual(fake_transport.last_call["timeout"], (7.0, 7.0))
        self.assertEqual(fake_transport.last_call["impersonate"], "chrome124")

    def test_fetch_snapshot_aborts_on_history_page_guard(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / ".MAL-Updater" / "config").mkdir(parents=True)
            session = self._build_session(root)
            calls = []

            def fake_get(url: str, *, params: dict[str, object] | None = None, phase: str | None = None):
                calls.append((url, params, phase))
                if url == CRUNCHYROLL_ME_URL:
                    return {"account_id": "acct-123", "email": "user@example.com"}
                if url.endswith("/watch-history"):
                    return {"total": 999999, "data": [self._history_entry(idx) for idx in range(100)]}
                raise AssertionError((url, params, phase))

            with patch("mal_updater.crunchyroll_snapshot.CRUNCHYROLL_HISTORY_PAGE_LIMIT", 2):
                with patch.object(_CrunchyrollAuthSession, "authorized_json_get", side_effect=fake_get):
                    with self.assertRaises(CrunchyrollSnapshotError) as ctx:
                        _fetch_snapshot_once(session, use_incremental_boundary=False)

            self.assertIn("watch-history exceeded page guard", str(ctx.exception))
            self.assertEqual(len([call for call in calls if call[0].endswith("/watch-history")]), 2)
            self.assertEqual(calls[-1][2], "watch-history page 2")

    def test_fetch_snapshot_page_caps_emit_partial_metadata(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / ".MAL-Updater" / "config").mkdir(parents=True)
            session = self._build_session(root)
            calls = []

            def fake_get(url: str, *, params: dict[str, object] | None = None, phase: str | None = None):
                calls.append((url, params, phase))
                if url == CRUNCHYROLL_ME_URL:
                    return {"account_id": "acct-123", "email": "user@example.com"}
                if url.endswith("/watch-history"):
                    page = int(params["page"])
                    return {"total": 999999, "data": [self._history_entry(page * 100 + idx) for idx in range(100)]}
                if url.endswith("/watchlist"):
                    start = int(params["start"])
                    return {"total": 999999, "data": [self._watchlist_entry(start + idx) for idx in range(100)]}
                raise AssertionError((url, params, phase))

            with patch.object(_CrunchyrollAuthSession, "authorized_json_get", side_effect=fake_get):
                result = _fetch_snapshot_once(
                    session,
                    use_incremental_boundary=False,
                    max_history_pages=2,
                    max_watchlist_pages=1,
                    history_start_page=5,
                    watchlist_start=300,
                )

            raw = result.snapshot.raw
            self.assertTrue(raw["partial"])
            self.assertTrue(raw["history_partial"])
            self.assertEqual(raw["history_start_page"], 5)
            self.assertEqual(raw["history_pages_fetched"], 2)
            self.assertEqual(raw["history_next_page"], 7)
            self.assertTrue(raw["watchlist_partial"])
            self.assertEqual(raw["watchlist_start"], 300)
            self.assertEqual(raw["watchlist_pages_fetched"], 1)
            self.assertEqual(raw["watchlist_next_start"], 400)
            self.assertEqual([call[1]["page"] for call in calls if call[0].endswith("/watch-history")], [5, 6])
            self.assertEqual([call[1]["start"] for call in calls if call[0].endswith("/watchlist")], [300])
            self.assertFalse(result.state_paths.sync_boundary_path.exists())
            session_state = json.loads(result.state_paths.session_state_path.read_text(encoding="utf-8"))
            self.assertEqual(session_state["crunchyroll_phase"], "python_live_snapshot_partial")
            self.assertEqual(session_state["last_error"], "partial snapshot; sync boundary not advanced")


if __name__ == "__main__":
    unittest.main()
