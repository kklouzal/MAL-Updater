from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from mal_updater.config import load_config
from mal_updater.contracts import EpisodeProgress, ProviderSnapshot, SeriesRef, WatchlistEntry
from mal_updater.hidive_auth import HidiveSession, HidiveStatePaths, HidiveTokenSet
import mal_updater.hidive_snapshot as hidive_snapshot
from mal_updater.hidive_snapshot import fetch_snapshot
from mal_updater.snapshot_authority import has_hidive_snapshot_authority
from mal_updater.provider_snapshot import snapshot_to_dict as shared_snapshot_to_dict
from mal_updater.provider_snapshot import write_snapshot_file as shared_write_snapshot_file


def _history_vod(vod_id: int, *, watched_at: int, series_id: int = 3560) -> dict:
    return {
        "id": vod_id, "title": f"E{vod_id}", "duration": 1440, "watchedAt": watched_at,
        "episodeInformation": {
            "seasonNumber": 1, "episodeNumber": 1, "season": 20010, "seasonTitle": "Season 1",
            "seriesInformation": {"id": series_id, "title": "Show"},
        },
    }


def _history_page(vods: list[dict], *, page: int, total_pages: int, total_results: int | None = None) -> dict:
    payload = {"vods": vods, "page": page, "resultsPerPage": 25, "totalPages": total_pages}
    if total_results is not None:
        payload["totalResults"] = total_results
    return payload


class HidiveProgressNormalizationTests(unittest.TestCase):
    def test_watch_progress_integers_are_seconds_and_only_fractions_below_one_are_ratios(self) -> None:
        normalize = hidive_snapshot._normalize_hidive_watch_progress
        self.assertEqual((1, 1 / 1440), normalize(1, 1440))
        self.assertEqual((1, 1 / 1440), normalize(1.0, 1440))
        self.assertEqual((0, 0.0), normalize(0, 1440))
        self.assertEqual((1137, 1137 / 1488), normalize(1137, 1488))
        self.assertEqual((1500, 1.0), normalize(1500, 1440))
        self.assertEqual((720, 0.5), normalize(0.5, 1440))
        self.assertEqual((None, 0.5), normalize(0.5, None))
        self.assertEqual((16, None), normalize(16, None))
        self.assertEqual((None, None), normalize(None, 1440))
        self.assertEqual((None, None), normalize(True, 1440))
        self.assertEqual((None, None), normalize(-3, 1440))

    def _continue_item(self, **overrides) -> dict:
        item = {
            "id": 2002, "title": "Episode 3", "duration": 1440, "watchedAt": 1770600000000,
            "episodeInformation": {
                "seasonNumber": 2, "episodeNumber": 3, "season": 35350, "seasonTitle": "Season 2",
                "seriesInformation": {"id": 4000, "title": "Example"},
            },
        }
        item.update(overrides)
        return item

    def test_one_second_continue_progress_is_not_completion(self) -> None:
        progress = hidive_snapshot._continue_item_to_progress(self._continue_item(watchProgress=1))
        assert progress is not None
        self.assertEqual(1000, progress.playback_position_ms)
        self.assertLess(progress.completion_ratio or 0.0, 0.01)
        self.assertEqual("position", progress.progress_observation_kind)
        self.assertEqual("unknown", progress.completion_assertion)

    def test_continue_item_without_progress_is_membership_not_ratio(self) -> None:
        progress = hidive_snapshot._continue_item_to_progress(self._continue_item())
        assert progress is not None
        self.assertIsNone(progress.completion_ratio)
        self.assertIsNone(progress.playback_position_ms)
        self.assertEqual("history_membership", progress.progress_observation_kind)
        self.assertEqual(1, hidive_snapshot._progress_evidence_rank(progress))

    def test_progress_carries_season_identity_and_episode_label(self) -> None:
        progress = hidive_snapshot._continue_item_to_progress(self._continue_item(watchProgress=600))
        assert progress is not None
        self.assertEqual(("35350", 2, "Season 2"), (progress.provider_season_key, progress.season_number, progress.season_title))
        self.assertEqual((3, "3"), (progress.episode_number, progress.episode_label))
        self.assertIsNone(progress.fully_watched)
        self.assertEqual("hidive_progress_v4", progress.normalization_logic_version)

    def test_decimal_episode_numbers_keep_label_without_truncated_number(self) -> None:
        for raw, expected in ((12.5, (None, "12.5")), ("12.5", (None, "12.5")), (12.0, (12, "12")), ("7", (7, "7")), ("SP", (None, "SP")), (None, (None, None))):
            info = {"seasonNumber": 1, "episodeNumber": raw, "season": 1, "seriesInformation": {"id": 7, "title": "Show"}}
            progress = hidive_snapshot._history_item_to_progress({"id": 9, "watchedAt": 1770000000000, "episodeInformation": info})
            assert progress is not None
            self.assertEqual(expected, (progress.episode_number, progress.episode_label), raw)

    def test_season_key_falls_back_to_season_number_without_season_id(self) -> None:
        info = {"seasonNumber": 2, "episodeNumber": 1, "seriesInformation": {"id": 7, "title": "Show"}}
        progress = hidive_snapshot._history_item_to_progress({"id": 9, "watchedAt": 1770000000000, "episodeInformation": info})
        assert progress is not None
        self.assertEqual("season-number:2", progress.provider_season_key)
        info_without = {"episodeNumber": 1, "seriesInformation": {"id": 7, "title": "Show"}}
        bare = hidive_snapshot._history_item_to_progress({"id": 9, "episodeInformation": info_without})
        assert bare is not None
        self.assertIsNone(bare.provider_season_key)


class HidiveSnapshotTests(unittest.TestCase):
    def _build_session(self, root: Path) -> HidiveSession:
        config = load_config(root)
        state_paths = HidiveStatePaths(
            root=root / ".MAL-Updater" / "state" / "hidive" / "default",
            access_token_path=root / ".MAL-Updater" / "state" / "hidive" / "default" / "authorisation_token.txt",
            refresh_token_path=root / ".MAL-Updater" / "state" / "hidive" / "default" / "refresh_token.txt",
            session_state_path=root / ".MAL-Updater" / "state" / "hidive" / "default" / "session.json",
            sync_boundary_path=root / ".MAL-Updater" / "state" / "hidive" / "default" / "sync_boundary.json",
        )
        return HidiveSession(
            config=config,
            profile="default",
            state_paths=state_paths,
            token=HidiveTokenSet(authorisation_token="access-token", refresh_token="refresh-token", account_id="acct-123"),
        )

    def _sample_snapshot(self) -> ProviderSnapshot:
        return ProviderSnapshot(
            contract_version="1.0",
            generated_at="2026-07-23T00:00:00Z",
            provider="hidive",
            account_id_hint="acct-123",
            series=[SeriesRef(provider_series_id="2312", title="Dungeon People", season_title="Dungeon People", season_number=1)],
            progress=[
                EpisodeProgress(
                    provider_episode_id="655788",
                    provider_series_id="2312",
                    episode_number=12,
                    episode_title="Dungeon People Episode 12",
                    playback_position_ms=600000,
                    duration_ms=1500000,
                    completion_ratio=0.4,
                    last_watched_at="2026-07-22T00:00:00Z",
                    audio_locale="en-US",
                    subtitle_locale=None,
                    rating="TV-14",
                )
            ],
            watchlist=[WatchlistEntry(provider_series_id="2312", added_at="2026-07-21T00:00:00Z", list_name="Favorites")],
            raw={"supports": {"history": True, "watchlists": True}, "nested": {"value": 2}},
        )

    def test_snapshot_serializer_exact_shape(self) -> None:
        snapshot = self._sample_snapshot()

        payload = shared_snapshot_to_dict(snapshot)

        self.assertEqual(
            ["contract_version", "generated_at", "provider", "account_id_hint", "series", "progress", "watchlist", "raw"],
            list(payload.keys()),
        )
        self.assertEqual("hidive", payload["provider"])
        self.assertEqual("2312", payload["series"][0]["provider_series_id"])
        self.assertEqual("655788", payload["progress"][0]["provider_episode_id"])

    def test_module_snapshot_writer_matches_shared_output(self) -> None:
        snapshot = self._sample_snapshot()
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            module_path = root / "module" / "snapshot.json"
            shared_path = root / "shared" / "snapshot.json"

            returned_path = hidive_snapshot.write_snapshot_file(module_path, snapshot)
            shared_write_snapshot_file(shared_path, snapshot)

            self.assertEqual(module_path, returned_path)
            self.assertEqual(shared_path.read_text(encoding="utf-8"), module_path.read_text(encoding="utf-8"))
            self.assertTrue(module_path.read_text(encoding="utf-8").endswith("\n"))

    def test_fetch_snapshot_combines_history_and_continue_watching(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / ".MAL-Updater" / "config").mkdir(parents=True)
            session = self._build_session(root)
            session.state_paths.root.mkdir(parents=True, exist_ok=True)

            history_payload = {
                "vods": [
                    {
                        "id": 1001,
                        "title": "Episode 12",
                        "duration": 1440,
                        "watchedAt": 1770504571302,
                        "externalAssetId": "EPSX0000000000117504",
                        "rating": "TV-14",
                        "episodeInformation": {
                            "seasonNumber": 1,
                            "episodeNumber": 12,
                            "season": 32844,
                            "seasonTitle": "Season 1",
                            "seriesInformation": {
                                "id": 3560,
                                "title": "Dusk Beyond the End of the World",
                            },
                        },
                    }
                ],
                "page": 1,
                "totalPages": 1,
            }
            home_payload = {
                "buckets": [
                    {
                        "name": "CONTINUE WATCHING",
                        "bucketId": 25401,
                        "type": "VOD_VIDEO",
                        "contentList": [
                            {
                                "id": 2002,
                                "title": "Episode 3",
                                "duration": 1500,
                                "watchedAt": 1770600000000,
                                "externalAssetId": "EPSX0000000000999999",
                                "watchProgress": 600,
                                "rating": "TV-14",
                                "episodeInformation": {
                                    "seasonNumber": 2,
                                    "episodeNumber": 3,
                                    "seasonTitle": "Season 2",
                                    "seriesInformation": {
                                        "id": 4000,
                                        "title": "Example Continue Show",
                                    },
                                },
                            }
                        ],
                    }
                ]
            }
            favourites_payload = {
                "events": [
                    {
                        "id": 5005,
                        "title": "Favorite Show",
                        "publishedDate": 1770700000000,
                    }
                ],
                "page": 1,
                "totalPages": 1,
            }
            custom_collection_payload = {"watchlists": [], "pagingInfo": {"moreDataAvailable": False}}

            with patch("mal_updater.hidive_snapshot.start_hidive_session", return_value=session), patch.object(
                HidiveSession,
                "json_get",
                side_effect=[history_payload, home_payload, favourites_payload, custom_collection_payload],
            ):
                result = fetch_snapshot(load_config(root), use_incremental_boundary=False)

        self.assertEqual(result.snapshot.provider, "hidive")
        self.assertEqual(3, len(result.snapshot.series))
        self.assertEqual(2, len(result.snapshot.progress))
        progress_by_episode = {item.provider_episode_id: item for item in result.snapshot.progress}
        self.assertEqual(1001, int(progress_by_episode["1001"].provider_episode_id))
        self.assertIsNone(progress_by_episode["1001"].completion_ratio)
        self.assertIsNone(progress_by_episode["1001"].playback_position_ms)
        # HIDIVE clears an episode's resume point once it is finished.
        self.assertEqual("explicit_completed", progress_by_episode["1001"].progress_observation_kind)
        self.assertEqual("inferred", progress_by_episode["1001"].completion_assertion)
        self.assertEqual(600000, progress_by_episode["2002"].playback_position_ms)
        self.assertEqual(0.4, progress_by_episode["2002"].completion_ratio)
        self.assertEqual("position", progress_by_episode["2002"].progress_observation_kind)
        self.assertEqual("unknown", progress_by_episode["2002"].completion_assertion)
        self.assertEqual(1, len(result.snapshot.watchlist))
        self.assertEqual('5005', result.snapshot.watchlist[0].provider_series_id)
        self.assertEqual('Favorites', result.snapshot.watchlist[0].list_name)
        self.assertEqual({"history": True, "continue_watching": True, "watchlists": True}, result.snapshot.raw["supports"])
        self.assertFalse(result.snapshot.raw["sync_boundary_present"])
        self.assertFalse(result.snapshot.raw["sync_boundary_usable"])
        self.assertEqual("full_refresh", result.snapshot.raw["sync_boundary_mode"])
        self.assertTrue(has_hidive_snapshot_authority(result._ingestion_authority))
        self.assertEqual(str(session.state_paths.sync_boundary_path), result.snapshot.raw["sync_boundary_path"])

    def test_default_fetch_without_boundary_uses_account_refresh_surfaces(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / ".MAL-Updater" / "config").mkdir(parents=True)
            session = self._build_session(root)
            session.state_paths.root.mkdir(parents=True, exist_ok=True)

            history_payload = {
                "vods": [
                    {
                        "id": 1001,
                        "title": "Episode 12",
                        "duration": 1440,
                        "watchedAt": 1770504571302,
                        "episodeInformation": {
                            "seasonNumber": 1,
                            "episodeNumber": 12,
                            "seasonTitle": "Season 1",
                            "seriesInformation": {"id": 3560, "title": "Dusk Beyond the End of the World"},
                        },
                    }
                ],
                "page": 1,
                "totalPages": 1,
            }
            home_payload = {
                "buckets": [
                    {
                        "name": "CONTINUE WATCHING",
                        "page": 1,
                        "resultsPerPage": 5,
                        "totalPages": 4,
                        "totalResults": 18,
                        "contentList": [
                            {
                                "id": 2002,
                                "title": "Episode 3",
                                "duration": 1500,
                                "watchedAt": 1770600000000,
                                "watchProgress": 600,
                                "episodeInformation": {
                                    "seasonNumber": 2,
                                    "episodeNumber": 3,
                                    "seasonTitle": "Season 2",
                                    "seriesInformation": {"id": 4000, "title": "Example Continue Show"},
                                },
                            }
                        ],
                    }
                ]
            }
            favourites_payload = {"events": [{"id": 5005, "title": "Favorite Show"}], "page": 1, "totalPages": 1}
            custom_collection_payload = {"watchlists": [], "pagingInfo": {"moreDataAvailable": False}}

            with patch("mal_updater.hidive_snapshot.start_hidive_session", return_value=session), patch.object(
                HidiveSession,
                "json_get",
                side_effect=[history_payload, home_payload, favourites_payload, custom_collection_payload],
            ):
                result = fetch_snapshot(load_config(root))

        self.assertEqual("full_refresh", result.snapshot.raw["sync_boundary_mode"])
        self.assertFalse(result.snapshot.raw["hot_surface_only"])
        self.assertEqual({"history": True, "continue_watching": True, "watchlists": True}, result.snapshot.raw["supports"])
        self.assertEqual(1, result.snapshot.raw["history_pages_fetched"])
        self.assertEqual(1, result.snapshot.raw["favourite_pages_fetched"])
        self.assertEqual(1, result.continue_count)
        self.assertEqual(1, result.favourite_count)

    def test_fetch_snapshot_stops_early_when_incremental_boundary_matches(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / ".MAL-Updater" / "config").mkdir(parents=True)
            session = self._build_session(root)
            session.state_paths.root.mkdir(parents=True, exist_ok=True)
            session.state_paths.sync_boundary_path.write_text(
                '{\n'
                '  "schema_version": 1,\n'
                '  "generated_at": "2026-03-20T00:00:00Z",\n'
                '  "account_id_hint": "acct-123",\n'
                '  "history": {"first_seen": ["3560|1001|1770504571302"]},\n'
                '  "continue_watching": {"first_seen": ["4000|2002|1770600000000|600"]},\n'
                '  "favourites": {"first_seen": ["5005|Favorite Show"]}\n'
                '}\n',
                encoding="utf-8",
            )
            history_payload = {
                "vods": [
                    {
                        "id": 1001,
                        "title": "Episode 12",
                        "duration": 1440,
                        "watchedAt": 1770504571302,
                        "episodeInformation": {
                            "seasonNumber": 1,
                            "episodeNumber": 12,
                            "seasonTitle": "Season 1",
                            "seriesInformation": {"id": 3560, "title": "Dusk Beyond the End of the World"},
                        },
                    }
                ],
                "page": 1,
                "totalPages": 9,
            }
            home_payload = {
                "buckets": [
                    {
                        "name": "CONTINUE WATCHING",
                        "page": 1,
                        "resultsPerPage": 5,
                        "totalPages": 4,
                        "totalResults": 18,
                        "contentList": [
                            {
                                "id": 2002,
                                "title": "Episode 3",
                                "duration": 1500,
                                "watchedAt": 1770600000000,
                                "watchProgress": 600,
                                "episodeInformation": {
                                    "seasonNumber": 2,
                                    "episodeNumber": 3,
                                    "seasonTitle": "Season 2",
                                    "seriesInformation": {"id": 4000, "title": "Example Continue Show"},
                                },
                            }
                        ],
                    }
                ]
            }
            with patch("mal_updater.hidive_snapshot.start_hidive_session", return_value=session), patch.object(
                HidiveSession,
                "json_get",
                side_effect=[history_payload, home_payload],
            ) as mock_get:
                result = fetch_snapshot(load_config(root))

        self.assertFalse(result.snapshot.raw["history_stopped_early"])
        self.assertTrue(result.snapshot.raw["continue_stopped_early"])
        self.assertFalse(result.snapshot.raw["favourite_stopped_early"])
        self.assertTrue(result.snapshot.raw["sync_boundary_present"])
        self.assertTrue(result.snapshot.raw["sync_boundary_usable"])
        self.assertEqual("hot", result.snapshot.raw["sync_boundary_mode"])
        self.assertTrue(result.snapshot.raw["hot_surface_only"])
        self.assertEqual({"history": True, "continue_watching": True, "watchlists": False}, result.snapshot.raw["supports"])
        self.assertEqual(1, result.snapshot.raw["history_pages_fetched"])
        self.assertEqual(1, result.snapshot.raw["continue_pages_fetched"])
        self.assertTrue(result.snapshot.raw["continue_partial"])
        self.assertTrue(result.snapshot.raw["continue_unpageable"])
        self.assertIn("continue_watching_partial_unpageable", {item["code"] for item in result.snapshot.raw["diagnostics"]})
        self.assertEqual(0, result.snapshot.raw["favourite_pages_fetched"])
        self.assertEqual(2, mock_get.call_count)

    def test_full_fetch_pages_whole_history_with_rpp_and_p(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / ".MAL-Updater" / "config").mkdir(parents=True)
            session = self._build_session(root)
            session.state_paths.root.mkdir(parents=True, exist_ok=True)
            pages = [
                _history_page([_history_vod(1000 + index, watched_at=1770504571302 - index) for index in range(25)], page=1, total_pages=2, total_results=26),
                _history_page([_history_vod(2000, watched_at=1770000000000)], page=2, total_pages=2, total_results=26),
            ]
            with patch("mal_updater.hidive_snapshot.start_hidive_session", return_value=session), patch.object(
                HidiveSession,
                "json_get",
                side_effect=[*pages, {"buckets": []}, {"events": [], "page": 1, "totalPages": 1}, {"watchlists": [], "pagingInfo": {"moreDataAvailable": False}}],
            ) as mock_get:
                result = fetch_snapshot(load_config(root), use_incremental_boundary=False)

        self.assertEqual(
            [{"rpp": 25, "p": 1}, {"rpp": 25, "p": 2}],
            [call.kwargs["params"] for call in mock_get.call_args_list[:2]],
        )
        self.assertEqual(26, result.history_count)
        self.assertEqual(2, result.snapshot.raw["history_pages_fetched"])
        self.assertFalse(result.snapshot.raw["history_stopped_early"])
        self.assertTrue(result.snapshot.raw["history_full_complete"])
        self.assertFalse(result.snapshot.raw["partial"])
        self.assertEqual("complete", result.snapshot.fetch_provenance[0].completeness)
        self.assertNotIn("history_partial_unpageable", {item["code"] for item in result.snapshot.raw["diagnostics"]})

    def test_history_page_echo_mismatch_is_non_advancing_and_partial(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / ".MAL-Updater" / "config").mkdir(parents=True)
            session = self._build_session(root)
            session.state_paths.root.mkdir(parents=True, exist_ok=True)
            first = _history_page([_history_vod(1000 + index, watched_at=1770504571302 - index) for index in range(25)], page=1, total_pages=17, total_results=401)
            ignored = _history_page([_history_vod(9000 + index, watched_at=1760000000000 - index) for index in range(25)], page=1, total_pages=17, total_results=401)
            with patch("mal_updater.hidive_snapshot.start_hidive_session", return_value=session), patch.object(
                HidiveSession,
                "json_get",
                side_effect=[first, ignored, {"buckets": []}, {"events": [], "page": 1, "totalPages": 1}, {"watchlists": [], "pagingInfo": {"moreDataAvailable": False}}],
            ):
                result = fetch_snapshot(load_config(root), use_incremental_boundary=False)

        self.assertEqual(25, result.history_count)
        self.assertTrue(result.snapshot.raw["history_non_advancing_detected"])
        self.assertTrue(result.snapshot.raw["partial"])
        self.assertFalse(result.snapshot.raw["history_full_complete"])
        diagnostic = next(item for item in result.snapshot.raw["diagnostics"] if item["code"] == "history_pagination_non_advancing")
        self.assertEqual((2, 1), (diagnostic["requested_page"], diagnostic["echoed_page"]))

    def test_hot_run_pages_down_to_boundary_and_rewrites_history_markers(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / ".MAL-Updater" / "config").mkdir(parents=True)
            session = self._build_session(root)
            session.state_paths.root.mkdir(parents=True, exist_ok=True)
            session.state_paths.sync_boundary_path.write_text(json.dumps({
                "schema_version": 1, "generated_at": "2026-08-01T00:00:00Z", "account_id_hint": "acct-123",
                "history": {"first_seen": ["3560|3005|1700000000000"]},
                "continue_watching": {"first_seen": []},
                "favourites": {"first_seen": ["5005|Favorite Show"], "backfill_seen": ["5006|Older"]},
            }), encoding="utf-8")
            pages = [
                _history_page([_history_vod(1000 + index, watched_at=1780000000000 - index) for index in range(25)], page=1, total_pages=17),
                _history_page([_history_vod(2000 + index, watched_at=1750000000000 - index) for index in range(25)], page=2, total_pages=17),
                _history_page([_history_vod(3005, watched_at=1700000000000)], page=3, total_pages=17),
            ]
            with patch("mal_updater.hidive_snapshot.start_hidive_session", return_value=session), patch.object(
                HidiveSession, "json_get", side_effect=[*pages, {"buckets": []}],
            ) as mock_get:
                result = fetch_snapshot(load_config(root))
            boundary = json.loads(session.state_paths.sync_boundary_path.read_text(encoding="utf-8"))

        self.assertEqual(4, mock_get.call_count)
        raw = result.snapshot.raw
        self.assertEqual("hot", raw["sync_boundary_mode"])
        self.assertEqual(3, raw["history_pages_fetched"])
        self.assertTrue(raw["history_front_boundary_complete"])
        self.assertFalse(raw["history_full_complete"])
        self.assertFalse(raw["sync_boundary_escalated_to_full_refresh"])
        self.assertTrue(raw["sync_boundary_rewrite_eligible"])
        self.assertEqual("3560|1000|1780000000000", boundary["history"]["first_seen"][0])
        self.assertEqual(["5005|Favorite Show"], boundary["favourites"]["first_seen"])
        self.assertEqual(["5006|Older"], boundary["favourites"]["backfill_seen"])

    def test_hot_run_escalates_to_full_refresh_when_boundary_is_unreachable(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / ".MAL-Updater" / "config").mkdir(parents=True)
            session = self._build_session(root)
            session.state_paths.root.mkdir(parents=True, exist_ok=True)
            session.state_paths.sync_boundary_path.write_text(json.dumps({
                "schema_version": 1, "generated_at": "2026-08-01T00:00:00Z", "account_id_hint": "acct-123",
                "history": {"first_seen": ["3560|404|1"]},
                "continue_watching": {"first_seen": []}, "favourites": {"first_seen": []},
            }), encoding="utf-8")
            pages = [
                _history_page([_history_vod(page * 100 + index, watched_at=1780000000000 - page * 100 - index) for index in range(25)], page=page, total_pages=3)
                for page in (1, 2)
            ] + [_history_page([_history_vod(300, watched_at=1700000000000)], page=3, total_pages=3)]
            with patch.object(hidive_snapshot, "HIDIVE_HOT_HISTORY_CATCHUP_PAGE_LIMIT", 2), patch(
                "mal_updater.hidive_snapshot.start_hidive_session", return_value=session,
            ), patch.object(
                HidiveSession, "json_get",
                side_effect=[*pages, {"buckets": []}, {"events": [{"id": 5005, "title": "Favorite Show"}], "page": 1, "totalPages": 1}, {"watchlists": [], "pagingInfo": {"moreDataAvailable": False}}],
            ) as mock_get:
                result = fetch_snapshot(load_config(root))
            boundary = json.loads(session.state_paths.sync_boundary_path.read_text(encoding="utf-8"))

        raw = result.snapshot.raw
        self.assertEqual({"rpp": 25, "p": 3}, mock_get.call_args_list[2].kwargs["params"])
        self.assertTrue(raw["sync_boundary_escalated_to_full_refresh"])
        self.assertEqual("full_refresh", raw["sync_boundary_mode"])
        self.assertEqual("hot_boundary_unreachable_full_refresh", raw["sync_boundary_refresh_kind"])
        self.assertFalse(raw["hot_surface_only"])
        self.assertEqual(51, raw["history_count"])
        self.assertTrue(raw["history_full_complete"])
        self.assertTrue(raw["watchlist_complete"])
        self.assertFalse(raw["partial"])
        self.assertIn("history_page_cap_hit", {item["code"] for item in raw["diagnostics"]})
        self.assertEqual("3560|100|1779999999900", boundary["history"]["first_seen"][0])
        self.assertEqual(["5005|Favorite Show"], boundary["favourites"]["first_seen"])

    def test_hot_run_that_reads_whole_history_without_marker_proves_front(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / ".MAL-Updater" / "config").mkdir(parents=True)
            session = self._build_session(root)
            session.state_paths.root.mkdir(parents=True, exist_ok=True)
            session.state_paths.sync_boundary_path.write_text(json.dumps({
                "schema_version": 1, "generated_at": "2026-08-01T00:00:00Z", "account_id_hint": "acct-123",
                "history": {"first_seen": ["3560|rewatched|1"]},
                "continue_watching": {"first_seen": []}, "favourites": {"first_seen": []},
            }), encoding="utf-8")
            with patch("mal_updater.hidive_snapshot.start_hidive_session", return_value=session), patch.object(
                HidiveSession, "json_get",
                side_effect=[_history_page([_history_vod(7, watched_at=1780000000000)], page=1, total_pages=1), {"buckets": []}],
            ):
                result = fetch_snapshot(load_config(root))
            boundary = json.loads(session.state_paths.sync_boundary_path.read_text(encoding="utf-8"))

        raw = result.snapshot.raw
        self.assertEqual("hot", raw["sync_boundary_mode"])
        self.assertTrue(raw["history_front_boundary_complete"])
        self.assertTrue(raw["history_full_complete"])
        self.assertFalse(raw["partial"])
        self.assertEqual(["3560|7|1780000000000"], boundary["history"]["first_seen"])

    def test_null_account_boundary_is_repaired_then_hot_run_stops_on_page_one(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / ".MAL-Updater" / "config").mkdir(parents=True)
            session = self._build_session(root)
            session.state_paths.root.mkdir(parents=True, exist_ok=True)
            session.state_paths.sync_boundary_path.write_text(
                '{"schema_version":1,"generated_at":"2026-03-20T00:00:00Z",'
                '"account_id_hint":null,"history":{"first_seen":["old"]},'
                '"continue_watching":{"first_seen":[]},"favourites":{"first_seen":[]}}\n',
                encoding="utf-8",
            )
            history_page = {
                "vods": [{
                    "id": 1001, "title": "Episode 12", "duration": 1440,
                    "watchedAt": 1770504571302,
                    "episodeInformation": {"seasonNumber": 1, "episodeNumber": 12,
                        "seasonTitle": "Season 1", "seriesInformation": {"id": 3560, "title": "Show"}},
                }],
                "page": 1, "totalPages": 77,
            }
            # The repair run reads the whole (single-page) history; the hot run
            # sees a long history and must stop at the page-1 boundary marker.
            full_history = {**history_page, "totalPages": 1}
            home_partial = {"buckets": [{
                "name": "CONTINUE WATCHING", "page": 1, "resultsPerPage": 1,
                "totalPages": 2, "totalResults": 2, "contentList": [],
            }]}
            favourites = {"events": [{"id": 5005, "title": "Favorite Show"}], "page": 1, "totalPages": 1}
            custom = {"watchlists": [], "pagingInfo": {"moreDataAvailable": False}}

            with patch("mal_updater.hidive_snapshot.start_hidive_session", return_value=session), patch.object(
                HidiveSession, "json_get",
                side_effect=[full_history, home_partial, favourites, custom],
            ):
                repaired = fetch_snapshot(load_config(root))

            boundary_payload = __import__("json").loads(session.state_paths.sync_boundary_path.read_text(encoding="utf-8"))
            self.assertEqual("acct-123", boundary_payload["account_id_hint"])
            self.assertEqual(["3560|1001|1770504571302"], boundary_payload["history"]["first_seen"])
            self.assertTrue(repaired.snapshot.raw["partial"])
            self.assertTrue(repaired.snapshot.raw["history_full_complete"])
            self.assertFalse(repaired.snapshot.raw["continue_complete"])
            self.assertTrue(repaired.snapshot.raw["watchlist_complete"])
            self.assertTrue(repaired.snapshot.raw["sync_boundary_rewrite_eligible"])

            with patch("mal_updater.hidive_snapshot.start_hidive_session", return_value=session), patch.object(
                HidiveSession, "json_get", side_effect=[history_page, home_partial],
            ) as mock_get:
                hot = fetch_snapshot(load_config(root))

            self.assertEqual("hot", hot.snapshot.raw["sync_boundary_mode"])
            self.assertTrue(hot.snapshot.raw["history_front_boundary_complete"])
            self.assertFalse(hot.snapshot.raw["history_full_complete"])
            self.assertFalse(hot.snapshot.raw["watchlist_complete"])
            self.assertEqual(0, hot.snapshot.raw["favourite_pages_fetched"])
            self.assertEqual(0, hot.snapshot.raw["custom_watchlist_collection_pages_fetched"])
            self.assertEqual(2, mock_get.call_count)
            self.assertEqual({"rpp": 25, "p": 1}, mock_get.call_args_list[0].kwargs["params"])

    def test_account_mismatch_fences_boundary_rewrite(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / ".MAL-Updater" / "config").mkdir(parents=True)
            session = self._build_session(root)
            session.state_paths.root.mkdir(parents=True, exist_ok=True)
            original = (
                '{"schema_version":1,"generated_at":"2026-03-20T00:00:00Z",'
                '"account_id_hint":"other-account","history":{"first_seen":["other-marker"]},'
                '"continue_watching":{"first_seen":[]},"favourites":{"first_seen":[]}}\n'
            )
            session.state_paths.sync_boundary_path.write_text(original, encoding="utf-8")
            responses = [
                {"vods": [], "page": 1, "totalPages": 1},
                {"buckets": []},
                {"events": [], "page": 1, "totalPages": 1},
                {"watchlists": [], "pagingInfo": {"moreDataAvailable": False}},
            ]
            with patch("mal_updater.hidive_snapshot.start_hidive_session", return_value=session), patch.object(
                HidiveSession, "json_get", side_effect=responses,
            ):
                result = fetch_snapshot(load_config(root))

            self.assertTrue(result.snapshot.raw["sync_boundary_present"])
            self.assertFalse(result.snapshot.raw["sync_boundary_usable"])
            self.assertEqual("account_mismatch", result.snapshot.raw["sync_boundary_account_status"])
            self.assertTrue(result.snapshot.raw["sync_boundary_rewrite_blocked_by_account_mismatch"])
            self.assertFalse(result.snapshot.raw["sync_boundary_rewrite_eligible"])
            self.assertEqual(original, session.state_paths.sync_boundary_path.read_text(encoding="utf-8"))

    def test_custom_watchlist_detail_paginates_with_zero_last_seen_and_preserves_cross_list_membership(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / ".MAL-Updater" / "config").mkdir(parents=True)
            session = self._build_session(root)
            session.state_paths.root.mkdir(parents=True, exist_ok=True)
            history_payload = {"vods": [], "page": 1, "totalPages": 1}
            home_payload = {"buckets": []}
            favourites_payload = {"events": [], "page": 1, "totalPages": 1}
            collection_payload = {
                "watchlists": [
                    {"watchlistExternalId": 111, "name": "Owned One", "ownership": "OWNED"},
                    {"watchlistExternalId": 222, "name": "Owned Two", "ownership": "OWNED"},
                ],
                "pagingInfo": {"moreDataAvailable": False, "lastSeen": "opaque"},
            }
            list_one_page_one = {
                "watchlistExternalId": 111,
                "name": "Owned One",
                "ownership": "OWNED",
                "content": [
                    {"id": 9001, "type": "VOD_SEASON", "title": "Season A", "series": {"seriesId": 7001, "title": "Shared Show"}},
                ],
                "pagingInfo": {"moreDataAvailable": True, "lastSeen": 0},
            }
            list_one_page_two = {
                "watchlistExternalId": 111,
                "name": "Owned One",
                "ownership": "OWNED",
                "content": [
                    {"id": 9002, "type": "VOD_SEASON", "title": "Season B", "series": {"seriesId": 7002, "title": "Paged Show"}},
                ],
                "pagingInfo": {"moreDataAvailable": False, "lastSeen": 9002},
            }
            list_two_page_one = {
                "watchlistExternalId": 222,
                "name": "Owned Two",
                "ownership": "OWNED",
                "content": [
                    {"id": 9001, "type": "VOD_SEASON", "title": "Season A", "series": {"seriesId": 7001, "title": "Shared Show"}},
                ],
                "pagingInfo": {"moreDataAvailable": False, "lastSeen": 9001},
            }

            with patch("mal_updater.hidive_snapshot.start_hidive_session", return_value=session), patch.object(
                HidiveSession,
                "json_get",
                side_effect=[history_payload, home_payload, favourites_payload, collection_payload, list_one_page_one, list_one_page_two, list_two_page_one],
            ) as mock_get:
                result = fetch_snapshot(load_config(root), use_incremental_boundary=False)

        self.assertEqual(2, result.snapshot.raw["custom_watchlist_list_count"])
        self.assertEqual(3, result.snapshot.raw["custom_watchlist_count"])
        self.assertEqual(3, result.snapshot.raw["custom_watchlist_detail_pages_fetched"])
        self.assertEqual(
            ["watchlist:111", "watchlist:111", "watchlist:222"],
            [entry.list_id for entry in result.snapshot.watchlist],
        )
        self.assertEqual(["7001", "7002", "7001"], [entry.provider_series_id for entry in result.snapshot.watchlist])
        self.assertEqual(["9001", "9002", "9001"], [entry.provider_item_id for entry in result.snapshot.watchlist])
        self.assertEqual({"7001", "7002"}, {series.provider_series_id for series in result.snapshot.series})
        detail_page_two_call = mock_get.call_args_list[5]
        self.assertEqual({"rpp": hidive_snapshot.HIDIVE_CUSTOM_WATCHLIST_RPP, "lastSeen": 0}, detail_page_two_call.kwargs["params"])


if __name__ == "__main__":
    unittest.main()


class HidiveContinueWatchingRequestTests(unittest.TestCase):
    def test_continue_watching_requests_full_bucket_with_bpp_and_rpp(self) -> None:
        # Probed 2026-11-18: size/page are ignored (5 items); rpp=25 returns
        # the whole 19-item bucket, rpp=50 is rejected with HTTP 400.
        calls = []

        class Session:
            def json_get(self, path, *, params=None, timeout_seconds=None):
                calls.append((path, params))
                return {"buckets": [{"name": "Continue Watching", "totalResults": 19, "totalPages": 1,
                                     "contentList": [{"id": n} for n in range(19)]}]}

        items, _stopped, metadata = hidive_snapshot._fetch_continue_items(Session())
        self.assertEqual([("/content/home", {"bpp": 5, "rpp": 25})], calls)
        self.assertEqual(19, len(items))
        self.assertFalse(metadata["partial"])
