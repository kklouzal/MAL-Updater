from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from mal_updater.config import load_config
from mal_updater.db import bootstrap_database, upsert_series_mapping
from mal_updater.ingestion import ingest_snapshot_payload
from mal_updater.mal_client import MalClient
from mal_updater.season_mappings import get_provider_season_mapping, upsert_provider_season_mapping
from mal_updater.sync_planner import (
    EpisodeProgressState,
    ProviderSeriesState,
    _provider_watched_episodes,
    _summarize_episode_progress,
    execute_approved_sync,
    load_provider_series_states,
)


def _row(number, *, key="S1", ratio=0.97, watched_at="2026-03-14T20:00:00Z", fully_watched=None, **extra):
    return EpisodeProgressState(
        provider=extra.pop("provider", "crunchyroll"),
        provider_episode_id=extra.pop("episode_id", f"{key}-{number}"),
        episode_number=number,
        completion_ratio=ratio,
        playback_position_ms=extra.pop("position", None),
        duration_ms=extra.pop("duration", None),
        last_watched_at=watched_at,
        provider_season_key=key,
        fully_watched=fully_watched,
        **extra,
    )


class EpisodeSummaryRuleTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        root = Path(self.temp_dir.name)
        (root / ".MAL-Updater" / "config").mkdir(parents=True)
        self.config = load_config(root)

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def test_unnumbered_specials_do_not_count_as_episodes(self) -> None:
        rows = [_row(1), _row(2), _row(3), _row(None, episode_id="pv")]
        summary = _summarize_episode_progress(rows, self.config)
        self.assertEqual(3, summary["completed_episode_count"])
        self.assertEqual(1, summary["completed_unnumbered_count"])

    def test_unit_with_only_unnumbered_items_counts_one(self) -> None:
        summary = _summarize_episode_progress([_row(None, episode_id="movie")], self.config)
        self.assertEqual(1, summary["completed_episode_count"])

    def test_provider_fully_watched_flag_completes_a_partially_played_finale(self) -> None:
        summary = _summarize_episode_progress([_row(12, ratio=0.9, fully_watched=True)], self.config)
        self.assertEqual(1, summary["completed_episode_count"])
        self.assertEqual(1, summary["completion_audit"]["completed_by"]["provider_fully_watched"])

    def test_hidive_history_episode_completed_when_next_episode_started_after_runtime(self) -> None:
        history = dict(provider="hidive", ratio=None, duration=24 * 60 * 1000, progress_observation_kind="history_membership")
        rows = [
            _row(1, watched_at="2026-08-20T03:35:00Z", **dict(history)),
            _row(2, watched_at="2026-08-20T04:00:00Z", **dict(history)),
            _row(3, watched_at="2026-08-20T04:05:00Z", **dict(history)),
        ]
        summary = _summarize_episode_progress(rows, self.config)
        # E1 -> E2 is 25 min (>= 90% of 24 min): inferred. E2 -> E3 is 5 min: not.
        self.assertEqual(1, summary["completed_episode_count"])
        self.assertEqual(1, summary["max_completed_episode_number"])


class ProviderWatchedEpisodesTests(unittest.TestCase):
    @staticmethod
    def _state(*, count: int, max_completed: int, min_number: int) -> ProviderSeriesState:
        return ProviderSeriesState(
            provider="crunchyroll",
            provider_series_id="cr-1",
            title="Example",
            season_title=None,
            season_number=None,
            progress_rows=count,
            max_episode_number=max_completed,
            completed_episode_count=count,
            max_completed_episode_number=max_completed,
            watchlist_status=None,
            last_watched_at=None,
            min_episode_number=min_number,
        )

    def test_first_episode_of_continuously_numbered_season_never_completes_it(self) -> None:
        # Season 2 numbered 13-25 on a 13-episode MAL entry: episode 13 is its first episode.
        watched, _info, review = _provider_watched_episodes(self._state(count=1, max_completed=13, min_number=13), 13)
        self.assertEqual(13, watched)
        self.assertTrue(review and review.startswith("provider_completion_rests_on_unobserved_episodes"))

    def test_completion_allowed_when_mal_already_records_the_lower_episodes(self) -> None:
        state = self._state(count=1, max_completed=13, min_number=13)
        self.assertEqual((13, None, None), _provider_watched_episodes(state, 13, current_watched=12))


class SeasonSplitExecutorTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        root = Path(self.temp_dir.name)
        (root / ".MAL-Updater" / "config").mkdir(parents=True)
        self.config = load_config(root)
        bootstrap_database(self.config.db_path)
        secrets = root / ".MAL-Updater" / "secrets"
        secrets.mkdir(parents=True, exist_ok=True)
        (secrets / "mal_client_id.txt").write_text("client-id\n", encoding="utf-8")
        (secrets / "mal_access_token.txt").write_text("access-token\n", encoding="utf-8")

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def _ingest(self, series: list[tuple[str, str]], progress: list[dict]) -> None:
        payload = {
            "contract_version": "1.0",
            "generated_at": "2026-03-14T21:00:00Z",
            "provider": "crunchyroll",
            "account_id_hint": None,
            "series": [
                {"provider_series_id": series_id, "title": title, "season_title": None, "season_number": None}
                for series_id, title in series
            ],
            "progress": progress,
            "watchlist": [],
            "raw": {},
        }
        ingest_snapshot_payload(payload, self.config)

    @staticmethod
    def _progress(series_id: str, key: str, number: int, *, season_number: int, watched_at: str) -> dict:
        return {
            "provider_episode_id": f"{series_id}-{key}-{number}",
            "provider_series_id": series_id,
            "episode_number": number,
            "episode_title": f"Episode {number}",
            "playback_position_ms": 1400000,
            "duration_ms": 1440000,
            "completion_ratio": 0.98,
            "last_watched_at": watched_at,
            "audio_locale": "ja-JP",
            "subtitle_locale": None,
            "rating": None,
            "provider_season_key": key,
            "season_number": season_number,
            "season_title": f"Season {season_number}",
        }

    def _map_series(self, series_id: str, mal_anime_id: int) -> None:
        upsert_series_mapping(
            self.config.db_path,
            provider="crunchyroll",
            provider_series_id=series_id,
            mal_anime_id=mal_anime_id,
            confidence=1.0,
            mapping_source="user_exact",
            approved_by_user=True,
            notes=None,
        )

    @staticmethod
    def _details(by_id: dict[int, dict | None]):
        def details(anime_id, **_kwargs):
            return {
                "id": anime_id,
                "title": f"MAL {anime_id}",
                "num_episodes": 12,
                "media_type": "tv",
                "status": "finished_airing",
                "my_list_status": by_id.get(anime_id),
                "alternative_titles": {},
            }

        return details

    @staticmethod
    def _write_echo(anime_id, *, status, num_watched_episodes, **_kwargs):
        return {"status": status, "num_episodes_watched": num_watched_episodes}

    def test_multi_season_series_writes_each_season_to_its_own_mal_entry(self) -> None:
        # Production regression: one Crunchyroll series holding two seasons with
        # continuous numbering (1-12, 13-15) was written to one MAL entry as 15.
        progress = [self._progress("cr-1", "S1", n, season_number=1, watched_at=f"2026-03-01T00:{n:02d}:00Z") for n in range(1, 13)]
        progress += [self._progress("cr-1", "S2", n, season_number=2, watched_at=f"2026-03-10T00:{n:02d}:00Z") for n in range(13, 16)]
        self._ingest([("cr-1", "Example Show")], progress)
        self._map_series("cr-1", 100)
        for key, mal_id, season in (("S1", 100, 1), ("S2", 200, 2)):
            upsert_provider_season_mapping(
                self.config.db_path, provider="crunchyroll", provider_series_id="cr-1", provider_season_key=key,
                mal_anime_id=mal_id, mapping_source="auto_exact", confidence=1.0,
                season_title=f"Season {season}", season_number=season, resolver_version="test",
            )

        states = load_provider_series_states(self.config, split_seasons=True)
        self.assertEqual({"S1", "S2"}, {state.provider_season_key for state in states})

        with patch.object(MalClient, "get_anime_details", side_effect=self._details({})), patch.object(
            MalClient, "update_my_list_status", side_effect=self._write_echo
        ) as update:
            results = execute_approved_sync(self.config, limit=None, exact_approved_only=True, dry_run=False)

        writes = {call.args[0]: call.kwargs for call in update.call_args_list}
        self.assertEqual("completed", writes[100]["status"])
        self.assertEqual(12, writes[100]["num_watched_episodes"])
        self.assertEqual("watching", writes[200]["status"])
        self.assertEqual(3, writes[200]["num_watched_episodes"])
        self.assertTrue(all(result.applied for result in results))

    def test_dry_run_provenance_uses_the_detail_of_the_recorded_season(self) -> None:
        from mal_updater.db import get_watch_confirmation_provenance
        from mal_updater.sync_planner import build_dry_run_sync_plan

        progress = [
            {**self._progress("hd-1", key, n, season_number=season, watched_at=watched_at), "provider_series_id": "hd-1"}
            for key, season, n, watched_at in (
                ("S1", 1, 1, "2026-03-01T00:00:00Z"),
                ("S2", 2, 1, "2026-03-10T00:00:00Z"),
            )
        ]
        ingest_snapshot_payload(
            {
                "contract_version": "1.0",
                "generated_at": "2026-03-14T21:00:00Z",
                "provider": "hidive",
                "account_id_hint": None,
                "series": [{"provider_series_id": "hd-1", "title": "Example Show", "season_title": None, "season_number": None}],
                "progress": progress,
                "watchlist": [],
                "raw": {},
            },
            self.config,
        )
        upsert_series_mapping(
            self.config.db_path, provider="hidive", provider_series_id="hd-1", mal_anime_id=100, confidence=1.0,
            mapping_source="user_exact", approved_by_user=True, notes=None,
        )
        for key, mal_id, season in (("S1", 100, 1), ("S2", 200, 2)):
            upsert_provider_season_mapping(
                self.config.db_path, provider="hidive", provider_series_id="hd-1", provider_season_key=key,
                mal_anime_id=mal_id, mapping_source="auto_exact", confidence=1.0,
                season_title=f"Season {season}", season_number=season, resolver_version="test",
            )

        def details(anime_id, **_kwargs):
            return {"id": anime_id, "title": f"MAL {anime_id}", "num_episodes": 24 if anime_id == 200 else 12,
                    "media_type": "tv", "status": "finished_airing", "my_list_status": None, "alternative_titles": {}}

        with patch.object(MalClient, "get_anime_details", side_effect=details):
            build_dry_run_sync_plan(self.config, limit=None, provider="hidive", approved_mappings_only=True)
        recorded = get_watch_confirmation_provenance(self.config.db_path, provider="hidive", provider_series_id="hd-1")
        assert recorded is not None
        # Season 2 was watched last (rank 0); its own MAL entry and size are recorded.
        self.assertEqual((200, 24), (recorded.mal_anime_id, recorded.mal_num_episodes))

    def test_split_cour_season_overflows_into_the_mal_sequel(self) -> None:
        # Production case: one provider season of 20 episodes is MAL Part 1 (12)
        # plus Part 2; previously it went to review as "exceeds MAL count".
        from mal_updater.db import replace_mal_anime_relations

        progress = [self._progress("cr-1", "S1", n, season_number=1, watched_at=f"2026-03-01T00:{n:02d}:00Z") for n in range(1, 21)]
        self._ingest([("cr-1", "Example Show")], progress)
        self._map_series("cr-1", 100)
        replace_mal_anime_relations(
            self.config.db_path,
            mal_anime_id=100,
            relations=[{"related_mal_anime_id": 200, "relation_type": "sequel", "raw": {"node": {"id": 200}}}],
        )
        with patch.object(MalClient, "get_anime_details", side_effect=self._details({})), patch.object(
            MalClient, "update_my_list_status", side_effect=self._write_echo
        ) as update:
            results = execute_approved_sync(self.config, limit=None, exact_approved_only=True, dry_run=False)

        writes = {call.args[0]: call.kwargs for call in update.call_args_list}
        self.assertEqual(("completed", 12), (writes[100]["status"], writes[100]["num_watched_episodes"]))
        self.assertEqual(("watching", 8), (writes[200]["status"], writes[200]["num_watched_episodes"]))
        # The unit's last watch was a Part 2 episode, so Part 1's finish date is unknown.
        self.assertIsNone(writes[100]["finish_date"])
        self.assertTrue(all(any(r.startswith("split_cour_overflow") for r in result.reasons) for result in results))

    def test_single_episode_past_mal_count_is_not_treated_as_part_two(self) -> None:
        # Production case: a 13-episode provider season whose MAL entry has 12
        # (episode 13 is an extra) must not mark the MAL sequel "watching 1".
        from mal_updater.db import replace_mal_anime_relations

        progress = [self._progress("cr-1", "S1", n, season_number=1, watched_at=f"2026-03-01T00:{n:02d}:00Z") for n in range(1, 14)]
        self._ingest([("cr-1", "Example Show")], progress)
        self._map_series("cr-1", 100)
        replace_mal_anime_relations(
            self.config.db_path,
            mal_anime_id=100,
            relations=[{"related_mal_anime_id": 200, "relation_type": "sequel", "raw": {"node": {"id": 200}}}],
        )
        with patch.object(MalClient, "get_anime_details", side_effect=self._details({})), patch.object(
            MalClient, "update_my_list_status", side_effect=AssertionError("must not write")
        ):
            results = execute_approved_sync(self.config, limit=None, exact_approved_only=True, dry_run=False)
        self.assertEqual(["review"], [result.proposal_decision for result in results])
        self.assertEqual([100], [result.mal_anime_id for result in results])

    def test_seasons_sharing_one_mal_entry_write_only_the_most_advanced(self) -> None:
        # Production case: a 103-episode MAL entry published as several provider
        # seasons (continuous numbering) must not get competing writes.
        progress = [self._progress("cr-1", "S1", n, season_number=1, watched_at=f"2026-03-01T00:{n:02d}:00Z") for n in range(1, 13)]
        progress += [self._progress("cr-1", "S2", n, season_number=2, watched_at=f"2026-03-05T00:{n - 12:02d}:00Z") for n in range(13, 21)]
        self._ingest([("cr-1", "Long Show")], progress)
        self._map_series("cr-1", 500)
        for key, season in (("S1", 1), ("S2", 2)):
            upsert_provider_season_mapping(
                self.config.db_path, provider="crunchyroll", provider_series_id="cr-1", provider_season_key=key,
                mal_anime_id=500, mapping_source="auto_exact", confidence=1.0,
                season_title=f"Season {season}", season_number=season, resolver_version="test",
            )

        def details(anime_id, **_kwargs):
            return {"id": anime_id, "title": "Long Show", "num_episodes": 103, "media_type": "tv",
                    "status": "finished_airing", "my_list_status": None, "alternative_titles": {}}

        with patch.object(MalClient, "get_anime_details", side_effect=details), patch.object(
            MalClient, "update_my_list_status", side_effect=self._write_echo
        ) as update:
            results = execute_approved_sync(self.config, limit=None, exact_approved_only=True, dry_run=False)

        self.assertEqual([(500, 20)], [(call.args[0], call.kwargs["num_watched_episodes"]) for call in update.call_args_list])
        self.assertTrue(any("superseded_by_more_advanced_unit_for_same_mal_entry" in r.reasons for r in results))

    def test_ambiguous_part_one_and_part_two_candidates_resolve_to_part_one(self) -> None:
        from mal_updater.db import replace_mal_anime_relations
        from mal_updater.mapping import MappingCandidate
        from mal_updater.sync_planner import _split_cour_first_part

        def candidate(mal_id: int, episodes: int, media_type: str = "tv") -> MappingCandidate:
            return MappingCandidate(mal_id, f"T{mal_id}", [], media_type, "finished_airing", episodes, 0.9, "q", [], {})

        replace_mal_anime_relations(
            self.config.db_path,
            mal_anime_id=11,
            relations=[{"related_mal_anime_id": 12, "relation_type": "sequel", "raw": {"node": {"id": 12}}}],
        )
        progress = [self._progress("cr-1", "S1", n, season_number=1, watched_at=f"2026-03-01T00:{n:02d}:00Z") for n in range(1, 24)]
        progress += [self._progress("cr-1", "S2", 1, season_number=2, watched_at="2026-03-09T00:00:00Z")]
        self._ingest([("cr-1", "Example Show")], progress)
        state = next(s for s in load_provider_series_states(self.config, split_seasons=True) if s.provider_season_key == "S1")
        from mal_updater.config import load_mal_secrets

        client = MalClient(self.config, load_mal_secrets(self.config))  # relations are local; no request is made
        # Candidate order from the engine may put Part 2 first.
        chosen = _split_cour_first_part(self.config, client, state, [candidate(12, 12), candidate(11, 11)])
        self.assertEqual(11, chosen.mal_anime_id)
        # Evidence that fits Part 1 alone is not a split.
        from dataclasses import replace

        short = replace(state, max_episode_number=10, completed_episode_count=10)
        self.assertIsNone(_split_cour_first_part(self.config, client, short, [candidate(11, 11), candidate(12, 12)]))
        # Numbering continued from an earlier season (13-24) is not a split cour.
        continued = replace(state, min_episode_number=13, max_episode_number=24)
        self.assertIsNone(_split_cour_first_part(self.config, client, continued, [candidate(11, 11), candidate(12, 12)]))
        # A movie candidate is never a split-cour part.
        self.assertIsNone(_split_cour_first_part(self.config, client, state, [candidate(11, 11), candidate(12, 12, "movie")]))

    def test_unresolved_season_is_reviewed_and_never_written(self) -> None:
        progress = [self._progress("cr-1", "S1", 1, season_number=1, watched_at="2026-03-01T00:00:00Z")]
        progress += [self._progress("cr-1", "S2", 1, season_number=2, watched_at="2026-03-02T00:00:00Z")]
        self._ingest([("cr-1", "Example Show")], progress)
        self._map_series("cr-1", 100)

        with patch.object(MalClient, "get_anime_details", side_effect=self._details({})), patch.object(
            MalClient, "update_my_list_status", side_effect=AssertionError("unresolved seasons must not be written")
        ):
            results = execute_approved_sync(self.config, limit=None, mapping_limit=0, exact_approved_only=True, dry_run=False)

        self.assertEqual({"review"}, {result.proposal_decision for result in results})
        self.assertTrue(all("season_mapping_pending_search_budget" in result.reasons for result in results))
        self.assertIsNone(
            get_provider_season_mapping(self.config.db_path, provider="crunchyroll", provider_series_id="cr-1", provider_season_key="S1")
        )

    def test_completed_count_beyond_mal_episode_count_is_reviewed_not_written(self) -> None:
        progress = [self._progress("cr-1", "S1", n, season_number=1, watched_at=f"2026-03-01T00:{n:02d}:00Z") for n in range(1, 15)]
        self._ingest([("cr-1", "Example Show")], progress)
        self._map_series("cr-1", 100)
        with patch.object(MalClient, "get_anime_details", side_effect=self._details({})), patch.object(
            MalClient, "update_my_list_status", side_effect=AssertionError("over-count must not be written")
        ):
            results = execute_approved_sync(self.config, limit=None, exact_approved_only=True, dry_run=False)
        self.assertEqual("review", results[0].proposal_decision)
        self.assertTrue(any(reason.startswith("provider_completed_episodes_exceed_mal_episode_count") for reason in results[0].reasons))

    def test_limit_caps_writes_most_recent_first_and_reports_deferred(self) -> None:
        series = [("cr-a", "Show A"), ("cr-b", "Show B"), ("cr-c", "Show C")]
        progress = [
            self._progress("cr-a", "A1", 1, season_number=1, watched_at="2026-03-01T00:00:00Z"),
            self._progress("cr-b", "B1", 1, season_number=1, watched_at="2026-03-03T00:00:00Z"),
            self._progress("cr-c", "C1", 1, season_number=1, watched_at="2026-03-02T00:00:00Z"),
        ]
        self._ingest(series, progress)
        for index, (series_id, _title) in enumerate(series):
            self._map_series(series_id, 300 + index)
        with patch.object(MalClient, "get_anime_details", side_effect=self._details({})), patch.object(
            MalClient, "update_my_list_status", side_effect=self._write_echo
        ) as update:
            results = execute_approved_sync(self.config, limit=1, exact_approved_only=True, dry_run=False)

        self.assertEqual([301], [call.args[0] for call in update.call_args_list])  # Show B, watched last
        deferred = [result for result in results if result.proposal_decision == "deferred"]
        self.assertEqual(["write_limit_reached deferred_proposals=2"], deferred[0].reasons)

    def test_watchlist_only_series_never_overrides_existing_mal_status(self) -> None:
        payload_series = [("cr-w", "Watchlist Show")]
        self._ingest(payload_series, [])
        ingest_snapshot_payload(
            {
                "contract_version": "1.0",
                "generated_at": "2026-03-15T21:00:00Z",
                "provider": "crunchyroll",
                "account_id_hint": None,
                "series": [{"provider_series_id": "cr-w", "title": "Watchlist Show", "season_title": None, "season_number": None}],
                "progress": [],
                "watchlist": [{"provider_series_id": "cr-w", "added_at": "2026-03-10T12:00:00Z", "status": "never_watched"}],
                "raw": {},
            },
            self.config,
        )
        self._map_series("cr-w", 400)
        with patch.object(
            MalClient, "get_anime_details", side_effect=self._details({400: {"status": "watching", "num_episodes_watched": 4}})
        ), patch.object(MalClient, "update_my_list_status", side_effect=AssertionError("must not write")):
            results = execute_approved_sync(self.config, limit=None, exact_approved_only=True, dry_run=False)
        self.assertEqual("skip", results[0].proposal_decision)
        self.assertIn("mal_entry_exists_watchlist_only", results[0].reasons)


_HEALTH_ARGS = dict(
    stale_hours=72.0,
    review_issue_type=None,
    review_worklist_limit=5,
    mapping_coverage_threshold=0.8,
    maintenance_review_limit=5,
)


class SeasonMappingCliAndHealthTests(unittest.TestCase):
    def test_unresolved_season_raises_health_warning_until_approved_via_cli(self) -> None:
        import io
        import json
        from contextlib import redirect_stdout

        from mal_updater.cli import main
        from mal_updater.health_report import build_health_report

        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / ".MAL-Updater" / "config").mkdir(parents=True)
            config = load_config(root)
            bootstrap_database(config.db_path)
            upsert_provider_season_mapping(
                config.db_path, provider="crunchyroll", provider_series_id="cr-1", provider_season_key="S2",
                mal_anime_id=None, mapping_source="unresolved", confidence=0.4,
                season_title="Season 2", season_number=2, resolver_version="test",
            )
            codes = {warning["code"] for warning in build_health_report(config, **_HEALTH_ARGS)["warnings"]}
            self.assertIn("unresolved_season_mappings", codes)

            out = io.StringIO()
            with redirect_stdout(out), patch(
                "sys.argv",
                ["mal-updater", "--project-root", td, "season-mappings", "--approve", "--provider", "crunchyroll",
                 "--provider-series-id", "cr-1", "--provider-season-key", "S2", "--mal-anime-id", "200"],
            ):
                self.assertEqual(0, main())
            listed = json.loads(out.getvalue())
            self.assertEqual([("S2", 200, "user_exact", True)], [
                (row["provider_season_key"], row["mal_anime_id"], row["mapping_source"], row["approved"]) for row in listed
            ])
            codes = {warning["code"] for warning in build_health_report(config, **_HEALTH_ARGS)["warnings"]}
            self.assertNotIn("unresolved_season_mappings", codes)


if __name__ == "__main__":
    unittest.main()
