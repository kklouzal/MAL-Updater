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
    SEASON_MAPPING_RESOLVER_VERSION,
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
        # Only the observed episode is counted (right under either reading).
        watched, info, review = _provider_watched_episodes(self._state(count=1, max_completed=13, min_number=13), 13)
        self.assertEqual(1, watched)
        self.assertIsNone(review)
        self.assertTrue(info and info.startswith("provider_completion_rests_on_unobserved_episodes_counted_observed"))

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
                season_title=f"Season {season}", season_number=season, resolver_version=SEASON_MAPPING_RESOLVER_VERSION,
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
                season_title=f"Season {season}", season_number=season, resolver_version=SEASON_MAPPING_RESOLVER_VERSION,
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

    def test_split_cour_assigns_each_completed_episode_to_its_part_despite_a_gap(self) -> None:
        # Production cases (Dead Mount Death Play, Re:Zero S2): one unfinished
        # episode used to keep the whole season in review.
        from mal_updater.db import replace_mal_anime_relations

        progress = [self._progress("cr-1", "S1", n, season_number=1, watched_at=f"2026-03-01T00:{n:02d}:00Z") for n in range(1, 21)]
        progress[4]["completion_ratio"] = 0.3  # episode 5 unfinished
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
            execute_approved_sync(self.config, limit=None, exact_approved_only=True, dry_run=False)
        writes = {call.args[0]: (call.kwargs["status"], call.kwargs["num_watched_episodes"]) for call in update.call_args_list}
        self.assertEqual({100: ("completed", 12), 200: ("watching", 8)}, writes)

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
            MalClient, "update_my_list_status", side_effect=self._write_echo
        ) as update:
            execute_approved_sync(self.config, limit=None, exact_approved_only=True, dry_run=False)
        # The sequel is untouched; the fully watched entry itself completes.
        self.assertEqual([(100, "completed", 12)], [(c.args[0], c.kwargs["status"], c.kwargs["num_watched_episodes"]) for c in update.call_args_list])

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
                season_title=f"Season {season}", season_number=season, resolver_version=SEASON_MAPPING_RESOLVER_VERSION,
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

        # Hypothetical states set absolute fields only; drop the in-season view.
        state = replace(state, ordinal_completed_episode_count=None, ordinal_max_completed_episode_number=None,
                        ordinal_max_episode_number=None, ordinal_min_episode_number=None,
                        ordinal_completed_episode_numbers=None)
        short = replace(state, max_episode_number=10, completed_episode_count=10)
        self.assertIsNone(_split_cour_first_part(self.config, client, short, [candidate(11, 11), candidate(12, 12)]))
        # Numbering continued from an earlier season (13-24) is not a split cour.
        continued = replace(state, min_episode_number=13, max_episode_number=24)
        self.assertIsNone(_split_cour_first_part(self.config, client, continued, [candidate(11, 11), candidate(12, 12)]))
        # A movie candidate is never a split-cour part.
        self.assertIsNone(_split_cour_first_part(self.config, client, state, [candidate(11, 11), candidate(12, 12, "movie")]))

    def test_absolute_numbered_season_uses_in_season_position_and_skips_specials(self) -> None:
        # Production shape (Attack on Titan Final Season): episodes numbered
        # 60-87 with sequence 1-28, plus "SP1"/"SP2" specials whose
        # episode_number is 1/2. MAL lists Part 1 (16) and Part 2 (12).
        from mal_updater.db import replace_mal_anime_relations

        progress = []
        for ordinal in range(1, 29):
            row = self._progress("cr-1", "FS", 59 + ordinal, season_number=4, watched_at=f"2026-03-01T{ordinal // 60:02d}:{ordinal % 60:02d}:00Z")
            row["season_episode_ordinal"] = ordinal
            row["episode_label"] = str(59 + ordinal)
            progress.append(row)
        for index, label in enumerate(("SP1", "SP2"), start=1):
            row = self._progress("cr-1", "FS", index, season_number=4, watched_at=f"2026-03-02T00:0{index}:00Z")
            row["provider_episode_id"] = f"cr-1-{label}"
            row["episode_label"] = label
            progress.append(row)
        self._ingest([("cr-1", "Titan")], progress)
        self._map_series("cr-1", 100)
        replace_mal_anime_relations(
            self.config.db_path,
            mal_anime_id=100,
            relations=[{"related_mal_anime_id": 200, "relation_type": "sequel", "raw": {"node": {"id": 200}}}],
        )

        def details(anime_id, **_kwargs):
            return {"id": anime_id, "title": f"MAL {anime_id}", "num_episodes": 16 if anime_id == 100 else 12,
                    "media_type": "tv", "status": "finished_airing", "my_list_status": None, "alternative_titles": {}}

        with patch.object(MalClient, "get_anime_details", side_effect=details), patch.object(
            MalClient, "update_my_list_status", side_effect=self._write_echo
        ) as update:
            execute_approved_sync(self.config, limit=None, exact_approved_only=True, dry_run=False)

        writes = {call.args[0]: (call.kwargs["status"], call.kwargs["num_watched_episodes"]) for call in update.call_args_list}
        self.assertEqual({100: ("completed", 16), 200: ("completed", 12)}, writes)

    def test_absolute_numbering_is_kept_when_it_fits_a_long_mal_entry(self) -> None:
        # Production shape (D.Gray-man): one 103-episode MAL entry published as
        # several provider seasons; season 4 is numbered 77-102 (sequence 1-26).
        progress = []
        for ordinal in range(1, 27):
            row = self._progress("cr-1", "S4", 76 + ordinal, season_number=4, watched_at=f"2026-03-01T00:{ordinal:02d}:00Z")
            row["season_episode_ordinal"] = ordinal
            progress.append(row)
        self._ingest([("cr-1", "Long Show")], progress)
        self._map_series("cr-1", 500)

        def details(anime_id, **_kwargs):
            return {"id": anime_id, "title": "Long Show", "num_episodes": 103, "media_type": "tv",
                    "status": "finished_airing", "my_list_status": {"status": "watching", "num_episodes_watched": 76},
                    "alternative_titles": {}}

        with patch.object(MalClient, "get_anime_details", side_effect=details), patch.object(
            MalClient, "update_my_list_status", side_effect=self._write_echo
        ) as update:
            execute_approved_sync(self.config, limit=None, exact_approved_only=True, dry_run=False)
        self.assertEqual([(500, 102)], [(call.args[0], call.kwargs["num_watched_episodes"]) for call in update.call_args_list])

    def test_rows_without_ordinals_keep_provider_numbering(self) -> None:
        # Rows stored before ordinals were captured carry only the absolute
        # number; mixing them with in-season positions would corrupt both views.
        progress = []
        for ordinal in range(1, 6):
            row = self._progress("cr-1", "FS", 59 + ordinal, season_number=4, watched_at=f"2026-03-01T00:0{ordinal}:00Z")
            if ordinal >= 4:
                row["season_episode_ordinal"] = ordinal
            progress.append(row)
        recap = self._progress("cr-1", "FS", 0, season_number=4, watched_at="2026-03-01T00:09:00Z")
        recap.update(provider_episode_id="cr-1-recap", episode_number=None, season_episode_ordinal=None)
        progress.append(recap)
        self._ingest([("cr-1", "Titan")], progress)
        (state,) = load_provider_series_states(self.config, split_seasons=True)
        self.assertIsNone(state.ordinal_completed_episode_count)
        self.assertEqual(64, state.max_completed_episode_number)

    def test_ova_season_never_auto_matches_a_tv_entry(self) -> None:
        # Production case: "Attack on Titan OADs" auto-matched the TV season 1.
        from mal_updater.mapping import MappingCandidate, MappingResult
        from mal_updater.sync_planner import _resolve_season_target

        progress = [self._progress("cr-1", "OAD", n, season_number=6, watched_at=f"2026-03-01T00:0{n}:00Z") for n in range(1, 4)]
        for row in progress:
            row["season_title"] = "Attack on Titan OADs"
        progress += [self._progress("cr-1", "S1", 1, season_number=1, watched_at="2026-02-01T00:00:00Z")]
        self._ingest([("cr-1", "Attack on Titan")], progress)
        state = next(s for s in load_provider_series_states(self.config, split_seasons=True) if s.provider_season_key == "OAD")
        tv = MappingCandidate(16498, "Shingeki no Kyojin", [], "tv", "finished_airing", 25, 1.0, "q", ["exact_normalized_title"], {})
        result = MappingResult(series=None, status="exact", confidence=1.0, chosen_candidate=tv, candidates=[tv], rationale=[])
        from mal_updater.config import load_mal_secrets

        with patch("mal_updater.sync_planner.map_series", return_value=result), patch(
            "mal_updater.sync_planner.should_auto_approve_mapping", return_value=True
        ):
            target = _resolve_season_target(self.config, MalClient(self.config, load_mal_secrets(self.config)), state, search_budget=[1])
        self.assertIsNone(target.mal_anime_id)
        self.assertIn("season_mapping_unresolved", target.reasons)

    def test_stale_automatic_season_match_survives_an_empty_re_resolution(self) -> None:
        from mal_updater.config import load_mal_secrets
        from mal_updater.mapping import MappingResult
        from mal_updater.sync_planner import _resolve_season_target

        progress = [self._progress("cr-1", key, 1, season_number=n, watched_at=f"2026-03-0{n}T00:00:00Z") for n, key in ((1, "S1"), (2, "S2"))]
        self._ingest([("cr-1", "Show")], progress)
        upsert_provider_season_mapping(
            self.config.db_path, provider="crunchyroll", provider_series_id="cr-1", provider_season_key="S2",
            mal_anime_id=222, mapping_source="auto_exact", confidence=1.0,
            season_title="Season 2", season_number=2, resolver_version="older",
        )
        state = next(s for s in load_provider_series_states(self.config, split_seasons=True) if s.provider_season_key == "S2")
        # map_series tolerates MAL search errors and then returns no candidates.
        empty = MappingResult(series=None, status="no_candidates", confidence=0.0, chosen_candidate=None, candidates=[], rationale=[])
        client = MalClient(self.config, load_mal_secrets(self.config))
        budget = [1]
        with patch("mal_updater.sync_planner.map_series", return_value=empty):
            target = _resolve_season_target(self.config, client, state, search_budget=budget)
        self.assertEqual(222, target.mal_anime_id)
        self.assertIn("season_mapping_reresolve_inconclusive", target.reasons)
        self.assertEqual(0, budget[0])
        stored = get_provider_season_mapping(self.config.db_path, provider="crunchyroll", provider_series_id="cr-1", provider_season_key="S2")
        assert stored is not None
        self.assertEqual(("auto_exact", "older"), (stored.mapping_source, stored.resolver_version))
        # With the search budget spent, the stale match keeps applying.
        with patch("mal_updater.sync_planner.map_series") as search:
            target = _resolve_season_target(self.config, client, state, search_budget=[0])
        search.assert_not_called()
        self.assertEqual(222, target.mal_anime_id)

    def test_sibling_season_with_its_own_mal_entry_uses_in_season_position(self) -> None:
        # Season 2 numbered 13-24 has its own 25-episode MAL entry: watching 13-20
        # is 8 episodes of that entry, although the absolute 20 would "fit".
        progress = []
        for n in range(1, 13):
            row = self._progress("cr-1", "S1", n, season_number=1, watched_at=f"2026-03-01T00:{n:02d}:00Z")
            row["season_episode_ordinal"] = n
            progress.append(row)
        for ordinal in range(1, 9):
            row = self._progress("cr-1", "S2", 12 + ordinal, season_number=2, watched_at=f"2026-03-05T00:{ordinal:02d}:00Z")
            row["season_episode_ordinal"] = ordinal
            progress.append(row)
        self._ingest([("cr-1", "Example Show")], progress)
        self._map_series("cr-1", 100)
        for key, mal_id, season in (("S1", 100, 1), ("S2", 200, 2)):
            upsert_provider_season_mapping(
                self.config.db_path, provider="crunchyroll", provider_series_id="cr-1", provider_season_key=key,
                mal_anime_id=mal_id, mapping_source="auto_exact", confidence=1.0,
                season_title=f"Season {season}", season_number=season, resolver_version=SEASON_MAPPING_RESOLVER_VERSION,
            )

        def details(anime_id, **_kwargs):
            return {"id": anime_id, "title": f"MAL {anime_id}", "num_episodes": 12 if anime_id == 100 else 25,
                    "media_type": "tv", "status": "finished_airing", "my_list_status": None, "alternative_titles": {}}

        with patch.object(MalClient, "get_anime_details", side_effect=details), patch.object(
            MalClient, "update_my_list_status", side_effect=self._write_echo
        ) as update:
            execute_approved_sync(self.config, limit=None, exact_approved_only=True, dry_run=False)
        writes = {call.args[0]: (call.kwargs["status"], call.kwargs["num_watched_episodes"]) for call in update.call_args_list}
        self.assertEqual({100: ("completed", 12), 200: ("watching", 8)}, writes)

        # The dry-run plan (and its review queue) uses the same sibling numbering.
        from mal_updater.sync_planner import build_dry_run_sync_plan

        with patch.object(MalClient, "get_anime_details", side_effect=details):
            proposals = build_dry_run_sync_plan(self.config, limit=None, approved_mappings_only=True)
        planned = {p.mal_anime_id: (p.proposed_my_list_status or {}).get("num_watched_episodes") for p in proposals}
        self.assertEqual(8, planned[200])

    def test_split_cour_part_two_completion_needs_observed_episodes(self) -> None:
        # Part 2 watched only at its last two episodes (provider 23-24): the
        # split must not complete the MAL sequel on unobserved episodes.
        from mal_updater.db import replace_mal_anime_relations

        numbers = list(range(1, 13)) + [23, 24]
        progress = [self._progress("cr-1", "S1", n, season_number=1, watched_at=f"2026-03-01T00:{n:02d}:00Z") for n in numbers]
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
            execute_approved_sync(self.config, limit=None, exact_approved_only=True, dry_run=False)
        writes = {call.args[0]: (call.kwargs["status"], call.kwargs["num_watched_episodes"]) for call in update.call_args_list}
        self.assertEqual({100: ("completed", 12), 200: ("watching", 2)}, writes)

    def _strong_result(self, mal_id: int, episodes: int, *others: tuple[int, int]):
        from mal_updater.mapping import MappingCandidate, MappingResult

        candidate = MappingCandidate(mal_id, f"T{mal_id}", [], "tv", "finished_airing", episodes, 0.9, "q", [], {})
        rest = [MappingCandidate(oid, f"T{oid}", [], "tv", "finished_airing", count, 0.8, "q", [], {}) for oid, count in others]
        return MappingResult(
            series=None, status="strong", confidence=0.9, chosen_candidate=candidate, candidates=[candidate, *rest], rationale=[]
        )

    def _resolve_s2(self, relations_from_anchor: list[dict], *, top: int = 200, others: tuple[tuple[int, int], ...] = ()):
        from mal_updater.config import load_mal_secrets
        from mal_updater.db import replace_mal_anime_relations
        from mal_updater.sync_planner import _resolve_season_target

        progress = [self._progress("cr-1", "S1", n, season_number=1, watched_at=f"2026-03-01T00:{n:02d}:00Z") for n in range(1, 13)]
        progress += [self._progress("cr-1", "S2", n, season_number=2, watched_at=f"2026-03-05T00:{n:02d}:00Z") for n in range(1, 13)]
        self._ingest([("cr-1", "Example Show")], progress)
        self._map_series("cr-1", 100)
        replace_mal_anime_relations(self.config.db_path, mal_anime_id=100, relations=relations_from_anchor)
        replace_mal_anime_relations(
            self.config.db_path, mal_anime_id=200,
            relations=[{"related_mal_anime_id": 100, "relation_type": "prequel", "raw": {"node": {"id": 100}}}],
        )
        state = next(s for s in load_provider_series_states(self.config, split_seasons=True) if s.provider_season_key == "S2")
        with patch("mal_updater.sync_planner.map_series", return_value=self._strong_result(top, 12, *others)), patch(
            "mal_updater.sync_planner.should_auto_approve_mapping", return_value=False
        ), patch.object(MalClient, "get_anime_details", side_effect=AssertionError("relations are local")):
            return _resolve_season_target(self.config, MalClient(self.config, load_mal_secrets(self.config)), state, search_budget=[1])

    def test_strong_season_match_accepted_when_franchise_and_episode_count_agree(self) -> None:
        target = self._resolve_s2([{"related_mal_anime_id": 200, "relation_type": "sequel", "raw": {"node": {"id": 200}}}])
        self.assertEqual(200, target.mal_anime_id)
        self.assertIn("season_mapping_accepted_by=franchise_episode_count_corroborated", target.reasons)

    def test_strong_season_match_outside_the_franchise_stays_unresolved(self) -> None:
        from mal_updater.db import replace_mal_anime_relations

        replace_mal_anime_relations(
            self.config.db_path, mal_anime_id=999,
            relations=[{"related_mal_anime_id": 100, "relation_type": "prequel", "raw": {"node": {"id": 100}}}],
        )
        target = self._resolve_s2([{"related_mal_anime_id": 999, "relation_type": "sequel", "raw": {"node": {"id": 999}}}])
        self.assertIsNone(target.mal_anime_id)

    def test_strong_match_declined_when_a_franchise_sibling_has_the_same_count(self) -> None:
        # Seasons 1 and 2 both have 12 episodes; the count cannot tell them apart.
        sequel = [{"related_mal_anime_id": 200, "relation_type": "sequel", "raw": {"node": {"id": 200}}}]
        target = self._resolve_s2(sequel, others=((100, 12),))
        self.assertIsNone(target.mal_anime_id)

    def test_strong_match_declined_when_it_is_the_anchor_for_a_later_season(self) -> None:
        sequel = [{"related_mal_anime_id": 200, "relation_type": "sequel", "raw": {"node": {"id": 200}}}]
        self.assertIsNone(self._resolve_s2(sequel, top=100).mal_anime_id)

    def test_strong_match_declined_when_another_season_already_maps_to_it(self) -> None:
        upsert_provider_season_mapping(
            self.config.db_path, provider="crunchyroll", provider_series_id="cr-1", provider_season_key="S1",
            mal_anime_id=200, mapping_source="auto_exact", confidence=1.0,
            season_title="Season 1", season_number=1, resolver_version=SEASON_MAPPING_RESOLVER_VERSION,
        )
        sequel = [{"related_mal_anime_id": 200, "relation_type": "sequel", "raw": {"node": {"id": 200}}}]
        self.assertIsNone(self._resolve_s2(sequel).mal_anime_id)

    def test_catalog_first_episode_gives_in_season_positions_despite_interleaved_recaps(self) -> None:
        # Production shape (Slime season 2): episodes 25-48, with two recap
        # specials interleaved so sequence numbers run to 26.
        from mal_updater.db import replace_mal_anime_relations
        from mal_updater.season_catalog import ProviderSeasonCatalog, upsert_provider_season_catalog

        progress = []
        for index, number in enumerate(range(25, 49), start=1):
            row = self._progress("cr-1", "S2", number, season_number=2, watched_at=f"2026-03-01T00:{index:02d}:00Z")
            row["season_episode_ordinal"] = index + (1 if number > 36 else 0) + (1 if number > 46 else 0)
            progress.append(row)
        progress.append(self._progress("cr-1", "S1", 1, season_number=1, watched_at="2026-02-01T00:00:00Z"))
        self._ingest([("cr-1", "Slime")], progress)
        self._map_series("cr-1", 100)
        upsert_provider_season_mapping(
            self.config.db_path, provider="crunchyroll", provider_series_id="cr-1", provider_season_key="S2",
            mal_anime_id=300, mapping_source="auto_exact", confidence=1.0, season_title="Season 2", season_number=2,
            resolver_version=SEASON_MAPPING_RESOLVER_VERSION,
        )
        upsert_provider_season_catalog(self.config.db_path, ProviderSeasonCatalog(
            provider="crunchyroll", provider_season_key="S2", title="Season 2", numbered_episode_count=24,
            total_episode_count=26, first_date="2021-01-12", last_date="2021-09-21", date_kind="original_air",
            complete=True, fetched_at="", first_episode_number=25))
        replace_mal_anime_relations(
            self.config.db_path, mal_anime_id=300,
            relations=[{"related_mal_anime_id": 400, "relation_type": "sequel", "raw": {"node": {"id": 400}}}],
        )
        with patch.object(MalClient, "get_anime_details", side_effect=self._details({})), patch.object(
            MalClient, "update_my_list_status", side_effect=self._write_echo
        ) as update:
            execute_approved_sync(self.config, limit=None, exact_approved_only=True, dry_run=False)
        writes = {call.args[0]: (call.kwargs["status"], call.kwargs["num_watched_episodes"]) for call in update.call_args_list if call.args[0] != 100}
        self.assertEqual({300: ("completed", 12), 400: ("completed", 12)}, writes)

    def test_interleaved_recap_does_not_shift_numbering_that_starts_at_one(self) -> None:
        # Production shape (NieR:Automata): episodes 1-24 plus recap "18.5",
        # so sequence numbers after it run one ahead (episode 24 -> 25).
        from mal_updater.db import replace_mal_anime_relations

        progress = []
        for number in range(1, 25):
            row = self._progress("cr-1", "S1", number, season_number=1, watched_at=f"2026-03-01T00:{number:02d}:00Z")
            row["season_episode_ordinal"] = number + (1 if number > 18 else 0)
            row["episode_label"] = str(number)
            progress.append(row)
        recap = self._progress("cr-1", "S1", 18, season_number=1, watched_at="2026-03-01T00:30:00Z")
        recap.update(provider_episode_id="cr-1-recap", episode_label="18.5", episode_number=None, season_episode_ordinal=None)
        progress.append(recap)
        self._ingest([("cr-1", "NieR")], progress)
        self._map_series("cr-1", 100)
        replace_mal_anime_relations(
            self.config.db_path, mal_anime_id=100,
            relations=[{"related_mal_anime_id": 200, "relation_type": "sequel", "raw": {"node": {"id": 200}}}],
        )
        with patch.object(MalClient, "get_anime_details", side_effect=self._details({})), patch.object(
            MalClient, "update_my_list_status", side_effect=self._write_echo
        ) as update:
            execute_approved_sync(self.config, limit=None, exact_approved_only=True, dry_run=False)
        writes = {call.args[0]: (call.kwargs["status"], call.kwargs["num_watched_episodes"]) for call in update.call_args_list}
        self.assertEqual({100: ("completed", 12), 200: ("completed", 12)}, writes)

    def test_extra_episode_beyond_a_fully_watched_mal_entry_completes_it(self) -> None:
        # Production cases (Arifureta S2, Kore wa Zombie): the provider season
        # carries an extra numbered episode after the MAL entry's 12.
        progress = [self._progress("cr-1", "S1", n, season_number=1, watched_at=f"2026-03-01T00:{n:02d}:00Z") for n in range(1, 14)]
        self._ingest([("cr-1", "Example Show")], progress)
        self._map_series("cr-1", 100)
        with patch.object(MalClient, "get_anime_details", side_effect=self._details({})), patch.object(
            MalClient, "update_my_list_status", side_effect=self._write_echo
        ) as update:
            results = execute_approved_sync(self.config, limit=None, exact_approved_only=True, dry_run=False)
        self.assertEqual([(100, "completed", 12)], [(c.args[0], c.kwargs["status"], c.kwargs["num_watched_episodes"]) for c in update.call_args_list])
        self.assertTrue(any(r.startswith("provider_extra_episodes_beyond_mal_count_ignored") for r in results[0].reasons))

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
                season_title="Season 2", season_number=2, resolver_version=SEASON_MAPPING_RESOLVER_VERSION,
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



class SeasonCatalogDateResolutionTests(unittest.TestCase):
    """Matching by season date windows (probed 2026-11-18 on the unresolved seasons)."""

    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        root = Path(self.temp_dir.name)
        (root / ".MAL-Updater" / "config").mkdir(parents=True)
        self.config = load_config(root)
        bootstrap_database(self.config.db_path)
        secrets = root / ".MAL-Updater" / "secrets"
        secrets.mkdir(parents=True, exist_ok=True)
        (secrets / "mal_client_id.txt").write_text("client-id\n", encoding="utf-8")

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def _seed(self, provider: str, season_title: str, *, relations: dict[int, list[tuple[int, str]]]) -> None:
        from mal_updater.db import replace_mal_anime_relations

        progress = []
        for key, number_range, title in (("S0", range(1, 3), "Season 0"), ("SX", range(1, 13), season_title)):
            for n in number_range:
                progress.append({
                    "provider_episode_id": f"{key}-{n}", "provider_series_id": "ser-1", "episode_number": n,
                    "episode_title": None, "playback_position_ms": 1400000, "duration_ms": 1440000, "completion_ratio": 0.98,
                    "last_watched_at": f"2026-03-0{1 if key == 'S0' else 2}T00:{n:02d}:00Z", "audio_locale": None,
                    "subtitle_locale": None, "rating": None, "provider_season_key": key, "season_number": 0 if key == "S0" else 2,
                    "season_title": title, "season_episode_ordinal": n,
                })
        ingest_snapshot_payload({
            "contract_version": "1.0", "generated_at": "2026-03-14T21:00:00Z", "provider": provider, "account_id_hint": None,
            "series": [{"provider_series_id": "ser-1", "title": "Show", "season_title": None, "season_number": None}],
            "progress": progress, "watchlist": [], "raw": {},
        }, self.config)
        upsert_series_mapping(self.config.db_path, provider=provider, provider_series_id="ser-1", mal_anime_id=100,
                              confidence=1.0, mapping_source="user_exact", approved_by_user=True, notes=None)
        for mal_id, rels in relations.items():
            replace_mal_anime_relations(self.config.db_path, mal_anime_id=mal_id, relations=[
                {"related_mal_anime_id": rid, "relation_type": kind, "raw": {"node": {"id": rid}}} for rid, kind in rels])

    def _resolve(self, provider, catalog, entries, *, engine_choice=None, engine_auto=False):
        from mal_updater.config import load_mal_secrets
        from mal_updater.mapping import MappingCandidate, MappingResult
        from mal_updater.season_catalog import ProviderSeasonCatalog, upsert_provider_season_catalog
        from mal_updater.sync_planner import _resolve_season_target

        upsert_provider_season_catalog(self.config.db_path, ProviderSeasonCatalog(
            provider=provider, provider_season_key="SX", title=None, numbered_episode_count=catalog.get("count", 12),
            total_episode_count=catalog.get("total", catalog.get("count", 12)), first_date=catalog["first"], last_date=catalog.get("last"),
            date_kind=catalog["kind"], complete=True, fetched_at=""))
        state = next(s for s in load_provider_series_states(self.config, split_seasons=True) if s.provider_season_key == "SX")
        chosen = None
        if engine_choice is not None:
            chosen = MappingCandidate(engine_choice, "T", [], "tv", None, 12, 0.9, "q", [], {})
        result = MappingResult(series=None, status="exact" if engine_auto else "strong", confidence=0.9,
                               chosen_candidate=chosen, candidates=[chosen] if chosen else [], rationale=[])

        def details(anime_id, **_kwargs):
            media, num, start, end = entries[anime_id]
            return {"id": anime_id, "title": f"T{anime_id}", "media_type": media, "num_episodes": num, "start_date": start, "end_date": end}

        with patch("mal_updater.sync_planner.map_series", return_value=result), patch(
            "mal_updater.sync_planner.should_auto_approve_mapping", return_value=engine_auto
        ), patch.object(MalClient, "get_anime_details", side_effect=details):
            return _resolve_season_target(self.config, MalClient(self.config, load_mal_secrets(self.config)), state, search_budget=[1])

    SIBLINGS = {100: ("tv", 12, "2021-04-10", "2021-06-26"), 200: ("tv", 12, "2025-04-05", "2025-06-21")}

    def test_same_length_sibling_is_told_apart_by_broadcast_window(self) -> None:
        self._seed("crunchyroll", "Season 1", relations={100: [(200, "sequel")], 200: [(100, "prequel")]})
        target = self._resolve("crunchyroll", {"kind": "original_air", "first": "2021-04-10", "last": "2021-06-26"},
                               self.SIBLINGS, engine_choice=200)
        self.assertEqual(100, target.mal_anime_id)
        self.assertIn("season_mapping_accepted_by=season_catalog_original_air", target.reasons)

    def test_original_air_dates_veto_a_wrong_auto_approved_title_match(self) -> None:
        self._seed("crunchyroll", "Season 1", relations={100: [(200, "sequel")], 200: [(100, "prequel")]})
        target = self._resolve("crunchyroll", {"kind": "original_air", "first": "2021-04-10", "last": "2021-06-26"},
                               self.SIBLINGS, engine_choice=200, engine_auto=True)
        self.assertEqual(100, target.mal_anime_id)

    def test_season_ending_with_the_sequel_resolves_to_part_one(self) -> None:
        self._seed("crunchyroll", "Season 2", relations={100: [(300, "sequel")], 300: [(100, "prequel"), (400, "sequel")], 400: [(300, "prequel")]})
        entries = {100: ("tv", 12, "2018-10-02", "2019-03-19"), 300: ("tv", 12, "2021-01-12", "2021-03-30"),
                   400: ("tv", 12, "2021-07-06", "2021-09-21")}
        target = self._resolve("crunchyroll", {"kind": "original_air", "first": "2021-01-12", "last": "2021-09-21", "count": 24}, entries)
        self.assertEqual(300, target.mal_anime_id)

    def test_hidive_publish_date_picks_latest_tv_entry_over_a_mini_ona(self) -> None:
        self._seed("hidive", "Season 1", relations={100: [(500, "sequel"), (600, "side_story")], 500: [(100, "prequel")], 600: []})
        entries = {100: ("tv", 12, "2023-07-07", "2023-09-22"), 500: ("tv", 12, "2026-04-07", "2026-06-23"),
                   600: ("ona", 12, "2023-07-09", "2023-09-24")}
        target = self._resolve("hidive", {"kind": "provider_publish", "first": "2024-01-25", "last": "2024-04-11"}, entries)
        self.assertEqual(100, target.mal_anime_id)

    def test_ova_season_matches_an_entry_mal_files_as_ona(self) -> None:
        # Production case: Crunchyroll "OVA Season 2" (3 episodes, 2023-11-01)
        # is MAL 54565, which MAL lists as an ONA starting 2023-11-02.
        self._seed("crunchyroll", "OVA Season 2", relations={100: [(800, "side_story")], 800: []})
        entries = {100: ("tv", 24, "2018-10-02", "2019-03-19"), 800: ("ona", 3, "2023-11-02", "2023-11-02")}
        target = self._resolve("crunchyroll", {"kind": "original_air", "first": "2023-11-01", "last": "2023-11-01", "count": 3}, entries)
        self.assertEqual(800, target.mal_anime_id)

    def test_new_catalog_evidence_retries_an_unresolved_season_immediately(self) -> None:
        from mal_updater.config import load_mal_secrets
        from mal_updater.db import connect
        from mal_updater.mapping import MappingResult
        from mal_updater.sync_planner import _resolve_season_target

        self._seed("hidive", "Season 1", relations={100: [(500, "sequel")], 500: [(100, "prequel")]})
        state = next(s for s in load_provider_series_states(self.config, split_seasons=True) if s.provider_season_key == "SX")
        empty = MappingResult(series=None, status="strong", confidence=0.5, chosen_candidate=None, candidates=[], rationale=[])
        client = MalClient(self.config, load_mal_secrets(self.config))
        with patch("mal_updater.sync_planner.map_series", return_value=empty):
            self.assertIsNone(_resolve_season_target(self.config, client, state, search_budget=[1]).mal_anime_id)
        with connect(self.config.db_path) as conn:
            conn.execute("UPDATE provider_season_mappings SET resolved_at = datetime('now', '-1 hour')")
            conn.commit()
        entries = {100: ("tv", 12, "2023-07-07", "2023-09-22"), 500: ("tv", 12, "2026-04-07", "2026-06-23")}
        target = self._resolve("hidive", {"kind": "provider_publish", "first": "2024-01-25", "last": "2024-04-11"}, entries)
        self.assertEqual(100, target.mal_anime_id)

    def test_veto_spares_a_season_inside_a_long_mal_entry_and_keeps_unconfirmed_matches(self) -> None:
        # Production cases: D.Gray-man "Season 3-4" is the second half of one
        # 2006-2008 MAL entry; re-checking an earlier automatic match that the
        # rules cannot confirm keeps it.
        from mal_updater.config import load_mal_secrets
        from mal_updater.season_catalog import ProviderSeasonCatalog
        from mal_updater.sync_planner import _catalog_contradicts

        entries = {100: ("tv", 103, "2006-10-03", "2008-09-30"), 200: ("tv", 12, "2011-01-11", "2011-03-09")}

        def details(anime_id, **_kwargs):
            media, num, start, end = entries[anime_id]
            return {"id": anime_id, "title": "T", "media_type": media, "num_episodes": num, "start_date": start, "end_date": end}

        catalog = ProviderSeasonCatalog(provider="crunchyroll", provider_season_key="SX", title=None, numbered_episode_count=52,
                                        total_episode_count=52, first_date="2007-10-09", last_date="2008-09-30",
                                        date_kind="original_air", complete=True, fetched_at="")
        with patch.object(MalClient, "get_anime_details", side_effect=details):
            client = MalClient(self.config, load_mal_secrets(self.config))
            self.assertFalse(_catalog_contradicts(client, catalog, 100))
            self.assertTrue(_catalog_contradicts(client, catalog, 200))

    def test_split_rule_never_takes_an_entry_the_dates_contradict(self) -> None:
        # Production case (Assassination Classroom): provider season 2 (25
        # episodes, 2016) fit "season 1 (22) + sequel" and was taken as Part 1.
        self._seed("crunchyroll", "Season 2", relations={100: [(300, "sequel")], 300: [(100, "prequel")]})
        entries = {100: ("tv", 22, "2015-01-10", "2015-06-20"), 300: ("tv", 25, "2016-01-08", "2016-06-30")}
        target = self._resolve("crunchyroll", {"kind": "original_air", "first": "2016-01-07", "last": "2016-06-30", "count": 25},
                               entries, engine_choice=300)
        self.assertEqual(300, target.mal_anime_id)

    def test_unconfirmed_earlier_automatic_match_is_kept(self) -> None:
        from mal_updater.season_mappings import get_provider_season_mapping

        self._seed("crunchyroll", "Season 2", relations={100: [(900, "sequel")], 900: [(100, "prequel")]})
        upsert_provider_season_mapping(
            self.config.db_path, provider="crunchyroll", provider_series_id="ser-1", provider_season_key="SX",
            mal_anime_id=900, mapping_source="auto_exact", confidence=0.9, season_title="Season 2", season_number=2,
            resolver_version="older",
        )
        entries = {100: ("tv", 12, "2018-01-01", "2018-03-20"), 900: ("tv", 12, "2021-01-01", "2021-03-20")}
        # The matcher now offers a different, unconfirmable candidate.
        target = self._resolve("crunchyroll", {"kind": "original_air", "first": "2021-02-15", "last": "2021-03-21"}, entries,
                               engine_choice=100)
        self.assertEqual(900, target.mal_anime_id)
        self.assertIn("season_mapping_reresolve_unconfirmed_kept", target.reasons)
        stored = get_provider_season_mapping(self.config.db_path, provider="crunchyroll", provider_series_id="ser-1", provider_season_key="SX")
        self.assertEqual(SEASON_MAPPING_RESOLVER_VERSION, stored.resolver_version)

    def test_unrelated_search_hit_does_not_block_a_franchise_date_match(self) -> None:
        # Production case (HIDIVE Last Boss Queen S1): the title search also
        # returned an unrelated 12-episode 2020 show, which looked like an
        # earlier same-length sibling.
        self._seed("hidive", "Season 1", relations={100: [(500, "sequel")], 500: [(100, "prequel")]})
        entries = {100: ("tv", 12, "2023-07-07", "2023-09-22"), 500: ("tv", 12, "2026-04-07", "2026-06-23"),
                   777: ("tv", 12, "2020-04-05", "2020-06-21")}
        target = self._resolve("hidive", {"kind": "provider_publish", "first": "2024-01-25", "last": "2024-08-15"}, entries,
                               engine_choice=777)
        self.assertEqual(100, target.mal_anime_id)

    def test_batch_dated_dub_catalog_falls_back_to_a_unique_episode_count(self) -> None:
        # Production case (The Promised Neverland): every dub episode dated
        # 2022-03-01; season 1 has 12 episodes and season 2 has 11.
        self._seed("crunchyroll", "Season 1", relations={100: [(200, "sequel")], 200: [(100, "prequel")]})
        entries = {100: ("tv", 12, "2019-01-11", "2019-03-29"), 200: ("tv", 11, "2021-01-08", "2021-03-26")}
        target = self._resolve("crunchyroll", {"kind": "original_air", "first": "2022-03-01", "last": "2022-03-01", "count": 12}, entries)
        self.assertEqual(100, target.mal_anime_id)

    def test_directors_cut_season_matches_its_alternative_version_entry(self) -> None:
        # Production case: Re:Zero "Season 1: Director's Cut" (2020-01-01).
        self._seed("crunchyroll", "Season 1: Director's Cut", relations={100: [(950, "alternative_version")], 950: []})
        entries = {100: ("tv", 25, "2016-04-04", "2016-09-19"), 950: ("tv", 13, "2020-01-01", "2020-03-25")}
        target = self._resolve("crunchyroll", {"kind": "original_air", "first": "2020-01-01", "last": "2020-04-01", "count": 13}, entries)
        self.assertEqual(950, target.mal_anime_id)

    def test_ova_of_a_sequel_reached_through_an_alternative_version_matches(self) -> None:
        # Production case: Steins;Gate (100) reaches Steins;Gate 0 (300) only
        # via "Divide By Zero" (950), an alternative version of the TV series
        # and the prequel of Steins;Gate 0; the Valentine OVA (400, a special
        # from 2018-12-21) is a side story of Steins;Gate 0.
        self._seed("crunchyroll", "OVA", relations={
            100: [(950, "alternative_version")], 950: [(100, "alternative_version"), (300, "sequel")],
            300: [(950, "prequel"), (400, "side_story")], 400: [(300, "parent_story")]})
        entries = {100: ("tv", 24, "2011-04-06", "2011-09-14"), 950: ("special", 1, "2015-06-10", "2015-06-10"),
                   300: ("tv", 23, "2018-04-12", "2018-09-27"), 400: ("special", 1, "2018-12-21", "2018-12-21")}
        target = self._resolve("crunchyroll", {"kind": "original_air", "first": "2018-12-21", "last": "2018-12-21", "count": 1, "total": 1}, entries)
        self.assertEqual(400, target.mal_anime_id)

    def test_dub_dated_single_film_matches_the_franchise_movie(self) -> None:
        # Production case: Konosuba "Legend of Crimson (English Dub)", one
        # item dated by the dub release, MAL movie from 2019.
        self._seed("crunchyroll", "Legend of Crimson (English Dub)", relations={100: [(960, "side_story"), (970, "side_story")], 960: [], 970: []})
        entries = {100: ("tv", 10, "2016-01-14", "2016-03-17"), 960: ("movie", 1, "2019-08-30", "2019-08-30"),
                   970: ("ova", 1, "2016-06-24", "2016-06-24")}
        target = self._resolve("crunchyroll", {"kind": "original_air", "first": "2020-03-25", "last": "2020-03-25", "count": 0, "total": 1}, entries)
        self.assertEqual(960, target.mal_anime_id)

    def test_ova_season_matches_its_side_story_ova_entry(self) -> None:
        self._seed("crunchyroll", "OVA Season 1", relations={100: [(700, "side_story")], 700: []})
        entries = {100: ("tv", 24, "2018-10-02", "2019-03-19"), 700: ("ova", 5, "2019-07-09", "2020-11-27")}
        target = self._resolve("crunchyroll", {"kind": "original_air", "first": "2019-07-09", "last": "2020-11-27", "count": 5}, entries)
        self.assertEqual(700, target.mal_anime_id)

    def test_ona_season_strictly_nearer_than_its_tv_sibling_takes_the_ona(self) -> None:
        # A chibi ONA's own provider season starts 3 days after the TV season:
        # the TV entry is still within tolerance but the ONA airs on that day.
        self._seed("crunchyroll", "Chiichai", relations={100: [(900, "sequel")], 900: [(100, "prequel")]})
        entries = {100: ("tv", 12, "2022-07-14", "2022-09-29"), 900: ("ona", 24, "2022-07-17", "2023-10-01")}
        target = self._resolve("crunchyroll", {"kind": "original_air", "first": "2022-07-17", "last": "2023-10-01", "count": 24}, entries)
        self.assertEqual(900, target.mal_anime_id)

    def test_count_fingerprint_declines_an_entry_another_season_maps_to(self) -> None:
        # Season 1 (24 episodes) is mapped; a batch-dated dub season 2 of 24
        # episodes that MAL splits 12 + 12 must not take season 1's entry.
        self._seed("crunchyroll", "Season 2", relations={100: [(200, "sequel")], 200: [(100, "prequel"), (300, "sequel")], 300: [(200, "prequel")]})
        upsert_provider_season_mapping(
            self.config.db_path, provider="crunchyroll", provider_series_id="ser-1", provider_season_key="S0",
            mal_anime_id=100, mapping_source="auto_exact", confidence=1.0, season_title="Season 0", season_number=0,
            resolver_version=SEASON_MAPPING_RESOLVER_VERSION,
        )
        entries = {100: ("tv", 24, "2019-01-01", "2019-06-20"), 200: ("tv", 12, "2021-01-01", "2021-03-20"),
                   300: ("tv", 12, "2021-07-01", "2021-09-20")}
        target = self._resolve("crunchyroll", {"kind": "original_air", "first": "2022-03-01", "last": "2022-03-01", "count": 24}, entries)
        self.assertIsNone(target.mal_anime_id)

    def test_film_fallback_skips_seasons_titled_as_non_film_extras(self) -> None:
        self._seed("crunchyroll", "OVA", relations={100: [(960, "side_story")], 960: []})
        entries = {100: ("tv", 10, "2016-01-14", "2016-03-17"), 960: ("movie", 1, "2019-08-30", "2019-08-30")}
        target = self._resolve("crunchyroll", {"kind": "original_air", "first": "2020-03-25", "last": "2020-03-25", "count": 0, "total": 1}, entries)
        self.assertIsNone(target.mal_anime_id)

    def test_earlier_tv_match_of_an_extras_season_is_not_kept(self) -> None:
        self._seed("crunchyroll", "OVAs", relations={100: []})
        upsert_provider_season_mapping(
            self.config.db_path, provider="crunchyroll", provider_series_id="ser-1", provider_season_key="SX",
            mal_anime_id=100, mapping_source="auto_exact", confidence=0.9, season_title="OVAs", season_number=2,
            resolver_version="older",
        )
        # A dub-dated catalog does not contradict the TV entry's dates.
        entries = {100: ("tv", 12, "2013-04-07", "2013-09-29")}
        target = self._resolve("crunchyroll", {"kind": "original_air", "first": "2013-06-01", "last": "2013-06-01", "count": 3}, entries,
                               engine_choice=100)
        self.assertIsNone(target.mal_anime_id)
        self.assertNotIn("season_mapping_reresolve_unconfirmed_kept", target.reasons)

    def test_same_week_mini_ona_does_not_take_a_split_tv_season(self) -> None:
        # The Devil is a Part-Timer! season 2 (probed): TV 48413 + sequel 53200
        # span the season; chibi ONA 52970 starts 3 days later and ends with it.
        self._seed("crunchyroll", "Season 2", relations={100: [(300, "sequel"), (900, "side_story")], 300: [(100, "prequel")], 900: []})
        entries = {100: ("tv", 12, "2022-07-14", "2022-09-29"), 300: ("tv", 12, "2023-07-13", "2023-09-28"),
                   900: ("ona", 24, "2022-07-17", "2023-10-01")}
        target = self._resolve("crunchyroll", {"kind": "original_air", "first": "2022-07-14", "last": "2023-09-28", "count": 24}, entries)
        self.assertEqual(100, target.mal_anime_id)
        # Without a sequel link (and watched past its count) the start date still
        # identifies the TV entry; the same-week ONA is never taken, and the
        # planner reviews the overflow.
        self.temp_dir.cleanup()
        self.setUp()
        self._seed("crunchyroll", "Season 2", relations={100: [(900, "side_story")], 900: []})
        entries[100] = ("tv", 10, "2022-07-14", "2022-09-29")
        target = self._resolve("crunchyroll", {"kind": "original_air", "first": "2022-07-14", "last": "2023-09-28", "count": 24}, entries)
        self.assertEqual(100, target.mal_anime_id)

    def test_batch_dated_catalog_does_not_veto_a_title_match(self) -> None:
        # A dub catalog dated by its release (all episodes one day) is not a broadcast window.
        self._seed("crunchyroll", "Season 1", relations={100: [(200, "sequel")], 200: [(100, "prequel")]})
        target = self._resolve("crunchyroll", {"kind": "original_air", "first": "2022-03-01", "last": "2022-03-01"},
                               self.SIBLINGS, engine_choice=100, engine_auto=True)
        self.assertEqual(100, target.mal_anime_id)
        self.assertIn("season_mapping_accepted_by=mapping_engine_auto_approval", target.reasons)

    def test_hidive_back_catalog_publish_date_is_ambiguous_between_equal_siblings(self) -> None:
        # Season 1 published after its equal-length sequel aired: the latest
        # started entry is the sequel, so the date cannot decide.
        self._seed("hidive", "Season 1", relations={100: [(200, "sequel")], 200: [(100, "prequel")]})
        target = self._resolve("hidive", {"kind": "provider_publish", "first": "2025-09-01", "last": "2025-09-10"}, self.SIBLINGS)
        self.assertIsNone(target.mal_anime_id)
        # A simulcast publish date matches the sequel's start and decides.
        self.temp_dir.cleanup()
        self.setUp()
        self._seed("hidive", "Season 2", relations={100: [(200, "sequel")], 200: [(100, "prequel")]})
        target = self._resolve("hidive", {"kind": "provider_publish", "first": "2025-04-06", "last": "2025-06-22"}, self.SIBLINGS)
        self.assertEqual(200, target.mal_anime_id)


if __name__ == "__main__":
    unittest.main()
