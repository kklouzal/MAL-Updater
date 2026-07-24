from __future__ import annotations

import io
import json
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

from mal_updater.cli import main as cli_main
from mal_updater.config import load_config
from mal_updater.db import (
    bootstrap_database,
    connect,
    get_mal_anime_metadata_map,
    insert_recommendation_snapshot_rows,
    list_latest_recommendation_snapshot_rows,
    replace_mal_anime_relations,
    replace_mal_recommendation_edges,
    replace_mal_user_anime_list_cache_generation,
    upsert_recommendation_provider_eligibility_evidence,
    upsert_mal_anime_metadata,
    upsert_series_mapping,
)
from mal_updater.mal_client import MalApiError
from mal_updater.recommendation_metadata import refresh_recommendation_metadata
from mal_updater.recommendations import Recommendation, build_recommendations, group_recommendations, trim_grouped_recommendations


class RecommendationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.project_root = Path(self.temp_dir.name)
        (self.project_root / ".MAL-Updater" / "config").mkdir(parents=True, exist_ok=True)
        self.config = load_config(self.project_root)
        bootstrap_database(self.config.db_path)

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def test_recommendation_snapshot_helper_round_trip(self) -> None:
        inserted = insert_recommendation_snapshot_rows(
            self.config.db_path,
            [
                {
                    "kind": "discovery_candidate",
                    "provider": "crunchyroll",
                    "providers": ["crunchyroll", "hidive"],
                    "provider_series_id": "series-1",
                    "title": "Snapshot Show",
                    "priority": 42,
                    "reasons": ["because"],
                    "scorecard": {"total": 12.5},
                    "context": {"mal_anime_id": 123, "availability_confidence": 0.75, "dub_signal": "dubbed"},
                },
                {
                    "kind": "discovery_candidate",
                    "provider": "mal",
                    "provider_series_id": "mal:456",
                    "title": "Context Only Availability",
                    "priority": 41,
                    "context": {
                        "mal_anime_id": 456,
                        "available_via_providers": ["crunchyroll"],
                        "english_dub_signal": "present",
                        "scorecard": {"total": 11.0},
                    },
                },
            ],
            run_id="run-test",
            generated_at="2026-07-05T17:55:00+00:00",
        )

        self.assertEqual(2, inserted)
        rows = list_latest_recommendation_snapshot_rows(self.config.db_path)
        self.assertEqual(2, len(rows))
        self.assertEqual("run-test", rows[0].run_id)
        self.assertEqual("Snapshot Show", rows[0].title)
        self.assertEqual(123, rows[0].mal_anime_id)
        self.assertEqual(12.5, rows[0].score)
        self.assertEqual(["crunchyroll", "hidive"], rows[0].availability_providers)
        context_only = next(row for row in rows if row.title == "Context Only Availability")
        self.assertEqual(456, context_only.mal_anime_id)
        self.assertEqual(11.0, context_only.score)
        self.assertEqual(["crunchyroll"], context_only.availability_providers)
        self.assertEqual("present", context_only.dub_signal)

    def test_cli_recommend_persist_snapshot_records_grouped_items(self) -> None:
        item = Recommendation(
            kind="discovery_candidate",
            priority=9,
            provider_series_id="mal:999",
            title="Grouped Candidate",
            season_title=None,
            provider="mal",
            reasons=["cached MAL graph support"],
            context={"mal_anime_id": 999, "scorecard": {"total": 33.0}},
        )
        argv = [
            "mal-updater",
            "--project-root",
            str(self.project_root),
            "recommend",
            "--persist-snapshot",
            "--include-dormant",
        ]
        with (
            patch("sys.argv", argv),
            patch("sys.stdout", new_callable=io.StringIO),
            patch("mal_updater.cli.build_recommendations", return_value=[item]),
        ):
            exit_code = cli_main()

        self.assertEqual(0, exit_code)
        rows = list_latest_recommendation_snapshot_rows(self.config.db_path)
        self.assertEqual(1, len(rows))
        self.assertEqual("Grouped Candidate", rows[0].title)
        self.assertEqual("discovery_candidate", rows[0].kind)
        self.assertEqual(33.0, rows[0].score)

    def test_cli_recommend_persist_snapshot_requires_actionable_provider_by_default(self) -> None:
        item = Recommendation(
            kind="discovery_candidate",
            priority=91,
            provider_series_id="mal:1234",
            title="MAL Only Candidate",
            season_title=None,
            provider="mal",
            reasons=["cached MAL graph support"],
            context={"mal_anime_id": 1234, "scorecard": {"total": 40.0}},
        )
        argv = ["mal-updater", "--project-root", str(self.project_root), "recommend", "--persist-snapshot"]
        with (
            patch("sys.argv", argv),
            patch("sys.stdout", new_callable=io.StringIO),
            patch("mal_updater.cli.build_recommendations", return_value=[item]) as build_mock,
        ):
            exit_code = cli_main()

        self.assertEqual(0, exit_code)
        self.assertEqual(True, build_mock.call_args.kwargs["require_provider_availability"])
        self.assertEqual(False, build_mock.call_args.kwargs["include_discovery_candidates_without_actionable_provider_evidence"])
        rows = list_latest_recommendation_snapshot_rows(self.config.db_path)
        self.assertEqual(1, len(rows))
        self.assertEqual("MAL Only Candidate", rows[0].title)
        self.assertEqual("mal", rows[0].provider)

    def test_cli_recommend_persist_snapshot_uses_full_candidate_limit_not_display_sections(self) -> None:
        recommendations = [
            Recommendation(
                kind="discovery_candidate",
                priority=200 - index,
                provider_series_id=f"mal:{index}",
                title=f"MAL Only Candidate {index}",
                season_title=None,
                provider="mal",
                reasons=["cached MAL graph support"],
                context={"mal_anime_id": index, "scorecard": {"total": float(200 - index)}},
            )
            for index in range(1, 83)
        ]
        recommendations.extend(
            Recommendation(
                kind="discovery_candidate",
                priority=100 - index,
                provider_series_id=f"cr:{index}",
                title=f"Available Candidate {index}",
                season_title=None,
                provider="crunchyroll",
                reasons=["provider availability"],
                context={"mal_anime_id": 1000 + index, "available_via_providers": ["crunchyroll"], "english_dub_signal": "present"},
            )
            for index in range(1, 6)
        )
        recommendations.extend(
            Recommendation(
                kind="resume_backlog",
                priority=50 - index,
                provider_series_id=f"hi:{index}",
                title=f"Resume Backlog {index}",
                season_title=None,
                provider="hidive",
                reasons=["resume"],
                context={"scorecard": {"total": float(50 - index)}},
            )
            for index in range(1, 19)
        )
        argv = ["mal-updater", "--project-root", str(self.project_root), "recommend", "--limit", "100", "--persist-snapshot"]
        with (
            patch("sys.argv", argv),
            patch("sys.stdout", new_callable=io.StringIO),
            patch("mal_updater.cli.build_recommendations", return_value=recommendations),
        ):
            exit_code = cli_main()

        self.assertEqual(0, exit_code)
        rows = list_latest_recommendation_snapshot_rows(self.config.db_path, limit=120)
        self.assertEqual(100, len(rows))
        self.assertEqual(82, sum(1 for row in rows if row.provider == "mal" and row.kind == "discovery_candidate"))
        self.assertEqual(5, sum(1 for row in rows if row.provider == "crunchyroll" and row.kind == "discovery_candidate"))
        self.assertGreater(sum(1 for row in rows if row.kind == "resume_backlog"), 0)

    def _insert_series(
        self,
        provider_series_id: str,
        *,
        title: str,
        season_title: str | None = None,
        season_number: int | None = None,
        watchlist_status: str | None = None,
        provider: str = "crunchyroll",
    ) -> None:
        with connect(self.config.db_path) as conn:
            conn.execute(
                """
                INSERT OR IGNORE INTO provider_series (provider, provider_series_id, title, season_title, season_number, raw_json)
                VALUES (?, ?, ?, ?, ?, '{}')
                """,
                (provider, provider_series_id, title, season_title, season_number),
            )
            if watchlist_status is not None:
                conn.execute(
                    """
                    INSERT INTO provider_watchlist (provider, provider_series_id, status, raw_json)
                    VALUES (?, ?, ?, '{}')
                    """,
                    (provider, provider_series_id, watchlist_status),
                )
            conn.commit()

    def _insert_progress(
        self,
        provider_series_id: str,
        provider_episode_id: str,
        *,
        episode_number: int,
        completion_ratio: float,
        last_watched_at: str,
        provider: str = "crunchyroll",
    ) -> None:
        with connect(self.config.db_path) as conn:
            conn.execute(
                """
                INSERT INTO provider_episode_progress (
                    provider,
                    provider_episode_id,
                    provider_series_id,
                    episode_number,
                    completion_ratio,
                    last_watched_at,
                    raw_json
                ) VALUES (?, ?, ?, ?, ?, ?, '{}')
                """,
                (provider, provider_episode_id, provider_series_id, episode_number, completion_ratio, last_watched_at),
            )
            conn.commit()

    def _map_series(self, provider_series_id: str, mal_anime_id: int, *, provider: str = "crunchyroll") -> None:
        upsert_series_mapping(
            self.config.db_path,
            provider=provider,
            provider_series_id=provider_series_id,
            mal_anime_id=mal_anime_id,
            confidence=1.0,
            mapping_source="user_approved",
            approved_by_user=True,
            notes=None,
        )

    def _cache_metadata(
        self,
        mal_anime_id: int,
        *,
        title: str,
        title_english: str | None = None,
        mean: float | None = None,
        popularity: int | None = None,
        genres: list[str] | None = None,
        studios: list[str] | None = None,
        source: str | None = None,
        start_season: dict | None = None,
        my_list_status: dict | None = None,
        num_episodes: int = 12,
    ) -> None:
        raw = {"id": mal_anime_id, "title": title}
        if genres:
            raw["genres"] = [{"name": genre} for genre in genres]
        if studios:
            raw["studios"] = [{"name": studio} for studio in studios]
        if source:
            raw["source"] = source
        if my_list_status:
            raw["my_list_status"] = my_list_status
        upsert_mal_anime_metadata(
            self.config.db_path,
            mal_anime_id=mal_anime_id,
            title=title,
            title_english=title_english,
            title_japanese=None,
            alternative_titles=[],
            media_type="tv",
            status="finished_airing",
            num_episodes=num_episodes,
            mean=mean,
            popularity=popularity,
            start_season=start_season,
            raw=raw,
        )

    def _cache_relations(self, mal_anime_id: int, relations: list[dict]) -> None:
        replace_mal_anime_relations(self.config.db_path, mal_anime_id=mal_anime_id, relations=relations)

    def _cache_recommendations(self, mal_anime_id: int, edges: list[dict], *, make_targets_available: bool = True) -> None:
        replace_mal_recommendation_edges(self.config.db_path, source_mal_anime_id=mal_anime_id, hop_distance=1, edges=edges)
        if not make_targets_available:
            return
        for edge in edges:
            target_id = edge.get("target_mal_anime_id")
            target_title = edge.get("target_title") or f"MAL {target_id}"
            if target_id is None:
                continue
            provider_series_id = f"available-target-{target_id}"
            self._insert_series(
                provider_series_id,
                title=target_title,
                season_title=f"{target_title} (English Dub)",
                watchlist_status=None,
            )
            self._map_series(provider_series_id, int(target_id))
            self._cache_provider_eligibility(int(target_id), provider_series_id=provider_series_id, provider_title=target_title)

    def _cache_provider_eligibility(
        self,
        mal_anime_id: int,
        *,
        provider: str = "crunchyroll",
        provider_series_id: str | None = None,
        provider_title: str | None = None,
        identity_match_kind: str = "approved_mapping",
        review_status: str = "verified",
        catalog_status: str = "present",
        english_dub_status: str = "present",
        audio_locales: list[str] | None = None,
        expires_at: str = "2099-01-01T00:00:00Z",
    ) -> None:
        upsert_recommendation_provider_eligibility_evidence(
            self.config.db_path,
            mal_anime_id=mal_anime_id,
            provider=provider,
            provider_series_id=provider_series_id or f"{provider}-{mal_anime_id}",
            provider_title=provider_title or f"Provider {mal_anime_id}",
            provider_url=f"https://example.test/{provider}/{mal_anime_id}",
            identity_match_kind=identity_match_kind,
            match_confidence=0.99,
            review_status=review_status,
            catalog_status=catalog_status,
            english_dub_status=english_dub_status,
            explicit_dub_evidence_source="provider_audio_locale",
            audio_locales=audio_locales if audio_locales is not None else ["en-US", "ja-JP"],
            source_evidence={"test": "provider_eligibility"},
            fetched_at="2026-07-19T00:00:00Z",
            expires_at=expires_at,
            last_verified_at="2026-07-19T00:00:00Z" if review_status == "verified" else None,
        )

    def _cache_mal_list_status(self, mal_anime_id: int, title: str, status: str, *, score: int = 0) -> None:
        replace_mal_user_anime_list_cache_generation(
            self.config.db_path,
            items=[
                {
                    "node": {"id": mal_anime_id, "title": title},
                    "list_status": {"status": status, "score": score, "num_episodes_watched": 12 if status == "completed" else 0},
                }
            ],
            refresh_run_id=f"list-{mal_anime_id}",
            fetched_at="2026-07-19T00:00:00Z",
            prune_absent=False,
        )

    def test_new_dubbed_episode_recommendation_detects_contiguous_tail_gap(self) -> None:
        self._insert_series(
            "series-1",
            title="Example Show",
            season_title="Example Show Season 2 (English Dub)",
            season_number=2,
            watchlist_status="in_progress",
        )
        recent = (datetime.now(timezone.utc) - timedelta(days=1)).replace(microsecond=0).isoformat().replace("+00:00", "Z")
        self._insert_progress("series-1", "ep-1", episode_number=1, completion_ratio=1.0, last_watched_at=recent)
        self._insert_progress("series-1", "ep-2", episode_number=2, completion_ratio=1.0, last_watched_at=recent)
        self._insert_progress("series-1", "ep-3", episode_number=3, completion_ratio=0.2, last_watched_at=recent)

        results = build_recommendations(
            self.config,
            limit=0,
            require_provider_availability=False,
            include_discovery_candidates_without_actionable_provider_evidence=True,
        )

        self.assertEqual(1, len(results))
        item = results[0]
        self.assertEqual("new_dubbed_episode", item.kind)
        self.assertEqual("series-1", item.provider_series_id)
        self.assertEqual(1, item.context["contiguous_tail_gap"])

    def test_completed_mal_target_suppresses_continuation_noise(self) -> None:
        self._insert_series(
            "series-1",
            title="Completed Show",
            season_title="Completed Show (English Dub)",
            watchlist_status="fully_watched",
        )
        self._map_series("series-1", 101)
        self._cache_metadata(
            101,
            title="Completed Show",
            my_list_status={"status": "completed", "num_episodes_watched": 12},
            num_episodes=12,
        )
        recent = (datetime.now(timezone.utc) - timedelta(days=1)).replace(microsecond=0).isoformat().replace("+00:00", "Z")
        self._insert_progress("series-1", "ep-1", episode_number=1, completion_ratio=1.0, last_watched_at=recent)
        self._insert_progress("series-1", "ep-2", episode_number=2, completion_ratio=1.0, last_watched_at=recent)
        self._insert_progress("series-1", "ep-3", episode_number=3, completion_ratio=0.2, last_watched_at=recent)

        results = build_recommendations(self.config, limit=0)

        self.assertEqual([], [item for item in results if item.provider_series_id == "series-1"])

    def test_incomplete_watching_mal_target_keeps_continuation(self) -> None:
        self._insert_series(
            "series-1",
            title="Watching Show",
            season_title="Watching Show (English Dub)",
            watchlist_status="in_progress",
        )
        self._map_series("series-1", 102)
        self._cache_metadata(
            102,
            title="Watching Show",
            my_list_status={"status": "watching", "num_episodes_watched": 2},
            num_episodes=12,
        )
        recent = (datetime.now(timezone.utc) - timedelta(days=1)).replace(microsecond=0).isoformat().replace("+00:00", "Z")
        self._insert_progress("series-1", "ep-1", episode_number=1, completion_ratio=1.0, last_watched_at=recent)
        self._insert_progress("series-1", "ep-2", episode_number=2, completion_ratio=1.0, last_watched_at=recent)
        self._insert_progress("series-1", "ep-3", episode_number=3, completion_ratio=0.2, last_watched_at=recent)

        item = build_recommendations(self.config, limit=0)[0]

        self.assertEqual("new_dubbed_episode", item.kind)
        self.assertEqual("watching", item.context["mal_watch_status"])
        self.assertEqual(2, item.context["mal_num_episodes_watched"])

    def test_missing_mal_watch_metadata_keeps_continuation_with_uncertainty(self) -> None:
        self._insert_series(
            "series-1",
            title="Metadata Missing Show",
            season_title="Metadata Missing Show (English Dub)",
            watchlist_status="in_progress",
        )
        self._map_series("series-1", 103)
        self._cache_metadata(103, title="Metadata Missing Show")
        recent = (datetime.now(timezone.utc) - timedelta(days=1)).replace(microsecond=0).isoformat().replace("+00:00", "Z")
        self._insert_progress("series-1", "ep-1", episode_number=1, completion_ratio=1.0, last_watched_at=recent)
        self._insert_progress("series-1", "ep-2", episode_number=2, completion_ratio=1.0, last_watched_at=recent)
        self._insert_progress("series-1", "ep-3", episode_number=3, completion_ratio=0.2, last_watched_at=recent)

        item = build_recommendations(self.config, limit=0)[0]

        self.assertEqual("new_dubbed_episode", item.kind)
        self.assertTrue(item.context["mal_watch_metadata_uncertain"])
        self.assertIn("avoiding over-suppression", item.context["mal_inclusion_rationale"])

    def test_new_season_recommendation_detects_completed_predecessor(self) -> None:
        self._insert_series(
            "season-1",
            title="Franchise Show",
            season_title="Franchise Show (English Dub)",
            season_number=1,
            watchlist_status="fully_watched",
        )
        self._insert_progress("season-1", "s1-ep-1", episode_number=1, completion_ratio=1.0, last_watched_at="2026-03-01T01:00:00Z")
        self._insert_progress("season-1", "s1-ep-2", episode_number=2, completion_ratio=1.0, last_watched_at="2026-03-01T02:00:00Z")

        self._insert_series(
            "season-2",
            title="Franchise Show",
            season_title="Franchise Show Season 2 (English Dub)",
            season_number=2,
            watchlist_status="never_watched",
        )

        results = build_recommendations(self.config, limit=0)

        self.assertEqual(1, len(results))
        item = results[0]
        self.assertEqual("new_season", item.kind)
        self.assertEqual("season-2", item.provider_series_id)
        self.assertEqual("season-1", item.context["predecessor_provider_series_id"])

    def test_relation_backed_new_season_recommendation_uses_hidive_mapping_context(self) -> None:
        self._insert_series(
            "hidive-s1",
            title="HIDIVE Show",
            season_title="HIDIVE Show (English Dub)",
            watchlist_status="fully_watched",
            provider="hidive",
        )
        self._insert_progress(
            "hidive-s1",
            "hidive-s1-ep-1",
            episode_number=1,
            completion_ratio=1.0,
            last_watched_at="2026-03-01T01:00:00Z",
            provider="hidive",
        )
        self._insert_series(
            "hidive-s2",
            title="HIDIVE Show Season 2",
            season_title="HIDIVE Show Season 2 (English Dub)",
            watchlist_status="never_watched",
            provider="hidive",
        )
        self._map_series("hidive-s1", 5100, provider="hidive")
        self._map_series("hidive-s2", 5200, provider="hidive")
        self._cache_metadata(5100, title="HIDIVE Show", my_list_status={"status": "completed", "num_episodes_watched": 12})
        self._cache_metadata(5200, title="HIDIVE Show Season 2", my_list_status={"status": "plan_to_watch", "num_episodes_watched": 0})
        self._cache_relations(
            5100,
            [
                {
                    "related_mal_anime_id": 5200,
                    "relation_type": "sequel",
                    "relation_type_formatted": "Sequel",
                    "related_title": "HIDIVE Show Season 2",
                    "raw": {"relation_type": "sequel", "node": {"id": 5200, "title": "HIDIVE Show Season 2"}},
                }
            ],
        )

        results = build_recommendations(self.config, limit=0)

        items = [item for item in results if item.provider == "hidive" and item.provider_series_id == "hidive-s2" and item.kind == "new_season"]
        self.assertEqual(1, len(items))
        self.assertEqual("hidive", items[0].context["provider"])
        self.assertEqual("hidive-s1", items[0].context["predecessor_provider_series_id"])

    def test_discovery_candidate_can_be_seeded_from_hidive_mapping(self) -> None:
        self._insert_series(
            "hidive-seed",
            title="HIDIVE Seed",
            season_title="HIDIVE Seed (English Dub)",
            watchlist_status="fully_watched",
            provider="hidive",
        )
        self._insert_progress(
            "hidive-seed",
            "hidive-seed-1",
            episode_number=1,
            completion_ratio=1.0,
            last_watched_at="2026-03-01T01:00:00Z",
            provider="hidive",
        )
        self._map_series("hidive-seed", 7100, provider="hidive")
        self._cache_metadata(7100, title="HIDIVE Seed", genres=["Sci-Fi"])
        self._cache_metadata(7200, title="HIDIVE Discovery", genres=["Sci-Fi"], mean=8.2, popularity=300)
        self._cache_recommendations(7100, [{"target_mal_anime_id": 7200, "target_title": "HIDIVE Discovery", "num_recommendations": 18, "raw": {}}])

        results = build_recommendations(self.config, limit=0)

        discovery = [item for item in results if item.kind == "discovery_candidate" and item.provider_series_id == "available-target-7200"]
        self.assertEqual(1, len(discovery))
        self.assertEqual("crunchyroll", discovery[0].provider)
        self.assertEqual([7100], discovery[0].context["supporting_mal_anime_ids"])

    def test_discovery_candidate_exports_scorecard_provider_tags_and_dub_signal(self) -> None:
        self._insert_series(
            "seed-score",
            title="Seed Score",
            season_title="Seed Score (English Dub)",
            watchlist_status="fully_watched",
        )
        self._insert_progress("seed-score", "seed-score-1", episode_number=1, completion_ratio=1.0, last_watched_at="2026-03-01T01:00:00Z")
        self._map_series("seed-score", 8100)
        self._insert_series(
            "candidate-hidive",
            title="Scorecard Discovery",
            season_title="Scorecard Discovery (English Dub)",
            watchlist_status="never_watched",
            provider="hidive",
        )
        self._map_series("candidate-hidive", 8200, provider="hidive")
        self._cache_metadata(8100, title="Seed Score", genres=["Adventure"], mean=9.0)
        self._cache_metadata(8200, title="Scorecard Discovery", genres=["Adventure"], mean=8.4, popularity=220)
        self._cache_recommendations(8100, [{"target_mal_anime_id": 8200, "target_title": "Scorecard Discovery", "num_recommendations": 24, "raw": {}}], make_targets_available=False)

        results = build_recommendations(
            self.config,
            limit=0,
            require_provider_availability=False,
            include_discovery_candidates_without_actionable_provider_evidence=True,
        )

        item = next(item for item in results if item.kind == "discovery_candidate" and item.context["mal_anime_id"] == 8200)
        scorecard = item.context["scorecard"]
        self.assertEqual(scorecard, item.as_dict()["scorecard"])
        self.assertEqual(scorecard["total"], item.as_dict()["scorecard_total"])
        self.assertIn("consensus", scorecard["components"])
        self.assertTrue(all(0 <= value <= 100 for value in scorecard["components"].values()))
        self.assertEqual({"crunchyroll": False, "hidive": True}, item.context["provider_availability_tags"])
        self.assertEqual("unknown", item.context["english_dub_signal"])
        self.assertEqual("unknown", scorecard["features"]["english_dub_signal"])
        self.assertIn("Scorecard Discovery", item.title)

    def test_provider_required_discovery_hides_dormant_unavailable_candidates(self) -> None:
        self._insert_series(
            "seed-a",
            title="Seed A",
            season_title="Seed A (English Dub)",
            watchlist_status="fully_watched",
        )
        self._insert_progress("seed-a", "seed-a-1", episode_number=1, completion_ratio=1.0, last_watched_at="2026-03-01T01:00:00Z")
        self._map_series("seed-a", 100)
        self._cache_metadata(100, title="Seed A")
        self._cache_metadata(300, title="Dormant Candidate", mean=8.1, popularity=250)
        self._cache_recommendations(100, [
            {"target_mal_anime_id": 300, "target_title": "Dormant Candidate", "num_recommendations": 20, "raw": {}},
        ], make_targets_available=False)

        visible = [
            item
            for item in build_recommendations(self.config, limit=0, require_provider_availability=True)
            if item.kind == "discovery_candidate"
        ]
        dormant = [
            item
            for item in build_recommendations(
                self.config,
                limit=0,
                require_provider_availability=False,
                include_discovery_candidates_without_actionable_provider_evidence=True,
            )
            if item.kind == "discovery_candidate"
        ]

        self.assertEqual([], visible)
        self.assertEqual(1, len(dormant))
        self.assertEqual("mal:300", dormant[0].provider_series_id)
        self.assertEqual("mal", dormant[0].provider)
        self.assertFalse(dormant[0].context["availability_visible"])
        self.assertEqual([], dormant[0].context["available_via_providers"])
        self.assertEqual("none", dormant[0].context["availability_confidence"])
        self.assertEqual("unknown", dormant[0].context["english_dub_signal"])

    def test_provider_required_discovery_uses_only_actionable_normalized_eligibility_evidence(self) -> None:
        self._insert_series("seed-actionable", title="Seed Actionable", season_title="Seed Actionable (English Dub)", watchlist_status="fully_watched")
        self._insert_progress("seed-actionable", "seed-actionable-1", episode_number=1, completion_ratio=1.0, last_watched_at="2026-03-01T01:00:00Z")
        self._map_series("seed-actionable", 100)
        self._cache_metadata(100, title="Seed Actionable", my_list_status={"status": "completed", "score": 9, "num_episodes_watched": 12})
        targets = {
            301: "Verified Dubbed Candidate",
            302: "Review Needed Candidate",
            303: "Title Alias Candidate",
            304: "Expired Candidate",
            305: "Non English Candidate",
            306: "Stale Candidate",
        }
        for mal_id, title in targets.items():
            self._cache_metadata(mal_id, title=title, mean=8.0, popularity=300)
        self._cache_recommendations(
            100,
            [
                {"target_mal_anime_id": mal_id, "target_title": title, "num_recommendations": 15, "raw": {}}
                for mal_id, title in targets.items()
            ],
            make_targets_available=False,
        )
        self._cache_provider_eligibility(301, provider_title="Verified Dubbed Candidate")
        self._cache_provider_eligibility(302, review_status="review-needed", catalog_status="unknown", english_dub_status="unknown")
        self._cache_provider_eligibility(303, identity_match_kind="title_alias")
        self._cache_provider_eligibility(304, expires_at="2000-01-01T00:00:00Z")
        self._cache_provider_eligibility(305, audio_locales=["ja-JP"])
        self._cache_provider_eligibility(306, review_status="stale", catalog_status="stale", english_dub_status="stale")

        strict = [
            item
            for item in build_recommendations(self.config, limit=0, require_provider_availability=True)
            if item.kind == "discovery_candidate"
        ]
        internal = [
            item
            for item in build_recommendations(
                self.config,
                limit=0,
                require_provider_availability=False,
                include_discovery_candidates_without_actionable_provider_evidence=True,
            )
            if item.kind == "discovery_candidate"
        ]

        self.assertEqual([301], [item.context["mal_anime_id"] for item in strict])
        self.assertEqual(sorted(targets), sorted(item.context["mal_anime_id"] for item in internal))
        item = strict[0]
        self.assertEqual("crunchyroll", item.provider)
        self.assertEqual("present", item.context["english_dub_signal"])
        self.assertEqual("verified_provider_eligibility", item.context["availability_confidence"])
        self.assertEqual("Verified Dubbed Candidate", item.title)
        self.assertIn("watchable dubbed as Verified Dubbed Candidate", item.context["why_recommended"])
        self.assertEqual("Verified Dubbed Candidate", item.context["available_provider_series"][0]["title"])
        self.assertEqual("provider_audio_locale", item.context["provider_eligibility_evidence"][0]["explicit_dub_evidence_source"])
        self.assertEqual("Seed Actionable", item.context["supporting_seed_details"][0]["title"])
        self.assertEqual(9, item.context["supporting_seed_details"][0]["user_score"])

    def test_diagnostic_discovery_carries_review_needed_provider_catalog_and_dub_evidence(self) -> None:
        self._insert_series("seed-review-evidence", title="Seed Review Evidence", season_title="Seed Review Evidence (English Dub)", watchlist_status="fully_watched")
        self._insert_progress("seed-review-evidence", "seed-review-evidence-1", episode_number=1, completion_ratio=1.0, last_watched_at="2026-03-01T01:00:00Z")
        self._map_series("seed-review-evidence", 100)
        self._cache_metadata(100, title="Seed Review Evidence", my_list_status={"status": "completed", "score": 9, "num_episodes_watched": 12})
        self._cache_metadata(302, title="Review Needed Candidate", mean=8.0, popularity=300)
        self._cache_recommendations(
            100,
            [{"target_mal_anime_id": 302, "target_title": "Review Needed Candidate", "num_recommendations": 15, "raw": {}}],
            make_targets_available=False,
        )
        self._cache_provider_eligibility(
            302,
            provider_series_id="cr-review-302",
            provider_title="Review Needed Candidate",
            identity_match_kind="provider_title_search",
            review_status="review-needed",
            catalog_status="present",
            english_dub_status="present",
        )

        strict = [
            item
            for item in build_recommendations(self.config, limit=0, require_provider_availability=True)
            if item.kind == "discovery_candidate"
        ]
        diagnostic = [
            item
            for item in build_recommendations(
                self.config,
                limit=0,
                require_provider_availability=False,
                include_discovery_candidates_without_actionable_provider_evidence=True,
            )
            if item.kind == "discovery_candidate"
        ]

        self.assertEqual([], strict)
        self.assertEqual(1, len(diagnostic))
        evidence = diagnostic[0].context["provider_eligibility_evidence"]
        self.assertEqual("mal", diagnostic[0].provider)
        self.assertEqual("provider_title_search", evidence[0]["identity_match_kind"])
        self.assertEqual("review-needed", evidence[0]["review_status"])
        self.assertEqual("present", evidence[0]["catalog_status"])
        self.assertEqual("present", evidence[0]["english_dub_status"])

    def test_provider_required_discovery_merges_multiple_actionable_providers(self) -> None:
        self._insert_series("seed-multi", title="Seed Multi", season_title="Seed Multi (English Dub)", watchlist_status="fully_watched")
        self._insert_progress("seed-multi", "seed-multi-1", episode_number=1, completion_ratio=1.0, last_watched_at="2026-03-01T01:00:00Z")
        self._map_series("seed-multi", 100)
        self._cache_metadata(100, title="Seed Multi")
        self._cache_metadata(330, title="Multi Provider Candidate", mean=8.4, popularity=200)
        self._cache_recommendations(100, [{"target_mal_anime_id": 330, "target_title": "Multi Provider Candidate", "num_recommendations": 20, "raw": {}}], make_targets_available=False)
        self._cache_provider_eligibility(330, provider="crunchyroll", provider_series_id="cr-330", provider_title="Multi Provider Candidate")
        self._cache_provider_eligibility(330, provider="hidive", provider_series_id="hi-330", provider_title="Multi Provider Candidate")

        item = next(item for item in build_recommendations(self.config, limit=0, require_provider_availability=True) if item.kind == "discovery_candidate")

        self.assertEqual(["crunchyroll", "hidive"], item.context["available_via_providers"])
        self.assertEqual({"crunchyroll": True, "hidive": True}, item.context["provider_availability_tags"])
        self.assertEqual(2, len(item.context["provider_eligibility_evidence"]))

    def test_discovery_candidate_surfaces_provider_availability_when_dub_unknown(self) -> None:
        self._insert_series(
            "seed-unknown-dub",
            title="Seed Unknown Dub",
            season_title="Seed Unknown Dub (English Dub)",
            watchlist_status="fully_watched",
        )
        self._insert_progress(
            "seed-unknown-dub",
            "seed-unknown-dub-1",
            episode_number=1,
            completion_ratio=1.0,
            last_watched_at="2026-03-01T01:00:00Z",
        )
        self._map_series("seed-unknown-dub", 100)
        self._insert_series(
            "hidive-unknown-dub",
            title="Provider Visible Discovery",
            season_title="Provider Visible Discovery",
            watchlist_status="never_watched",
            provider="hidive",
        )
        upsert_series_mapping(
            self.config.db_path,
            provider="hidive",
            provider_series_id="hidive-unknown-dub",
            mal_anime_id=300,
            confidence=0.91,
            mapping_source="reverse_provider_title_search",
            approved_by_user=False,
            notes=None,
        )
        self._cache_metadata(100, title="Seed Unknown Dub", genres=["Adventure"], mean=8.8)
        self._cache_metadata(300, title="Provider Visible Discovery", genres=["Adventure"], mean=8.1, popularity=250)
        self._cache_recommendations(
            100,
            [{"target_mal_anime_id": 300, "target_title": "Provider Visible Discovery", "num_recommendations": 20, "raw": {}}],
            make_targets_available=False,
        )

        visible = [
            item
            for item in build_recommendations(self.config, limit=0, require_provider_availability=False)
            if item.kind == "discovery_candidate" and item.context["mal_anime_id"] == 300
        ]
        require_dub = [
            item
            for item in build_recommendations(self.config, limit=0, require_provider_availability=True)
            if item.kind == "discovery_candidate" and item.context["mal_anime_id"] == 300
        ]

        self.assertEqual([], require_dub)
        self.assertEqual(1, len(visible))
        item = visible[0]
        self.assertEqual("hidive", item.provider)
        self.assertEqual("hidive-unknown-dub", item.provider_series_id)
        self.assertEqual(["hidive"], item.context["available_via_providers"])
        self.assertEqual({"crunchyroll": False, "hidive": True}, item.context["provider_availability_tags"])
        self.assertEqual("unknown", item.context["english_dub_signal"])
        self.assertFalse(item.context.get("english_dub", False))
        self.assertEqual("mapped", item.context["availability_confidence"])
        self.assertEqual(["mapped_mal"], item.context["availability_match_kinds"])
        self.assertEqual(1, len(item.context["available_provider_series"]))
        self.assertEqual("reverse_provider_title_search", item.context["available_provider_series"][0].get("mapping_source", "reverse_provider_title_search"))
        self.assertEqual("mapped_mal", item.context["available_provider_series"][0]["availability_match_kind"])

    def test_relation_backed_new_season_recommendation_detects_title_drift(self) -> None:
        self._insert_series(
            "sg",
            title="Steins;Gate",
            season_title="Steins;Gate (English Dub)",
            watchlist_status="fully_watched",
        )
        self._insert_progress("sg", "sg-1", episode_number=1, completion_ratio=1.0, last_watched_at="2026-03-01T01:00:00Z")
        self._insert_progress("sg", "sg-2", episode_number=2, completion_ratio=1.0, last_watched_at="2026-03-01T02:00:00Z")
        self._insert_series(
            "sg0",
            title="Steins;Gate 0",
            season_title="Steins;Gate 0 (English Dub)",
            watchlist_status="never_watched",
        )
        self._map_series("sg", 9253)
        self._map_series("sg0", 30484)
        self._cache_metadata(9253, title="Steins;Gate")
        self._cache_metadata(30484, title="Steins;Gate 0")
        self._cache_relations(
            9253,
            [
                {
                    "related_mal_anime_id": 30484,
                    "relation_type": "sequel",
                    "relation_type_formatted": "Sequel",
                    "related_title": "Steins;Gate 0",
                    "raw": {"relation_type": "sequel", "node": {"id": 30484, "title": "Steins;Gate 0"}},
                }
            ],
        )

        results = build_recommendations(self.config, limit=0)

        items = [item for item in results if item.provider_series_id == "sg0" and item.kind == "new_season"]
        self.assertEqual(1, len(items))
        self.assertTrue(items[0].context["relation_backed"])
        self.assertEqual("sg", items[0].context["predecessor_provider_series_id"])

    def test_new_season_recommendation_detects_roman_numeral_installments(self) -> None:
        self._insert_series(
            "overlord-2",
            title="Overlord",
            season_title="Overlord II (English Dub)",
            watchlist_status="fully_watched",
        )
        self._insert_progress("overlord-2", "ol2-ep-1", episode_number=1, completion_ratio=1.0, last_watched_at="2026-03-01T01:00:00Z")
        self._insert_progress("overlord-2", "ol2-ep-2", episode_number=2, completion_ratio=1.0, last_watched_at="2026-03-01T02:00:00Z")

        self._insert_series(
            "overlord-3",
            title="Overlord",
            season_title="Overlord III (English Dub)",
            watchlist_status="never_watched",
        )

        results = build_recommendations(self.config, limit=0)

        roman_items = [item for item in results if item.provider_series_id == "overlord-3"]
        self.assertEqual(1, len(roman_items))
        item = roman_items[0]
        self.assertEqual("new_season", item.kind)
        self.assertEqual("overlord-2", item.context["predecessor_provider_series_id"])
        self.assertEqual(3, item.context["installment_index"])

    def test_new_season_recommendation_detects_ordinal_season_naming(self) -> None:
        self._insert_series(
            "show-1",
            title="Ordinal Show",
            season_title="Ordinal Show (English Dub)",
            season_number=1,
            watchlist_status="fully_watched",
        )
        self._insert_progress("show-1", "show1-ep-1", episode_number=1, completion_ratio=1.0, last_watched_at="2026-03-02T01:00:00Z")
        self._insert_progress("show-1", "show1-ep-2", episode_number=2, completion_ratio=1.0, last_watched_at="2026-03-02T02:00:00Z")

        self._insert_series(
            "show-2",
            title="Ordinal Show",
            season_title="Ordinal Show Second Season (English Dub)",
            watchlist_status="never_watched",
        )

        results = build_recommendations(self.config, limit=0)

        ordinal_items = [item for item in results if item.provider_series_id == "show-2"]
        self.assertEqual(1, len(ordinal_items))
        item = ordinal_items[0]
        self.assertEqual("new_season", item.kind)
        self.assertEqual("show-1", item.context["predecessor_provider_series_id"])
        self.assertEqual(2, item.context["installment_index"])

    def test_new_season_recommendation_detects_part_style_names_only_with_season_context(self) -> None:
        self._insert_series(
            "final-part-1",
            title="Split Show",
            season_title="Split Show Final Season Part 1 (English Dub)",
            watchlist_status="fully_watched",
        )
        self._insert_progress(
            "final-part-1",
            "split-ep-1",
            episode_number=1,
            completion_ratio=1.0,
            last_watched_at="2026-03-03T01:00:00Z",
        )
        self._insert_progress(
            "final-part-1",
            "split-ep-2",
            episode_number=2,
            completion_ratio=1.0,
            last_watched_at="2026-03-03T02:00:00Z",
        )

        self._insert_series(
            "final-part-2",
            title="Split Show",
            season_title="Split Show Final Season Part 2 (English Dub)",
            watchlist_status="never_watched",
        )
        self._insert_series(
            "bare-part-2",
            title="Split Show",
            season_title="Split Show Part 2 (English Dub)",
            watchlist_status="never_watched",
        )

        results = build_recommendations(self.config, limit=0)

        final_items = [item for item in results if item.provider_series_id == "final-part-2"]
        self.assertEqual(1, len(final_items))
        item = final_items[0]
        self.assertEqual("new_season", item.kind)
        self.assertEqual("final-part-1", item.context["predecessor_provider_series_id"])
        self.assertEqual(2, item.context["installment_index"])

        bare_items = [item for item in results if item.provider_series_id == "bare-part-2"]
        self.assertEqual([], bare_items)

    def test_part_style_detection_ignores_titles_that_only_contain_season_word(self) -> None:
        self._insert_series(
            "title-season-part-1",
            title="Split Season Show",
            season_title="Split Season Show Final Season Part 1 (English Dub)",
            watchlist_status="fully_watched",
        )
        self._insert_progress(
            "title-season-part-1",
            "title-season-ep-1",
            episode_number=1,
            completion_ratio=1.0,
            last_watched_at="2026-03-04T01:00:00Z",
        )
        self._insert_progress(
            "title-season-part-1",
            "title-season-ep-2",
            episode_number=2,
            completion_ratio=1.0,
            last_watched_at="2026-03-04T02:00:00Z",
        )

        self._insert_series(
            "title-season-bare-part-2",
            title="Split Season Show",
            season_title="Split Season Show Part 2 (English Dub)",
            watchlist_status="never_watched",
        )

        results = build_recommendations(self.config, limit=0)

        bare_items = [item for item in results if item.provider_series_id == "title-season-bare-part-2"]
        self.assertEqual([], bare_items)

    def test_suppresses_skipped_episode_artifacts_that_are_not_tail_gaps(self) -> None:
        self._insert_series(
            "series-skip",
            title="Skip Show",
            season_title="Skip Show (English Dub)",
            season_number=1,
            watchlist_status="fully_watched",
        )
        self._insert_progress("series-skip", "ep-1", episode_number=1, completion_ratio=1.0, last_watched_at="2026-03-10T01:00:00Z")
        self._insert_progress("series-skip", "ep-2", episode_number=2, completion_ratio=0.1, last_watched_at="2026-03-10T02:00:00Z")
        self._insert_progress("series-skip", "ep-3", episode_number=3, completion_ratio=1.0, last_watched_at="2026-03-10T03:00:00Z")

        results = build_recommendations(self.config, limit=0)

        self.assertEqual([], [item for item in results if item.provider_series_id == "series-skip"])

    def test_suppresses_fully_watched_series_when_latest_episode_is_effectively_complete(self) -> None:
        self._insert_series(
            "series-complete",
            title="Complete Show",
            season_title="Complete Show (English Dub)",
            season_number=1,
            watchlist_status="fully_watched",
        )
        self._insert_progress("series-complete", "ep-1", episode_number=1, completion_ratio=1.0, last_watched_at="2026-03-10T01:00:00Z")
        self._insert_progress("series-complete", "ep-2", episode_number=2, completion_ratio=0.99995, last_watched_at="2026-03-10T02:00:00Z")

        results = build_recommendations(self.config, limit=0)

        self.assertEqual([], [item for item in results if item.provider_series_id == "series-complete"])

    def test_more_recent_in_progress_tail_gap_ranks_above_stale_one(self) -> None:
        now = datetime.now(timezone.utc).replace(microsecond=0)
        recent_base = now - timedelta(days=14)
        stale_base = now - timedelta(days=365 * 3)

        self._insert_series(
            "recent-show",
            title="Recent Show",
            season_title="Recent Show (English Dub)",
            watchlist_status="in_progress",
        )
        self._insert_progress("recent-show", "r1", episode_number=1, completion_ratio=1.0, last_watched_at=(recent_base - timedelta(hours=1)).isoformat().replace("+00:00", "Z"))
        self._insert_progress("recent-show", "r2", episode_number=2, completion_ratio=0.2, last_watched_at=recent_base.isoformat().replace("+00:00", "Z"))

        self._insert_series(
            "stale-show",
            title="Stale Show",
            season_title="Stale Show (English Dub)",
            watchlist_status="in_progress",
        )
        self._insert_progress("stale-show", "s1", episode_number=1, completion_ratio=1.0, last_watched_at=(stale_base - timedelta(hours=1)).isoformat().replace("+00:00", "Z"))
        self._insert_progress("stale-show", "s2", episode_number=2, completion_ratio=0.2, last_watched_at=stale_base.isoformat().replace("+00:00", "Z"))

        results = build_recommendations(self.config, limit=0)

        recent = [item for item in results if item.provider_series_id == "recent-show"][0]
        stale = [item for item in results if item.provider_series_id == "stale-show"][0]
        self.assertEqual("new_dubbed_episode", recent.kind)
        self.assertEqual("resume_backlog", stale.kind)
        self.assertGreater(recent.priority, stale.priority)

    def test_discovery_candidate_aggregates_recommendation_support(self) -> None:
        self._insert_series(
            "seed-a",
            title="Seed A",
            season_title="Seed A (English Dub)",
            watchlist_status="fully_watched",
        )
        self._insert_progress("seed-a", "seed-a-1", episode_number=1, completion_ratio=1.0, last_watched_at="2026-03-01T01:00:00Z")
        self._insert_series(
            "seed-b",
            title="Seed B",
            season_title="Seed B (English Dub)",
            watchlist_status="fully_watched",
        )
        self._insert_progress("seed-b", "seed-b-1", episode_number=1, completion_ratio=1.0, last_watched_at="2026-03-02T01:00:00Z")
        self._map_series("seed-a", 100)
        self._map_series("seed-b", 200)
        self._cache_metadata(100, title="Seed A")
        self._cache_metadata(200, title="Seed B")
        self._cache_metadata(300, title="Discovery Hit")
        self._cache_recommendations(100, [{"target_mal_anime_id": 300, "target_title": "Discovery Hit", "num_recommendations": 20, "raw": {}}])
        self._cache_recommendations(200, [{"target_mal_anime_id": 300, "target_title": "Discovery Hit", "num_recommendations": 15, "raw": {}}])

        results = build_recommendations(self.config, limit=0)

        discovery = [item for item in results if item.kind == "discovery_candidate"]
        self.assertEqual(1, len(discovery))
        item = discovery[0]
        self.assertEqual("available-target-300", item.provider_series_id)
        self.assertEqual(2, item.context["supporting_source_count"])
        self.assertEqual(35, item.context["aggregated_recommendation_votes"])
        self.assertEqual(20, item.context["best_single_source_votes"])
        self.assertEqual(15, item.context["cross_seed_support_votes"])
        self.assertEqual(3, item.context["support_balance_bonus"])
        self.assertIn("cross-seed consensus beyond the strongest seed: 15 vote(s)", item.reasons)

    def test_discovery_candidate_prefers_balanced_cross_seed_support_when_totals_tie(self) -> None:
        self._insert_series(
            "seed-a",
            title="Seed A",
            season_title="Seed A (English Dub)",
            watchlist_status="fully_watched",
        )
        self._insert_progress("seed-a", "seed-a-1", episode_number=1, completion_ratio=1.0, last_watched_at="2026-03-01T01:00:00Z")
        self._insert_series(
            "seed-b",
            title="Seed B",
            season_title="Seed B (English Dub)",
            watchlist_status="fully_watched",
        )
        self._insert_progress("seed-b", "seed-b-1", episode_number=1, completion_ratio=1.0, last_watched_at="2026-03-02T01:00:00Z")
        self._map_series("seed-a", 100)
        self._map_series("seed-b", 200)
        self._cache_metadata(100, title="Seed A")
        self._cache_metadata(200, title="Seed B")
        self._cache_metadata(300, title="Balanced Pick", mean=8.0, popularity=500)
        self._cache_metadata(400, title="Bursty Pick", mean=8.0, popularity=500)
        self._cache_recommendations(100, [
            {"target_mal_anime_id": 300, "target_title": "Balanced Pick", "num_recommendations": 20, "raw": {}},
            {"target_mal_anime_id": 400, "target_title": "Bursty Pick", "num_recommendations": 30, "raw": {}},
        ])
        self._cache_recommendations(200, [
            {"target_mal_anime_id": 300, "target_title": "Balanced Pick", "num_recommendations": 10, "raw": {}},
            {"target_mal_anime_id": 400, "target_title": "Bursty Pick", "num_recommendations": 0, "raw": {}},
        ])

        results = [item for item in build_recommendations(self.config, limit=0) if item.kind == "discovery_candidate"]

        self.assertEqual(["available-target-300", "available-target-400"], [item.provider_series_id for item in results])
        self.assertEqual(10, results[0].context["cross_seed_support_votes"])
        self.assertEqual(0, results[1].context["cross_seed_support_votes"])
        self.assertGreater(results[0].context["support_balance_bonus"], results[1].context["support_balance_bonus"])
        self.assertGreater(results[0].priority, results[1].priority)

    def test_discovery_candidate_can_come_from_harvested_dormant_edge_with_provider_dub_availability(self) -> None:
        self._insert_series("dormant-seed", title="Dormant Seed")
        self._map_series("dormant-seed", 100)
        self._cache_metadata(100, title="Dormant Seed")
        self._cache_metadata(200, title="Harvested Pick", mean=8.1, popularity=400)
        self._cache_recommendations(
            100,
            [
                {"target_mal_anime_id": 200, "target_title": "Harvested Pick", "num_recommendations": 12, "raw": {}},
                {"target_mal_anime_id": 100, "target_title": "Dormant Seed", "num_recommendations": 99, "raw": {}},
            ],
        )

        results = [
            item
            for item in build_recommendations(
                self.config,
                limit=0,
                require_provider_availability=False,
                include_discovery_candidates_without_actionable_provider_evidence=True,
            )
            if item.kind == "discovery_candidate"
        ]

        self.assertEqual(["available-target-200"], [item.provider_series_id for item in results])
        self.assertEqual("Harvested Pick", results[0].title)
        self.assertEqual(12, results[0].context["aggregated_recommendation_votes"])
        self.assertEqual([100], results[0].context["supporting_mal_anime_ids"])
        self.assertEqual("mapped", results[0].context["availability_confidence"])
        self.assertTrue(results[0].context["availability_visible"])
        self.assertEqual("unknown", results[0].context["english_dub_signal"])

    def test_discovery_candidate_suppresses_mal_listed_harvested_target(self) -> None:
        self._insert_series("dormant-seed", title="Dormant Seed")
        self._map_series("dormant-seed", 100)
        self._cache_metadata(100, title="Dormant Seed")
        self._cache_metadata(
            200,
            title="Already Listed Pick",
            mean=8.1,
            popularity=400,
            my_list_status={"status": "plan_to_watch", "num_episodes_watched": 0},
        )
        self._cache_recommendations(
            100,
            [{"target_mal_anime_id": 200, "target_title": "Already Listed Pick", "num_recommendations": 12, "raw": {}}],
        )

        results = [item for item in build_recommendations(self.config, limit=0) if item.kind == "discovery_candidate"]

        self.assertEqual([], results)

    def test_discovery_candidate_suppresses_target_directly_from_list_cache(self) -> None:
        self._insert_series("dormant-seed", title="Dormant Seed")
        self._map_series("dormant-seed", 100)
        self._cache_metadata(100, title="Dormant Seed")
        self._cache_metadata(200, title="Already Listed Pick", mean=8.1, popularity=400)
        self._cache_mal_list_status(200, "Already Listed Pick", "plan_to_watch")
        self._cache_recommendations(
            100,
            [{"target_mal_anime_id": 200, "target_title": "Already Listed Pick", "num_recommendations": 12, "raw": {}}],
        )

        results = [item for item in build_recommendations(self.config, limit=0) if item.kind == "discovery_candidate"]

        self.assertEqual([], results)

    def test_discovery_candidate_can_use_completed_list_cache_seed_without_provider_mapping(self) -> None:
        self._cache_metadata(100, title="Cached Seed", genres=["Adventure"], my_list_status={"status": "completed", "score": 9})
        self._cache_mal_list_status(100, "Cached Seed", "completed", score=9)
        self._cache_metadata(200, title="Cache Seed Pick", genres=["Adventure"], mean=8.2, popularity=300)
        self._cache_recommendations(
            100,
            [{"target_mal_anime_id": 200, "target_title": "Cache Seed Pick", "num_recommendations": 12, "raw": {}}],
        )

        results = [item for item in build_recommendations(self.config, limit=0) if item.kind == "discovery_candidate"]

        self.assertEqual(["available-target-200"], [item.provider_series_id for item in results])
        self.assertEqual([100], results[0].context["supporting_mal_anime_ids"])
        self.assertEqual(9, results[0].context["supporting_seed_details"][0]["user_score"])

    def test_discovery_candidate_does_not_positive_seed_plan_or_dropped_list_cache_rows(self) -> None:
        for status in ("plan_to_watch", "dropped"):
            with self.subTest(status=status):
                self.tearDown()
                self.setUp()
                self._cache_metadata(100, title="Suppressed Seed", my_list_status={"status": status, "score": 2})
                self._cache_mal_list_status(100, "Suppressed Seed", status, score=2)
                self._cache_metadata(200, title="Should Not Appear", mean=8.2, popularity=300)
                self._cache_recommendations(
                    100,
                    [{"target_mal_anime_id": 200, "target_title": "Should Not Appear", "num_recommendations": 12, "raw": {}}],
                )

                results = [item for item in build_recommendations(self.config, limit=0) if item.kind == "discovery_candidate"]

                self.assertEqual([], results)

    def test_discovery_candidate_suppresses_mapped_provider_watched_target(self) -> None:
        self._insert_series("dormant-seed", title="Dormant Seed")
        self._map_series("dormant-seed", 100)
        self._cache_metadata(100, title="Dormant Seed")
        self._insert_series("watched-target", title="Already Watched Pick", watchlist_status="fully_watched")
        self._map_series("watched-target", 200)
        self._cache_metadata(200, title="Already Watched Pick", mean=8.1, popularity=400)
        self._cache_recommendations(
            100,
            [{"target_mal_anime_id": 200, "target_title": "Already Watched Pick", "num_recommendations": 12, "raw": {}}],
        )

        results = [item for item in build_recommendations(self.config, limit=0) if item.kind == "discovery_candidate"]

        self.assertEqual([], results)

    def test_discovery_candidate_suppresses_title_alias_provider_progress_target(self) -> None:
        self._insert_series("dormant-seed", title="Dormant Seed")
        self._map_series("dormant-seed", 100)
        self._cache_metadata(100, title="Dormant Seed")
        self._insert_series("alias-target", title="Already Watched Pick")
        recent = (datetime.now(timezone.utc) - timedelta(days=1)).replace(microsecond=0).isoformat().replace("+00:00", "Z")
        self._insert_progress("alias-target", "alias-ep-1", episode_number=1, completion_ratio=1.0, last_watched_at=recent)
        self._cache_metadata(200, title="Already Watched Pick", mean=8.1, popularity=400)
        self._cache_recommendations(
            100,
            [{"target_mal_anime_id": 200, "target_title": "Already Watched Pick", "num_recommendations": 12, "raw": {}}],
        )

        results = [item for item in build_recommendations(self.config, limit=0) if item.kind == "discovery_candidate"]

        self.assertEqual([], results)

    def test_discovery_candidate_keeps_unwatched_new_season_provider_match_with_guard_context(self) -> None:
        self._insert_series("dormant-seed", title="Dormant Seed")
        self._map_series("dormant-seed", 100)
        self._cache_metadata(100, title="Dormant Seed")
        self._insert_series("new-season", title="Fresh Pick", season_title="Fresh Pick Season 2 (English Dub)", watchlist_status="not_started")
        self._map_series("new-season", 200)
        self._cache_metadata(200, title="Fresh Pick", mean=8.1, popularity=400)
        self._cache_recommendations(
            100,
            [{"target_mal_anime_id": 200, "target_title": "Fresh Pick", "num_recommendations": 12, "raw": {}}],
            make_targets_available=False,
        )

        results = [
            item
            for item in build_recommendations(
                self.config,
                limit=0,
                require_provider_availability=False,
                include_discovery_candidates_without_actionable_provider_evidence=True,
            )
            if item.kind == "discovery_candidate"
        ]

        self.assertEqual(["new-season"], [item.provider_series_id for item in results])
        self.assertTrue(results[0].context["suppression_checked"])
        self.assertEqual([], results[0].context["suppression_evidence"])
        self.assertIn("suppression guard", " ".join(results[0].reasons))

    def test_discovery_candidate_prefers_more_recent_start_season_when_support_ties(self) -> None:
        self._insert_series(
            "seed-a",
            title="Seed A",
            season_title="Seed A (English Dub)",
            watchlist_status="fully_watched",
        )
        self._insert_progress("seed-a", "seed-a-1", episode_number=1, completion_ratio=1.0, last_watched_at="2026-03-01T01:00:00Z")
        self._map_series("seed-a", 100)
        self._cache_metadata(100, title="Seed A")
        now = datetime.now(timezone.utc)
        current_season = ("winter", "spring", "summer", "fall")[(now.month - 1) // 3]
        recent_season = {"year": now.year, "season": current_season}
        older_season = {"year": max(now.year - 12, 2000), "season": current_season}
        self._cache_metadata(300, title="Recent Pick", mean=8.0, popularity=500, start_season=recent_season)
        self._cache_metadata(400, title="Older Pick", mean=8.0, popularity=500, start_season=older_season)
        self._cache_recommendations(100, [
            {"target_mal_anime_id": 300, "target_title": "Recent Pick", "num_recommendations": 20, "raw": {}},
            {"target_mal_anime_id": 400, "target_title": "Older Pick", "num_recommendations": 20, "raw": {}},
        ])

        results = [item for item in build_recommendations(self.config, limit=0) if item.kind == "discovery_candidate"]

        self.assertEqual(["available-target-300", "available-target-400"], [item.provider_series_id for item in results])
        self.assertGreater(results[0].context["freshness_bonus"], results[1].context["freshness_bonus"])
        self.assertEqual("current_or_upcoming", results[0].context["freshness_bucket"])
        self.assertEqual(current_season.title() + " " + str(now.year), results[0].context["start_season_label"])
        self.assertIn("recent MAL start season", " ".join(results[0].reasons))
        self.assertGreater(results[0].priority, results[1].priority)

    def test_discovery_candidate_applies_modest_age_decay_to_legacy_catalog_titles(self) -> None:
        self._insert_series(
            "seed-a",
            title="Seed A",
            season_title="Seed A (English Dub)",
            watchlist_status="fully_watched",
        )
        self._insert_progress("seed-a", "seed-a-1", episode_number=1, completion_ratio=1.0, last_watched_at="2026-03-01T01:00:00Z")
        self._map_series("seed-a", 100)
        self._cache_metadata(100, title="Seed A")
        now = datetime.now(timezone.utc)
        current_season = ("winter", "spring", "summer", "fall")[(now.month - 1) // 3]
        aging_season = {"year": max(now.year - 10, 2000), "season": current_season}
        legacy_season = {"year": max(now.year - 24, 1985), "season": current_season}
        self._cache_metadata(300, title="Aging Pick", mean=8.0, popularity=500, start_season=aging_season)
        self._cache_metadata(400, title="Legacy Pick", mean=8.0, popularity=500, start_season=legacy_season)
        self._cache_recommendations(100, [
            {"target_mal_anime_id": 300, "target_title": "Aging Pick", "num_recommendations": 20, "raw": {}},
            {"target_mal_anime_id": 400, "target_title": "Legacy Pick", "num_recommendations": 20, "raw": {}},
        ])

        results = [item for item in build_recommendations(self.config, limit=0) if item.kind == "discovery_candidate"]

        self.assertEqual(["available-target-300", "available-target-400"], [item.provider_series_id for item in results])
        self.assertEqual("aging_catalog", results[0].context["freshness_bucket"])
        self.assertEqual(2, results[0].context["freshness_penalty"])
        self.assertEqual("legacy_catalog", results[1].context["freshness_bucket"])
        self.assertEqual(6, results[1].context["freshness_penalty"])
        self.assertGreater(results[1].context["catalog_age_in_seasons"], results[0].context["catalog_age_in_seasons"])
        self.assertIn("older MAL catalog title received modest age decay", " ".join(results[1].reasons))
        self.assertGreater(results[0].priority, results[1].priority)

    def test_discovery_candidate_prefers_recently_active_seed_support_when_votes_tie(self) -> None:
        recent = (datetime.now(timezone.utc) - timedelta(days=10)).replace(microsecond=0).isoformat().replace("+00:00", "Z")
        stale = (datetime.now(timezone.utc) - timedelta(days=220)).replace(microsecond=0).isoformat().replace("+00:00", "Z")
        self._insert_series(
            "seed-recent",
            title="Recent Seed",
            season_title="Recent Seed (English Dub)",
            watchlist_status="fully_watched",
        )
        self._insert_progress("seed-recent", "seed-recent-1", episode_number=1, completion_ratio=1.0, last_watched_at=recent)
        self._insert_series(
            "seed-stale",
            title="Stale Seed",
            season_title="Stale Seed (English Dub)",
            watchlist_status="fully_watched",
        )
        self._insert_progress("seed-stale", "seed-stale-1", episode_number=1, completion_ratio=1.0, last_watched_at=stale)
        self._map_series("seed-recent", 100)
        self._map_series("seed-stale", 200)
        self._cache_metadata(100, title="Recent Seed")
        self._cache_metadata(200, title="Stale Seed")
        self._cache_metadata(300, title="Recent-Supported Pick", mean=8.0, popularity=500)
        self._cache_metadata(400, title="Stale-Supported Pick", mean=8.0, popularity=500)
        self._cache_recommendations(100, [
            {"target_mal_anime_id": 300, "target_title": "Recent-Supported Pick", "num_recommendations": 20, "raw": {}},
        ])
        self._cache_recommendations(200, [
            {"target_mal_anime_id": 400, "target_title": "Stale-Supported Pick", "num_recommendations": 20, "raw": {}},
        ])

        results = [item for item in build_recommendations(self.config, limit=0) if item.kind == "discovery_candidate"]

        self.assertEqual(["available-target-300", "available-target-400"], [item.provider_series_id for item in results])
        self.assertGreater(results[0].context["recent_seed_activity_bonus"], results[1].context["recent_seed_activity_bonus"])
        self.assertLess(results[0].context["freshest_supporting_seed_days"], results[1].context["freshest_supporting_seed_days"])
        self.assertEqual(0, results[0].context["stale_support_penalty"])
        self.assertGreater(results[1].context["stale_support_penalty"], 0)
        self.assertIn("recently active seed watch history", " ".join(results[0].reasons))
        self.assertIn("older supporting seed activity counted conservatively", " ".join(results[1].reasons))
        self.assertGreater(results[0].priority, results[1].priority)

    def test_discovery_candidate_penalizes_stale_support_even_when_seed_quality_is_high(self) -> None:
        fresh = (datetime.now(timezone.utc) - timedelta(days=45)).replace(microsecond=0).isoformat().replace("+00:00", "Z")
        stale = (datetime.now(timezone.utc) - timedelta(days=900)).replace(microsecond=0).isoformat().replace("+00:00", "Z")
        self._insert_series(
            "seed-fresh-loved",
            title="Fresh Loved Seed",
            season_title="Fresh Loved Seed (English Dub)",
            watchlist_status="fully_watched",
        )
        self._insert_progress("seed-fresh-loved", "seed-fresh-loved-1", episode_number=1, completion_ratio=1.0, last_watched_at=fresh)
        self._insert_series(
            "seed-stale-loved",
            title="Stale Loved Seed",
            season_title="Stale Loved Seed (English Dub)",
            watchlist_status="fully_watched",
        )
        self._insert_progress("seed-stale-loved", "seed-stale-loved-1", episode_number=1, completion_ratio=1.0, last_watched_at=stale)
        self._map_series("seed-fresh-loved", 100)
        self._map_series("seed-stale-loved", 200)
        self._cache_metadata(100, title="Fresh Loved Seed", my_list_status={"status": "completed", "num_episodes_watched": 12, "score": 10})
        self._cache_metadata(200, title="Stale Loved Seed", my_list_status={"status": "completed", "num_episodes_watched": 12, "score": 10})
        self._cache_metadata(300, title="Fresh Loved Pick", mean=8.0, popularity=500)
        self._cache_metadata(400, title="Stale Loved Pick", mean=8.0, popularity=500)
        self._cache_recommendations(100, [
            {"target_mal_anime_id": 300, "target_title": "Fresh Loved Pick", "num_recommendations": 20, "raw": {}},
        ])
        self._cache_recommendations(200, [
            {"target_mal_anime_id": 400, "target_title": "Stale Loved Pick", "num_recommendations": 20, "raw": {}},
        ])

        results = [item for item in build_recommendations(self.config, limit=0) if item.kind == "discovery_candidate"]

        self.assertEqual(["available-target-300", "available-target-400"], [item.provider_series_id for item in results])
        self.assertEqual(3, results[0].context["seed_quality_bonus"])
        self.assertEqual(3, results[1].context["seed_quality_bonus"])
        self.assertEqual(0, results[0].context["stale_supporting_seed_count"])
        self.assertEqual(1, results[1].context["stale_supporting_seed_count"])
        self.assertEqual(1.0, results[1].context["stale_support_ratio"])
        self.assertEqual(3, results[1].context["stale_support_penalty"])
        self.assertIn("older supporting seed activity counted conservatively", " ".join(results[1].reasons))
        self.assertGreater(results[0].priority, results[1].priority)

    def test_discovery_candidate_stale_heavy_multi_seed_support_counts_less_like_fresh_consensus(self) -> None:
        seed_specs = (
            ("fresh-a", "Fresh Seed A", 100, "2026-03-01T01:00:00Z"),
            ("fresh-b", "Fresh Seed B", 200, "2026-03-02T01:00:00Z"),
            ("fresh-c", "Fresh Seed C", 250, "2026-03-03T01:00:00Z"),
            ("stale-a", "Stale Seed A", 300, "2024-01-01T01:00:00Z"),
            ("stale-b", "Stale Seed B", 350, "2024-01-02T01:00:00Z"),
            ("fresh-d", "Fresh Seed D", 360, "2026-03-04T01:00:00Z"),
        )
        for provider_series_id, title, mal_anime_id, watched_at in seed_specs:
            self._insert_series(
                provider_series_id,
                title=title,
                season_title=f"{title} (English Dub)",
                watchlist_status="fully_watched",
            )
            self._insert_progress(
                provider_series_id,
                provider_series_id + "-1",
                episode_number=1,
                completion_ratio=1.0,
                last_watched_at=watched_at,
            )
            self._map_series(provider_series_id, mal_anime_id)
            self._cache_metadata(
                mal_anime_id,
                title=title,
                my_list_status={"status": "completed", "num_episodes_watched": 12, "score": 8},
            )

        self._cache_metadata(400, title="Fresh Consensus Pick", mean=8.0, popularity=500)
        self._cache_metadata(500, title="Stale-Heavy Consensus Pick", mean=8.0, popularity=500)

        for source_id in (100, 200, 250):
            self._cache_recommendations(source_id, [
                {"target_mal_anime_id": 400, "target_title": "Fresh Consensus Pick", "num_recommendations": 6, "raw": {}},
            ])
        for source_id in (300, 350, 360):
            self._cache_recommendations(source_id, [
                {"target_mal_anime_id": 500, "target_title": "Stale-Heavy Consensus Pick", "num_recommendations": 6, "raw": {}},
            ])

        results = [item for item in build_recommendations(self.config, limit=0) if item.kind == "discovery_candidate"]

        self.assertEqual(["available-target-400", "available-target-500"], [item.provider_series_id for item in results])
        stale_heavy = results[1]
        self.assertEqual(3, stale_heavy.context["supporting_source_count"])
        self.assertEqual(3, stale_heavy.context["base_effective_supporting_seed_count"])
        self.assertEqual(2, stale_heavy.context["stale_supporting_seed_count"])
        self.assertAlmostEqual(2 / 3, stale_heavy.context["stale_support_ratio"])
        self.assertEqual(1, stale_heavy.context["stale_consensus_discount"])
        self.assertEqual(2, stale_heavy.context["effective_supporting_seed_count"])
        self.assertIn(
            "stale-heavy multi-seed consensus counted less strongly (2/3 supporting seed title(s) were stale)",
            stale_heavy.reasons,
        )
        self.assertLessEqual(stale_heavy.priority, results[0].priority)

    def test_discovery_candidate_prefers_higher_scored_seed_support_when_votes_tie(self) -> None:
        self._insert_series(
            "seed-loved",
            title="Loved Seed",
            season_title="Loved Seed (English Dub)",
            watchlist_status="fully_watched",
        )
        self._insert_progress("seed-loved", "seed-loved-1", episode_number=1, completion_ratio=1.0, last_watched_at="2026-03-01T01:00:00Z")
        self._insert_series(
            "seed-neutral",
            title="Neutral Seed",
            season_title="Neutral Seed (English Dub)",
            watchlist_status="fully_watched",
        )
        self._insert_progress("seed-neutral", "seed-neutral-1", episode_number=1, completion_ratio=1.0, last_watched_at="2026-03-01T01:00:00Z")
        self._map_series("seed-loved", 100)
        self._map_series("seed-neutral", 200)
        self._cache_metadata(100, title="Loved Seed", my_list_status={"status": "completed", "num_episodes_watched": 12, "score": 10})
        self._cache_metadata(200, title="Neutral Seed", my_list_status={"status": "completed", "num_episodes_watched": 12, "score": 6})
        self._cache_metadata(300, title="Loved-Line Pick", mean=8.0, popularity=500)
        self._cache_metadata(400, title="Neutral-Line Pick", mean=8.0, popularity=500)
        self._cache_recommendations(100, [
            {"target_mal_anime_id": 300, "target_title": "Loved-Line Pick", "num_recommendations": 20, "raw": {}},
        ])
        self._cache_recommendations(200, [
            {"target_mal_anime_id": 400, "target_title": "Neutral-Line Pick", "num_recommendations": 20, "raw": {}},
        ])

        results = [item for item in build_recommendations(self.config, limit=0) if item.kind == "discovery_candidate"]

        self.assertEqual(["available-target-300", "available-target-400"], [item.provider_series_id for item in results])
        self.assertGreater(results[0].context["seed_quality_bonus"], results[1].context["seed_quality_bonus"])
        self.assertEqual(10, results[0].context["best_supporting_seed_score"])
        self.assertIn("higher-confidence seed taste signals", " ".join(results[0].reasons))
        self.assertGreater(results[0].priority, results[1].priority)

    def test_discovery_candidate_prefers_deeper_completion_seed_support_without_scores(self) -> None:
        self._insert_series(
            "seed-deep",
            title="Deep Seed",
            season_title="Deep Seed (English Dub)",
            watchlist_status="fully_watched",
        )
        for episode_number in range(1, 25):
            self._insert_progress(
                "seed-deep",
                f"seed-deep-{episode_number}",
                episode_number=episode_number,
                completion_ratio=1.0,
                last_watched_at="2026-03-01T01:00:00Z",
            )
        self._insert_series(
            "seed-light",
            title="Light Seed",
            season_title="Light Seed (English Dub)",
            watchlist_status="fully_watched",
        )
        self._insert_progress("seed-light", "seed-light-1", episode_number=1, completion_ratio=1.0, last_watched_at="2026-03-01T01:00:00Z")
        self._map_series("seed-deep", 100)
        self._map_series("seed-light", 200)
        self._cache_metadata(100, title="Deep Seed")
        self._cache_metadata(200, title="Light Seed")
        self._cache_metadata(300, title="Deep-Line Pick", mean=8.0, popularity=500)
        self._cache_metadata(400, title="Light-Line Pick", mean=8.0, popularity=500)
        self._cache_recommendations(100, [
            {"target_mal_anime_id": 300, "target_title": "Deep-Line Pick", "num_recommendations": 20, "raw": {}},
        ])
        self._cache_recommendations(200, [
            {"target_mal_anime_id": 400, "target_title": "Light-Line Pick", "num_recommendations": 20, "raw": {}},
        ])

        results = [item for item in build_recommendations(self.config, limit=0) if item.kind == "discovery_candidate"]

        self.assertEqual(["available-target-300", "available-target-400"], [item.provider_series_id for item in results])
        self.assertGreater(results[0].context["seed_quality_bonus"], results[1].context["seed_quality_bonus"])
        self.assertIsNone(results[0].context["best_supporting_seed_score"])
        self.assertIn("stronger seed engagement signals", " ".join(results[0].reasons))
        self.assertGreater(results[0].priority, results[1].priority)

    def test_discovery_candidate_penalizes_neutral_seed_support_when_votes_tie(self) -> None:
        self._insert_series(
            "seed-liked",
            title="Liked Seed",
            season_title="Liked Seed (English Dub)",
            watchlist_status="fully_watched",
        )
        self._insert_progress("seed-liked", "seed-liked-1", episode_number=1, completion_ratio=1.0, last_watched_at="2026-03-01T01:00:00Z")
        self._insert_series(
            "seed-neutral",
            title="Neutral Seed",
            season_title="Neutral Seed (English Dub)",
            watchlist_status="fully_watched",
        )
        self._insert_progress("seed-neutral", "seed-neutral-1", episode_number=1, completion_ratio=1.0, last_watched_at="2026-03-01T01:00:00Z")
        self._map_series("seed-liked", 100)
        self._map_series("seed-neutral", 200)
        self._cache_metadata(100, title="Liked Seed", my_list_status={"status": "completed", "num_episodes_watched": 12, "score": 8})
        self._cache_metadata(200, title="Neutral Seed", my_list_status={"status": "completed", "num_episodes_watched": 12, "score": 6})
        self._cache_metadata(300, title="Liked-Line Pick", mean=8.0, popularity=500)
        self._cache_metadata(400, title="Neutral-Line Pick", mean=8.0, popularity=500)
        self._cache_recommendations(100, [
            {"target_mal_anime_id": 300, "target_title": "Liked-Line Pick", "num_recommendations": 20, "raw": {}},
        ])
        self._cache_recommendations(200, [
            {"target_mal_anime_id": 400, "target_title": "Neutral-Line Pick", "num_recommendations": 20, "raw": {}},
        ])

        results = [item for item in build_recommendations(self.config, limit=0) if item.kind == "discovery_candidate"]

        self.assertEqual(["available-target-300", "available-target-400"], [item.provider_series_id for item in results])
        self.assertEqual(0, results[0].context["neutral_supporting_seed_count"])
        self.assertEqual(1, results[1].context["neutral_supporting_seed_count"])
        self.assertEqual({200: 6}, results[1].context["neutral_supporting_seed_scores"])
        self.assertEqual(1.0, results[1].context["neutral_support_ratio"])
        self.assertEqual(4, results[1].context["neutral_support_penalty"])
        self.assertIn("explicit neutral seed support counted conservatively", " ".join(results[1].reasons))
        self.assertGreater(results[0].priority, results[1].priority)

    def test_discovery_candidate_penalizes_low_scored_seed_support_when_votes_tie(self) -> None:
        self._insert_series(
            "seed-liked",
            title="Liked Seed",
            season_title="Liked Seed (English Dub)",
            watchlist_status="fully_watched",
        )
        self._insert_progress("seed-liked", "seed-liked-1", episode_number=1, completion_ratio=1.0, last_watched_at="2026-03-01T01:00:00Z")
        self._insert_series(
            "seed-disliked",
            title="Disliked Seed",
            season_title="Disliked Seed (English Dub)",
            watchlist_status="fully_watched",
        )
        self._insert_progress("seed-disliked", "seed-disliked-1", episode_number=1, completion_ratio=1.0, last_watched_at="2026-03-01T01:00:00Z")
        self._map_series("seed-liked", 100)
        self._map_series("seed-disliked", 200)
        self._cache_metadata(100, title="Liked Seed", my_list_status={"status": "completed", "num_episodes_watched": 12, "score": 8})
        self._cache_metadata(200, title="Disliked Seed", my_list_status={"status": "completed", "num_episodes_watched": 12, "score": 3})
        self._cache_metadata(300, title="Liked-Line Pick", mean=8.0, popularity=500)
        self._cache_metadata(400, title="Disliked-Line Pick", mean=8.0, popularity=500)
        self._cache_recommendations(100, [
            {"target_mal_anime_id": 300, "target_title": "Liked-Line Pick", "num_recommendations": 20, "raw": {}},
        ])
        self._cache_recommendations(200, [
            {"target_mal_anime_id": 400, "target_title": "Disliked-Line Pick", "num_recommendations": 20, "raw": {}},
        ])

        results = [item for item in build_recommendations(self.config, limit=0) if item.kind == "discovery_candidate"]

        self.assertEqual(["available-target-300"], [item.provider_series_id for item in results])
        self.assertEqual(0, results[0].context["seed_quality_penalty"])
        self.assertEqual(0, results[0].context["disliked_supporting_seed_count"])

    def test_discovery_candidate_keeps_positive_seed_quality_above_disliked_support_penalty(self) -> None:
        self._insert_series(
            "seed-loved",
            title="Loved Seed",
            season_title="Loved Seed (English Dub)",
            watchlist_status="fully_watched",
        )
        self._insert_progress("seed-loved", "seed-loved-1", episode_number=1, completion_ratio=1.0, last_watched_at="2026-03-01T01:00:00Z")
        self._insert_series(
            "seed-disliked",
            title="Disliked Seed",
            season_title="Disliked Seed (English Dub)",
            watchlist_status="fully_watched",
        )
        self._insert_progress("seed-disliked", "seed-disliked-1", episode_number=1, completion_ratio=1.0, last_watched_at="2026-03-01T01:00:00Z")
        self._map_series("seed-loved", 100)
        self._map_series("seed-disliked", 200)
        self._cache_metadata(100, title="Loved Seed", my_list_status={"status": "completed", "num_episodes_watched": 12, "score": 10})
        self._cache_metadata(200, title="Disliked Seed", my_list_status={"status": "completed", "num_episodes_watched": 12, "score": 3})
        self._cache_metadata(300, title="Mixed-Signal Pick", mean=8.0, popularity=500)
        self._cache_metadata(400, title="Disliked-Only Pick", mean=8.0, popularity=500)
        self._cache_recommendations(100, [
            {"target_mal_anime_id": 300, "target_title": "Mixed-Signal Pick", "num_recommendations": 12, "raw": {}},
        ])
        self._cache_recommendations(200, [
            {"target_mal_anime_id": 300, "target_title": "Mixed-Signal Pick", "num_recommendations": 8, "raw": {}},
            {"target_mal_anime_id": 400, "target_title": "Disliked-Only Pick", "num_recommendations": 20, "raw": {}},
        ])

        results = [item for item in build_recommendations(self.config, limit=0) if item.kind == "discovery_candidate"]

        self.assertEqual(["available-target-300"], [item.provider_series_id for item in results])
        self.assertGreater(results[0].context["seed_quality_bonus"], 0)
        self.assertEqual(3, results[0].context["seed_quality_penalty"])
        self.assertEqual(10, results[0].context["best_supporting_seed_score"])
        self.assertEqual(3, results[0].context["lowest_supporting_seed_score"])
        self.assertEqual(1, results[0].context["disliked_supporting_seed_count"])
        self.assertEqual(1, results[0].context["negative_supporting_seed_count"])
        self.assertEqual(0.5, results[0].context["negative_support_ratio"])
        self.assertEqual(1, results[0].context["effective_supporting_seed_count"])
        self.assertEqual(3, results[0].context["mixed_signal_penalty"])
        self.assertIn("mixed-signal support decay applied (1/2 supporting seed title(s) were dropped/disliked)", results[0].reasons)

    def test_discovery_candidate_neutral_support_does_not_count_like_positive_multi_seed_consensus(self) -> None:
        for provider_series_id, title in (("seed-liked", "Liked Seed"), ("seed-neutral-a", "Neutral Seed A"), ("seed-neutral-b", "Neutral Seed B")):
            self._insert_series(
                provider_series_id,
                title=title,
                season_title=f"{title} (English Dub)",
                watchlist_status="fully_watched",
            )
            self._insert_progress(
                provider_series_id,
                provider_series_id + "-1",
                episode_number=1,
                completion_ratio=1.0,
                last_watched_at="2026-03-01T01:00:00Z",
            )

        self._map_series("seed-liked", 100)
        self._map_series("seed-neutral-a", 200)
        self._map_series("seed-neutral-b", 250)
        self._cache_metadata(100, title="Liked Seed", my_list_status={"status": "completed", "num_episodes_watched": 12, "score": 8})
        self._cache_metadata(200, title="Neutral Seed A", my_list_status={"status": "completed", "num_episodes_watched": 12, "score": 6})
        self._cache_metadata(250, title="Neutral Seed B", my_list_status={"status": "completed", "num_episodes_watched": 12, "score": 6})
        self._cache_metadata(300, title="Clean Support Pick", mean=8.0, popularity=500)
        self._cache_metadata(400, title="Neutral-Heavy Pick", mean=8.0, popularity=500)

        self._cache_recommendations(100, [
            {"target_mal_anime_id": 300, "target_title": "Clean Support Pick", "num_recommendations": 20, "raw": {}},
            {"target_mal_anime_id": 400, "target_title": "Neutral-Heavy Pick", "num_recommendations": 8, "raw": {}},
        ])
        self._cache_recommendations(200, [
            {"target_mal_anime_id": 400, "target_title": "Neutral-Heavy Pick", "num_recommendations": 10, "raw": {}},
        ])
        self._cache_recommendations(250, [
            {"target_mal_anime_id": 400, "target_title": "Neutral-Heavy Pick", "num_recommendations": 10, "raw": {}},
        ])

        results = [item for item in build_recommendations(self.config, limit=0) if item.kind == "discovery_candidate"]

        self.assertEqual({"available-target-300", "available-target-400"}, {item.provider_series_id for item in results})
        neutral_heavy = next(item for item in results if item.provider_series_id == "available-target-400")
        clean_support = next(item for item in results if item.provider_series_id == "available-target-300")
        self.assertEqual(2, neutral_heavy.context["neutral_supporting_seed_count"])
        self.assertAlmostEqual(2 / 3, neutral_heavy.context["neutral_support_ratio"])
        self.assertEqual(1, neutral_heavy.context["effective_supporting_seed_count"])
        self.assertEqual(18, neutral_heavy.context["cross_seed_support_votes"])
        self.assertEqual(0, neutral_heavy.context["effective_cross_seed_support_votes"])
        self.assertEqual(0, neutral_heavy.context["support_balance_bonus"])
        self.assertEqual(4, neutral_heavy.context["neutral_support_penalty"])
        self.assertIn(
            "support spread counted conservatively after neutral/stale seed weighting (0/18 effective cross-seed vote(s))",
            neutral_heavy.reasons,
        )
        self.assertLessEqual(neutral_heavy.context["support_balance_bonus"], clean_support.context["support_balance_bonus"])

    def test_discovery_candidate_stale_support_spread_gets_less_balance_bonus_than_fresh_spread(self) -> None:
        for provider_series_id, title, watched_at, mal_anime_id in (
            ("seed-fresh-a", "Fresh Seed A", "2026-03-01T01:00:00Z", 100),
            ("seed-fresh-b", "Fresh Seed B", "2026-03-02T01:00:00Z", 200),
            ("seed-stale-a", "Stale Seed A", "2023-01-01T01:00:00Z", 300),
            ("seed-stale-b", "Stale Seed B", "2023-01-02T01:00:00Z", 350),
        ):
            self._insert_series(
                provider_series_id,
                title=title,
                season_title=f"{title} (English Dub)",
                watchlist_status="fully_watched",
            )
            self._insert_progress(
                provider_series_id,
                provider_series_id + "-1",
                episode_number=1,
                completion_ratio=1.0,
                last_watched_at=watched_at,
            )
            self._map_series(provider_series_id, mal_anime_id)
            self._cache_metadata(mal_anime_id, title=title, my_list_status={"status": "completed", "num_episodes_watched": 12, "score": 8})

        self._cache_metadata(400, title="Fresh Spread Pick", mean=8.0, popularity=500)
        self._cache_metadata(500, title="Stale Spread Pick", mean=8.0, popularity=500)
        self._cache_recommendations(100, [{"target_mal_anime_id": 400, "target_title": "Fresh Spread Pick", "num_recommendations": 20, "raw": {}}])
        self._cache_recommendations(200, [{"target_mal_anime_id": 400, "target_title": "Fresh Spread Pick", "num_recommendations": 10, "raw": {}}])
        self._cache_recommendations(300, [{"target_mal_anime_id": 500, "target_title": "Stale Spread Pick", "num_recommendations": 20, "raw": {}}])
        self._cache_recommendations(350, [{"target_mal_anime_id": 500, "target_title": "Stale Spread Pick", "num_recommendations": 10, "raw": {}}])

        results = [item for item in build_recommendations(self.config, limit=0) if item.kind == "discovery_candidate"]

        self.assertEqual(["available-target-400", "available-target-500"], [item.provider_series_id for item in results])
        fresh_spread, stale_spread = results
        self.assertEqual(10, fresh_spread.context["effective_cross_seed_support_votes"])
        self.assertEqual(6, stale_spread.context["effective_cross_seed_support_votes"])
        self.assertGreater(fresh_spread.context["support_balance_bonus"], stale_spread.context["support_balance_bonus"])
        self.assertIn(
            "support spread counted conservatively after neutral/stale seed weighting (6/10 effective cross-seed vote(s))",
            stale_spread.reasons,
        )
        self.assertGreater(fresh_spread.priority, stale_spread.priority)

    def test_discovery_candidate_majority_negative_support_ranks_below_clean_support(self) -> None:
        for provider_series_id, title in (("seed-loved", "Loved Seed"), ("seed-disliked-a", "Disliked Seed A"), ("seed-disliked-b", "Disliked Seed B")):
            self._insert_series(
                provider_series_id,
                title=title,
                season_title=f"{title} (English Dub)",
                watchlist_status="fully_watched",
            )
            self._insert_progress(
                provider_series_id,
                provider_series_id + "-1",
                episode_number=1,
                completion_ratio=1.0,
                last_watched_at="2026-03-01T01:00:00Z",
            )

        self._map_series("seed-loved", 100)
        self._map_series("seed-disliked-a", 200)
        self._map_series("seed-disliked-b", 250)
        self._cache_metadata(100, title="Loved Seed", my_list_status={"status": "completed", "num_episodes_watched": 12, "score": 10})
        self._cache_metadata(200, title="Disliked Seed A", my_list_status={"status": "completed", "num_episodes_watched": 12, "score": 3})
        self._cache_metadata(250, title="Disliked Seed B", my_list_status={"status": "completed", "num_episodes_watched": 12, "score": 4})
        self._cache_metadata(300, title="Clean Support Pick", mean=8.0, popularity=500)
        self._cache_metadata(400, title="Majority Negative Pick", mean=8.0, popularity=500)

        self._cache_recommendations(100, [
            {"target_mal_anime_id": 300, "target_title": "Clean Support Pick", "num_recommendations": 20, "raw": {}},
            {"target_mal_anime_id": 400, "target_title": "Majority Negative Pick", "num_recommendations": 20, "raw": {}},
        ])
        self._cache_recommendations(200, [
            {"target_mal_anime_id": 400, "target_title": "Majority Negative Pick", "num_recommendations": 4, "raw": {}},
        ])
        self._cache_recommendations(250, [
            {"target_mal_anime_id": 400, "target_title": "Majority Negative Pick", "num_recommendations": 4, "raw": {}},
        ])

        results = [item for item in build_recommendations(self.config, limit=0) if item.kind == "discovery_candidate"]

        self.assertEqual(["available-target-300", "available-target-400"], [item.provider_series_id for item in results])
        majority_negative = results[1]
        self.assertEqual(2, majority_negative.context["negative_supporting_seed_count"])
        self.assertAlmostEqual(2 / 3, majority_negative.context["negative_support_ratio"])
        self.assertEqual(1, majority_negative.context["effective_supporting_seed_count"])
        self.assertEqual(5, majority_negative.context["mixed_signal_penalty"])
        self.assertLess(majority_negative.priority, results[0].priority)

    def test_discovery_candidate_suppresses_disliked_only_seed_support(self) -> None:
        self._insert_series(
            "seed-disliked",
            title="Disliked Seed",
            season_title="Disliked Seed (English Dub)",
            watchlist_status="fully_watched",
        )
        self._insert_progress("seed-disliked", "seed-disliked-1", episode_number=1, completion_ratio=1.0, last_watched_at="2026-03-01T01:00:00Z")
        self._map_series("seed-disliked", 200)
        self._cache_metadata(200, title="Disliked Seed", my_list_status={"status": "completed", "num_episodes_watched": 12, "score": 3})
        self._cache_metadata(400, title="Disliked-Only Pick", mean=8.0, popularity=500)
        self._cache_recommendations(200, [
            {"target_mal_anime_id": 400, "target_title": "Disliked-Only Pick", "num_recommendations": 20, "raw": {}},
        ])

        results = [item for item in build_recommendations(self.config, limit=0) if item.kind == "discovery_candidate"]

        self.assertEqual([], results)

    def test_discovery_candidate_suppresses_dropped_only_seed_support(self) -> None:
        self._insert_series(
            "seed-dropped",
            title="Dropped Seed",
            season_title="Dropped Seed (English Dub)",
            watchlist_status="fully_watched",
        )
        self._insert_progress("seed-dropped", "seed-dropped-1", episode_number=1, completion_ratio=1.0, last_watched_at="2026-03-01T01:00:00Z")
        self._map_series("seed-dropped", 200)
        self._cache_metadata(200, title="Dropped Seed", my_list_status={"status": "dropped", "num_episodes_watched": 4, "score": 6})
        self._cache_metadata(400, title="Dropped-Only Pick", mean=8.0, popularity=500)
        self._cache_recommendations(200, [
            {"target_mal_anime_id": 400, "target_title": "Dropped-Only Pick", "num_recommendations": 20, "raw": {}},
        ])

        results = [item for item in build_recommendations(self.config, limit=0) if item.kind == "discovery_candidate"]

        self.assertEqual([], results)

    def test_discovery_candidate_prefers_cached_genre_overlap(self) -> None:
        self._insert_series(
            "seed-a",
            title="Seed A",
            season_title="Seed A (English Dub)",
            watchlist_status="fully_watched",
        )
        self._insert_progress("seed-a", "seed-a-1", episode_number=1, completion_ratio=1.0, last_watched_at="2026-03-01T01:00:00Z")
        self._map_series("seed-a", 100)
        self._cache_metadata(100, title="Seed A", genres=["Sci-Fi", "Thriller"])
        self._cache_metadata(300, title="Genre Match", mean=8.0, popularity=500, genres=["Sci-Fi", "Action"])
        self._cache_metadata(400, title="Genre Miss", mean=8.0, popularity=500, genres=["Romance"])
        self._cache_recommendations(100, [
            {"target_mal_anime_id": 300, "target_title": "Genre Match", "num_recommendations": 20, "raw": {}},
            {"target_mal_anime_id": 400, "target_title": "Genre Miss", "num_recommendations": 20, "raw": {}},
        ])

        results = [item for item in build_recommendations(self.config, limit=0) if item.kind == "discovery_candidate"]

        self.assertEqual(["available-target-300", "available-target-400"], [item.provider_series_id for item in results])
        self.assertEqual(["Sci-Fi"], results[0].context["shared_genres"])
        self.assertGreater(results[0].context["genre_overlap_score"], 0)
        self.assertIn("shared seed genres: Sci-Fi", results[0].reasons)
        self.assertEqual([], results[1].context["shared_genres"])
        self.assertEqual(0, results[1].context["genre_overlap_score"])
        self.assertGreater(results[0].priority, results[1].priority)

    def test_discovery_candidate_prefers_cached_studio_overlap_when_votes_tie(self) -> None:
        self._insert_series(
            "seed-a",
            title="Seed A",
            season_title="Seed A (English Dub)",
            watchlist_status="fully_watched",
        )
        self._insert_progress("seed-a", "seed-a-1", episode_number=1, completion_ratio=1.0, last_watched_at="2026-03-01T01:00:00Z")
        self._map_series("seed-a", 100)
        self._cache_metadata(100, title="Seed A", studios=["Bones"])
        self._cache_metadata(300, title="Studio Match", mean=8.0, popularity=500, studios=["Bones"])
        self._cache_metadata(400, title="Studio Miss", mean=8.0, popularity=500, studios=["Madhouse"])
        self._cache_recommendations(100, [
            {"target_mal_anime_id": 300, "target_title": "Studio Match", "num_recommendations": 20, "raw": {}},
            {"target_mal_anime_id": 400, "target_title": "Studio Miss", "num_recommendations": 20, "raw": {}},
        ])

        results = [item for item in build_recommendations(self.config, limit=0) if item.kind == "discovery_candidate"]

        self.assertEqual(["available-target-300", "available-target-400"], [item.provider_series_id for item in results])
        self.assertEqual(["Bones"], results[0].context["shared_studios"])
        self.assertGreater(results[0].context["studio_overlap_score"], 0)
        self.assertIn("shared seed studios: Bones", results[0].reasons)
        self.assertEqual([], results[1].context["shared_studios"])
        self.assertEqual(0, results[1].context["studio_overlap_score"])
        self.assertGreater(results[0].priority, results[1].priority)

    def test_discovery_candidate_prefers_cached_source_overlap_when_votes_tie(self) -> None:
        self._insert_series(
            "seed-a",
            title="Seed A",
            season_title="Seed A (English Dub)",
            watchlist_status="fully_watched",
        )
        self._insert_progress("seed-a", "seed-a-1", episode_number=1, completion_ratio=1.0, last_watched_at="2026-03-01T01:00:00Z")
        self._map_series("seed-a", 100)
        self._cache_metadata(100, title="Seed A", source="light_novel")
        self._cache_metadata(300, title="Source Match", mean=8.0, popularity=500, source="light_novel")
        self._cache_metadata(400, title="Source Miss", mean=8.0, popularity=500, source="manga")
        self._cache_recommendations(100, [
            {"target_mal_anime_id": 300, "target_title": "Source Match", "num_recommendations": 20, "raw": {}},
            {"target_mal_anime_id": 400, "target_title": "Source Miss", "num_recommendations": 20, "raw": {}},
        ])

        results = [item for item in build_recommendations(self.config, limit=0) if item.kind == "discovery_candidate"]

        self.assertEqual(["available-target-300", "available-target-400"], [item.provider_series_id for item in results])
        self.assertEqual("light_novel", results[0].context["source"])
        self.assertGreater(results[0].context["source_overlap_score"], 0)
        self.assertIn("shared seed source material: light_novel", results[0].reasons)
        self.assertEqual("manga", results[1].context["source"])
        self.assertEqual(0, results[1].context["source_overlap_score"])
        self.assertGreater(results[0].priority, results[1].priority)

    def test_discovery_candidate_prefers_higher_mean_even_without_metadata_rich_tie(self) -> None:
        self._insert_series(
            "seed-a",
            title="Seed A",
            season_title="Seed A (English Dub)",
            watchlist_status="fully_watched",
        )
        self._insert_progress("seed-a", "seed-a-1", episode_number=1, completion_ratio=1.0, last_watched_at="2026-03-01T01:00:00Z")
        self._map_series("seed-a", 100)
        self._cache_metadata(100, title="Seed A", genres=["Sci-Fi"], studios=["Bones"], source="light_novel")
        self._cache_metadata(300, title="Elite Mean Pick", mean=8.6, popularity=500, genres=["Romance"], studios=["Madhouse"], source="manga")
        self._cache_metadata(400, title="Baseline Mean Pick", mean=8.1, popularity=500, genres=["Romance"], studios=["Madhouse"], source="manga")
        self._cache_recommendations(100, [
            {"target_mal_anime_id": 300, "target_title": "Elite Mean Pick", "num_recommendations": 20, "raw": {}},
            {"target_mal_anime_id": 400, "target_title": "Baseline Mean Pick", "num_recommendations": 20, "raw": {}},
        ])

        results = [item for item in build_recommendations(self.config, limit=0) if item.kind == "discovery_candidate"]

        self.assertEqual(["available-target-300", "available-target-400"], [item.provider_series_id for item in results])
        self.assertEqual(0, results[0].context["metadata_match_dimensions"])
        self.assertEqual(2, results[0].context["catalog_quality_bonus"])
        self.assertEqual("elite", results[0].context["catalog_mean_band"])
        self.assertIsNone(results[0].context["catalog_popularity_band"])
        self.assertEqual(2, results[0].context["catalog_quality_adjustment"])
        self.assertEqual(0, results[1].context["catalog_quality_bonus"])
        self.assertEqual(0, results[1].context["catalog_quality_adjustment"])
        self.assertIn(
            "global catalog quality/adoption calibration slightly favored this candidate (elite MAL mean)",
            results[0].reasons,
        )
        self.assertGreater(results[0].priority, results[1].priority)

    def test_discovery_candidate_prefers_broader_adoption_even_without_metadata_rich_tie(self) -> None:
        self._insert_series(
            "seed-a",
            title="Seed A",
            season_title="Seed A (English Dub)",
            watchlist_status="fully_watched",
        )
        self._insert_progress("seed-a", "seed-a-1", episode_number=1, completion_ratio=1.0, last_watched_at="2026-03-01T01:00:00Z")
        self._map_series("seed-a", 100)
        self._cache_metadata(100, title="Seed A", genres=["Sci-Fi"], studios=["Bones"], source="light_novel")
        self._cache_metadata(300, title="Broader Adoption Pick", mean=8.0, popularity=280, genres=["Romance"], studios=["Madhouse"], source="manga")
        self._cache_metadata(400, title="Narrower Adoption Pick", mean=8.0, popularity=450, genres=["Romance"], studios=["Madhouse"], source="manga")
        self._cache_recommendations(100, [
            {"target_mal_anime_id": 300, "target_title": "Broader Adoption Pick", "num_recommendations": 20, "raw": {}},
            {"target_mal_anime_id": 400, "target_title": "Narrower Adoption Pick", "num_recommendations": 20, "raw": {}},
        ])

        results = [item for item in build_recommendations(self.config, limit=0) if item.kind == "discovery_candidate"]

        self.assertEqual(["available-target-300", "available-target-400"], [item.provider_series_id for item in results])
        self.assertEqual(0, results[0].context["metadata_match_dimensions"])
        self.assertEqual(1, results[0].context["catalog_quality_bonus"])
        self.assertIsNone(results[0].context["catalog_mean_band"])
        self.assertEqual("broad", results[0].context["catalog_popularity_band"])
        self.assertEqual(0, results[1].context["catalog_quality_bonus"])
        self.assertIn(
            "global catalog quality/adoption calibration slightly favored this candidate (broad catalog adoption)",
            results[0].reasons,
        )
        self.assertGreater(results[0].priority, results[1].priority)

    def test_discovery_candidate_tempers_low_mean_even_without_metadata_rich_tie(self) -> None:
        self._insert_series(
            "seed-a",
            title="Seed A",
            season_title="Seed A (English Dub)",
            watchlist_status="fully_watched",
        )
        self._insert_progress("seed-a", "seed-a-1", episode_number=1, completion_ratio=1.0, last_watched_at="2026-03-01T01:00:00Z")
        self._map_series("seed-a", 100)
        self._cache_metadata(100, title="Seed A", genres=["Sci-Fi"], studios=["Bones"], source="light_novel")
        self._cache_metadata(300, title="Baseline Pick", mean=7.2, popularity=800, genres=["Romance"], studios=["Madhouse"], source="manga")
        self._cache_metadata(400, title="Low Mean Pick", mean=6.4, popularity=800, genres=["Romance"], studios=["Madhouse"], source="manga")
        self._cache_recommendations(100, [
            {"target_mal_anime_id": 300, "target_title": "Baseline Pick", "num_recommendations": 20, "raw": {}},
            {"target_mal_anime_id": 400, "target_title": "Low Mean Pick", "num_recommendations": 20, "raw": {}},
        ])

        results = [item for item in build_recommendations(self.config, limit=0) if item.kind == "discovery_candidate"]

        self.assertEqual(["available-target-300", "available-target-400"], [item.provider_series_id for item in results])
        self.assertEqual(0, results[0].context["catalog_quality_penalty"])
        self.assertEqual(0, results[0].context["catalog_quality_adjustment"])
        self.assertEqual(2, results[1].context["catalog_quality_penalty"])
        self.assertEqual(-2, results[1].context["catalog_quality_adjustment"])
        self.assertEqual("low", results[1].context["catalog_low_mean_band"])
        self.assertIsNone(results[1].context["catalog_niche_popularity_band"])
        self.assertIn(
            "global catalog quality/adoption calibration slightly tempered this candidate (low MAL mean)",
            results[1].reasons,
        )
        self.assertGreater(results[0].priority, results[1].priority)

    def test_discovery_candidate_tempers_very_niche_catalog_adoption_even_without_metadata_rich_tie(self) -> None:
        self._insert_series(
            "seed-a",
            title="Seed A",
            season_title="Seed A (English Dub)",
            watchlist_status="fully_watched",
        )
        self._insert_progress("seed-a", "seed-a-1", episode_number=1, completion_ratio=1.0, last_watched_at="2026-03-01T01:00:00Z")
        self._map_series("seed-a", 100)
        self._cache_metadata(100, title="Seed A", genres=["Sci-Fi"], studios=["Bones"], source="light_novel")
        self._cache_metadata(300, title="Baseline Adoption Pick", mean=7.4, popularity=1000, genres=["Romance"], studios=["Madhouse"], source="manga")
        self._cache_metadata(400, title="Very Niche Pick", mean=7.4, popularity=11000, genres=["Romance"], studios=["Madhouse"], source="manga")
        self._cache_recommendations(100, [
            {"target_mal_anime_id": 300, "target_title": "Baseline Adoption Pick", "num_recommendations": 20, "raw": {}},
            {"target_mal_anime_id": 400, "target_title": "Very Niche Pick", "num_recommendations": 20, "raw": {}},
        ])

        results = [item for item in build_recommendations(self.config, limit=0) if item.kind == "discovery_candidate"]

        self.assertEqual(["available-target-300", "available-target-400"], [item.provider_series_id for item in results])
        self.assertEqual(0, results[0].context["catalog_quality_penalty"])
        self.assertEqual(0, results[0].context["catalog_quality_adjustment"])
        self.assertEqual(2, results[1].context["catalog_quality_penalty"])
        self.assertEqual(-2, results[1].context["catalog_quality_adjustment"])
        self.assertIsNone(results[1].context["catalog_low_mean_band"])
        self.assertEqual("very_niche", results[1].context["catalog_niche_popularity_band"])
        self.assertIn(
            "global catalog quality/adoption calibration slightly tempered this candidate (very niche catalog adoption)",
            results[1].reasons,
        )
        self.assertGreater(results[0].priority, results[1].priority)

    def test_discovery_candidate_exposes_net_catalog_quality_adjustment(self) -> None:
        self._insert_series(
            "seed-a",
            title="Seed A",
            season_title="Seed A (English Dub)",
            watchlist_status="fully_watched",
        )
        self._insert_progress("seed-a", "seed-a-1", episode_number=1, completion_ratio=1.0, last_watched_at="2026-03-01T01:00:00Z")
        self._map_series("seed-a", 100)
        self._cache_metadata(100, title="Seed A", genres=["Sci-Fi"], studios=["Bones"], source="light_novel")
        self._cache_metadata(300, title="Elite Niche Pick", mean=8.7, popularity=12000, genres=["Romance"], studios=["Madhouse"], source="manga")
        self._cache_recommendations(100, [
            {"target_mal_anime_id": 300, "target_title": "Elite Niche Pick", "num_recommendations": 20, "raw": {}},
        ])

        results = [item for item in build_recommendations(self.config, limit=0) if item.kind == "discovery_candidate"]

        self.assertEqual(["available-target-300"], [item.provider_series_id for item in results])
        self.assertEqual(2, results[0].context["catalog_quality_bonus"])
        self.assertEqual(2, results[0].context["catalog_quality_penalty"])
        self.assertEqual(0, results[0].context["catalog_quality_adjustment"])
        self.assertEqual("elite", results[0].context["catalog_mean_band"])
        self.assertEqual("very_niche", results[0].context["catalog_niche_popularity_band"])

    def test_discovery_candidate_prefers_metadata_rich_alignment_when_votes_tie(self) -> None:
        self._insert_series(
            "seed-a",
            title="Seed A",
            season_title="Seed A (English Dub)",
            watchlist_status="fully_watched",
        )
        self._insert_progress("seed-a", "seed-a-1", episode_number=1, completion_ratio=1.0, last_watched_at="2026-03-01T01:00:00Z")
        self._map_series("seed-a", 100)
        self._cache_metadata(100, title="Seed A", genres=["Sci-Fi"], studios=["Bones"], source="light_novel")
        self._cache_metadata(300, title="Metadata Rich", mean=8.0, popularity=500, genres=["Sci-Fi"], studios=["Bones"], source="light_novel")
        self._cache_metadata(400, title="Genre Only", mean=8.0, popularity=500, genres=["Sci-Fi"], studios=["Madhouse"], source="manga")
        self._cache_recommendations(100, [
            {"target_mal_anime_id": 300, "target_title": "Metadata Rich", "num_recommendations": 20, "raw": {}},
            {"target_mal_anime_id": 400, "target_title": "Genre Only", "num_recommendations": 20, "raw": {}},
        ])

        results = [item for item in build_recommendations(self.config, limit=0) if item.kind == "discovery_candidate"]

        self.assertEqual(["available-target-300", "available-target-400"], [item.provider_series_id for item in results])
        self.assertEqual(3, results[0].context["metadata_match_dimensions"])
        self.assertGreater(results[0].context["metadata_affinity_bonus"], 0)
        self.assertEqual(1, results[1].context["metadata_match_dimensions"])
        self.assertEqual(0, results[1].context["metadata_affinity_bonus"])
        self.assertIn("metadata-rich seed alignment across 3 dimensions counted as an extra tie-break", results[0].reasons)
        self.assertGreater(results[0].priority, results[1].priority)

    def test_discovery_candidate_prefers_popular_metadata_rich_alignment_when_votes_tie(self) -> None:
        self._insert_series(
            "seed-a",
            title="Seed A",
            season_title="Seed A (English Dub)",
            watchlist_status="fully_watched",
        )
        self._insert_progress("seed-a", "seed-a-1", episode_number=1, completion_ratio=1.0, last_watched_at="2026-03-01T01:00:00Z")
        self._map_series("seed-a", 100)
        self._cache_metadata(100, title="Seed A", genres=["Sci-Fi"], studios=["Bones"], source="light_novel")
        self._cache_metadata(300, title="Popular Metadata Rich", mean=8.0, popularity=200, genres=["Sci-Fi"], studios=["Bones"], source="light_novel")
        self._cache_metadata(400, title="Obscure Metadata Rich", mean=8.0, popularity=5000, genres=["Sci-Fi"], studios=["Bones"], source="light_novel")
        self._cache_recommendations(100, [
            {"target_mal_anime_id": 300, "target_title": "Popular Metadata Rich", "num_recommendations": 20, "raw": {}},
            {"target_mal_anime_id": 400, "target_title": "Obscure Metadata Rich", "num_recommendations": 20, "raw": {}},
        ])

        results = [item for item in build_recommendations(self.config, limit=0) if item.kind == "discovery_candidate"]

        self.assertEqual(["available-target-300", "available-target-400"], [item.provider_series_id for item in results])
        self.assertEqual(3, results[0].context["metadata_match_dimensions"])
        self.assertEqual(3, results[1].context["metadata_match_dimensions"])
        self.assertGreater(results[0].context["metadata_affinity_bonus"], results[1].context["metadata_affinity_bonus"])
        self.assertGreater(results[0].priority, results[1].priority)

    def test_discovery_candidate_prefers_higher_mean_inside_metadata_rich_tie(self) -> None:
        self._insert_series(
            "seed-a",
            title="Seed A",
            season_title="Seed A (English Dub)",
            watchlist_status="fully_watched",
        )
        self._insert_progress("seed-a", "seed-a-1", episode_number=1, completion_ratio=1.0, last_watched_at="2026-03-01T01:00:00Z")
        self._map_series("seed-a", 100)
        self._cache_metadata(100, title="Seed A", genres=["Sci-Fi"], studios=["Bones"], source="light_novel")
        self._cache_metadata(300, title="Higher Mean Metadata Rich", mean=8.6, popularity=500, genres=["Sci-Fi"], studios=["Bones"], source="light_novel")
        self._cache_metadata(400, title="Lower Mean Metadata Rich", mean=8.1, popularity=500, genres=["Sci-Fi"], studios=["Bones"], source="light_novel")
        self._cache_recommendations(100, [
            {"target_mal_anime_id": 300, "target_title": "Higher Mean Metadata Rich", "num_recommendations": 20, "raw": {}},
            {"target_mal_anime_id": 400, "target_title": "Lower Mean Metadata Rich", "num_recommendations": 20, "raw": {}},
        ])

        results = [item for item in build_recommendations(self.config, limit=0) if item.kind == "discovery_candidate"]

        self.assertEqual(["available-target-300", "available-target-400"], [item.provider_series_id for item in results])
        self.assertEqual(3, results[0].context["metadata_match_dimensions"])
        self.assertEqual(2, results[0].context["metadata_quality_bonus"])
        self.assertEqual("elite", results[0].context["metadata_mean_band"])
        self.assertIsNone(results[0].context["metadata_popularity_band"])
        self.assertEqual(0, results[1].context["metadata_quality_bonus"])
        self.assertIn(
            "metadata-rich tie-break also favored stronger MAL quality/adoption signals (elite MAL mean)",
            results[0].reasons,
        )
        self.assertGreater(results[0].priority, results[1].priority)

    def test_discovery_candidate_prefers_broader_adoption_inside_metadata_rich_tie(self) -> None:
        self._insert_series(
            "seed-a",
            title="Seed A",
            season_title="Seed A (English Dub)",
            watchlist_status="fully_watched",
        )
        self._insert_progress("seed-a", "seed-a-1", episode_number=1, completion_ratio=1.0, last_watched_at="2026-03-01T01:00:00Z")
        self._map_series("seed-a", 100)
        self._cache_metadata(100, title="Seed A", genres=["Sci-Fi"], studios=["Bones"], source="light_novel")
        self._cache_metadata(300, title="Broadly Adopted Metadata Rich", mean=8.0, popularity=300, genres=["Sci-Fi"], studios=["Bones"], source="light_novel")
        self._cache_metadata(400, title="Less Adopted Metadata Rich", mean=8.0, popularity=450, genres=["Sci-Fi"], studios=["Bones"], source="light_novel")
        self._cache_recommendations(100, [
            {"target_mal_anime_id": 300, "target_title": "Broadly Adopted Metadata Rich", "num_recommendations": 20, "raw": {}},
            {"target_mal_anime_id": 400, "target_title": "Less Adopted Metadata Rich", "num_recommendations": 20, "raw": {}},
        ])

        results = [item for item in build_recommendations(self.config, limit=0) if item.kind == "discovery_candidate"]

        self.assertEqual(["available-target-300", "available-target-400"], [item.provider_series_id for item in results])
        self.assertEqual(3, results[0].context["metadata_match_dimensions"])
        self.assertEqual(1, results[0].context["metadata_quality_bonus"])
        self.assertIsNone(results[0].context["metadata_mean_band"])
        self.assertEqual("broad", results[0].context["metadata_popularity_band"])
        self.assertEqual(0, results[1].context["metadata_quality_bonus"])
        self.assertIn(
            "metadata-rich tie-break also favored stronger MAL quality/adoption signals (broad catalog adoption)",
            results[0].reasons,
        )
        self.assertGreater(results[0].priority, results[1].priority)

    def test_discovery_candidate_skips_direct_franchise_relations_from_seed_titles(self) -> None:
        self._insert_series(
            "seed-a",
            title="Seed A",
            season_title="Seed A (English Dub)",
            watchlist_status="fully_watched",
        )
        self._insert_progress("seed-a", "seed-a-1", episode_number=1, completion_ratio=1.0, last_watched_at="2026-03-01T01:00:00Z")
        self._map_series("seed-a", 100)
        self._cache_metadata(100, title="Seed A")
        self._cache_metadata(300, title="Seed A Sequel")
        self._cache_metadata(400, title="Different Discovery")
        self._cache_relations(
            100,
            [
                {
                    "related_mal_anime_id": 300,
                    "relation_type": "sequel",
                    "relation_type_formatted": "Sequel",
                    "related_title": "Seed A Sequel",
                    "raw": {"relation_type": "sequel", "node": {"id": 300, "title": "Seed A Sequel"}},
                }
            ],
        )
        self._cache_recommendations(100, [
            {"target_mal_anime_id": 300, "target_title": "Seed A Sequel", "num_recommendations": 20, "raw": {}},
            {"target_mal_anime_id": 400, "target_title": "Different Discovery", "num_recommendations": 18, "raw": {}},
        ])

        results = [item for item in build_recommendations(self.config, limit=0) if item.kind == "discovery_candidate"]

        self.assertEqual(["available-target-400"], [item.provider_series_id for item in results])

    def test_discovery_candidate_skips_franchise_relations_even_when_edge_comes_from_different_seed(self) -> None:
        self._insert_series(
            "seed-a",
            title="Seed A",
            season_title="Seed A (English Dub)",
            watchlist_status="fully_watched",
        )
        self._insert_progress("seed-a", "seed-a-1", episode_number=1, completion_ratio=1.0, last_watched_at="2026-03-01T01:00:00Z")
        self._insert_series(
            "seed-b",
            title="Seed B",
            season_title="Seed B (English Dub)",
            watchlist_status="fully_watched",
        )
        self._insert_progress("seed-b", "seed-b-1", episode_number=1, completion_ratio=1.0, last_watched_at="2026-03-02T01:00:00Z")
        self._map_series("seed-a", 100)
        self._map_series("seed-b", 200)
        self._cache_metadata(100, title="Seed A")
        self._cache_metadata(200, title="Seed B")
        self._cache_metadata(300, title="Seed A Sequel")
        self._cache_metadata(400, title="Different Discovery")
        self._cache_relations(
            100,
            [
                {
                    "related_mal_anime_id": 300,
                    "relation_type": "sequel",
                    "relation_type_formatted": "Sequel",
                    "related_title": "Seed A Sequel",
                    "raw": {"relation_type": "sequel", "node": {"id": 300, "title": "Seed A Sequel"}},
                }
            ],
        )
        self._cache_recommendations(200, [
            {"target_mal_anime_id": 300, "target_title": "Seed A Sequel", "num_recommendations": 20, "raw": {}},
            {"target_mal_anime_id": 400, "target_title": "Different Discovery", "num_recommendations": 18, "raw": {}},
        ])

        results = [item for item in build_recommendations(self.config, limit=0) if item.kind == "discovery_candidate"]

        self.assertEqual(["available-target-400"], [item.provider_series_id for item in results])

    def test_discovery_candidate_surfaces_titles_available_in_provider_catalog(self) -> None:
        self._insert_series(
            "seed-a",
            title="Seed A",
            season_title="Seed A (English Dub)",
            watchlist_status="fully_watched",
        )
        self._insert_progress("seed-a", "seed-a-1", episode_number=1, completion_ratio=1.0, last_watched_at="2026-03-01T01:00:00Z")
        self._insert_series(
            "already-here",
            title="Already Here",
            season_title="Already Here (English Dub)",
            watchlist_status="available",
        )
        self._map_series("seed-a", 100)
        self._cache_metadata(100, title="Seed A")
        self._cache_metadata(300, title="Already Here")
        self._cache_recommendations(100, [
            {"target_mal_anime_id": 300, "target_title": "Already Here", "num_recommendations": 20, "raw": {}},
        ], make_targets_available=False)

        results = [
            item
            for item in build_recommendations(
                self.config,
                limit=0,
                require_provider_availability=False,
                include_discovery_candidates_without_actionable_provider_evidence=True,
            )
            if item.kind == "discovery_candidate"
        ]

        self.assertEqual(1, len(results))
        self.assertEqual("already-here", results[0].provider_series_id)
        self.assertEqual("crunchyroll", results[0].provider)
        self.assertEqual(["crunchyroll"], results[0].context["available_via_providers"])
        self.assertEqual("title_alias", results[0].context["availability_confidence"])
        self.assertEqual(["title_alias"], results[0].context["availability_match_kinds"])

    def test_discovery_candidate_prefers_mapped_provider_availability_confidence(self) -> None:
        self._insert_series(
            "seed-a",
            title="Seed A",
            season_title="Seed A (English Dub)",
            watchlist_status="fully_watched",
        )
        self._insert_progress("seed-a", "seed-a-1", episode_number=1, completion_ratio=1.0, last_watched_at="2026-03-01T01:00:00Z")
        self._insert_series(
            "mapped-here",
            title="Mapped Here",
            season_title="Mapped Here (English Dub)",
            watchlist_status="available",
        )
        self._map_series("seed-a", 100)
        self._map_series("mapped-here", 300)
        self._cache_metadata(100, title="Seed A")
        self._cache_metadata(300, title="Mapped Here")
        self._cache_recommendations(100, [
            {"target_mal_anime_id": 300, "target_title": "Mapped Here", "num_recommendations": 20, "raw": {}},
        ], make_targets_available=False)

        results = [
            item
            for item in build_recommendations(
                self.config,
                limit=0,
                require_provider_availability=False,
                include_discovery_candidates_without_actionable_provider_evidence=True,
            )
            if item.kind == "discovery_candidate"
        ]

        self.assertEqual(1, len(results))
        self.assertEqual("mapped", results[0].context["availability_confidence"])
        self.assertEqual(["mapped_mal"], results[0].context["availability_match_kinds"])
        self.assertEqual("mapped_mal", results[0].context["available_provider_series"][0]["availability_match_kind"])

    def test_provider_required_discovery_ignores_fully_watched_provider_catalog_matches(self) -> None:
        self._insert_series(
            "seed-a",
            title="Seed A",
            season_title="Seed A (English Dub)",
            watchlist_status="fully_watched",
        )
        self._insert_progress("seed-a", "seed-a-1", episode_number=1, completion_ratio=1.0, last_watched_at="2026-03-01T01:00:00Z")
        self._insert_series(
            "already-watched",
            title="Already Watched",
            season_title="Already Watched (English Dub)",
            watchlist_status="fully_watched",
        )
        self._map_series("seed-a", 100)
        self._cache_metadata(100, title="Seed A")
        self._cache_metadata(300, title="Already Watched")
        self._cache_recommendations(100, [
            {"target_mal_anime_id": 300, "target_title": "Already Watched", "num_recommendations": 20, "raw": {}},
        ])

        visible = [
            item
            for item in build_recommendations(self.config, limit=0, require_provider_availability=True)
            if item.kind == "discovery_candidate"
        ]

        self.assertEqual([], visible)

    def test_provider_required_discovery_accepts_verified_exact_title_search_evidence(self) -> None:
        self._insert_series(
            "seed-a",
            title="Seed A",
            season_title="Seed A (English Dub)",
            watchlist_status="fully_watched",
        )
        self._insert_progress("seed-a", "seed-a-1", episode_number=1, completion_ratio=1.0, last_watched_at="2026-03-01T01:00:00Z")
        self._map_series("seed-a", 100)
        self._cache_metadata(100, title="Seed A")
        self._cache_metadata(300, title="Exact Search Target")
        self._cache_recommendations(100, [
            {"target_mal_anime_id": 300, "target_title": "Exact Search Target", "num_recommendations": 20, "raw": {}},
        ], make_targets_available=False)
        self._cache_provider_eligibility(
            300,
            provider_series_id="exact-search-target",
            provider_title="Exact Search Target",
            identity_match_kind="provider_title_search_exact",
        )

        visible = [
            item
            for item in build_recommendations(self.config, limit=0, require_provider_availability=True)
            if item.kind == "discovery_candidate"
        ]

        self.assertEqual(1, len(visible))
        self.assertEqual("exact-search-target", visible[0].provider_series_id)
        self.assertEqual("present", visible[0].context["english_dub_signal"])
        self.assertEqual("provider_title_search_exact", visible[0].context["provider_eligibility_evidence"][0]["identity_match_kind"])

    def test_provider_required_discovery_accepts_verified_franchise_shell_eligibility_evidence(self) -> None:
        self._insert_series(
            "seed-a",
            title="Seed A",
            season_title="Seed A (English Dub)",
            watchlist_status="fully_watched",
        )
        self._insert_progress("seed-a", "seed-a-1", episode_number=1, completion_ratio=1.0, last_watched_at="2026-03-01T01:00:00Z")
        self._map_series("seed-a", 100)
        self._cache_metadata(100, title="Seed A")
        self._cache_metadata(303, title="Shell Target")
        self._cache_recommendations(100, [
            {"target_mal_anime_id": 303, "target_title": "Shell Target", "num_recommendations": 20, "raw": {}},
        ], make_targets_available=False)
        self._cache_provider_eligibility(
            303,
            provider_series_id="aggregate-shell",
            provider_title="Shell Franchise",
            identity_match_kind="provider_franchise_shell_child_match",
        )

        visible = [
            item
            for item in build_recommendations(self.config, limit=0, require_provider_availability=True)
            if item.kind == "discovery_candidate"
        ]

        self.assertEqual(1, len(visible))
        self.assertEqual("aggregate-shell", visible[0].provider_series_id)
        self.assertEqual("provider_franchise_shell_child_match", visible[0].context["provider_eligibility_evidence"][0]["identity_match_kind"])

    def test_provider_required_discovery_rejects_legacy_franchise_shell_metadata_evidence(self) -> None:
        self._insert_series(
            "seed-a",
            title="Seed A",
            season_title="Seed A (English Dub)",
            watchlist_status="fully_watched",
        )
        self._insert_progress("seed-a", "seed-a-1", episode_number=1, completion_ratio=1.0, last_watched_at="2026-03-01T01:00:00Z")
        self._map_series("seed-a", 100)
        self._cache_metadata(100, title="Seed A")
        self._cache_metadata(37450, title="Legacy Shell Target")
        self._cache_recommendations(100, [
            {"target_mal_anime_id": 37450, "target_title": "Legacy Shell Target", "num_recommendations": 20, "raw": {}},
        ], make_targets_available=False)
        self._cache_provider_eligibility(
            37450,
            provider_series_id="legacy-aggregate-shell",
            provider_title="Legacy Shell Franchise",
            identity_match_kind="provider_franchise_shell_metadata_match",
        )

        visible = [
            item
            for item in build_recommendations(self.config, limit=0, require_provider_availability=True)
            if item.kind == "discovery_candidate"
        ]
        diagnostic = [
            item
            for item in build_recommendations(
                self.config,
                limit=0,
                require_provider_availability=False,
                include_discovery_candidates_without_actionable_provider_evidence=True,
            )
            if item.kind == "discovery_candidate"
        ]

        self.assertEqual([], visible)
        self.assertEqual(1, len(diagnostic))
        self.assertEqual("mal", diagnostic[0].provider)
        self.assertEqual("provider_franchise_shell_metadata_match", diagnostic[0].context["provider_eligibility_evidence"][0]["identity_match_kind"])

    def test_discovery_target_metadata_refresh_considers_hidive_mappings(self) -> None:
        self._insert_series(
            "hidive-seed",
            title="HIDIVE Seed",
            season_title="HIDIVE Seed (English Dub)",
            watchlist_status="fully_watched",
            provider="hidive",
        )
        self._insert_progress(
            "hidive-seed",
            "hidive-seed-1",
            episode_number=1,
            completion_ratio=1.0,
            last_watched_at="2026-03-01T01:00:00Z",
            provider="hidive",
        )
        self._map_series("hidive-seed", 9100, provider="hidive")

        def fake_get_anime_details(anime_id: int, *, fields: str = "") -> dict:
            payloads = {
                9100: {
                    "id": 9100,
                    "title": "HIDIVE Seed",
                    "alternative_titles": {},
                    "media_type": "tv",
                    "status": "finished_airing",
                    "num_episodes": 12,
                    "mean": 8.1,
                    "popularity": 500,
                    "start_season": {"year": 2020, "season": "winter"},
                    "related_anime": [],
                    "recommendations": [
                        {"node": {"id": 9200, "title": "HIDIVE Discovery"}, "num_recommendations": 20},
                    ],
                    "my_list_status": {"status": "completed", "num_episodes_watched": 12},
                },
                9200: {
                    "id": 9200,
                    "title": "HIDIVE Discovery",
                    "alternative_titles": {"en": "HIDIVE Discovery"},
                    "media_type": "tv",
                    "status": "finished_airing",
                    "num_episodes": 24,
                    "mean": 8.7,
                    "popularity": 120,
                    "start_season": {"year": 2022, "season": "fall"},
                    "my_list_status": {"status": "plan_to_watch", "num_episodes_watched": 0},
                },
            }
            return payloads[anime_id]

        with patch("mal_updater.recommendation_metadata.MalClient.get_anime_details", side_effect=fake_get_anime_details) as get_details:
            summary = refresh_recommendation_metadata(
                self.config,
                include_discovery_targets=True,
                discovery_target_limit=1,
            )

        self.assertEqual(1, summary.considered)
        self.assertEqual(1, summary.refreshed)
        requested_ids = [call.args[0] for call in get_details.call_args_list]
        self.assertEqual([9100, 9200], requested_ids)

        metadata_by_id = get_mal_anime_metadata_map(self.config.db_path)
        self.assertIn(9100, metadata_by_id)
        self.assertIn(9200, metadata_by_id)

    def test_recommendation_metadata_refresh_continues_after_mal_detail_failure(self) -> None:
        self._insert_series("seed-a", title="Seed A", watchlist_status="fully_watched")
        self._insert_progress("seed-a", "seed-a-1", episode_number=1, completion_ratio=1.0, last_watched_at="2026-03-01T01:00:00Z")
        self._map_series("seed-a", 100)
        self._insert_series("seed-b", title="Seed B", watchlist_status="fully_watched")
        self._insert_progress("seed-b", "seed-b-1", episode_number=1, completion_ratio=1.0, last_watched_at="2026-03-02T01:00:00Z")
        self._map_series("seed-b", 200)
        self._insert_series("seed-c", title="Seed C", watchlist_status="fully_watched")
        self._insert_progress("seed-c", "seed-c-1", episode_number=1, completion_ratio=1.0, last_watched_at="2026-03-03T01:00:00Z")
        self._map_series("seed-c", 300)

        def fake_get_anime_details(anime_id: int, *, fields: str = "") -> dict:
            if anime_id == 200:
                raise MalApiError("MAL API anime details failed for anime_id=200: HTTP 504")
            return {
                "id": anime_id,
                "title": f"Seed {anime_id}",
                "alternative_titles": {},
                "media_type": "tv",
                "status": "finished_airing",
                "num_episodes": 12,
                "mean": 8.0,
                "popularity": anime_id,
                "start_season": {"year": 2020, "season": "winter"},
                "related_anime": [],
                "recommendations": [],
                "my_list_status": {"status": "completed", "num_episodes_watched": 12},
            }

        with patch("mal_updater.recommendation_metadata.MalClient.get_anime_details", side_effect=fake_get_anime_details) as get_details:
            summary = refresh_recommendation_metadata(self.config)

        self.assertEqual(3, summary.considered)
        self.assertEqual(2, summary.refreshed)
        self.assertEqual(1, len(summary.failures or []))
        failure = (summary.failures or [])[0]
        self.assertEqual(200, failure.mal_anime_id)
        self.assertEqual("mapped_metadata", failure.stage)
        self.assertIn("HTTP 504", failure.error)
        self.assertEqual([100, 200, 300], [call.args[0] for call in get_details.call_args_list])
        self.assertEqual(1, summary.as_dict()["failed"])
        self.assertEqual(200, summary.as_dict()["failures"][0]["mal_anime_id"])

        metadata_by_id = get_mal_anime_metadata_map(self.config.db_path)
        self.assertIn(100, metadata_by_id)
        self.assertNotIn(200, metadata_by_id)
        self.assertIn(300, metadata_by_id)

    def test_discovery_target_metadata_refresh_skips_already_mapped_titles(self) -> None:
        self._insert_series(
            "seed-a",
            title="Seed A",
            season_title="Seed A (English Dub)",
            watchlist_status="fully_watched",
        )
        self._insert_progress("seed-a", "seed-a-1", episode_number=1, completion_ratio=1.0, last_watched_at="2026-03-01T01:00:00Z")
        self._insert_series(
            "seed-b",
            title="Mapped Follow-Up",
            season_title="Mapped Follow-Up (English Dub)",
            watchlist_status="fully_watched",
        )
        self._insert_progress("seed-b", "seed-b-1", episode_number=1, completion_ratio=1.0, last_watched_at="2026-03-02T01:00:00Z")
        self._map_series("seed-a", 100)
        self._map_series("seed-b", 200)

        def fake_get_anime_details(anime_id: int, *, fields: str = "") -> dict:
            payloads = {
                100: {
                    "id": 100,
                    "title": "Seed A",
                    "alternative_titles": {},
                    "media_type": "tv",
                    "status": "finished_airing",
                    "num_episodes": 12,
                    "mean": 8.1,
                    "popularity": 500,
                    "start_season": {"year": 2020, "season": "winter"},
                    "related_anime": [],
                    "recommendations": [
                        {"node": {"id": 200, "title": "Mapped Follow-Up"}, "num_recommendations": 50},
                        {"node": {"id": 300, "title": "Unmapped Discovery"}, "num_recommendations": 10},
                    ],
                    "my_list_status": {"status": "completed", "num_episodes_watched": 12},
                },
                200: {
                    "id": 200,
                    "title": "Mapped Follow-Up",
                    "alternative_titles": {},
                    "media_type": "tv",
                    "status": "finished_airing",
                    "num_episodes": 12,
                    "mean": 8.0,
                    "popularity": 600,
                    "start_season": {"year": 2021, "season": "spring"},
                    "related_anime": [],
                    "recommendations": [],
                    "my_list_status": {"status": "completed", "num_episodes_watched": 12},
                },
                300: {
                    "id": 300,
                    "title": "Unmapped Discovery",
                    "alternative_titles": {"en": "Unmapped Discovery"},
                    "media_type": "tv",
                    "status": "finished_airing",
                    "num_episodes": 24,
                    "mean": 8.4,
                    "popularity": 140,
                    "start_season": {"year": 2022, "season": "fall"},
                    "my_list_status": {"status": "plan_to_watch", "num_episodes_watched": 0},
                },
            }
            return payloads[anime_id]

        with patch("mal_updater.recommendation_metadata.MalClient.get_anime_details", side_effect=fake_get_anime_details) as get_details:
            summary = refresh_recommendation_metadata(
                self.config,
                limit=1,
                include_discovery_targets=True,
                discovery_target_limit=1,
            )

        self.assertEqual(1, summary.considered)
        self.assertEqual(1, summary.refreshed)
        self.assertEqual(1, summary.discovery_considered)
        self.assertEqual(1, summary.discovery_refreshed)
        requested_ids = [call.args[0] for call in get_details.call_args_list]
        self.assertEqual([100, 300], requested_ids)

        metadata_by_id = get_mal_anime_metadata_map(self.config.db_path)
        self.assertIn(300, metadata_by_id)
        self.assertNotIn(200, metadata_by_id)

    def test_discovery_target_metadata_refresh_persists_my_list_status_and_suppresses_candidate(self) -> None:
        self._insert_series(
            "seed-a",
            title="Seed A",
            season_title="Seed A (English Dub)",
            watchlist_status="fully_watched",
        )
        self._insert_progress("seed-a", "seed-a-1", episode_number=1, completion_ratio=1.0, last_watched_at="2026-03-01T01:00:00Z")
        self._insert_series(
            "seed-b",
            title="Seed B",
            season_title="Seed B (English Dub)",
            watchlist_status="fully_watched",
        )
        self._insert_progress("seed-b", "seed-b-1", episode_number=1, completion_ratio=1.0, last_watched_at="2026-03-02T01:00:00Z")
        self._map_series("seed-a", 100)
        self._map_series("seed-b", 200)

        baseline = build_recommendations(self.config, limit=0)
        baseline_discovery = [item for item in baseline if item.kind == "discovery_candidate"]
        self.assertEqual([], baseline_discovery)

        def fake_get_anime_details(anime_id: int, *, fields: str = "") -> dict:
            payloads = {
                100: {
                    "id": 100,
                    "title": "Seed A",
                    "alternative_titles": {},
                    "media_type": "tv",
                    "status": "finished_airing",
                    "num_episodes": 12,
                    "mean": 8.1,
                    "popularity": 500,
                    "start_season": {"year": 2020, "season": "winter"},
                    "related_anime": [],
                    "recommendations": [
                        {"node": {"id": 300, "title": "Discovery Hit"}, "num_recommendations": 20},
                    ],
                    "my_list_status": {"status": "completed", "num_episodes_watched": 12},
                },
                200: {
                    "id": 200,
                    "title": "Seed B",
                    "alternative_titles": {},
                    "media_type": "tv",
                    "status": "finished_airing",
                    "num_episodes": 13,
                    "mean": 7.9,
                    "popularity": 800,
                    "start_season": {"year": 2021, "season": "spring"},
                    "related_anime": [],
                    "recommendations": [
                        {"node": {"id": 300, "title": "Discovery Hit"}, "num_recommendations": 15},
                    ],
                    "my_list_status": {"status": "completed", "num_episodes_watched": 13},
                },
                300: {
                    "id": 300,
                    "title": "Discovery Hit",
                    "alternative_titles": {"en": "Discovery Hit"},
                    "media_type": "tv",
                    "status": "finished_airing",
                    "num_episodes": 24,
                    "mean": 8.7,
                    "popularity": 120,
                    "start_season": {"year": 2022, "season": "fall"},
                    "my_list_status": {"status": "watching", "num_episodes_watched": 3},
                },
            }
            return payloads[anime_id]

        with patch("mal_updater.recommendation_metadata.MalClient.get_anime_details", side_effect=fake_get_anime_details) as get_details:
            summary = refresh_recommendation_metadata(
                self.config,
                include_discovery_targets=True,
            )

        self.assertEqual(2, summary.considered)
        self.assertEqual(2, summary.refreshed)
        requested_ids = [call.args[0] for call in get_details.call_args_list]
        self.assertEqual([100, 200, 300], requested_ids)

        metadata_by_id = get_mal_anime_metadata_map(self.config.db_path)
        self.assertEqual("watching", metadata_by_id[300].raw["my_list_status"]["status"])
        self.assertEqual(3, metadata_by_id[300].raw["my_list_status"]["num_episodes_watched"])

        results = build_recommendations(self.config, limit=0)
        discovery = [item for item in results if item.kind == "discovery_candidate" and item.provider_series_id == "available-target-300"]
        self.assertEqual([], discovery)

    def test_discovery_target_limit_prefers_higher_aggregate_votes_when_support_count_ties(self) -> None:
        self._insert_series(
            "seed-a",
            title="Seed A",
            season_title="Seed A (English Dub)",
            watchlist_status="fully_watched",
        )
        self._insert_progress("seed-a", "seed-a-1", episode_number=1, completion_ratio=1.0, last_watched_at="2026-03-01T01:00:00Z")
        self._insert_series(
            "seed-b",
            title="Seed B",
            season_title="Seed B (English Dub)",
            watchlist_status="fully_watched",
        )
        self._insert_progress("seed-b", "seed-b-1", episode_number=1, completion_ratio=1.0, last_watched_at="2026-03-02T01:00:00Z")
        self._map_series("seed-a", 100)
        self._map_series("seed-b", 200)

        def fake_get_anime_details(anime_id: int, *, fields: str = "") -> dict:
            payloads = {
                100: {
                    "id": 100,
                    "title": "Seed A",
                    "alternative_titles": {},
                    "media_type": "tv",
                    "status": "finished_airing",
                    "num_episodes": 12,
                    "mean": 8.1,
                    "popularity": 500,
                    "start_season": {"year": 2020, "season": "winter"},
                    "related_anime": [],
                    "recommendations": [
                        {"node": {"id": 300, "title": "Top Hit"}, "num_recommendations": 40},
                        {"node": {"id": 400, "title": "Runner Up"}, "num_recommendations": 30},
                    ],
                    "my_list_status": {"status": "completed", "num_episodes_watched": 12},
                },
                200: {
                    "id": 200,
                    "title": "Seed B",
                    "alternative_titles": {},
                    "media_type": "tv",
                    "status": "finished_airing",
                    "num_episodes": 13,
                    "mean": 7.9,
                    "popularity": 800,
                    "start_season": {"year": 2021, "season": "spring"},
                    "related_anime": [],
                    "recommendations": [
                        {"node": {"id": 300, "title": "Top Hit"}, "num_recommendations": 5},
                        {"node": {"id": 400, "title": "Runner Up"}, "num_recommendations": 25},
                    ],
                    "my_list_status": {"status": "completed", "num_episodes_watched": 13},
                },
                300: {
                    "id": 300,
                    "title": "Top Hit",
                    "alternative_titles": {"en": "Top Hit"},
                    "media_type": "tv",
                    "status": "finished_airing",
                    "num_episodes": 24,
                    "mean": 8.7,
                    "popularity": 120,
                    "start_season": {"year": 2022, "season": "fall"},
                    "my_list_status": {"status": "watching", "num_episodes_watched": 1},
                },
                400: {
                    "id": 400,
                    "title": "Runner Up",
                    "alternative_titles": {"en": "Runner Up"},
                    "media_type": "tv",
                    "status": "finished_airing",
                    "num_episodes": 12,
                    "mean": 8.0,
                    "popularity": 240,
                    "start_season": {"year": 2021, "season": "summer"},
                    "my_list_status": {"status": "plan_to_watch", "num_episodes_watched": 0},
                },
            }
            return payloads[anime_id]

        with patch("mal_updater.recommendation_metadata.MalClient.get_anime_details", side_effect=fake_get_anime_details) as get_details:
            summary = refresh_recommendation_metadata(
                self.config,
                include_discovery_targets=True,
                discovery_target_limit=1,
            )

        self.assertEqual(2, summary.considered)
        self.assertEqual(2, summary.refreshed)
        requested_ids = [call.args[0] for call in get_details.call_args_list]
        self.assertEqual([100, 200, 400], requested_ids)

        metadata_by_id = get_mal_anime_metadata_map(self.config.db_path)
        self.assertIn(400, metadata_by_id)
        self.assertEqual("plan_to_watch", metadata_by_id[400].raw["my_list_status"]["status"])
        self.assertNotIn(300, metadata_by_id)

    def test_discovery_target_limit_prefers_more_balanced_cross_seed_support_when_totals_tie(self) -> None:
        self._insert_series(
            "seed-a",
            title="Seed A",
            season_title="Seed A (English Dub)",
            watchlist_status="fully_watched",
        )
        self._insert_progress("seed-a", "seed-a-1", episode_number=1, completion_ratio=1.0, last_watched_at="2026-03-01T01:00:00Z")
        self._insert_series(
            "seed-b",
            title="Seed B",
            season_title="Seed B (English Dub)",
            watchlist_status="fully_watched",
        )
        self._insert_progress("seed-b", "seed-b-1", episode_number=1, completion_ratio=1.0, last_watched_at="2026-03-02T01:00:00Z")
        self._map_series("seed-a", 100)
        self._map_series("seed-b", 200)

        def fake_get_anime_details(anime_id: int, *, fields: str = "") -> dict:
            payloads = {
                100: {
                    "id": 100,
                    "title": "Seed A",
                    "alternative_titles": {},
                    "media_type": "tv",
                    "status": "finished_airing",
                    "num_episodes": 12,
                    "mean": 8.1,
                    "popularity": 500,
                    "start_season": {"year": 2020, "season": "winter"},
                    "related_anime": [],
                    "recommendations": [
                        {"node": {"id": 300, "title": "Balanced Pick"}, "num_recommendations": 20},
                        {"node": {"id": 400, "title": "Bursty Pick"}, "num_recommendations": 30},
                    ],
                    "my_list_status": {"status": "completed", "num_episodes_watched": 12},
                },
                200: {
                    "id": 200,
                    "title": "Seed B",
                    "alternative_titles": {},
                    "media_type": "tv",
                    "status": "finished_airing",
                    "num_episodes": 13,
                    "mean": 7.9,
                    "popularity": 800,
                    "start_season": {"year": 2021, "season": "spring"},
                    "related_anime": [],
                    "recommendations": [
                        {"node": {"id": 300, "title": "Balanced Pick"}, "num_recommendations": 10},
                        {"node": {"id": 400, "title": "Bursty Pick"}, "num_recommendations": 0},
                    ],
                    "my_list_status": {"status": "completed", "num_episodes_watched": 13},
                },
                300: {
                    "id": 300,
                    "title": "Balanced Pick",
                    "alternative_titles": {"en": "Balanced Pick"},
                    "media_type": "tv",
                    "status": "finished_airing",
                    "num_episodes": 24,
                    "mean": 8.6,
                    "popularity": 140,
                    "start_season": {"year": 2022, "season": "spring"},
                    "my_list_status": {"status": "watching", "num_episodes_watched": 2},
                },
                400: {
                    "id": 400,
                    "title": "Bursty Pick",
                    "alternative_titles": {"en": "Bursty Pick"},
                    "media_type": "tv",
                    "status": "finished_airing",
                    "num_episodes": 12,
                    "mean": 8.6,
                    "popularity": 140,
                    "start_season": {"year": 2022, "season": "spring"},
                    "my_list_status": {"status": "watching", "num_episodes_watched": 2},
                },
            }
            return payloads[anime_id]

        with patch("mal_updater.recommendation_metadata.MalClient.get_anime_details", side_effect=fake_get_anime_details) as get_details:
            summary = refresh_recommendation_metadata(
                self.config,
                include_discovery_targets=True,
                discovery_target_limit=1,
            )

        self.assertEqual(2, summary.considered)
        self.assertEqual(2, summary.refreshed)
        requested_ids = [call.args[0] for call in get_details.call_args_list]
        self.assertEqual([100, 200, 300], requested_ids)

        metadata_by_id = get_mal_anime_metadata_map(self.config.db_path)
        self.assertIn(300, metadata_by_id)
        self.assertNotIn(400, metadata_by_id)

    def test_discovery_target_limit_prefers_aggregate_multi_seed_support(self) -> None:
        self._insert_series(
            "seed-a",
            title="Seed A",
            season_title="Seed A (English Dub)",
            watchlist_status="fully_watched",
        )
        self._insert_progress("seed-a", "seed-a-1", episode_number=1, completion_ratio=1.0, last_watched_at="2026-03-01T01:00:00Z")
        self._insert_series(
            "seed-b",
            title="Seed B",
            season_title="Seed B (English Dub)",
            watchlist_status="fully_watched",
        )
        self._insert_progress("seed-b", "seed-b-1", episode_number=1, completion_ratio=1.0, last_watched_at="2026-03-02T01:00:00Z")
        self._map_series("seed-a", 100)
        self._map_series("seed-b", 200)

        def fake_get_anime_details(anime_id: int, *, fields: str = "") -> dict:
            payloads = {
                100: {
                    "id": 100,
                    "title": "Seed A",
                    "alternative_titles": {},
                    "media_type": "tv",
                    "status": "finished_airing",
                    "num_episodes": 12,
                    "mean": 8.1,
                    "popularity": 500,
                    "start_season": {"year": 2020, "season": "winter"},
                    "related_anime": [],
                    "recommendations": [
                        {"node": {"id": 300, "title": "Burst Hit"}, "num_recommendations": 45},
                        {"node": {"id": 400, "title": "Consensus Pick"}, "num_recommendations": 22},
                    ],
                    "my_list_status": {"status": "completed", "num_episodes_watched": 12},
                },
                200: {
                    "id": 200,
                    "title": "Seed B",
                    "alternative_titles": {},
                    "media_type": "tv",
                    "status": "finished_airing",
                    "num_episodes": 13,
                    "mean": 7.9,
                    "popularity": 800,
                    "start_season": {"year": 2021, "season": "spring"},
                    "related_anime": [],
                    "recommendations": [
                        {"node": {"id": 400, "title": "Consensus Pick"}, "num_recommendations": 21},
                    ],
                    "my_list_status": {"status": "completed", "num_episodes_watched": 13},
                },
                300: {
                    "id": 300,
                    "title": "Burst Hit",
                    "alternative_titles": {"en": "Burst Hit"},
                    "media_type": "tv",
                    "status": "finished_airing",
                    "num_episodes": 12,
                    "mean": 8.0,
                    "popularity": 220,
                    "start_season": {"year": 2021, "season": "fall"},
                    "my_list_status": {"status": "plan_to_watch", "num_episodes_watched": 0},
                },
                400: {
                    "id": 400,
                    "title": "Consensus Pick",
                    "alternative_titles": {"en": "Consensus Pick"},
                    "media_type": "tv",
                    "status": "finished_airing",
                    "num_episodes": 24,
                    "mean": 8.6,
                    "popularity": 140,
                    "start_season": {"year": 2022, "season": "spring"},
                    "my_list_status": {"status": "watching", "num_episodes_watched": 2},
                },
            }
            return payloads[anime_id]

        with patch("mal_updater.recommendation_metadata.MalClient.get_anime_details", side_effect=fake_get_anime_details) as get_details:
            summary = refresh_recommendation_metadata(
                self.config,
                include_discovery_targets=True,
                discovery_target_limit=1,
            )

        self.assertEqual(2, summary.considered)
        self.assertEqual(2, summary.refreshed)
        requested_ids = [call.args[0] for call in get_details.call_args_list]
        self.assertEqual([100, 200, 400], requested_ids)

        metadata_by_id = get_mal_anime_metadata_map(self.config.db_path)
        self.assertIn(400, metadata_by_id)
        self.assertEqual("watching", metadata_by_id[400].raw["my_list_status"]["status"])
        self.assertNotIn(300, metadata_by_id)

    def test_tail_gap_older_than_fresh_window_is_classified_as_resume_backlog(self) -> None:
        self._insert_series(
            "backlog-show",
            title="Backlog Show",
            season_title="Backlog Show (English Dub)",
            season_number=1,
            watchlist_status="in_progress",
        )
        stale = (datetime.now(timezone.utc) - timedelta(days=23)).replace(microsecond=0)
        self._insert_progress("backlog-show", "b1", episode_number=1, completion_ratio=1.0, last_watched_at=stale.isoformat().replace("+00:00", "Z"))
        self._insert_progress(
            "backlog-show",
            "b2",
            episode_number=2,
            completion_ratio=0.2,
            last_watched_at=(stale + timedelta(hours=1)).isoformat().replace("+00:00", "Z"),
        )

        results = build_recommendations(self.config, limit=0)

        self.assertEqual(1, len(results))
        item = results[0]
        self.assertEqual("resume_backlog", item.kind)
        self.assertIn("backlog continuation", " ".join(item.reasons))
        self.assertEqual(1, item.context["contiguous_tail_gap"])

    def test_tail_gap_within_fresh_window_stays_fresh_dubbed_episode(self) -> None:
        self._insert_series(
            "fresh-show",
            title="Fresh Show",
            season_title="Fresh Show (English Dub)",
            season_number=1,
            watchlist_status="in_progress",
        )
        recent = (datetime.now(timezone.utc) - timedelta(days=21)).replace(microsecond=0)
        self._insert_progress("fresh-show", "f1", episode_number=1, completion_ratio=1.0, last_watched_at=recent.isoformat().replace("+00:00", "Z"))
        self._insert_progress(
            "fresh-show",
            "f2",
            episode_number=2,
            completion_ratio=0.2,
            last_watched_at=(recent + timedelta(hours=1)).isoformat().replace("+00:00", "Z"),
        )

        results = build_recommendations(self.config, limit=0)

        self.assertEqual(1, len(results))
        item = results[0]
        self.assertEqual("new_dubbed_episode", item.kind)
        self.assertEqual(1, item.context["contiguous_tail_gap"])

    def test_filters_out_non_english_dub_candidates(self) -> None:
        self._insert_series(
            "series-fr",
            title="Foreign Dub Show",
            season_title="Foreign Dub Show (French Dub)",
            season_number=1,
            watchlist_status="in_progress",
        )
        self._insert_progress("series-fr", "ep-1", episode_number=1, completion_ratio=1.0, last_watched_at="2026-03-10T01:00:00Z")
        self._insert_progress("series-fr", "ep-2", episode_number=2, completion_ratio=0.1, last_watched_at="2026-03-10T02:00:00Z")

        results = build_recommendations(self.config, limit=0)

        self.assertEqual([], results)

    def test_cross_provider_duplicate_new_season_recommendations_merge_by_mapped_mal_target(self) -> None:
        self._insert_series(
            "cr-s1",
            title="Shared Franchise",
            season_title="Shared Franchise (English Dub)",
            season_number=1,
            watchlist_status="fully_watched",
            provider="crunchyroll",
        )
        self._insert_progress(
            "cr-s1",
            "cr-s1-ep-1",
            episode_number=1,
            completion_ratio=1.0,
            last_watched_at="2026-03-01T01:00:00Z",
            provider="crunchyroll",
        )
        self._insert_series(
            "cr-s2",
            title="Shared Franchise",
            season_title="Shared Franchise Season 2 (English Dub)",
            season_number=2,
            watchlist_status="never_watched",
            provider="crunchyroll",
        )
        self._insert_series(
            "hd-s2",
            title="Shared Franchise",
            season_title="Shared Franchise Season 2 (English Dub)",
            season_number=2,
            watchlist_status="available",
            provider="hidive",
        )
        self._map_series("cr-s1", 6100, provider="crunchyroll")
        self._map_series("cr-s2", 6200, provider="crunchyroll")
        self._map_series("hd-s2", 6200, provider="hidive")

        results = [item for item in build_recommendations(self.config, limit=0) if item.kind == "new_season"]

        self.assertEqual(1, len(results))
        item = results[0]
        self.assertEqual("crunchyroll", item.provider)
        self.assertEqual("cr-s2", item.provider_series_id)
        self.assertTrue(item.context["cross_provider_merged"])
        self.assertEqual(["crunchyroll", "hidive"], item.context["available_via_providers"])
        self.assertEqual(["crunchyroll", "hidive"], item.available_providers())
        serialized = item.as_dict()
        self.assertEqual(["crunchyroll", "hidive"], serialized["providers"])
        self.assertEqual(2, serialized["provider_count"])
        self.assertTrue(serialized["multi_provider"])
        self.assertEqual("Crunchyroll + HIDIVE", serialized["provider_label"])
        self.assertEqual(3, item.context["availability_priority_bonus"])
        self.assertEqual(107, item.priority)
        self.assertIn("available via multiple providers: Crunchyroll + HIDIVE", item.reasons)
        self.assertEqual("hidive", item.context["alternate_provider_series"][0]["provider"])
        self.assertEqual("hd-s2", item.context["alternate_provider_series"][0]["provider_series_id"])

    def test_build_recommendations_does_not_merge_cross_provider_items_when_mapping_is_unapproved(self) -> None:
        self._insert_series(
            "base-show",
            title="Needs Review Show",
            season_title="Needs Review Show (English Dub)",
            season_number=1,
            watchlist_status="fully_watched",
            provider="crunchyroll",
        )
        self._insert_progress(
            "base-show",
            "base-show-1",
            episode_number=1,
            completion_ratio=1.0,
            last_watched_at="2026-03-10T01:00:00Z",
            provider="crunchyroll",
        )
        self._insert_series(
            "cr-next",
            title="Needs Review Show",
            season_title="Needs Review Show Season 2 (English Dub)",
            season_number=2,
            watchlist_status="available",
            provider="crunchyroll",
        )
        self._insert_series(
            "hd-next",
            title="Needs Review Show",
            season_title="Needs Review Show Season 2 (English Dub)",
            season_number=2,
            watchlist_status="available",
            provider="hidive",
        )

        self._map_series("cr-next", 9100, provider="crunchyroll")
        upsert_series_mapping(
            self.config.db_path,
            provider="hidive",
            provider_series_id="hd-next",
            mal_anime_id=9100,
            confidence=0.92,
            mapping_source="live_search",
            approved_by_user=False,
            notes=None,
        )

        results = [item for item in build_recommendations(self.config, limit=0) if item.kind == "new_season"]

        self.assertEqual(2, len(results))
        by_series_id = {item.provider_series_id: item for item in results}
        self.assertNotIn("available via multiple providers: Crunchyroll + HIDIVE", by_series_id["cr-next"].reasons)
        self.assertEqual("needs_review", by_series_id["hd-next"].context["cross_provider_reconciliation_status"])
        self.assertEqual("unapproved_mapping", by_series_id["hd-next"].context["reconciliation_blocked_reason"])
        self.assertNotIn("cross_provider_merged", by_series_id["cr-next"].context)

    def test_group_recommendations_splits_known_kinds_into_named_sections(self) -> None:
        grouped = group_recommendations(
            [
                Recommendation(
                    kind="resume_backlog",
                    priority=20,
                    provider="hidive",
                    provider_series_id="series-backlog",
                    title="Backlog Show",
                    season_title="Backlog Show (English Dub)",
                ),
                Recommendation(
                    kind="new_dubbed_episode",
                    priority=60,
                    provider="crunchyroll",
                    provider_series_id="series-fresh",
                    title="Fresh Show",
                    season_title="Fresh Show (English Dub)",
                ),
                Recommendation(
                    kind="new_season",
                    priority=100,
                    provider="crunchyroll",
                    provider_series_id="series-next",
                    title="Next Season Show",
                    season_title="Next Season Show Season 2 (English Dub)",
                ),
                Recommendation(
                    kind="discovery_candidate",
                    priority=50,
                    provider="mal",
                    provider_series_id="mal:300",
                    title="Discovery Hit",
                    season_title=None,
                ),
            ]
        )

        self.assertEqual(
            ["continue_next", "fresh_dubbed_episodes", "discovery_candidates", "resume_backlog"],
            [section["key"] for section in grouped],
        )
        self.assertEqual("series-next", grouped[0]["items"][0]["provider_series_id"])
        self.assertEqual(["crunchyroll"], grouped[0]["providers"])
        self.assertFalse(grouped[0]["mixed_providers"])
        self.assertEqual("series-fresh", grouped[1]["items"][0]["provider_series_id"])
        self.assertEqual("mal:300", grouped[2]["items"][0]["provider_series_id"])
        self.assertEqual(["mal"], grouped[2]["providers"])
        self.assertEqual("series-backlog", grouped[3]["items"][0]["provider_series_id"])
        self.assertEqual(["hidive"], grouped[3]["providers"])

    def test_group_recommendations_marks_mixed_provider_sections(self) -> None:
        grouped = group_recommendations(
            [
                Recommendation(
                    kind="new_season",
                    priority=100,
                    provider="crunchyroll",
                    provider_series_id="cr-next",
                    title="Next Season Show",
                    season_title="Next Season Show Season 2 (English Dub)",
                ),
                Recommendation(
                    kind="new_season",
                    priority=95,
                    provider="hidive",
                    provider_series_id="hd-next",
                    title="Another Season Show",
                    season_title="Another Season Show Season 2 (English Dub)",
                ),
            ]
        )

        self.assertEqual(1, len(grouped))
        self.assertEqual("continue_next", grouped[0]["key"])
        self.assertTrue(grouped[0]["mixed_providers"])
        self.assertEqual(["crunchyroll", "hidive"], grouped[0]["providers"])
        self.assertEqual({"crunchyroll": 1, "hidive": 1}, grouped[0]["provider_counts"])
        self.assertEqual("Crunchyroll + HIDIVE", grouped[0]["provider_label"])
        self.assertEqual(0, grouped[0]["multi_provider_item_count"])
        self.assertIn("multiple providers", grouped[0]["mixed_provider_note"])

    def test_group_recommendations_marks_merged_cross_provider_items_as_mixed_sections(self) -> None:
        grouped = group_recommendations(
            [
                Recommendation(
                    kind="new_season",
                    priority=100,
                    provider="crunchyroll",
                    provider_series_id="cr-next",
                    title="Next Season Show",
                    season_title="Next Season Show Season 2 (English Dub)",
                    context={
                        "cross_provider_merged": True,
                        "available_via_providers": ["crunchyroll", "hidive"],
                    },
                )
            ]
        )

        self.assertEqual(1, len(grouped))
        self.assertEqual("continue_next", grouped[0]["key"])
        self.assertTrue(grouped[0]["mixed_providers"])
        self.assertEqual(["crunchyroll", "hidive"], grouped[0]["providers"])
        self.assertEqual({"crunchyroll": 1, "hidive": 1}, grouped[0]["provider_counts"])
        self.assertEqual("Crunchyroll + HIDIVE", grouped[0]["provider_label"])
        self.assertEqual(1, grouped[0]["multi_provider_item_count"])
        self.assertEqual(["crunchyroll", "hidive"], grouped[0]["items"][0]["providers"])
        self.assertTrue(grouped[0]["items"][0]["multi_provider"])

    def test_group_recommendations_prefers_broader_availability_when_priorities_tie(self) -> None:
        grouped = group_recommendations(
            [
                Recommendation(
                    kind="new_season",
                    priority=100,
                    provider="hidive",
                    provider_series_id="hd-only",
                    title="Shared Priority",
                    season_title="Shared Priority Season 2",
                ),
                Recommendation(
                    kind="new_season",
                    priority=100,
                    provider="crunchyroll",
                    provider_series_id="cr-multi",
                    title="Shared Priority",
                    season_title="Shared Priority Season 2",
                    context={"available_via_providers": ["crunchyroll", "hidive"]},
                ),
            ]
        )

        self.assertEqual("cr-multi", grouped[0]["items"][0]["provider_series_id"])
        self.assertEqual("hd-only", grouped[0]["items"][1]["provider_series_id"])

    def test_build_recommendations_multi_provider_new_season_gets_small_raw_priority_bonus(self) -> None:
        self._insert_series(
            "base-show",
            title="Bonus Show",
            season_title="Bonus Show (English Dub)",
            season_number=1,
            watchlist_status="fully_watched",
            provider="crunchyroll",
        )
        self._insert_progress(
            "base-show",
            "base-show-1",
            episode_number=1,
            completion_ratio=1.0,
            last_watched_at="2026-03-12T01:00:00Z",
            provider="crunchyroll",
        )
        self._insert_series(
            "cr-next",
            title="Bonus Show",
            season_title="Bonus Show Season 2 (English Dub)",
            season_number=2,
            watchlist_status="never_watched",
            provider="crunchyroll",
        )
        self._insert_series(
            "hd-next",
            title="Bonus Show",
            season_title="Bonus Show Season 2 (English Dub)",
            season_number=2,
            watchlist_status="available",
            provider="hidive",
        )
        self._insert_series(
            "single-base",
            title="Single Provider Rival",
            season_title="Single Provider Rival (English Dub)",
            season_number=1,
            watchlist_status="fully_watched",
            provider="hidive",
        )
        self._insert_progress(
            "single-base",
            "single-base-1",
            episode_number=1,
            completion_ratio=1.0,
            last_watched_at="2026-03-11T01:00:00Z",
            provider="hidive",
        )
        self._insert_series(
            "single-next",
            title="Single Provider Rival",
            season_title="Single Provider Rival Season 2 (English Dub)",
            season_number=2,
            watchlist_status="never_watched",
            provider="hidive",
        )

        self._map_series("cr-next", 8200, provider="crunchyroll")
        self._map_series("hd-next", 8200, provider="hidive")

        results = [item for item in build_recommendations(self.config, limit=0) if item.kind == "new_season"]

        self.assertEqual(["cr-next", "single-next"], [item.provider_series_id for item in results[:2]])
        self.assertEqual(107, results[0].priority)
        self.assertEqual(104, results[1].priority)
        self.assertEqual(3, results[0].context["availability_priority_bonus"])

    def test_build_recommendations_multi_provider_new_episode_gets_small_raw_priority_bonus(self) -> None:
        recent = (datetime.now(timezone.utc) - timedelta(days=1)).replace(microsecond=0).isoformat().replace("+00:00", "Z")
        self._insert_series(
            "cr-fresh",
            title="Episode Bonus",
            season_title="Episode Bonus (English Dub)",
            watchlist_status="in_progress",
            provider="crunchyroll",
        )
        self._insert_progress(
            "cr-fresh",
            "cr-fresh-1",
            episode_number=1,
            completion_ratio=1.0,
            last_watched_at=recent,
            provider="crunchyroll",
        )
        self._insert_progress(
            "cr-fresh",
            "cr-fresh-2",
            episode_number=2,
            completion_ratio=0.0,
            last_watched_at=recent,
            provider="crunchyroll",
        )
        self._insert_series(
            "hd-fresh",
            title="Episode Bonus",
            season_title="Episode Bonus (English Dub)",
            watchlist_status="in_progress",
            provider="hidive",
        )
        self._insert_progress(
            "hd-fresh",
            "hd-fresh-1",
            episode_number=1,
            completion_ratio=1.0,
            last_watched_at=recent,
            provider="hidive",
        )
        self._insert_progress(
            "hd-fresh",
            "hd-fresh-2",
            episode_number=2,
            completion_ratio=0.0,
            last_watched_at=recent,
            provider="hidive",
        )
        self._insert_series(
            "single-fresh",
            title="Episode Single Rival",
            season_title="Episode Single Rival (English Dub)",
            watchlist_status="in_progress",
            provider="hidive",
        )
        self._insert_progress(
            "single-fresh",
            "single-fresh-1",
            episode_number=1,
            completion_ratio=1.0,
            last_watched_at=recent,
            provider="hidive",
        )
        self._insert_progress(
            "single-fresh",
            "single-fresh-2",
            episode_number=2,
            completion_ratio=0.0,
            last_watched_at=recent,
            provider="hidive",
        )

        self._map_series("cr-fresh", 8300, provider="crunchyroll")
        self._map_series("hd-fresh", 8300, provider="hidive")

        results = [item for item in build_recommendations(self.config, limit=0) if item.kind == "new_dubbed_episode"]

        self.assertEqual(["cr-fresh", "single-fresh"], [item.provider_series_id for item in results[:2]])
        self.assertEqual(3, results[0].context["availability_priority_bonus"])
        self.assertEqual(results[1].priority + 3, results[0].priority)

    def test_build_recommendations_limit_prefers_broader_availability_when_priorities_tie(self) -> None:
        self._insert_series(
            "base-show",
            title="Shared Priority",
            season_title="Shared Priority (English Dub)",
            season_number=1,
            watchlist_status="fully_watched",
            provider="crunchyroll",
        )
        self._insert_progress(
            "base-show",
            "base-show-1",
            episode_number=1,
            completion_ratio=1.0,
            last_watched_at="2026-03-12T01:00:00Z",
            provider="crunchyroll",
        )

        self._insert_series(
            "cr-next",
            title="Shared Priority",
            season_title="Shared Priority Season 2 (English Dub)",
            season_number=2,
            watchlist_status="never_watched",
            provider="crunchyroll",
        )
        self._insert_series(
            "hd-next",
            title="Shared Priority",
            season_title="Shared Priority Season 2 (English Dub)",
            season_number=2,
            watchlist_status="available",
            provider="hidive",
        )
        self._insert_series(
            "hd-other",
            title="Shared Priority Rival",
            season_title="Shared Priority Rival Season 2 (English Dub)",
            season_number=2,
            watchlist_status="available",
            provider="hidive",
        )
        self._insert_series(
            "hd-other-base",
            title="Shared Priority Rival",
            season_title="Shared Priority Rival (English Dub)",
            season_number=1,
            watchlist_status="fully_watched",
            provider="hidive",
        )
        self._insert_progress(
            "hd-other-base",
            "hd-other-base-1",
            episode_number=1,
            completion_ratio=1.0,
            last_watched_at="2026-03-11T01:00:00Z",
            provider="hidive",
        )

        self._map_series("cr-next", 6200, provider="crunchyroll")
        self._map_series("hd-next", 6200, provider="hidive")

        results = build_recommendations(self.config, limit=1)

        self.assertEqual(1, len(results))
        self.assertEqual("cr-next", results[0].provider_series_id)
        self.assertEqual(["crunchyroll", "hidive"], results[0].available_providers())

    def test_recommend_cli_limit_prefers_broader_availability_when_priorities_tie(self) -> None:
        self._insert_series(
            "base-show",
            title="CLI Shared Priority",
            season_title="CLI Shared Priority (English Dub)",
            season_number=1,
            watchlist_status="fully_watched",
            provider="crunchyroll",
        )
        self._insert_progress(
            "base-show",
            "base-show-1",
            episode_number=1,
            completion_ratio=1.0,
            last_watched_at="2026-03-12T01:00:00Z",
            provider="crunchyroll",
        )
        self._insert_series(
            "cr-next",
            title="CLI Shared Priority",
            season_title="CLI Shared Priority Season 2 (English Dub)",
            season_number=2,
            watchlist_status="never_watched",
            provider="crunchyroll",
        )
        self._insert_series(
            "hd-next",
            title="CLI Shared Priority",
            season_title="CLI Shared Priority Season 2 (English Dub)",
            season_number=2,
            watchlist_status="available",
            provider="hidive",
        )
        self._insert_series(
            "hd-other",
            title="CLI Shared Priority Rival",
            season_title="CLI Shared Priority Rival Season 2 (English Dub)",
            season_number=2,
            watchlist_status="available",
            provider="hidive",
        )
        self._insert_series(
            "hd-other-base",
            title="CLI Shared Priority Rival",
            season_title="CLI Shared Priority Rival (English Dub)",
            season_number=1,
            watchlist_status="fully_watched",
            provider="hidive",
        )
        self._insert_progress(
            "hd-other-base",
            "hd-other-base-1",
            episode_number=1,
            completion_ratio=1.0,
            last_watched_at="2026-03-11T01:00:00Z",
            provider="hidive",
        )

        self._map_series("cr-next", 7200, provider="crunchyroll")
        self._map_series("hd-next", 7200, provider="hidive")

        argv = [
            "mal-updater",
            "--project-root",
            str(self.project_root),
            "recommend",
            "--limit",
            "1",
            "--flat",
        ]
        with patch("sys.argv", argv), patch("sys.stdout", new_callable=io.StringIO) as stdout:
            exit_code = cli_main()

        self.assertEqual(0, exit_code)
        payload = json.loads(stdout.getvalue())
        self.assertEqual(1, len(payload))
        self.assertEqual("cr-next", payload[0]["provider_series_id"])
        self.assertEqual(["crunchyroll", "hidive"], payload[0]["providers"])

    def test_trim_grouped_recommendations_preserves_section_visibility_under_global_limit(self) -> None:
        sections = [
            {
                "key": "continue_next",
                "count": 2,
                "items": [{"provider_series_id": "c1"}, {"provider_series_id": "c2"}],
            },
            {
                "key": "discovery_candidates",
                "count": 2,
                "items": [{"provider_series_id": "d1"}, {"provider_series_id": "d2"}],
            },
            {
                "key": "resume_backlog",
                "count": 3,
                "items": [{"provider_series_id": "r1"}, {"provider_series_id": "r2"}, {"provider_series_id": "r3"}],
            },
        ]

        trimmed = trim_grouped_recommendations(sections, 4)

        self.assertEqual(["continue_next", "discovery_candidates", "resume_backlog"], [section["key"] for section in trimmed])
        self.assertEqual([2, 1, 1], [section["count"] for section in trimmed])
        self.assertEqual("c2", trimmed[0]["items"][1]["provider_series_id"])
        self.assertEqual(2, trimmed[1]["total_count"])
        self.assertEqual(3, trimmed[2]["total_count"])

    def test_group_recommendations_keeps_unknown_kinds_under_other(self) -> None:
        grouped = group_recommendations(
            [
                Recommendation(
                    kind="mystery_lane",
                    priority=10,
                    provider_series_id="m1",
                    title="Mystery Show",
                    season_title=None,
                )
            ]
        )

        self.assertEqual(1, len(grouped))
        self.assertEqual("other", grouped[0]["key"])
        self.assertEqual(["mystery_lane"], grouped[0]["kinds"])
        self.assertEqual("m1", grouped[0]["items"][0]["provider_series_id"])

    def test_recommend_cli_defaults_to_grouped_output(self) -> None:
        self._insert_series(
            "series-next",
            title="Next Season Show",
            season_title="Next Season Show Season 2 (English Dub)",
            season_number=2,
            watchlist_status="available",
        )
        self._insert_series(
            "series-base",
            title="Next Season Show",
            season_title="Next Season Show (English Dub)",
            season_number=1,
            watchlist_status="fully_watched",
        )
        self._insert_progress(
            "series-base",
            "series-base-1",
            episode_number=1,
            completion_ratio=1.0,
            last_watched_at="2026-03-10T01:00:00Z",
        )

        argv = [
            "mal-updater",
            "--project-root",
            str(self.project_root),
            "recommend",
            "--limit",
            "1",
        ]
        with patch("sys.argv", argv), patch("sys.stdout", new_callable=io.StringIO) as stdout:
            exit_code = cli_main()

        self.assertEqual(0, exit_code)
        payload = json.loads(stdout.getvalue())
        self.assertEqual(["continue_next"], [section["key"] for section in payload])
        self.assertEqual("series-next", payload[0]["items"][0]["provider_series_id"])

    def test_recommend_coverage_reports_fresh_stale_and_unharvested_sources(self) -> None:
        self._insert_series("fresh-seed", title="Fresh Seed", watchlist_status="fully_watched")
        self._insert_progress("fresh-seed", "fresh-seed-1", episode_number=1, completion_ratio=1.0, last_watched_at="2026-03-01T01:00:00Z")
        self._map_series("fresh-seed", 100)
        self._cache_recommendations(
            100,
            [{"target_mal_anime_id": 900, "target_title": "Target", "num_recommendations": 3, "raw": {}}],
            make_targets_available=False,
        )

        self._insert_series("stale-seed", title="Stale Seed", watchlist_status="fully_watched")
        self._map_series("stale-seed", 200)
        self._cache_recommendations(
            200,
            [{"target_mal_anime_id": 901, "target_title": "Old Target", "num_recommendations": 1, "raw": {}}],
            make_targets_available=False,
        )
        with connect(self.config.db_path) as conn:
            conn.execute("UPDATE mal_recommendation_harvest_status SET fetched_at = '2000-01-01 00:00:00' WHERE source_mal_anime_id = 200")
            conn.execute("UPDATE mal_anime_recommendations SET fetched_at = '2000-01-01 00:00:00' WHERE source_mal_anime_id = 200")
            conn.commit()

        self._insert_series("missing-seed", title="Missing Seed", watchlist_status="available")
        self._map_series("missing-seed", 300)

        argv = [
            "mal-updater",
            "--project-root",
            str(self.project_root),
            "recommend-coverage",
            "--stale-after-days",
            "14",
        ]
        with patch("sys.argv", argv), patch("sys.stdout", new_callable=io.StringIO) as stdout:
            exit_code = cli_main()

        self.assertEqual(0, exit_code)
        payload = json.loads(stdout.getvalue())
        self.assertEqual(
            {"mapped_sources": 3, "watched_sources": 3, "fresh": 1, "stale": 1, "unharvested": 1, "total_edges": 2, "fresh_coverage_ratio": 1 / 3},
            payload["summary"],
        )
        statuses = {source["mal_anime_id"]: source["status"] for source in payload["sources"]}
        self.assertEqual({100: "fresh", 200: "stale", 300: "unharvested"}, statuses)

    def test_recommend_coverage_counts_empty_harvest_as_fresh(self) -> None:
        self._insert_series("empty-seed", title="Empty Seed", watchlist_status="fully_watched")
        self._map_series("empty-seed", 400)
        self._cache_recommendations(400, [])

        argv = ["mal-updater", "--project-root", str(self.project_root), "recommend-coverage"]
        with patch("sys.argv", argv), patch("sys.stdout", new_callable=io.StringIO) as stdout:
            exit_code = cli_main()

        self.assertEqual(0, exit_code)
        payload = json.loads(stdout.getvalue())
        self.assertEqual(1, payload["summary"]["fresh"])
        self.assertEqual(0, payload["sources"][0]["edge_count"])
        self.assertEqual("fresh", payload["sources"][0]["status"])

    def test_recommend_cli_grouped_limit_does_not_let_backlog_starve_discovery(self) -> None:
        for index in range(5):
            provider_series_id = f"seed-{index}"
            self._insert_series(
                provider_series_id,
                title=f"Discovery Seed {index}",
                season_title=f"Discovery Seed {index} (English Dub)",
                watchlist_status="fully_watched",
            )
            self._insert_progress(
                provider_series_id,
                f"{provider_series_id}-1",
                episode_number=1,
                completion_ratio=1.0,
                last_watched_at=f"2026-03-{index + 1:02d}T01:00:00Z",
            )
            self._map_series(provider_series_id, 100 + index)
            self._cache_metadata(100 + index, title=f"Discovery Seed {index}")
            self._cache_recommendations(
                100 + index,
                [{"target_mal_anime_id": 9000, "target_title": "Discovery Winner", "num_recommendations": 30 - index, "raw": {}}],
                make_targets_available=False,
            )

        self._cache_metadata(9000, title="Discovery Winner")
        self._insert_series(
            "discovery-winner",
            title="Discovery Winner",
            season_title="Discovery Winner (English Dub)",
            watchlist_status="available",
        )
        self._map_series("discovery-winner", 9000)

        for index in range(4):
            backlog_series_id = f"backlog-{index}"
            self._insert_series(
                backlog_series_id,
                title=f"Backlog Show {index}",
                season_title=f"Backlog Show {index} (English Dub)",
                watchlist_status="available",
            )
            recent = (datetime.now(timezone.utc) - timedelta(days=10 + index)).replace(microsecond=0).isoformat().replace("+00:00", "Z")
            self._insert_progress(backlog_series_id, f"{backlog_series_id}-1", episode_number=1, completion_ratio=1.0, last_watched_at=recent)
            self._insert_progress(backlog_series_id, f"{backlog_series_id}-2", episode_number=2, completion_ratio=0.0, last_watched_at=recent)

        argv = [
            "mal-updater",
            "--project-root",
            str(self.project_root),
            "recommend",
            "--limit",
            "2",
            "--include-dormant",
        ]
        with patch("sys.argv", argv), patch("sys.stdout", new_callable=io.StringIO) as stdout:
            exit_code = cli_main()

        self.assertEqual(0, exit_code)
        payload = json.loads(stdout.getvalue())
        self.assertLessEqual(len(payload), 2)
        self.assertIn("discovery_candidates", [section["key"] for section in payload])
        discovery_section = next(section for section in payload if section["key"] == "discovery_candidates")
        self.assertEqual("discovery-winner", discovery_section["items"][0]["provider_series_id"])

    def test_recommend_cli_excludes_mal_only_discovery_candidates_by_default(self) -> None:
        self._insert_series(
            "seed-a",
            title="Seed A",
            season_title="Seed A (English Dub)",
            watchlist_status="fully_watched",
        )
        self._insert_progress("seed-a", "seed-a-1", episode_number=1, completion_ratio=1.0, last_watched_at="2026-03-01T01:00:00Z")
        self._map_series("seed-a", 100)
        self._cache_metadata(100, title="Seed A")
        self._cache_metadata(300, title="Dormant Candidate")
        self._cache_recommendations(100, [
            {"target_mal_anime_id": 300, "target_title": "Dormant Candidate", "num_recommendations": 20, "raw": {}},
        ], make_targets_available=False)

        argv = ["mal-updater", "--project-root", str(self.project_root), "recommend", "--limit", "0", "--flat"]
        with patch("sys.argv", argv), patch("sys.stdout", new_callable=io.StringIO) as stdout:
            exit_code = cli_main()

        self.assertEqual(0, exit_code)
        payload = json.loads(stdout.getvalue())
        self.assertEqual([], payload)

    def test_recommend_cli_surfaces_provider_available_non_english_dub_discovery_as_unknown(self) -> None:
        self._insert_series("seed-a", title="Seed A", season_title="Seed A (English Dub)", watchlist_status="fully_watched")
        self._insert_progress("seed-a", "seed-a-1", episode_number=1, completion_ratio=1.0, last_watched_at="2026-03-01T01:00:00Z")
        self._map_series("seed-a", 100)
        self._cache_metadata(100, title="Seed A")
        self._cache_metadata(301, title="Sub Only Candidate")
        self._cache_recommendations(100, [{"target_mal_anime_id": 301, "target_title": "Sub Only Candidate", "num_recommendations": 20, "raw": {}}], make_targets_available=False)
        self._insert_series("sub-only", title="Sub Only Candidate", season_title="Sub Only Candidate", watchlist_status="available")

        argv = ["mal-updater", "--project-root", str(self.project_root), "recommend", "--limit", "0", "--flat", "--include-dormant"]
        with patch("sys.argv", argv), patch("sys.stdout", new_callable=io.StringIO) as stdout:
            exit_code = cli_main()

        self.assertEqual(0, exit_code)
        payload = json.loads(stdout.getvalue())
        self.assertEqual(1, len(payload))
        self.assertEqual("Sub Only Candidate", payload[0]["title"])
        self.assertEqual(["crunchyroll"], payload[0]["context"]["available_via_providers"])
        self.assertEqual("unknown", payload[0]["context"]["english_dub_signal"])
        self.assertFalse(payload[0]["context"].get("english_dub", False))

    def test_recommend_cli_diagnostic_title_marker_discovery_preserves_metadata_without_dub_proof(self) -> None:
        self._insert_series("seed-a", title="Seed A", season_title="Seed A (English Dub)", watchlist_status="fully_watched")
        self._insert_progress("seed-a", "seed-a-1", episode_number=1, completion_ratio=1.0, last_watched_at="2026-03-01T01:00:00Z")
        self._map_series("seed-a", 100)
        self._cache_metadata(100, title="Seed A", genres=["Action"])
        self._cache_metadata(302, title="Romaji Candidate", title_english="English Candidate", genres=["Comedy", "Fantasy"])
        self._cache_recommendations(100, [{"target_mal_anime_id": 302, "target_title": "Romaji Candidate", "num_recommendations": 20, "raw": {}}], make_targets_available=False)
        self._insert_series("dubbed", title="Romaji Candidate", season_title="Romaji Candidate (English Dub)", watchlist_status="available")

        argv = ["mal-updater", "--project-root", str(self.project_root), "recommend", "--limit", "0", "--flat", "--include-dormant"]
        with patch("sys.argv", argv), patch("sys.stdout", new_callable=io.StringIO) as stdout:
            exit_code = cli_main()

        self.assertEqual(0, exit_code)
        payload = json.loads(stdout.getvalue())
        self.assertEqual(1, len(payload))
        self.assertEqual("dubbed", payload[0]["provider_series_id"])
        self.assertEqual("English Candidate", payload[0]["context"]["english_title"])
        self.assertEqual(["Comedy", "Fantasy"], payload[0]["context"]["genres"])
        self.assertEqual("unknown", payload[0]["scorecard"]["features"]["english_dub_signal"])

    def test_recommend_cli_flat_flag_preserves_legacy_list_output(self) -> None:
        self._insert_series(
            "series-fresh",
            title="Fresh Show",
            season_title="Fresh Show (English Dub)",
            watchlist_status="available",
        )
        recent = (datetime.now(timezone.utc) - timedelta(days=2)).replace(microsecond=0)
        self._insert_progress(
            "series-fresh",
            "series-fresh-1",
            episode_number=1,
            completion_ratio=1.0,
            last_watched_at=recent.isoformat().replace("+00:00", "Z"),
        )
        self._insert_progress(
            "series-fresh",
            "series-fresh-2",
            episode_number=2,
            completion_ratio=0.0,
            last_watched_at=(recent + timedelta(hours=1)).isoformat().replace("+00:00", "Z"),
        )

        argv = [
            "mal-updater",
            "--project-root",
            str(self.project_root),
            "recommend",
            "--limit",
            "1",
            "--flat",
        ]
        with patch("sys.argv", argv), patch("sys.stdout", new_callable=io.StringIO) as stdout:
            exit_code = cli_main()

        self.assertEqual(0, exit_code)
        payload = json.loads(stdout.getvalue())
        self.assertIsInstance(payload, list)
        self.assertEqual("new_dubbed_episode", payload[0]["kind"])
        self.assertEqual("series-fresh", payload[0]["provider_series_id"])


if __name__ == "__main__":
    unittest.main()
