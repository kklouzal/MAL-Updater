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
    replace_mal_anime_relations,
    replace_mal_recommendation_edges,
    upsert_mal_anime_metadata,
    upsert_series_mapping,
)
from mal_updater.recommendation_metadata import refresh_recommendation_metadata
from mal_updater.recommendations import Recommendation, build_recommendations, group_recommendations


class RecommendationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.project_root = Path(self.temp_dir.name)
        (self.project_root / ".MAL-Updater" / "config").mkdir(parents=True, exist_ok=True)
        self.config = load_config(self.project_root)
        bootstrap_database(self.config.db_path)

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

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
                INSERT INTO provider_series (provider, provider_series_id, title, season_title, season_number, raw_json)
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
        mean: float | None = None,
        popularity: int | None = None,
        genres: list[str] | None = None,
        studios: list[str] | None = None,
        source: str | None = None,
        start_season: dict | None = None,
        my_list_status: dict | None = None,
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
            title_english=None,
            title_japanese=None,
            alternative_titles=[],
            media_type="tv",
            status="finished_airing",
            num_episodes=12,
            mean=mean,
            popularity=popularity,
            start_season=start_season,
            raw=raw,
        )

    def _cache_relations(self, mal_anime_id: int, relations: list[dict]) -> None:
        replace_mal_anime_relations(self.config.db_path, mal_anime_id=mal_anime_id, relations=relations)

    def _cache_recommendations(self, mal_anime_id: int, edges: list[dict]) -> None:
        replace_mal_recommendation_edges(self.config.db_path, source_mal_anime_id=mal_anime_id, hop_distance=1, edges=edges)

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

        results = build_recommendations(self.config, limit=0)

        self.assertEqual(1, len(results))
        item = results[0]
        self.assertEqual("new_dubbed_episode", item.kind)
        self.assertEqual("series-1", item.provider_series_id)
        self.assertEqual(1, item.context["contiguous_tail_gap"])

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
        self._cache_metadata(5100, title="HIDIVE Show")
        self._cache_metadata(5200, title="HIDIVE Show Season 2")
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

        discovery = [item for item in results if item.kind == "discovery_candidate" and item.provider_series_id == "mal:7200"]
        self.assertEqual(1, len(discovery))
        self.assertEqual("mal", discovery[0].provider)
        self.assertEqual([7100], discovery[0].context["supporting_mal_anime_ids"])

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
        self.assertEqual("mal:300", item.provider_series_id)
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

        self.assertEqual(["mal:300", "mal:400"], [item.provider_series_id for item in results])
        self.assertEqual(10, results[0].context["cross_seed_support_votes"])
        self.assertEqual(0, results[1].context["cross_seed_support_votes"])
        self.assertGreater(results[0].context["support_balance_bonus"], results[1].context["support_balance_bonus"])
        self.assertGreater(results[0].priority, results[1].priority)

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

        self.assertEqual(["mal:300", "mal:400"], [item.provider_series_id for item in results])
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

        self.assertEqual(["mal:300", "mal:400"], [item.provider_series_id for item in results])
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

        self.assertEqual(["mal:300", "mal:400"], [item.provider_series_id for item in results])
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

        self.assertEqual(["mal:300", "mal:400"], [item.provider_series_id for item in results])
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

        self.assertEqual(["mal:400", "mal:500"], [item.provider_series_id for item in results])
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

        self.assertEqual(["mal:300", "mal:400"], [item.provider_series_id for item in results])
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

        self.assertEqual(["mal:300", "mal:400"], [item.provider_series_id for item in results])
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

        self.assertEqual(["mal:300", "mal:400"], [item.provider_series_id for item in results])
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

        self.assertEqual(["mal:300"], [item.provider_series_id for item in results])
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

        self.assertEqual(["mal:300"], [item.provider_series_id for item in results])
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

        self.assertEqual({"mal:300", "mal:400"}, {item.provider_series_id for item in results})
        neutral_heavy = next(item for item in results if item.provider_series_id == "mal:400")
        clean_support = next(item for item in results if item.provider_series_id == "mal:300")
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

        self.assertEqual(["mal:400", "mal:500"], [item.provider_series_id for item in results])
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

        self.assertEqual(["mal:300", "mal:400"], [item.provider_series_id for item in results])
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

        self.assertEqual(["mal:300", "mal:400"], [item.provider_series_id for item in results])
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

        self.assertEqual(["mal:300", "mal:400"], [item.provider_series_id for item in results])
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

        self.assertEqual(["mal:300", "mal:400"], [item.provider_series_id for item in results])
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

        self.assertEqual(["mal:300", "mal:400"], [item.provider_series_id for item in results])
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

        self.assertEqual(["mal:300", "mal:400"], [item.provider_series_id for item in results])
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

        self.assertEqual(["mal:300", "mal:400"], [item.provider_series_id for item in results])
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

        self.assertEqual(["mal:300", "mal:400"], [item.provider_series_id for item in results])
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

        self.assertEqual(["mal:300"], [item.provider_series_id for item in results])
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

        self.assertEqual(["mal:300", "mal:400"], [item.provider_series_id for item in results])
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

        self.assertEqual(["mal:300", "mal:400"], [item.provider_series_id for item in results])
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

        self.assertEqual(["mal:300", "mal:400"], [item.provider_series_id for item in results])
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

        self.assertEqual(["mal:300", "mal:400"], [item.provider_series_id for item in results])
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

        self.assertEqual(["mal:400"], [item.provider_series_id for item in results])

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

        self.assertEqual(["mal:400"], [item.provider_series_id for item in results])

    def test_discovery_candidate_skips_titles_already_present_in_provider_catalog(self) -> None:
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
        ])

        results = [item for item in build_recommendations(self.config, limit=0) if item.kind == "discovery_candidate"]

        self.assertEqual([], results)

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
        discovery = [item for item in results if item.kind == "discovery_candidate" and item.provider_series_id == "mal:300"]
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
