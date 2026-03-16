from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from mal_updater.config import load_config
from mal_updater.db import (
    bootstrap_database,
    connect,
    replace_mal_anime_relations,
    replace_mal_recommendation_edges,
    upsert_mal_anime_metadata,
    upsert_series_mapping,
)
from mal_updater.recommendations import build_recommendations


class RecommendationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.project_root = Path(self.temp_dir.name)
        (self.project_root / "config").mkdir(parents=True, exist_ok=True)
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
    ) -> None:
        with connect(self.config.db_path) as conn:
            conn.execute(
                """
                INSERT INTO provider_series (provider, provider_series_id, title, season_title, season_number, raw_json)
                VALUES ('crunchyroll', ?, ?, ?, ?, '{}')
                """,
                (provider_series_id, title, season_title, season_number),
            )
            if watchlist_status is not None:
                conn.execute(
                    """
                    INSERT INTO provider_watchlist (provider, provider_series_id, status, raw_json)
                    VALUES ('crunchyroll', ?, ?, '{}')
                    """,
                    (provider_series_id, watchlist_status),
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
                ) VALUES ('crunchyroll', ?, ?, ?, ?, ?, '{}')
                """,
                (provider_episode_id, provider_series_id, episode_number, completion_ratio, last_watched_at),
            )
            conn.commit()

    def _map_series(self, provider_series_id: str, mal_anime_id: int) -> None:
        upsert_series_mapping(
            self.config.db_path,
            provider="crunchyroll",
            provider_series_id=provider_series_id,
            mal_anime_id=mal_anime_id,
            confidence=1.0,
            mapping_source="user_approved",
            approved_by_user=True,
            notes=None,
        )

    def _cache_metadata(self, mal_anime_id: int, *, title: str) -> None:
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
            mean=None,
            popularity=None,
            start_season=None,
            raw={"id": mal_anime_id, "title": title},
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
        self._insert_progress("series-1", "ep-1", episode_number=1, completion_ratio=1.0, last_watched_at="2026-03-10T01:00:00Z")
        self._insert_progress("series-1", "ep-2", episode_number=2, completion_ratio=1.0, last_watched_at="2026-03-10T02:00:00Z")
        self._insert_progress("series-1", "ep-3", episode_number=3, completion_ratio=0.2, last_watched_at="2026-03-11T02:00:00Z")

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
        self._insert_series(
            "recent-show",
            title="Recent Show",
            season_title="Recent Show (English Dub)",
            watchlist_status="in_progress",
        )
        self._insert_progress("recent-show", "r1", episode_number=1, completion_ratio=1.0, last_watched_at="2026-03-14T01:00:00Z")
        self._insert_progress("recent-show", "r2", episode_number=2, completion_ratio=0.2, last_watched_at="2026-03-14T02:00:00Z")

        self._insert_series(
            "stale-show",
            title="Stale Show",
            season_title="Stale Show (English Dub)",
            watchlist_status="in_progress",
        )
        self._insert_progress("stale-show", "s1", episode_number=1, completion_ratio=1.0, last_watched_at="2020-03-10T01:00:00Z")
        self._insert_progress("stale-show", "s2", episode_number=2, completion_ratio=0.2, last_watched_at="2020-03-10T02:00:00Z")

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

    def test_stale_tail_gap_is_classified_as_resume_backlog(self) -> None:
        self._insert_series(
            "backlog-show",
            title="Backlog Show",
            season_title="Backlog Show (English Dub)",
            season_number=1,
            watchlist_status="in_progress",
        )
        self._insert_progress("backlog-show", "b1", episode_number=1, completion_ratio=1.0, last_watched_at="2020-01-10T01:00:00Z")
        self._insert_progress("backlog-show", "b2", episode_number=2, completion_ratio=0.2, last_watched_at="2020-01-10T02:00:00Z")

        results = build_recommendations(self.config, limit=0)

        self.assertEqual(1, len(results))
        item = results[0]
        self.assertEqual("resume_backlog", item.kind)
        self.assertIn("backlog continuation", " ".join(item.reasons))
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


if __name__ == "__main__":
    unittest.main()
