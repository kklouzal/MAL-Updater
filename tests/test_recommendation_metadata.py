from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from mal_updater.config import load_config
from mal_updater.db import (
    bootstrap_database,
    connect,
    get_mal_anime_metadata_map,
    get_mal_recommendation_edges_map,
    list_mal_user_anime_list_cache,
    replace_mal_recommendation_edges,
    replace_mal_user_anime_list_cache_generation,
    upsert_mal_anime_metadata,
    upsert_series_mapping,
)
from mal_updater.mal_client import MalApiError
from mal_updater.recommendation_metadata import (
    CHARACTER_VOICE_ACTOR_CAPABILITY_NOTE,
    DETAIL_FIELDS,
    refresh_mal_user_anime_list_cache,
    refresh_recommendation_metadata,
)


class RecommendationMetadataRefreshTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.project_root = Path(self.temp_dir.name)
        (self.project_root / ".MAL-Updater" / "config").mkdir(parents=True, exist_ok=True)
        self.config = load_config(self.project_root)
        bootstrap_database(self.config.db_path)

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def _insert_series(self, provider_series_id: str, *, title: str, watchlist_status: str | None = "fully_watched") -> None:
        with connect(self.config.db_path) as conn:
            conn.execute(
                """
                INSERT INTO provider_series (provider, provider_series_id, title, raw_json)
                VALUES ('crunchyroll', ?, ?, '{}')
                """,
                (provider_series_id, title),
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

    def _cache_metadata(self, mal_anime_id: int, *, title: str, raw: dict | None = None, fetched_at: str | None = None) -> None:
        payload = raw or {"id": mal_anime_id, "title": title}
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
            mean=8.0,
            popularity=100,
            start_season={"year": 2020, "season": "spring"},
            raw=payload,
        )
        if fetched_at is not None:
            with connect(self.config.db_path) as conn:
                conn.execute(
                    "UPDATE mal_anime_metadata SET fetched_at = ?, updated_at = ? WHERE mal_anime_id = ?",
                    (fetched_at, fetched_at, mal_anime_id),
                )
                conn.commit()

    def _cache_harvest_status(
        self,
        mal_anime_id: int,
        *,
        status: str = "fetched",
        fetched_at: str = "2999-01-01 00:00:00",
        num_edges: int = 0,
    ) -> None:
        with connect(self.config.db_path) as conn:
            conn.execute(
                """
                INSERT INTO mal_recommendation_harvest_status (source_mal_anime_id, status, num_edges, fetched_at)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(source_mal_anime_id) DO UPDATE SET
                    status = excluded.status,
                    num_edges = excluded.num_edges,
                    fetched_at = excluded.fetched_at
                """,
                (mal_anime_id, status, num_edges, fetched_at),
            )
            conn.commit()

    def _harvest_status(self, mal_anime_id: int) -> dict:
        with connect(self.config.db_path) as conn:
            row = conn.execute(
                "SELECT source_mal_anime_id, status, num_edges, fetched_at FROM mal_recommendation_harvest_status WHERE source_mal_anime_id = ?",
                (mal_anime_id,),
            ).fetchone()
        self.assertIsNotNone(row)
        return {key: row[key] for key in row.keys()}

    def _seed_detail(self, anime_id: int, *, recommendations: list[dict] | None = None) -> dict:
        return {
            "id": anime_id,
            "title": f"Seed {anime_id}",
            "alternative_titles": {"en": f"Seed {anime_id} EN", "synonyms": [f"Alias {anime_id}"]},
            "main_picture": {"medium": "https://example.invalid/a.jpg"},
            "synopsis": "Official detail payload",
            "media_type": "tv",
            "status": "finished_airing",
            "num_episodes": 12,
            "mean": 8.1,
            "rank": anime_id,
            "popularity": anime_id + 100,
            "num_list_users": 12345,
            "num_scoring_users": 6789,
            "rating": "pg_13",
            "average_episode_duration": 1420,
            "statistics": {"status": {"completed": "1000"}},
            "start_season": {"year": 2020, "season": "winter"},
            "source": "manga",
            "genres": [{"id": 1, "name": "Action"}],
            "studios": [{"id": 2, "name": "Bones"}],
            "related_anime": [],
            "recommendations": recommendations or [],
            "my_list_status": {"status": "completed", "score": 9, "num_episodes_watched": 12},
        }

    def _list_item(self, anime_id: int, title: str, status: str, *, score: int = 0) -> dict:
        return {
            "node": {"id": anime_id, "title": title},
            "list_status": {"status": status, "score": score, "num_episodes_watched": 12 if status == "completed" else 0},
        }

    def test_mal_list_refresh_terminal_complete_can_prune_absent_rows(self) -> None:
        replace_mal_user_anime_list_cache_generation(
            self.config.db_path,
            items=[self._list_item(90, "Old", "completed", score=8)],
            refresh_run_id="old",
            fetched_at="2026-07-19T00:00:00Z",
            prune_absent=True,
        )

        with patch(
            "mal_updater.recommendation_metadata.MalClient.iter_my_anime_list_pages",
            return_value=iter([{"data": [self._list_item(100, "New", "completed", score=9)], "paging": {}}]),
        ):
            summary = refresh_mal_user_anime_list_cache(self.config, max_pages=3, prune_on_complete=True)

        self.assertEqual("ok", summary.status)
        self.assertFalse(summary.partial)
        self.assertEqual(1, summary.pages)
        self.assertEqual({"completed": 1}, summary.by_status)
        self.assertEqual(1, summary.scored)
        self.assertEqual([100], [row.mal_anime_id for row in list_mal_user_anime_list_cache(self.config.db_path)])

    def test_mal_list_refresh_partial_upserts_seen_rows_and_preserves_absent_rows(self) -> None:
        replace_mal_user_anime_list_cache_generation(
            self.config.db_path,
            items=[self._list_item(90, "Old", "completed", score=8)],
            refresh_run_id="old",
            fetched_at="2026-07-19T00:00:00Z",
            prune_absent=True,
        )

        with patch(
            "mal_updater.recommendation_metadata.MalClient.iter_my_anime_list_pages",
            return_value=iter([
                {
                    "data": [self._list_item(100, "New", "watching", score=7)],
                    "paging": {"next": "https://api.myanimelist.net/v2/users/@me/animelist?offset=100"},
                }
            ]),
        ):
            summary = refresh_mal_user_anime_list_cache(self.config, max_pages=1, prune_on_complete=True)

        self.assertEqual("partial", summary.status)
        self.assertTrue(summary.partial)
        self.assertEqual(1, summary.pages)
        self.assertEqual(1, summary.preserved_absent)
        self.assertEqual([90, 100], [row.mal_anime_id for row in list_mal_user_anime_list_cache(self.config.db_path)])

    def test_mal_list_refresh_failure_aborts_without_upserting_partial_page_rows(self) -> None:
        replace_mal_user_anime_list_cache_generation(
            self.config.db_path,
            items=[self._list_item(90, "Old", "completed", score=8)],
            refresh_run_id="old",
            fetched_at="2026-07-19T00:00:00Z",
            prune_absent=True,
        )

        def failing_pages(**_: object):
            yield {"data": [self._list_item(100, "New", "watching", score=7)], "paging": {}}
            raise MalApiError("safe failure")

        with patch("mal_updater.recommendation_metadata.MalClient.iter_my_anime_list_pages", side_effect=failing_pages):
            summary = refresh_mal_user_anime_list_cache(self.config, max_pages=3)

        self.assertEqual("failed", summary.status)
        self.assertTrue(summary.partial)
        self.assertEqual(1, summary.pages)
        self.assertEqual(1, summary.items)
        self.assertEqual({"watching": 1}, summary.by_status)
        self.assertEqual([90], [row.mal_anime_id for row in list_mal_user_anime_list_cache(self.config.db_path)])

    def test_expanded_official_fields_and_user_list_status_are_requested_and_cached_raw(self) -> None:
        self._insert_series("seed-100", title="Seed 100")
        self._map_series("seed-100", 100)

        seen_fields: list[str] = []

        def fake_get_anime_details(anime_id: int, *, fields: str = "") -> dict:
            seen_fields.append(fields)
            return self._seed_detail(anime_id)

        with patch("mal_updater.recommendation_metadata.MalClient.get_anime_details", side_effect=fake_get_anime_details):
            summary = refresh_recommendation_metadata(self.config)

        self.assertEqual(1, summary.refreshed)
        self.assertEqual([DETAIL_FIELDS], seen_fields)
        for field in ("rank", "num_list_users", "num_scoring_users", "rating", "average_episode_duration", "statistics"):
            self.assertIn(field, DETAIL_FIELDS)
        metadata = get_mal_anime_metadata_map(self.config.db_path)[100]
        self.assertEqual(100, metadata.raw["rank"])
        self.assertEqual(12345, metadata.raw["num_list_users"])
        self.assertEqual(6789, metadata.raw["num_scoring_users"])
        self.assertEqual("pg_13", metadata.raw["rating"])
        self.assertEqual(1420, metadata.raw["average_episode_duration"])
        self.assertEqual({"completed": "1000"}, metadata.raw["statistics"]["status"])
        self.assertEqual("manga", metadata.raw["source"])
        self.assertEqual("Bones", metadata.raw["studios"][0]["name"])
        self.assertEqual(9, metadata.raw["my_list_status"]["score"])
        self.assertIn("Official MAL API v2", CHARACTER_VOICE_ACTOR_CAPABILITY_NOTE)
        self.assertIn("unofficial", CHARACTER_VOICE_ACTOR_CAPABILITY_NOTE)

    def test_refresh_order_prioritizes_unharvested_failed_stale_then_stale_metadata_then_fresh(self) -> None:
        setup = [
            (100, "missing-harvest", "2999-01-01 00:00:00", None, None),
            (200, "stale-metadata", "2000-01-01 00:00:00", "fetched", "2999-01-01 00:00:00"),
            (300, "fresh", "2999-01-01 00:00:00", "fetched", "2999-01-01 00:00:00"),
            (400, "failed-harvest", "2999-01-01 00:00:00", "failed", "2999-01-01 00:00:00"),
            (500, "stale-harvest", "2999-01-01 00:00:00", "fetched", "2000-01-01 00:00:00"),
        ]
        for anime_id, provider_id, metadata_fetched_at, harvest_status, harvest_fetched_at in setup:
            self._insert_series(provider_id, title=provider_id)
            self._map_series(provider_id, anime_id)
            self._cache_metadata(anime_id, title=provider_id, fetched_at=metadata_fetched_at)
            if harvest_status is not None and harvest_fetched_at is not None:
                self._cache_harvest_status(anime_id, status=harvest_status, fetched_at=harvest_fetched_at)

        def fake_get_anime_details(anime_id: int, *, fields: str = "") -> dict:
            return self._seed_detail(anime_id)

        with patch("mal_updater.recommendation_metadata.MalClient.get_anime_details", side_effect=fake_get_anime_details) as get_details:
            summary = refresh_recommendation_metadata(self.config, limit=5)

        self.assertEqual([100, 400, 500, 200, 300], [call.args[0] for call in get_details.call_args_list])
        self.assertEqual(5, summary.eligible_seed_count)
        self.assertEqual(1, summary.harvest_unharvested)
        self.assertEqual(1, summary.harvest_failed)
        self.assertEqual(1, summary.harvest_stale)
        self.assertEqual(0, summary.harvested_edge_count)
        self.assertEqual(1, summary.as_dict()["harvest_failed"])

    def test_failed_harvest_state_is_recorded_and_prioritized_for_retry(self) -> None:
        self._insert_series("seed-100", title="Seed 100")
        self._map_series("seed-100", 100)

        with patch(
            "mal_updater.recommendation_metadata.MalClient.get_anime_details",
            side_effect=MalApiError("MAL API anime details failed for anime_id=100: HTTP 504"),
        ):
            failed_summary = refresh_recommendation_metadata(self.config)

        self.assertEqual(0, failed_summary.refreshed)
        self.assertEqual(1, failed_summary.as_dict()["failed"])
        self.assertEqual("failed", self._harvest_status(100)["status"])

        def fake_get_anime_details(anime_id: int, *, fields: str = "") -> dict:
            return self._seed_detail(anime_id, recommendations=[{"node": {"id": 900, "title": "Retry Target"}, "num_recommendations": 7}])

        with patch("mal_updater.recommendation_metadata.MalClient.get_anime_details", side_effect=fake_get_anime_details) as get_details:
            retry_summary = refresh_recommendation_metadata(self.config)

        self.assertEqual([100], [call.args[0] for call in get_details.call_args_list])
        self.assertEqual(1, retry_summary.harvest_failed)
        self.assertEqual(1, retry_summary.refreshed)
        self.assertEqual(1, retry_summary.harvested_edge_count)
        status = self._harvest_status(100)
        self.assertEqual("fetched", status["status"])
        self.assertEqual(1, status["num_edges"])

    def test_overlap_edges_keep_per_seed_evidence_and_harvest_status(self) -> None:
        for anime_id in (100, 200):
            provider_id = f"seed-{anime_id}"
            self._insert_series(provider_id, title=provider_id)
            self._map_series(provider_id, anime_id)

        def fake_get_anime_details(anime_id: int, *, fields: str = "") -> dict:
            vote_count = 11 if anime_id == 100 else 5
            return self._seed_detail(
                anime_id,
                recommendations=[{"node": {"id": 900, "title": "Shared Target"}, "num_recommendations": vote_count}],
            )

        with patch("mal_updater.recommendation_metadata.MalClient.get_anime_details", side_effect=fake_get_anime_details):
            summary = refresh_recommendation_metadata(self.config)

        self.assertEqual(2, summary.harvested_edge_count)
        edges_by_source = get_mal_recommendation_edges_map(self.config.db_path)
        self.assertEqual({100, 200}, set(edges_by_source))
        self.assertEqual(900, edges_by_source[100][0].target_mal_anime_id)
        self.assertEqual("Shared Target", edges_by_source[100][0].target_title)
        self.assertEqual(11, edges_by_source[100][0].num_recommendations)
        self.assertEqual(5, edges_by_source[200][0].num_recommendations)
        self.assertTrue(edges_by_source[100][0].fetched_at)
        self.assertEqual(11, edges_by_source[100][0].raw["num_recommendations"])
        self.assertEqual("fetched", self._harvest_status(100)["status"])
        self.assertEqual("fetched", self._harvest_status(200)["status"])

    def test_discovery_hydration_skips_mapped_and_known_listed_targets_before_limit(self) -> None:
        self._insert_series("seed-100", title="Seed 100")
        self._map_series("seed-100", 100)
        self._insert_series("already-mapped", title="Already Mapped")
        self._map_series("already-mapped", 200)
        self._cache_metadata(
            300,
            title="Known Listed",
            raw={"id": 300, "title": "Known Listed", "my_list_status": {"status": "completed", "num_episodes_watched": 12}},
        )

        def fake_get_anime_details(anime_id: int, *, fields: str = "") -> dict:
            if anime_id == 100:
                return self._seed_detail(
                    100,
                    recommendations=[
                        {"node": {"id": 200, "title": "Already Mapped"}, "num_recommendations": 50},
                        {"node": {"id": 300, "title": "Known Listed"}, "num_recommendations": 40},
                        {"node": {"id": 400, "title": "Hydratable"}, "num_recommendations": 30},
                    ],
                )
            return {
                "id": 400,
                "title": "Hydratable",
                "alternative_titles": {},
                "media_type": "tv",
                "status": "finished_airing",
                "num_episodes": 12,
                "mean": 8.4,
                "rank": 400,
                "popularity": 10,
                "num_list_users": 2000,
                "num_scoring_users": 1500,
                "rating": "pg_13",
                "average_episode_duration": 1440,
                "statistics": {"status": {"plan_to_watch": "100"}},
                "start_season": {"year": 2023, "season": "fall"},
                "source": "original",
                "genres": [],
                "studios": [],
                "my_list_status": {"status": "plan_to_watch", "num_episodes_watched": 0},
            }

        with patch("mal_updater.recommendation_metadata.MalClient.get_anime_details", side_effect=fake_get_anime_details) as get_details:
            summary = refresh_recommendation_metadata(
                self.config,
                limit=1,
                include_discovery_targets=True,
                discovery_target_limit=1,
            )

        self.assertEqual([100, 400], [call.args[0] for call in get_details.call_args_list])
        self.assertEqual(1, summary.discovery_considered)
        self.assertEqual(1, summary.discovery_refreshed)
        self.assertEqual({"already_mapped": 1, "already_listed": 1}, summary.target_hydration_skip_reasons)
        self.assertEqual({"already_mapped": 1, "already_listed": 1}, summary.as_dict()["target_hydration_skip_reasons"])
        metadata = get_mal_anime_metadata_map(self.config.db_path)
        self.assertIn(400, metadata)
        self.assertEqual("original", metadata[400].raw["source"])


if __name__ == "__main__":
    unittest.main()
