from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from mal_updater.config import load_config
from mal_updater.db import (
    MAL_RECOMMENDATION_SOURCE_PUBLIC_USERRECS,
    bootstrap_database,
    connect,
    create_or_get_active_mal_public_userrecs_generation,
    get_active_mal_public_userrecs_generation,
    get_mal_public_userrecs_generation,
    get_mal_anime_metadata_map,
    get_mal_recommendation_edges_map,
    list_mal_user_anime_list_cache,
    mark_mal_public_userrecs_generation_ready,
    replace_mal_public_userrecs_recommendation_edges,
    replace_mal_public_userrecs_staged_page,
    replace_mal_recommendation_edges,
    replace_mal_user_anime_list_cache_generation,
    upsert_mal_anime_metadata,
    upsert_series_mapping,
)
from mal_updater.mal_user_recommendations import (
    PublicMalRecommendationEdge,
    PublicMalUserRecommendationsError,
    PublicUserRecommendationsPageFetchResult,
    build_public_user_recs_url,
    public_user_recs_page_anchor,
    public_user_recs_page_fingerprint,
)
from mal_updater.mal_client import MalApiError
from mal_updater.recommendation_metadata import (
    CHARACTER_VOICE_ACTOR_CAPABILITY_NOTE,
    DETAIL_FIELDS,
    MAL_USER_LIST_FIELDS,
    MAL_USER_LIST_STATUS_PREFERENCE_FIELDS,
    refresh_full_user_recommendation_harvest,
    refresh_mal_user_anime_list_cache,
    refresh_recommendation_metadata,
)


def _public_userrecs_page_result(
    *,
    source_id: int,
    page_url: str,
    next_url: str | None,
    targets: list[tuple[int, str, int]],
) -> PublicUserRecommendationsPageFetchResult:
    edges = [
        PublicMalRecommendationEdge(
            target_mal_anime_id=target_id,
            target_title=title,
            num_recommendations=count,
            page_url=page_url,
        )
        for target_id, title, count in targets
    ]
    return PublicUserRecommendationsPageFetchResult(
        source_mal_anime_id=source_id,
        requested_url=page_url,
        final_url=page_url,
        next_url=next_url,
        page_fingerprint=public_user_recs_page_fingerprint(final_url=page_url, next_url=next_url, edges=edges),
        anchor=public_user_recs_page_anchor(edges),
        edges=edges,
    )


class _FakePublicUserRecsClient:
    def __init__(self, pages: dict[tuple[int, str], PublicUserRecommendationsPageFetchResult | BaseException]) -> None:
        self.pages = pages
        self.requested: list[tuple[int, str]] = []

    def fetch_page(self, source_mal_anime_id: int, *, page_url: str, max_body_bytes: int):  # type: ignore[no-untyped-def]
        key = (int(source_mal_anime_id), page_url)
        self.requested.append(key)
        result = self.pages.get(key)
        if result is None:  # pragma: no cover - fixture guard
            raise AssertionError(f"unexpected public-userrecs fetch {key!r}")
        if isinstance(result, BaseException):
            raise result
        return result


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
            "start_date": "2020-01-10",
            "end_date": "2020-03-27",
            "broadcast": {"day_of_the_week": "friday", "start_time": "23:30", "timezone": "Asia/Tokyo"},
            "pictures": [{"medium": "https://example.invalid/gallery.jpg"}],
            "background": "Official background payload",
            "nsfw": "white",
            "statistics": {"status": {"completed": "1000"}},
            "start_season": {"year": 2020, "season": "winter"},
            "source": "manga",
            "genres": [{"id": 1, "name": "Action"}],
            "studios": [{"id": 2, "name": "Bones"}],
            "related_anime": [],
            "related_manga": [{"node": {"id": 50, "title": "Seed Manga"}, "relation_type": "adaptation"}],
            "recommendations": recommendations or [],
            "my_list_status": {"status": "completed", "score": 9, "num_episodes_watched": 12},
        }

    def _list_item(self, anime_id: int, title: str, status: str, *, score: int = 0) -> dict:
        return {
            "node": {"id": anime_id, "title": title},
            "list_status": {"status": status, "score": score, "num_episodes_watched": 12 if status == "completed" else 0},
        }

    def test_mal_list_refresh_requests_official_preference_field_names(self) -> None:
        seen_fields: list[str] = []

        def fake_pages(**kwargs: object):
            seen_fields.append(str(kwargs.get("fields") or ""))
            yield {"data": [], "paging": {}}

        with patch("mal_updater.recommendation_metadata.MalClient.iter_my_anime_list_pages", side_effect=fake_pages):
            summary = refresh_mal_user_anime_list_cache(self.config, max_pages=1)

        self.assertEqual("ok", summary.status)
        self.assertEqual([MAL_USER_LIST_FIELDS], seen_fields)
        for field in MAL_USER_LIST_STATUS_PREFERENCE_FIELDS:
            self.assertIn(field, MAL_USER_LIST_FIELDS)

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
        for field in (
            "rank",
            "num_list_users",
            "num_scoring_users",
            "rating",
            "average_episode_duration",
            "start_date",
            "end_date",
            "broadcast",
            "pictures",
            "background",
            "nsfw",
            "related_manga",
            "statistics",
        ):
            self.assertIn(field, DETAIL_FIELDS)
        metadata = get_mal_anime_metadata_map(self.config.db_path)[100]
        self.assertEqual(100, metadata.rank)
        self.assertEqual(12345, metadata.num_list_users)
        self.assertEqual(6789, metadata.num_scoring_users)
        self.assertEqual("pg_13", metadata.rating)
        self.assertEqual(1420, metadata.average_episode_duration)
        self.assertEqual("2020-01-10", metadata.start_date)
        self.assertEqual("2020-03-27", metadata.end_date)
        self.assertEqual("friday", metadata.broadcast_day)
        self.assertEqual("23:30", metadata.broadcast_time)
        self.assertEqual("Asia/Tokyo", metadata.broadcast_timezone)
        self.assertEqual("white", metadata.nsfw)
        self.assertEqual(100, metadata.raw["rank"])
        self.assertEqual(12345, metadata.raw["num_list_users"])
        self.assertEqual(6789, metadata.raw["num_scoring_users"])
        self.assertEqual("pg_13", metadata.raw["rating"])
        self.assertEqual(1420, metadata.raw["average_episode_duration"])
        self.assertEqual("friday", metadata.raw["broadcast"]["day_of_the_week"])
        self.assertEqual("Official background payload", metadata.raw["background"])
        self.assertEqual("Seed Manga", metadata.raw["related_manga"][0]["node"]["title"])
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

        self.assertEqual([100, 400, 500, 200], [call.args[0] for call in get_details.call_args_list])
        self.assertEqual(1, summary.fresh_skipped)
        self.assertEqual(5, summary.eligible_seed_count)
        self.assertEqual(1, summary.harvest_unharvested)
        self.assertEqual(1, summary.harvest_failed)
        self.assertEqual(1, summary.harvest_stale)
        self.assertEqual(0, summary.harvested_edge_count)
        self.assertEqual(1, summary.as_dict()["harvest_failed"])

    def test_positive_cached_list_entries_join_seed_order_without_provider_mapping(self) -> None:
        replace_mal_user_anime_list_cache_generation(
            self.config.db_path,
            items=[
                self._list_item(700, "Positive On Hold", "on_hold", score=6),
                self._list_item(800, "Suppressed Plan To Watch", "plan_to_watch"),
            ],
            refresh_run_id="cached-list",
            fetched_at="2026-07-19T00:00:00Z",
            prune_absent=True,
        )

        def fake_get_anime_details(anime_id: int, *, fields: str = "") -> dict:
            return self._seed_detail(anime_id)

        with patch("mal_updater.recommendation_metadata.MalClient.get_anime_details", side_effect=fake_get_anime_details) as get_details:
            summary = refresh_recommendation_metadata(self.config)

        self.assertEqual([700], [call.args[0] for call in get_details.call_args_list])
        self.assertEqual(1, summary.eligible_seed_count)
        self.assertEqual(1, summary.harvest_unharvested)
        self.assertEqual(1, summary.refreshed)

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
                "start_date": "2023-10-01",
                "end_date": "2023-12-24",
                "broadcast": {"day_of_the_week": "sunday", "start_time": "00:00"},
                "pictures": [],
                "background": "Hydratable background",
                "nsfw": "white",
                "statistics": {"status": {"plan_to_watch": "100"}},
                "start_season": {"year": 2023, "season": "fall"},
                "source": "original",
                "genres": [],
                "studios": [],
                "related_manga": [],
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


class FullUserRecommendationHarvestResumableTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.project_root = Path(self.temp_dir.name)
        (self.project_root / ".MAL-Updater" / "config").mkdir(parents=True, exist_ok=True)
        self.config = load_config(self.project_root)
        bootstrap_database(self.config.db_path)

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def _list_item(self, anime_id: int, title: str, status: str = "completed") -> dict:
        return {
            "node": {"id": anime_id, "title": title},
            "list_status": {"status": status, "score": 8, "num_episodes_watched": 12},
        }

    def _seed_positive_list(self, *items: tuple[int, str]) -> None:
        replace_mal_user_anime_list_cache_generation(
            self.config.db_path,
            items=[self._list_item(anime_id, title) for anime_id, title in items],
            refresh_run_id="cached-list",
            fetched_at="2026-07-28T00:00:00Z",
            prune_absent=True,
        )

    def _source_url(self, anime_id: int, title: str) -> str:
        return build_public_user_recs_url(self.config.mal.public_base_url, source_mal_anime_id=anime_id, source_title=title)

    def _published_targets(self, source_id: int = 1) -> dict[int, int]:
        with connect(self.config.db_path) as conn:
            rows = conn.execute(
                """
                SELECT target_mal_anime_id, num_recommendations
                FROM mal_anime_recommendations
                WHERE source_mal_anime_id = ? AND source_kind = 'mal_recommendation'
                ORDER BY target_mal_anime_id
                """,
                (source_id,),
            ).fetchall()
        return {int(row["target_mal_anime_id"]): int(row["num_recommendations"] or 0) for row in rows}

    def _harvest_status(self, source_id: int = 1) -> dict[str, object] | None:
        with connect(self.config.db_path) as conn:
            row = conn.execute(
                """
                SELECT status, num_edges, source_type, is_complete, pages_fetched, source_url, last_error, failure_count
                FROM mal_recommendation_harvest_status
                WHERE source_mal_anime_id = ?
                """,
                (source_id,),
            ).fetchone()
        return None if row is None else {key: row[key] for key in row.keys()}

    def test_interrupted_multirun_resume_uses_persisted_cursor_and_terminal_publish(self) -> None:
        self._seed_positive_list((1, "Seed 1"))
        page1 = self._source_url(1, "Seed 1")
        page2 = f"{page1}?p=2"
        first_client = _FakePublicUserRecsClient(
            {
                (1, page1): _public_userrecs_page_result(
                    source_id=1,
                    page_url=page1,
                    next_url=page2,
                    targets=[(10, "Ten", 3)],
                )
            }
        )

        first = refresh_full_user_recommendation_harvest(self.config, max_pages=1, client=first_client)

        self.assertEqual("partial", first.status)
        self.assertEqual(0, first.failed)
        self.assertEqual(1, first.as_dict()["paused"])
        self.assertEqual([(1, page1)], first_client.requested)
        generation = get_active_mal_public_userrecs_generation(self.config.db_path, source_mal_anime_id=1)
        self.assertIsNotNone(generation)
        self.assertEqual("paused", generation.status)
        self.assertEqual(page2, generation.cursor_url)
        self.assertEqual(1, generation.pages_fetched)
        self.assertEqual({}, self._published_targets())

        second_client = _FakePublicUserRecsClient(
            {
                (1, page2): _public_userrecs_page_result(
                    source_id=1,
                    page_url=page2,
                    next_url=None,
                    targets=[(20, "Twenty", 5)],
                )
            }
        )

        second = refresh_full_user_recommendation_harvest(self.config, max_pages=1, client=second_client)

        self.assertEqual("ok", second.status)
        self.assertEqual(1, second.harvested)
        self.assertEqual(0, second.failed)
        self.assertEqual([(1, page2)], second_client.requested)
        self.assertIsNone(get_active_mal_public_userrecs_generation(self.config.db_path, source_mal_anime_id=1))
        self.assertEqual({10: 3, 20: 5}, self._published_targets())
        status = self._harvest_status()
        self.assertIsNotNone(status)
        self.assertEqual("fetched", status["status"])
        self.assertEqual(MAL_RECOMMENDATION_SOURCE_PUBLIC_USERRECS, status["source_type"])
        self.assertEqual(1, status["is_complete"])
        self.assertEqual(2, status["pages_fetched"])

    def test_fetch_failure_preserves_staged_generation_and_existing_published_edges(self) -> None:
        self._seed_positive_list((1, "Seed 1"))
        page1 = self._source_url(1, "Seed 1")
        page2 = f"{page1}?p=2"
        replace_mal_public_userrecs_recommendation_edges(
            self.config.db_path,
            source_mal_anime_id=1,
            edges=[{"target_mal_anime_id": 90, "target_title": "Old", "num_recommendations": 2, "raw": {}, "provenance": {}}],
            pages_fetched=1,
            source_url=page1,
        )
        first_client = _FakePublicUserRecsClient(
            {
                (1, page1): _public_userrecs_page_result(
                    source_id=1,
                    page_url=page1,
                    next_url=page2,
                    targets=[(10, "Ten", 3)],
                )
            }
        )
        refresh_full_user_recommendation_harvest(self.config, max_pages=1, force_refresh=True, client=first_client)

        failing_client = _FakePublicUserRecsClient({(1, page2): PublicMalUserRecommendationsError("temporary parser failure")})
        failed = refresh_full_user_recommendation_harvest(self.config, max_pages=1, force_refresh=True, client=failing_client)

        self.assertEqual("failed", failed.status)
        self.assertEqual(1, failed.failed)
        self.assertEqual([(1, page2)], failing_client.requested)
        generation = get_active_mal_public_userrecs_generation(self.config.db_path, source_mal_anime_id=1)
        self.assertIsNotNone(generation)
        self.assertEqual("paused", generation.status)
        self.assertEqual(page2, generation.cursor_url)
        self.assertEqual(1, generation.pages_fetched)
        self.assertIn("temporary parser failure", generation.last_error or "")
        self.assertEqual({90: 2}, self._published_targets())
        status = self._harvest_status()
        self.assertIsNotNone(status)
        self.assertEqual("fetched", status["status"])
        self.assertEqual(1, status["is_complete"])
        self.assertIn("temporary parser failure", status["last_error"] or "")
        self.assertEqual(0, status["failure_count"])

    def test_drift_restarts_generation_before_fetching_from_source_url(self) -> None:
        self._seed_positive_list((1, "Seed 1"))
        page1 = self._source_url(1, "Seed 1")
        page2 = f"{page1}?p=2"
        generation = create_or_get_active_mal_public_userrecs_generation(
            self.config.db_path,
            source_mal_anime_id=1,
            source_title="Seed 1",
            source_url=page1,
        )
        replace_mal_public_userrecs_staged_page(
            self.config.db_path,
            generation_id=generation.generation_id,
            page_number=1,
            page_url=page1,
            page_fingerprint="old-fingerprint",
            next_url=page2,
            edges=[{"target_mal_anime_id": 10, "target_title": "Ten", "num_recommendations": 1, "raw": {}, "provenance": {}}],
        )
        with connect(self.config.db_path) as conn:
            conn.execute(
                "UPDATE mal_public_userrecs_crawl_generations SET last_page_fingerprint = 'corrupt' WHERE generation_id = ?",
                (generation.generation_id,),
            )
            conn.commit()
        client = _FakePublicUserRecsClient(
            {
                (1, page1): _public_userrecs_page_result(
                    source_id=1,
                    page_url=page1,
                    next_url=None,
                    targets=[(30, "Thirty", 7)],
                )
            }
        )

        summary = refresh_full_user_recommendation_harvest(self.config, max_pages=1, force_refresh=True, client=client)

        self.assertEqual("ok", summary.status)
        self.assertEqual(1, len(summary.restarted_sources))
        self.assertIn("fingerprint", summary.restarted_sources[0]["reason"])
        self.assertEqual([(1, page1)], client.requested)
        old = get_mal_public_userrecs_generation(self.config.db_path, generation_id=generation.generation_id)
        self.assertIsNotNone(old)
        self.assertEqual("discarded", old.status)
        self.assertEqual({30: 7}, self._published_targets())

    def test_active_generation_is_selected_before_new_due_source_under_limit(self) -> None:
        self._seed_positive_list((1, "Seed 1"), (2, "Seed 2"))
        source2_url = self._source_url(2, "Seed 2")
        create_or_get_active_mal_public_userrecs_generation(
            self.config.db_path,
            source_mal_anime_id=2,
            source_title="Seed 2",
            source_url=source2_url,
        )
        client = _FakePublicUserRecsClient(
            {
                (2, source2_url): _public_userrecs_page_result(
                    source_id=2,
                    page_url=source2_url,
                    next_url=None,
                    targets=[(200, "Two Hundred", 4)],
                )
            }
        )

        summary = refresh_full_user_recommendation_harvest(self.config, limit=1, max_pages=1, client=client)

        self.assertEqual("ok", summary.status)
        self.assertEqual([(2, source2_url)], client.requested)
        self.assertEqual([2], [source["mal_anime_id"] for source in summary.harvested_sources])
        self.assertEqual({200: 4}, self._published_targets(source_id=2))
        self.assertEqual({}, self._published_targets(source_id=1))

    def test_open_generations_do_not_starve_behind_stale_due_sources(self) -> None:
        self._seed_positive_list((1, "Seed 1"), (2, "Seed 2"))
        source1_url = self._source_url(1, "Seed 1")
        source2_url = self._source_url(2, "Seed 2")
        create_or_get_active_mal_public_userrecs_generation(
            self.config.db_path,
            source_mal_anime_id=2,
            source_title="Seed 2",
            source_url=source2_url,
        )
        replace_mal_public_userrecs_recommendation_edges(
            self.config.db_path,
            source_mal_anime_id=1,
            edges=[{"target_mal_anime_id": 100, "target_title": "Old", "num_recommendations": 1, "raw": {}, "provenance": {}}],
            pages_fetched=1,
            source_url=source1_url,
        )
        with connect(self.config.db_path) as conn:
            conn.execute(
                "UPDATE mal_recommendation_harvest_status SET fetched_at = '2000-01-01 00:00:00' WHERE source_mal_anime_id = 1"
            )
            conn.commit()
        client = _FakePublicUserRecsClient(
            {
                (2, source2_url): _public_userrecs_page_result(
                    source_id=2,
                    page_url=source2_url,
                    next_url=None,
                    targets=[(200, "Two Hundred", 4)],
                )
            }
        )

        summary = refresh_full_user_recommendation_harvest(self.config, limit=1, max_pages=1, client=client)

        self.assertEqual("ok", summary.status)
        self.assertEqual([(2, source2_url)], client.requested)
        self.assertEqual([2], [source["mal_anime_id"] for source in summary.harvested_sources])
        self.assertEqual({100: 1}, self._published_targets(source_id=1))
        self.assertEqual({200: 4}, self._published_targets(source_id=2))

    def test_next_link_loop_restarts_and_discards_instead_of_mixed_publish(self) -> None:
        self._seed_positive_list((1, "Seed 1"))
        page1 = self._source_url(1, "Seed 1")
        page2 = f"{page1}?p=2"
        first_client = _FakePublicUserRecsClient(
            {
                (1, page1): _public_userrecs_page_result(
                    source_id=1,
                    page_url=page1,
                    next_url=page2,
                    targets=[(10, "Ten", 3)],
                )
            }
        )
        first = refresh_full_user_recommendation_harvest(self.config, max_pages=1, client=first_client)
        self.assertEqual("partial", first.status)
        old_generation = get_active_mal_public_userrecs_generation(self.config.db_path, source_mal_anime_id=1)
        self.assertIsNotNone(old_generation)

        loop_client = _FakePublicUserRecsClient(
            {
                (1, page2): _public_userrecs_page_result(
                    source_id=1,
                    page_url=page2,
                    next_url=page1,
                    targets=[(20, "Twenty", 5)],
                )
            }
        )

        loop = refresh_full_user_recommendation_harvest(self.config, max_pages=1, client=loop_client)

        self.assertEqual("partial", loop.status)
        self.assertEqual(1, len(loop.restarted_sources))
        self.assertIn("loops", loop.restarted_sources[0]["reason"])
        self.assertEqual([(1, page2)], loop_client.requested)
        discarded = get_mal_public_userrecs_generation(self.config.db_path, generation_id=old_generation.generation_id)
        self.assertIsNotNone(discarded)
        self.assertEqual("discarded", discarded.status)
        active = get_active_mal_public_userrecs_generation(self.config.db_path, source_mal_anime_id=1)
        self.assertIsNotNone(active)
        self.assertNotEqual(old_generation.generation_id, active.generation_id)
        self.assertEqual("paused", active.status)
        self.assertEqual(page1, active.cursor_url)
        self.assertEqual({}, self._published_targets())

    def test_ready_generation_drift_restarts_before_publish_and_preserves_existing_graph(self) -> None:
        self._seed_positive_list((1, "Seed 1"))
        page1 = self._source_url(1, "Seed 1")
        page2 = f"{page1}?p=2"
        replace_mal_public_userrecs_recommendation_edges(
            self.config.db_path,
            source_mal_anime_id=1,
            edges=[{"target_mal_anime_id": 90, "target_title": "Old", "num_recommendations": 2, "raw": {}, "provenance": {}}],
            pages_fetched=1,
            source_url=page1,
        )
        generation = create_or_get_active_mal_public_userrecs_generation(
            self.config.db_path,
            source_mal_anime_id=1,
            source_title="Seed 1",
            source_url=page1,
        )
        replace_mal_public_userrecs_staged_page(
            self.config.db_path,
            generation_id=generation.generation_id,
            page_number=1,
            page_url=page1,
            page_fingerprint="fp-ready",
            next_url=None,
            edges=[{"target_mal_anime_id": 10, "target_title": "Ten", "num_recommendations": 9, "raw": {}, "provenance": {}}],
        )
        mark_mal_public_userrecs_generation_ready(self.config.db_path, generation_id=generation.generation_id)
        with connect(self.config.db_path) as conn:
            conn.execute(
                "UPDATE mal_public_userrecs_staged_pages SET page_fingerprint = 'corrupt' WHERE generation_id = ?",
                (generation.generation_id,),
            )
            conn.commit()
        client = _FakePublicUserRecsClient(
            {
                (1, page1): _public_userrecs_page_result(
                    source_id=1,
                    page_url=page1,
                    next_url=page2,
                    targets=[(30, "Thirty", 7)],
                )
            }
        )

        summary = refresh_full_user_recommendation_harvest(self.config, max_pages=1, force_refresh=True, client=client)

        self.assertEqual("partial", summary.status)
        self.assertEqual(0, summary.harvested)
        self.assertEqual(1, len(summary.restarted_sources))
        self.assertIn("fingerprint", summary.restarted_sources[0]["reason"])
        self.assertEqual([(1, page1)], client.requested)
        old = get_mal_public_userrecs_generation(self.config.db_path, generation_id=generation.generation_id)
        self.assertIsNotNone(old)
        self.assertEqual("discarded", old.status)
        active = get_active_mal_public_userrecs_generation(self.config.db_path, source_mal_anime_id=1)
        self.assertIsNotNone(active)
        self.assertEqual("paused", active.status)
        self.assertEqual(page2, active.cursor_url)
        self.assertEqual({90: 2}, self._published_targets())

    def test_one_page_public_userrecs_harvest_publishes_complete_legacy_behavior(self) -> None:
        self._seed_positive_list((1, "Seed 1"))
        page1 = self._source_url(1, "Seed 1")
        client = _FakePublicUserRecsClient(
            {
                (1, page1): _public_userrecs_page_result(
                    source_id=1,
                    page_url=page1,
                    next_url=None,
                    targets=[(10, "Ten", 3)],
                )
            }
        )

        summary = refresh_full_user_recommendation_harvest(self.config, max_pages=1, client=client)

        self.assertEqual("ok", summary.status)
        self.assertEqual(1, summary.harvested)
        self.assertEqual(0, summary.failed)
        self.assertEqual(0, summary.as_dict()["paused"])
        self.assertEqual({10: 3}, self._published_targets())


if __name__ == "__main__":
    unittest.main()
