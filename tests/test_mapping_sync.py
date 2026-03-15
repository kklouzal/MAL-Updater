from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from mal_updater.config import MalSecrets, load_config
from mal_updater.db import list_review_queue_entries, list_series_mappings, upsert_series_mapping
from mal_updater.ingestion import ingest_snapshot_payload
from mal_updater.mal_client import MalClient
from mal_updater.mapping import (
    SeriesMappingInput,
    build_search_queries,
    map_series,
    normalize_title,
    should_auto_approve_mapping,
)
from mal_updater.sync_planner import (
    build_dry_run_sync_plan,
    build_mapping_review,
    execute_approved_sync,
    persist_mapping_review_queue,
    persist_sync_review_queue,
)
from tests.test_validation_ingestion import sample_snapshot


class MappingTests(unittest.TestCase):
    def test_normalize_title_strips_dub_and_season_noise(self) -> None:
        self.assertEqual(
            normalize_title("BOFURI: I Don’t Want to Get Hurt, so I’ll Max Out My Defense. Season 2 (English Dub)"),
            "bofuri i don t want to get hurt so i ll max out my defense",
        )

    def test_build_search_queries_combines_generic_season_title_with_base_title(self) -> None:
        queries = build_search_queries(
            SeriesMappingInput(
                provider="crunchyroll",
                provider_series_id="series-123",
                title="Campfire Cooking in Another World with My Absurd Skill",
                season_title="Season 2",
                season_number=2,
            )
        )

        self.assertEqual(
            queries,
            [
                "Season 2",
                "Campfire Cooking in Another World with My Absurd Skill Season 2",
                "Campfire Cooking in Another World with My Absurd Skill",
            ],
        )

    def test_map_series_classifies_exact_match_conservatively(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / "config").mkdir()
            config = load_config(root)
            client = MalClient(
                config,
                MalSecrets(
                    client_id="client-id",
                    client_secret=None,
                    access_token="access-token",
                    refresh_token=None,
                    client_id_path=root / "secrets" / "mal_client_id.txt",
                    client_secret_path=root / "secrets" / "mal_client_secret.txt",
                    access_token_path=root / "secrets" / "mal_access_token.txt",
                    refresh_token_path=root / "secrets" / "mal_refresh_token.txt",
                ),
            )

            with patch.object(
                MalClient,
                "search_anime",
                return_value={
                    "data": [
                        {
                            "node": {
                                "id": 42,
                                "title": "Attack on Titan Final Season",
                                "alternative_titles": {"synonyms": ["Attack on Titan Final Season (English Dub)"]},
                                "media_type": "tv",
                                "status": "finished_airing",
                                "num_episodes": 16,
                            }
                        },
                        {"node": {"id": 99, "title": "Random Other Show", "alternative_titles": {}, "media_type": "tv"}},
                    ]
                },
            ):
                result = map_series(
                    client,
                    SeriesMappingInput(
                        provider="crunchyroll",
                        provider_series_id="series-123",
                        title="Attack on Titan",
                        season_title="Attack on Titan Final Season (English Dub)",
                    ),
                )

        self.assertEqual(result.status, "exact")
        self.assertIsNotNone(result.chosen_candidate)
        self.assertEqual(result.chosen_candidate.mal_anime_id, 42)

    def test_map_series_uses_season_and_episode_evidence_to_avoid_wrong_sequel(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / "config").mkdir()
            config = load_config(root)
            client = MalClient(
                config,
                MalSecrets(
                    client_id="client-id",
                    client_secret=None,
                    access_token="access-token",
                    refresh_token=None,
                    client_id_path=root / "secrets" / "mal_client_id.txt",
                    client_secret_path=root / "secrets" / "mal_client_secret.txt",
                    access_token_path=root / "secrets" / "mal_access_token.txt",
                    refresh_token_path=root / "secrets" / "mal_refresh_token.txt",
                ),
            )

            with patch.object(
                MalClient,
                "search_anime",
                return_value={
                    "data": [
                        {
                            "node": {
                                "id": 100,
                                "title": "Example Show Season 1",
                                "alternative_titles": {"synonyms": []},
                                "media_type": "tv",
                                "status": "finished_airing",
                                "num_episodes": 12,
                            }
                        },
                        {
                            "node": {
                                "id": 200,
                                "title": "Example Show Season 2",
                                "alternative_titles": {"synonyms": []},
                                "media_type": "tv",
                                "status": "finished_airing",
                                "num_episodes": 12,
                            }
                        },
                    ]
                },
            ):
                result = map_series(
                    client,
                    SeriesMappingInput(
                        provider="crunchyroll",
                        provider_series_id="series-123",
                        title="Example Show",
                        season_title="Example Show Season 2",
                        season_number=2,
                        max_episode_number=12,
                        completed_episode_count=12,
                    ),
                )

        self.assertIsNotNone(result.chosen_candidate)
        self.assertEqual(result.chosen_candidate.mal_anime_id, 200)
        self.assertTrue(any("season_number_match=2" == reason for reason in result.rationale))

    def test_should_auto_approve_exact_unique_match(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / "config").mkdir()
            config = load_config(root)
            client = MalClient(
                config,
                MalSecrets(
                    client_id="client-id",
                    client_secret=None,
                    access_token="access-token",
                    refresh_token=None,
                    client_id_path=root / "secrets" / "mal_client_id.txt",
                    client_secret_path=root / "secrets" / "mal_client_secret.txt",
                    access_token_path=root / "secrets" / "mal_access_token.txt",
                    refresh_token_path=root / "secrets" / "mal_refresh_token.txt",
                ),
            )

            with patch.object(
                MalClient,
                "search_anime",
                return_value={
                    "data": [
                        {
                            "node": {
                                "id": 200,
                                "title": "Example Show Season 2",
                                "alternative_titles": {"synonyms": ["Example Show Season 2 (English Dub)"]},
                                "media_type": "tv",
                                "status": "finished_airing",
                                "num_episodes": 12,
                            }
                        },
                        {
                            "node": {
                                "id": 100,
                                "title": "Different Show",
                                "alternative_titles": {"synonyms": []},
                                "media_type": "tv",
                                "status": "finished_airing",
                                "num_episodes": 12,
                            }
                        },
                    ]
                },
            ):
                result = map_series(
                    client,
                    SeriesMappingInput(
                        provider="crunchyroll",
                        provider_series_id="series-123",
                        title="Example Show",
                        season_title="Example Show Season 2 (English Dub)",
                        season_number=2,
                        max_episode_number=12,
                        completed_episode_count=12,
                    ),
                )

        self.assertEqual(result.status, "exact")
        self.assertTrue(should_auto_approve_mapping(result))

    def test_should_not_auto_approve_when_season_evidence_conflicts(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / "config").mkdir()
            config = load_config(root)
            client = MalClient(
                config,
                MalSecrets(
                    client_id="client-id",
                    client_secret=None,
                    access_token="access-token",
                    refresh_token=None,
                    client_id_path=root / "secrets" / "mal_client_id.txt",
                    client_secret_path=root / "secrets" / "mal_client_secret.txt",
                    access_token_path=root / "secrets" / "mal_access_token.txt",
                    refresh_token_path=root / "secrets" / "mal_refresh_token.txt",
                ),
            )

            with patch.object(
                MalClient,
                "search_anime",
                return_value={
                    "data": [
                        {
                            "node": {
                                "id": 100,
                                "title": "Example Show Season 1",
                                "alternative_titles": {"synonyms": ["Example Show Season 2 (English Dub)"]},
                                "media_type": "tv",
                                "status": "finished_airing",
                                "num_episodes": 12,
                            }
                        },
                        {
                            "node": {
                                "id": 200,
                                "title": "Another Different Show",
                                "alternative_titles": {"synonyms": []},
                                "media_type": "tv",
                                "status": "finished_airing",
                                "num_episodes": 12,
                            }
                        },
                    ]
                },
            ):
                result = map_series(
                    client,
                    SeriesMappingInput(
                        provider="crunchyroll",
                        provider_series_id="series-123",
                        title="Example Show",
                        season_title="Example Show Season 2 (English Dub)",
                        season_number=2,
                        max_episode_number=12,
                        completed_episode_count=12,
                    ),
                )

        self.assertEqual(result.status, "exact")
        self.assertFalse(should_auto_approve_mapping(result))

    def test_map_series_combined_generic_season_query_promotes_safe_exact_match(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / "config").mkdir()
            config = load_config(root)
            client = MalClient(
                config,
                MalSecrets(
                    client_id="client-id",
                    client_secret=None,
                    access_token="access-token",
                    refresh_token=None,
                    client_id_path=root / "secrets" / "mal_client_id.txt",
                    client_secret_path=root / "secrets" / "mal_client_secret.txt",
                    access_token_path=root / "secrets" / "mal_access_token.txt",
                    refresh_token_path=root / "secrets" / "mal_refresh_token.txt",
                ),
            )

            def fake_search(query: str, limit: int = 5) -> dict[str, object]:
                if query == "Campfire Cooking in Another World with My Absurd Skill Season 2":
                    return {
                        "data": [
                            {
                                "node": {
                                    "id": 500,
                                    "title": "Campfire Cooking in Another World with My Absurd Skill Season 2",
                                    "alternative_titles": {"synonyms": []},
                                    "media_type": "tv",
                                    "status": "currently_airing",
                                    "num_episodes": 12,
                                }
                            }
                        ]
                    }
                return {"data": []}

            with patch.object(MalClient, "search_anime", side_effect=fake_search):
                result = map_series(
                    client,
                    SeriesMappingInput(
                        provider="crunchyroll",
                        provider_series_id="series-123",
                        title="Campfire Cooking in Another World with My Absurd Skill",
                        season_title="Season 2",
                        season_number=2,
                        max_episode_number=12,
                        completed_episode_count=12,
                    ),
                )

        self.assertEqual(result.status, "exact")
        self.assertEqual(result.chosen_candidate.mal_anime_id, 500)
        self.assertTrue(should_auto_approve_mapping(result))

    def test_map_series_uses_roman_installment_hint_to_break_tie(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / "config").mkdir()
            config = load_config(root)
            client = MalClient(
                config,
                MalSecrets(
                    client_id="client-id",
                    client_secret=None,
                    access_token="access-token",
                    refresh_token=None,
                    client_id_path=root / "secrets" / "mal_client_id.txt",
                    client_secret_path=root / "secrets" / "mal_client_secret.txt",
                    access_token_path=root / "secrets" / "mal_access_token.txt",
                    refresh_token_path=root / "secrets" / "mal_refresh_token.txt",
                ),
            )

            with patch.object(
                MalClient,
                "search_anime",
                return_value={
                    "data": [
                        {
                            "node": {
                                "id": 300,
                                "title": "A Certain Magical Index II",
                                "alternative_titles": {"synonyms": []},
                                "media_type": "tv",
                                "status": "finished_airing",
                                "num_episodes": 24,
                            }
                        },
                        {
                            "node": {
                                "id": 400,
                                "title": "A Certain Magical Index III",
                                "alternative_titles": {"synonyms": []},
                                "media_type": "tv",
                                "status": "finished_airing",
                                "num_episodes": 26,
                            }
                        },
                    ]
                },
            ):
                result = map_series(
                    client,
                    SeriesMappingInput(
                        provider="crunchyroll",
                        provider_series_id="series-123",
                        title="A Certain Magical Index",
                        season_title="A Certain Magical Index III (English Dub)",
                        season_number=3,
                        max_episode_number=26,
                        completed_episode_count=26,
                    ),
                )

        self.assertEqual(result.status, "exact")
        self.assertEqual(result.chosen_candidate.mal_anime_id, 400)
        self.assertIn("roman_installment_match=roman:3", result.rationale)
        self.assertTrue(should_auto_approve_mapping(result))

    def test_build_search_queries_combines_generic_cour_title_with_base_title(self) -> None:
        queries = build_search_queries(
            SeriesMappingInput(
                provider="crunchyroll",
                provider_series_id="series-123",
                title="Example Show",
                season_title="2nd Cour",
                season_number=1,
            )
        )

        self.assertEqual(
            queries,
            [
                "2nd Cour",
                "Example Show 2nd Cour",
                "Example Show",
            ],
        )

    def test_map_series_uses_split_installment_match_for_part_vs_cour(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / "config").mkdir()
            config = load_config(root)
            client = MalClient(
                config,
                MalSecrets(
                    client_id="client-id",
                    client_secret=None,
                    access_token="access-token",
                    refresh_token=None,
                    client_id_path=root / "secrets" / "mal_client_id.txt",
                    client_secret_path=root / "secrets" / "mal_client_secret.txt",
                    access_token_path=root / "secrets" / "mal_access_token.txt",
                    refresh_token_path=root / "secrets" / "mal_refresh_token.txt",
                ),
            )

            with patch.object(
                MalClient,
                "search_anime",
                return_value={
                    "data": [
                        {
                            "node": {
                                "id": 500,
                                "title": "Example Show Final Season Part 1",
                                "alternative_titles": {"synonyms": []},
                                "media_type": "tv",
                                "status": "finished_airing",
                                "num_episodes": 12,
                            }
                        },
                        {
                            "node": {
                                "id": 600,
                                "title": "Example Show The Final Season 2nd Cour",
                                "alternative_titles": {"synonyms": []},
                                "media_type": "tv",
                                "status": "finished_airing",
                                "num_episodes": 12,
                            }
                        },
                    ]
                },
            ):
                result = map_series(
                    client,
                    SeriesMappingInput(
                        provider="crunchyroll",
                        provider_series_id="series-123",
                        title="Example Show",
                        season_title="Example Show Final Season Part 2 (English Dub)",
                        season_number=4,
                        max_episode_number=12,
                        completed_episode_count=12,
                    ),
                )

        self.assertEqual(result.chosen_candidate.mal_anime_id, 600)
        self.assertIn("split_installment_match=split:2", result.rationale)

    def test_map_series_prefers_title_season_hint_when_provider_metadata_is_noisy(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / "config").mkdir()
            config = load_config(root)
            client = MalClient(
                config,
                MalSecrets(
                    client_id="client-id",
                    client_secret=None,
                    access_token="access-token",
                    refresh_token=None,
                    client_id_path=root / "secrets" / "mal_client_id.txt",
                    client_secret_path=root / "secrets" / "mal_client_secret.txt",
                    access_token_path=root / "secrets" / "mal_access_token.txt",
                    refresh_token_path=root / "secrets" / "mal_refresh_token.txt",
                ),
            )

            with patch.object(
                MalClient,
                "search_anime",
                return_value={
                    "data": [
                        {
                            "node": {
                                "id": 700,
                                "title": "Example Show Season 2",
                                "alternative_titles": {"synonyms": []},
                                "media_type": "tv",
                                "status": "finished_airing",
                                "num_episodes": 12,
                            }
                        },
                        {
                            "node": {
                                "id": 800,
                                "title": "Example Show Season 3",
                                "alternative_titles": {"synonyms": []},
                                "media_type": "tv",
                                "status": "finished_airing",
                                "num_episodes": 12,
                            }
                        },
                    ]
                },
            ):
                result = map_series(
                    client,
                    SeriesMappingInput(
                        provider="crunchyroll",
                        provider_series_id="series-123",
                        title="Example Show",
                        season_title="Example Show Season 2",
                        season_number=3,
                        max_episode_number=12,
                        completed_episode_count=12,
                    ),
                )

        self.assertEqual(result.chosen_candidate.mal_anime_id, 700)
        self.assertIn("provider_season_metadata_conflict=metadata:3;title:2", result.rationale)

    def test_map_series_does_not_penalize_exact_movie_title_inside_collection(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / "config").mkdir()
            config = load_config(root)
            client = MalClient(
                config,
                MalSecrets(
                    client_id="client-id",
                    client_secret=None,
                    access_token="access-token",
                    refresh_token=None,
                    client_id_path=root / "secrets" / "mal_client_id.txt",
                    client_secret_path=root / "secrets" / "mal_client_secret.txt",
                    access_token_path=root / "secrets" / "mal_access_token.txt",
                    refresh_token_path=root / "secrets" / "mal_refresh_token.txt",
                ),
            )

            with patch.object(
                MalClient,
                "search_anime",
                return_value={
                    "data": [
                        {
                            "node": {
                                "id": 900,
                                "title": "Dragon Ball Super: Super Hero",
                                "alternative_titles": {"synonyms": ["Dragon Ball Super: Super Hero (English Dub)"]},
                                "media_type": "movie",
                                "status": "finished_airing",
                                "num_episodes": 1,
                            }
                        }
                    ]
                },
            ):
                result = map_series(
                    client,
                    SeriesMappingInput(
                        provider="crunchyroll",
                        provider_series_id="series-123",
                        title="Dragon Ball Movies",
                        season_title="Dragon Ball Super: Super Hero (English Dub)",
                        season_number=2115,
                        max_episode_number=1,
                        completed_episode_count=1,
                    ),
                )

        self.assertEqual(result.status, "exact")
        self.assertEqual(result.chosen_candidate.mal_anime_id, 900)
        self.assertIn("movie_type_allowed_for_exact_title", result.rationale)


class PersistedMappingTests(unittest.TestCase):
    def test_upsert_and_list_series_mappings(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / "config").mkdir()
            config = load_config(root)
            payload = sample_snapshot()
            ingest_snapshot_payload(payload, config)

            created = upsert_series_mapping(
                config.db_path,
                provider="crunchyroll",
                provider_series_id="series-123",
                mal_anime_id=321,
                confidence=0.99,
                mapping_source="user_approved",
                approved_by_user=True,
                notes="looked correct",
            )
            items = list_series_mappings(config.db_path, provider="crunchyroll", approved_only=True)

        self.assertEqual(created.mal_anime_id, 321)
        self.assertEqual(len(items), 1)
        self.assertTrue(items[0].approved_by_user)
        self.assertEqual(items[0].notes, "looked correct")


class DryRunPlannerTests(unittest.TestCase):
    def test_build_dry_run_sync_plan_proposes_forward_only_update(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / "config").mkdir()
            config = load_config(root)
            payload = sample_snapshot()
            payload["series"][0]["title"] = "Example Show"
            payload["series"][0]["season_title"] = "Example Show (English Dub)"
            payload["progress"][0]["episode_number"] = 3
            payload["progress"][0]["completion_ratio"] = 0.95
            ingest_snapshot_payload(payload, config)
            (root / "secrets").mkdir(exist_ok=True)
            (root / "secrets" / "mal_client_id.txt").write_text("client-id\n", encoding="utf-8")
            (root / "secrets" / "mal_access_token.txt").write_text("access-token\n", encoding="utf-8")

            with patch.object(
                MalClient,
                "search_anime",
                return_value={
                    "data": [
                        {
                            "node": {
                                "id": 123,
                                "title": "Example Show",
                                "alternative_titles": {"synonyms": ["Example Show (English Dub)"]},
                                "media_type": "tv",
                                "status": "finished_airing",
                                "num_episodes": 12,
                            }
                        }
                    ]
                },
            ), patch.object(
                MalClient,
                "get_anime_details",
                return_value={
                    "id": 123,
                    "title": "Example Show",
                    "num_episodes": 12,
                    "my_list_status": {"status": "watching", "num_episodes_watched": 1},
                },
            ):
                proposals = build_dry_run_sync_plan(config, limit=5, mapping_limit=3)

        self.assertEqual(len(proposals), 1)
        proposal = proposals[0]
        self.assertEqual(proposal.decision, "propose_update")
        self.assertEqual(proposal.proposed_my_list_status, {"status": "watching", "num_watched_episodes": 3})
        self.assertEqual(proposal.mapping_source, "auto_exact")
        self.assertTrue(proposal.persisted_mapping_approved)
        self.assertIn("preserve_meaningful_score", proposal.reasons)

    def test_build_dry_run_sync_plan_refuses_to_decrease_existing_progress(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / "config").mkdir()
            config = load_config(root)
            payload = sample_snapshot()
            payload["progress"][0]["completion_ratio"] = 0.95
            ingest_snapshot_payload(payload, config)
            (root / "secrets").mkdir(exist_ok=True)
            (root / "secrets" / "mal_client_id.txt").write_text("client-id\n", encoding="utf-8")
            (root / "secrets" / "mal_access_token.txt").write_text("access-token\n", encoding="utf-8")

            with patch.object(
                MalClient,
                "search_anime",
                return_value={
                    "data": [
                        {
                            "node": {
                                "id": 123,
                                "title": "Example Show",
                                "alternative_titles": {"synonyms": []},
                                "media_type": "tv",
                                "status": "finished_airing",
                                "num_episodes": 12,
                            }
                        }
                    ]
                },
            ), patch.object(
                MalClient,
                "get_anime_details",
                return_value={
                    "id": 123,
                    "title": "Example Show",
                    "num_episodes": 12,
                    "my_list_status": {"status": "watching", "num_episodes_watched": 5},
                },
            ):
                proposals = build_dry_run_sync_plan(config, limit=5, mapping_limit=3)

        self.assertEqual(proposals[0].decision, "skip")
        self.assertTrue(any("refusing_to_decrease_mal_progress" in reason for reason in proposals[0].reasons))

    def test_build_mapping_review_preserves_user_approved_mapping(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / "config").mkdir()
            config = load_config(root)
            payload = sample_snapshot()
            ingest_snapshot_payload(payload, config)
            (root / "secrets").mkdir(exist_ok=True)
            (root / "secrets" / "mal_client_id.txt").write_text("client-id\n", encoding="utf-8")
            (root / "secrets" / "mal_access_token.txt").write_text("access-token\n", encoding="utf-8")
            upsert_series_mapping(
                config.db_path,
                provider="crunchyroll",
                provider_series_id="series-123",
                mal_anime_id=777,
                confidence=1.0,
                mapping_source="user_approved",
                approved_by_user=True,
                notes="manual approval",
            )

            with patch.object(MalClient, "search_anime", side_effect=AssertionError("should not search approved mapping")):
                items = build_mapping_review(config, limit=5, mapping_limit=3)

        self.assertEqual(len(items), 1)
        self.assertEqual(items[0].decision, "preserved")
        self.assertEqual(items[0].suggested_mal_anime_id, 777)
        self.assertEqual(items[0].mapping_status, "approved")

    def test_build_mapping_review_auto_approves_exact_unique_match(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / "config").mkdir()
            config = load_config(root)
            payload = sample_snapshot()
            payload["series"][0]["title"] = "Example Show"
            payload["series"][0]["season_title"] = "Example Show Season 2 (English Dub)"
            payload["series"][0]["season_number"] = 2
            payload["progress"][0]["episode_number"] = 12
            payload["progress"][0]["completion_ratio"] = 0.95
            ingest_snapshot_payload(payload, config)
            (root / "secrets").mkdir(exist_ok=True)
            (root / "secrets" / "mal_client_id.txt").write_text("client-id\n", encoding="utf-8")
            (root / "secrets" / "mal_access_token.txt").write_text("access-token\n", encoding="utf-8")

            with patch.object(
                MalClient,
                "search_anime",
                return_value={
                    "data": [
                        {
                            "node": {
                                "id": 222,
                                "title": "Example Show Season 2",
                                "alternative_titles": {"synonyms": ["Example Show Season 2 (English Dub)"]},
                                "media_type": "tv",
                                "status": "finished_airing",
                                "num_episodes": 12,
                            }
                        },
                        {"node": {"id": 999, "title": "Different Show", "alternative_titles": {}, "media_type": "tv"}},
                    ]
                },
            ):
                items = build_mapping_review(config, limit=5, mapping_limit=3)
                persisted = list_series_mappings(config.db_path, provider="crunchyroll", approved_only=True)

        self.assertEqual(len(items), 1)
        self.assertEqual(items[0].decision, "auto_approved")
        self.assertEqual(items[0].mapping_status, "approved")
        self.assertTrue(any(reason == "auto_approved_exact_unique_match" for reason in items[0].reasons))
        self.assertEqual(len(persisted), 1)
        self.assertEqual(persisted[0].mal_anime_id, 222)
        self.assertEqual(persisted[0].mapping_source, "auto_exact")

    def test_build_dry_run_sync_plan_uses_user_approved_mapping_without_search(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / "config").mkdir()
            config = load_config(root)
            payload = sample_snapshot()
            payload["progress"][0]["episode_number"] = 2
            payload["progress"][0]["completion_ratio"] = 0.95
            ingest_snapshot_payload(payload, config)
            (root / "secrets").mkdir(exist_ok=True)
            (root / "secrets" / "mal_client_id.txt").write_text("client-id\n", encoding="utf-8")
            (root / "secrets" / "mal_access_token.txt").write_text("access-token\n", encoding="utf-8")
            upsert_series_mapping(
                config.db_path,
                provider="crunchyroll",
                provider_series_id="series-123",
                mal_anime_id=555,
                confidence=1.0,
                mapping_source="user_approved",
                approved_by_user=True,
                notes=None,
            )

            with patch.object(MalClient, "search_anime", side_effect=AssertionError("should not search approved mapping")), patch.object(
                MalClient,
                "get_anime_details",
                return_value={
                    "id": 555,
                    "title": "Approved Show",
                    "num_episodes": 12,
                    "my_list_status": {"status": "watching", "num_episodes_watched": 0},
                },
            ):
                proposals = build_dry_run_sync_plan(config, limit=5, mapping_limit=3)

        self.assertEqual(len(proposals), 1)
        self.assertEqual(proposals[0].mal_anime_id, 555)
        self.assertTrue(proposals[0].persisted_mapping_approved)
        self.assertEqual(proposals[0].mapping_status, "approved")
        self.assertEqual(proposals[0].decision, "propose_update")

    def test_build_dry_run_sync_plan_auto_approves_exact_unique_match_for_sync(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / "config").mkdir()
            config = load_config(root)
            payload = sample_snapshot()
            payload["series"][0]["title"] = "Example Show"
            payload["series"][0]["season_title"] = "Example Show Season 2 (English Dub)"
            payload["series"][0]["season_number"] = 2
            payload["progress"][0]["episode_number"] = 12
            payload["progress"][0]["completion_ratio"] = 0.95
            ingest_snapshot_payload(payload, config)
            (root / "secrets").mkdir(exist_ok=True)
            (root / "secrets" / "mal_client_id.txt").write_text("client-id\n", encoding="utf-8")
            (root / "secrets" / "mal_access_token.txt").write_text("access-token\n", encoding="utf-8")

            with patch.object(
                MalClient,
                "search_anime",
                return_value={
                    "data": [
                        {
                            "node": {
                                "id": 333,
                                "title": "Example Show Season 2",
                                "alternative_titles": {"synonyms": ["Example Show Season 2 (English Dub)"]},
                                "media_type": "tv",
                                "status": "finished_airing",
                                "num_episodes": 12,
                            }
                        },
                        {"node": {"id": 999, "title": "Different Show", "alternative_titles": {}, "media_type": "tv"}},
                    ]
                },
            ), patch.object(
                MalClient,
                "get_anime_details",
                return_value={
                    "id": 333,
                    "title": "Example Show Season 2",
                    "num_episodes": 12,
                    "my_list_status": {"status": "watching", "num_episodes_watched": 0},
                },
            ):
                proposals = build_dry_run_sync_plan(config, limit=5, mapping_limit=3)
                persisted = list_series_mappings(config.db_path, provider="crunchyroll", approved_only=True)

        self.assertEqual(len(proposals), 1)
        self.assertEqual(proposals[0].mapping_status, "approved")
        self.assertTrue(proposals[0].persisted_mapping_approved)
        self.assertEqual(proposals[0].mapping_source, "auto_exact")
        self.assertTrue(any(reason == "auto_approved_exact_unique_match" for reason in proposals[0].reasons))
        self.assertEqual(len(persisted), 1)
        self.assertEqual(persisted[0].mal_anime_id, 333)

    def test_build_dry_run_sync_plan_can_require_approved_mappings_only(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / "config").mkdir()
            config = load_config(root)
            payload = sample_snapshot()
            ingest_snapshot_payload(payload, config)
            (root / "secrets").mkdir(exist_ok=True)
            (root / "secrets" / "mal_client_id.txt").write_text("client-id\n", encoding="utf-8")
            (root / "secrets" / "mal_access_token.txt").write_text("access-token\n", encoding="utf-8")

            with patch.object(MalClient, "search_anime", side_effect=AssertionError("approved-only should not live search")):
                proposals = build_dry_run_sync_plan(config, limit=5, mapping_limit=3, approved_mappings_only=True)

        self.assertEqual(len(proposals), 1)
        self.assertEqual(proposals[0].decision, "review")
        self.assertTrue(any(reason == "approved_mappings_only_enabled" for reason in proposals[0].reasons))

    def test_persist_mapping_review_queue_only_keeps_unresolved_items(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / "config").mkdir()
            config = load_config(root)
            payload = sample_snapshot()
            ingest_snapshot_payload(payload, config)
            (root / "secrets").mkdir(exist_ok=True)
            (root / "secrets" / "mal_client_id.txt").write_text("client-id\n", encoding="utf-8")
            (root / "secrets" / "mal_access_token.txt").write_text("access-token\n", encoding="utf-8")

            with patch.object(MalClient, "search_anime", return_value={"data": []}):
                items = build_mapping_review(config, limit=5, mapping_limit=3)
            persist_mapping_review_queue(config, items)
            rows = list_review_queue_entries(config.db_path, status="open", issue_type="mapping_review")

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].severity, "error")
        self.assertEqual(rows[0].payload["decision"], "needs_manual_match")

    def test_persist_sync_review_queue_keeps_review_and_skip_rows(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / "config").mkdir()
            config = load_config(root)
            payload = sample_snapshot()
            payload["progress"][0]["completion_ratio"] = 0.95
            ingest_snapshot_payload(payload, config)
            (root / "secrets").mkdir(exist_ok=True)
            (root / "secrets" / "mal_client_id.txt").write_text("client-id\n", encoding="utf-8")
            (root / "secrets" / "mal_access_token.txt").write_text("access-token\n", encoding="utf-8")

            with patch.object(
                MalClient,
                "search_anime",
                return_value={
                    "data": [
                        {
                            "node": {
                                "id": 123,
                                "title": "Example Show",
                                "alternative_titles": {"synonyms": []},
                                "media_type": "tv",
                                "status": "finished_airing",
                                "num_episodes": 12,
                            }
                        }
                    ]
                },
            ), patch.object(
                MalClient,
                "get_anime_details",
                return_value={
                    "id": 123,
                    "title": "Example Show",
                    "num_episodes": 12,
                    "my_list_status": {"status": "completed", "num_episodes_watched": 12},
                },
            ):
                proposals = build_dry_run_sync_plan(config, limit=5, mapping_limit=3)
            persist_sync_review_queue(config, proposals)
            rows = list_review_queue_entries(config.db_path, status="open", issue_type="sync_review")

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].payload["decision"], "skip")

    def test_build_dry_run_sync_plan_fills_missing_finish_date_only_when_completed(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / "config").mkdir()
            config = load_config(root)
            payload = sample_snapshot()
            payload["progress"][0]["episode_number"] = 12
            payload["progress"][0]["completion_ratio"] = 0.95
            payload["progress"][0]["last_watched_at"] = "2026-03-14T22:10:00Z"
            ingest_snapshot_payload(payload, config)
            (root / "secrets").mkdir(exist_ok=True)
            (root / "secrets" / "mal_client_id.txt").write_text("client-id\n", encoding="utf-8")
            (root / "secrets" / "mal_access_token.txt").write_text("access-token\n", encoding="utf-8")

            with patch.object(
                MalClient,
                "search_anime",
                return_value={
                    "data": [
                        {
                            "node": {
                                "id": 123,
                                "title": "Example Show",
                                "alternative_titles": {"synonyms": []},
                                "media_type": "tv",
                                "status": "finished_airing",
                                "num_episodes": 12,
                            }
                        }
                    ]
                },
            ), patch.object(
                MalClient,
                "get_anime_details",
                return_value={
                    "id": 123,
                    "title": "Example Show",
                    "num_episodes": 12,
                    "my_list_status": {"status": "completed", "num_episodes_watched": 12, "finish_date": None},
                },
            ):
                proposals = build_dry_run_sync_plan(config, limit=5, mapping_limit=3)

        self.assertEqual(proposals[0].decision, "propose_update")
        self.assertEqual(
            proposals[0].proposed_my_list_status,
            {"status": "completed", "num_watched_episodes": 12, "finish_date": "2026-03-14"},
        )
        self.assertIn("fill_missing_finish_date", proposals[0].reasons)
        self.assertIn("preserve_meaningful_start_date", proposals[0].reasons)

    def test_build_dry_run_sync_plan_preserves_existing_finish_date(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / "config").mkdir()
            config = load_config(root)
            payload = sample_snapshot()
            payload["progress"][0]["episode_number"] = 12
            payload["progress"][0]["completion_ratio"] = 0.95
            payload["progress"][0]["last_watched_at"] = "2026-03-14T22:10:00Z"
            ingest_snapshot_payload(payload, config)
            (root / "secrets").mkdir(exist_ok=True)
            (root / "secrets" / "mal_client_id.txt").write_text("client-id\n", encoding="utf-8")
            (root / "secrets" / "mal_access_token.txt").write_text("access-token\n", encoding="utf-8")

            with patch.object(
                MalClient,
                "search_anime",
                return_value={
                    "data": [
                        {
                            "node": {
                                "id": 123,
                                "title": "Example Show",
                                "alternative_titles": {"synonyms": []},
                                "media_type": "tv",
                                "status": "finished_airing",
                                "num_episodes": 12,
                            }
                        }
                    ]
                },
            ), patch.object(
                MalClient,
                "get_anime_details",
                return_value={
                    "id": 123,
                    "title": "Example Show",
                    "num_episodes": 12,
                    "my_list_status": {"status": "completed", "num_episodes_watched": 12, "finish_date": "2025-01-01"},
                },
            ):
                proposals = build_dry_run_sync_plan(config, limit=5, mapping_limit=3)

        self.assertEqual(proposals[0].decision, "skip")
        self.assertTrue(any(reason == "mal_already_matches_or_exceeds_proposal" for reason in proposals[0].reasons))

    def test_build_dry_run_sync_plan_preserves_meaningful_zero_progress_on_plan_to_watch(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / "config").mkdir()
            config = load_config(root)
            payload = sample_snapshot()
            payload["progress"] = []
            ingest_snapshot_payload(payload, config)
            (root / "secrets").mkdir(exist_ok=True)
            (root / "secrets" / "mal_client_id.txt").write_text("client-id\n", encoding="utf-8")
            (root / "secrets" / "mal_access_token.txt").write_text("access-token\n", encoding="utf-8")

            with patch.object(
                MalClient,
                "search_anime",
                return_value={
                    "data": [
                        {
                            "node": {
                                "id": 123,
                                "title": "Example Show",
                                "alternative_titles": {"synonyms": []},
                                "media_type": "tv",
                                "status": "finished_airing",
                                "num_episodes": 12,
                            }
                        }
                    ]
                },
            ), patch.object(
                MalClient,
                "get_anime_details",
                return_value={
                    "id": 123,
                    "title": "Example Show",
                    "num_episodes": 12,
                    "my_list_status": {"status": "plan_to_watch", "num_episodes_watched": 0},
                },
            ):
                proposals = build_dry_run_sync_plan(config, limit=5, mapping_limit=3)

        self.assertEqual(proposals[0].decision, "skip")
        self.assertIn("mal_already_matches_or_exceeds_proposal", proposals[0].reasons)

    def test_build_dry_run_sync_plan_overrides_plan_to_watch_when_crunchyroll_has_completed_episode_evidence(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / "config").mkdir()
            config = load_config(root)
            payload = sample_snapshot()
            payload["progress"][0]["episode_number"] = 2
            payload["progress"][0]["completion_ratio"] = 0.95
            ingest_snapshot_payload(payload, config)
            (root / "secrets").mkdir(exist_ok=True)
            (root / "secrets" / "mal_client_id.txt").write_text("client-id\n", encoding="utf-8")
            (root / "secrets" / "mal_access_token.txt").write_text("access-token\n", encoding="utf-8")

            with patch.object(
                MalClient,
                "search_anime",
                return_value={
                    "data": [
                        {
                            "node": {
                                "id": 123,
                                "title": "Example Show",
                                "alternative_titles": {"synonyms": []},
                                "media_type": "tv",
                                "status": "finished_airing",
                                "num_episodes": 12,
                            }
                        }
                    ]
                },
            ), patch.object(
                MalClient,
                "get_anime_details",
                return_value={
                    "id": 123,
                    "title": "Example Show",
                    "num_episodes": 12,
                    "my_list_status": {"status": "plan_to_watch", "num_episodes_watched": 0},
                },
            ):
                proposals = build_dry_run_sync_plan(config, limit=5, mapping_limit=3)

        self.assertEqual(proposals[0].decision, "propose_update")
        self.assertEqual(proposals[0].proposed_my_list_status, {"status": "watching", "num_watched_episodes": 2})
        self.assertIn("override_plan_to_watch_due_to_crunchyroll_watch_evidence", proposals[0].reasons)

    def test_build_dry_run_sync_plan_suppresses_watching_zero_episode_proposals(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / "config").mkdir()
            config = load_config(root)
            payload = sample_snapshot()
            payload["progress"][0]["completion_ratio"] = 0.40
            payload["progress"][0]["episode_number"] = 1
            ingest_snapshot_payload(payload, config)
            (root / "secrets").mkdir(exist_ok=True)
            (root / "secrets" / "mal_client_id.txt").write_text("client-id\n", encoding="utf-8")
            (root / "secrets" / "mal_access_token.txt").write_text("access-token\n", encoding="utf-8")

            with patch.object(
                MalClient,
                "search_anime",
                return_value={
                    "data": [
                        {
                            "node": {
                                "id": 123,
                                "title": "Example Show",
                                "alternative_titles": {"synonyms": []},
                                "media_type": "tv",
                                "status": "finished_airing",
                                "num_episodes": 12,
                            }
                        }
                    ]
                },
            ), patch.object(
                MalClient,
                "get_anime_details",
                return_value={
                    "id": 123,
                    "title": "Example Show",
                    "num_episodes": 12,
                    "my_list_status": None,
                },
            ):
                proposals = build_dry_run_sync_plan(config, limit=5, mapping_limit=3)

        self.assertEqual(proposals[0].decision, "skip")
        self.assertIsNone(proposals[0].proposed_my_list_status)
        self.assertIn("partial_crunchyroll_activity_without_completed_episode", proposals[0].reasons)
        self.assertIn("no_actionable_crunchyroll_state", proposals[0].reasons)

    def test_build_dry_run_sync_plan_counts_follow_on_near_complete_episode_as_watched(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / "config").mkdir()
            config = load_config(root)
            payload = sample_snapshot()
            payload["progress"] = [
                {
                    **payload["progress"][0],
                    "provider_episode_id": "episode-1",
                    "episode_number": 1,
                    "playback_position_ms": 1322000,
                    "duration_ms": 1440024,
                    "completion_ratio": 0.9180402548846408,
                    "last_watched_at": "2026-03-14T20:00:00Z",
                },
                {
                    **payload["progress"][0],
                    "provider_episode_id": "episode-2",
                    "episode_number": 2,
                    "playback_position_ms": 1440000,
                    "duration_ms": 1440066,
                    "completion_ratio": 0.9999541687672648,
                    "last_watched_at": "2026-03-14T20:30:00Z",
                },
            ]
            ingest_snapshot_payload(payload, config)
            (root / "secrets").mkdir(exist_ok=True)
            (root / "secrets" / "mal_client_id.txt").write_text("client-id\n", encoding="utf-8")
            (root / "secrets" / "mal_access_token.txt").write_text("access-token\n", encoding="utf-8")

            with patch.object(
                MalClient,
                "search_anime",
                return_value={
                    "data": [
                        {
                            "node": {
                                "id": 123,
                                "title": "Example Show",
                                "alternative_titles": {"synonyms": []},
                                "media_type": "tv",
                                "status": "finished_airing",
                                "num_episodes": 12,
                            }
                        }
                    ]
                },
            ), patch.object(
                MalClient,
                "get_anime_details",
                return_value={
                    "id": 123,
                    "title": "Example Show",
                    "num_episodes": 12,
                    "my_list_status": {"status": "watching", "num_episodes_watched": 0},
                },
            ):
                proposals = build_dry_run_sync_plan(config, limit=5, mapping_limit=3)

        self.assertEqual(proposals[0].decision, "propose_update")
        self.assertEqual(proposals[0].proposed_my_list_status, {"status": "watching", "num_watched_episodes": 2})
        self.assertIn("completion_policy=ratio>=0.95_or_remaining<=120s_or_later_episode_progress_with_ratio>=0.85", proposals[0].reasons)

    def test_build_dry_run_sync_plan_counts_last_episode_within_credits_window_as_completed(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / "config").mkdir()
            config = load_config(root)
            payload = sample_snapshot()
            payload["progress"][0]["episode_number"] = 12
            payload["progress"][0]["playback_position_ms"] = 1322000
            payload["progress"][0]["duration_ms"] = 1440024
            payload["progress"][0]["completion_ratio"] = 0.9180402548846408
            ingest_snapshot_payload(payload, config)
            (root / "secrets").mkdir(exist_ok=True)
            (root / "secrets" / "mal_client_id.txt").write_text("client-id\n", encoding="utf-8")
            (root / "secrets" / "mal_access_token.txt").write_text("access-token\n", encoding="utf-8")

            with patch.object(
                MalClient,
                "search_anime",
                return_value={
                    "data": [
                        {
                            "node": {
                                "id": 123,
                                "title": "Example Show",
                                "alternative_titles": {"synonyms": []},
                                "media_type": "tv",
                                "status": "finished_airing",
                                "num_episodes": 12,
                            }
                        }
                    ]
                },
            ), patch.object(
                MalClient,
                "get_anime_details",
                return_value={
                    "id": 123,
                    "title": "Example Show",
                    "num_episodes": 12,
                    "my_list_status": {"status": "watching", "num_episodes_watched": 11, "finish_date": None},
                },
            ):
                proposals = build_dry_run_sync_plan(config, limit=5, mapping_limit=3)

        self.assertEqual(proposals[0].decision, "propose_update")
        self.assertEqual(
            proposals[0].proposed_my_list_status,
            {"status": "completed", "num_watched_episodes": 12, "finish_date": "2026-03-14"},
        )

    def test_build_dry_run_sync_plan_leaves_ambiguous_near_complete_episode_incomplete(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / "config").mkdir()
            config = load_config(root)
            payload = sample_snapshot()
            payload["progress"] = [
                {
                    **payload["progress"][0],
                    "provider_episode_id": "episode-6",
                    "episode_number": 6,
                    "playback_position_ms": 1440000,
                    "duration_ms": 1440066,
                    "completion_ratio": 0.9999541687672648,
                    "last_watched_at": "2026-03-14T19:00:00Z",
                },
                {
                    **payload["progress"][0],
                    "provider_episode_id": "episode-7",
                    "episode_number": 7,
                    "playback_position_ms": 1280000,
                    "duration_ms": 1420046,
                    "completion_ratio": 0.9013792510946829,
                    "last_watched_at": "2026-03-14T20:00:00Z",
                },
            ]
            ingest_snapshot_payload(payload, config)
            (root / "secrets").mkdir(exist_ok=True)
            (root / "secrets" / "mal_client_id.txt").write_text("client-id\n", encoding="utf-8")
            (root / "secrets" / "mal_access_token.txt").write_text("access-token\n", encoding="utf-8")

            with patch.object(
                MalClient,
                "search_anime",
                return_value={
                    "data": [
                        {
                            "node": {
                                "id": 123,
                                "title": "Example Show",
                                "alternative_titles": {"synonyms": []},
                                "media_type": "tv",
                                "status": "finished_airing",
                                "num_episodes": 12,
                            }
                        }
                    ]
                },
            ), patch.object(
                MalClient,
                "get_anime_details",
                return_value={
                    "id": 123,
                    "title": "Example Show",
                    "num_episodes": 12,
                    "my_list_status": {"status": "watching", "num_episodes_watched": 0},
                },
            ):
                proposals = build_dry_run_sync_plan(config, limit=5, mapping_limit=3)

        self.assertEqual(proposals[0].decision, "propose_update")
        self.assertEqual(proposals[0].proposed_my_list_status, {"status": "watching", "num_watched_episodes": 6})

    def test_build_dry_run_sync_plan_deduplicates_alternate_episode_variants_by_episode_number(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / "config").mkdir()
            config = load_config(root)
            payload = sample_snapshot()
            payload["progress"] = [
                {
                    **payload["progress"][0],
                    "provider_episode_id": "episode-1-dub-a",
                    "episode_number": 1,
                    "playback_position_ms": 1440000,
                    "duration_ms": 1440066,
                    "completion_ratio": 0.9999541687672648,
                    "last_watched_at": "2026-03-14T18:00:00Z",
                },
                {
                    **payload["progress"][0],
                    "provider_episode_id": "episode-1-dub-b",
                    "episode_number": 1,
                    "playback_position_ms": 1440000,
                    "duration_ms": 1440066,
                    "completion_ratio": 0.9999541687672648,
                    "last_watched_at": "2026-03-14T18:05:00Z",
                },
                {
                    **payload["progress"][0],
                    "provider_episode_id": "episode-2-dub-a",
                    "episode_number": 2,
                    "playback_position_ms": 1440000,
                    "duration_ms": 1440066,
                    "completion_ratio": 0.9999541687672648,
                    "last_watched_at": "2026-03-14T18:30:00Z",
                },
            ]
            ingest_snapshot_payload(payload, config)
            (root / "secrets").mkdir(exist_ok=True)
            (root / "secrets" / "mal_client_id.txt").write_text("client-id\n", encoding="utf-8")
            (root / "secrets" / "mal_access_token.txt").write_text("access-token\n", encoding="utf-8")

            with patch.object(
                MalClient,
                "search_anime",
                return_value={
                    "data": [
                        {
                            "node": {
                                "id": 123,
                                "title": "Example Show",
                                "alternative_titles": {"synonyms": []},
                                "media_type": "tv",
                                "status": "finished_airing",
                                "num_episodes": 12,
                            }
                        }
                    ]
                },
            ), patch.object(
                MalClient,
                "get_anime_details",
                return_value={
                    "id": 123,
                    "title": "Example Show",
                    "num_episodes": 12,
                    "my_list_status": {"status": "watching", "num_episodes_watched": 0},
                },
            ):
                proposals = build_dry_run_sync_plan(config, limit=5, mapping_limit=3)

        self.assertEqual(proposals[0].decision, "propose_update")
        self.assertEqual(proposals[0].proposed_my_list_status, {"status": "watching", "num_watched_episodes": 2})

    def test_execute_approved_sync_dry_run_only_targets_approved_safe_updates(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / "config").mkdir()
            config = load_config(root)
            payload = sample_snapshot()
            payload["progress"][0]["episode_number"] = 4
            payload["progress"][0]["completion_ratio"] = 0.95
            ingest_snapshot_payload(payload, config)
            (root / "secrets").mkdir(exist_ok=True)
            (root / "secrets" / "mal_client_id.txt").write_text("client-id\n", encoding="utf-8")
            (root / "secrets" / "mal_access_token.txt").write_text("access-token\n", encoding="utf-8")
            upsert_series_mapping(
                config.db_path,
                provider="crunchyroll",
                provider_series_id="series-123",
                mal_anime_id=888,
                confidence=1.0,
                mapping_source="user_approved",
                approved_by_user=True,
                notes=None,
            )

            with patch.object(
                MalClient,
                "get_anime_details",
                return_value={
                    "id": 888,
                    "title": "Approved Show",
                    "num_episodes": 12,
                    "my_list_status": {"status": "watching", "num_episodes_watched": 2, "score": 9},
                },
            ), patch.object(MalClient, "update_my_list_status", side_effect=AssertionError("dry-run should not write")):
                results = execute_approved_sync(config, limit=5, dry_run=True)

        self.assertEqual(len(results), 1)
        self.assertFalse(results[0].applied)
        self.assertEqual(results[0].proposal_decision, "propose_update")
        self.assertEqual(results[0].requested_status, {"status": "watching", "num_watched_episodes": 4})
        self.assertIn("executor_dry_run", results[0].reasons)

    def test_execute_approved_sync_performs_live_write_when_safe(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / "config").mkdir()
            config = load_config(root)
            payload = sample_snapshot()
            payload["progress"][0]["episode_number"] = 4
            payload["progress"][0]["completion_ratio"] = 0.95
            ingest_snapshot_payload(payload, config)
            (root / "secrets").mkdir(exist_ok=True)
            (root / "secrets" / "mal_client_id.txt").write_text("client-id\n", encoding="utf-8")
            (root / "secrets" / "mal_access_token.txt").write_text("access-token\n", encoding="utf-8")
            upsert_series_mapping(
                config.db_path,
                provider="crunchyroll",
                provider_series_id="series-123",
                mal_anime_id=888,
                confidence=1.0,
                mapping_source="user_approved",
                approved_by_user=True,
                notes=None,
            )

            with patch.object(
                MalClient,
                "get_anime_details",
                return_value={
                    "id": 888,
                    "title": "Approved Show",
                    "num_episodes": 12,
                    "my_list_status": {"status": "watching", "num_episodes_watched": 2, "score": 9},
                },
            ), patch.object(
                MalClient,
                "update_my_list_status",
                return_value={"status": "watching", "num_episodes_watched": 4, "score": 9},
            ) as update_mock:
                results = execute_approved_sync(config, limit=5, dry_run=False)

        self.assertEqual(len(results), 1)
        self.assertTrue(results[0].applied)
        update_mock.assert_called_once_with(888, status="watching", num_watched_episodes=4, score=None, start_date=None, finish_date=None)
        self.assertEqual(results[0].response_status["score"], 9)

    def test_execute_approved_sync_includes_missing_finish_date_when_safe(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / "config").mkdir()
            config = load_config(root)
            payload = sample_snapshot()
            payload["progress"][0]["episode_number"] = 12
            payload["progress"][0]["completion_ratio"] = 0.95
            payload["progress"][0]["last_watched_at"] = "2026-03-14T22:10:00Z"
            ingest_snapshot_payload(payload, config)
            (root / "secrets").mkdir(exist_ok=True)
            (root / "secrets" / "mal_client_id.txt").write_text("client-id\n", encoding="utf-8")
            (root / "secrets" / "mal_access_token.txt").write_text("access-token\n", encoding="utf-8")
            upsert_series_mapping(
                config.db_path,
                provider="crunchyroll",
                provider_series_id="series-123",
                mal_anime_id=888,
                confidence=1.0,
                mapping_source="user_approved",
                approved_by_user=True,
                notes=None,
            )

            with patch.object(
                MalClient,
                "get_anime_details",
                return_value={
                    "id": 888,
                    "title": "Approved Show",
                    "num_episodes": 12,
                    "my_list_status": {"status": "completed", "num_episodes_watched": 12, "finish_date": None, "score": 9},
                },
            ), patch.object(
                MalClient,
                "update_my_list_status",
                return_value={"status": "completed", "num_episodes_watched": 12, "finish_date": "2026-03-14", "score": 9},
            ) as update_mock:
                results = execute_approved_sync(config, limit=5, dry_run=False)

        self.assertTrue(results[0].applied)
        update_mock.assert_called_once_with(
            888,
            status="completed",
            num_watched_episodes=12,
            score=None,
            start_date=None,
            finish_date="2026-03-14",
        )
        self.assertEqual(results[0].requested_status["finish_date"], "2026-03-14")

    def test_execute_approved_sync_skips_non_forward_safe_completed_downgrade(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / "config").mkdir()
            config = load_config(root)
            payload = sample_snapshot()
            payload["progress"][0]["episode_number"] = 3
            payload["progress"][0]["completion_ratio"] = 0.95
            ingest_snapshot_payload(payload, config)
            (root / "secrets").mkdir(exist_ok=True)
            (root / "secrets" / "mal_client_id.txt").write_text("client-id\n", encoding="utf-8")
            (root / "secrets" / "mal_access_token.txt").write_text("access-token\n", encoding="utf-8")
            upsert_series_mapping(
                config.db_path,
                provider="crunchyroll",
                provider_series_id="series-123",
                mal_anime_id=888,
                confidence=1.0,
                mapping_source="user_approved",
                approved_by_user=True,
                notes=None,
            )

            with patch.object(
                MalClient,
                "get_anime_details",
                return_value={
                    "id": 888,
                    "title": "Approved Show",
                    "num_episodes": 12,
                    "my_list_status": {"status": "completed", "num_episodes_watched": 12},
                },
            ), patch.object(MalClient, "update_my_list_status", side_effect=AssertionError("unsafe proposal should not write")):
                results = execute_approved_sync(config, limit=5, dry_run=False)

        self.assertEqual(len(results), 1)
        self.assertFalse(results[0].applied)
        self.assertEqual(results[0].proposal_decision, "skip")
        self.assertTrue(any("refusing_to_decrease_mal_progress" in reason or "refusing_to_downgrade_completed_mal_entry" in reason for reason in results[0].reasons))


if __name__ == "__main__":
    unittest.main()
