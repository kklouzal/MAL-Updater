from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from mal_updater.config import MalSecrets, load_config
from mal_updater.db import list_series_mappings, upsert_series_mapping
from mal_updater.ingestion import ingest_snapshot_payload
from mal_updater.mal_client import MalClient
from mal_updater.mapping import SeriesMappingInput, map_series, normalize_title
from mal_updater.sync_planner import build_dry_run_sync_plan, build_mapping_review
from tests.test_validation_ingestion import sample_snapshot


class MappingTests(unittest.TestCase):
    def test_normalize_title_strips_dub_and_season_noise(self) -> None:
        self.assertEqual(
            normalize_title("BOFURI: I Don’t Want to Get Hurt, so I’ll Max Out My Defense. Season 2 (English Dub)"),
            "bofuri i don t want to get hurt so i ll max out my defense",
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
        self.assertEqual(proposal.mapping_source, "live_search")

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


if __name__ == "__main__":
    unittest.main()
