from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from mal_updater.config import load_config
from mal_updater.ingestion import ingest_snapshot_payload
from mal_updater.mapping import SeriesMappingInput, map_series, normalize_title
from mal_updater.config import MalSecrets
from mal_updater.mal_client import MalClient
from mal_updater.sync_planner import build_dry_run_sync_plan
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
                        provider_series_id="series-1",
                        title="Attack on Titan",
                        season_title="Attack on Titan Final Season (English Dub)",
                    ),
                )

        self.assertEqual(result.status, "exact")
        self.assertIsNotNone(result.chosen_candidate)
        self.assertEqual(result.chosen_candidate.mal_anime_id, 42)


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


if __name__ == "__main__":
    unittest.main()
