from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from mal_updater.config import MalSecrets, load_config
from mal_updater.db import list_review_queue_entries, list_series_mappings, upsert_series_mapping
from mal_updater.ingestion import ingest_snapshot_payload
from mal_updater.mal_client import MalClient
from mal_updater.mapping import SeriesMappingInput, map_series, normalize_title
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
