from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from mal_updater.config import load_config
from mal_updater.db import (
    bootstrap_database,
    connect,
    get_mal_user_anime_list_cache,
    replace_mal_user_anime_list_cache_generation,
    upsert_mal_anime_detail_cache,
    upsert_series_mapping,
)
from mal_updater.ingestion import ingest_snapshot_payload
from mal_updater.mal_client import MAL_DETAIL_CACHE_LOGIC_VERSION, MalApiError, MalClient
from mal_updater.sync_planner import execute_approved_sync
from tests.test_validation_ingestion import sample_snapshot


class SyncExecutorStaleMalStateTests(unittest.TestCase):
    def _setup_sync(self, root: Path, *, provider_episode: int = 9, mal_anime_id: int = 53590):
        (root / ".MAL-Updater" / "config").mkdir(parents=True)
        config = load_config(root)
        bootstrap_database(config.db_path)
        payload = sample_snapshot()
        payload["progress"][0]["episode_number"] = provider_episode
        payload["progress"][0]["completion_ratio"] = 0.95
        ingest_snapshot_payload(payload, config)
        secrets = root / ".MAL-Updater" / "secrets"
        secrets.mkdir(parents=True, exist_ok=True)
        (secrets / "mal_client_id.txt").write_text("client-id\n", encoding="utf-8")
        (secrets / "mal_access_token.txt").write_text("access-token\n", encoding="utf-8")
        upsert_series_mapping(
            config.db_path,
            provider="crunchyroll",
            provider_series_id="series-123",
            mal_anime_id=mal_anime_id,
            confidence=1.0,
            mapping_source="user_exact",
            approved_by_user=True,
            notes=None,
        )
        return config

    @staticmethod
    def _detail(my_list_status, *, mal_anime_id: int = 53590):
        return {
            "id": mal_anime_id,
            "title": "Production-shaped Show",
            "num_episodes": 12,
            "media_type": "tv",
            "status": "currently_airing",
            "my_list_status": my_list_status,
            "alternative_titles": {},
        }

    def test_live_apply_revalidates_stale_cached_null_and_skips_when_mal_is_ahead(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            config = self._setup_sync(Path(td), provider_episode=9)
            stale_detail = self._detail(None)
            live_detail = self._detail({"status": "watching", "num_episodes_watched": 10, "score": 8})

            def details_for_cache_mode(*_args, **kwargs):
                return live_detail if kwargs.get("force_refresh") else stale_detail

            with patch.object(MalClient, "get_anime_details", side_effect=details_for_cache_mode) as details, patch.object(
                MalClient,
                "update_my_list_status",
                side_effect=AssertionError("authoritative MAL progress is ahead; no write is allowed"),
            ) as update:
                results = execute_approved_sync(config, limit=0, exact_approved_only=True, dry_run=False)

            details.assert_called_once()
            self.assertTrue(details.call_args.kwargs["force_refresh"])
            self.assertTrue(details.call_args.kwargs["require_user"])
            update.assert_not_called()
            self.assertEqual(1, len(results))
            self.assertFalse(results[0].applied)
            self.assertEqual("skip", results[0].proposal_decision)
            self.assertTrue(any("refusing_to_decrease_mal_progress" in reason for reason in results[0].reasons))

    def test_live_apply_fails_closed_when_authoritative_refresh_fails(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            config = self._setup_sync(Path(td), provider_episode=9)

            def unavailable_when_live(*_args, **kwargs):
                if kwargs.get("force_refresh"):
                    raise MalApiError("live MAL read unavailable")
                return self._detail(None)

            with patch.object(
                MalClient,
                "get_anime_details",
                side_effect=unavailable_when_live,
            ), patch.object(
                MalClient,
                "update_my_list_status",
                side_effect=AssertionError("write must not follow a failed live revalidation"),
            ) as update:
                results = execute_approved_sync(config, limit=0, exact_approved_only=True, dry_run=False)

            update.assert_not_called()
            self.assertFalse(results[0].applied)
            self.assertEqual("error", results[0].proposal_decision)
            self.assertIn("mal_live_state_revalidation_failed:live MAL read unavailable", results[0].reasons)

    def test_live_apply_treats_nullable_unlisted_state_as_new_entry(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            config = self._setup_sync(Path(td), provider_episode=10)
            live = self._detail(None)
            with patch.object(MalClient, "get_anime_details", return_value=live), patch.object(
                MalClient,
                "update_my_list_status",
                return_value={"status": "watching", "num_episodes_watched": 10},
            ) as update:
                results = execute_approved_sync(config, limit=0, exact_approved_only=True, dry_run=False)

            update.assert_called_once_with(
                53590,
                status="watching",
                num_watched_episodes=10,
                score=None,
                start_date=None,
                finish_date=None,
            )
            self.assertTrue(results[0].applied)
            self.assertIn("executor_revalidated_live_mal_state", results[0].reasons)
            self.assertIn("would_create_new_mal_entry", results[0].reasons)

    def test_live_apply_fails_closed_for_missing_or_malformed_user_state(self) -> None:
        malformed_details = (
            {key: value for key, value in self._detail(None).items() if key != "my_list_status"},
            self._detail("not-an-object"),
            self._detail({}),
            self._detail({"status": "watching"}),
            self._detail({"status": "unknown", "num_episodes_watched": 9}),
            self._detail({"status": "watching", "num_episodes_watched": "9"}),
        )
        for live in malformed_details:
            with self.subTest(live=live), tempfile.TemporaryDirectory() as td:
                config = self._setup_sync(Path(td), provider_episode=10)
                with patch.object(MalClient, "get_anime_details", return_value=live), patch.object(
                    MalClient,
                    "update_my_list_status",
                    side_effect=AssertionError("invalid live user state must not authorize a write"),
                ) as update:
                    results = execute_approved_sync(config, limit=0, exact_approved_only=True, dry_run=False)

                update.assert_not_called()
                self.assertFalse(results[0].applied)
                self.assertEqual("error", results[0].proposal_decision)
                self.assertIn("mal_live_user_state_invalid_fail_closed", results[0].reasons)

    def test_live_apply_fails_closed_for_mismatched_or_malformed_detail_identity(self) -> None:
        malformed_details = (
            self._detail({"status": "watching", "num_episodes_watched": 9}, mal_anime_id=99999),
            {
                "id": 53590,
                "num_episodes": 12,
                "my_list_status": {"status": "watching", "num_episodes_watched": 9},
            },
        )
        for live in malformed_details:
            with self.subTest(live=live), tempfile.TemporaryDirectory() as td:
                config = self._setup_sync(Path(td), provider_episode=10)
                with patch.object(MalClient, "get_anime_details", return_value=live), patch.object(
                    MalClient,
                    "update_my_list_status",
                    side_effect=AssertionError("invalid detail identity must not authorize a write"),
                ) as update:
                    results = execute_approved_sync(config, limit=0, exact_approved_only=True, dry_run=False)

                update.assert_not_called()
                self.assertFalse(results[0].applied)
                self.assertEqual("error", results[0].proposal_decision)
                self.assertIn("mal_live_user_state_invalid_fail_closed", results[0].reasons)

    def test_user_list_cache_ahead_of_live_detail_blocks_write_for_review(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            config = self._setup_sync(Path(td), provider_episode=10)
            replace_mal_user_anime_list_cache_generation(
                config.db_path,
                items=[
                    {
                        "node": {"id": 53590, "title": "Production-shaped Show"},
                        "list_status": {
                            "status": "watching",
                            "score": 8,
                            "num_episodes_watched": 10,
                            "start_date": "2026-08-01",
                            "updated_at": "2026-08-10T02:00:00Z",
                        },
                    }
                ],
                refresh_run_id="fresh-user-list",
                fetched_at="2026-08-10T02:00:00Z",
            )
            stale_detail = self._detail(None)
            inconsistent_live_detail = self._detail({"status": "watching", "num_episodes_watched": 9, "score": 0})

            def details_for_cache_mode(*_args, **kwargs):
                return inconsistent_live_detail if kwargs.get("force_refresh") else stale_detail

            with patch.object(MalClient, "get_anime_details", side_effect=details_for_cache_mode), patch.object(
                MalClient,
                "update_my_list_status",
                side_effect=AssertionError("conflicting MAL read surfaces must fail closed"),
            ) as update:
                results = execute_approved_sync(config, limit=0, exact_approved_only=True, dry_run=False)

            update.assert_not_called()
            self.assertFalse(results[0].applied)
            self.assertEqual("review", results[0].proposal_decision)
            self.assertIn("mal_user_list_cache_progress_ahead_of_live_detail cached=10 live=9", results[0].reasons)

    def test_success_reconciles_caches_and_next_hour_is_idempotent(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            config = self._setup_sync(Path(td), provider_episode=10)
            replace_mal_user_anime_list_cache_generation(
                config.db_path,
                items=[
                    {
                        "node": {"id": 53590, "title": "Production-shaped Show"},
                        "list_status": {"status": "watching", "num_episodes_watched": 9, "score": 8},
                    }
                ],
                refresh_run_id="before-write",
                fetched_at="2026-08-10T02:00:00Z",
            )
            fields_key = ",".join(
                sorted({"id", "title", "num_episodes", "media_type", "status", "my_list_status", "alternative_titles"})
            )
            upsert_mal_anime_detail_cache(
                config.db_path,
                mal_anime_id=53590,
                fields_key=fields_key,
                logic_version=MAL_DETAIL_CACHE_LOGIC_VERSION,
                response=self._detail(None),
                fetched_at="2026-08-10T01:42:35Z",
                expires_at="2026-08-24T01:42:35Z",
            )
            before = self._detail({"status": "watching", "num_episodes_watched": 9, "score": 8})
            after = self._detail({"status": "watching", "num_episodes_watched": 10, "score": 8})
            live_reads = [before, after]

            with patch.object(MalClient, "get_anime_details", side_effect=lambda *_args, **_kwargs: live_reads.pop(0)), patch.object(
                MalClient,
                "update_my_list_status",
                return_value={"status": "watching", "num_episodes_watched": 10, "score": 8},
            ) as update:
                first = execute_approved_sync(config, limit=0, exact_approved_only=True, dry_run=False)
                second = execute_approved_sync(config, limit=0, exact_approved_only=True, dry_run=False)

            self.assertTrue(first[0].applied)
            self.assertIn("reconciled_local_mal_user_state_caches", first[0].reasons)
            self.assertFalse(second[0].applied)
            self.assertEqual("skip", second[0].proposal_decision)
            self.assertEqual(1, update.call_count)
            reconciled = get_mal_user_anime_list_cache(config.db_path, 53590)
            self.assertIsNotNone(reconciled)
            self.assertEqual(10, reconciled.num_episodes_watched)
            self.assertEqual(8, reconciled.user_score)
            with connect(config.db_path) as conn:
                remaining_mutable_detail_rows = conn.execute(
                    "SELECT COUNT(*) FROM mal_anime_detail_cache "
                    "WHERE mal_anime_id = 53590 AND (',' || fields_key || ',') LIKE '%,my_list_status,%'"
                ).fetchone()[0]
            self.assertEqual(0, remaining_mutable_detail_rows)

    def test_ordinary_stale_list_cache_does_not_block_authoritative_forward_progress(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            config = self._setup_sync(Path(td), provider_episode=10)
            replace_mal_user_anime_list_cache_generation(
                config.db_path,
                items=[
                    {
                        "node": {"id": 53590, "title": "Production-shaped Show"},
                        "list_status": {
                            "status": "watching",
                            "score": 8,
                            "num_episodes_watched": 8,
                            "start_date": "2026-08-01",
                        },
                    }
                ],
                refresh_run_id="ordinary-stale-list",
                fetched_at="2026-08-09T02:00:00Z",
            )
            live = self._detail({"status": "watching", "num_episodes_watched": 9, "score": 0})
            with patch.object(MalClient, "get_anime_details", return_value=live), patch.object(
                MalClient,
                "update_my_list_status",
                return_value={"status": "watching", "num_episodes_watched": 10, "score": 0},
            ) as update:
                results = execute_approved_sync(config, limit=0, exact_approved_only=True, dry_run=False)

            update.assert_called_once()
            self.assertTrue(results[0].applied)

    def test_unconfirmed_put_response_is_not_applied_or_reconciled(self) -> None:
        malformed_responses = (
            {},
            {"status": "watching"},
            {"status": "watching", "num_episodes_watched": 9},
            {"status": "completed", "num_episodes_watched": 10},
            {"status": "watching", "num_episodes_watched": "10"},
        )
        for response in malformed_responses:
            with self.subTest(response=response), tempfile.TemporaryDirectory() as td:
                config = self._setup_sync(Path(td), provider_episode=10)
                replace_mal_user_anime_list_cache_generation(
                    config.db_path,
                    items=[
                        {
                            "node": {"id": 53590, "title": "Production-shaped Show"},
                            "list_status": {"status": "watching", "num_episodes_watched": 9, "score": 8},
                        }
                    ],
                    refresh_run_id="before-unconfirmed-write",
                    fetched_at="2026-08-10T02:00:00Z",
                )
                live = self._detail({"status": "watching", "num_episodes_watched": 9, "score": 8})
                with patch.object(MalClient, "get_anime_details", return_value=live), patch.object(
                    MalClient,
                    "update_my_list_status",
                    return_value=response,
                ):
                    results = execute_approved_sync(config, limit=0, exact_approved_only=True, dry_run=False)

                self.assertFalse(results[0].applied)
                self.assertEqual("error", results[0].proposal_decision)
                self.assertTrue(any(reason.startswith("mal_update_failed:MAL update response") for reason in results[0].reasons))
                cached = get_mal_user_anime_list_cache(config.db_path, 53590)
                self.assertIsNotNone(cached)
                self.assertEqual(9, cached.num_episodes_watched)

    def test_manual_on_hold_status_is_not_replaced_by_watching(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            config = self._setup_sync(Path(td), provider_episode=9)
            live = self._detail({"status": "on_hold", "num_episodes_watched": 8, "score": 9})
            with patch.object(MalClient, "get_anime_details", return_value=live), patch.object(
                MalClient,
                "update_my_list_status",
                side_effect=AssertionError("manual list state must not be destructively replaced"),
            ) as update:
                results = execute_approved_sync(config, limit=0, exact_approved_only=True, dry_run=False)

            update.assert_not_called()
            self.assertEqual("review", results[0].proposal_decision)
            self.assertIn("refusing_destructive_mal_status_change current=on_hold proposed=watching", results[0].reasons)

    def test_result_and_reconciled_cache_drop_unexpected_write_response_text(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            config = self._setup_sync(Path(td), provider_episode=10)
            live = self._detail({"status": "watching", "num_episodes_watched": 9, "score": 8})
            sentinel = "SENTINEL-private-mal-text"
            with patch.object(MalClient, "get_anime_details", return_value=live), patch.object(
                MalClient,
                "update_my_list_status",
                return_value={
                    "status": "watching",
                    "num_episodes_watched": 10,
                    "score": 8,
                    "comments": sentinel,
                    "tags": [sentinel],
                },
            ):
                results = execute_approved_sync(config, limit=0, exact_approved_only=True, dry_run=False)

            rendered = str(results[0].as_dict())
            self.assertNotIn(sentinel, rendered)
            self.assertNotIn("comments", results[0].response_status)
            self.assertNotIn("tags", results[0].response_status)


if __name__ == "__main__":
    unittest.main()
