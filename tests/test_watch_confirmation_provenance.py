from __future__ import annotations

import io
import json
import sys
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest.mock import patch

from mal_updater.cli import main
from mal_updater.config import load_config
from mal_updater.db import (
    bootstrap_database,
    connect,
    get_watch_confirmation_provenance,
    list_watch_confirmation_provenance,
    upsert_series_mapping,
    upsert_watch_confirmation_provenance,
)
from mal_updater.sync_planner import build_dry_run_sync_plan, execute_approved_sync


class WatchConfirmationProvenanceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.project_root = Path(self.temp_dir.name)
        self.runtime_env = patch.dict("os.environ", {"MAL_UPDATER_RUNTIME_ROOT": str(self.project_root / ".MAL-Updater")}, clear=False)
        self.runtime_env.start()
        (self.project_root / ".MAL-Updater" / "config").mkdir(parents=True, exist_ok=True)
        self.config = load_config(self.project_root)
        bootstrap_database(self.config.db_path)

    def tearDown(self) -> None:
        self.runtime_env.stop()
        self.temp_dir.cleanup()

    def _seed_completed_approved_series(self) -> None:
        with connect(self.config.db_path) as conn:
            conn.execute(
                """
                INSERT INTO provider_series (provider, provider_series_id, title, season_title, raw_json, last_seen_at, account_observed_at)
                VALUES ('crunchyroll', 'series-2', 'Provider Two', 'Provider Two Dub', '{}', '2026-07-25T00:05:00Z', '2026-07-25T00:05:00Z')
                """
            )
            conn.execute(
                """
                INSERT INTO provider_episode_progress (
                    provider, provider_episode_id, provider_series_id, episode_number,
                    completion_ratio, playback_position_ms, duration_ms, last_watched_at,
                    raw_json, last_seen_at
                ) VALUES
                    ('crunchyroll', 'series-2-ep1', 'series-2', 1, 1.0, 1440000, 1440000, '2026-07-25T00:01:00Z', '{}', '2026-07-25T00:02:00Z'),
                    ('crunchyroll', 'series-2-ep2', 'series-2', 2, 0.96, 1440000, 1440000, '2026-07-25T00:03:00Z', '{}', '2026-07-25T00:04:00Z')
                """
            )
            conn.commit()
        upsert_series_mapping(
            self.config.db_path,
            provider="crunchyroll",
            provider_series_id="series-2",
            mal_anime_id=222,
            confidence=0.99,
            mapping_source="user_exact",
            approved_by_user=True,
            notes=None,
        )

    def _seed_existing_provenance_marker(self) -> None:
        upsert_watch_confirmation_provenance(
            self.config.db_path,
            provider="crunchyroll",
            provider_series_id="series-2",
            identity_key="mal:111",
            mal_anime_id=111,
            source_title="Existing Provider Two",
            mapped_mal_title="Existing MAL Two",
            completion_decision="existing_marker",
            completion_status="existing",
            generated_at="2026-07-24T00:00:00Z",
        )

    def _assert_existing_provenance_marker_unchanged(self) -> None:
        row = get_watch_confirmation_provenance(
            self.config.db_path,
            provider="crunchyroll",
            provider_series_id="series-2",
        )
        self.assertIsNotNone(row)
        assert row is not None
        self.assertEqual("mal:111", row.identity_key)
        self.assertEqual(111, row.mal_anime_id)
        self.assertEqual("Existing Provider Two", row.source_title)
        self.assertEqual("Existing MAL Two", row.mapped_mal_title)
        self.assertEqual("existing_marker", row.completion_decision)

    def test_upsert_readback_and_current_snapshot_replacement(self) -> None:
        first = upsert_watch_confirmation_provenance(
            self.config.db_path,
            provider="crunchyroll",
            provider_series_id="series-1",
            identity_key="mal:100",
            mal_anime_id=100,
            source_title="Provider One",
            mapped_mal_title="MAL One",
            progress_rows=2,
            completed_episode_count=2,
            max_episode_number=2,
            max_completed_episode_number=2,
            provider_watched_episodes=2,
            mal_num_episodes=2,
            confirmed_complete=True,
            completion_decision="provider_completed_known_mal_episode_count",
            completion_status="complete",
            completed_by={"ratio_threshold": 2},
            completed_examples={"ratio_threshold": ["ep1", "ep2"]},
            incomplete_examples=[],
            thresholds={"completion_threshold": 0.95},
            progress_audit={"audit": "first"},
            mapping_audit={"mapping_status": "approved"},
            decision_audit={"decision": "propose_update"},
            generated_at="2026-07-25T00:00:00Z",
        )
        self.assertTrue(first.confirmed_complete)
        self.assertEqual("mal:100", first.identity_key)

        second = upsert_watch_confirmation_provenance(
            self.config.db_path,
            provider="crunchyroll",
            provider_series_id="series-1",
            identity_key="mal:200",
            mal_anime_id=200,
            source_title="Provider One Retargeted",
            mapped_mal_title="MAL Two",
            completion_decision="provider_watchlist_without_progress",
            completion_status="watchlist_only",
            generated_at="2026-07-25T01:00:00Z",
        )

        self.assertEqual(200, second.mal_anime_id)
        self.assertEqual("mal:200", second.identity_key)
        self.assertFalse(second.confirmed_complete)
        row = get_watch_confirmation_provenance(
            self.config.db_path,
            provider="crunchyroll",
            provider_series_id="series-1",
        )
        self.assertIsNotNone(row)
        assert row is not None
        self.assertEqual(200, row.mal_anime_id)
        rows = list_watch_confirmation_provenance(self.config.db_path, provider="crunchyroll")
        self.assertEqual(["series-1"], [item.provider_series_id for item in rows])
        self.assertEqual([row], list_watch_confirmation_provenance(self.config.db_path, identity_key="mal:200"))

    def test_dry_run_sync_plan_leaves_watch_completion_provenance_unchanged(self) -> None:
        self._seed_completed_approved_series()
        self._seed_existing_provenance_marker()

        with patch(
            "mal_updater.sync_planner.MalClient.get_anime_details",
            return_value={"id": 222, "title": "MAL Two", "num_episodes": 2, "my_list_status": {"status": "watching", "num_episodes_watched": 1}},
        ):
            proposals = build_dry_run_sync_plan(self.config, limit=1, approved_mappings_only=True)

        self.assertEqual(1, len(proposals))
        self.assertEqual("propose_update", proposals[0].decision)
        self._assert_existing_provenance_marker_unchanged()

    def test_execute_approved_sync_dry_run_leaves_watch_completion_provenance_unchanged(self) -> None:
        self._seed_completed_approved_series()
        self._seed_existing_provenance_marker()

        with patch(
            "mal_updater.sync_planner.MalClient.get_anime_details",
            return_value={"id": 222, "title": "MAL Two", "num_episodes": 2, "my_list_status": {"status": "watching", "num_episodes_watched": 1}},
        ), patch("mal_updater.sync_planner.MalClient.update_my_list_status", side_effect=AssertionError("dry-run should not write")):
            results = execute_approved_sync(self.config, limit=1, exact_approved_only=True, dry_run=True)

        self.assertEqual(1, len(results))
        self.assertFalse(results[0].applied)
        self.assertEqual("propose_update", results[0].proposal_decision)
        self._assert_existing_provenance_marker_unchanged()

    def test_apply_sync_without_execute_leaves_watch_completion_provenance_unchanged(self) -> None:
        self._seed_completed_approved_series()
        self._seed_existing_provenance_marker()

        output = io.StringIO()
        with patch(
            "mal_updater.sync_planner.MalClient.get_anime_details",
            return_value={"id": 222, "title": "MAL Two", "num_episodes": 2, "my_list_status": {"status": "watching", "num_episodes_watched": 1}},
        ), patch(
            "mal_updater.sync_planner.MalClient.update_my_list_status",
            side_effect=AssertionError("apply-sync without --execute should not write"),
        ), patch.object(
            sys,
            "argv",
            ["mal-updater", "--project-root", str(self.project_root), "apply-sync", "--limit", "1", "--exact-approved-only"],
        ), redirect_stdout(output):
            rc = main()

        self.assertEqual(0, rc)
        payload = json.loads(output.getvalue())
        self.assertEqual(1, len(payload))
        self.assertEqual("propose_update", payload[0]["proposal_decision"])
        self.assertIn("executor_dry_run", payload[0]["reasons"])
        self._assert_existing_provenance_marker_unchanged()

    def test_execute_approved_sync_live_persists_watch_completion_provenance(self) -> None:
        self._seed_completed_approved_series()
        self._seed_existing_provenance_marker()

        with patch(
            "mal_updater.sync_planner.MalClient.get_anime_details",
            return_value={"id": 222, "title": "MAL Two", "num_episodes": 2, "my_list_status": {"status": "watching", "num_episodes_watched": 1}},
        ), patch(
            "mal_updater.sync_planner.MalClient.update_my_list_status",
            return_value={"status": "completed", "num_episodes_watched": 2},
        ):
            results = execute_approved_sync(self.config, limit=1, exact_approved_only=True, dry_run=False)

        self.assertEqual(1, len(results))
        self.assertTrue(results[0].applied)
        row = get_watch_confirmation_provenance(
            self.config.db_path,
            provider="crunchyroll",
            provider_series_id="series-2",
        )
        self.assertIsNotNone(row)
        assert row is not None
        self.assertEqual("mal:222", row.identity_key)
        self.assertEqual(222, row.mal_anime_id)
        self.assertEqual("MAL Two", row.mapped_mal_title)
        self.assertEqual(2, row.progress_rows)
        self.assertEqual(2, row.completed_episode_count)
        self.assertEqual(2, row.provider_watched_episodes)
        self.assertTrue(row.confirmed_complete)
        self.assertEqual("provider_completed_known_mal_episode_count", row.completion_decision)
        self.assertEqual("complete", row.completion_status)
        self.assertEqual("2026-07-25T00:04:00Z", row.last_progress_seen_at)
        self.assertEqual("2026-07-25T00:05:00Z", row.last_series_seen_at)
        self.assertEqual({"ratio_threshold": 2, "credits_window": 0, "later_episode_evidence": 0}, row.completed_by)
        self.assertEqual("propose_update", row.decision_audit["decision"])
        self.assertEqual("approved", row.mapping_audit["mapping_status"])

    def test_execute_approved_sync_live_persists_provenance_for_skip_outcome(self) -> None:
        self._seed_completed_approved_series()
        self._seed_existing_provenance_marker()

        with patch(
            "mal_updater.sync_planner.MalClient.get_anime_details",
            return_value={
                "id": 222,
                "title": "MAL Two",
                "num_episodes": 2,
                "my_list_status": {"status": "completed", "num_episodes_watched": 2, "finish_date": "2026-07-25"},
            },
        ), patch("mal_updater.sync_planner.MalClient.update_my_list_status", side_effect=AssertionError("skip outcome should not write")):
            results = execute_approved_sync(self.config, limit=1, exact_approved_only=True, dry_run=False)

        self.assertEqual(1, len(results))
        self.assertFalse(results[0].applied)
        self.assertEqual("skip", results[0].proposal_decision)
        row = get_watch_confirmation_provenance(
            self.config.db_path,
            provider="crunchyroll",
            provider_series_id="series-2",
        )
        self.assertIsNotNone(row)
        assert row is not None
        self.assertEqual("mal:222", row.identity_key)
        self.assertEqual("MAL Two", row.mapped_mal_title)
        self.assertEqual("skip", row.decision_audit["decision"])


if __name__ == "__main__":
    unittest.main()
