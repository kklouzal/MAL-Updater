from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from mal_updater.config import load_config
from mal_updater.db import (
    bootstrap_database,
    connect,
    get_watch_confirmation_provenance,
    list_watch_confirmation_provenance,
    upsert_series_mapping,
    upsert_watch_confirmation_provenance,
)
from mal_updater.sync_planner import build_dry_run_sync_plan


class WatchConfirmationProvenanceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.project_root = Path(self.temp_dir.name)
        (self.project_root / ".MAL-Updater" / "config").mkdir(parents=True, exist_ok=True)
        self.config = load_config(self.project_root)
        bootstrap_database(self.config.db_path)

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

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

    def test_dry_run_sync_plan_persists_provider_watch_completion_evidence(self) -> None:
        with connect(self.config.db_path) as conn:
            conn.execute(
                """
                INSERT INTO provider_series (provider, provider_series_id, title, season_title, raw_json, last_seen_at)
                VALUES ('crunchyroll', 'series-2', 'Provider Two', 'Provider Two Dub', '{}', '2026-07-25T00:05:00Z')
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

        with patch(
            "mal_updater.sync_planner.MalClient.get_anime_details",
            return_value={"id": 222, "title": "MAL Two", "num_episodes": 2, "my_list_status": {"status": "watching", "num_episodes_watched": 1}},
        ):
            proposals = build_dry_run_sync_plan(self.config, limit=1, approved_mappings_only=True)

        self.assertEqual(1, len(proposals))
        self.assertEqual("propose_update", proposals[0].decision)
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


if __name__ == "__main__":
    unittest.main()
