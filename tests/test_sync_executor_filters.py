from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from mal_updater.config import load_config
from mal_updater.db import bootstrap_database, upsert_series_mapping
from mal_updater.ingestion import ingest_snapshot_payload
from mal_updater.sync_planner import execute_approved_sync
from tests.test_validation_ingestion import sample_snapshot


class SyncExecutorFilterTests(unittest.TestCase):
    def test_exact_approved_only_skips_non_exact_approved_mapping(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / "config").mkdir()
            config = load_config(root)
            bootstrap_database(config.db_path)
            ingest_snapshot_payload(sample_snapshot(), config)
            upsert_series_mapping(
                config.db_path,
                provider="crunchyroll",
                provider_series_id="series-123",
                mal_anime_id=1,
                confidence=0.91,
                mapping_source="user_approved",
                approved_by_user=True,
                notes="manual strong approval",
            )

            with patch("mal_updater.sync_planner.MalClient.get_anime_details") as get_details:
                results = execute_approved_sync(config, limit=0, exact_approved_only=True, dry_run=True)

            self.assertEqual(results, [])
            get_details.assert_not_called()

    def test_exact_approved_only_allows_auto_exact_mapping(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / "config").mkdir()
            config = load_config(root)
            bootstrap_database(config.db_path)
            ingest_snapshot_payload(sample_snapshot(), config)
            upsert_series_mapping(
                config.db_path,
                provider="crunchyroll",
                provider_series_id="series-123",
                mal_anime_id=1,
                confidence=1.0,
                mapping_source="auto_exact",
                approved_by_user=True,
                notes="auto exact",
            )

            detail = {
                "id": 1,
                "title": "Example Show",
                "num_episodes": 12,
                "media_type": "tv",
                "status": "finished_airing",
                "my_list_status": None,
                "alternative_titles": {},
            }
            with patch("mal_updater.sync_planner.MalClient.get_anime_details", return_value=detail):
                results = execute_approved_sync(config, limit=0, exact_approved_only=True, dry_run=True)

            self.assertEqual(len(results), 1)
            self.assertEqual(results[0].provider_series_id, "series-123")
            self.assertIn("exact_approved_only_enabled", results[0].reasons)


if __name__ == "__main__":
    unittest.main()
