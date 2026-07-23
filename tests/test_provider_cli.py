from __future__ import annotations

import io
import json
import sys
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest.mock import patch

from mal_updater.cli import build_parser, main
from mal_updater.config import load_config
from mal_updater.db import get_series_mapping
from mal_updater.ingestion import ingest_snapshot_payload
from tests.test_validation_ingestion import sample_snapshot


class ProviderCliTests(unittest.TestCase):
    def test_dry_run_sync_passes_provider_to_sync_planner(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / ".MAL-Updater" / "config").mkdir(parents=True)
            output = io.StringIO()
            with patch("mal_updater.cli.build_dry_run_sync_plan", return_value=[] ) as build_mock, patch.object(
                sys, "argv", [
                    "mal-updater",
                    "--project-root",
                    str(root),
                    "dry-run-sync",
                    "--provider",
                    "hidive",
                    "--limit",
                    "5",
                ]
            ), redirect_stdout(output):
                rc = main()
            self.assertEqual(0, rc)
            self.assertEqual("hidive", build_mock.call_args.kwargs["provider"])
            payload = json.loads(output.getvalue())
            self.assertEqual([], payload["proposals"])

    def test_dry_run_sync_provider_all_passes_aggregate_to_sync_planner(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / ".MAL-Updater" / "config").mkdir(parents=True)
            output = io.StringIO()
            with patch("mal_updater.cli.build_dry_run_sync_plan", return_value=[]) as build_mock, patch.object(
                sys, "argv", [
                    "mal-updater",
                    "--project-root",
                    str(root),
                    "dry-run-sync",
                    "--provider",
                    "all",
                    "--limit",
                    "5",
                ]
            ), redirect_stdout(output):
                rc = main()
            self.assertEqual(0, rc)
            self.assertIsNone(build_mock.call_args.kwargs["provider"])
            payload = json.loads(output.getvalue())
            self.assertEqual([], payload["proposals"])

    def test_approve_mapping_provider_option_persists_selected_provider(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / ".MAL-Updater" / "config").mkdir(parents=True)
            config = load_config(root)
            payload = sample_snapshot()
            payload["provider"] = "hidive"
            payload["series"][0]["provider_series_id"] = "hidive-series-123"
            payload["progress"][0]["provider_series_id"] = "hidive-series-123"
            payload["watchlist"][0]["provider_series_id"] = "hidive-series-123"
            ingest_snapshot_payload(payload, config)
            output = io.StringIO()
            with patch.object(
                sys,
                "argv",
                [
                    "mal-updater",
                    "--project-root",
                    str(root),
                    "approve-mapping",
                    "hidive-series-123",
                    "123",
                    "--provider",
                    "hidive",
                    "--confidence",
                    "0.9",
                ],
            ), redirect_stdout(output):
                rc = main()

            self.assertEqual(0, rc)
            payload = json.loads(output.getvalue())
            self.assertEqual("hidive", payload["provider"])
            self.assertIsNone(get_series_mapping(config.db_path, "crunchyroll", "hidive-series-123"))
            mapping = get_series_mapping(config.db_path, "hidive", "hidive-series-123")
            self.assertIsNotNone(mapping)
            assert mapping is not None
            self.assertEqual(123, mapping.mal_anime_id)

    def test_approve_mapping_without_provider_preserves_crunchyroll_default(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / ".MAL-Updater" / "config").mkdir(parents=True)
            config = load_config(root)
            ingest_snapshot_payload(sample_snapshot(), config)
            output = io.StringIO()
            with patch.object(
                sys,
                "argv",
                [
                    "mal-updater",
                    "--project-root",
                    str(root),
                    "approve-mapping",
                    "series-123",
                    "123",
                ],
            ), redirect_stdout(output):
                rc = main()

            self.assertEqual(0, rc)
            payload = json.loads(output.getvalue())
            self.assertEqual("crunchyroll", payload["provider"])
            self.assertIsNotNone(get_series_mapping(config.db_path, "crunchyroll", "series-123"))

    def test_provider_help_warns_against_whole_library_crawling(self) -> None:
        parser = build_parser()
        provider_action = next(action for action in parser._actions if getattr(action, "choices", None))
        provider_parser = provider_action.choices["provider-fetch-snapshot"]
        help_text = provider_parser.format_help()

        self.assertIn("account-scoped history/watchlist surfaces", help_text)
        self.assertIn("never crawl whole Crunchyroll/HIDIVE libraries", help_text)

    def test_recommend_metadata_help_documents_paced_mal_refreshes(self) -> None:
        parser = build_parser()
        provider_action = next(action for action in parser._actions if getattr(action, "choices", None))
        recommend_parser = provider_action.choices["recommend-refresh-metadata"]
        help_text = recommend_parser.format_help()

        self.assertIn("MAL refreshes are paced by client throttling", help_text)
        self.assertIn("spread over time", help_text)

    def test_reserved_sync_command_stays_parseable_for_cli_compatibility(self) -> None:
        parser = build_parser()
        provider_action = next(action for action in parser._actions if getattr(action, "choices", None))

        self.assertIn("sync", provider_action.choices)
        self.assertIn("Reserved for future sync orchestration", provider_action.choices["sync"].format_help())


if __name__ == "__main__":
    unittest.main()
