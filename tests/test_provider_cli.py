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


if __name__ == "__main__":
    unittest.main()
