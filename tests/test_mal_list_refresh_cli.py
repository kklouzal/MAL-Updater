from __future__ import annotations

import io
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from mal_updater.cli import _cmd_mal_list_refresh, build_parser
from mal_updater.db import MalUserAnimeListRefreshSummary


class MalListRefreshCliTests(unittest.TestCase):
    def test_parser_uses_conservative_default_budget_and_explicit_complete_opt_in(self) -> None:
        parser = build_parser()
        args = parser.parse_args(["mal-list-refresh"])

        self.assertEqual(3, args.max_pages)
        self.assertFalse(args.complete)
        self.assertEqual("json", args.format)

        args = parser.parse_args(["mal-list-refresh", "--max-pages", "12", "--complete", "--format", "summary"])
        self.assertEqual(12, args.max_pages)
        self.assertTrue(args.complete)
        self.assertEqual("summary", args.format)

    def test_command_reports_partial_without_treating_it_as_complete_or_failed_process(self) -> None:
        summary = MalUserAnimeListRefreshSummary(
            status="partial",
            refresh_run_id="run",
            generation=2,
            pages=3,
            items=300,
            upserted=300,
            preserved_absent=25,
            scored=120,
            unscored=180,
            by_status={"completed": 200, "watching": 100},
            partial=True,
            error="max_pages reached before MAL anime list pagination completed; seen rows upserted and absent rows retained",
        )
        with tempfile.TemporaryDirectory() as tmp, patch(
            "mal_updater.cli.refresh_mal_user_anime_list_cache",
            return_value=summary,
        ) as refresh, patch("sys.stdout", new_callable=io.StringIO) as stdout:
            exit_code = _cmd_mal_list_refresh(Path(tmp), ["all"], 100, 3, False, "json")

        self.assertEqual(0, exit_code)
        refresh.assert_called_once()
        payload = json.loads(stdout.getvalue())
        self.assertEqual("partial", payload["status"])
        self.assertTrue(payload["partial"])
        self.assertEqual(3, payload["pages"])

    def test_command_summary_output_includes_counts(self) -> None:
        summary = MalUserAnimeListRefreshSummary(
            status="ok",
            refresh_run_id="run",
            generation=2,
            pages=1,
            items=2,
            upserted=2,
            pruned=1,
            scored=1,
            unscored=1,
            by_status={"completed": 1, "plan_to_watch": 1},
        )
        with tempfile.TemporaryDirectory() as tmp, patch(
            "mal_updater.cli.refresh_mal_user_anime_list_cache",
            return_value=summary,
        ), patch("sys.stdout", new_callable=io.StringIO) as stdout:
            exit_code = _cmd_mal_list_refresh(Path(tmp), ["all"], 100, 25, True, "summary")

        self.assertEqual(0, exit_code)
        output = stdout.getvalue()
        self.assertIn("status=ok", output)
        self.assertIn("pages=1", output)
        self.assertIn('by_status={"completed": 1, "plan_to_watch": 1}', output)


if __name__ == "__main__":
    unittest.main()
