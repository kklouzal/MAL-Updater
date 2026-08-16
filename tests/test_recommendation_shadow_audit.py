from __future__ import annotations

import json
import sqlite3
import sys
import tempfile
import unittest
from contextlib import redirect_stdout
from io import StringIO
from pathlib import Path
from unittest.mock import patch

from mal_updater.cli import main as cli_main
from mal_updater.config import load_config
from mal_updater.db import bootstrap_database, connect, insert_recommendation_snapshot_rows
from mal_updater.mal_client import MalApiError
from mal_updater.recommendation_shadow_audit import build_mal_suggestions_shadow_audit, load_shadow_cohorts


class MalSuggestionsShadowAuditTests(unittest.TestCase):
    def test_aggregate_artifact_has_overlap_novelty_quality_but_no_personal_rows(self) -> None:
        payload = {
            "data": [
                {"node": {"id": 10, "title": "Private Title A", "mean": 8.5, "num_scoring_users": 100, "media_type": "tv", "status": "finished_airing"}},
                {"node": {"id": 20, "title": "Private Title B", "mean": 7.0, "num_scoring_users": 50, "media_type": "movie", "status": "currently_airing"}},
            ],
            "paging": {"next": "https://api.myanimelist.net/v2/anime/suggestions?offset=2"},
        }
        audit = build_mal_suggestions_shadow_audit(
            payload,
            {"mapped": {10}, "listed": {10, 30}, "recommended": {20, 40}, "discovery_recommended": {20}},
            limit=100,
            generated_at="2026-08-15T00:00:00Z",
        )

        self.assertEqual("mal-suggestions-shadow-audit-v1", audit["schema_version"])
        self.assertEqual(1, audit["overlap"]["mapped"]["count"])
        self.assertEqual(1, audit["novelty"]["vs_list_count"])
        self.assertEqual(0, audit["novelty"]["vs_list_and_recommendations_count"])
        self.assertEqual({"observed": 2, "minimum": 7, "median": 7.75, "maximum": 8.5}, audit["candidate_quality_inputs"]["mean"])
        self.assertEqual(0, audit["source"]["additional_pages_followed"])
        self.assertFalse(audit["privacy"]["raw_payload_retained"])
        rendered = json.dumps(audit)
        for private_value in ("Private Title A", "Private Title B", "suggestions?offset=2", '"10"', '"20"'):
            self.assertNotIn(private_value, rendered)

    def test_schema_and_first_page_bound_fail_closed(self) -> None:
        with self.assertRaises(MalApiError):
            build_mal_suggestions_shadow_audit({"data": "bad"}, {}, limit=100)
        with self.assertRaises(MalApiError):
            build_mal_suggestions_shadow_audit(
                {"data": [{"node": {"id": index + 1, "title": "x"}} for index in range(2)]},
                {},
                limit=1,
            )

    def test_cohort_read_is_query_only_and_does_not_mutate_operational_db(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / ".MAL-Updater" / "config").mkdir(parents=True)
            config = load_config(root)
            bootstrap_database(config.db_path)
            with connect(config.db_path) as conn:
                conn.execute(
                    "INSERT INTO provider_series(provider, provider_series_id, title) VALUES ('crunchyroll', 'one', 'One')"
                )
                conn.execute(
                    "INSERT INTO mal_series_mapping(provider, provider_series_id, mal_anime_id, mapping_source) "
                    "VALUES ('crunchyroll', 'one', 10, 'test')"
                )
            insert_recommendation_snapshot_rows(
                config.db_path,
                [{"kind": "discovery_candidate", "title": "Twenty", "mal_anime_id": 20}],
                run_id="latest",
                generated_at="2026-08-15T00:00:00Z",
            )
            before = config.db_path.read_bytes()

            cohorts = load_shadow_cohorts(config.db_path)

            self.assertEqual({10}, cohorts["mapped"])
            self.assertEqual({20}, cohorts["recommended"])
            self.assertEqual({20}, cohorts["discovery_recommended"])
            self.assertEqual(before, config.db_path.read_bytes())
            with sqlite3.connect(config.db_path) as conn:
                self.assertEqual(0, conn.execute("PRAGMA user_version").fetchone()[0])

    def test_missing_operational_db_fails_without_creating_it(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "missing.sqlite3"
            with self.assertRaises(FileNotFoundError):
                load_shadow_cohorts(db_path)
            self.assertFalse(db_path.exists())

    def test_cli_writes_only_privacy_safe_artifact_and_does_not_change_db(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / ".MAL-Updater" / "config").mkdir(parents=True)
            config = load_config(root)
            bootstrap_database(config.db_path)
            before = config.db_path.read_bytes()
            output = root / "shadow.json"
            payload = {"data": [{"node": {"id": 99, "title": "Sensitive Suggestion", "mean": 8.25}}]}
            argv = [
                "mal-updater",
                "--project-root",
                str(root),
                "recommend-suggestions-audit",
                "--output",
                str(output),
            ]

            with (
                patch.object(sys, "argv", argv),
                patch("mal_updater.cli.load_mal_secrets"),
                patch("mal_updater.cli.MalClient.get_anime_suggestions", return_value=payload) as fetch,
                redirect_stdout(StringIO()),
            ):
                self.assertEqual(0, cli_main())

            fetch.assert_called_once_with(limit=100)
            self.assertEqual(before, config.db_path.read_bytes())
            artifact = output.read_text(encoding="utf-8")
            self.assertNotIn("Sensitive Suggestion", artifact)
            self.assertNotIn("99", artifact)
            self.assertEqual(0, json.loads(artifact)["operational_effects"]["candidate_rows_persisted"])


if __name__ == "__main__":
    unittest.main()
