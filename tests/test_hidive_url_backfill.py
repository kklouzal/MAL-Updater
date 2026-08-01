from __future__ import annotations

import json
import sqlite3
import tempfile
import unittest
from contextlib import closing
import io
from contextlib import redirect_stdout
from pathlib import Path

from mal_updater.cli import _cmd_backfill_hidive_series_urls
from mal_updater.config import load_config
from mal_updater.db import (
    backfill_hidive_series_urls,
    bootstrap_database,
    connect,
    upsert_provider_title_search_cache,
    upsert_recommendation_provider_eligibility_evidence,
)


class HidiveUrlBackfillTests(unittest.TestCase):
    def test_backfill_hidive_series_urls_is_dry_run_then_idempotent_apply(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            db_path = Path(td) / "mal-updater.sqlite3"
            bootstrap_database(db_path)
            with connect(db_path) as conn:
                conn.execute(
                    """
                    INSERT INTO provider_series(provider, provider_series_id, title, season_title, raw_json, account_observed_at)
                    VALUES ('hidive', '2312', 'Dungeon People', 'Dungeon People', ?, CURRENT_TIMESTAMP)
                    """,
                    (json.dumps({"provider_series_id": "2312", "title": "Dungeon People", "url": "https://www.hidive.com/season/dungeon-people"}),),
                )
                conn.commit()
            upsert_recommendation_provider_eligibility_evidence(
                db_path,
                mal_anime_id=1,
                provider="hidive",
                provider_series_id="2312",
                provider_title="Dungeon People",
                provider_url="https://www.hidive.com/season/2312",
                identity_match_kind="provider_title_search_exact",
                review_status="verified",
                catalog_status="present",
                english_dub_status="present",
                audio_locales=["en-US"],
                source_evidence={},
                fetched_at="2026-07-30T00:00:00Z",
                expires_at="2026-08-30T00:00:00Z",
            )
            upsert_provider_title_search_cache(
                db_path,
                provider="hidive",
                normalized_query="dungeon people",
                query="Dungeon People",
                candidate_mal_anime_id=1,
                candidate_title="Dungeon People",
                matches=[{"provider_series_id": "2312", "title": "Dungeon People", "url": "https://www.hidive.com/season/dungeon-people"}],
                status="ok",
                fetched_at="2026-07-30T00:00:00Z",
                expires_at="2026-08-30T00:00:00Z",
            )
            upsert_provider_title_search_cache(
                db_path,
                provider="hidive",
                normalized_query="dungeon people",
                query="Dungeon People",
                candidate_mal_anime_id=2,
                candidate_title="Dungeon People Alternate",
                matches=[{"provider_series_id": "2312", "title": "Dungeon People", "url": "https://www.hidive.com/season/dungeon-people"}],
                status="ok",
                fetched_at="2026-07-30T00:00:00Z",
                expires_at="2026-08-30T00:00:00Z",
                logic_version="provider-title-v2",
                search_limit=5,
                identity_key="mal:2",
            )
            snapshot_context = {
                "provider_eligibility_evidence": [
                    {
                        "provider": "hidive",
                        "provider_series_id": "2312",
                        "provider_title": "Dungeon People",
                        "provider_url": "https://www.hidive.com/season/dungeon-people",
                    },
                    {
                        "provider": "crunchyroll",
                        "provider_series_id": "cr-1",
                        "provider_title": "Unrelated",
                        "provider_url": "https://www.hidive.com/season/should-stay-crunchyroll",
                    },
                    {
                        "provider": "hidive",
                        "provider_title": "Missing id",
                        "provider_url": "https://www.hidive.com/season/missing-id",
                    },
                ],
                "available_provider_series": [
                    {
                        "provider": "HIDIVE",
                        "provider_series_id": "777",
                        "title": "Another HIDIVE Series",
                        "provider_url": "http://www.hidive.com/season/another-hidive-series",
                        "url": "https://www.hidive.com/season/not-a-provider-url-field",
                    },
                    "preserve non-object items",
                ],
                "provider_url": "https://www.hidive.com/season/root-provider-url-is-not-a-provider-object",
            }
            with connect(db_path) as conn:
                conn.execute(
                    """
                    INSERT INTO recommendation_score_snapshots(
                        run_id, generated_at, kind, provider, title, provider_series_id,
                        mal_anime_id, score, priority, context_json
                    ) VALUES ('run-1', '2026-07-30 00:00:00', 'discovery_candidate', 'hidive', 'Dungeon People', '2312', 1, 1.0, 1, ?)
                    """,
                    (json.dumps(snapshot_context),),
                )
                conn.execute(
                    """
                    INSERT INTO recommendation_score_snapshots(
                        run_id, generated_at, kind, provider, title, provider_series_id,
                        mal_anime_id, score, priority, context_json
                    ) VALUES ('run-1', '2026-07-30 00:00:00', 'discovery_candidate', 'hidive', 'Malformed', '9999', 2, 1.0, 2, ?)
                    """,
                    ("{not json https://www.hidive.com/season/malformed",),
                )
                conn.commit()

            preview = backfill_hidive_series_urls(db_path)

            self.assertTrue(preview["dry_run"])
            self.assertEqual(1, preview["provider_series"]["matched"])
            self.assertEqual(1, preview["eligibility"]["matched"])
            self.assertEqual(2, preview["provider_title_search_cache"]["matched"])
            self.assertEqual(2, preview["provider_title_search_cache"]["sample_count"])
            self.assertEqual(
                {"", "mal:2"},
                {sample["identity_key"] for sample in preview["provider_title_search_cache"]["samples"]},
            )
            self.assertEqual(1, preview["recommendation_score_snapshots"]["matched"])
            self.assertEqual(1, preview["recommendation_score_snapshots"]["sample_count"])
            with closing(sqlite3.connect(db_path)) as conn:
                raw = json.loads(conn.execute("SELECT raw_json FROM provider_series WHERE provider='hidive'").fetchone()[0])
                self.assertEqual("https://www.hidive.com/season/dungeon-people", raw["url"])
                context = json.loads(conn.execute("SELECT context_json FROM recommendation_score_snapshots WHERE title='Dungeon People'").fetchone()[0])
                self.assertEqual(
                    "https://www.hidive.com/season/dungeon-people",
                    context["provider_eligibility_evidence"][0]["provider_url"],
                )

            applied = backfill_hidive_series_urls(db_path, apply=True)
            second = backfill_hidive_series_urls(db_path, apply=True)

            self.assertEqual(1, applied["provider_series"]["updated"])
            self.assertEqual(1, applied["eligibility"]["updated"])
            self.assertEqual(2, applied["provider_title_search_cache"]["updated"])
            self.assertEqual(1, applied["recommendation_score_snapshots"]["updated"])
            self.assertEqual(0, second["provider_series"]["matched"])
            self.assertEqual(0, second["eligibility"]["matched"])
            self.assertEqual(0, second["provider_title_search_cache"]["matched"])
            self.assertEqual(0, second["recommendation_score_snapshots"]["matched"])
            with closing(sqlite3.connect(db_path)) as conn:
                raw = json.loads(conn.execute("SELECT raw_json FROM provider_series WHERE provider='hidive'").fetchone()[0])
                self.assertEqual("https://www.hidive.com/series/2312", raw["url"])
                provider_url = conn.execute("SELECT provider_url FROM recommendation_provider_eligibility_evidence WHERE provider='hidive'").fetchone()[0]
                self.assertEqual("https://www.hidive.com/series/2312", provider_url)
                cache_rows = conn.execute(
                    """
                    SELECT matches_json FROM provider_title_search_cache
                    WHERE provider='hidive' AND normalized_query='dungeon people'
                    ORDER BY logic_version, search_limit, identity_key
                    """
                ).fetchall()
                self.assertEqual(2, len(cache_rows))
                for row in cache_rows:
                    matches = json.loads(row[0])
                    self.assertEqual("https://www.hidive.com/series/2312", matches[0]["url"])
                context = json.loads(conn.execute("SELECT context_json FROM recommendation_score_snapshots WHERE title='Dungeon People'").fetchone()[0])
                self.assertEqual("https://www.hidive.com/series/2312", context["provider_eligibility_evidence"][0]["provider_url"])
                self.assertEqual("https://www.hidive.com/season/should-stay-crunchyroll", context["provider_eligibility_evidence"][1]["provider_url"])
                self.assertEqual("https://www.hidive.com/season/missing-id", context["provider_eligibility_evidence"][2]["provider_url"])
                self.assertEqual("https://www.hidive.com/series/777", context["available_provider_series"][0]["provider_url"])
                self.assertEqual("https://www.hidive.com/season/not-a-provider-url-field", context["available_provider_series"][0]["url"])
                self.assertEqual("https://www.hidive.com/season/root-provider-url-is-not-a-provider-object", context["provider_url"])

    def test_backfill_hidive_series_urls_summary_includes_sample_counts(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            config = load_config(root)
            bootstrap_database(config.db_path)
            with connect(config.db_path) as conn:
                conn.execute(
                    """
                    INSERT INTO recommendation_score_snapshots(
                        run_id, generated_at, kind, provider, title, provider_series_id,
                        mal_anime_id, score, priority, context_json
                    ) VALUES ('run-1', '2026-07-30 00:00:00', 'discovery_candidate', 'hidive', 'Dungeon People', '2312', 1, 1.0, 1, ?)
                    """,
                    (
                        json.dumps(
                            {
                                "provider_eligibility_evidence": [
                                    {
                                        "provider": "hidive",
                                        "provider_series_id": "2312",
                                        "provider_url": "https://www.hidive.com/season/dungeon-people",
                                    }
                                ]
                            }
                        ),
                    ),
                )
                conn.commit()

            output = io.StringIO()
            with redirect_stdout(output):
                self.assertEqual(0, _cmd_backfill_hidive_series_urls(root, apply=False, output_format="summary"))

            summary = output.getvalue()
            self.assertIn("provider_series_sample_count=0", summary)
            self.assertIn("eligibility_sample_count=0", summary)
            self.assertIn("provider_title_search_cache_sample_count=0", summary)
            self.assertIn("recommendation_score_snapshots_matched=1", summary)
            self.assertIn("recommendation_score_snapshots_updated=0", summary)
            self.assertIn("recommendation_score_snapshots_sample_count=1", summary)


if __name__ == "__main__":
    unittest.main()
