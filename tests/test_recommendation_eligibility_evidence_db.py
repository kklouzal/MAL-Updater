from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from mal_updater.db import (
    apply_migrations,
    bootstrap_database,
    connect,
    delete_recommendation_provider_eligibility_evidence,
    get_recommendation_provider_eligibility_evidence,
    insert_recommendation_snapshot_rows,
    list_latest_recommendation_snapshot_rows,
    list_recommendation_provider_eligibility_evidence_for_mal_ids,
    mark_stale_recommendation_provider_eligibility_evidence,
    upsert_recommendation_provider_eligibility_evidence,
)


class RecommendationEligibilityEvidenceDbTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.temp_dir.name) / "mal-updater.sqlite3"
        bootstrap_database(self.db_path)

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def test_fresh_bootstrap_applies_eligibility_schema(self) -> None:
        with connect(self.db_path) as conn:
            columns = {row["name"] for row in conn.execute("PRAGMA table_info(recommendation_provider_eligibility_evidence)")}
            snapshot_columns = {row["name"] for row in conn.execute("PRAGMA table_info(recommendation_score_snapshots)")}
            migrations = {row["version"] for row in conn.execute("SELECT version FROM schema_migrations")}

        self.assertIn("mal_anime_id", columns)
        self.assertIn("english_dub_status", columns)
        self.assertIn("source_evidence_json", columns)
        self.assertIn("availability_confidence_label", snapshot_columns)
        self.assertIn("006_recommendation_eligibility_evidence.sql", migrations)

    def test_existing_schema_upgrade_is_idempotent(self) -> None:
        db_path = Path(self.temp_dir.name) / "upgrade.sqlite3"
        with connect(db_path) as conn:
            conn.execute(
                """
                CREATE TABLE schema_migrations (
                    version TEXT PRIMARY KEY,
                    applied_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
            for migration in [
                "001_initial.sql",
                "002_mal_metadata_cache.sql",
                "003_mal_recommendation_edges.sql",
                "004_provider_search_cache.sql",
                "004_mal_recommendation_harvest_status.sql",
            ]:
                conn.executescript((Path(__file__).resolve().parents[1] / "migrations" / migration).read_text(encoding="utf-8"))
                conn.execute("INSERT INTO schema_migrations(version) VALUES (?)", (migration,))
            conn.executescript((Path(__file__).resolve().parents[1] / "migrations" / "005_recommendation_score_snapshots.sql").read_text(encoding="utf-8"))
            conn.execute("INSERT INTO schema_migrations(version) VALUES (?)", ("005_recommendation_score_snapshots.sql",))
            conn.commit()
            apply_migrations(conn)
            apply_migrations(conn)
            columns = {row["name"] for row in conn.execute("PRAGMA table_info(recommendation_provider_eligibility_evidence)")}
            snapshot_columns = {row["name"] for row in conn.execute("PRAGMA table_info(recommendation_score_snapshots)")}
            migration_rows = conn.execute(
                "SELECT COUNT(*) AS n FROM schema_migrations WHERE version = '006_recommendation_eligibility_evidence.sql'"
            ).fetchone()["n"]

        self.assertIn("review_status", columns)
        self.assertIn("availability_confidence_label", snapshot_columns)
        self.assertEqual(1, migration_rows)

    def test_evidence_upsert_replace_read_actionable_filter_stale_and_delete(self) -> None:
        initial = upsert_recommendation_provider_eligibility_evidence(
            self.db_path,
            mal_anime_id=101,
            provider="crunchyroll",
            provider_series_id="cr-series-1",
            provider_title="Candidate Title",
            provider_url="https://example.test/watch/cr-series-1",
            identity_match_kind="title_alias",
            match_confidence=0.72,
            review_status="review-needed",
            catalog_status="unknown",
            english_dub_status="unknown",
            explicit_dub_evidence_source=None,
            audio_locales=["ja-JP"],
            source_evidence={"query": "candidate", "matches": [{"title": "Candidate Title"}]},
            fetched_at="2026-07-19T00:00:00+00:00",
            expires_at="2026-07-20T00:00:00+00:00",
        )
        self.assertEqual("unknown", initial.catalog_status)
        self.assertEqual("unknown", initial.english_dub_status)
        self.assertEqual([], list_recommendation_provider_eligibility_evidence_for_mal_ids(
            self.db_path,
            [101],
            actionable_only=True,
            now="2026-07-19T12:00:00+00:00",
        ))

        updated = upsert_recommendation_provider_eligibility_evidence(
            self.db_path,
            mal_anime_id=101,
            provider="crunchyroll",
            provider_series_id="cr-series-1",
            provider_title="Verified Title",
            provider_url="https://example.test/watch/cr-series-1",
            identity_match_kind="manual_verified",
            match_confidence=0.98,
            review_status="verified",
            catalog_status="present",
            english_dub_status="present",
            explicit_dub_evidence_source="provider_audio_locale",
            audio_locales=["en-US", "ja-JP"],
            source_evidence={"provider_payload": {"audio_locales": ["en-US", "ja-JP"]}},
            fetched_at="2026-07-19T01:00:00+00:00",
            expires_at="2026-07-20T00:00:00+00:00",
            last_verified_at="2026-07-19T01:01:00+00:00",
        )

        self.assertEqual("Verified Title", updated.provider_title)
        self.assertEqual(["en-US", "ja-JP"], updated.audio_locales)
        self.assertEqual("provider_audio_locale", updated.explicit_dub_evidence_source)
        self.assertEqual({"provider_payload": {"audio_locales": ["en-US", "ja-JP"]}}, updated.source_evidence)
        self.assertEqual(updated, get_recommendation_provider_eligibility_evidence(
            self.db_path,
            mal_anime_id=101,
            provider="crunchyroll",
            provider_series_id="cr-series-1",
        ))
        actionable = list_recommendation_provider_eligibility_evidence_for_mal_ids(
            self.db_path,
            [101],
            actionable_only=True,
            now="2026-07-19T12:00:00+00:00",
        )
        self.assertEqual([updated], actionable)
        self.assertEqual([], list_recommendation_provider_eligibility_evidence_for_mal_ids(
            self.db_path,
            [101],
            actionable_only=True,
            now="2026-07-21T00:00:00+00:00",
        ))

        self.assertEqual(1, mark_stale_recommendation_provider_eligibility_evidence(
            self.db_path,
            now="2026-07-21T00:00:00+00:00",
            provider="crunchyroll",
        ))
        stale = get_recommendation_provider_eligibility_evidence(
            self.db_path,
            mal_anime_id=101,
            provider="crunchyroll",
            provider_series_id="cr-series-1",
        )
        self.assertIsNotNone(stale)
        assert stale is not None
        self.assertEqual("stale", stale.review_status)
        self.assertEqual("stale", stale.catalog_status)
        self.assertEqual("stale", stale.english_dub_status)
        self.assertEqual(1, delete_recommendation_provider_eligibility_evidence(
            self.db_path,
            mal_anime_id=101,
            provider="crunchyroll",
            provider_series_id="cr-series-1",
        ))
        self.assertIsNone(get_recommendation_provider_eligibility_evidence(
            self.db_path,
            mal_anime_id=101,
            provider="crunchyroll",
            provider_series_id="cr-series-1",
        ))

    def test_provider_and_status_validation_do_not_convert_unknown_to_present(self) -> None:
        with self.assertRaises(ValueError):
            upsert_recommendation_provider_eligibility_evidence(
                self.db_path,
                mal_anime_id=1,
                provider="netflix",
                provider_series_id="nf-1",
                fetched_at="2026-07-19T00:00:00+00:00",
                expires_at="2026-07-20T00:00:00+00:00",
            )
        with self.assertRaises(ValueError):
            upsert_recommendation_provider_eligibility_evidence(
                self.db_path,
                mal_anime_id=1,
                provider="hidive",
                provider_series_id="hd-1",
                catalog_status="true",
                fetched_at="2026-07-19T00:00:00+00:00",
                expires_at="2026-07-20T00:00:00+00:00",
            )

        unknown = upsert_recommendation_provider_eligibility_evidence(
            self.db_path,
            mal_anime_id=1,
            provider="hidive",
            provider_series_id="hd-1",
            fetched_at="2026-07-19T00:00:00+00:00",
            expires_at="2026-07-20T00:00:00+00:00",
        )
        self.assertEqual("unknown", unknown.review_status)
        self.assertEqual("unknown", unknown.catalog_status)
        self.assertEqual("unknown", unknown.english_dub_status)
        self.assertEqual([], list_recommendation_provider_eligibility_evidence_for_mal_ids(
            self.db_path,
            [1],
            actionable_only=True,
            now="2026-07-19T12:00:00+00:00",
        ))

    def test_snapshot_availability_confidence_label_and_numeric_round_trip(self) -> None:
        inserted = insert_recommendation_snapshot_rows(
            self.db_path,
            [
                {
                    "kind": "discovery_candidate",
                    "provider": "mal",
                    "title": "Alias Evidence",
                    "provider_series_id": "mal:1",
                    "availability_confidence": "title_alias",
                    "context": {"mal_anime_id": 1},
                },
                {
                    "kind": "discovery_candidate",
                    "provider": "crunchyroll",
                    "title": "Numeric Evidence",
                    "provider_series_id": "cr-2",
                    "availability_confidence": 0.83,
                    "availability_confidence_label": "mapped",
                    "context": {"mal_anime_id": 2},
                },
            ],
            run_id="run-labels",
            generated_at="2026-07-19T00:30:00+00:00",
        )

        self.assertEqual(2, inserted)
        rows = {row.title: row for row in list_latest_recommendation_snapshot_rows(self.db_path, limit=None)}
        self.assertIsNone(rows["Alias Evidence"].availability_confidence)
        self.assertEqual("title_alias", rows["Alias Evidence"].availability_confidence_label)
        self.assertEqual(0.83, rows["Numeric Evidence"].availability_confidence)
        self.assertEqual("mapped", rows["Numeric Evidence"].availability_confidence_label)


if __name__ == "__main__":
    unittest.main()
