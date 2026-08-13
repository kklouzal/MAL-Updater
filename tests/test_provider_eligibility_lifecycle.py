from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from mal_updater.db import (
    bootstrap_database,
    connect,
    list_due_recommendation_provider_eligibility_evidence,
    list_recommendation_provider_eligibility_evidence_for_mal_ids,
    record_recommendation_provider_eligibility_lifecycle_result,
    record_recommendation_provider_eligibility_negative_scope,
    upsert_recommendation_provider_eligibility_evidence,
)
from mal_updater.provider_eligibility_lifecycle import (
    PROVIDER_ELIGIBILITY_REFRESH_SCHEDULE_VERSION,
    provider_eligibility_refresh_due_at,
    provider_eligibility_refresh_schedule_key,
    stable_provider_eligibility_refresh_jitter_days,
    ProviderEligibilityProcessLease,
)


class ProviderEligibilityLifecycleTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.temp_dir.name) / "eligibility.sqlite3"
        bootstrap_database(self.db_path)

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def test_stable_jitter_is_bounded_and_independent_of_python_hash_seed(self) -> None:
        kwargs = {"mal_anime_id": 101, "provider": "Crunchyroll", "provider_series_id": "series/日本語"}
        local_value = stable_provider_eligibility_refresh_jitter_days(**kwargs)
        self.assertGreaterEqual(local_value, -15)
        self.assertLessEqual(local_value, 15)
        code = (
            "import json; from mal_updater.provider_eligibility_lifecycle import "
            "stable_provider_eligibility_refresh_jitter_days as f; "
            f"print(json.dumps(f(**{kwargs!r})))"
        )
        outputs = []
        for seed in ("1", "999"):
            env = dict(os.environ)
            env["PYTHONHASHSEED"] = seed
            env["PYTHONPATH"] = str(Path(__file__).resolve().parents[1] / "src")
            outputs.append(json.loads(subprocess.check_output([sys.executable, "-c", code], text=True, env=env)))
        self.assertEqual([local_value, local_value], outputs)

    def test_due_date_is_105_to_135_days_and_zero_explicitly_disables(self) -> None:
        anchor = datetime(2026, 1, 1, tzinfo=timezone.utc)
        due = provider_eligibility_refresh_due_at(
            successful_verified_at=anchor,
            mal_anime_id=101,
            provider="crunchyroll",
            provider_series_id="cr-101",
        )
        self.assertIsNotNone(due)
        assert due is not None
        due_dt = datetime.fromisoformat(due.replace("Z", "+00:00"))
        self.assertGreaterEqual((due_dt - anchor).days, 105)
        self.assertLessEqual((due_dt - anchor).days, 135)
        self.assertIsNone(provider_eligibility_refresh_due_at(
            successful_verified_at=anchor,
            mal_anime_id=101,
            provider="crunchyroll",
            provider_series_id="cr-101",
            target_days=0,
        ))

    def _positive_row(self) -> tuple[str, str]:
        verified_at = "2026-01-01T00:00:00Z"
        schedule_key = provider_eligibility_refresh_schedule_key(
            mal_anime_id=101, provider="crunchyroll", provider_series_id="cr-101"
        )
        due = provider_eligibility_refresh_due_at(
            successful_verified_at=verified_at,
            mal_anime_id=101,
            provider="crunchyroll",
            provider_series_id="cr-101",
        )
        assert due is not None
        upsert_recommendation_provider_eligibility_evidence(
            self.db_path,
            mal_anime_id=101,
            provider="crunchyroll",
            provider_series_id="cr-101",
            identity_match_kind="provider_title_search_exact",
            review_status="verified",
            catalog_status="present",
            english_dub_status="present",
            audio_locales=["en-US"],
            fetched_at=verified_at,
            expires_at="2026-01-08T00:00:00Z",
            last_verified_at=verified_at,
            logic_version="provider-eligibility-v1",
            verification_outcome="positive",
            refresh_due_at=due,
            refresh_schedule_version=PROVIDER_ELIGIBILITY_REFRESH_SCHEDULE_VERSION,
            refresh_schedule_key=schedule_key,
            last_successful_positive_at=verified_at,
        )
        return due, schedule_key

    def test_due_and_failed_refresh_keep_positive_actionable_until_contradicted(self) -> None:
        due, schedule_key = self._positive_row()
        actionable = list_recommendation_provider_eligibility_evidence_for_mal_ids(
            self.db_path, [101], actionable_only=True, now="2027-01-01T00:00:00Z"
        )
        self.assertEqual(1, len(actionable))
        self.assertEqual([101], [row.mal_anime_id for row in list_due_recommendation_provider_eligibility_evidence(
            self.db_path, provider="crunchyroll", now=due, limit=2
        )])

        failed = record_recommendation_provider_eligibility_lifecycle_result(
            self.db_path,
            mal_anime_id=101,
            provider="crunchyroll",
            provider_series_id="cr-101",
            outcome="failed",
            attempted_at=due,
            next_retry_at="2027-01-02T00:00:00Z",
        )
        self.assertEqual("positive", failed.verification_outcome)
        self.assertEqual("2026-01-01T00:00:00Z", failed.last_successful_positive_at)
        self.assertEqual(schedule_key, failed.refresh_schedule_key)
        self.assertEqual(due, failed.refresh_due_at)
        self.assertEqual(1, len(list_recommendation_provider_eligibility_evidence_for_mal_ids(
            self.db_path, [101], actionable_only=True, now="2027-01-01T00:00:00Z"
        )))

        contradicted = record_recommendation_provider_eligibility_lifecycle_result(
            self.db_path,
            mal_anime_id=101,
            provider="crunchyroll",
            provider_series_id="cr-101",
            outcome="negative",
            attempted_at="2027-01-03T00:00:00Z",
            refresh_due_at="2027-05-01T00:00:00Z",
            invalidation_reason="successful_affirmative_no_match",
        )
        self.assertEqual("negative", contradicted.verification_outcome)
        self.assertEqual("2027-01-03T00:00:00Z", contradicted.invalidated_at)
        self.assertEqual([], list_recommendation_provider_eligibility_evidence_for_mal_ids(
            self.db_path, [101], actionable_only=True, now="2027-01-03T00:00:00Z"
        ))

    def test_affirmative_negative_atomically_revokes_prior_positive_scope(self) -> None:
        self._positive_row()
        contradicted = record_recommendation_provider_eligibility_negative_scope(
            self.db_path,
            mal_anime_id=101,
            provider="crunchyroll",
            attempted_at="2027-01-03T00:00:00Z",
            expires_at="2027-01-10T00:00:00Z",
            refresh_due_at="2027-05-03T00:00:00Z",
            refresh_schedule_version=PROVIDER_ELIGIBILITY_REFRESH_SCHEDULE_VERSION,
            refresh_schedule_key="sha256:negative-scope",
            invalidation_reason="successful_affirmative_no_match",
            source_evidence={"result": "no_acceptable_match"},
            logic_version="provider-eligibility-v1",
        )
        self.assertEqual(1, contradicted)
        self.assertEqual([], list_recommendation_provider_eligibility_evidence_for_mal_ids(
            self.db_path, [101], actionable_only=True, now="2027-01-03T00:00:00Z"
        ))

    def test_provider_specific_process_lease_reports_busy(self) -> None:
        lease_dir = Path(self.temp_dir.name) / "leases"
        first = ProviderEligibilityProcessLease(lease_dir, "crunchyroll")
        second = ProviderEligibilityProcessLease(lease_dir, "crunchyroll")
        self.assertTrue(first.try_acquire())
        try:
            self.assertFalse(second.try_acquire())
            self.assertEqual("lease_busy", second.status["reason"])
        finally:
            first.release()

    def test_migration_backfills_outcome_and_deterministic_schedule_from_last_success(self) -> None:
        original = __import__("mal_updater.db", fromlist=["MIGRATIONS"]).MIGRATIONS
        import mal_updater.db as db

        old_path = Path(self.temp_dir.name) / "v19.sqlite3"
        try:
            db.MIGRATIONS = original[: db.MIGRATION_FILENAMES.index(db.PROVIDER_ELIGIBILITY_REFRESH_LIFECYCLE_MIGRATION)]
            bootstrap_database(old_path)
        finally:
            db.MIGRATIONS = original
        with connect(old_path) as conn:
            conn.execute(
                """
                INSERT INTO recommendation_provider_eligibility_evidence (
                    mal_anime_id, provider, provider_series_id, review_status,
                    catalog_status, english_dub_status, fetched_at, expires_at,
                    last_verified_at
                ) VALUES (202, 'hidive', 'hd-202', 'verified', 'present', 'present', ?, ?, ?)
                """,
                ("2026-02-01T00:00:00Z", "2026-02-08T00:00:00Z", "2026-02-01T00:00:00Z"),
            )
            conn.commit()
        bootstrap_database(old_path)
        with connect(old_path) as conn:
            row = conn.execute(
                "SELECT * FROM recommendation_provider_eligibility_evidence WHERE mal_anime_id = 202"
            ).fetchone()
            indexes = {index["name"] for index in conn.execute("PRAGMA index_list(recommendation_provider_eligibility_evidence)")}
        assert row is not None
        self.assertEqual("positive", row["verification_outcome"])
        self.assertEqual("2026-02-01T00:00:00Z", row["last_successful_positive_at"])
        self.assertEqual(PROVIDER_ELIGIBILITY_REFRESH_SCHEDULE_VERSION, row["refresh_schedule_version"])
        self.assertEqual(
            provider_eligibility_refresh_due_at(
                successful_verified_at="2026-02-01T00:00:00Z",
                mal_anime_id=202,
                provider="hidive",
                provider_series_id="hd-202",
            ),
            row["refresh_due_at"],
        )
        self.assertIn("idx_recommendation_eligibility_refresh_due", indexes)
        self.assertIn("idx_recommendation_eligibility_last_known_positive", indexes)


if __name__ == "__main__":
    unittest.main()
