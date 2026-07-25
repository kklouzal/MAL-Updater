from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from mal_updater.db import (
    apply_migrations,
    bootstrap_database,
    connect,
    get_recommendation_provider_enrichment_cursor,
    get_recommendation_provider_enrichment_progress,
    list_recommendation_provider_enrichment_attempts,
    record_recommendation_provider_enrichment_attempt,
    update_recommendation_provider_enrichment_attempt_outcome,
)


class RecommendationProviderEnrichmentCursorDbTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.temp_dir.name) / "mal-updater.sqlite3"
        bootstrap_database(self.db_path)

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def test_fresh_bootstrap_applies_provider_cursor_schema_and_catalog_marker(self) -> None:
        with connect(self.db_path) as conn:
            cursor_columns = {
                row["name"]
                for row in conn.execute("PRAGMA table_info(recommendation_provider_enrichment_cursor)")
            }
            attempt_columns = {
                row["name"]
                for row in conn.execute("PRAGMA table_info(recommendation_provider_enrichment_attempts)")
            }
            indexes = {
                row["name"]
                for row in conn.execute("PRAGMA index_list(recommendation_provider_enrichment_attempts)")
            }
            migration_rows = conn.execute(
                """
                SELECT COUNT(*) AS n
                FROM schema_migrations
                WHERE version = '014_recommendation_provider_enrichment_cursor.sql'
                """
            ).fetchone()["n"]
            apply_migrations(conn)
            apply_migrations(conn)
            migration_rows_after_reapply = conn.execute(
                """
                SELECT COUNT(*) AS n
                FROM schema_migrations
                WHERE version = '014_recommendation_provider_enrichment_cursor.sql'
                """
            ).fetchone()["n"]

        self.assertEqual(
            {
                "provider",
                "cursor_mal_anime_id",
                "cursor_rank_key_json",
                "cursor_generation",
                "wrapped_at",
                "last_attempted_mal_anime_id",
                "last_attempted_rank_key_json",
                "last_attempted_at",
                "last_selection_class",
                "last_outcome",
                "created_at",
                "updated_at",
            },
            cursor_columns,
        )
        self.assertEqual(
            {
                "provider",
                "mal_anime_id",
                "rank_key_json",
                "selection_class",
                "attempted_at",
                "attempt_count",
                "last_outcome",
                "created_at",
                "updated_at",
            },
            attempt_columns,
        )
        self.assertIn("idx_recommendation_provider_enrichment_attempts_provider_time", indexes)
        self.assertEqual(1, migration_rows)
        self.assertEqual(1, migration_rows_after_reapply)

    def test_attempt_progress_persists_and_round_trips_rank_key(self) -> None:
        self.assertIsNone(
            get_recommendation_provider_enrichment_cursor(self.db_path, provider="Crunchyroll")
        )

        cursor = record_recommendation_provider_enrichment_attempt(
            self.db_path,
            provider=" Crunchyroll ",
            mal_anime_id=101,
            rank_key={"rank": 3, "mal_anime_id": 101, "priority": 7},
            selection_class="uncovered",
            attempted_at="2026-07-25T22:20:00+00:00",
            outcome="no_match",
        )

        self.assertEqual("crunchyroll", cursor.provider)
        self.assertEqual(101, cursor.cursor_mal_anime_id)
        self.assertEqual(1, cursor.cursor_generation)
        self.assertIsNone(cursor.wrapped_at)
        self.assertEqual(101, cursor.last_attempted_mal_anime_id)
        self.assertEqual("uncovered", cursor.last_selection_class)
        self.assertEqual("no_match", cursor.last_outcome)
        self.assertEqual({"mal_anime_id": 101, "priority": 7, "rank": 3}, cursor.cursor_rank_key)
        self.assertEqual(cursor.cursor_rank_key, cursor.last_attempted_rank_key)

        reopened_cursor = get_recommendation_provider_enrichment_cursor(
            self.db_path,
            provider="crunchyroll",
        )
        self.assertEqual(cursor, reopened_cursor)
        attempts = list_recommendation_provider_enrichment_attempts(
            self.db_path,
            provider="crunchyroll",
        )
        self.assertEqual(1, len(attempts))
        self.assertEqual(101, attempts[0].mal_anime_id)
        self.assertEqual(1, attempts[0].attempt_count)
        self.assertEqual("2026-07-25T22:20:00+00:00", attempts[0].attempted_at)
        self.assertEqual("uncovered", attempts[0].selection_class)
        self.assertEqual("no_match", attempts[0].last_outcome)
        self.assertEqual({"mal_anime_id": 101, "priority": 7, "rank": 3}, attempts[0].rank_key)

    def test_same_mal_candidate_is_tracked_independently_per_provider(self) -> None:
        record_recommendation_provider_enrichment_attempt(
            self.db_path,
            provider="crunchyroll",
            mal_anime_id=202,
            rank_key={"rank": 1, "mal_anime_id": 202},
            selection_class="uncovered",
            attempted_at="2026-07-25T22:21:00+00:00",
            outcome="matched_present",
        )
        record_recommendation_provider_enrichment_attempt(
            self.db_path,
            provider="hidive",
            mal_anime_id=202,
            rank_key={"rank": 1, "mal_anime_id": 202},
            selection_class="expired_refresh_due",
            attempted_at="2026-07-25T22:22:00+00:00",
            outcome="no_match",
        )

        self.assertEqual(
            1,
            update_recommendation_provider_enrichment_attempt_outcome(
                self.db_path,
                provider="crunchyroll",
                mal_anime_id=202,
                outcome="verified_present",
            ),
        )
        crunchyroll_progress = get_recommendation_provider_enrichment_progress(
            self.db_path,
            provider="crunchyroll",
            mal_anime_ids=[202],
        )
        hidive_progress = get_recommendation_provider_enrichment_progress(
            self.db_path,
            provider="hidive",
            mal_anime_ids=[202],
        )

        self.assertEqual("crunchyroll", crunchyroll_progress.provider)
        self.assertEqual("hidive", hidive_progress.provider)
        self.assertEqual("verified_present", crunchyroll_progress.attempts_by_mal_anime_id[202].last_outcome)
        self.assertEqual("verified_present", crunchyroll_progress.cursor.last_outcome)
        self.assertEqual("no_match", hidive_progress.attempts_by_mal_anime_id[202].last_outcome)
        self.assertEqual("no_match", hidive_progress.cursor.last_outcome)

    def test_cursor_advances_on_repeated_outcomes_and_preserves_wrap_progress(self) -> None:
        first = record_recommendation_provider_enrichment_attempt(
            self.db_path,
            provider="crunchyroll",
            mal_anime_id=301,
            rank_key={"rank": 10, "mal_anime_id": 301},
            selection_class="uncovered",
            attempted_at="2026-07-25T22:23:00+00:00",
            outcome="no_match",
        )
        self.assertEqual(1, first.cursor_generation)
        self.assertIsNone(first.wrapped_at)

        wrapped = record_recommendation_provider_enrichment_attempt(
            self.db_path,
            provider="crunchyroll",
            mal_anime_id=302,
            rank_key={"rank": 1, "mal_anime_id": 302},
            selection_class="failed_retry_due",
            attempted_at="2026-07-25T22:24:00+00:00",
            outcome="provider_failure",
            wrapped=True,
        )
        self.assertEqual(2, wrapped.cursor_generation)
        self.assertEqual(302, wrapped.cursor_mal_anime_id)
        self.assertEqual("2026-07-25T22:24:00+00:00", wrapped.wrapped_at)
        self.assertEqual("provider_failure", wrapped.last_outcome)

        reranked = record_recommendation_provider_enrichment_attempt(
            self.db_path,
            provider="crunchyroll",
            mal_anime_id=302,
            rank_key={"rank": 0, "mal_anime_id": 302},
            selection_class="logic_refresh_due",
            attempted_at="2026-07-25T22:25:00+00:00",
            outcome="verified_absent",
        )
        self.assertEqual(3, reranked.cursor_generation)
        self.assertEqual("2026-07-25T22:24:00+00:00", reranked.wrapped_at)
        self.assertEqual({"mal_anime_id": 302, "rank": 0}, reranked.cursor_rank_key)
        self.assertEqual("verified_absent", reranked.last_outcome)

        progress = get_recommendation_provider_enrichment_progress(
            self.db_path,
            provider="crunchyroll",
            mal_anime_ids=[301, 302, 999],
        )
        self.assertEqual({301, 302}, set(progress.attempts_by_mal_anime_id))
        self.assertEqual(1, progress.attempts_by_mal_anime_id[301].attempt_count)
        self.assertEqual(2, progress.attempts_by_mal_anime_id[302].attempt_count)
        self.assertEqual("logic_refresh_due", progress.attempts_by_mal_anime_id[302].selection_class)
        self.assertEqual("2026-07-25T22:25:00+00:00", progress.attempts_by_mal_anime_id[302].attempted_at)
        self.assertEqual({"mal_anime_id": 302, "rank": 0}, progress.attempts_by_mal_anime_id[302].rank_key)

    def test_validation_rejects_empty_provider_selection_class_timestamp_and_outcome(self) -> None:
        valid_args = {
            "provider": "crunchyroll",
            "mal_anime_id": 401,
            "rank_key": {"rank": 1},
            "selection_class": "uncovered",
            "attempted_at": "2026-07-25T22:26:00+00:00",
            "outcome": "no_match",
        }
        for override in [
            {"provider": "   "},
            {"selection_class": "   "},
            {"attempted_at": "   "},
            {"outcome": "   "},
        ]:
            with self.assertRaises(ValueError):
                record_recommendation_provider_enrichment_attempt(
                    self.db_path,
                    **{**valid_args, **override},
                )


if __name__ == "__main__":
    unittest.main()
