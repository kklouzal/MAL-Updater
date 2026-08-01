from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from mal_updater.db import (
    bootstrap_database,
    connect,
    get_provider_title_search_cache,
    upsert_provider_title_search_cache,
)


class ProviderTitleSearchCacheTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.db_path = Path(self.temp.name) / "mal-updater.sqlite3"
        bootstrap_database(self.db_path)

    def tearDown(self) -> None:
        self.temp.cleanup()

    def test_full_key_variants_coexist_and_expire_independently(self) -> None:
        upsert_provider_title_search_cache(
            self.db_path,
            provider="hidive",
            normalized_query="shared title",
            query="Shared Title",
            candidate_mal_anime_id=101,
            candidate_title="Shared Title",
            matches=[{"provider_series_id": "variant-a", "title": "Shared Title A"}],
            status="ok",
            fetched_at="2026-08-01T00:00:00Z",
            expires_at="2026-08-02T00:00:00Z",
            logic_version="provider-title-v2",
            search_limit=5,
            identity_key="mal:101",
        )
        upsert_provider_title_search_cache(
            self.db_path,
            provider="hidive",
            normalized_query="shared title",
            query="Shared Title",
            candidate_mal_anime_id=202,
            candidate_title="Shared Title Other",
            matches=[{"provider_series_id": "variant-b", "title": "Shared Title B"}],
            status="ok",
            fetched_at="2026-08-01T00:00:00Z",
            expires_at="2026-08-05T00:00:00Z",
            logic_version="provider-title-v3",
            search_limit=7,
            identity_key="mal:202",
        )

        with connect(self.db_path) as conn:
            row_count = conn.execute(
                """
                SELECT COUNT(*) FROM provider_title_search_cache
                WHERE provider = 'hidive' AND normalized_query = 'shared title'
                """
            ).fetchone()[0]
        self.assertEqual(2, row_count)

        variant_a = get_provider_title_search_cache(
            self.db_path,
            provider="hidive",
            normalized_query="shared title",
            now="2026-08-01T12:00:00Z",
            logic_version="provider-title-v2",
            search_limit=5,
            identity_key="mal:101",
        )
        variant_b = get_provider_title_search_cache(
            self.db_path,
            provider="hidive",
            normalized_query="shared title",
            now="2026-08-03T00:00:00Z",
            logic_version="provider-title-v3",
            search_limit=7,
            identity_key="mal:202",
        )
        expired_a = get_provider_title_search_cache(
            self.db_path,
            provider="hidive",
            normalized_query="shared title",
            now="2026-08-03T00:00:00Z",
            logic_version="provider-title-v2",
            search_limit=5,
            identity_key="mal:101",
        )

        self.assertIsNotNone(variant_a)
        assert variant_a is not None
        self.assertEqual("variant-a", variant_a.matches[0]["provider_series_id"])
        self.assertIsNotNone(variant_b)
        assert variant_b is not None
        self.assertEqual("variant-b", variant_b.matches[0]["provider_series_id"])
        self.assertIsNone(expired_a)

    def test_upsert_updates_only_the_matching_full_key_variant(self) -> None:
        for identity_key, provider_series_id in (("mal:101", "variant-a"), ("mal:202", "variant-b")):
            upsert_provider_title_search_cache(
                self.db_path,
                provider="hidive",
                normalized_query="shared title",
                query="Shared Title",
                candidate_mal_anime_id=int(identity_key.split(":", 1)[1]),
                candidate_title="Shared Title",
                matches=[{"provider_series_id": provider_series_id, "title": provider_series_id}],
                status="ok",
                fetched_at="2026-08-01T00:00:00Z",
                expires_at="2027-08-01T00:00:00Z",
                logic_version="provider-title-v2",
                search_limit=5,
                identity_key=identity_key,
            )

        upsert_provider_title_search_cache(
            self.db_path,
            provider="hidive",
            normalized_query="shared title",
            query="Shared Title Updated",
            candidate_mal_anime_id=101,
            candidate_title="Shared Title Updated",
            matches=[{"provider_series_id": "variant-a-updated", "title": "Updated"}],
            status="ok",
            fetched_at="2026-08-02T00:00:00Z",
            expires_at="2027-08-02T00:00:00Z",
            logic_version="provider-title-v2",
            search_limit=5,
            identity_key="mal:101",
        )

        updated = get_provider_title_search_cache(
            self.db_path,
            provider="hidive",
            normalized_query="shared title",
            logic_version="provider-title-v2",
            search_limit=5,
            identity_key="mal:101",
        )
        untouched = get_provider_title_search_cache(
            self.db_path,
            provider="hidive",
            normalized_query="shared title",
            logic_version="provider-title-v2",
            search_limit=5,
            identity_key="mal:202",
        )

        self.assertIsNotNone(updated)
        self.assertIsNotNone(untouched)
        assert updated is not None and untouched is not None
        self.assertEqual("variant-a-updated", updated.matches[0]["provider_series_id"])
        self.assertEqual("variant-b", untouched.matches[0]["provider_series_id"])

    def test_lookup_requires_full_key_or_explicit_legacy_mode(self) -> None:
        upsert_provider_title_search_cache(
            self.db_path,
            provider="hidive",
            normalized_query="legacy title",
            query="Legacy Title",
            candidate_mal_anime_id=None,
            candidate_title=None,
            matches=[{"provider_series_id": "legacy"}],
            status="ok",
            fetched_at="2026-08-01T00:00:00Z",
            expires_at="2027-08-01T00:00:00Z",
        )
        upsert_provider_title_search_cache(
            self.db_path,
            provider="hidive",
            normalized_query="legacy title",
            query="Legacy Title",
            candidate_mal_anime_id=101,
            candidate_title="Legacy Title Current",
            matches=[{"provider_series_id": "current"}],
            status="ok",
            fetched_at="2026-08-01T00:00:00Z",
            expires_at="2027-08-01T00:00:00Z",
            logic_version="provider-title-v2",
            search_limit=5,
            identity_key="mal:101",
        )

        with self.assertRaisesRegex(ValueError, "full semantic key"):
            get_provider_title_search_cache(
                self.db_path,
                provider="hidive",
                normalized_query="legacy title",
            )
        with self.assertRaisesRegex(ValueError, "requires logic_version, search_limit, and identity_key together"):
            get_provider_title_search_cache(
                self.db_path,
                provider="hidive",
                normalized_query="legacy title",
                logic_version="provider-title-v2",
            )

        legacy = get_provider_title_search_cache(
            self.db_path,
            provider="hidive",
            normalized_query="legacy title",
            legacy_lookup=True,
        )
        self.assertIsNotNone(legacy)
        assert legacy is not None
        self.assertEqual("legacy-v1", legacy.logic_version)
        self.assertEqual(10, legacy.search_limit)
        self.assertEqual("", legacy.identity_key)
        self.assertEqual("legacy", legacy.matches[0]["provider_series_id"])


if __name__ == "__main__":
    unittest.main()
