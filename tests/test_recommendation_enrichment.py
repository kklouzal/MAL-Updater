from __future__ import annotations

import sqlite3
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

from mal_updater.config import load_config
from mal_updater.db import bootstrap_database, upsert_mal_anime_metadata
from mal_updater.provider_registry import get_provider
from mal_updater.provider_types import ProviderSearchResult
from mal_updater import providers as _providers  # noqa: F401 - register provider instances
from mal_updater import recommendation_enrichment as enrichment


class FakeProvider:
    slug = "fake"

    def __init__(self, matches):
        self.matches = matches
        self.calls = []

    def search_title(self, config, query: str, *, limit: int = 10):
        self.calls.append((query, limit))
        return list(self.matches)


class RecommendationEnrichmentTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.root = Path(self.tempdir.name)
        self.config = load_config(self.root)
        bootstrap_database(self.config.db_path)
        self._original_build_recommendations = enrichment.build_recommendations

    def tearDown(self) -> None:
        enrichment.build_recommendations = self._original_build_recommendations
        self.tempdir.cleanup()

    def _insert_meta(
        self,
        mal_id: int = 101,
        *,
        title: str = "Romaji Title",
        english: str | None = "English Title",
        japanese: str | None = "日本語タイトル",
        alt_en: str | None = None,
        synonyms: list[str] | None = None,
    ) -> None:
        raw = {"id": mal_id, "title": title, "alternative_titles": {}}
        if alt_en is not None:
            raw["alternative_titles"]["en"] = alt_en
        if synonyms is not None:
            raw["alternative_titles"]["synonyms"] = synonyms
        upsert_mal_anime_metadata(
            self.config.db_path,
            mal_anime_id=mal_id,
            title=title,
            title_english=english,
            title_japanese=japanese,
            alternative_titles=list(synonyms or []),
            media_type=None,
            status=None,
            num_episodes=None,
            mean=None,
            popularity=None,
            start_season=None,
            raw=raw,
        )

    def _recommendations(self, *mal_ids: int):
        enrichment.build_recommendations = lambda *args, **kwargs: [
            SimpleNamespace(kind="discovery_candidate", context={"mal_anime_id": mal_id}) for mal_id in mal_ids
        ]

    def _cache_row(self):
        with sqlite3.connect(self.config.db_path) as conn:
            conn.row_factory = sqlite3.Row
            return conn.execute("SELECT * FROM provider_title_search_cache").fetchone()


    def test_registered_provider_instances_expose_search_title(self):
        self.assertTrue(callable(getattr(get_provider("crunchyroll"), "search_title", None)))
        self.assertTrue(callable(getattr(get_provider("hidive"), "search_title", None)))

    def test_selects_english_title_only_not_japanese_romaji_or_synonyms(self):
        meta = SimpleNamespace(
            title="Romaji Main",
            title_english="English Main",
            title_japanese="日本語メイン",
            raw={"alternative_titles": {"en": "English Alt", "synonyms": ["Synonym One", "Synonym Two"]}},
        )

        self.assertEqual(enrichment.select_english_provider_search_queries(meta), ["English Main", "English Alt"])

    def test_cache_miss_hit_and_365_day_expiry_prevent_repeated_search(self):
        self._insert_meta(101, english="Frieren")
        self._recommendations(101)
        provider = FakeProvider([
            {"provider_series_id": "cr-frieren", "title": "Frieren", "audio_locales": ["en-US"]}
        ])
        now = datetime(2026, 1, 1, tzinfo=timezone.utc)

        first = enrichment.enrich_discovery_provider_availability(
            self.config,
            providers=[provider],
            candidate_limit=1,
            now=now,
        )
        row = self._cache_row()
        self.assertEqual(first.cache_misses, 1)
        self.assertEqual(first.provider_searches, 1)
        self.assertEqual(len(provider.calls), 1)
        self.assertEqual(row["query"], "Frieren")
        self.assertEqual(row["expires_at"], "2027-01-01T00:00:00Z")

        second = enrichment.enrich_discovery_provider_availability(
            self.config,
            providers=[provider],
            candidate_limit=1,
            now=now + timedelta(days=364),
        )
        self.assertEqual(second.cache_hits, 1)
        self.assertEqual(second.cache_misses, 0)
        self.assertEqual(len(provider.calls), 1, "provider must not be searched again within TTL")

        third = enrichment.enrich_discovery_provider_availability(
            self.config,
            providers=[provider],
            candidate_limit=1,
            now=now + timedelta(days=366),
        )
        self.assertEqual(third.cache_misses, 1)
        self.assertEqual(third.provider_searches, 1)
        self.assertEqual(len(provider.calls), 2)

    def test_strong_exact_match_auto_enriches_series_mapping(self):
        self._insert_meta(202, english="Delicious in Dungeon")
        self._recommendations(202)
        provider = FakeProvider([
            {"provider_series_id": "cr-dungeon", "title": "Delicious in Dungeon", "audio_locales": ["en-US"]}
        ])

        summary = enrichment.enrich_discovery_provider_availability(
            self.config,
            providers=[provider],
            candidate_limit=1,
            now=datetime(2026, 1, 1, tzinfo=timezone.utc),
        )

        self.assertEqual(summary.strong_matches, 1)
        with sqlite3.connect(self.config.db_path) as conn:
            row = conn.execute(
                "SELECT provider_series_id, mal_anime_id, approved_by_user, mapping_source FROM mal_series_mapping WHERE provider = 'fake'"
            ).fetchone()
        self.assertEqual(row, ("cr-dungeon", 202, 0, "reverse_provider_title_search"))

    def test_single_near_exact_match_auto_enriches_series_mapping(self):
        self._insert_meta(303, english="The Apothecary Diaries")
        self._recommendations(303)
        provider = FakeProvider([
            {"provider_series_id": "cr-apothecary", "title": "The Apothecary Diaries Season 1", "audio_locales": []}
        ])

        summary = enrichment.enrich_discovery_provider_availability(
            self.config,
            providers=[provider],
            candidate_limit=1,
            now=datetime(2026, 1, 1, tzinfo=timezone.utc),
        )

        self.assertEqual(summary.strong_matches, 1)
        with sqlite3.connect(self.config.db_path) as conn:
            row = conn.execute("SELECT provider_series_id, mal_anime_id FROM mal_series_mapping WHERE provider = 'fake'").fetchone()
        self.assertEqual(row, ("cr-apothecary", 303))

    def test_ambiguous_multiple_or_weak_match_goes_to_review_without_auto_map(self):
        self._insert_meta(404, english="Kaina")
        self._recommendations(404)
        provider = FakeProvider([
            {"provider_series_id": "cr-kaina-a", "title": "Kaina", "audio_locales": []},
            {"provider_series_id": "cr-kaina-b", "title": "Kaina", "audio_locales": []},
        ])

        summary = enrichment.enrich_discovery_provider_availability(
            self.config,
            providers=[provider],
            candidate_limit=1,
            now=datetime(2026, 1, 1, tzinfo=timezone.utc),
        )

        self.assertEqual(summary.ambiguous_matches, 1)
        with sqlite3.connect(self.config.db_path) as conn:
            mapped = conn.execute("SELECT COUNT(*) FROM mal_series_mapping WHERE provider = 'fake'").fetchone()[0]
            review = conn.execute("SELECT COUNT(*) FROM review_queue WHERE issue_type = 'discovery_provider_search_match_review'").fetchone()[0]
        self.assertEqual(mapped, 0)
        self.assertEqual(review, 1)

    def test_hidive_provider_search_result_can_seed_mapping_backed_availability(self):
        self._insert_meta(505, english="Dungeon People")
        self._recommendations(505)
        provider = FakeProvider([
            ProviderSearchResult(provider_series_id="2312", title="Dungeon People", season_title="Dungeon People", raw={"type": "VOD_SERIES"})
        ])
        provider.slug = "hidive"

        summary = enrichment.enrich_discovery_provider_availability(
            self.config,
            providers=[provider],
            candidate_limit=1,
            now=datetime(2026, 1, 1, tzinfo=timezone.utc),
        )

        self.assertEqual(summary.strong_matches, 1)
        with sqlite3.connect(self.config.db_path) as conn:
            provider_row = conn.execute("SELECT provider, provider_series_id, title FROM provider_series WHERE provider = 'hidive'").fetchone()
            mapping_row = conn.execute("SELECT provider, provider_series_id, mal_anime_id, mapping_source FROM mal_series_mapping WHERE provider = 'hidive'").fetchone()
        self.assertEqual(provider_row, ("hidive", "2312", "Dungeon People"))
        self.assertEqual(mapping_row, ("hidive", "2312", 505, "reverse_provider_title_search"))


if __name__ == "__main__":
    unittest.main()
