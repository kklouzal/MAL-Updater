from __future__ import annotations

import sqlite3
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

from mal_updater.config import load_config
from mal_updater.db import bootstrap_database, get_recommendation_provider_eligibility_evidence, upsert_mal_anime_metadata, upsert_series_mapping
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

    def test_selects_english_aliases_before_romaji_fallback(self):
        meta = SimpleNamespace(
            title="Romaji Main",
            title_english="English Main",
            title_japanese="日本語メイン",
            raw={"alternative_titles": {"en": "English Alt", "synonyms": ["Synonym One", "Synonym Two"]}},
        )

        self.assertEqual(enrichment.select_english_provider_search_queries(meta), ["English Main", "English Alt", "Synonym One", "Synonym Two"])

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

    def test_strong_exact_match_queues_review_without_auto_mapping(self):
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
            mapped = conn.execute("SELECT COUNT(*) FROM mal_series_mapping WHERE provider = 'fake'").fetchone()[0]
            review = conn.execute("SELECT COUNT(*) FROM review_queue WHERE issue_type = 'discovery_provider_search_match_review' AND provider = 'fake'").fetchone()[0]
        self.assertEqual(mapped, 0)
        self.assertEqual(review, 2)

    def test_title_only_english_dub_marker_does_not_create_dub_evidence(self):
        self._insert_meta(212, english="Dub Marker Only")
        self._recommendations(212)
        provider = FakeProvider([
            {
                "provider_series_id": "cr-title-only-dub",
                "title": "Dub Marker Only (English Dub)",
                "season_title": "Dub Marker Only (English Dub)",
                "audio_locales": [],
            }
        ])

        summary = enrichment.enrich_discovery_provider_availability(
            self.config,
            providers=[provider],
            candidate_limit=1,
            now=datetime(2026, 1, 1, tzinfo=timezone.utc),
        )

        self.assertEqual(summary.strong_matches, 1)
        with sqlite3.connect(self.config.db_path) as conn:
            info_rows = conn.execute(
                "SELECT COUNT(*) FROM review_queue WHERE issue_type = 'discovery_provider_search_match_review' AND severity = 'info'"
            ).fetchone()[0]
            dub_rows = conn.execute(
                "SELECT COUNT(*) FROM review_queue WHERE issue_type = 'discovery_provider_search_match_review' AND payload_json LIKE '%strong_english_dub_evidence%'"
            ).fetchone()[0]
        self.assertEqual(info_rows, 1, "strong title-only matches may be queued for review")
        self.assertEqual(dub_rows, 0, "title-only English Dub labels are not explicit audio-locale evidence")

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
            mapped = conn.execute("SELECT COUNT(*) FROM mal_series_mapping WHERE provider = 'fake'").fetchone()[0]
            review = conn.execute("SELECT COUNT(*) FROM review_queue WHERE issue_type = 'discovery_provider_search_match_review' AND provider = 'fake'").fetchone()[0]
        self.assertEqual(mapped, 0)
        self.assertEqual(review, 1)

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

    def test_hidive_provider_search_result_queues_review_without_mapping(self):
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
            mapped = conn.execute("SELECT COUNT(*) FROM mal_series_mapping WHERE provider = 'hidive'").fetchone()[0]
            review = conn.execute("SELECT COUNT(*) FROM review_queue WHERE issue_type = 'discovery_provider_search_match_review' AND provider = 'hidive'").fetchone()[0]
        self.assertEqual(provider_row, ("hidive", "2312", "Dungeon People"))
        self.assertEqual(mapped, 0)
        self.assertEqual(review, 1)

    def test_enrichment_persists_review_needed_then_verified_actionable_eligibility_evidence(self):
        self._insert_meta(707, english="Eligibility Show")
        self._recommendations(707)
        provider = FakeProvider([
            ProviderSearchResult(
                provider_series_id="707",
                title="Eligibility Show",
                season_title="Eligibility Show",
                url="https://www.hidive.com/season/707",
                audio_locales=["en-US", "ja-JP"],
                raw={"tags": ["Audio|English", "Audio|Japanese"]},
            )
        ])
        provider.slug = "hidive"
        now = datetime(2026, 7, 19, tzinfo=timezone.utc)

        summary = enrichment.enrich_discovery_provider_availability(
            self.config,
            providers=[provider],
            candidate_limit=1,
            now=now,
        )
        evidence = get_recommendation_provider_eligibility_evidence(
            self.config.db_path,
            mal_anime_id=707,
            provider="hidive",
            provider_series_id="707",
        )
        self.assertEqual(1, summary.eligibility_evidence_upserted)
        self.assertEqual(0, summary.verified_eligibility_evidence_upserted)
        self.assertIsNotNone(evidence)
        assert evidence is not None
        self.assertEqual("review-needed", evidence.review_status)
        self.assertEqual("unknown", evidence.catalog_status)
        self.assertEqual("unknown", evidence.english_dub_status)
        self.assertEqual("provider_audio_tag", evidence.explicit_dub_evidence_source)
        self.assertEqual("2026-07-26T00:00:00Z", evidence.expires_at)

        upsert_series_mapping(
            self.config.db_path,
            provider="hidive",
            provider_series_id="707",
            mal_anime_id=707,
            confidence=0.99,
            mapping_source="user_exact",
            approved_by_user=True,
            notes=None,
        )
        summary = enrichment.enrich_discovery_provider_availability(
            self.config,
            providers=[provider],
            candidate_limit=1,
            now=now + timedelta(hours=1),
        )
        evidence = get_recommendation_provider_eligibility_evidence(
            self.config.db_path,
            mal_anime_id=707,
            provider="hidive",
            provider_series_id="707",
        )
        self.assertEqual(1, summary.verified_eligibility_evidence_upserted)
        self.assertIsNotNone(evidence)
        assert evidence is not None
        self.assertEqual("approved_mapping", evidence.identity_match_kind)
        self.assertEqual("verified", evidence.review_status)
        self.assertEqual("present", evidence.catalog_status)
        self.assertEqual("present", evidence.english_dub_status)
        self.assertEqual("2026-07-26T01:00:00Z", evidence.expires_at)

    def test_hidive_false_negative_aliases_are_searched_and_queued(self):
        fixtures = [
            (601, "Seireitsukai no Blade Dance", "Blade Dance of the Elementalers", "1180"),
            (602, "Rakudai Kishi no Cavalry", "Chivalry of a Failed Knight", "1277"),
            (603, "Vinland Saga", "VINLAND SAGA", "1249"),
            (604, "Dororo", "Dororo", "1181"),
        ]
        for mal_id, romaji, alias, _provider_id in fixtures:
            self._insert_meta(mal_id, title=romaji, english=None, synonyms=[alias])
        self._recommendations(*(mal_id for mal_id, *_ in fixtures))

        class AliasProvider(FakeProvider):
            slug = "hidive"

            def search_title(self, config, query: str, *, limit: int = 10):
                self.calls.append((query, limit))
                for _mal_id, _romaji, alias, provider_id in fixtures:
                    if query == alias:
                        return [ProviderSearchResult(provider_series_id=provider_id, title=alias, season_title=alias, raw={"type": "VOD_SERIES"})]
                return []

        provider = AliasProvider([])
        summary = enrichment.enrich_discovery_provider_availability(
            self.config,
            providers=[provider],
            candidate_limit=10,
            now=datetime(2026, 1, 1, tzinfo=timezone.utc),
        )

        self.assertEqual([call[0] for call in provider.calls], [alias for _mal_id, _romaji, alias, _provider_id in fixtures])
        self.assertEqual(summary.strong_matches, 4)
        with sqlite3.connect(self.config.db_path) as conn:
            mapped = conn.execute("SELECT COUNT(*) FROM mal_series_mapping WHERE provider = 'hidive'").fetchone()[0]
            review = conn.execute("SELECT COUNT(*) FROM review_queue WHERE issue_type = 'discovery_provider_search_match_review' AND provider = 'hidive'").fetchone()[0]
        self.assertEqual(mapped, 0)
        self.assertEqual(review, 4)

    def test_crunchyroll_false_negative_english_titles_are_preferred(self):
        titles = [
            "Akame ga Kill!",
            "The World God Only Knows",
            "Tokyo Revengers",
            "DARLING in the FRANXX",
            "Naruto",
            "PSYCHO-PASS",
            "VINLAND SAGA",
            "SPY x FAMILY",
            "One Piece",
            "BOCCHI THE ROCK!",
        ]
        for index, title in enumerate(titles, start=701):
            self._insert_meta(index, title=f"Romaji {index}", english=title)
        self._recommendations(*range(701, 701 + len(titles)))

        class CrunchyProvider(FakeProvider):
            slug = "crunchyroll"

            def search_title(self, config, query: str, *, limit: int = 10):
                self.calls.append((query, limit))
                return [{"provider_series_id": f"cr-{normalize}", "title": query, "audio_locales": []} for normalize in [query.lower().replace(" ", "-")]]

        provider = CrunchyProvider([])
        summary = enrichment.enrich_discovery_provider_availability(
            self.config,
            providers=[provider],
            candidate_limit=20,
            now=datetime(2026, 1, 1, tzinfo=timezone.utc),
        )

        self.assertEqual([call[0] for call in provider.calls], titles)
        self.assertEqual(summary.strong_matches, len(titles))
        with sqlite3.connect(self.config.db_path) as conn:
            mapped = conn.execute("SELECT COUNT(*) FROM mal_series_mapping WHERE provider = 'crunchyroll'").fetchone()[0]
        self.assertEqual(mapped, 0)


if __name__ == "__main__":
    unittest.main()
