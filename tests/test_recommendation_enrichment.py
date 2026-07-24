from __future__ import annotations

import json
import sqlite3
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

from mal_updater.config import load_config
from mal_updater.db import bootstrap_database, get_recommendation_provider_eligibility_evidence, get_series_mapping, upsert_mal_anime_metadata, upsert_series_mapping
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
        alt_ja: str | None = None,
        synonyms: list[str] | None = None,
        num_episodes: int | None = None,
        start_year: int | None = None,
        synopsis: str | None = None,
    ) -> None:
        raw = {"id": mal_id, "title": title, "alternative_titles": {}}
        if alt_en is not None:
            raw["alternative_titles"]["en"] = alt_en
        if alt_ja is not None:
            raw["alternative_titles"]["ja"] = alt_ja
        if synonyms is not None:
            raw["alternative_titles"]["synonyms"] = synonyms
        start_season = {"year": start_year, "season": "unknown"} if start_year is not None else None
        if start_season is not None:
            raw["start_season"] = start_season
        if num_episodes is not None:
            raw["num_episodes"] = num_episodes
        if synopsis is not None:
            raw["synopsis"] = synopsis
        upsert_mal_anime_metadata(
            self.config.db_path,
            mal_anime_id=mal_id,
            title=title,
            title_english=english,
            title_japanese=japanese,
            alternative_titles=list(synonyms or []),
            media_type=None,
            status=None,
            num_episodes=num_episodes,
            mean=None,
            popularity=None,
            start_season=start_season,
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

    def _review_payloads(self, *, provider: str | None = None) -> list[dict]:
        sql = "SELECT payload_json FROM review_queue WHERE issue_type = 'discovery_provider_search_match_review'"
        params: tuple[str, ...] = ()
        if provider is not None:
            sql += " AND provider = ?"
            params = (provider,)
        sql += " ORDER BY id"
        with sqlite3.connect(self.config.db_path) as conn:
            return [json.loads(row[0]) for row in conn.execute(sql, params).fetchall()]

    def _provider_match(self, *, audio_locales=None, **overrides):
        match = {
            "provider_series_id": "cr-merge",
            "title": "Merge Show",
            "season_title": "Merge Show",
            "url": "https://example.test/cr-merge",
            "audio_locales": list(audio_locales or []),
            "catalog_status": "present",
            "detail_evidence_source": "provider_title_search_result",
            "raw": {"type": "series", "availability_status": "available"},
        }
        match.update(overrides)
        return match

    def _review_entry(self, *, decision="strong_provider_search_candidate_no_auto_link", query="Merge Show", audio_locales=None, **match_overrides):
        match = self._provider_match(audio_locales=audio_locales, **match_overrides)
        status = enrichment._english_dub_status_from_match("crunchyroll", match)
        source = enrichment._explicit_dub_evidence_source("crunchyroll", match)
        return {
            "provider": "crunchyroll",
            "provider_series_id": match["provider_series_id"],
            "severity": "info",
            "payload": {
                "mal_anime_id": 900,
                "candidate_title": "Merge Show",
                "query": query,
                "match": match,
                "decision": decision,
                "english_dub_status": status,
                "audio_locales": list(audio_locales or []),
                "explicit_dub_evidence_source": source,
            },
        }


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

    def test_metadata_exact_english_hit_is_strong_even_when_synonym_query_differs(self):
        self._insert_meta(801, title="Romaji Main", english=None, alt_en="English Exact", synonyms=["Romaji Search"])
        self._recommendations(801)

        class MetadataProvider(FakeProvider):
            slug = "crunchyroll"

            def search_title(self, config, query: str, *, limit: int = 10):
                self.calls.append((query, limit))
                if query == "Romaji Search":
                    return [
                        {"provider_series_id": "cr-unrelated-a", "title": "Unrelated A", "audio_locales": []},
                        {"provider_series_id": "cr-english", "title": "English Exact", "audio_locales": ["en-US"]},
                        {"provider_series_id": "cr-unrelated-b", "title": "Unrelated B", "audio_locales": []},
                    ]
                return [{"provider_series_id": "cr-initial-residue", "title": "Initial Residue", "audio_locales": []}]

        provider = MetadataProvider([])
        summary = enrichment.enrich_discovery_provider_availability(
            self.config,
            providers=[provider],
            candidate_limit=1,
            now=datetime(2026, 1, 1, tzinfo=timezone.utc),
            persist_review_queue=False,
        )

        self.assertEqual([call[0] for call in provider.calls], ["English Exact", "Romaji Search"])
        self.assertEqual(1, summary.strong_matches)
        self.assertEqual(0, summary.ambiguous_matches)
        self.assertEqual(1, summary.exact_verified_identities_no_review)
        self.assertEqual(0, summary.dry_run_review_entries)
        self.assertEqual(0, summary.review_entries_written)
        self.assertEqual([], self._review_payloads(provider="crunchyroll"))
        evidence = get_recommendation_provider_eligibility_evidence(
            self.config.db_path,
            mal_anime_id=801,
            provider="crunchyroll",
            provider_series_id="cr-english",
        )
        self.assertIsNotNone(evidence)
        assert evidence is not None
        self.assertEqual("verified", evidence.review_status)
        self.assertEqual("provider_title_search_exact", evidence.identity_match_kind)

    def test_unrelated_acronym_residue_does_not_create_ambiguous_review(self):
        self._insert_meta(802, title="One Punch Man", english="One Punch Man", synonyms=["OPM"])
        self._insert_meta(803, title="Hunter x Hunter", english="Hunter x Hunter", synonyms=["HxH"])
        self._recommendations(802, 803)

        class AcronymResidueProvider(FakeProvider):
            def search_title(self, config, query: str, *, limit: int = 10):
                self.calls.append((query, limit))
                if query in {"One Punch Man", "OPM"}:
                    return [
                        {"provider_series_id": "residue-one-piece", "title": "One Piece", "audio_locales": []},
                        {"provider_series_id": "residue-one-room", "title": "One Room", "audio_locales": []},
                    ]
                return [
                    {"provider_series_id": "residue-monster-hunter", "title": "Monster Hunter Stories", "audio_locales": []},
                    {"provider_series_id": "residue-dxd", "title": "High School DxD HERO", "audio_locales": []},
                ]

        summary = enrichment.enrich_discovery_provider_availability(
            self.config,
            providers=[AcronymResidueProvider([])],
            candidate_limit=2,
            now=datetime(2026, 1, 1, tzinfo=timezone.utc),
        )

        self.assertEqual(0, summary.strong_matches)
        self.assertEqual(0, summary.ambiguous_matches)
        with sqlite3.connect(self.config.db_path) as conn:
            review = conn.execute("SELECT COUNT(*) FROM review_queue WHERE issue_type = 'discovery_provider_search_match_review'").fetchone()[0]
        self.assertEqual(0, review)

    def test_multiple_distinct_exact_target_provider_ids_are_ambiguous(self):
        meta = SimpleNamespace(
            title="Romaji Duplicate",
            title_english=None,
            title_japanese=None,
            alternative_titles=["Exact Alias"],
            raw={"alternative_titles": {"synonyms": ["Exact Alias"]}},
        )
        matches = [
            {"provider_series_id": "exact-a", "title": "Exact Alias", "audio_locales": []},
            {"provider_series_id": "unrelated", "title": "Unrelated Alias", "audio_locales": []},
            {"provider_series_id": "exact-b", "season_title": "Exact Alias", "audio_locales": []},
        ]

        kind, selected = enrichment.classify_provider_matches("Romaji Duplicate", matches, meta)

        self.assertEqual("ambiguous", kind)
        self.assertEqual(["exact-a", "exact-b"], [match["provider_series_id"] for match in selected])

    def test_rascal_series_shell_is_plausible_ambiguous_not_strong(self):
        meta = SimpleNamespace(
            title="Seishun Buta Yarou wa Bunny Girl Senpai no Yume wo Minai",
            title_english="Rascal Does Not Dream of Bunny Girl Senpai",
            title_japanese=None,
            alternative_titles=[],
            raw={"alternative_titles": {}},
        )

        kind, selected = enrichment.classify_provider_matches(
            "Rascal Does Not Dream of Bunny Girl Senpai",
            [{"provider_series_id": "rascal-shell", "title": "Rascal Does Not Dream Series", "audio_locales": []}],
            meta,
        )

        self.assertEqual("ambiguous", kind)
        self.assertEqual(["rascal-shell"], [match["provider_series_id"] for match in selected])

    def test_metadata_alias_examples_match_exact_target_family(self):
        cases = [
            (
                "Akame ga Kiru!",
                SimpleNamespace(
                    title="Akame ga Kiru!",
                    title_english=None,
                    title_japanese=None,
                    alternative_titles=["Akame ga Kill!"],
                    raw={"alternative_titles": {"synonyms": ["Akame ga Kill!"]}},
                ),
                "Akame ga Kill!",
            ),
            (
                "Kami-tachi ni Hirowareta Otoko",
                SimpleNamespace(
                    title="Kami-tachi ni Hirowareta Otoko",
                    title_english=None,
                    title_japanese=None,
                    alternative_titles=[],
                    raw={"alternative_titles": {"en": "By the Grace of the Gods"}},
                ),
                "By the Grace of the Gods",
            ),
        ]
        for query, meta, provider_title in cases:
            with self.subTest(provider_title=provider_title):
                match = {"provider_series_id": provider_title.casefold().replace(" ", "-"), "title": provider_title, "audio_locales": []}
                kind, selected = enrichment.classify_provider_matches(query, [match], meta)

                self.assertEqual("strong", kind)
                self.assertEqual([match["provider_series_id"]], [item["provider_series_id"] for item in selected])
                self.assertEqual("provider_title_search_exact", enrichment._search_identity_match_kind(query, selected[0], meta))

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

    def test_strong_exact_match_with_english_audio_coalesces_discovery_review_evidence(self):
        self._insert_meta(202, english="Delicious in Dungeon")
        self._recommendations(202)
        provider = FakeProvider([
            {"provider_series_id": "cr-dungeon", "title": "Delicious in Dungeon", "audio_locales": ["en-US"]}
        ])
        provider.slug = "crunchyroll"

        summary = enrichment.enrich_discovery_provider_availability(
            self.config,
            providers=[provider],
            candidate_limit=1,
            now=datetime(2026, 1, 1, tzinfo=timezone.utc),
        )

        self.assertEqual(summary.strong_matches, 1)
        self.assertEqual(1, summary.eligibility_evidence_upserted)
        self.assertEqual(1, summary.verified_eligibility_evidence_upserted)
        self.assertEqual(1, summary.exact_verified_identities_no_review)
        self.assertEqual(1, summary.as_dict()["exact_verified_identities_no_review"])
        self.assertEqual(0, summary.review_entries_written)
        self.assertEqual(0, summary.dry_run_review_entries)
        with sqlite3.connect(self.config.db_path) as conn:
            mapped = conn.execute("SELECT COUNT(*) FROM mal_series_mapping WHERE provider = 'crunchyroll'").fetchone()[0]
            review = conn.execute("SELECT COUNT(*) FROM review_queue WHERE issue_type = 'discovery_provider_search_match_review' AND provider = 'crunchyroll'").fetchone()[0]
        self.assertEqual(mapped, 0)
        self.assertEqual(review, 0)
        self.assertEqual([], self._review_payloads(provider="crunchyroll"))
        evidence = get_recommendation_provider_eligibility_evidence(
            self.config.db_path,
            mal_anime_id=202,
            provider="crunchyroll",
            provider_series_id="cr-dungeon",
        )
        self.assertIsNotNone(evidence)
        assert evidence is not None
        self.assertEqual("verified", evidence.review_status)
        self.assertEqual("provider_title_search_exact", evidence.identity_match_kind)
        self.assertEqual("present", evidence.catalog_status)
        self.assertEqual("present", evidence.english_dub_status)
        self.assertEqual("provider_audio_locale", evidence.explicit_dub_evidence_source)
        self.assertIn("en-US", evidence.audio_locales)
        self.assertEqual("2026-01-01T00:00:00Z", evidence.last_verified_at)
        self.assertIsNone(get_series_mapping(self.config.db_path, "crunchyroll", "cr-dungeon"))

    def test_current_query_only_direct_decision_remains_reviewed(self):
        match = {
            "provider_series_id": "cr-current-query",
            "title": "Current Query Only",
            "audio_locales": ["en-US"],
            "catalog_status": "present",
        }
        decision = enrichment.classify_provider_matches("Current Query Only", [match])
        self.assertEqual("strong", decision.kind)
        self.assertIn("title_exact_mal_alias:current_query", decision.reasons)

        summary = enrichment.EnrichmentSummary()
        review_entries: list[dict] = []
        enrichment._upsert_exact_identity_or_append_review(
            self.config,
            summary,
            review_entries,
            provider="crunchyroll",
            provider_series_id="cr-current-query",
            mal_id=902,
            candidate_title="Different MAL Title",
            query="Current Query Only",
            match=match,
            mapping=None,
            decision=decision,
            title_family=[
                enrichment.TargetTitleAlias(
                    text="Different MAL Title",
                    normalized=enrichment.normalize_title("Different MAL Title"),
                    source="title",
                    substantive=True,
                )
            ],
            fetched_at="2026-01-01T00:00:00Z",
            expires_at="2026-01-08T00:00:00Z",
        )

        self.assertEqual(1, summary.eligibility_evidence_upserted)
        self.assertEqual(1, summary.verified_eligibility_evidence_upserted)
        self.assertEqual(0, summary.exact_verified_identities_no_review)
        self.assertEqual(1, len(review_entries))
        self.assertEqual("strong_provider_search_candidate_no_auto_link", review_entries[0]["payload"]["decision"])

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
        provider.slug = "crunchyroll"

        summary = enrichment.enrich_discovery_provider_availability(
            self.config,
            providers=[provider],
            candidate_limit=1,
            now=datetime(2026, 1, 1, tzinfo=timezone.utc),
        )

        self.assertEqual(summary.strong_matches, 1)
        self.assertEqual(0, summary.review_entries_written)
        self.assertEqual(1, summary.exact_verified_identities_no_review)
        with sqlite3.connect(self.config.db_path) as conn:
            info_rows = conn.execute(
                "SELECT COUNT(*) FROM review_queue WHERE issue_type = 'discovery_provider_search_match_review' AND severity = 'info'"
            ).fetchone()[0]
            dub_rows = conn.execute(
                "SELECT COUNT(*) FROM review_queue WHERE issue_type = 'discovery_provider_search_match_review' AND payload_json LIKE '%strong_english_dub_evidence%'"
            ).fetchone()[0]
        self.assertEqual(info_rows, 0)
        self.assertEqual(dub_rows, 0, "title-only English Dub labels are not explicit audio-locale evidence")
        self.assertEqual([], self._review_payloads(provider="crunchyroll"))
        evidence = get_recommendation_provider_eligibility_evidence(
            self.config.db_path,
            mal_anime_id=212,
            provider="crunchyroll",
            provider_series_id="cr-title-only-dub",
        )
        self.assertIsNotNone(evidence)
        assert evidence is not None
        self.assertEqual("verified", evidence.review_status)
        self.assertEqual("provider_title_search_exact", evidence.identity_match_kind)
        self.assertEqual("unknown", evidence.english_dub_status)
        self.assertEqual([], evidence.audio_locales)
        self.assertIsNone(evidence.explicit_dub_evidence_source)
        self.assertIsNone(evidence.last_verified_at)
        self.assertIsNone(get_series_mapping(self.config.db_path, "crunchyroll", "cr-title-only-dub"))

    def test_discovery_review_generation_deduplicates_same_candidate_query_key(self):
        self._insert_meta(213, english="Duplicate Show")
        self._recommendations(213, 213)
        provider = FakeProvider([
            {"provider_series_id": "cr-duplicate", "title": "Duplicate Show", "audio_locales": ["ja-JP"]},
            {"provider_series_id": "cr-duplicate", "title": "Duplicate Show", "audio_locales": ["en-US", "ja-JP"]},
        ])
        provider.slug = "crunchyroll"

        summary = enrichment.enrich_discovery_provider_availability(
            self.config,
            providers=[provider],
            candidate_limit=2,
            now=datetime(2026, 1, 1, tzinfo=timezone.utc),
        )

        self.assertEqual(summary.strong_matches, 2)
        self.assertEqual(summary.provider_searches, 1)
        self.assertEqual(summary.cache_hits, 1)
        self.assertEqual(summary.review_entries_written, 0)
        self.assertEqual(summary.exact_verified_identities_no_review, 2)
        self.assertEqual([], self._review_payloads(provider="crunchyroll"))
        evidence = get_recommendation_provider_eligibility_evidence(
            self.config.db_path,
            mal_anime_id=213,
            provider="crunchyroll",
            provider_series_id="cr-duplicate",
        )
        self.assertIsNotNone(evidence)
        assert evidence is not None
        self.assertEqual("present", evidence.english_dub_status)
        self.assertEqual(["ja-JP", "en-US"], evidence.audio_locales)

    def test_dedupe_discovery_review_entries_preserves_decision_query_and_evidence_semantics(self):
        entries = [
            self._review_entry(audio_locales=["ja-JP"]),
            self._review_entry(audio_locales=["en-US", "ja-JP"]),
            self._review_entry(decision="manual_followup", audio_locales=["en-US"]),
            self._review_entry(decision="manual_followup", audio_locales=["en-US"]),
            self._review_entry(query="Merge Show Alternate", audio_locales=["en-US"]),
        ]

        deduped = enrichment._dedupe_discovery_review_entries(entries)

        self.assertEqual(3, len(deduped))
        merged_payload = deduped[0]["payload"]
        self.assertEqual("strong_provider_search_candidate_no_auto_link", merged_payload["decision"])
        self.assertEqual("present", merged_payload["english_dub_status"])
        self.assertEqual(["ja-JP", "en-US"], merged_payload["audio_locales"])
        self.assertEqual(["ja-JP", "en-US"], merged_payload["match"]["audio_locales"])
        self.assertEqual("provider_audio_locale", merged_payload["explicit_dub_evidence_source"])
        self.assertEqual(
            ["strong_provider_search_candidate_no_auto_link", "manual_followup", "strong_provider_search_candidate_no_auto_link"],
            [entry["payload"]["decision"] for entry in deduped],
        )
        self.assertEqual("Merge Show Alternate", deduped[2]["payload"]["query"])

    def test_provider_match_dedupe_keeps_materially_distinct_records(self):
        matches = [
            self._provider_match(audio_locales=["en-US"]),
            self._provider_match(audio_locales=["en-US"], season_title="Merge Show Season 2"),
            self._provider_match(audio_locales=["en-US"], catalog_status="absent"),
            self._provider_match(audio_locales=["en-US"], detail_evidence_source="crunchyroll_cms_series"),
        ]

        deduped = enrichment._dedupe_provider_matches(matches)

        self.assertEqual(4, len(deduped))

    def test_provider_match_dedupe_merges_audio_only_differences(self):
        matches = [
            self._provider_match(audio_locales=["ja-JP"]),
            self._provider_match(audio_locales=["en-US", "ja-JP"]),
        ]

        deduped = enrichment._dedupe_provider_matches(matches)

        self.assertEqual(1, len(deduped))
        self.assertEqual(["ja-JP", "en-US"], deduped[0]["audio_locales"])

    def test_non_english_audio_evidence_stays_non_actionable(self):
        self._insert_meta(214, english="Japanese Audio Only")
        self._recommendations(214)
        provider = FakeProvider([
            {"provider_series_id": "cr-ja-only", "title": "Japanese Audio Only", "audio_locales": ["ja-JP"]}
        ])
        provider.slug = "crunchyroll"

        summary = enrichment.enrich_discovery_provider_availability(
            self.config,
            providers=[provider],
            candidate_limit=1,
            now=datetime(2026, 1, 1, tzinfo=timezone.utc),
        )

        self.assertEqual(1, summary.eligibility_evidence_upserted)
        self.assertEqual(0, summary.verified_eligibility_evidence_upserted)
        self.assertEqual(1, summary.exact_verified_identities_no_review)
        self.assertEqual(0, summary.review_entries_written)
        self.assertEqual([], self._review_payloads(provider="crunchyroll"))
        evidence = get_recommendation_provider_eligibility_evidence(
            self.config.db_path,
            mal_anime_id=214,
            provider="crunchyroll",
            provider_series_id="cr-ja-only",
        )
        self.assertIsNotNone(evidence)
        assert evidence is not None
        self.assertEqual("verified", evidence.review_status)
        self.assertEqual("provider_title_search_exact", evidence.identity_match_kind)
        self.assertEqual("absent", evidence.english_dub_status)
        self.assertEqual(["ja-JP"], evidence.audio_locales)
        self.assertIsNone(evidence.explicit_dub_evidence_source)
        self.assertIsNone(evidence.last_verified_at)
        self.assertIsNone(get_series_mapping(self.config.db_path, "crunchyroll", "cr-ja-only"))

    def test_crunchyroll_detail_evidence_hydrates_review_needed_catalog_and_dub_status(self):
        self._insert_meta(222, english="Detail Needed")
        self._recommendations(222)

        class DetailProvider(FakeProvider):
            slug = "crunchyroll"

            def __init__(self):
                super().__init__([{
                    "provider_series_id": "cr-detail",
                    "title": "Detail Needed",
                    "audio_locales": [],
                    "catalog_status": "present",
                }])
                self.detail_calls = []

            def fetch_search_result_detail(self, config, match):
                self.detail_calls.append(match["provider_series_id"])
                return {
                    **match,
                    "audio_locales": ["ja-JP", "en-US"],
                    "catalog_status": "present",
                    "detail_evidence_source": "crunchyroll_cms_series",
                }

        provider = DetailProvider()
        summary = enrichment.enrich_discovery_provider_availability(
            self.config,
            providers=[provider],
            candidate_limit=1,
            now=datetime(2026, 7, 19, tzinfo=timezone.utc),
        )

        evidence = get_recommendation_provider_eligibility_evidence(
            self.config.db_path,
            mal_anime_id=222,
            provider="crunchyroll",
            provider_series_id="cr-detail",
        )
        self.assertEqual(["cr-detail"], provider.detail_calls)
        self.assertEqual(1, summary.provider_detail_probes)
        self.assertEqual(1, summary.eligibility_evidence_upserted)
        self.assertIsNotNone(evidence)
        assert evidence is not None
        self.assertEqual("verified", evidence.review_status)
        self.assertEqual("provider_title_search_exact", evidence.identity_match_kind)
        self.assertEqual(0.9, evidence.match_confidence)
        self.assertEqual("present", evidence.catalog_status)
        self.assertEqual("present", evidence.english_dub_status)
        self.assertEqual("provider_audio_locale", evidence.explicit_dub_evidence_source)
        self.assertEqual("crunchyroll_cms_series", evidence.source_evidence["catalog_evidence_source"])

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
        provider.slug = "crunchyroll"

        summary = enrichment.enrich_discovery_provider_availability(
            self.config,
            providers=[provider],
            candidate_limit=1,
            now=datetime(2026, 1, 1, tzinfo=timezone.utc),
        )

        self.assertEqual(summary.ambiguous_matches, 1)
        with sqlite3.connect(self.config.db_path) as conn:
            mapped = conn.execute("SELECT COUNT(*) FROM mal_series_mapping WHERE provider = 'crunchyroll'").fetchone()[0]
            review = conn.execute("SELECT COUNT(*) FROM review_queue WHERE issue_type = 'discovery_provider_search_match_review'").fetchone()[0]
        self.assertEqual(mapped, 0)
        self.assertEqual(review, 1)
        payload = self._review_payloads(provider="crunchyroll")[0]
        self.assertEqual("ambiguous_no_auto_link", payload["decision"])
        self.assertEqual({"cr-kaina-a", "cr-kaina-b"}, {match["provider_series_id"] for match in payload["matches"]})
        self.assertIn("multiple_exact_mal_alias_provider_ids", payload["provider_search_match_reasons"])
        self.assertIsNone(get_recommendation_provider_eligibility_evidence(self.config.db_path, mal_anime_id=404, provider="crunchyroll", provider_series_id="cr-kaina-a"))
        self.assertIsNone(get_recommendation_provider_eligibility_evidence(self.config.db_path, mal_anime_id=404, provider="crunchyroll", provider_series_id="cr-kaina-b"))

    def test_provider_title_matching_mal_english_alias_is_strong_even_when_query_is_romanized_synonym(self):
        self._insert_meta(808, title="Akame ga Kiru!", english="Akame ga Kill!", synonyms=["Akame ga Kiru!"])
        self._recommendations(808)

        class AliasRescueProvider(FakeProvider):
            slug = "crunchyroll"

            def search_title(self, config, query: str, *, limit: int = 10):
                self.calls.append((query, limit))
                if query == "Akame ga Kiru!":
                    return [
                        {"provider_series_id": "cr-akame", "title": "Akame ga Kill!", "audio_locales": ["ja-JP"]},
                        {"provider_series_id": "cr-kira", "title": "KIRA KIRA☆PRECURE A LA MODE", "audio_locales": ["ja-JP"]},
                        {"provider_series_id": "cr-double", "title": "DOUBLE DECKER! DOUG & KIRILL", "audio_locales": ["en-US", "ja-JP"]},
                    ]
                return []

        provider = AliasRescueProvider([])
        summary = enrichment.enrich_discovery_provider_availability(
            self.config,
            providers=[provider],
            candidate_limit=1,
            now=datetime(2026, 1, 1, tzinfo=timezone.utc),
        )

        self.assertEqual([call[0] for call in provider.calls], ["Akame ga Kill!", "Akame ga Kiru!"])
        self.assertEqual(summary.strong_matches, 1)
        self.assertEqual(summary.ambiguous_matches, 0)
        self.assertEqual(1, summary.exact_verified_identities_no_review)
        self.assertEqual([], self._review_payloads(provider="crunchyroll"))
        evidence = get_recommendation_provider_eligibility_evidence(
            self.config.db_path,
            mal_anime_id=808,
            provider="crunchyroll",
            provider_series_id="cr-akame",
        )
        self.assertIsNotNone(evidence)
        assert evidence is not None
        self.assertEqual("verified", evidence.review_status)
        self.assertEqual("provider_title_search_exact", evidence.identity_match_kind)
        self.assertEqual("absent", evidence.english_dub_status)

    def test_alias_metadata_rescues_by_the_grace_style_unique_exact_target_result(self):
        self._insert_meta(
            809,
            title="Kamitachi ni Hirowareta Otoko",
            english="By the Grace of the Gods",
            synonyms=["The man picked up by the gods", "Kamihiro"],
        )
        self._recommendations(809)

        class GraceProvider(FakeProvider):
            slug = "crunchyroll"

            def search_title(self, config, query: str, *, limit: int = 10):
                self.calls.append((query, limit))
                if query == "The man picked up by the gods":
                    return [
                        {"provider_series_id": "cr-grace", "title": "By the Grace of the Gods", "audio_locales": ["en-US", "ja-JP"]},
                        {"provider_series_id": "cr-god-games", "title": "Gods' Games We Play", "audio_locales": ["ja-JP"]},
                    ]
                return []

        provider = GraceProvider([])
        summary = enrichment.enrich_discovery_provider_availability(
            self.config,
            providers=[provider],
            candidate_limit=1,
            now=datetime(2026, 1, 1, tzinfo=timezone.utc),
        )

        self.assertEqual([call[0] for call in provider.calls], ["By the Grace of the Gods", "The man picked up by the gods", "Kamihiro"])
        self.assertEqual(summary.strong_matches, 1)
        self.assertEqual(1, summary.exact_verified_identities_no_review)
        self.assertEqual([], self._review_payloads(provider="crunchyroll"))
        evidence = get_recommendation_provider_eligibility_evidence(
            self.config.db_path,
            mal_anime_id=809,
            provider="crunchyroll",
            provider_series_id="cr-grace",
        )
        self.assertIsNotNone(evidence)
        assert evidence is not None
        self.assertEqual("provider_title_search_exact", evidence.identity_match_kind)
        self.assertEqual("present", evidence.english_dub_status)
        self.assertEqual("2026-01-01T00:00:00Z", evidence.last_verified_at)

    def test_ambiguous_provider_search_suppresses_unrelated_acronym_hits(self):
        self._insert_meta(810, title="One Punch Man", english=None, synonyms=["OPM"])
        self._recommendations(810)
        provider = FakeProvider([
            {"provider_series_id": "cr-op-destiny", "title": "takt op.Destiny", "audio_locales": ["en-US", "ja-JP"]},
            {"provider_series_id": "cr-opus", "title": "Opus.COLORs", "audio_locales": ["ja-JP"]},
            {"provider_series_id": "cr-asuka", "title": "Magical Girl Spec-Ops Asuka", "audio_locales": ["en-US", "ja-JP"]},
        ])
        provider.slug = "crunchyroll"

        summary = enrichment.enrich_discovery_provider_availability(
            self.config,
            providers=[provider],
            candidate_limit=1,
            now=datetime(2026, 1, 1, tzinfo=timezone.utc),
        )

        self.assertEqual(summary.strong_matches, 0)
        self.assertEqual(summary.ambiguous_matches, 0)
        self.assertEqual(self._review_payloads(provider="crunchyroll"), [])
        for provider_series_id in ("cr-op-destiny", "cr-opus", "cr-asuka"):
            self.assertIsNone(
                get_recommendation_provider_eligibility_evidence(
                    self.config.db_path,
                    mal_anime_id=810,
                    provider="crunchyroll",
                    provider_series_id=provider_series_id,
                )
            )

    def test_strong_full_title_query_coalesces_noisy_alias_query_for_same_provider_target(self):
        self._insert_meta(811, title="Shigatsu wa Kimi no Uso", english="Your Lie in April", synonyms=["Kimiuso"])
        self._recommendations(811)

        class NoisyAliasProvider(FakeProvider):
            slug = "crunchyroll"

            def search_title(self, config, query: str, *, limit: int = 10):
                self.calls.append((query, limit))
                if query == "Your Lie in April":
                    return [{"provider_series_id": "cr-kimiuso", "title": "Your Lie in April", "audio_locales": ["en-US", "ja-JP"]}]
                if query == "Kimiuso":
                    return [
                        {"provider_series_id": "cr-kimiuso", "title": "Your Lie in April", "audio_locales": ["ja-JP"]},
                        {"provider_series_id": "cr-love-live", "title": "Love Live! Sunshine!!", "audio_locales": ["en-US", "ja-JP"]},
                    ]
                return []

        provider = NoisyAliasProvider([])
        summary = enrichment.enrich_discovery_provider_availability(
            self.config,
            providers=[provider],
            candidate_limit=1,
            now=datetime(2026, 1, 1, tzinfo=timezone.utc),
        )

        self.assertEqual(summary.strong_matches, 2)
        self.assertEqual(summary.exact_verified_identities_no_review, 2)
        self.assertEqual([], self._review_payloads(provider="crunchyroll"))
        evidence = get_recommendation_provider_eligibility_evidence(
            self.config.db_path,
            mal_anime_id=811,
            provider="crunchyroll",
            provider_series_id="cr-kimiuso",
        )
        self.assertIsNotNone(evidence)
        assert evidence is not None
        self.assertEqual("present", evidence.english_dub_status)
        self.assertEqual("provider_audio_locale", evidence.explicit_dub_evidence_source)

    def test_provider_series_shell_remains_review_not_auto_link(self):
        self._insert_meta(
            812,
            title="Seishun Buta Yarou wa Bunny Girl Senpai no Yume wo Minai",
            english="Rascal Does Not Dream of Bunny Girl Senpai",
            synonyms=["AoButa"],
        )
        self._recommendations(812)

        class RascalProvider(FakeProvider):
            slug = "crunchyroll"

            def search_title(self, config, query: str, *, limit: int = 10):
                self.calls.append((query, limit))
                if query == "Rascal Does Not Dream of Bunny Girl Senpai":
                    return [
                        {"provider_series_id": "cr-rascal", "title": "Rascal Does Not Dream Series", "audio_locales": ["en-US", "ja-JP"]},
                        {"provider_series_id": "cr-otokonoko", "title": "Senpai is an Otokonoko", "audio_locales": ["ja-JP"]},
                    ]
                return [{"provider_series_id": "cr-nobuna", "title": "The Ambition of Oda Nobuna", "audio_locales": ["ja-JP"]}]

        provider = RascalProvider([])

        summary = enrichment.enrich_discovery_provider_availability(
            self.config,
            providers=[provider],
            candidate_limit=1,
            now=datetime(2026, 1, 1, tzinfo=timezone.utc),
        )

        self.assertEqual(summary.strong_matches, 0)
        self.assertEqual(summary.ambiguous_matches, 1)
        payload = self._review_payloads(provider="crunchyroll")[0]
        self.assertEqual("ambiguous_no_auto_link", payload["decision"])
        self.assertEqual(["cr-rascal"], [match["provider_series_id"] for match in payload["matches"]])
        self.assertIn("franchise_shell_overlap", payload["provider_search_match_reasons"])
        self.assertIsNone(get_recommendation_provider_eligibility_evidence(self.config.db_path, mal_anime_id=812, provider="crunchyroll", provider_series_id="cr-rascal"))

    def test_franchise_shell_child_identity_persists_eligibility_without_mapping_or_review(self):
        self._insert_meta(
            813,
            title="Seishun Buta Yarou wa Bunny Girl Senpai no Yume wo Minai",
            english="Rascal Does Not Dream of Bunny Girl Senpai",
            num_episodes=13,
            start_year=2018,
        )
        self._recommendations(813)

        class RascalShellProvider(FakeProvider):
            slug = "crunchyroll"

            def __init__(self):
                super().__init__([{
                    "provider_series_id": "GYW4MG9G6",
                    "title": "Rascal Does Not Dream Series",
                    "season_title": "Rascal Does Not Dream Series",
                    "audio_locales": ["en-US", "ja-JP"],
                    "catalog_status": "present",
                    "raw": {"series_metadata": {"season_count": 5, "episode_count": 29, "series_launch_year": 2018}},
                }])
                self.child_calls = []

            def fetch_search_result_children(self, config, match):
                self.child_calls.append(match["provider_series_id"])
                return [
                    {"id": "season-1", "title": "Rascal Does Not Dream of Bunny Girl Senpai", "episode_count": 13, "audio_locales": ["en-US", "ja-JP"]},
                    {"id": "season-2", "title": "Rascal Does Not Dream of a Dreaming Girl", "episode_count": 1, "audio_locales": ["ja-JP"]},
                ]

        provider = RascalShellProvider()
        summary = enrichment.enrich_discovery_provider_availability(
            self.config,
            providers=[provider],
            candidate_limit=1,
            now=datetime(2026, 7, 23, tzinfo=timezone.utc),
        )

        self.assertEqual(["GYW4MG9G6"], provider.child_calls)
        self.assertEqual(1, summary.strong_matches)
        self.assertEqual(0, summary.ambiguous_matches)
        self.assertEqual(1, summary.franchise_shell_verified_matches)
        self.assertEqual(1, summary.franchise_shell_verified_identities_no_review)
        self.assertEqual(1, summary.aggregate_shells_verified_no_review)
        self.assertEqual(1, summary.eligibility_evidence_upserted)
        self.assertEqual(1, summary.verified_eligibility_evidence_upserted)
        self.assertEqual(0, summary.review_entries_written)
        self.assertEqual([], self._review_payloads(provider="crunchyroll"))
        self.assertIsNone(get_series_mapping(self.config.db_path, "crunchyroll", "GYW4MG9G6"))
        evidence = get_recommendation_provider_eligibility_evidence(
            self.config.db_path,
            mal_anime_id=813,
            provider="crunchyroll",
            provider_series_id="GYW4MG9G6",
        )
        self.assertIsNotNone(evidence)
        assert evidence is not None
        self.assertEqual("verified", evidence.review_status)
        self.assertEqual("provider_franchise_shell_child_match", evidence.identity_match_kind)
        self.assertEqual("present", evidence.catalog_status)
        self.assertEqual("present", evidence.english_dub_status)
        self.assertEqual("provider_audio_locale", evidence.explicit_dub_evidence_source)
        self.assertEqual("2026-07-23T00:00:00Z", evidence.last_verified_at)
        self.assertEqual("Rascal Does Not Dream of Bunny Girl Senpai", evidence.source_evidence["identity_evidence"]["child_title"])
        child_identity = evidence.source_evidence["identity_evidence"]["child_identity"]
        self.assertEqual("season-1", child_identity["id"])
        self.assertEqual("Rascal Does Not Dream of Bunny Girl Senpai", child_identity["title"])
        self.assertEqual("rascal does not dream of bunny girl senpai", child_identity["normalized_title"])
        self.assertEqual(13, child_identity["episode_count"])
        self.assertEqual(["en-US", "ja-JP"], child_identity["audio_locales"])
        child_titles = evidence.source_evidence["identity_evidence"]["child_titles"]
        self.assertEqual(
            ["Rascal Does Not Dream of Bunny Girl Senpai", "Rascal Does Not Dream of a Dreaming Girl"],
            [child["title"] for child in child_titles if child["field"] == "title"],
        )

    def test_franchise_shell_metadata_synopsis_without_child_stays_queued(self):
        target_synopsis = (
            "The rare Puberty Syndrome affects teenagers. Sakuta Azusagawa meets Mai Sakurajima, "
            "a former child actress, in the school library while she is wearing a bunny girl costume. "
            "As Sakuta investigates the syndrome, more students around the school become involved."
        )
        provider_description = (
            "Sakuta Azusagawa never expected to find a bunny girl in the library. The former actress "
            "he meets disappeared from view, and Sakuta learns this may be a symptom of puberty syndrome."
        )
        self._insert_meta(
            817,
            title="Seishun Buta Yarou wa Bunny Girl Senpai no Yume wo Minai",
            english="Rascal Does Not Dream of Bunny Girl Senpai",
            num_episodes=13,
            start_year=2018,
            synopsis=target_synopsis,
        )
        self._recommendations(817)
        provider = FakeProvider([{
            "provider_series_id": "GYW4MG9G6",
            "title": "Rascal Does Not Dream Series",
            "season_title": "Rascal Does Not Dream Series",
            "audio_locales": ["en-US", "ja-JP"],
            "catalog_status": "present",
            "raw": {
                "description": provider_description,
                "series_metadata": {"season_count": 5, "episode_count": 29, "series_launch_year": 2018, "audio_locales": ["en-US", "ja-JP"]},
            },
        }])
        provider.slug = "crunchyroll"

        summary = enrichment.enrich_discovery_provider_availability(
            self.config,
            providers=[provider],
            candidate_limit=1,
            now=datetime(2026, 7, 23, tzinfo=timezone.utc),
        )

        self.assertEqual(0, summary.franchise_shell_verified_matches)
        self.assertEqual(1, summary.ambiguous_matches)
        payload = self._review_payloads(provider="crunchyroll")[0]
        self.assertEqual("ambiguous_no_auto_link", payload["decision"])
        self.assertIn("franchise_shell_overlap", payload["provider_search_match_reasons"])
        self.assertIsNone(get_recommendation_provider_eligibility_evidence(
            self.config.db_path,
            mal_anime_id=817,
            provider="crunchyroll",
            provider_series_id="GYW4MG9G6",
        ))

    def test_franchise_shell_without_matching_child_stays_queued_ambiguous(self):
        self._insert_meta(
            814,
            title="Seishun Buta Yarou wa Bunny Girl Senpai no Yume wo Minai",
            english="Rascal Does Not Dream of Bunny Girl Senpai",
            num_episodes=13,
            start_year=2018,
        )
        self._recommendations(814)

        class ShellProvider(FakeProvider):
            slug = "crunchyroll"

            def __init__(self):
                super().__init__([{
                    "provider_series_id": "GYW4MG9G6",
                    "title": "Rascal Does Not Dream Series",
                    "season_title": "Rascal Does Not Dream Series",
                    "audio_locales": ["en-US", "ja-JP"],
                    "catalog_status": "present",
                    "raw": {"series_metadata": {"season_count": 5, "episode_count": 29, "series_launch_year": 2018}},
                }])

            def fetch_search_result_children(self, config, match):
                return [{"id": "season-2", "title": "Rascal Does Not Dream of a Dreaming Girl", "episode_count": 1, "audio_locales": ["en-US", "ja-JP"]}]

        summary = enrichment.enrich_discovery_provider_availability(
            self.config,
            providers=[ShellProvider()],
            candidate_limit=1,
            now=datetime(2026, 7, 23, tzinfo=timezone.utc),
        )

        self.assertEqual(0, summary.franchise_shell_verified_matches)
        self.assertEqual(1, summary.ambiguous_matches)
        payload = self._review_payloads(provider="crunchyroll")[0]
        self.assertEqual("ambiguous_no_auto_link", payload["decision"])
        self.assertIn("franchise_shell_overlap", payload["provider_search_match_reasons"])
        self.assertIsNone(get_recommendation_provider_eligibility_evidence(self.config.db_path, mal_anime_id=814, provider="crunchyroll", provider_series_id="GYW4MG9G6"))

    def test_multiple_franchise_shell_child_identities_stay_ambiguous(self):
        self._insert_meta(
            818,
            title="Seishun Buta Yarou wa Bunny Girl Senpai no Yume wo Minai",
            english="Rascal Does Not Dream of Bunny Girl Senpai",
            num_episodes=13,
            start_year=2018,
        )
        self._recommendations(818)

        class MultipleShellProvider(FakeProvider):
            slug = "crunchyroll"

            def __init__(self):
                super().__init__([
                    {"provider_series_id": "GYW4MG9G6", "title": "Rascal Does Not Dream Series", "audio_locales": ["ja-JP"]},
                    {"provider_series_id": "GOTHER", "title": "Rascal Does Not Dream Collection", "audio_locales": ["ja-JP"]},
                ])
                self.child_calls = []

            def fetch_search_result_children(self, config, match):
                self.child_calls.append(match["provider_series_id"])
                return [{"id": f"{match['provider_series_id']}-season-1", "title": "Rascal Does Not Dream of Bunny Girl Senpai", "episode_count": 13}]

        provider = MultipleShellProvider()
        summary = enrichment.enrich_discovery_provider_availability(
            self.config,
            providers=[provider],
            candidate_limit=1,
            now=datetime(2026, 7, 23, tzinfo=timezone.utc),
        )

        self.assertEqual(["GYW4MG9G6", "GOTHER"], provider.child_calls)
        self.assertEqual(0, summary.franchise_shell_verified_matches)
        self.assertEqual(1, summary.ambiguous_matches)
        payload = self._review_payloads(provider="crunchyroll")[0]
        self.assertEqual({"GYW4MG9G6", "GOTHER"}, {match["provider_series_id"] for match in payload["matches"]})
        self.assertIsNone(get_recommendation_provider_eligibility_evidence(self.config.db_path, mal_anime_id=818, provider="crunchyroll", provider_series_id="GYW4MG9G6"))
        self.assertIsNone(get_recommendation_provider_eligibility_evidence(self.config.db_path, mal_anime_id=818, provider="crunchyroll", provider_series_id="GOTHER"))

    def test_franchise_shell_child_detail_failure_stays_queued(self):
        self._insert_meta(
            819,
            title="Seishun Buta Yarou wa Bunny Girl Senpai no Yume wo Minai",
            english="Rascal Does Not Dream of Bunny Girl Senpai",
            num_episodes=13,
            start_year=2018,
        )
        self._recommendations(819)

        class FailingShellProvider(FakeProvider):
            slug = "crunchyroll"

            def __init__(self):
                super().__init__([{"provider_series_id": "GYW4MG9G6", "title": "Rascal Does Not Dream Series", "audio_locales": ["ja-JP"]}])
                self.child_calls = []

            def fetch_search_result_children(self, config, match):
                self.child_calls.append(match["provider_series_id"])
                raise RuntimeError("detail unavailable")

        provider = FailingShellProvider()
        summary = enrichment.enrich_discovery_provider_availability(
            self.config,
            providers=[provider],
            candidate_limit=1,
            now=datetime(2026, 7, 23, tzinfo=timezone.utc),
        )

        self.assertEqual(["GYW4MG9G6"], provider.child_calls)
        self.assertEqual(1, summary.provider_detail_failures)
        self.assertEqual(0, summary.franchise_shell_verified_matches)
        self.assertEqual(1, summary.ambiguous_matches)
        self.assertEqual("ambiguous_no_auto_link", self._review_payloads(provider="crunchyroll")[0]["decision"])
        self.assertIsNone(get_recommendation_provider_eligibility_evidence(self.config.db_path, mal_anime_id=819, provider="crunchyroll", provider_series_id="GYW4MG9G6"))

    def test_franchise_shell_child_detail_probe_is_reused_across_duplicate_queries(self):
        self._insert_meta(
            820,
            title="Seishun Buta Yarou wa Bunny Girl Senpai no Yume wo Minai",
            english="Rascal Does Not Dream of Bunny Girl Senpai",
            synonyms=["Rascal Does Not Dream"],
            num_episodes=13,
            start_year=2018,
        )
        self._recommendations(820)

        class DuplicateQueryShellProvider(FakeProvider):
            slug = "crunchyroll"

            def __init__(self):
                super().__init__([])
                self.child_calls = []

            def search_title(self, config, query: str, *, limit: int = 10):
                self.calls.append((query, limit))
                return [{"provider_series_id": "GYW4MG9G6", "title": "Rascal Does Not Dream Series", "audio_locales": ["ja-JP"]}]

            def fetch_search_result_children(self, config, match):
                self.child_calls.append(match["provider_series_id"])
                return [{"id": "season-1", "title": "Rascal Does Not Dream of Bunny Girl Senpai", "episode_count": 13, "audio_locales": ["ja-JP"]}]

        provider = DuplicateQueryShellProvider()
        summary = enrichment.enrich_discovery_provider_availability(
            self.config,
            providers=[provider],
            candidate_limit=1,
            now=datetime(2026, 7, 23, tzinfo=timezone.utc),
        )

        self.assertEqual(["Rascal Does Not Dream of Bunny Girl Senpai", "Rascal Does Not Dream"], [call[0] for call in provider.calls])
        self.assertEqual(["GYW4MG9G6"], provider.child_calls)
        self.assertEqual(1, summary.provider_detail_probes)
        self.assertEqual(2, summary.franchise_shell_verified_matches)
        self.assertEqual(0, summary.ambiguous_matches)

    def test_overlapping_franchise_shell_names_do_not_auto_resolve_wrong_installment(self):
        self._insert_meta(
            815,
            title="Seishun Buta Yarou wa Randoseru Girl no Yume wo Minai",
            english="Rascal Does Not Dream of a Knapsack Kid",
            num_episodes=1,
            start_year=2023,
        )
        self._recommendations(815)
        provider = FakeProvider([{
            "provider_series_id": "GYW4MG9G6",
            "title": "Rascal Does Not Dream Series",
            "season_title": "Rascal Does Not Dream Series",
            "audio_locales": ["en-US", "ja-JP"],
            "catalog_status": "present",
            "raw": {"series_metadata": {"season_count": 5, "episode_count": 29, "series_launch_year": 2018}},
        }])
        provider.slug = "crunchyroll"

        summary = enrichment.enrich_discovery_provider_availability(
            self.config,
            providers=[provider],
            candidate_limit=1,
            now=datetime(2026, 7, 23, tzinfo=timezone.utc),
        )

        self.assertEqual(0, summary.franchise_shell_verified_matches)
        self.assertEqual(1, summary.ambiguous_matches)
        self.assertEqual(1, len(self._review_payloads(provider="crunchyroll")))
        self.assertIsNone(get_recommendation_provider_eligibility_evidence(self.config.db_path, mal_anime_id=815, provider="crunchyroll", provider_series_id="GYW4MG9G6"))

    def test_franchise_shell_child_identity_without_english_dub_stays_non_actionable(self):
        self._insert_meta(
            816,
            title="Seishun Buta Yarou wa Bunny Girl Senpai no Yume wo Minai",
            english="Rascal Does Not Dream of Bunny Girl Senpai",
            num_episodes=13,
            start_year=2018,
        )
        self._recommendations(816)

        class SubOnlyShellProvider(FakeProvider):
            slug = "crunchyroll"

            def __init__(self):
                super().__init__([{
                    "provider_series_id": "GYW4MG9G6",
                    "title": "Rascal Does Not Dream Series",
                    "season_title": "Rascal Does Not Dream Series",
                    "audio_locales": ["ja-JP"],
                    "catalog_status": "present",
                    "raw": {"series_metadata": {"season_count": 5, "episode_count": 29, "series_launch_year": 2018, "audio_locales": ["ja-JP"]}},
                }])

            def fetch_search_result_children(self, config, match):
                return [{"id": "season-1", "title": "Rascal Does Not Dream of Bunny Girl Senpai", "episode_count": 13, "audio_locales": ["ja-JP"]}]

        summary = enrichment.enrich_discovery_provider_availability(
            self.config,
            providers=[SubOnlyShellProvider()],
            candidate_limit=1,
            now=datetime(2026, 7, 23, tzinfo=timezone.utc),
        )

        self.assertEqual(1, summary.franchise_shell_verified_matches)
        self.assertEqual(1, summary.eligibility_evidence_upserted)
        self.assertEqual(0, summary.verified_eligibility_evidence_upserted)
        evidence = get_recommendation_provider_eligibility_evidence(
            self.config.db_path,
            mal_anime_id=816,
            provider="crunchyroll",
            provider_series_id="GYW4MG9G6",
        )
        self.assertIsNotNone(evidence)
        assert evidence is not None
        self.assertEqual("verified", evidence.review_status)
        self.assertEqual("provider_franchise_shell_child_match", evidence.identity_match_kind)
        self.assertEqual("absent", evidence.english_dub_status)
        self.assertEqual(["ja-JP"], evidence.audio_locales)
        self.assertIsNone(evidence.last_verified_at)

    def test_franchise_shell_child_identity_accepts_generic_number_of_episodes(self):
        self._insert_meta(
            821,
            title="Seishun Buta Yarou wa Bunny Girl Senpai no Yume wo Minai",
            english="Rascal Does Not Dream of Bunny Girl Senpai",
            num_episodes=13,
            start_year=2018,
        )
        self._recommendations(821)

        class NumberedChildShellProvider(FakeProvider):
            slug = "crunchyroll"

            def __init__(self):
                super().__init__([{
                    "provider_series_id": "GYW4MG9G6",
                    "title": "Rascal Does Not Dream Series",
                    "season_title": "Rascal Does Not Dream Series",
                    "audio_locales": ["en-US", "ja-JP"],
                    "catalog_status": "present",
                    "raw": {"series_metadata": {"season_count": 5, "episode_count": 29, "series_launch_year": 2018}},
                }])

            def fetch_search_result_children(self, config, match):
                return [{"id": "season-1", "title": "Rascal Does Not Dream of Bunny Girl Senpai", "number_of_episodes": "13", "audio_locales": ["en-US", "ja-JP"]}]

        summary = enrichment.enrich_discovery_provider_availability(
            self.config,
            providers=[NumberedChildShellProvider()],
            candidate_limit=1,
            now=datetime(2026, 7, 23, tzinfo=timezone.utc),
        )

        self.assertEqual(1, summary.franchise_shell_verified_matches)
        evidence = get_recommendation_provider_eligibility_evidence(
            self.config.db_path,
            mal_anime_id=821,
            provider="crunchyroll",
            provider_series_id="GYW4MG9G6",
        )
        self.assertIsNotNone(evidence)
        assert evidence is not None
        self.assertEqual("provider_franchise_shell_child_match", evidence.identity_match_kind)
        self.assertEqual(13, evidence.source_evidence["identity_evidence"]["child_episode_count"])
        self.assertIn("provider_child_episode_count_matches_target=13", evidence.source_evidence["identity_evidence"]["verification_reasons"])

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
        self.assertEqual(review, 0)
        self.assertEqual(1, summary.exact_verified_identities_no_review)
        evidence = get_recommendation_provider_eligibility_evidence(
            self.config.db_path,
            mal_anime_id=505,
            provider="hidive",
            provider_series_id="2312",
        )
        self.assertIsNotNone(evidence)
        assert evidence is not None
        self.assertEqual("verified", evidence.review_status)
        self.assertEqual("unknown", evidence.english_dub_status)
        self.assertIsNone(evidence.last_verified_at)
        self.assertIsNone(get_series_mapping(self.config.db_path, "hidive", "2312"))

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
        self.assertEqual(1, summary.verified_eligibility_evidence_upserted)
        self.assertIsNotNone(evidence)
        assert evidence is not None
        self.assertEqual("verified", evidence.review_status)
        self.assertEqual("provider_title_search_exact", evidence.identity_match_kind)
        self.assertEqual(0.9, evidence.match_confidence)
        self.assertEqual("present", evidence.catalog_status)
        self.assertEqual("present", evidence.english_dub_status)
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

    def test_provider_search_result_raw_catalog_evidence_survives_normalization(self):
        match = enrichment._match_to_dict(
            ProviderSearchResult(
                provider_series_id="2312",
                title="Dungeon People",
                season_title="Dungeon People",
                url="https://www.hidive.com/season/2312",
                audio_locales=["ja-JP", "en-US"],
                raw={"type": "VOD_SERIES", "catalog_status": "present", "catalog_evidence_source": "hidive_algolia_vod_series", "tags": ["Audio|Japanese", "Audio|English"]},
            )
        )

        self.assertEqual("present", match["catalog_status"])
        self.assertEqual("hidive_algolia_vod_series", match["detail_evidence_source"])
        self.assertEqual(["Audio|Japanese", "Audio|English"], match["raw"]["tags"])

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
        self.assertEqual(review, 0)
        self.assertEqual(summary.exact_verified_identities_no_review, 4)

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
