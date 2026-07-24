from __future__ import annotations

import unittest
from types import SimpleNamespace
from unittest.mock import patch

from mal_updater.providers import crunchyroll


class CrunchyrollProviderSearchTests(unittest.TestCase):
    def test_discover_item_without_audio_locales_keeps_dub_evidence_unknown(self) -> None:
        result = crunchyroll._series_from_crunchyroll_item(
            {
                "id": "GY12345",
                "title": "Provider Search Dub Unknown",
                "season_title": "Provider Search Dub Unknown (English Dub)",
                "slug_title": "provider-search-dub-unknown",
            }
        )

        self.assertIsNotNone(result)
        assert result is not None
        self.assertEqual(result["provider_series_id"], "GY12345")
        self.assertEqual(result["audio_locales"], [])
        self.assertIn("English Dub", result["season_title"])

    def test_discover_item_preserves_explicit_audio_locales_when_present(self) -> None:
        result = crunchyroll._series_from_crunchyroll_item(
            {"id": "GY67890", "title": "Explicit Audio", "audio_locales": ["en-US", "ja-JP"]}
        )

        self.assertIsNotNone(result)
        assert result is not None
        self.assertEqual(result["audio_locales"], ["en-US", "ja-JP"])

    def test_discover_item_normalizes_nested_series_metadata_audio_locales(self) -> None:
        item = {
            "id": "G6NQ5DWZ6",
            "title": "My Hero Academia",
            "series_metadata": {
                "availability_status": "available",
                "is_dubbed": True,
                "audio_locales": ["de-DE", "en_US", "EN-gb", "ja-JP", "en-US"],
            },
        }

        result = crunchyroll._series_from_crunchyroll_item(item)

        self.assertIsNotNone(result)
        assert result is not None
        self.assertEqual(result["audio_locales"], ["de-DE", "en-US", "en-GB", "ja-JP"])
        self.assertEqual(result["raw"]["series_metadata"]["is_dubbed"], True)
        self.assertEqual(result["raw"]["series_metadata"]["availability_status"], "available")

    def test_season_child_normalization_uses_season_metadata_audio_and_number_of_episodes(self) -> None:
        seasons = crunchyroll._seasons_from_crunchyroll_payload(
            {
                "data": [
                    {
                        "id": "GRZXCM4XJ",
                        "series_id": "GYW4MG9G6",
                        "title": "Rascal Does Not Dream of Bunny Girl Senpai",
                        "season_metadata": {"audio_locales": ["en_US", "ja-JP"], "number_of_episodes": 13, "season_number": 1},
                    }
                ]
            }
        )

        self.assertEqual(1, len(seasons))
        self.assertEqual("GRZXCM4XJ", seasons[0]["id"])
        self.assertEqual("GYW4MG9G6", seasons[0]["series_id"])
        self.assertEqual("Rascal Does Not Dream of Bunny Girl Senpai", seasons[0]["title"])
        self.assertEqual(1, seasons[0]["season_number"])
        self.assertEqual(13, seasons[0]["episode_count"])
        self.assertEqual(["en-US", "ja-JP"], seasons[0]["audio_locales"])

    def test_discover_item_does_not_infer_english_from_dub_flag_or_title(self) -> None:
        result = crunchyroll._series_from_crunchyroll_item(
            {
                "id": "GJONLY",
                "title": "Japanese Only English Title",
                "season_title": "Japanese Only (English Dub)",
                "series_metadata": {"is_dubbed": True, "audio_locales": ["ja-JP"]},
            }
        )

        self.assertIsNotNone(result)
        assert result is not None
        self.assertEqual(result["audio_locales"], ["ja-JP"])

    def test_discover_item_handles_malformed_series_metadata_audio_locales(self) -> None:
        result = crunchyroll._series_from_crunchyroll_item(
            {"id": "GMALFORMED", "title": "Malformed", "series_metadata": {"audio_locales": "en-US"}}
        )

        self.assertIsNotNone(result)
        assert result is not None
        self.assertEqual(result["audio_locales"], [])

    def test_cms_series_detail_merge_supplies_audio_without_changing_search_title(self) -> None:
        match = {
            "provider_series_id": "G6NQ5DWZ6",
            "title": "My Hero Academia",
            "season_title": "My Hero Academia",
            "audio_locales": [],
            "raw": {"source": "search"},
        }
        detail = {
            "id": "G6NQ5DWZ6",
            "title": "My Hero Academia",
            "slug_title": "my-hero-academia",
            "availability_status": "available",
            "audio_locales": ["ja-JP", "en_US"],
        }

        result = crunchyroll._merge_search_result_with_detail(match, detail)

        self.assertEqual(result["provider_series_id"], "G6NQ5DWZ6")
        self.assertEqual(result["title"], "My Hero Academia")
        self.assertEqual(result["audio_locales"], ["ja-JP", "en-US"])
        self.assertEqual(result["catalog_status"], "present")
        self.assertEqual(result["detail_evidence_source"], "crunchyroll_cms_series")
        self.assertIn("detail", result["raw"])

    def test_cms_series_detail_merge_exposes_child_season_identity_metadata(self) -> None:
        match = {
            "provider_series_id": "GYW4MG9G6",
            "title": "Rascal Does Not Dream Series",
            "season_title": "Rascal Does Not Dream Series",
            "audio_locales": [],
            "raw": {"source": "search"},
        }
        detail = {
            "id": "GYW4MG9G6",
            "title": "Rascal Does Not Dream Series",
            "audio_locales": ["ja-JP"],
            "seasons": [
                {
                    "id": "GRZXCM4XJ",
                    "series_id": "GYW4MG9G6",
                    "title": "Rascal Does Not Dream of Bunny Girl Senpai",
                    "season_metadata": {"season_number": 1, "episode_count": 13, "audio_locales": ["ja-JP"]},
                }
            ],
        }

        result = crunchyroll._merge_search_result_with_detail(match, detail)

        self.assertEqual("Rascal Does Not Dream of Bunny Girl Senpai", result["children"][0]["title"])
        self.assertEqual("GRZXCM4XJ", result["children"][0]["id"])
        self.assertEqual("GYW4MG9G6", result["children"][0]["series_id"])
        self.assertEqual(1, result["children"][0]["season_number"])
        self.assertEqual(13, result["children"][0]["episode_count"])
        self.assertEqual(["ja-JP"], result["children"][0]["audio_locales"])

    def test_search_normalization_dedupes_and_limits_grouped_results(self) -> None:
        payload = {
            "data": [
                {
                    "type": "top_results",
                    "items": [
                        {"id": "A", "title": "A", "type": "series", "series_metadata": {"audio_locales": ["en-US"]}},
                        {"id": "A", "title": "A Duplicate", "type": "series", "series_metadata": {"audio_locales": ["ja-JP"]}},
                        {"id": "B", "title": "B", "type": "movie_listing", "series_metadata": {"audio_locales": ["ja-JP"]}},
                    ],
                },
                {"type": "series", "items": [{"id": "C", "title": "C", "type": "series"}]},
            ]
        }

        results = []
        seen_ids: set[str] = set()
        for bucket_type in ("top_results", "series", "movie_listing"):
            for bucket in payload["data"]:
                if bucket.get("type") != bucket_type:
                    continue
                for item in bucket.get("items", []):
                    summary = crunchyroll._series_from_crunchyroll_item(item)
                    if summary is None or summary["provider_series_id"] in seen_ids:
                        continue
                    seen_ids.add(summary["provider_series_id"])
                    results.append(summary)
                    if len(results) >= 2:
                        break
                if len(results) >= 2:
                    break
            if len(results) >= 2:
                break

        self.assertEqual(["A", "B"], [result["provider_series_id"] for result in results])
        self.assertEqual(["en-US"], results[0]["audio_locales"])

    def test_search_title_passes_configured_spacing_jitter_to_pacer_without_network(self) -> None:
        payload = {
            "data": [
                {
                    "type": "top_results",
                    "items": [{"id": "GSEARCH", "title": "Search Result", "type": "series"}],
                }
            ]
        }
        config = SimpleNamespace(
            crunchyroll=SimpleNamespace(
                request_spacing_seconds=1.25,
                request_spacing_jitter_seconds=0.75,
                request_timeout_seconds=9.0,
                locale="en-US",
            )
        )
        captured = {}

        class FakeSession:
            def authorized_json_get(self, endpoint, *, params=None, phase=None):
                return payload

        def fake_start_auth_session(config_arg, *, profile, timeout_seconds, pacer):
            captured["config"] = config_arg
            captured["profile"] = profile
            captured["timeout_seconds"] = timeout_seconds
            captured["pacer"] = pacer
            return FakeSession()

        with patch("mal_updater.crunchyroll_snapshot._start_auth_session", side_effect=fake_start_auth_session):
            results = crunchyroll._search_title(config, "Search Result", limit=3)

        self.assertEqual(["GSEARCH"], [result["provider_series_id"] for result in results])
        self.assertEqual(1.25, captured["pacer"].spacing_seconds)
        self.assertEqual(0.75, captured["pacer"].jitter_seconds)
        self.assertEqual(9.0, captured["timeout_seconds"])

    def test_fetch_search_result_detail_passes_configured_spacing_jitter_to_pacer_without_network(self) -> None:
        config = SimpleNamespace(
            crunchyroll=SimpleNamespace(
                request_spacing_seconds=2.5,
                request_spacing_jitter_seconds=1.5,
                request_timeout_seconds=8.0,
                locale="en-US",
            )
        )
        match = {"provider_series_id": "GDETAIL", "title": "Detail Result", "audio_locales": [], "raw": {}}
        captured = {}

        class FakeSession:
            def authorized_json_get(self, endpoint, *, params=None, phase=None):
                captured["endpoint"] = endpoint
                captured["params"] = params
                captured["phase"] = phase
                return {"data": {"id": "GDETAIL", "title": "Detail Result", "audio_locales": ["en-US"]}}

        def fake_start_auth_session(config_arg, *, profile, timeout_seconds, pacer):
            captured["config"] = config_arg
            captured["profile"] = profile
            captured["timeout_seconds"] = timeout_seconds
            captured["pacer"] = pacer
            return FakeSession()

        with patch("mal_updater.crunchyroll_snapshot._start_auth_session", side_effect=fake_start_auth_session):
            result = crunchyroll._fetch_search_result_detail(config, match)

        self.assertEqual(["en-US"], result["audio_locales"])
        self.assertEqual(2.5, captured["pacer"].spacing_seconds)
        self.assertEqual(1.5, captured["pacer"].jitter_seconds)
        self.assertEqual(8.0, captured["timeout_seconds"])
        self.assertEqual("title-detail", captured["phase"])

    def test_fetch_search_result_children_passes_configured_spacing_jitter_without_network(self) -> None:
        config = SimpleNamespace(
            crunchyroll=SimpleNamespace(
                request_spacing_seconds=3.0,
                request_spacing_jitter_seconds=1.0,
                request_timeout_seconds=7.0,
                locale="en-US",
            )
        )
        match = {"provider_series_id": "GYW4MG9G6", "title": "Rascal Does Not Dream Series"}
        captured = {}

        class FakeSession:
            def authorized_json_get(self, endpoint, *, params=None, phase=None):
                captured["endpoint"] = endpoint
                captured["params"] = params
                captured["phase"] = phase
                return {"data": [{"id": "GRZXCM4XJ", "series_id": "GYW4MG9G6", "title": "Rascal Does Not Dream of Bunny Girl Senpai", "season_number": 1, "episode_count": 13}]}

        def fake_start_auth_session(config_arg, *, profile, timeout_seconds, pacer):
            captured["config"] = config_arg
            captured["profile"] = profile
            captured["timeout_seconds"] = timeout_seconds
            captured["pacer"] = pacer
            return FakeSession()

        with patch("mal_updater.crunchyroll_snapshot._start_auth_session", side_effect=fake_start_auth_session):
            result = crunchyroll._fetch_search_result_children(config, match)

        self.assertEqual("title-detail-seasons", captured["phase"])
        self.assertTrue(captured["endpoint"].endswith("/content/v2/cms/series/GYW4MG9G6/seasons"))
        self.assertEqual(3.0, captured["pacer"].spacing_seconds)
        self.assertEqual(1.0, captured["pacer"].jitter_seconds)
        self.assertEqual(7.0, captured["timeout_seconds"])
        self.assertEqual("Rascal Does Not Dream of Bunny Girl Senpai", result[0]["title"])
        self.assertEqual("GRZXCM4XJ", result[0]["id"])
        self.assertEqual("GYW4MG9G6", result[0]["series_id"])
        self.assertEqual(1, result[0]["season_number"])


if __name__ == "__main__":
    unittest.main()
