from __future__ import annotations

import unittest

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


if __name__ == "__main__":
    unittest.main()
