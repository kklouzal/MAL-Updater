from __future__ import annotations

import json
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from mal_updater.providers import hidive
from mal_updater.provider_types import ProviderSearchResult


class _FakeCurlResponse:
    def __init__(self, payload: dict):
        self.payload = payload
        self.raise_for_status_called = False

    def raise_for_status(self) -> None:
        self.raise_for_status_called = True

    def json(self) -> dict:
        return self.payload


class HidiveProviderSearchTests(unittest.TestCase):
    def test_normalizes_algolia_vod_series_hits_and_ignores_videos(self) -> None:
        payload = {
            "hits": [
                {"objectID": "VOD_VIDEO_655788", "id": 655788, "type": "VOD_VIDEO", "localisations": {"en_US": {"title": "E12 - Dungeon People"}}},
                {
                    "objectID": "VOD_SERIES_2312",
                    "id": 2312,
                    "type": "VOD_SERIES",
                    "localisations": {"en_US": {"title": "Dungeon People"}},
                    "slug": "dungeon-people",
                    "tags": ["AUDIO|English", "Audio|Japanese", "SUBTITLES|English", "English"],
                },
                {"objectID": "VOD_SERIES_2312", "id": 2312, "type": "VOD_SERIES", "title": "Dungeon People Duplicate"},
            ]
        }

        results = hidive._normalize_algolia_hits(payload, limit=10)

        self.assertEqual(1, len(results))
        self.assertIsInstance(results[0], ProviderSearchResult)
        self.assertEqual(results[0].provider_series_id, "2312")
        self.assertEqual(results[0].title, "Dungeon People")
        self.assertEqual(results[0].season_title, "Dungeon People")
        self.assertEqual(results[0].url, "https://www.hidive.com/season/dungeon-people")
        self.assertEqual(results[0].audio_locales, ["en-US", "ja-JP"])
        self.assertEqual(results[0].raw["type"], "VOD_SERIES")
        self.assertIn("AUDIO|English", results[0].raw["tags"])

    def test_explicit_english_audio_requires_audio_tag_not_title_or_bare_tag(self) -> None:
        payload = {
            "hits": [
                {
                    "objectID": "VOD_SERIES_100",
                    "type": "VOD_SERIES",
                    "title": "English Named Japanese Only Show",
                    "tags": ["English", "SUBTITLES|English", "Audio|Japanese"],
                }
            ]
        }

        results = hidive._normalize_algolia_hits(payload, limit=10)

        self.assertEqual(1, len(results))
        self.assertEqual(results[0].audio_locales, ["ja-JP"])

    def test_malformed_tags_do_not_create_audio_locales(self) -> None:
        payload = {"hits": [{"objectID": "VOD_SERIES_101", "type": "VOD_SERIES", "title": "Malformed", "tags": "AUDIO|English"}]}

        results = hidive._normalize_algolia_hits(payload, limit=10)

        self.assertEqual(1, len(results))
        self.assertEqual(results[0].audio_locales, [])

    def test_normalize_algolia_hits_dedupes_and_applies_limit(self) -> None:
        payload = {
            "hits": [
                {"objectID": "VOD_SERIES_1", "type": "VOD_SERIES", "title": "One", "tags": ["Audio|English"]},
                {"objectID": "VOD_SERIES_1", "type": "VOD_SERIES", "title": "One Duplicate", "tags": ["Audio|Japanese"]},
                {"objectID": "VOD_SERIES_2", "type": "VOD_SERIES", "title": "Two", "tags": ["Audio|Japanese"]},
                {"objectID": "VOD_SERIES_3", "type": "VOD_SERIES", "title": "Three", "tags": ["Audio|English"]},
            ]
        }

        results = hidive._normalize_algolia_hits(payload, limit=2)

        self.assertEqual(["1", "2"], [result.provider_series_id for result in results])
        self.assertEqual(["en-US"], results[0].audio_locales)
        self.assertEqual(["ja-JP"], results[1].audio_locales)

    def test_normalize_algolia_hits_rejects_malformed_payload(self) -> None:
        with self.assertRaisesRegex(ValueError, "Unexpected HIDIVE Algolia"):
            hidive._normalize_algolia_hits({"hits": {"not": "a list"}}, limit=10)

    def test_search_title_posts_series_only_algolia_query_with_curl_cffi(self) -> None:
        payload = {
            "hits": [
                {
                    "objectID": "VOD_SERIES_42",
                    "type": "VOD_SERIES",
                    "localisations": {"en_US": {"title": "Test Show"}},
                    "tags": ["Audio|English"],
                }
            ]
        }
        response = _FakeCurlResponse(payload)
        config = SimpleNamespace(request_timeout_seconds=12)

        with patch.object(hidive, "curl_requests", SimpleNamespace(post=lambda *args, **kwargs: response)) as fake_curl:
            with patch.object(fake_curl, "post", return_value=response) as mock_post:
                results = hidive._search_title(config, "Test Show", limit=3)

        self.assertEqual(["42"], [r.provider_series_id for r in results])
        self.assertEqual([["en-US"]], [r.audio_locales for r in results])
        self.assertTrue(response.raise_for_status_called)
        call = mock_post.call_args
        self.assertEqual(call.args[0], hidive.HIDIVE_ALGOLIA_ENDPOINT)
        self.assertEqual(call.kwargs["headers"]["x-algolia-application-id"], hidive.HIDIVE_ALGOLIA_APP_ID)
        self.assertEqual(call.kwargs["headers"]["content-type"], "application/json")
        self.assertEqual(call.kwargs["timeout"], (12.0, 12.0))
        self.assertEqual(call.kwargs["impersonate"], "chrome124")
        body = json.loads(call.kwargs["data"])
        self.assertIn("query=Test+Show", body["params"])
        self.assertIn("hitsPerPage=3", body["params"])
        self.assertIn("filters=type%3AVOD_SERIES", body["params"])


if __name__ == "__main__":
    unittest.main()
