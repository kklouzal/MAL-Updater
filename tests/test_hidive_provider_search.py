from __future__ import annotations

import json
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from mal_updater.providers import hidive
from mal_updater.provider_types import ProviderSearchResult


class _FakeResponse:
    def __init__(self, payload: dict):
        self.payload = payload

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def read(self):
        return json.dumps(self.payload).encode("utf-8")


class HidiveProviderSearchTests(unittest.TestCase):
    def test_normalizes_algolia_vod_series_hits_and_ignores_videos(self) -> None:
        payload = {
            "hits": [
                {"objectID": "VOD_VIDEO_655788", "id": 655788, "type": "VOD_VIDEO", "localisations": {"en_US": {"title": "E12 - Dungeon People"}}},
                {"objectID": "VOD_SERIES_2312", "id": 2312, "type": "VOD_SERIES", "localisations": {"en_US": {"title": "Dungeon People"}}, "slug": "dungeon-people"},
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
        self.assertEqual(results[0].raw["type"], "VOD_SERIES")

    def test_search_title_posts_series_only_algolia_query(self) -> None:
        payload = {"hits": [{"objectID": "VOD_SERIES_42", "type": "VOD_SERIES", "localisations": {"en_US": {"title": "Test Show"}}}]}
        config = SimpleNamespace(hidive=SimpleNamespace(request_timeout_seconds=12))

        with patch("mal_updater.providers.hidive.urlopen", return_value=_FakeResponse(payload)) as mock_urlopen:
            results = hidive._search_title(config, "Test Show", limit=3)

        self.assertEqual(["42"], [r.provider_series_id for r in results])
        request = mock_urlopen.call_args.args[0]
        self.assertEqual(request.method, "POST")
        self.assertIn("prod-dce.hidive-livestreaming-events", request.full_url)
        self.assertEqual(request.headers["X-algolia-application-id"], hidive.HIDIVE_ALGOLIA_APP_ID)
        self.assertEqual(request.headers["Content-type"], "application/json")
        body = json.loads(request.data.decode("utf-8"))
        self.assertIn("query=Test+Show", body["params"])
        self.assertIn("hitsPerPage=3", body["params"])
        self.assertIn("filters=type%3AVOD_SERIES", body["params"])


if __name__ == "__main__":
    unittest.main()
