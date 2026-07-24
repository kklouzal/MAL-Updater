from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from mal_updater.config import load_config
from mal_updater.providers.crunchyroll import CrunchyrollProvider


class _Session:
    def __init__(self) -> None:
        self.calls: list[str] = []

    def authorized_json_get(self, url: str, **_kwargs):
        self.calls.append(url)
        if url.endswith("/discover/search"):
            return {"data": [{"type": "series", "items": [{"type": "series", "id": "SERIES", "title": "Example"}]}]}
        if url.endswith("/seasons"):
            return {"data": [{"id": "SEASON", "series_id": "SERIES", "title": "Example Season 1", "season_number": 1}]}
        return {"data": [{"type": "series", "id": "SERIES", "title": "Example", "audio_locales": ["en-US"]}]}


class CrunchyrollProviderSessionTests(unittest.TestCase):
    def test_search_detail_and_children_reuse_one_authenticated_session(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            config = load_config(Path(td))
            provider = CrunchyrollProvider()
            session = _Session()
            with patch("mal_updater.crunchyroll_snapshot._start_auth_session", return_value=session) as start:
                batch_session = provider.create_request_session(config)
                results = provider.search_title(config, "Example", session=batch_session)
                detail = provider.fetch_search_result_detail(config, results[0], session=batch_session)
                children = provider.fetch_search_result_children(config, results[0], session=batch_session)

            start.assert_called_once()
            self.assertEqual(3, len(session.calls))
            self.assertEqual(["en-US"], detail["audio_locales"])
            self.assertEqual("SEASON", children[0]["id"])


if __name__ == "__main__":
    unittest.main()
