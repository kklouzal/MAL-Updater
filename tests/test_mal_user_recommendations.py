from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from urllib.parse import urlparse

from mal_updater.config import AppConfig, MalSettings
from mal_updater.db import (
    MAL_RECOMMENDATION_SOURCE_OFFICIAL_DETAIL,
    MAL_RECOMMENDATION_SOURCE_PUBLIC_USERRECS,
    bootstrap_database,
    connect,
    record_mal_recommendation_harvest_failure,
    replace_mal_public_userrecs_recommendation_edges,
    replace_mal_recommendation_edges,
)
from mal_updater.mal_user_recommendations import (
    PublicMalUserRecommendationsClient,
    PublicMalUserRecommendationsError,
    build_public_user_recs_url,
    parse_public_user_recommendations_page,
    validate_public_user_recs_url,
)


PUBLIC_BASE_URL = "https://myanimelist.net"
SOURCE_URL = "https://myanimelist.net/anime/1/Cowboy_Bebop/userrecs"


class _Headers(dict):
    def get_content_charset(self) -> str | None:
        return None


class _HtmlResponse:
    status = 200

    def __init__(self, url: str, body: str | bytes) -> None:
        self._url = url
        self._body = body.encode("utf-8") if isinstance(body, str) else body
        self.headers = _Headers()

    def __enter__(self) -> "_HtmlResponse":
        return self

    def __exit__(self, *_: object) -> None:
        return None

    def geturl(self) -> str:
        return self._url

    def read(self, size: int = -1) -> bytes:
        if size is None or size < 0:
            return self._body
        return self._body[:size]


class _FakeOpener:
    def __init__(self, pages: dict[str, str | bytes]) -> None:
        self.pages = pages
        self.requested_urls: list[str] = []
        self.user_agents: list[str | None] = []

    def open(self, request, timeout):  # type: ignore[no-untyped-def]
        self.requested_urls.append(request.full_url)
        self.user_agents.append(request.headers.get("User-agent") or request.headers.get("User-Agent"))
        try:
            return _HtmlResponse(request.full_url, self.pages[request.full_url])
        except KeyError as exc:  # pragma: no cover - test fixture guard
            raise AssertionError(f"unexpected URL {request.full_url}") from exc


def _config(root: Path) -> AppConfig:
    runtime_root = root / ".MAL-Updater"
    return AppConfig(
        project_root=root,
        workspace_root=root,
        runtime_root=runtime_root,
        settings_path=runtime_root / "config" / "settings.toml",
        config_dir=runtime_root / "config",
        secrets_dir=runtime_root / "secrets",
        data_dir=runtime_root / "data",
        state_dir=runtime_root / "state",
        cache_dir=runtime_root / "cache",
        db_path=runtime_root / "data" / "mal_updater.sqlite3",
        mal=MalSettings(request_spacing_seconds=0.0, request_spacing_jitter_seconds=0.0),
    )


def _block(target_id: int, title: str, *, more_users: int | None = None) -> str:
    more = f'<a href="/recommendations/anime/1-{target_id}">Read recommendations by {more_users} more users</a>' if more_users is not None else ""
    return f"""
    <tr class="borderClass">
      <td><a href="/anime/{target_id}/{title.replace(' ', '_')}">{title}</a></td>
      <td>Recommended by <a href="/profile/private{target_id}">private{target_id}</a>. {more}</td>
    </tr>
    """


def _page(blocks: list[str], *, next_href: str | None = None) -> str:
    next_link = f'<a rel="next" href="{next_href}">Next</a>' if next_href else ""
    return f"<html><body><h1>User recommendations</h1><table>{''.join(blocks)}</table>{next_link}</body></html>"


class PublicMalUserRecommendationsParserTests(unittest.TestCase):
    def test_parser_extracts_more_than_ten_targets_and_one_page_complete_counts(self) -> None:
        blocks = [_block(1000 + index, f"Target {index}", more_users=index) for index in range(12)]
        parsed = parse_public_user_recommendations_page(
            _page(blocks),
            source_mal_anime_id=1,
            page_url=SOURCE_URL,
            public_base_url=PUBLIC_BASE_URL,
        )

        self.assertIsNone(parsed.next_url)
        self.assertEqual(12, len(parsed.edges))
        counts = {edge.target_mal_anime_id: edge.num_recommendations for edge in parsed.edges}
        self.assertEqual(1, counts[1000])
        self.assertEqual(12, counts[1011])
        self.assertEqual(1011, parsed.edges[0].target_mal_anime_id)

    def test_parser_validates_advertised_next_and_rejects_out_of_origin(self) -> None:
        parsed = parse_public_user_recommendations_page(
            _page([_block(2, "Two")], next_href="/anime/1/Cowboy_Bebop/userrecs?p=2"),
            source_mal_anime_id=1,
            page_url=SOURCE_URL,
            public_base_url=PUBLIC_BASE_URL,
        )
        self.assertEqual("/anime/1/Cowboy_Bebop/userrecs", urlparse(parsed.next_url or "").path)

        with self.assertRaises(PublicMalUserRecommendationsError):
            parse_public_user_recommendations_page(
                _page([_block(2, "Two")], next_href="https://evil.example/anime/1/Cowboy_Bebop/userrecs?p=2"),
                source_mal_anime_id=1,
                page_url=SOURCE_URL,
                public_base_url=PUBLIC_BASE_URL,
            )

    def test_parser_ignores_out_of_origin_target_anime_links(self) -> None:
        parsed = parse_public_user_recommendations_page(
            _page(
                [
                    """
                    <tr class="borderClass">
                      <td><a href="https://evil.example/anime/2/Two">Two</a></td>
                      <td>Recommended by <a href="/profile/private2">private2</a>.</td>
                    </tr>
                    """
                ]
            ),
            source_mal_anime_id=1,
            page_url=SOURCE_URL,
            public_base_url=PUBLIC_BASE_URL,
        )

        self.assertEqual([], parsed.edges)

    def test_validate_public_user_recs_url_requires_myanimelist_https_same_origin_and_path(self) -> None:
        self.assertEqual(
            SOURCE_URL,
            validate_public_user_recs_url(SOURCE_URL, public_base_url=PUBLIC_BASE_URL, source_mal_anime_id=1),
        )
        for unsafe in (
            "http://myanimelist.net/anime/1/Cowboy_Bebop/userrecs",
            "https://token@myanimelist.net/anime/1/Cowboy_Bebop/userrecs",
            "https://evil.example/anime/1/Cowboy_Bebop/userrecs",
            "https://myanimelist.net/anime/2/Other/userrecs",
            "https://myanimelist.net/anime/1/Cowboy_Bebop",
        ):
            with self.subTest(unsafe=unsafe):
                with self.assertRaises(PublicMalUserRecommendationsError):
                    validate_public_user_recs_url(unsafe, public_base_url=PUBLIC_BASE_URL, source_mal_anime_id=1)
        with self.assertRaises(PublicMalUserRecommendationsError):
            validate_public_user_recs_url(SOURCE_URL, public_base_url="https://proxy.example", source_mal_anime_id=1)

    def test_malformed_page_without_recommendation_surface_fails(self) -> None:
        with self.assertRaises(PublicMalUserRecommendationsError):
            parse_public_user_recommendations_page(
                "<html><body><p>maintenance</p></body></html>",
                source_mal_anime_id=1,
                page_url=SOURCE_URL,
                public_base_url=PUBLIC_BASE_URL,
            )

    def test_generic_error_or_challenge_recommendation_text_cannot_prove_empty_harvest(self) -> None:
        generic_pages = {
            "global_nav": """
                <html><body>
                  <nav><a href="/recommendations.php">Recommendations</a></nav>
                  <main><h1>Temporarily unavailable</h1><p>Try anime recommendations later.</p></main>
                </body></html>
            """,
            "challenge": """
                <html><body>
                  <h1>Checking your browser before accessing MyAnimeList</h1>
                  <p>Recommended by our community: sign in after the challenge.</p>
                </body></html>
            """,
            "error": """
                <html><head><title>Recommendations - MyAnimeList.net</title></head>
                <body><h1>404 Not Found</h1><p>Recommended by navigation, not a userrecs page.</p></body></html>
            """,
            "edge_like_error": """
                <html><body><h1>Temporary error</h1><table>
                  <tr><td><a href="/anime/2/Two">Two</a></td><td>Recommended by error copy.</td></tr>
                </table></body></html>
            """,
        }
        for name, html in generic_pages.items():
            with self.subTest(name=name):
                with self.assertRaises(PublicMalUserRecommendationsError):
                    parse_public_user_recommendations_page(
                        html,
                        source_mal_anime_id=1,
                        page_url=SOURCE_URL,
                        public_base_url=PUBLIC_BASE_URL,
                    )

    def test_recognized_empty_userrecs_page_is_complete_with_no_edges(self) -> None:
        parsed = parse_public_user_recommendations_page(
            """
            <html><body>
              <div id="content">
                <div class="border_solid">
                  <div class="floatRightHeader"><a href="/myrecommendations.php?go=make&amp;aid=1">Make a recommendation</a></div>
                  <h2 class="h2_overwrite">Recommendations</h2>
                </div>
                <div class="borderClass"><p>No recommendations have been made for this title yet.</p></div>
              </div>
            </body></html>
            """,
            source_mal_anime_id=1,
            page_url=SOURCE_URL,
            public_base_url=PUBLIC_BASE_URL,
        )

        self.assertEqual([], parsed.edges)
        self.assertIsNone(parsed.next_url)


class PublicMalUserRecommendationsClientTests(unittest.TestCase):
    def test_client_traverses_next_page_and_merges_duplicate_targets_deterministically(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            config = _config(Path(tmp))
            start_url = build_public_user_recs_url(config.mal.public_base_url, source_mal_anime_id=1, source_title="Cowboy Bebop")
            page2_url = f"{start_url}?p=2"
            opener = _FakeOpener(
                {
                    start_url: _page([_block(2, "Two", more_users=1), _block(3, "Three", more_users=2)], next_href=page2_url),
                    page2_url: _page([_block(2, "Two", more_users=4), _block(4, "Four")]),
                }
            )
            client = PublicMalUserRecommendationsClient(config, opener=opener, sleep=lambda _seconds: None, clock=lambda: 0.0)

            result = client.harvest(1, source_title="Cowboy Bebop", max_pages=2)

        self.assertTrue(result.complete)
        self.assertEqual(2, result.pages_fetched)
        self.assertEqual([start_url, page2_url], opener.requested_urls)
        self.assertTrue(all("public-user-recommendation-harvest" in (agent or "") for agent in opener.user_agents))
        counts = {edge.target_mal_anime_id: edge.num_recommendations for edge in result.edges}
        self.assertEqual({2: 5, 3: 3, 4: 1}, counts)
        self.assertEqual([2, 3, 4], [edge.target_mal_anime_id for edge in result.edges])

    def test_client_reports_loop_partial_and_max_page_partial_without_returning_edges(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            config = _config(Path(tmp))
            start_url = build_public_user_recs_url(config.mal.public_base_url, source_mal_anime_id=1, source_title="Cowboy Bebop")
            loop_client = PublicMalUserRecommendationsClient(
                config,
                opener=_FakeOpener({start_url: _page([_block(2, "Two")], next_href=start_url)}),
                sleep=lambda _seconds: None,
                clock=lambda: 0.0,
            )
            loop = loop_client.harvest(1, source_title="Cowboy Bebop", max_pages=3)
            partial_client = PublicMalUserRecommendationsClient(
                config,
                opener=_FakeOpener({start_url: _page([_block(2, "Two")], next_href=f"{start_url}?p=2")}),
                sleep=lambda _seconds: None,
                clock=lambda: 0.0,
            )
            partial = partial_client.harvest(1, source_title="Cowboy Bebop", max_pages=1)

        self.assertEqual("failed", loop.status)
        self.assertTrue(loop.partial)
        self.assertEqual([], loop.edges)
        self.assertIn("loop", loop.error or "")
        self.assertEqual("failed", partial.status)
        self.assertTrue(partial.partial)
        self.assertEqual([], partial.edges)
        self.assertIn("max_pages", partial.error or "")

    def test_client_rejects_oversize_body(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            config = _config(Path(tmp))
            start_url = build_public_user_recs_url(config.mal.public_base_url, source_mal_anime_id=1, source_title="Cowboy Bebop")
            client = PublicMalUserRecommendationsClient(
                config,
                opener=_FakeOpener({start_url: b"x" * 1025}),
                sleep=lambda _seconds: None,
                clock=lambda: 0.0,
            )
            with self.assertRaisesRegex(PublicMalUserRecommendationsError, "body exceeded"):
                client.harvest(1, source_title="Cowboy Bebop", max_body_bytes=1024)


class PublicMalUserRecommendationsDbTests(unittest.TestCase):
    def test_complete_replace_is_atomic_and_failure_preserves_previous_complete_edges(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "mal.sqlite3"
            bootstrap_database(db_path)
            complete_edges = [
                {"target_mal_anime_id": 10, "target_title": "Ten", "num_recommendations": 7, "raw": {"source": "public_mal_userrecs"}, "provenance": {"page_count": 1}},
                {"target_mal_anime_id": 11, "target_title": "Eleven", "num_recommendations": 3, "raw": {"source": "public_mal_userrecs"}, "provenance": {"page_count": 1}},
            ]
            self.assertTrue(
                replace_mal_public_userrecs_recommendation_edges(
                    db_path,
                    source_mal_anime_id=1,
                    edges=complete_edges,
                    pages_fetched=1,
                    source_url=SOURCE_URL,
                )
            )

            with self.assertRaises(KeyError):
                replace_mal_public_userrecs_recommendation_edges(
                    db_path,
                    source_mal_anime_id=1,
                    edges=[{"target_mal_anime_id": 99, "raw": {}}, {"raw": {}}],
                    pages_fetched=2,
                    source_url=SOURCE_URL,
                )

            record_mal_recommendation_harvest_failure(
                db_path,
                source_mal_anime_id=1,
                source_type=MAL_RECOMMENDATION_SOURCE_PUBLIC_USERRECS,
                error="malformed page",
                pages_fetched=1,
                source_url=SOURCE_URL,
            )

            with connect(db_path) as conn:
                rows = conn.execute(
                    """
                    SELECT target_mal_anime_id, num_recommendations, harvest_source, complete_harvest, provenance_json
                    FROM mal_anime_recommendations
                    WHERE source_mal_anime_id = 1
                    ORDER BY target_mal_anime_id
                    """
                ).fetchall()
                status = conn.execute(
                    """
                    SELECT status, num_edges, source_type, is_complete, pages_fetched, source_url, last_error, failure_count
                    FROM mal_recommendation_harvest_status
                    WHERE source_mal_anime_id = 1
                    """
                ).fetchone()

        self.assertEqual([10, 11], [int(row["target_mal_anime_id"]) for row in rows])
        self.assertEqual([7, 3], [int(row["num_recommendations"]) for row in rows])
        self.assertTrue(all(row["harvest_source"] == MAL_RECOMMENDATION_SOURCE_PUBLIC_USERRECS for row in rows))
        self.assertTrue(all(int(row["complete_harvest"]) == 1 for row in rows))
        self.assertEqual({"page_count": 1}, json.loads(rows[0]["provenance_json"]))
        self.assertEqual("fetched", status["status"])
        self.assertEqual(2, status["num_edges"])
        self.assertEqual(MAL_RECOMMENDATION_SOURCE_PUBLIC_USERRECS, status["source_type"])
        self.assertEqual(1, status["is_complete"])
        self.assertEqual(1, status["pages_fetched"])
        self.assertEqual(SOURCE_URL, status["source_url"])
        self.assertEqual("malformed page", status["last_error"])
        self.assertEqual(1, status["failure_count"])

    def test_official_recommendation_refresh_does_not_clobber_complete_public_harvest(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "mal.sqlite3"
            bootstrap_database(db_path)
            replace_mal_public_userrecs_recommendation_edges(
                db_path,
                source_mal_anime_id=1,
                edges=[{"target_mal_anime_id": 10, "target_title": "Ten", "num_recommendations": 7, "raw": {}, "provenance": {}}],
                pages_fetched=1,
                source_url=SOURCE_URL,
            )
            stored = replace_mal_recommendation_edges(
                db_path,
                source_mal_anime_id=1,
                hop_distance=1,
                edges=[{"target_mal_anime_id": 99, "target_title": "Official Only", "num_recommendations": 1, "raw": {}}],
                source_type=MAL_RECOMMENDATION_SOURCE_OFFICIAL_DETAIL,
                complete=False,
            )
            with connect(db_path) as conn:
                targets = [
                    int(row["target_mal_anime_id"])
                    for row in conn.execute(
                        "SELECT target_mal_anime_id FROM mal_anime_recommendations WHERE source_mal_anime_id = 1 ORDER BY target_mal_anime_id"
                    )
                ]
                status = conn.execute("SELECT status, source_type, is_complete FROM mal_recommendation_harvest_status WHERE source_mal_anime_id = 1").fetchone()

        self.assertFalse(stored)
        self.assertEqual([10], targets)
        self.assertEqual("fetched", status["status"])
        self.assertEqual(MAL_RECOMMENDATION_SOURCE_PUBLIC_USERRECS, status["source_type"])
        self.assertEqual(1, status["is_complete"])


if __name__ == "__main__":
    unittest.main()
