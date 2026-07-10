from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from threading import Thread
from urllib.request import urlopen
from http.server import ThreadingHTTPServer
import json
import io
from unittest.mock import patch

from mal_updater.db import bootstrap_database, connect, insert_recommendation_snapshot_rows
from mal_updater.cli import build_parser
from mal_updater.recommendation_dashboard import DASHBOARD_DEFAULT_RECOMMENDATION_LIMIT, build_dashboard_payload, make_dashboard_handler, render_dynamic_dashboard_html, render_recommendation_dashboard, write_recommendation_dashboard
from mal_updater.recommendations import Recommendation


class RecommendationDashboardTests(unittest.TestCase):
    def test_render_includes_sortable_requested_columns_and_escapes_content(self) -> None:
        item = Recommendation(
            kind="discovery_candidate",
            priority=87,
            provider_series_id="cr-1",
            title="A <Great> Show",
            season_title="A <Great> Show (English Dub)",
            provider="crunchyroll",
            reasons=["recommended by 2 watched/mapped seed title(s)", "MAL mean score: 8.2"],
            context={
                "available_via_providers": ["crunchyroll", "hidive"],
                "supporting_source_count": 2,
                "aggregated_recommendation_votes": 34,
                "mean": 8.2,
                "popularity": 321,
                "completed_episode_count": 2,
                "max_episode_number": 12,
                "mal_watch_status": "watching",
                "mal_num_episodes_watched": 2,
                "mal_num_episodes": 12,
                "english_title": "The Great Show",
                "genres": ["Action", "Comedy"],
                "english_dub_signal": "present",
            },
        )

        html = render_recommendation_dashboard([item])

        for label in (
            "English title",
            "Score",
            "Source count",
            "Total votes",
            "Crunchyroll",
            "HIDIVE",
            "English dub",
            "MAL mean",
            "MAL popularity",
            "Provider progress",
            "MAL watch status",
            "Genres",
        ):
            self.assertIn(label, html)
        self.assertIn('data-key="score" data-type="number"', html)
        self.assertIn("The Great Show", html)
        self.assertIn("Action, Comedy", html)
        self.assertNotIn("recommended by 2 watched/mapped seed title", html)
        self.assertIn("34", html)
        self.assertIn("321", html)
        self.assertIn("2/12", html)
        self.assertIn("watching (2/12)", html)
        self.assertIn("addEventListener('click'", html)

    def test_dashboard_marks_hidive_mapping_backed_discovery_availability(self) -> None:
        item = Recommendation(
            kind="discovery_candidate",
            priority=92,
            provider_series_id="2312",
            title="Dungeon People",
            season_title="Dungeon People",
            provider="hidive",
            reasons=["available on HIDIVE via approved/provider mapping"],
            context={"available_via_providers": ["hidive"], "availability_visible": True, "supporting_source_count": 1},
        )

        html = render_recommendation_dashboard([item])
        self.assertIn("Dungeon People", html)
        self.assertIn("HIDIVE", html)
        self.assertIn("Discovery candidates", html)

    def test_static_dashboard_groups_discovery_and_backlog_sections(self) -> None:
        html = render_recommendation_dashboard(
            [
                Recommendation(
                    kind="discovery_candidate",
                    priority=95,
                    provider_series_id="mal:1",
                    title="MAL Only Candidate",
                    season_title=None,
                    provider=None,
                    reasons=["high-confidence MAL/catalog recommendation"],
                    context={"available_via_providers": [], "supporting_source_count": 3},
                ),
                Recommendation(
                    kind="discovery_candidate",
                    priority=90,
                    provider_series_id="cr-1",
                    title="Available Candidate",
                    season_title=None,
                    provider="crunchyroll",
                    reasons=["available now"],
                    context={"available_via_providers": ["crunchyroll"], "supporting_source_count": 2, "english_dub_signal": "present"},
                ),
                Recommendation(
                    kind="resume_backlog",
                    priority=80,
                    provider_series_id="hi-1",
                    title="Resume Me",
                    season_title=None,
                    provider="hidive",
                    reasons=["resume backlog"],
                ),
            ]
        )

        self.assertIn("Discovery candidates", html)
        self.assertIn("High-confidence discovery", html)
        self.assertIn("Resume backlog", html)
        self.assertIn("MAL Only Candidate", html)
        self.assertLess(html.index("Discovery candidates"), html.index("Available Candidate"))
        self.assertLess(html.index("High-confidence discovery"), html.index("MAL Only Candidate"))
        self.assertLess(html.index("Resume backlog"), html.index("Resume Me"))
        self.assertGreaterEqual(html.count('class="recommendations"'), 3)

    def test_static_dashboard_limit_is_per_section_not_global(self) -> None:
        rows = [
            Recommendation(kind="discovery_candidate", priority=200 - i, provider_series_id=f"mal:{i}", title=f"Discovery {i}", season_title=None, provider=None, reasons=["discovery"], context={"available_via_providers": []})
            for i in range(5)
        ]
        rows.extend(
            [
                Recommendation(kind="discovery_candidate", priority=120, provider_series_id="cr-available", title="Available Now", season_title=None, provider="crunchyroll", reasons=["available"], context={"available_via_providers": ["crunchyroll"], "english_dub_signal": "present"}),
                Recommendation(kind="resume_backlog", priority=80, provider_series_id="hi-resume", title="Resume Still Visible", season_title=None, provider="hidive", reasons=["resume"]),
            ]
        )

        html = render_recommendation_dashboard(rows, limit=2)

        self.assertIn("High-confidence discovery (2 of 5)", html)
        self.assertIn("Discovery 0", html)
        self.assertIn("Discovery 1", html)
        self.assertNotIn("Discovery 4", html)
        self.assertIn("Discovery candidates (1)", html)
        self.assertIn("Available Now", html)
        self.assertIn("Resume backlog (1)", html)
        self.assertIn("Resume Still Visible", html)

    def test_write_dashboard_creates_parent_directories(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            output = Path(temp_dir) / "nested" / "recommendations.html"
            written = write_recommendation_dashboard(output, [])

            self.assertEqual(output, written)
            html = output.read_text(encoding="utf-8")
            self.assertIn("No recommendations found.", html)
            self.assertIn("Click any column header to sort", html)

    def test_recommend_dashboard_cli_defaults_to_dashboard_limit_and_legacy_flag_help_is_noop(self) -> None:
        parser = build_parser()

        args = parser.parse_args(["recommend-dashboard", "--output", "recommendations.html"])
        self.assertEqual(DASHBOARD_DEFAULT_RECOMMENDATION_LIMIT, args.limit)

        with patch("sys.stdout", new_callable=io.StringIO) as stdout, self.assertRaises(SystemExit):
            parser.parse_args(["recommend-dashboard", "--help"])
        help_text = stdout.getvalue()
        self.assertNotIn("hidden by default", help_text)
        self.assertIn("Backward-compatible no-op", help_text)

    def test_live_dashboard_payload_reads_current_database_state(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "state.db"
            bootstrap_database(db_path)
            with connect(db_path) as conn:
                conn.execute("INSERT INTO provider_series (provider, provider_series_id, title) VALUES ('crunchyroll', 'cr-1', 'Show')")
                conn.execute("INSERT INTO mal_series_mapping (provider, provider_series_id, mal_anime_id, mapping_source, approved_by_user) VALUES ('crunchyroll', 'cr-1', 123, 'user_exact', 1)")
                conn.execute("INSERT INTO review_queue (provider, provider_series_id, issue_type, payload_json) VALUES ('crunchyroll', 'cr-1', 'mapping_candidate', '{}')")
                conn.execute(
                    "INSERT INTO sync_runs (provider, contract_version, mode, completed_at, status, summary_json) VALUES (?, ?, ?, CURRENT_TIMESTAMP, ?, ?)",
                    ("crunchyroll", "1", "snapshot", "completed", json.dumps({"rows": 1})),
                )
                conn.commit()
            insert_recommendation_snapshot_rows(
                db_path,
                [
                    {
                        "kind": "discovery_candidate",
                        "provider": "crunchyroll",
                        "title": "Fresh Show",
                        "provider_series_id": "cr-1",
                        "priority": 91,
                        "reasons": ["reason"],
                        "available_via_providers": ["crunchyroll"],
                        "context": {
                            "aggregated_recommendation_votes": 12,
                            "supporting_source_count": 2,
                            "supporting_seed_titles": ["Seed A", "Seed B"],
                            "english_dub": True,
                            "mal_watch_status": "plan_to_watch",
                            "english_title": "Fresh English Show",
                            "genres": ["Drama", "Sci-Fi"],
                            "english_dub_signal": "present",
                        },
                    },
                    {"kind": "resume_backlog", "provider": "hidive", "title": "Resume Me", "provider_series_id": "hi-1", "priority": 80, "reasons": ["resume"]},
                ],
                run_id="run-1",
                generated_at="2026-07-05T20:00:00Z",
            )

            payload = build_dashboard_payload(db_path)

            self.assertEqual(payload["snapshot"]["run_id"], "run-1")
            self.assertEqual(payload["snapshot"]["item_count"], 2)
            self.assertEqual(payload["recommendations"]["sections"]["discovery_available_now"][0]["english_title"], "Fresh English Show")
            self.assertEqual(payload["recommendations"]["sections"]["discovery_available_now"][0]["genres"], ["Drama", "Sci-Fi"])
            self.assertEqual(payload["recommendations"]["sections"]["discovery_available_now"][0]["reasons"], [])
            self.assertEqual(payload["recommendations"]["section_metadata"]["discovery_available_now"]["label"], "Discovery candidates")
            self.assertEqual(payload["recommendations"]["section_metadata"]["discovery_available_now"]["title_label"], "English title")
            self.assertIn("English dub evidence", payload["recommendations"]["section_metadata"]["discovery_available_now"]["description"])
            self.assertEqual(payload["recommendations"]["section_metadata"]["resume_backlog"]["label"], "Resume backlog")
            evidence = payload["recommendations"]["sections"]["discovery_available_now"][0]["evidence"]
            self.assertEqual(evidence["mal_recommendation_votes"], 12)
            self.assertEqual(evidence["seed_count"], 2)
            self.assertEqual(evidence["compact_seeds"], "Seed A, Seed B")
            self.assertEqual(evidence["availability_provider_label"], "crunchyroll")
            self.assertEqual(evidence["dub_signal"], "present")
            self.assertEqual(evidence["mal_watch_status"], "plan_to_watch")
            self.assertEqual(payload["operational"]["provider_counts_by_provider"]["crunchyroll"]["series"], 1)
            self.assertEqual(payload["operational"]["mappings"]["approved"], 1)
            self.assertEqual(payload["recent_sync_runs"][0]["status"], "completed")

    def test_live_dashboard_splits_mal_only_discovery_into_high_confidence(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "state.db"
            bootstrap_database(db_path)
            insert_recommendation_snapshot_rows(
                db_path,
                [
                    {
                        "kind": "discovery_candidate",
                        "provider": "mal",
                        "title": "MAL Only Candidate",
                        "provider_series_id": "mal:100",
                        "priority": 95,
                        "context": {"mal_anime_id": 100, "aggregated_recommendation_votes": 21},
                    },
                    {
                        "kind": "discovery_candidate",
                        "provider": "crunchyroll",
                        "title": "Available Candidate",
                        "provider_series_id": "cr-200",
                        "priority": 90,
                        "available_via_providers": ["crunchyroll"],
                        "context": {"mal_anime_id": 200, "english_dub_signal": "present"},
                    },
                ],
                run_id="run-split",
                generated_at="2026-07-05T20:00:00Z",
            )

            payload = build_dashboard_payload(db_path)

            self.assertEqual(payload["recommendations"]["sections"]["discovery_available_now"][0]["title"], "Available Candidate")
            self.assertEqual(payload["recommendations"]["sections"]["discovery_high_confidence"][0]["title"], "MAL Only Candidate")


    def test_live_dashboard_payload_limit_is_per_section_not_global(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "state.db"
            bootstrap_database(db_path)
            rows = [
                {"kind": "discovery_candidate", "provider": "mal", "title": f"Discovery {i}", "provider_series_id": f"mal:{i}", "priority": 200 - i, "context": {"mal_anime_id": i}}
                for i in range(5)
            ]
            rows.extend(
                [
                    {"kind": "discovery_candidate", "provider": "crunchyroll", "title": "Available Now", "provider_series_id": "cr-1", "priority": 120, "available_via_providers": ["crunchyroll"], "context": {"mal_anime_id": 100, "english_dub_signal": "present"}},
                    {"kind": "resume_backlog", "provider": "hidive", "title": "Resume Still Visible", "provider_series_id": "hi-1", "priority": 80, "reasons": ["resume"]},
                ]
            )
            insert_recommendation_snapshot_rows(db_path, rows, run_id="run-starvation", generated_at="2026-07-05T20:00:00Z")

            payload = build_dashboard_payload(db_path, limit=2)

            self.assertEqual(payload["recommendations"]["limit_scope"], "per_section")
            self.assertEqual(payload["recommendations"]["section_totals"]["discovery_available_now"], 1)
            self.assertEqual(payload["recommendations"]["section_totals"]["discovery_high_confidence"], 5)
            self.assertEqual(len(payload["recommendations"]["sections"]["discovery_high_confidence"]), 2)
            self.assertEqual(payload["recommendations"]["sections"]["discovery_available_now"][0]["title"], "Available Now")
            self.assertEqual(payload["recommendations"]["sections"]["resume_backlog"][0]["title"], "Resume Still Visible")

    def test_dashboard_serve_cli_default_limit_is_polished_dashboard_default(self) -> None:
        args = build_parser().parse_args(["dashboard-serve"])
        self.assertEqual(args.limit, DASHBOARD_DEFAULT_RECOMMENDATION_LIMIT)

    def test_live_dashboard_html_and_json_handler(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "state.db"
            bootstrap_database(db_path)
            html = render_dynamic_dashboard_html()
            self.assertIn("/api/dashboard", html)
            self.assertIn("Latest recommendation snapshot", html)
            self.assertIn("MAL rec votes", html)
            self.assertIn("section_metadata", html)

            server = ThreadingHTTPServer(("127.0.0.1", 0), make_dashboard_handler(db_path))
            thread = Thread(target=server.serve_forever, daemon=True)
            thread.start()
            try:
                root = urlopen(f"http://127.0.0.1:{server.server_port}/", timeout=5).read().decode("utf-8")
                api = json.loads(urlopen(f"http://127.0.0.1:{server.server_port}/api/dashboard", timeout=5).read().decode("utf-8"))
                override = json.loads(urlopen(f"http://127.0.0.1:{server.server_port}/api/dashboard?limit=3", timeout=5).read().decode("utf-8"))
            finally:
                server.shutdown()
                server.server_close()
                thread.join(timeout=5)
            self.assertIn("MAL-Updater live dashboard", root)
            self.assertIn("snapshot", api)
            self.assertEqual(api["recommendations"]["limit"], DASHBOARD_DEFAULT_RECOMMENDATION_LIMIT)
            self.assertEqual(override["recommendations"]["limit"], 3)
            self.assertTrue(any("No persisted recommendation snapshot" in item["message"] for item in api["indicators"]))


if __name__ == "__main__":
    unittest.main()
