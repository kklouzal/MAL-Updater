from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from threading import Thread
from urllib.request import urlopen
from http.server import ThreadingHTTPServer
import json
import io
from types import SimpleNamespace
from unittest.mock import patch

from mal_updater.db import bootstrap_database, connect, insert_recommendation_snapshot_rows, upsert_recommendation_provider_eligibility_evidence
from mal_updater.cli import build_parser, _cmd_recommend_snapshots
from mal_updater.recommendation_dashboard import DASHBOARD_DEFAULT_RECOMMENDATION_LIMIT, _current_ranked_discovery_rows_from_local_state, build_dashboard_payload, make_dashboard_handler, render_dynamic_dashboard_html, render_recommendation_dashboard, write_recommendation_dashboard
from mal_updater.recommendations import Recommendation


def _verified_provider_evidence(provider: str = "crunchyroll", provider_series_id: str = "verified-1", provider_title: str = "Verified Candidate") -> dict[str, object]:
    return {
        "provider": provider,
        "provider_series_id": provider_series_id,
        "provider_title": provider_title,
        "provider_url": f"https://example.test/{provider}/{provider_series_id}",
        "identity_match_kind": "approved_mapping",
        "match_confidence": 1.0,
        "review_status": "verified",
        "catalog_status": "present",
        "english_dub_status": "present",
        "explicit_dub_evidence_source": "provider_audio_locale",
        "fetched_at": "2026-07-18T00:00:00Z",
        "last_verified_at": "2026-07-18T00:00:00Z",
        "expires_at": "2099-01-01T00:00:00Z",
        "fresh": True,
        "expired": False,
    }


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
        self.assertIn("Ranked discovery recommendations", html)
        self.assertIn("Discovery only", html)
        self.assertIn("HIDIVE (unverified)", html)

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
                    context={"available_via_providers": ["crunchyroll"], "supporting_source_count": 2, "english_dub_signal": "present", "provider_eligibility_evidence": [_verified_provider_evidence(provider_series_id="cr-1", provider_title="Available Candidate")]},
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

        self.assertIn("Watchable now", html)
        self.assertIn("Ranked discovery recommendations", html)
        self.assertIn("Resume backlog", html)
        self.assertIn("MAL Only Candidate", html)
        self.assertLess(html.index("Watchable now"), html.index("Available Candidate"))
        self.assertLess(html.index("Ranked discovery recommendations"), html.index("MAL Only Candidate"))
        self.assertLess(html.index("Resume backlog"), html.index("Resume Me"))
        self.assertGreaterEqual(html.count('class="recommendations"'), 3)

    def test_static_dashboard_limit_is_per_section_not_global(self) -> None:
        rows = [
            Recommendation(kind="discovery_candidate", priority=200 - i, provider_series_id=f"mal:{i}", title=f"Discovery {i}", season_title=None, provider=None, reasons=["discovery"], context={"available_via_providers": []})
            for i in range(5)
        ]
        rows.extend(
            [
                Recommendation(kind="discovery_candidate", priority=120, provider_series_id="cr-available", title="Available Now", season_title=None, provider="crunchyroll", reasons=["available"], context={"available_via_providers": ["crunchyroll"], "english_dub_signal": "present", "provider_eligibility_evidence": [_verified_provider_evidence(provider_series_id="cr-available", provider_title="Available Now")]}),
                Recommendation(kind="resume_backlog", priority=80, provider_series_id="hi-resume", title="Resume Still Visible", season_title=None, provider="hidive", reasons=["resume"]),
            ]
        )

        html = render_recommendation_dashboard(rows, limit=2)

        self.assertIn("Ranked discovery recommendations (2 of 5)", html)
        self.assertIn("Discovery 0", html)
        self.assertIn("Discovery 1", html)
        self.assertNotIn("Discovery 4", html)
        self.assertIn("Watchable now (1)", html)
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

    def test_recommend_dashboard_cli_defaults_to_dashboard_limit_and_include_dormant_is_diagnostic(self) -> None:
        parser = build_parser()

        args = parser.parse_args(["recommend-dashboard", "--output", "recommendations.html"])
        self.assertEqual(DASHBOARD_DEFAULT_RECOMMENDATION_LIMIT, args.limit)

        with patch("sys.stdout", new_callable=io.StringIO) as stdout, self.assertRaises(SystemExit):
            parser.parse_args(["recommend-dashboard", "--help"])
        help_text = stdout.getvalue()
        normalized_help = " ".join(help_text.split())
        self.assertIn("Operator diagnostic", help_text)
        self.assertIn("without actionable verified provider+dub eligibility", normalized_help)
        self.assertNotIn("Backward-compatible no-op", help_text)

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
                            "provider_eligibility_evidence": [_verified_provider_evidence(provider_series_id="cr-1", provider_title="Fresh Show")],
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
            self.assertEqual(payload["recommendations"]["section_metadata"]["discovery_available_now"]["label"], "Watchable now")
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

    def test_static_dashboard_strict_discovery_row_shows_actionable_evidence(self) -> None:
        item = Recommendation(
            kind="discovery_candidate",
            priority=98,
            provider_series_id="cr-actionable",
            title="Actionable Candidate",
            season_title="Actionable Candidate",
            provider="crunchyroll",
            reasons=["fallback reason should stay concise"],
            context={
                "available_via_providers": ["crunchyroll"],
                "english_dub_signal": "present",
                "why_recommended": "Recommended because two completed favorites strongly support it.",
                "scorecard": {"total": 88.5, "components": {"consensus": 90, "affinity": 70, "availability": 100, "dub_watchable": 100}},
                "supporting_seed_details": [
                    {"mal_anime_id": 100, "title": "Seed Favorite", "num_recommendation_votes": 17, "user_score": 9, "status": "completed"}
                ],
                "provider_eligibility_evidence": [
                    {
                        "provider": "crunchyroll",
                        "provider_title": "Actionable Candidate",
                        "provider_url": "https://www.crunchyroll.com/series/actionable",
                        "identity_match_kind": "approved_mapping",
                        "review_status": "verified",
                        "catalog_status": "present",
                        "english_dub_status": "present",
                        "explicit_dub_evidence_source": "provider_audio_locale",
                        "fetched_at": "2026-07-18T00:00:00Z",
                        "last_verified_at": "2026-07-18T00:00:00Z",
                        "expires_at": "2027-01-01T00:00:00Z",
                        "fresh": True,
                        "source_evidence": {"large": "raw-json-should-not-render"},
                    }
                ],
            },
        )

        html = render_recommendation_dashboard([item])

        self.assertIn("Watchable now", html)
        self.assertIn("https://www.crunchyroll.com/series/actionable", html)
        self.assertIn("English dub evidence", html)
        self.assertIn("identity approved_mapping", html)
        self.assertIn("review verified", html)
        self.assertIn("catalog present", html)
        self.assertIn("fresh; verified 2026-07-18T00:00:00Z; expires 2027-01-01T00:00:00Z", html)
        self.assertIn("Recommended because two completed favorites strongly support it.", html)
        self.assertIn("total 88.5", html)
        self.assertIn("Seed Favorite (17 MAL votes, score 9, completed)", html)
        self.assertNotIn("raw-json-should-not-render", html)

    def test_static_dashboard_marks_provider_search_dub_evidence_unverified_not_actionable(self) -> None:
        item = Recommendation(
            kind="discovery_candidate",
            priority=95,
            provider_series_id="mal:123",
            title="Review Needed Candidate",
            season_title=None,
            provider="mal",
            reasons=["diagnostic"],
            context={
                "mal_anime_id": 123,
                "english_dub_signal": "unknown",
                "provider_eligibility_evidence": [
                    {
                        "provider": "crunchyroll",
                        "provider_title": "Review Needed Candidate",
                        "provider_url": "https://www.crunchyroll.com/series/review-needed",
                        "identity_match_kind": "provider_title_search",
                        "review_status": "review-needed",
                        "catalog_status": "present",
                        "english_dub_status": "present",
                        "explicit_dub_evidence_source": "provider_audio_locale",
                        "fetched_at": "2026-07-19T00:00:00Z",
                        "expires_at": "2026-07-26T00:00:00Z",
                        "fresh": True,
                    }
                ],
            },
        )

        html = render_recommendation_dashboard([item])

        self.assertIn("Ranked discovery recommendations", html)
        self.assertNotIn("Watchable now — verified provider+dub proof", html)
        self.assertIn("Crunchyroll (unverified)", html)
        self.assertIn("present (unverified)", html)
        self.assertIn("review review-needed", html)
        self.assertIn("catalog present", html)

    def test_live_dashboard_payload_exposes_multi_provider_url_seed_and_scorecard_details(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "state.db"
            bootstrap_database(db_path)
            insert_recommendation_snapshot_rows(
                db_path,
                [
                    {
                        "kind": "discovery_candidate",
                        "provider": "crunchyroll",
                        "title": "Multi Provider Actionable",
                        "provider_series_id": "cr-multi",
                        "priority": 99,
                        "available_via_providers": ["crunchyroll", "hidive"],
                        "context": {
                            "mal_anime_id": 777,
                            "english_dub_signal": "present",
                            "why_recommended": "Both top watched seeds point at this title.",
                            "scorecard": {"total": 91.25, "components": {"consensus": 95, "affinity": 80, "availability": 100, "dub_watchable": 100}},
                            "supporting_seed_details": [
                                {"mal_anime_id": 1, "title": "Seed One", "num_recommendation_votes": 13, "user_score": 10, "status": "completed"},
                                {"mal_anime_id": 2, "title": "Seed Two", "num_recommendation_votes": 8, "user_score": 8, "status": "watching"},
                            ],
                            "provider_eligibility_evidence": [
                                {
                                    "provider": "crunchyroll",
                                    "provider_title": "Multi Provider Actionable",
                                    "provider_url": "https://www.crunchyroll.com/series/multi",
                                    "identity_match_kind": "approved_mapping",
                                    "match_confidence": 1.0,
                                    "review_status": "verified",
                                    "catalog_status": "present",
                                    "english_dub_status": "present",
                                    "fetched_at": "2026-07-18T00:00:00Z",
                                    "last_verified_at": "2026-07-18T00:00:00Z",
                                    "expires_at": "2027-01-01T00:00:00Z",
                                    "fresh": True,
                                },
                                {
                                    "provider": "hidive",
                                    "provider_title": "Multi Provider Actionable",
                                    "provider_url": "https://www.hidive.com/season/multi",
                                    "identity_match_kind": "manual_verified",
                                    "match_confidence": 0.98,
                                    "review_status": "verified",
                                    "catalog_status": "present",
                                    "english_dub_status": "present",
                                    "fetched_at": "2026-07-18T00:00:00Z",
                                    "last_verified_at": "2026-07-18T00:00:00Z",
                                    "expires_at": "2027-01-01T00:00:00Z",
                                    "fresh": True,
                                },
                            ],
                        },
                    }
                ],
                run_id="run-actionable",
                generated_at="2026-07-18T01:00:00Z",
            )

            payload = build_dashboard_payload(db_path)

            row = payload["recommendations"]["sections"]["discovery_available_now"][0]
            self.assertTrue(row["actionable"])
            self.assertFalse(row["diagnostic_only"])
            self.assertEqual(payload["recommendations"]["mode"], "strict_actionable")
            self.assertEqual(["crunchyroll", "hidive"], row["availability"]["providers"])
            self.assertEqual("https://www.crunchyroll.com/series/multi", row["provider_badges"][0]["url"])
            self.assertEqual("https://www.hidive.com/season/multi", row["provider_badges"][1]["url"])
            self.assertIn("identity approved_mapping, manual_verified", row["verification"])
            self.assertIn("review verified", row["verification"])
            self.assertIn("fresh; verified 2026-07-18T00:00:00Z; expires 2027-01-01T00:00:00Z", row["evidence_freshness"])
            self.assertEqual("present", row["english_dub_evidence"])
            self.assertIn("Both top watched seeds", row["why_recommended"])
            self.assertIn("total 91.25", row["scorecard_summary"])
            self.assertEqual("Seed One", row["evidence"]["top_supporting_seeds"][0]["title"])
            self.assertIn("13 MAL votes", row["seed_details"])

    def test_live_dashboard_empty_coverage_state_counts_diagnostics_without_claiming_actionable_titles(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "state.db"
            bootstrap_database(db_path)
            upsert_recommendation_provider_eligibility_evidence(
                db_path,
                mal_anime_id=500,
                provider="crunchyroll",
                provider_series_id="cr-review",
                provider_title="Review Needed",
                fetched_at="2026-07-18T00:00:00Z",
                expires_at="2027-01-01T00:00:00Z",
                review_status="review-needed",
                catalog_status="unknown",
                english_dub_status="unknown",
            )
            upsert_recommendation_provider_eligibility_evidence(
                db_path,
                mal_anime_id=501,
                provider="hidive",
                provider_series_id="hi-stale",
                provider_title="Stale Evidence",
                fetched_at="2025-01-01T00:00:00Z",
                expires_at="2000-01-01T00:00:00Z",
                review_status="stale",
                catalog_status="stale",
                english_dub_status="stale",
            )
            insert_recommendation_snapshot_rows(
                db_path,
                [
                    {
                        "kind": "discovery_candidate",
                        "provider": "mal",
                        "title": "Dormant Only",
                        "provider_series_id": "mal:500",
                        "priority": 88,
                        "context": {"mal_anime_id": 500, "english_dub_signal": "unknown"},
                    }
                ],
                run_id="run-empty-strict",
                generated_at="2026-07-18T02:00:00Z",
            )

            payload = build_dashboard_payload(db_path)
            html = render_dynamic_dashboard_html()

            state = payload["recommendations"]["coverage_state"]
            self.assertEqual(0, state["strict_actionable_count"])
            self.assertEqual(1, state["dormant_candidate_count"])
            self.assertEqual(1, state["evidence_pending_review_count"])
            self.assertEqual(1, state["stale_evidence_count"])
            self.assertIn("Zero titles currently have verified current Crunchyroll/HIDIVE + English-dub evidence", state["message"])
            self.assertIn("recommend --include-dormant --limit 120", state["next_diagnostic_command"])
            self.assertEqual("diagnostic_snapshot", payload["recommendations"]["mode"])
            self.assertTrue(payload["recommendations"]["sections"]["discovery_high_confidence"][0]["diagnostic_only"])
            self.assertIn("Discovery visibility enabled", html)
            self.assertIn("No Watchable now discovery titles", html)

    def test_live_dashboard_keeps_strict_snapshot_and_fallback_discovery_visibility_separate(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "state.db"
            bootstrap_database(db_path)
            insert_recommendation_snapshot_rows(
                db_path,
                [
                    {
                        "kind": "discovery_candidate",
                        "provider": "mal",
                        "title": "Ranked Unverified Discovery",
                        "provider_series_id": "mal:700",
                        "priority": 120,
                        "context": {
                            "mal_anime_id": 700,
                            "aggregated_recommendation_votes": 33,
                            "supporting_source_count": 3,
                            "english_dub_signal": "unknown",
                        },
                    }
                ],
                run_id="run-diagnostic-discovery",
                generated_at="2026-07-19T00:00:00Z",
            )
            insert_recommendation_snapshot_rows(
                db_path,
                [
                    {"kind": "resume_backlog", "provider": "crunchyroll", "title": f"Resume {index}", "provider_series_id": f"cr-resume-{index}", "priority": 80 - index}
                    for index in range(18)
                ],
                run_id="run-strict-resume-only",
                generated_at="2026-07-19T05:00:00Z",
            )

            payload = build_dashboard_payload(db_path, limit=120)

            self.assertEqual("run-strict-resume-only", payload["snapshot"]["run_id"])
            self.assertEqual(18, payload["recommendations"]["section_totals"]["resume_backlog"])
            self.assertEqual(1, payload["recommendations"]["section_totals"]["discovery_high_confidence"])
            self.assertEqual(0, payload["recommendations"]["coverage_state"]["strict_actionable_count"])
            row = payload["recommendations"]["sections"]["discovery_high_confidence"][0]
            self.assertFalse(row["actionable"])
            self.assertTrue(row["diagnostic_only"])
            self.assertEqual("unknown/unverified", row["english_dub_evidence"])
            self.assertIn("unverified", row["provider_evidence"])
            self.assertEqual("run-diagnostic-discovery", payload["recommendations"]["diagnostic_source_snapshot"]["run_id"])

    def test_current_diagnostic_discovery_rows_keep_genres_as_api_array(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / ".MAL-Updater" / "data" / "state.db"
            db_path.parent.mkdir(parents=True)
            item = Recommendation(
                kind="discovery_candidate",
                priority=101,
                provider_series_id="mal:900",
                title="Genre Shape Candidate",
                season_title=None,
                provider="mal",
                context={"genres": ["Action", "Comedy"], "english_dub_signal": "unknown"},
            )

            with (
                patch("mal_updater.recommendation_dashboard.load_config", return_value=SimpleNamespace(db_path=db_path)),
                patch("mal_updater.recommendation_dashboard.build_recommendations", return_value=[item]),
            ):
                rows, source = _current_ranked_discovery_rows_from_local_state(db_path, limit=120)

            self.assertEqual("local-diagnostic-current", source["run_id"])
            self.assertEqual(["Action", "Comedy"], rows[0]["genres"])
            self.assertEqual("unknown/unverified", rows[0]["english_dub_evidence"])

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
                        "context": {"mal_anime_id": 200, "english_dub_signal": "present", "provider_eligibility_evidence": [_verified_provider_evidence(provider_series_id="cr-200", provider_title="Available Candidate")]},
                    },
                ],
                run_id="run-split",
                generated_at="2026-07-05T20:00:00Z",
            )

            payload = build_dashboard_payload(db_path)

            self.assertEqual(payload["recommendations"]["sections"]["discovery_available_now"][0]["title"], "Available Candidate")
            self.assertEqual(payload["recommendations"]["sections"]["discovery_high_confidence"][0]["title"], "MAL Only Candidate")

    def test_live_dashboard_shows_provider_availability_when_english_dub_unknown(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "state.db"
            bootstrap_database(db_path)
            insert_recommendation_snapshot_rows(
                db_path,
                [
                    {
                        "kind": "discovery_candidate",
                        "provider": "hidive",
                        "title": "Provider Visible Dub Unknown",
                        "provider_series_id": "hi-unknown",
                        "priority": 90,
                        "available_via_providers": ["hidive"],
                        "context": {"mal_anime_id": 200, "english_dub_signal": "unknown"},
                    },
                ],
                run_id="run-unknown-dub",
                generated_at="2026-07-05T20:00:00Z",
            )

            payload = build_dashboard_payload(db_path)

            row = payload["recommendations"]["sections"]["discovery_high_confidence"][0]
            self.assertEqual("Provider Visible Dub Unknown", row["title"])
            self.assertEqual(["hidive"], row["availability_providers"])
            self.assertEqual("hidive", row["evidence"]["availability_provider_label"])
            self.assertEqual("unknown", row["dub_signal"])
            self.assertEqual("unknown", row["evidence"]["dub_signal"])
            self.assertEqual([], payload["recommendations"]["sections"].get("discovery_available_now", []))


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
                    {"kind": "discovery_candidate", "provider": "crunchyroll", "title": "Available Now", "provider_series_id": "cr-1", "priority": 120, "available_via_providers": ["crunchyroll"], "context": {"mal_anime_id": 100, "english_dub_signal": "present", "provider_eligibility_evidence": [_verified_provider_evidence(provider_series_id="cr-1", provider_title="Available Now")]}},
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

    def test_dashboard_surfaces_availability_match_dub_and_review_columns(self) -> None:
        item = Recommendation(
            kind="discovery_candidate",
            priority=91,
            provider_series_id="cr-review",
            title="Review Candidate",
            season_title="Review Candidate",
            provider="crunchyroll",
            reasons=["available but needs mapping review"],
            context={
                "available_via_providers": ["crunchyroll"],
                "availability_confidence": "title_alias",
                "availability_match_kinds": ["title_alias"],
                "available_provider_series": [
                    {
                        "provider": "crunchyroll",
                        "provider_series_id": "cr-review",
                        "availability_match_kind": "title_alias",
                        "mapping_confidence": 0.62,
                        "mapping_source": "provider_search",
                    }
                ],
                "english_dub_signal": "none",
                "review_needed": True,
                "supporting_source_count": 1,
            },
        )

        html = render_recommendation_dashboard([item])

        for text in ("Dub status", "Availability match", "Availability confidence", "Match source", "Mapping confidence", "Review"):
            self.assertIn(text, html)
        for text in ("title_alias", "provider_search", "0.62", "none", "yes"):
            self.assertIn(text, html)

    def test_dashboard_api_payload_exposes_availability_match_dub_and_review_details(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "state.db"
            bootstrap_database(db_path)
            insert_recommendation_snapshot_rows(
                db_path,
                [
                    {
                        "kind": "discovery_candidate",
                        "provider": "crunchyroll",
                        "title": "Mapped Candidate",
                        "provider_series_id": "cr-available",
                        "priority": 101,
                        "available_via_providers": ["crunchyroll"],
                        "context": {
                            "mal_anime_id": 5001,
                            "english_dub_signal": "present",
                            "availability_confidence": "mapped",
                            "availability_match_kinds": ["mapped_mal"],
                            "available_provider_series": [
                                {
                                    "provider": "crunchyroll",
                                    "provider_series_id": "cr-available",
                                    "availability_match_kind": "mapped_mal",
                                    "mapping_confidence": 1.0,
                                    "mapping_source": "approved_mapping",
                                }
                            ],
                            "availability_review_needed": True,
                        },
                    },
                    {
                        "kind": "discovery_candidate",
                        "provider": "hidive",
                        "title": "Unknown Dub Candidate",
                        "provider_series_id": "hi-unknown",
                        "priority": 90,
                        "available_via_providers": ["hidive"],
                        "context": {"english_dub_signal": "unknown", "availability_match_kinds": ["title_alias"]},
                    },
                    {
                        "kind": "discovery_candidate",
                        "provider": "hidive",
                        "title": "No Dub Candidate",
                        "provider_series_id": "hi-none",
                        "priority": 89,
                        "available_via_providers": ["hidive"],
                        "context": {"english_dub_signal": "none", "availability_match_kinds": ["title_alias"]},
                    },
                ],
                run_id="run-availability",
                generated_at="2026-07-11T02:00:00Z",
            )

            payload = build_dashboard_payload(db_path, limit=10)

            rows = {row["title"]: row for section in payload["recommendations"]["sections"].values() for row in section}
            mapped = rows["Mapped Candidate"]
            self.assertEqual(["crunchyroll"], mapped["availability"]["providers"])
            self.assertEqual(["mapped_mal"], mapped["availability"]["match_kinds"])
            self.assertEqual(["approved_mapping"], mapped["availability"]["match_sources"])
            self.assertEqual([1.0], mapped["availability"]["match_confidences"])
            self.assertEqual("present", mapped["availability"]["dub_status"])
            self.assertTrue(mapped["availability"]["review_needed"])
            self.assertEqual("unknown", rows["Unknown Dub Candidate"]["availability"]["dub_status"])
            self.assertEqual("none", rows["No Dub Candidate"]["availability"]["dub_status"])

    def test_recommend_snapshots_cli_payload_includes_operator_availability_summary(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            project_root = Path(temp_dir)
            runtime_root = project_root / ".MAL-Updater"
            db_path = runtime_root / "data" / "mal_updater.sqlite3"
            db_path.parent.mkdir(parents=True, exist_ok=True)
            bootstrap_database(db_path)
            insert_recommendation_snapshot_rows(
                db_path,
                [
                    {
                        "kind": "discovery_candidate",
                        "provider": "crunchyroll",
                        "title": "CLI Candidate",
                        "provider_series_id": "cr-cli",
                        "priority": 99,
                        "available_via_providers": ["crunchyroll"],
                        "context": {
                            "english_dub_signal": "unknown",
                            "availability_match_kinds": ["title_alias"],
                            "available_provider_series": [
                                {
                                    "provider": "crunchyroll",
                                    "provider_series_id": "cr-cli",
                                    "availability_match_kind": "title_alias",
                                    "mapping_confidence": 0.7,
                                    "mapping_source": "provider_search",
                                }
                            ],
                            "review_needed": True,
                        },
                    }
                ],
                run_id="run-cli",
                generated_at="2026-07-11T02:05:00Z",
            )

            with patch("sys.stdout", new_callable=io.StringIO) as stdout:
                exit_code = _cmd_recommend_snapshots(project_root, limit=10, output_format="json")

            self.assertEqual(0, exit_code)
            payload = json.loads(stdout.getvalue())
            self.assertEqual(["crunchyroll"], payload[0]["availability"]["providers"])
            self.assertEqual(["title_alias"], payload[0]["availability"]["match_kinds"])
            self.assertEqual(["provider_search"], payload[0]["availability"]["match_sources"])
            self.assertEqual([0.7], payload[0]["availability"]["match_confidences"])
            self.assertEqual("unknown", payload[0]["availability"]["dub_status"])
            self.assertTrue(payload[0]["availability"]["review_needed"])

    def test_dashboard_serve_cli_default_limit_is_polished_dashboard_default(self) -> None:
        args = build_parser().parse_args(["dashboard-serve"])
        self.assertEqual(args.limit, DASHBOARD_DEFAULT_RECOMMENDATION_LIMIT)

    def test_live_dashboard_html_and_json_handler(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "state.db"
            bootstrap_database(db_path)
            html = render_dynamic_dashboard_html()
            self.assertIn("/api/dashboard", html)
            self.assertIn("Recommendations", html)
            self.assertIn("Top watched seeds", html)
            self.assertIn("MAL vote", html)
            self.assertIn("section_metadata", html)
            self.assertIn("Array.isArray(r.genres)", html)
            self.assertNotIn("(r.genres || []).join(', ')", html)

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
