from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from threading import Thread
from urllib.request import urlopen
from http.server import ThreadingHTTPServer
import json
import io
import os
import sqlite3
from types import SimpleNamespace
from unittest.mock import patch

from mal_updater.config import load_config
from mal_updater.db import bootstrap_database, connect, insert_recommendation_snapshot_rows, replace_mal_recommendation_edges, upsert_mal_anime_metadata, upsert_recommendation_provider_eligibility_evidence
from mal_updater.cli import build_parser, _cmd_recommend_snapshots
from mal_updater.recommendation_dashboard import DASHBOARD_DEFAULT_RECOMMENDATION_LIMIT, DASHBOARD_MAX_RECOMMENDATION_LIMIT, DASHBOARD_MIN_RECOMMENDATION_LIMIT, GENRE_AFFINITY_DEFAULT_LIMIT, aggregate_genre_affinity, _current_ranked_discovery_rows_from_local_state, _eligibility_coverage_counts, _is_displayable_discovery, _strict_actionability_failure_reasons, build_dashboard_payload, make_dashboard_handler, render_dynamic_dashboard_html, render_dynamic_debug_html, render_recommendation_dashboard, write_recommendation_dashboard
from mal_updater.recommendations import Recommendation, build_recommendations


def _verified_provider_evidence(
    provider: str = "crunchyroll",
    provider_series_id: str = "verified-1",
    provider_title: str = "Verified Candidate",
    *,
    identity_match_kind: str = "approved_mapping",
    audio_locales: list[str] | None = None,
    last_verified_at: str | None = "2026-07-18T00:00:00Z",
    expires_at: str = "2099-01-01T00:00:00Z",
) -> dict[str, object]:
    return {
        "provider": provider,
        "provider_series_id": provider_series_id,
        "provider_title": provider_title,
        "provider_url": f"https://example.test/{provider}/{provider_series_id}",
        "identity_match_kind": identity_match_kind,
        "match_confidence": 1.0,
        "review_status": "verified",
        "catalog_status": "present",
        "english_dub_status": "present",
        "explicit_dub_evidence_source": "provider_audio_locale",
        "audio_locales": list(audio_locales if audio_locales is not None else ["en-US", "ja-JP"]),
        "fetched_at": "2026-07-18T00:00:00Z",
        "last_verified_at": last_verified_at,
        "expires_at": expires_at,
        "fresh": expires_at > "2026-07-24T00:00:00Z",
        "expired": expires_at <= "2026-07-24T00:00:00Z",
    }


_SQLITE_WRITE_ACTIONS = {
    sqlite3.SQLITE_INSERT,
    sqlite3.SQLITE_UPDATE,
    sqlite3.SQLITE_DELETE,
    sqlite3.SQLITE_CREATE_TABLE,
    sqlite3.SQLITE_CREATE_INDEX,
    sqlite3.SQLITE_CREATE_TRIGGER,
    sqlite3.SQLITE_CREATE_VIEW,
    sqlite3.SQLITE_CREATE_TEMP_TABLE,
    sqlite3.SQLITE_CREATE_TEMP_INDEX,
    sqlite3.SQLITE_CREATE_TEMP_TRIGGER,
    sqlite3.SQLITE_CREATE_TEMP_VIEW,
    sqlite3.SQLITE_DROP_TABLE,
    sqlite3.SQLITE_DROP_INDEX,
    sqlite3.SQLITE_DROP_TRIGGER,
    sqlite3.SQLITE_DROP_VIEW,
    sqlite3.SQLITE_DROP_TEMP_TABLE,
    sqlite3.SQLITE_DROP_TEMP_INDEX,
    sqlite3.SQLITE_DROP_TEMP_TRIGGER,
    sqlite3.SQLITE_DROP_TEMP_VIEW,
    sqlite3.SQLITE_ALTER_TABLE,
}


def _query_only_connect_trap(write_actions: list[tuple[int, str | None, str | None]]):
    def trapped_connect(db_path: Path):
        conn = connect(db_path)
        conn.execute("PRAGMA query_only = ON")

        def authorizer(action: int, arg1: str | None, arg2: str | None, db_name: str | None, trigger: str | None) -> int:
            if action in _SQLITE_WRITE_ACTIONS:
                write_actions.append((action, arg1, arg2))
                return sqlite3.SQLITE_DENY
            return sqlite3.SQLITE_OK

        conn.set_authorizer(authorizer)
        return conn

    return trapped_connect


class RecommendationDashboardTests(unittest.TestCase):

    def test_genre_affinity_aggregates_limits_and_normalizes_watched_titles(self) -> None:
        rows = [
            {"list_status": "completed", "metadata_raw_json": {"genres": [{"name": "Action"}, {"name": "Comedy"}]}},
            {"list_status": "watching", "metadata_raw_json": {"genres": [{"name": "Action"}, {"name": "Drama"}]}},
            {"list_status": "completed", "metadata_raw_json": {"genres": [{"name": "Drama"}, {"name": "Drama"}]}},
            {"list_status": "plan_to_watch", "metadata_raw_json": {"genres": [{"name": "Action"}]}},
            {"list_status": "completed", "metadata_raw_json": {"genres": []}},
        ]

        model = aggregate_genre_affinity(rows, limit=2)

        self.assertEqual(2, model["limit"])
        self.assertEqual(3, model["available_genre_count"])
        self.assertEqual(3, model["represented_title_count"])
        self.assertEqual(
            [
                {"genre": "Action", "title_count": 2, "weighted_count": 1.5, "normalized": 100.0},
                {"genre": "Drama", "title_count": 2, "weighted_count": 1.5, "normalized": 100.0},
            ],
            model["axes"],
        )

    def test_genre_affinity_empty_sparse_payload_is_explicit(self) -> None:
        model = aggregate_genre_affinity([
            {"list_status": "dropped", "metadata_raw_json": {"genres": [{"name": "Action"}]}},
            {"list_status": "completed", "metadata_raw_json": "not-json"},
        ])

        self.assertEqual([], model["axes"])
        self.assertEqual(GENRE_AFFINITY_DEFAULT_LIMIT, model["limit"])
        self.assertEqual(0, model["available_genre_count"])
        self.assertEqual(0, model["represented_title_count"])

    def test_live_dashboard_payload_includes_genre_affinity_from_watched_metadata(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "state.db"
            bootstrap_database(db_path)
            with connect(db_path) as conn:
                for mal_id, status, genres in (
                    (1, "completed", ["Action", "Adventure"]),
                    (2, "watching", ["Action", "Comedy"]),
                    (3, "on_hold", ["Adventure"]),
                ):
                    conn.execute(
                        """
                        INSERT INTO mal_user_anime_list_cache (
                            mal_anime_id,title,list_status,user_score,num_episodes_watched,node_json,list_status_json,raw_json,
                            refresh_run_id,refresh_generation,fetched_at,last_seen_at
                        ) VALUES (?, ?, ?, NULL, NULL, '{}', '{}', '{}', 'test', 1, '2026-08-15T00:00:00Z', '2026-08-15T00:00:00Z')
                        """,
                        (mal_id, f"Title {mal_id}", status),
                    )
                    conn.execute(
                        """
                        INSERT INTO mal_anime_metadata (mal_anime_id,title,raw_json)
                        VALUES (?, ?, ?)
                        """,
                        (mal_id, f"Title {mal_id}", json.dumps({"genres": [{"name": genre} for genre in genres]})),
                    )

            payload = build_dashboard_payload(db_path)

        self.assertEqual("genre_affinity", next(key for key in payload if key == "genre_affinity"))
        self.assertEqual(
            [
                {"genre": "Action", "title_count": 2, "weighted_count": 1.5, "normalized": 100.0},
                {"genre": "Adventure", "title_count": 1, "weighted_count": 1.0, "normalized": 66.7},
                {"genre": "Comedy", "title_count": 1, "weighted_count": 0.5, "normalized": 33.3},
            ],
            payload["genre_affinity"]["axes"],
        )

    def test_live_dashboard_renders_genre_affinity_above_recommendations_with_accessible_detail(self) -> None:
        html = render_dynamic_dashboard_html()

        self.assertIn('function genreAffinitySection(model)', html)
        self.assertIn('id="genre-affinity"', html)
        self.assertIn('role="img" aria-labelledby="genre-affinity-svg-title genre-affinity-svg-desc"', html)
        self.assertIn('Exact values', html)
        self.assertIn('Not enough completed/currently-watching titles with genre metadata', html)
        self.assertLess(html.index('${genreAffinitySection(data.genre_affinity)}'), html.index('<section><h2>Recommendations</h2>'))

    def test_strict_actionability_shared_semantics_cover_identity_locales_failures_and_counts(self) -> None:
        accepted_cases = [
            ("approved_mapping", ["en"]),
            ("manual_verified", ["EN_US"]),
            ("user_exact", ["en-GB"]),
            ("auto_exact", ["ja-JP", "en-US"]),
            ("provider_title_search_exact", ["en-us"]),
            ("provider_franchise_shell_child_match", ["EN-gb", "ja-JP"]),
        ]
        for identity_kind, audio_locales in accepted_cases:
            with self.subTest(identity_kind=identity_kind, audio_locales=audio_locales):
                row = {
                    "kind": "discovery_candidate",
                    "context": {
                        "provider_eligibility_evidence": [
                            _verified_provider_evidence(identity_match_kind=identity_kind, audio_locales=audio_locales)
                        ]
                    },
                }
                self.assertTrue(_is_displayable_discovery(row))
                self.assertEqual([], _strict_actionability_failure_reasons(row))

        failure_cases = {
            "legacy_identity": (
                {"identity_match_kind": "provider_title_search"},
                "Crunchyroll/HIDIVE identity unverified",
            ),
            "missing_english_audio_locale": (
                {"audio_locales": ["ja-JP"]},
                "English audio-locales missing/unverified",
            ),
            "missing_verified_timestamp": (
                {"last_verified_at": None},
                "current provider verification stale or missing",
            ),
            "expired_verified_timestamp": (
                {"expires_at": "2000-01-01T00:00:00Z", "last_verified_at": "2000-01-01T00:00:00Z", "fresh": False, "expired": True},
                "current provider verification stale or missing",
            ),
            "review_not_verified": (
                {"review_status": "review-needed"},
                "provider identity review unverified",
            ),
            "catalog_not_present": (
                {"catalog_status": "unknown"},
                "current provider catalog presence unverified",
            ),
            "dub_not_present": (
                {"english_dub_status": "unknown"},
                "English-dub evidence unknown",
            ),
        }
        for label, (overrides, expected_reason) in failure_cases.items():
            with self.subTest(label=label):
                evidence = _verified_provider_evidence()
                evidence.update(overrides)
                row = {"kind": "discovery_candidate", "context": {"provider_eligibility_evidence": [evidence]}}
                self.assertFalse(_is_displayable_discovery(row))
                self.assertIn(expected_reason, _strict_actionability_failure_reasons(row))

        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "state.db"
            bootstrap_database(db_path)
            for index, (identity_kind, audio_locales) in enumerate(accepted_cases, start=1):
                upsert_recommendation_provider_eligibility_evidence(
                    db_path,
                    mal_anime_id=1000 + index,
                    provider="crunchyroll" if index % 2 else "hidive",
                    provider_series_id=f"strict-{index}",
                    provider_title=f"Strict {index}",
                    identity_match_kind=identity_kind,
                    review_status="verified",
                    catalog_status="present",
                    english_dub_status="present",
                    audio_locales=audio_locales,
                    fetched_at="2026-07-18T00:00:00Z",
                    expires_at="2099-01-01T00:00:00Z",
                    last_verified_at="2026-07-18T00:00:00Z",
                )
            upsert_recommendation_provider_eligibility_evidence(
                db_path,
                mal_anime_id=2000,
                provider="crunchyroll",
                provider_series_id="ja-only",
                provider_title="Japanese Audio Only",
                identity_match_kind="approved_mapping",
                review_status="verified",
                catalog_status="present",
                english_dub_status="present",
                audio_locales=["ja-JP"],
                fetched_at="2026-07-18T00:00:00Z",
                expires_at="2099-01-01T00:00:00Z",
                last_verified_at="2026-07-18T00:00:00Z",
            )
            upsert_recommendation_provider_eligibility_evidence(
                db_path,
                mal_anime_id=2001,
                provider="hidive",
                provider_series_id="review-needed",
                provider_title="Review Needed",
                identity_match_kind="manual_verified",
                review_status="review-needed",
                catalog_status="unknown",
                english_dub_status="unknown",
                audio_locales=[],
                fetched_at="2026-07-18T00:00:00Z",
                expires_at="2099-01-01T00:00:00Z",
                last_verified_at=None,
            )
            counts = _eligibility_coverage_counts(db_path)
            self.assertEqual(len(accepted_cases), counts["strict_current"])
            self.assertEqual(1, counts["pending_review"])
            self.assertEqual(0, counts["stale"])

    def test_render_combines_provider_proof_with_title_and_hides_diagnostic_only_columns(self) -> None:
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

        for label in ("Title", "Score", "Source count", "Total votes", "MAL mean", "MAL popularity", "Genres"):
            self.assertIn(label, html)
        self.assertNotIn("English title / provider proof", html)
        for hidden_label in ("English dub", "Provider progress", "MAL watch status"):
            self.assertNotIn(f">{hidden_label}<", html)
        self.assertNotIn('data-key="provider_evidence"', html)
        self.assertNotIn('data-key="verification"', html)
        self.assertNotIn('data-key="evidence_freshness"', html)
        self.assertNotIn(">Identity/review/catalog<", html)
        self.assertNotIn(">Evidence freshness/expiry<", html)
        self.assertIn('data-key="score" data-type="number" aria-sort="none"', html)
        self.assertIn('data-key="title" data-sort-value="The Great Show (A &lt;Great&gt; Show (English Dub))"', html)
        self.assertIn('<span class="title-text">The Great Show', html)
        self.assertRegex(
            html,
            r'<span class="title-text">The Great Show.*?</span><div class="title-providers" aria-label="Provider proof">.*?Crunchyroll.*?<br>.*?HIDIVE',
        )
        self.assertIn("Action, Comedy", html)
        self.assertNotIn("recommended by 2 watched/mapped seed title", html)
        self.assertIn("34", html)
        self.assertIn("321", html)
        self.assertNotIn(">2/12<", html)
        self.assertNotIn("watching (2/12)", html)
        self.assertIn("class=\"table-scroll\"", html)
        self.assertIn("aria-sort=\"none\"", html)
        self.assertIn("cell?.dataset.sortValue ?? cell?.textContent.trim()", html)
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
            self.assertIn("No recommendations in this section.", html)
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

        with patch("sys.stdout", new_callable=io.StringIO) as stdout, self.assertRaises(SystemExit):
            parser.parse_args(["dashboard-serve", "--help"])
        serve_help_text = stdout.getvalue()
        self.assertIn(f"default: {DASHBOARD_DEFAULT_RECOMMENDATION_LIMIT}", serve_help_text)
        self.assertNotIn("default: 16", serve_help_text)

    def test_parser_excludes_dead_sync_placeholder_but_keeps_real_sync_commands(self) -> None:
        parser = build_parser()
        command_action = next(action for action in parser._actions if getattr(action, "choices", None))
        commands = set(command_action.choices)

        self.assertIn("dry-run-sync", commands)
        self.assertIn("apply-sync", commands)
        self.assertIn("dashboard-serve", commands)
        self.assertNotIn("sync", commands)
        with patch("sys.stderr", new_callable=io.StringIO), self.assertRaises(SystemExit):
            parser.parse_args(["sync"])

    def test_build_dashboard_payload_never_bootstraps_schema(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "state.db"
            bootstrap_database(db_path)

            with patch("mal_updater.recommendation_dashboard.bootstrap_database", side_effect=AssertionError("payload must not bootstrap")):
                payload = build_dashboard_payload(db_path)

            self.assertEqual(
                {"generated_at", "snapshot", "genre_affinity", "recommendations", "coverage", "operational", "recent_sync_runs", "indicators"},
                set(payload),
            )
            self.assertTrue(any("No persisted recommendation snapshot" in item["message"] for item in payload["indicators"]))

    def test_build_dashboard_payload_uses_supplied_container_config(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            runtime_root = Path(temp_dir) / "data"
            with patch.dict(
                os.environ,
                {
                    "MAL_UPDATER_RUNTIME_ROOT": str(runtime_root),
                    "MAL_UPDATER_SETTINGS_PATH": str(runtime_root / "config" / "settings.toml"),
                },
                clear=False,
            ):
                config = load_config(Path(temp_dir))
                config.db_path.parent.mkdir(parents=True, exist_ok=True)
                bootstrap_database(config.db_path)

                payload = build_dashboard_payload(config.db_path, config=config)

            diagnostics = payload["operational"]["provider_enrichment"]
            self.assertEqual(["no_configured_credentialed_providers"], diagnostics["reason_codes"])
            self.assertNotIn("dashboard_config_unavailable", diagnostics["reason_codes"])

    def test_build_dashboard_payload_rejects_mismatched_supplied_config_fail_closed(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            project_root = Path(temp_dir)
            config = load_config(project_root)
            db_path = project_root / "standalone.db"
            bootstrap_database(db_path)

            payload = build_dashboard_payload(db_path, config=config)

            diagnostics = payload["operational"]["provider_enrichment"]
            self.assertEqual("unknown", diagnostics["status"])
            self.assertEqual(["dashboard_config_unavailable"], diagnostics["reason_codes"])

    def test_build_dashboard_payload_missing_database_is_read_only_unavailable(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / ".MAL-Updater" / "data" / "state.db"

            payload = build_dashboard_payload(db_path)

            self.assertFalse(db_path.parent.exists())
            self.assertFalse(db_path.exists())
            self.assertEqual(
                {"generated_at", "snapshot", "genre_affinity", "recommendations", "coverage", "operational", "recent_sync_runs", "indicators"},
                set(payload),
            )
            self.assertEqual(
                {
                    "mode",
                    "strict_default",
                    "items",
                    "sections",
                    "section_totals",
                    "section_metadata",
                    "coverage_state",
                    "diagnostic_source_snapshot",
                    "limit",
                    "limit_scope",
                },
                set(payload["recommendations"]),
            )
            self.assertEqual([], payload["recommendations"]["items"])
            self.assertIn("Dashboard data unavailable", payload["recommendations"]["coverage_state"]["message"])
            self.assertTrue(any("payload reads do not bootstrap schema" in item["message"] for item in payload["indicators"]))

    def test_build_dashboard_payload_uninitialized_schema_is_explicit_without_migration(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "state.db"
            db_path.touch()

            payload = build_dashboard_payload(db_path)

            self.assertTrue(db_path.exists())
            self.assertEqual(b"", db_path.read_bytes())
            self.assertEqual([], payload["recommendations"]["items"])
            self.assertTrue(any("schema is not initialized" in item["message"] for item in payload["indicators"]))

    def test_build_dashboard_payload_corrupt_database_error_is_not_hidden(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "state.db"
            db_path.write_bytes(b"not a sqlite database")

            with self.assertRaises(sqlite3.DatabaseError):
                build_dashboard_payload(db_path)

    def test_live_dashboard_payload_reads_current_database_state(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "state.db"
            bootstrap_database(db_path)
            with connect(db_path) as conn:
                conn.execute("INSERT INTO provider_series (provider, provider_series_id, title, account_observed_at) VALUES ('crunchyroll', 'cr-1', 'Show', CURRENT_TIMESTAMP)")
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

            self.assertEqual(
                {"generated_at", "snapshot", "genre_affinity", "recommendations", "coverage", "operational", "recent_sync_runs", "indicators"},
                set(payload),
            )
            self.assertEqual(
                {
                    "mode",
                    "strict_default",
                    "items",
                    "sections",
                    "section_totals",
                    "section_metadata",
                    "coverage_state",
                    "diagnostic_source_snapshot",
                    "limit",
                    "limit_scope",
                },
                set(payload["recommendations"]),
            )
            self.assertEqual("per_section", payload["recommendations"]["limit_scope"])
            self.assertEqual("strict_actionable", payload["recommendations"]["mode"])
            self.assertEqual(payload["snapshot"]["run_id"], "run-1")
            self.assertEqual(payload["snapshot"]["item_count"], 2)
            row = payload["recommendations"]["sections"]["discovery_available_now"][0]
            self.assertTrue(
                {
                    "id",
                    "run_id",
                    "generated_at",
                    "kind",
                    "provider",
                    "title",
                    "display_title",
                    "provider_series_id",
                    "mal_anime_id",
                    "score",
                    "priority",
                    "reasons",
                    "context",
                    "evidence",
                    "availability",
                    "actionable",
                    "diagnostic_only",
                    "visibility_label",
                    "strict_actionability",
                }.issubset(row)
            )
            self.assertEqual("Fresh English Show", row["english_title"])
            self.assertEqual(["Drama", "Sci-Fi"], row["genres"])
            self.assertEqual([], row["reasons"])
            self.assertEqual(
                {
                    "providers",
                    "match_kinds",
                    "match_sources",
                    "match_confidences",
                    "confidence",
                    "confidence_label",
                    "dub_status",
                    "review_needed",
                    "provider_badges",
                    "verification",
                    "freshness",
                },
                set(row["availability"]),
            )
            self.assertEqual(payload["recommendations"]["section_metadata"]["discovery_available_now"]["label"], "Watchable now")
            self.assertEqual(payload["recommendations"]["section_metadata"]["discovery_available_now"]["title_label"], "English title")
            self.assertIn("English dub evidence", payload["recommendations"]["section_metadata"]["discovery_available_now"]["description"])
            self.assertEqual(payload["recommendations"]["section_metadata"]["resume_backlog"]["label"], "Resume backlog")
            evidence = row["evidence"]
            self.assertTrue(
                {
                    "mal_recommendation_votes",
                    "seed_count",
                    "seed_ids",
                    "seed_titles",
                    "compact_seeds",
                    "availability_providers",
                    "availability_match_kinds",
                    "availability_match_sources",
                    "availability_match_confidences",
                    "availability_confidence",
                    "availability_confidence_label",
                    "provider_eligibility_evidence",
                    "verification_label",
                    "evidence_freshness_label",
                    "dub_status",
                    "english_dub_present",
                    "review_needed",
                    "mal_watch_status",
                    "why_recommended",
                }.issubset(evidence)
            )
            self.assertEqual(evidence["mal_recommendation_votes"], 12)
            self.assertEqual(evidence["seed_count"], 2)
            self.assertEqual(evidence["compact_seeds"], "Seed A, Seed B")
            self.assertEqual(evidence["availability_provider_label"], "crunchyroll")
            self.assertEqual(evidence["dub_signal"], "present")
            self.assertEqual(evidence["mal_watch_status"], "plan_to_watch")
            self.assertEqual(row, payload["recommendations"]["items"][0])
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
                        "audio_locales": ["en-US", "ja-JP"],
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
        self.assertIn('class="provider-link"', html)
        self.assertIn('aria-label="Open Crunchyroll provider proof"', html)
        self.assertNotIn("Actionable Candidate</span></a> Actionable Candidate", html)
        self.assertEqual(1, html.count(">Actionable Candidate</span>"))
        self.assertRegex(
            html,
            r'<span class="title-text">Actionable Candidate</span><div class="title-providers" aria-label="Provider proof"><a class="provider-link"',
        )
        self.assertNotIn(">English dub evidence<", html)
        self.assertNotIn("identity approved_mapping", html)
        self.assertNotIn("review verified", html)
        self.assertNotIn("catalog present", html)
        self.assertNotIn("fresh; verified 2026-07-18T00:00:00Z; expires 2027-01-01T00:00:00Z", html)
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
                        "audio_locales": ["en-US", "ja-JP"],
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
        self.assertNotIn("present (unverified)", html)
        self.assertNotIn("review review-needed", html)
        self.assertNotIn("catalog present", html)

    def test_dashboard_strict_actionability_matches_shared_identity_locale_and_freshness_semantics(self) -> None:
        accepted = [
            ("approved_mapping", ["en-US"]),
            ("manual_verified", ["EN_gb"]),
            ("user_exact", ["en"]),
            ("auto_exact", ["ja-JP", "en_CA"]),
            ("provider_title_search_exact", ["en-AU"]),
            ("provider_franchise_shell_child_match", ["en-GB", "ja-JP"]),
        ]
        for identity_kind, audio_locales in accepted:
            row = {
                "kind": "discovery_candidate",
                "context": {"provider_eligibility_evidence": [_verified_provider_evidence(identity_match_kind=identity_kind, audio_locales=audio_locales)]},
            }
            self.assertTrue(_is_displayable_discovery(row), identity_kind)
            self.assertEqual([], _strict_actionability_failure_reasons(row))

        failure_cases = [
            ("missing last verification", _verified_provider_evidence(last_verified_at=None), "current provider verification stale or missing"),
            ("expired verification", _verified_provider_evidence(expires_at="2000-01-01T00:00:00Z"), "current provider verification stale or missing"),
            ("review not verified", {**_verified_provider_evidence(), "review_status": "review-needed"}, "provider identity review unverified"),
            ("catalog absent", {**_verified_provider_evidence(), "catalog_status": "absent"}, "current provider catalog presence unverified"),
            ("dub absent", {**_verified_provider_evidence(), "english_dub_status": "absent"}, "English-dub evidence unknown"),
            ("non-English audio", _verified_provider_evidence(audio_locales=["ja-JP"]), "English audio-locales missing/unverified"),
        ]
        for label, evidence, expected_reason in failure_cases:
            row = {"kind": "discovery_candidate", "context": {"provider_eligibility_evidence": [evidence]}}
            self.assertFalse(_is_displayable_discovery(row), label)
            self.assertIn(expected_reason, _strict_actionability_failure_reasons(row), label)

    def test_live_dashboard_coverage_counts_use_same_strict_display_semantics(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "state.db"
            bootstrap_database(db_path)

            def store_evidence(mal_id: int, provider_series_id: str, title: str, *, identity_match_kind: str, audio_locales: list[str], review_status: str = "verified", catalog_status: str = "present", english_dub_status: str = "present", expires_at: str = "2027-01-01T00:00:00Z", last_verified_at: str | None = "2026-07-18T00:00:00Z") -> dict[str, object]:
                upsert_recommendation_provider_eligibility_evidence(
                    db_path,
                    mal_anime_id=mal_id,
                    provider="crunchyroll",
                    provider_series_id=provider_series_id,
                    provider_title=title,
                    provider_url=f"https://example.test/{provider_series_id}",
                    identity_match_kind=identity_match_kind,
                    match_confidence=0.99,
                    review_status=review_status,
                    catalog_status=catalog_status,
                    english_dub_status=english_dub_status,
                    explicit_dub_evidence_source="provider_audio_locale" if audio_locales else None,
                    audio_locales=audio_locales,
                    source_evidence={"test": "dashboard_strict_parity"},
                    fetched_at="2026-07-18T00:00:00Z",
                    expires_at=expires_at,
                    last_verified_at=last_verified_at,
                )
                fresh = expires_at > "2026-07-24T00:00:00Z"
                return {
                    "provider": "crunchyroll",
                    "provider_series_id": provider_series_id,
                    "provider_title": title,
                    "provider_url": f"https://example.test/{provider_series_id}",
                    "identity_match_kind": identity_match_kind,
                    "match_confidence": 0.99,
                    "review_status": review_status,
                    "catalog_status": catalog_status,
                    "english_dub_status": english_dub_status,
                    "explicit_dub_evidence_source": "provider_audio_locale" if audio_locales else None,
                    "audio_locales": audio_locales,
                    "fetched_at": "2026-07-18T00:00:00Z",
                    "last_verified_at": last_verified_at,
                    "expires_at": expires_at,
                    "fresh": fresh,
                    "expired": not fresh,
                }

            title_search = store_evidence(901, "cr-title", "Title Exact", identity_match_kind="provider_title_search_exact", audio_locales=["EN_gb"])
            shell = store_evidence(902, "cr-shell", "Shell Child", identity_match_kind="provider_franchise_shell_child_match", audio_locales=["en-CA"])
            no_audio = store_evidence(903, "cr-no-audio", "No Audio", identity_match_kind="provider_title_search_exact", audio_locales=[])
            store_evidence(904, "cr-review", "Review Needed", identity_match_kind="provider_title_search_exact", audio_locales=["en-US"], review_status="review-needed", catalog_status="unknown", english_dub_status="unknown", last_verified_at=None)
            store_evidence(905, "cr-stale", "Stale", identity_match_kind="provider_title_search_exact", audio_locales=["en-US"], expires_at="2000-01-01T00:00:00Z")

            insert_recommendation_snapshot_rows(
                db_path,
                [
                    {"kind": "discovery_candidate", "provider": "crunchyroll", "title": "Title Exact", "provider_series_id": "cr-title", "priority": 99, "context": {"mal_anime_id": 901, "english_dub_signal": "present", "provider_eligibility_evidence": [title_search]}},
                    {"kind": "discovery_candidate", "provider": "crunchyroll", "title": "Shell Child", "provider_series_id": "cr-shell", "priority": 98, "context": {"mal_anime_id": 902, "english_dub_signal": "present", "provider_eligibility_evidence": [shell]}},
                    {"kind": "discovery_candidate", "provider": "crunchyroll", "title": "No Audio", "provider_series_id": "cr-no-audio", "priority": 97, "context": {"mal_anime_id": 903, "english_dub_signal": "present", "provider_eligibility_evidence": [no_audio]}},
                ],
                run_id="run-strict-parity",
                generated_at="2026-07-18T03:00:00Z",
            )

            counts = _eligibility_coverage_counts(db_path)
            payload = build_dashboard_payload(db_path)

            self.assertEqual(5, counts["total"])
            self.assertEqual(2, counts["strict_current"])
            self.assertEqual(1, counts["pending_review"])
            self.assertEqual(0, counts["stale"])
            self.assertEqual(2, payload["recommendations"]["section_totals"]["discovery_available_now"])
            self.assertEqual(1, payload["recommendations"]["section_totals"]["discovery_high_confidence"])
            dormant = payload["recommendations"]["sections"]["discovery_high_confidence"][0]
            self.assertEqual("No Audio", dormant["title"])
            self.assertIn("English audio-locales missing/unverified", dormant["strict_actionability"]["missing"])

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
                                    "audio_locales": ["en-US", "ja-JP"],
                                    "fetched_at": "2026-07-18T00:00:00Z",
                                    "last_verified_at": "2026-07-18T00:00:00Z",
                                    "expires_at": "2027-01-01T00:00:00Z",
                                    "fresh": True,
                                },
                                {
                                    "provider": "hidive",
                                    "provider_title": "Multi Provider Actionable",
                                    "provider_url": "https://www.hidive.com/series/multi",
                                    "identity_match_kind": "manual_verified",
                                    "match_confidence": 0.98,
                                    "review_status": "verified",
                                    "catalog_status": "present",
                                    "english_dub_status": "present",
                                    "audio_locales": ["en-US", "ja-JP"],
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
            self.assertEqual("https://www.hidive.com/series/multi", row["provider_badges"][1]["url"])
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

    def test_dashboard_current_fallback_uses_read_only_scoring_without_cache_merge_or_sql_writes(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            project_root = Path(temp_dir)
            db_path = project_root / ".MAL-Updater" / "data" / "mal_updater.sqlite3"
            db_path.parent.mkdir(parents=True)
            bootstrap_database(db_path)
            with connect(db_path) as conn:
                conn.execute(
                    """
                    INSERT INTO provider_series (provider, provider_series_id, title, season_title, raw_json, account_observed_at)
                    VALUES ('crunchyroll', 'seed-series', 'Read Only Seed', 'Read Only Seed (English Dub)', '{}', CURRENT_TIMESTAMP)
                    """
                )
                conn.execute(
                    """
                    INSERT INTO provider_watchlist (provider, provider_series_id, status, raw_json)
                    VALUES ('crunchyroll', 'seed-series', 'fully_watched', '{}')
                    """
                )
                conn.execute(
                    """
                    INSERT INTO mal_series_mapping (provider, provider_series_id, mal_anime_id, mapping_source, approved_by_user)
                    VALUES ('crunchyroll', 'seed-series', 100, 'user_exact', 1)
                    """
                )
                conn.commit()
            upsert_mal_anime_metadata(
                db_path,
                mal_anime_id=100,
                title="Read Only Seed",
                title_english=None,
                title_japanese=None,
                alternative_titles=[],
                media_type="tv",
                status="finished_airing",
                num_episodes=12,
                mean=8.5,
                popularity=500,
                start_season=None,
                raw={"id": 100, "title": "Read Only Seed", "my_list_status": {"status": "completed", "score": 9}},
            )
            upsert_mal_anime_metadata(
                db_path,
                mal_anime_id=200,
                title="Read Only Pick",
                title_english=None,
                title_japanese=None,
                alternative_titles=[],
                media_type="tv",
                status="finished_airing",
                num_episodes=12,
                mean=8.2,
                popularity=600,
                start_season=None,
                raw={"id": 200, "title": "Read Only Pick"},
            )
            replace_mal_recommendation_edges(
                db_path,
                source_mal_anime_id=100,
                hop_distance=1,
                edges=[{"target_mal_anime_id": 200, "target_title": "Read Only Pick", "num_recommendations": 18, "raw": {}}],
            )
            insert_recommendation_snapshot_rows(
                db_path,
                [{"kind": "resume_backlog", "provider": "crunchyroll", "title": "Strict Resume", "provider_series_id": "cr-resume", "priority": 80}],
                run_id="run-strict-resume",
                generated_at="2026-07-19T05:00:00Z",
            )

            write_actions: list[tuple[int, str | None, str | None]] = []
            trapped_connect = _query_only_connect_trap(write_actions)
            with (
                patch("mal_updater.db.connect", side_effect=trapped_connect),
                patch("mal_updater.sync_planner.connect", side_effect=trapped_connect),
                patch("mal_updater.recommendation_dashboard.connect", side_effect=trapped_connect),
                patch("mal_updater.recommendations.merge_mal_user_anime_list_cache_into_metadata", side_effect=AssertionError("dashboard fallback must not merge caches")),
                patch("mal_updater.recommendation_dashboard.build_recommendations", wraps=build_recommendations) as build_spy,
            ):
                payload = build_dashboard_payload(db_path, limit=120)

            self.assertEqual([], write_actions)
            self.assertTrue(build_spy.called)
            self.assertTrue(build_spy.call_args.kwargs["read_only"])
            row = payload["recommendations"]["sections"]["discovery_high_confidence"][0]
            self.assertEqual("Read Only Pick", row["title"])
            self.assertTrue(row["diagnostic_only"])
            self.assertEqual("local-diagnostic-current", payload["recommendations"]["diagnostic_source_snapshot"]["run_id"])

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

    def test_current_diagnostic_discovery_rows_do_not_merge_metadata_cache(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            project_root = Path(temp_dir)
            config = load_config(project_root)
            config.db_path.parent.mkdir(parents=True, exist_ok=True)
            bootstrap_database(config.db_path)

            with patch("mal_updater.recommendations.merge_mal_user_anime_list_cache_into_metadata") as merge_mock:
                rows, source = _current_ranked_discovery_rows_from_local_state(config.db_path, limit=120)

            self.assertEqual([], rows)
            self.assertIsNone(source)
            merge_mock.assert_not_called()

    def test_live_dashboard_indicator_labels_local_current_diagnostics_without_persisted_claim(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            project_root = Path(temp_dir)
            db_path = project_root / ".MAL-Updater" / "data" / "state.db"
            db_path.parent.mkdir(parents=True)
            bootstrap_database(db_path)
            insert_recommendation_snapshot_rows(
                db_path,
                [{"kind": "resume_backlog", "provider": "crunchyroll", "title": "Strict Resume", "provider_series_id": "cr-resume", "priority": 80}],
                run_id="run-strict-resume",
                generated_at="2026-07-19T05:00:00Z",
            )
            item = Recommendation(
                kind="discovery_candidate",
                priority=101,
                provider_series_id="mal:901",
                title="Local Current Candidate",
                season_title=None,
                provider="mal",
                context={"english_dub_signal": "unknown"},
            )

            with (
                patch("mal_updater.recommendation_dashboard.load_config", return_value=SimpleNamespace(db_path=db_path)),
                patch("mal_updater.recommendation_dashboard.build_recommendations", return_value=[item]),
            ):
                payload = build_dashboard_payload(db_path, limit=120)

            self.assertEqual("run-strict-resume", payload["snapshot"]["run_id"])
            self.assertEqual("local-diagnostic-current", payload["recommendations"]["diagnostic_source_snapshot"]["run_id"])
            messages = [item["message"] for item in payload["indicators"]]
            source_messages = [message for message in messages if "Ranked discovery recommendations are sourced" in message]
            self.assertEqual(1, len(source_messages))
            self.assertIn("current local diagnostic scorer output", source_messages[0])
            self.assertNotIn("latest persisted diagnostic discovery snapshot", source_messages[0])

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

        self.assertNotIn(">Dub status<", html)
        for text in ("Availability match", "Availability confidence", "Match source", "Mapping confidence", "Review"):
            self.assertIn(text, html)
        for text in ("title_alias", "provider_search", "0.62", "yes"):
            self.assertIn(text, html)
        self.assertNotIn(">none<", html)

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
                            "availability_confidence": 0.83,
                            "availability_confidence_label": "mapped",
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
            self.assertEqual(0.83, mapped["availability_confidence"])
            self.assertEqual("mapped", mapped["availability_confidence_label"])
            self.assertEqual("mapped", mapped["availability"]["confidence_label"])
            self.assertEqual("mapped", mapped["evidence"]["availability_confidence_label"])
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
                            "availability_confidence": 0.7,
                            "availability_confidence_label": "title alias",
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
            self.assertEqual(0.7, payload[0]["availability_confidence"])
            self.assertEqual("title alias", payload[0]["availability_confidence_label"])
            self.assertEqual("title alias", payload[0]["availability"]["confidence_label"])
            self.assertEqual("unknown", payload[0]["availability"]["dub_status"])
            self.assertTrue(payload[0]["availability"]["review_needed"])

    def test_dashboard_serve_cli_default_limit_is_polished_dashboard_default(self) -> None:
        args = build_parser().parse_args(["dashboard-serve"])
        self.assertEqual(args.limit, DASHBOARD_DEFAULT_RECOMMENDATION_LIMIT)
        parser = build_parser()
        provider_action = next(action for action in parser._actions if getattr(action, "choices", None))
        help_text = provider_action.choices["dashboard-serve"].format_help()
        self.assertIn(f"{DASHBOARD_MIN_RECOMMENDATION_LIMIT}-{DASHBOARD_MAX_RECOMMENDATION_LIMIT}", help_text)
        self.assertIn("invalid query values", help_text)
        self.assertIn("use the default", help_text)

    def test_dashboard_api_handler_get_is_read_only(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            project_root = Path(temp_dir)
            config = load_config(project_root)
            config.db_path.parent.mkdir(parents=True, exist_ok=True)
            bootstrap_database(config.db_path)
            insert_recommendation_snapshot_rows(
                config.db_path,
                [{"kind": "resume_backlog", "provider": "crunchyroll", "title": "Read Only Resume", "provider_series_id": "cr-readonly", "priority": 80}],
                run_id="run-read-only-api",
                generated_at="2026-07-24T07:00:00Z",
            )
            before_bytes = config.db_path.read_bytes()
            write_actions: list[tuple[int, str | None, str | None]] = []
            trapped_connect = _query_only_connect_trap(write_actions)

            with (
                patch("mal_updater.recommendation_dashboard.bootstrap_database", side_effect=AssertionError("GET must not bootstrap")),
                patch("mal_updater.db.connect", side_effect=trapped_connect),
                patch("mal_updater.sync_planner.connect", side_effect=trapped_connect),
                patch("mal_updater.recommendation_dashboard.connect", side_effect=trapped_connect),
                patch("mal_updater.recommendations.merge_mal_user_anime_list_cache_into_metadata") as merge_mock,
            ):
                server = ThreadingHTTPServer(("127.0.0.1", 0), make_dashboard_handler(config.db_path))
                thread = Thread(target=server.serve_forever, daemon=True)
                thread.start()
                try:
                    api = json.loads(urlopen(f"http://127.0.0.1:{server.server_port}/api/dashboard", timeout=5).read().decode("utf-8"))
                finally:
                    server.shutdown()
                    server.server_close()
                    thread.join(timeout=5)

            self.assertIn("recommendations", api)
            self.assertEqual([], write_actions)
            merge_mock.assert_not_called()
            self.assertEqual(before_bytes, config.db_path.read_bytes())

    def test_live_dashboard_recommendation_table_combines_provider_with_title_and_omits_removed_columns(self) -> None:
        html = render_dynamic_dashboard_html()

        self.assertIn("const headings = ['Priority', 'Title', 'Why recommended', 'Scorecard', 'Top watched seeds', 'Genres']", html)
        self.assertNotIn("Identity/review/catalog", html)
        self.assertNotIn("Freshness/expiry", html)
        self.assertNotIn("r.verification || e.verification_label", html)
        self.assertNotIn("r.evidence_freshness || e.evidence_freshness_label", html)
        self.assertNotIn("${titleLabel} / provider proof", html)
        self.assertIn('class="title-providers" aria-label="Provider proof"', html)
        self.assertIn('scope="row"', html)
        self.assertIn('class="table-scroll" tabindex="0" role="region"', html)
        self.assertIn('class=\"provider-link\"', html)
        self.assertIn('aria-label=\"${esc(`Open ${provider} provider proof`)}\"', html)
        self.assertNotIn("const title = b.title", html)
        for heading in ("English dub", "Provider progress", "MAL watch status"):
            self.assertNotIn(f"<th>{heading}</th>", html)
        self.assertNotIn("const progress = r =>", html)

    def test_live_dashboard_multi_genre_filter_menu_is_payload_derived_and_client_side(self) -> None:
        html = render_dynamic_dashboard_html()

        self.assertIn('id="genre-menu-trigger"', html)
        self.assertIn('aria-haspopup="menu"', html)
        self.assertIn('aria-controls="genre-menu"', html)
        self.assertIn('role="menu" aria-label="Available genres"', html)
        self.assertIn('role="menuitem" class="genre-option"', html)
        self.assertIn("Add genre filter", html)
        self.assertIn("function dashboardGenres(data)", html)
        self.assertIn("Object.values(data.recommendations?.sections || {}).flat()", html)
        self.assertIn("selectedGenres.every(genre => r.genres.includes(genre))", html)
        self.assertIn("if (genre && !selectedGenres.includes(genre))", html)
        self.assertIn("filter(genre => !selectedGenres.includes(genre))", html)
        self.assertNotIn("fetch('/api/dashboard?genre=", html)

    def test_live_dashboard_multi_genre_chips_are_removable_and_dismissible(self) -> None:
        html = render_dynamic_dashboard_html()

        self.assertIn('class="genre-chip"', html)
        self.assertIn('class="genre-remove"', html)
        self.assertIn("Remove ${genre} genre filter", html)
        self.assertIn("selectedGenres = selectedGenres.filter", html)
        self.assertIn("event.key !== 'Escape'", html)
        self.assertIn("event.target.closest('[data-genre-control]')", html)
        self.assertIn("No recommendations match the current filters in this section.", html)

    def test_live_dashboard_hidden_titles_persist_and_compose_with_multi_genre_filter(self) -> None:
        html = render_dynamic_dashboard_html()

        self.assertIn("localStorage.getItem(dismissedTitlesKey)", html)
        self.assertIn("localStorage.setItem(dismissedTitlesKey", html)
        self.assertIn('id="show-hidden-titles" type="checkbox"', html)
        self.assertIn("selectedGenres.length ? rows.filter", html)
        self.assertIn("showHiddenTitles ? genreRows : genreRows.filter", html)
        self.assertIn("hiddenTitles.has(recommendationIdentity(r))", html)
        self.assertIn("hiddenTitles.delete(identity)", html)
        self.assertIn("hiddenTitles.add(identity)", html)
        self.assertIn("hidden ? 'Unhide' : 'Hide'", html)
        self.assertIn("hidden-recommendation", html)
        self.assertIn("background:#29343d", html)
        self.assertNotIn("background:#ff", html)

    def test_live_dashboard_hidden_identity_prefers_mal_then_canonical_provider_then_safe_fallback(self) -> None:
        html = render_dynamic_dashboard_html()

        mal_identity = "if (r.mal_anime_id != null && String(r.mal_anime_id).trim()) return `mal:${String(r.mal_anime_id).trim()}`"
        provider_identity = "if (r.provider && r.provider_series_id) return `provider:${String(r.provider).trim().toLowerCase()}:${String(r.provider_series_id).trim()}`"
        fallback_identity = "return `fallback:${encodeURIComponent(`${r.kind || 'recommendation'}|${title}`)}`"
        self.assertIn(mal_identity, html)
        self.assertIn(provider_identity, html)
        self.assertIn(fallback_identity, html)
        self.assertLess(html.index(mal_identity), html.index(provider_identity))
        self.assertLess(html.index(provider_identity), html.index(fallback_identity))
        self.assertIn('data-recommendation-id="${esc(identity)}"', html)

    def test_live_dashboard_removes_duplicate_watchable_copy_but_keeps_green_banner(self) -> None:
        html = render_dynamic_dashboard_html()

        duplicate = "Actionable discovery titles require fresh verified Crunchyroll or HIDIVE availability plus explicit English dub evidence."
        self.assertNotIn(duplicate, html)
        self.assertIn('<section class="banner good"><strong>Watchable now dashboard:</strong>', html)
        self.assertIn("name !== 'discovery_available_now' && meta.description", html)

    def test_debug_dashboard_does_not_enable_main_recommendation_controls(self) -> None:
        debug_html = render_dynamic_debug_html()

        self.assertNotIn('id="genre-filter"', debug_html)
        self.assertNotIn('id="genre-menu-trigger"', debug_html)
        self.assertNotIn("let selectedGenres", debug_html)
        self.assertNotIn('id="show-hidden-titles"', debug_html)
        self.assertNotIn("localStorage.getItem", debug_html)
        self.assertNotIn("renderDashboard(data)", debug_html)

    def test_live_dashboard_scopes_wide_tables_and_wraps_recent_sync_runs(self) -> None:
        html = render_dynamic_debug_html()

        self.assertIn("table{border-collapse:collapse;width:100%;background:#161d24}", html)
        self.assertIn(".table-scroll table{min-width:54rem}", html)
        self.assertIn(".ordinary-table-scroll table{min-width:36rem}", html)
        self.assertNotIn("table{border-collapse:collapse;width:100%;min-width:70rem", html)
        self.assertIn(
            'class=\"ordinary-table-scroll\" tabindex=\"0\" role=\"region\" aria-label=\"Recent provider sync runs table\"',
            html,
        )

    def test_live_dashboard_nested_operational_counts_render_without_object_stringification(self) -> None:
        html = render_dynamic_dashboard_html()

        self.assertIn("const countValue = value =>", html)
        self.assertIn("typeof value === 'object'", html)
        self.assertIn("${countValue(v)}", html)
        self.assertNotIn("${esc(v)}</div>", html)

    def test_live_dashboard_html_and_json_handler(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "state.db"
            bootstrap_database(db_path)
            html = render_dynamic_dashboard_html()
            debug_html = render_dynamic_debug_html(settings_href="/settings")
            self.assertIn("/api/dashboard", html)
            self.assertIn("Recommendations", html)
            self.assertIn("Top watched seeds", html)
            self.assertIn("MAL vote", html)
            self.assertIn("section_metadata", html)
            self.assertIn("Array.isArray(r.genres)", html)
            self.assertNotIn("(r.genres || []).join(', ')", html)
            for moved_section in (
                "Snapshot",
                "Strict coverage",
                "MAL harvest coverage",
                "<h2>Providers</h2>",
                "Review queue",
                "Public MAL userrecs crawl",
                "Crawl coverage",
                "Backlog/open",
                "Hourly throughput",
                "Source-start ETA",
                "Completion ETA",
                "Recent provider sync runs",
            ):
                self.assertNotIn(moved_section, html)
                self.assertIn(moved_section, debug_html)
            self.assertIn('href="/debug">Debug</a>', html)
            self.assertIn('href="/">Dashboard</a>', debug_html)
            self.assertIn('href="/settings">Settings</a>', debug_html)
            self.assertNotIn("Provider enrichment health", html)
            self.assertNotIn("providerEnrichmentSection(data.operational?.provider_enrichment)", html)
            self.assertIn("Provider enrichment health", debug_html)
            self.assertIn("providerEnrichmentSection(data.operational?.provider_enrichment)", debug_html)
            self.assertNotIn("<h2>Indicators</h2>", html)
            self.assertIn("<h2>Indicators</h2>", debug_html)
            self.assertIn("(data.indicators || []).map", debug_html)
            self.assertIn("i.level === 'error' ? 'bad' : 'warn'", debug_html)
            self.assertIn("unknown_pages_per_source", debug_html)

            server = ThreadingHTTPServer(("127.0.0.1", 0), make_dashboard_handler(db_path))
            thread = Thread(target=server.serve_forever, daemon=True)
            thread.start()
            try:
                root = urlopen(f"http://127.0.0.1:{server.server_port}/", timeout=5).read().decode("utf-8")
                debug = urlopen(f"http://127.0.0.1:{server.server_port}/debug", timeout=5).read().decode("utf-8")
                api = json.loads(urlopen(f"http://127.0.0.1:{server.server_port}/api/dashboard", timeout=5).read().decode("utf-8"))
                override = json.loads(urlopen(f"http://127.0.0.1:{server.server_port}/api/dashboard?limit=3", timeout=5).read().decode("utf-8"))
                invalid = json.loads(urlopen(f"http://127.0.0.1:{server.server_port}/api/dashboard?limit=not-an-int", timeout=5).read().decode("utf-8"))
                negative = json.loads(urlopen(f"http://127.0.0.1:{server.server_port}/api/dashboard?limit=-99", timeout=5).read().decode("utf-8"))
                excessive = json.loads(urlopen(f"http://127.0.0.1:{server.server_port}/api/dashboard?limit=999999", timeout=5).read().decode("utf-8"))
            finally:
                server.shutdown()
                server.server_close()
                thread.join(timeout=5)
            self.assertIn("MAL-Updater live dashboard", root)
            self.assertNotIn("Provider enrichment health", root)
            self.assertIn("MAL-Updater debug", debug)
            self.assertIn("Provider enrichment health", debug)
            self.assertIn("snapshot", api)
            self.assertEqual(
                {"generated_at", "snapshot", "genre_affinity", "recommendations", "coverage", "operational", "recent_sync_runs", "indicators"},
                set(api),
            )
            self.assertIn("sections", api["recommendations"])
            self.assertIn("section_metadata", api["recommendations"])
            self.assertIn("coverage_state", api["recommendations"])
            self.assertIn("public_userrecs", api["coverage"])
            self.assertEqual(["unknown_pages_per_source"], api["coverage"]["public_userrecs"]["backlog"]["completion_eta_reason_codes"])
            self.assertEqual(api["recommendations"]["limit"], DASHBOARD_DEFAULT_RECOMMENDATION_LIMIT)
            self.assertEqual(override["recommendations"]["limit"], 3)
            self.assertEqual(invalid["recommendations"]["limit"], DASHBOARD_DEFAULT_RECOMMENDATION_LIMIT)
            self.assertEqual(negative["recommendations"]["limit"], DASHBOARD_MIN_RECOMMENDATION_LIMIT)
            self.assertEqual(excessive["recommendations"]["limit"], DASHBOARD_MAX_RECOMMENDATION_LIMIT)
            self.assertTrue(any("No persisted recommendation snapshot" in item["message"] for item in api["indicators"]))

    def test_main_dashboard_compacts_only_authoritative_failures(self) -> None:
        html = render_dynamic_dashboard_html()

        self.assertNotIn("<h2>Indicators</h2>", html)
        self.assertIn("function failureSection(data)", html)
        self.assertIn("filter(i => i.level === 'error')", html)
        self.assertIn("data.coverage?.summary?.failed", html)
        self.assertIn("if (!failures.length) return ''", html)
        self.assertNotIn("No stale/partial/failure indicators.", html)
        self.assertNotIn("Recommendation harvest coverage is stale, failed, or incomplete.", html)
        self.assertIn('id="dashboard-failures" class="bad">Failures', html)
        self.assertIn("Recommendation harvest failures: ${failedHarvests}.", html)

    def test_dashboard_api_payload_contract_is_unchanged_by_presentation_split(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "state.db"
            bootstrap_database(db_path)

            payload = build_dashboard_payload(db_path)

        self.assertEqual(
            {"generated_at", "snapshot", "genre_affinity", "recommendations", "coverage", "operational", "recent_sync_runs", "indicators"},
            set(payload),
        )
        self.assertIsInstance(payload["indicators"], list)
        self.assertTrue(any(item["level"] == "warning" for item in payload["indicators"]))


if __name__ == "__main__":
    unittest.main()
