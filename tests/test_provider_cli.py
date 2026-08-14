from __future__ import annotations

import io
import inspect
import json
import subprocess
import sys
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from mal_updater.cli import _cmd_provider_fetch_snapshot, build_parser, main
from mal_updater.config import load_config
from mal_updater.contracts import ProviderSnapshot
from mal_updater.db import get_series_mapping
from mal_updater.hidive_auth import HidiveAuthError
from mal_updater.hidive_snapshot import HidiveSnapshotError
from mal_updater.ingestion import ingest_snapshot_payload
from mal_updater.provider_types import ProviderFetchResult, ProviderModule
from mal_updater.providers.hidive import HidiveProvider
from tests.test_validation_ingestion import sample_snapshot


class _FakeSummary:
    def as_dict(self) -> dict[str, object]:
        return {"status": "ok"}


def _sample_provider_snapshot(provider: str = "hidive", raw: dict[str, object] | None = None) -> ProviderSnapshot:
    return ProviderSnapshot(
        contract_version="1.0",
        generated_at="2026-07-24T00:00:00Z",
        provider=provider,
        account_id_hint=None,
        raw=raw or {},
    )


class _CliFakeProvider:
    slug = "hidive"
    display_name = "HIDIVE"

    def __init__(
        self,
        *,
        snapshot: ProviderSnapshot | None = None,
        error: Exception | None = None,
        slug: str = "hidive",
        display_name: str = "HIDIVE",
    ) -> None:
        self.slug = slug
        self.display_name = display_name
        self.snapshot = snapshot or _sample_provider_snapshot()
        self.error = error

    def fetch_snapshot(
        self,
        config,
        *,
        profile: str = "default",
        full_refresh: bool = False,
        max_history_pages: int | None = None,
        max_watchlist_pages: int | None = None,
        history_start_page: int = 1,
        watchlist_start: int = 0,
    ) -> ProviderFetchResult:
        if self.error is not None:
            raise self.error
        return ProviderFetchResult(snapshot=self.snapshot)

    def write_snapshot_file(self, path: Path, snapshot: ProviderSnapshot) -> Path:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("{}\n", encoding="utf-8")
        return path


EXPECTED_CLI_COMMAND_SURFACE = (
    "apply-sync",
    "approve-mapping",
    "backfill-hidive-series-urls",
    "bootstrap-audit",
    "crunchyroll-auth-login",
    "crunchyroll-fetch-snapshot",
    "dashboard-serve",
    "dry-run-sync",
    "exact-approved-sync-cycle",
    "health-check",
    "health-check-cycle",
    "ingest-snapshot",
    "init",
    "install-service",
    "list-mappings",
    "list-review-queue",
    "mal-auth-login",
    "mal-auth-url",
    "mal-list-refresh",
    "mal-list-reinitialize",
    "mal-refresh",
    "mal-whoami",
    "map-series",
    "provider-auth-login",
    "provider-fetch-snapshot",
    "provider-stale-rows",
    "push-recommendations-webhook",
    "recommend",
    "recommend-coverage",
    "recommend-dashboard",
    "recommend-enrich-provider-availability",
    "recommend-maintain",
    "recommend-refresh-full-userrecs",
    "recommend-refresh-metadata",
    "recommend-reinitialize-full-userrecs",
    "recommend-snapshots",
    "refresh-mapping-review-queue",
    "reopen-review-queue",
    "resolve-review-queue",
    "restart-service",
    "review-mappings",
    "review-queue-apply-worklist",
    "review-queue-next",
    "review-queue-refresh-worklist",
    "review-queue-worklist",
    "runtime-retention-audit",
    "service-run",
    "service-run-once",
    "service-status",
    "start-service",
    "status",
    "stop-service",
    "uninstall-service",
    "validate-snapshot",
)


def _command_parsers() -> dict[str, object]:
    parser = build_parser()
    command_action = next(action for action in parser._actions if getattr(action, "choices", None))
    return command_action.choices


def _command_help_map() -> dict[str, str]:
    parser = build_parser()
    command_action = next(action for action in parser._actions if getattr(action, "choices", None))
    return {action.dest: action.help for action in command_action._choices_actions}


class ProviderCliTests(unittest.TestCase):
    def test_dry_run_sync_passes_provider_to_sync_planner(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / ".MAL-Updater" / "config").mkdir(parents=True)
            output = io.StringIO()
            with patch("mal_updater.cli.build_dry_run_sync_plan", return_value=[] ) as build_mock, patch.object(
                sys, "argv", [
                    "mal-updater",
                    "--project-root",
                    str(root),
                    "dry-run-sync",
                    "--provider",
                    "hidive",
                    "--limit",
                    "5",
                ]
            ), redirect_stdout(output):
                rc = main()
            self.assertEqual(0, rc)
            self.assertEqual("hidive", build_mock.call_args.kwargs["provider"])
            payload = json.loads(output.getvalue())
            self.assertEqual([], payload["proposals"])

    def test_dry_run_sync_provider_all_passes_aggregate_to_sync_planner(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / ".MAL-Updater" / "config").mkdir(parents=True)
            output = io.StringIO()
            with patch("mal_updater.cli.build_dry_run_sync_plan", return_value=[]) as build_mock, patch.object(
                sys, "argv", [
                    "mal-updater",
                    "--project-root",
                    str(root),
                    "dry-run-sync",
                    "--provider",
                    "all",
                    "--limit",
                    "5",
                ]
            ), redirect_stdout(output):
                rc = main()
            self.assertEqual(0, rc)
            self.assertIsNone(build_mock.call_args.kwargs["provider"])
            payload = json.loads(output.getvalue())
            self.assertEqual([], payload["proposals"])

    def test_approve_mapping_provider_option_persists_selected_provider(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / ".MAL-Updater" / "config").mkdir(parents=True)
            config = load_config(root)
            payload = sample_snapshot()
            payload["provider"] = "hidive"
            payload["series"][0]["provider_series_id"] = "hidive-series-123"
            payload["progress"][0]["provider_series_id"] = "hidive-series-123"
            payload["watchlist"][0]["provider_series_id"] = "hidive-series-123"
            ingest_snapshot_payload(payload, config)
            output = io.StringIO()
            with patch.object(
                sys,
                "argv",
                [
                    "mal-updater",
                    "--project-root",
                    str(root),
                    "approve-mapping",
                    "hidive-series-123",
                    "123",
                    "--provider",
                    "hidive",
                    "--confidence",
                    "0.9",
                ],
            ), redirect_stdout(output):
                rc = main()

            self.assertEqual(0, rc)
            payload = json.loads(output.getvalue())
            self.assertEqual("hidive", payload["provider"])
            self.assertIsNone(get_series_mapping(config.db_path, "crunchyroll", "hidive-series-123"))
            mapping = get_series_mapping(config.db_path, "hidive", "hidive-series-123")
            self.assertIsNotNone(mapping)
            assert mapping is not None
            self.assertEqual(123, mapping.mal_anime_id)

    def test_approve_mapping_without_provider_preserves_crunchyroll_default(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / ".MAL-Updater" / "config").mkdir(parents=True)
            config = load_config(root)
            ingest_snapshot_payload(sample_snapshot(), config)
            output = io.StringIO()
            with patch.object(
                sys,
                "argv",
                [
                    "mal-updater",
                    "--project-root",
                    str(root),
                    "approve-mapping",
                    "series-123",
                    "123",
                ],
            ), redirect_stdout(output):
                rc = main()

            self.assertEqual(0, rc)
            payload = json.loads(output.getvalue())
            self.assertEqual("crunchyroll", payload["provider"])
            self.assertIsNotNone(get_series_mapping(config.db_path, "crunchyroll", "series-123"))

    def test_provider_fetch_snapshot_uses_shared_provider_serializer(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / ".MAL-Updater" / "config").mkdir(parents=True)
            provider = _CliFakeProvider()
            output = io.StringIO()

            with patch("mal_updater.cli.get_provider", return_value=provider), patch(
                "mal_updater.cli.provider_snapshot.snapshot_to_dict",
                return_value={"provider": "hidive", "serializer": "shared"},
            ) as serializer_mock, redirect_stdout(output):
                rc = _cmd_provider_fetch_snapshot(root, "hidive", "default", None, ingest=False, full_refresh=False)

            self.assertEqual(0, rc)
            serializer_mock.assert_called_once_with(provider.snapshot)
            self.assertEqual({"provider": "hidive", "serializer": "shared"}, json.loads(output.getvalue()))

    def test_provider_fetch_snapshot_renders_hidive_errors_without_traceback(self) -> None:
        for error_cls in (HidiveAuthError, HidiveSnapshotError):
            with self.subTest(error_cls=error_cls.__name__), tempfile.TemporaryDirectory() as td:
                root = Path(td)
                (root / ".MAL-Updater" / "config").mkdir(parents=True)
                provider = _CliFakeProvider(error=error_cls("HIDIVE test failure"))
                stdout = io.StringIO()
                stderr = io.StringIO()

                with patch("mal_updater.cli.get_provider", return_value=provider), redirect_stdout(stdout), redirect_stderr(stderr):
                    rc = _cmd_provider_fetch_snapshot(root, "hidive", "default", None, ingest=False, full_refresh=False)

                self.assertEqual(1, rc)
                combined_output = stdout.getvalue() + stderr.getvalue()
                self.assertIn("HIDIVE test failure", stderr.getvalue())
                self.assertNotIn("Traceback", combined_output)

    def test_provider_fetch_snapshot_partial_warning_names_actual_provider(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / ".MAL-Updater" / "config").mkdir(parents=True)
            provider = _CliFakeProvider()
            stderr = io.StringIO()
            output = io.StringIO()
            payload = {"provider": "hidive", "raw": {"partial": True}}

            with patch("mal_updater.cli.get_provider", return_value=provider), patch(
                "mal_updater.cli.provider_snapshot.snapshot_to_dict",
                return_value=payload,
            ), patch("mal_updater.cli.ingest_snapshot_payload", return_value=_FakeSummary()) as ingest_mock, redirect_stdout(output), redirect_stderr(stderr):
                rc = _cmd_provider_fetch_snapshot(root, "hidive", "default", None, ingest=True, full_refresh=True)

            self.assertEqual(0, rc)
            self.assertEqual("hot", ingest_mock.call_args.kwargs["mode"])
            self.assertIn("Partial HIDIVE snapshot detected", stderr.getvalue())
            self.assertNotIn("Crunchyroll", stderr.getvalue())

    def test_provider_fetch_snapshot_ingests_completed_bootstrap_as_full_refresh(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / ".MAL-Updater" / "config").mkdir(parents=True)
            snapshot = _sample_provider_snapshot(
                provider="crunchyroll",
                raw={"partial": False, "sync_boundary_refresh_kind": "bootstrap_full_refresh"},
            )
            provider = _CliFakeProvider(snapshot=snapshot, slug="crunchyroll", display_name="Crunchyroll")
            output = io.StringIO()

            with patch("mal_updater.cli.get_provider", return_value=provider), patch(
                "mal_updater.cli.ingest_snapshot_payload", return_value=_FakeSummary()
            ) as ingest_mock, redirect_stdout(output):
                rc = _cmd_provider_fetch_snapshot(root, "crunchyroll", "default", None, ingest=True, full_refresh=False)

            self.assertEqual(0, rc)
            self.assertEqual("full_refresh", ingest_mock.call_args.kwargs["mode"])

    def test_provider_fetch_snapshot_ingests_partial_bootstrap_as_hot(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / ".MAL-Updater" / "config").mkdir(parents=True)
            snapshot = _sample_provider_snapshot(
                provider="crunchyroll",
                raw={"partial": True, "sync_boundary_refresh_kind": "bootstrap_full_refresh"},
            )
            provider = _CliFakeProvider(snapshot=snapshot, slug="crunchyroll", display_name="Crunchyroll")
            output = io.StringIO()
            stderr = io.StringIO()

            with patch("mal_updater.cli.get_provider", return_value=provider), patch(
                "mal_updater.cli.ingest_snapshot_payload", return_value=_FakeSummary()
            ) as ingest_mock, redirect_stdout(output), redirect_stderr(stderr):
                rc = _cmd_provider_fetch_snapshot(root, "crunchyroll", "default", None, ingest=True, full_refresh=False)

            self.assertEqual(0, rc)
            self.assertEqual("hot", ingest_mock.call_args.kwargs["mode"])
            self.assertIn("Partial Crunchyroll snapshot detected", stderr.getvalue())

    def test_hidive_provider_fetch_signature_accepts_protocol_page_kwargs_safely(self) -> None:
        protocol_parameters = inspect.signature(ProviderModule.fetch_snapshot).parameters
        hidive_parameters = inspect.signature(HidiveProvider.fetch_snapshot).parameters
        for name in ("profile", "full_refresh", "max_history_pages", "max_watchlist_pages", "history_start_page", "watchlist_start"):
            with self.subTest(parameter=name):
                self.assertIn(name, hidive_parameters)
                self.assertEqual(protocol_parameters[name].default, hidive_parameters[name].default)

        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / ".MAL-Updater" / "config").mkdir(parents=True)
            config = load_config(root)
            provider = HidiveProvider()
            snapshot = _sample_provider_snapshot()
            fetch_result = SimpleNamespace(snapshot=snapshot, history_count=1, continue_count=0, favourite_count=0)

            with patch("mal_updater.providers.hidive.fetch_hidive_snapshot", return_value=fetch_result) as fetch_mock:
                result = provider.fetch_snapshot(
                    config,
                    max_history_pages=None,
                    max_watchlist_pages=None,
                    history_start_page=1,
                    watchlist_start=0,
                )

            self.assertIs(snapshot, result.snapshot)
            self.assertTrue(fetch_mock.call_args.kwargs["use_incremental_boundary"])
            with self.assertRaisesRegex(ValueError, "max_history_pages"):
                provider.fetch_snapshot(config, max_history_pages=1)

    def test_cli_command_surface_snapshot_and_representative_help(self) -> None:
        command_parsers = _command_parsers()
        command_help = _command_help_map()
        actual_commands = tuple(sorted(command_parsers))

        self.assertEqual(EXPECTED_CLI_COMMAND_SURFACE, actual_commands)
        self.assertEqual(54, len(actual_commands))

        for command, expected_help in {
            "provider-fetch-snapshot": "account-scoped history/watchlist details",
            "apply-sync": "Guarded MAL executor",
            "push-recommendations-webhook": "configured webhook ingress",
            "service-run-once": "Run one MAL-Updater daemon loop pass and exit",
            "recommend-dashboard": "sortable local HTML recommendation dashboard",
            "dashboard-serve": "live local HTTP dashboard",
            "recommend-refresh-full-userrecs": "per-source per-run",
            "recommend-reinitialize-full-userrecs": "quarantined public-userrecs source",
            "mal-list-reinitialize": "quarantined exact MAL account/query traversal",
            "runtime-retention-audit": "retention inventory audit",
        }.items():
            with self.subTest(command=command):
                help_text = command_parsers[command].format_help()
                self.assertIn(f"usage: mal-updater {command}", help_text)
                self.assertIn(expected_help, command_help[command])

    def test_state_changing_cli_parser_gates_are_explicit(self) -> None:
        parser = build_parser()

        apply_args = parser.parse_args(["apply-sync"])
        self.assertFalse(apply_args.execute)
        self.assertTrue(parser.parse_args(["apply-sync", "--execute"]).execute)

        provider_fetch_args = parser.parse_args(["provider-fetch-snapshot", "--provider", "hidive"])
        self.assertFalse(provider_fetch_args.ingest)
        self.assertTrue(parser.parse_args(["provider-fetch-snapshot", "--provider", "hidive", "--ingest"]).ingest)

        push_args = parser.parse_args(["push-recommendations-webhook"])
        self.assertFalse(push_args.dry_run)
        self.assertTrue(parser.parse_args(["push-recommendations-webhook", "--dry-run"]).dry_run)

        full_userrecs_args = parser.parse_args(["recommend-refresh-full-userrecs"])
        self.assertEqual(10, full_userrecs_args.max_pages)
        provider_eligibility_args = parser.parse_args(["recommend-enrich-provider-availability"])
        self.assertEqual(5, provider_eligibility_args.limit)

        enrich_args = parser.parse_args(["recommend-enrich-provider-availability"])
        self.assertEqual(5, enrich_args.limit)

        command_parsers = _command_parsers()
        command_help = _command_help_map()
        self.assertEqual("service-run-once", parser.parse_args(["service-run-once"]).command)
        self.assertIn("Run one MAL-Updater daemon loop pass and exit", command_help["service-run-once"])

    def test_state_changing_cli_gates_are_forwarded_by_main(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / ".MAL-Updater" / "config").mkdir(parents=True)

            for extra_args, expected_execute in (([], False), (["--execute"], True)):
                with self.subTest(command="apply-sync", extra_args=extra_args), patch.object(
                    sys,
                    "argv",
                    ["mal-updater", "--project-root", str(root), "apply-sync", *extra_args],
                ), patch("mal_updater.cli._run_apply_sync", return_value=[]) as run_apply, redirect_stdout(io.StringIO()):
                    self.assertEqual(0, main())
                self.assertEqual(expected_execute, run_apply.call_args.kwargs["execute"])

            for extra_args, expected_ingest in (([], False), (["--ingest"], True)):
                with self.subTest(command="provider-fetch-snapshot", extra_args=extra_args), patch.object(
                    sys,
                    "argv",
                    ["mal-updater", "--project-root", str(root), "provider-fetch-snapshot", "--provider", "hidive", *extra_args],
                ), patch("mal_updater.cli._cmd_provider_fetch_snapshot", return_value=0) as fetch_cmd:
                    self.assertEqual(0, main())
                self.assertEqual(expected_ingest, fetch_cmd.call_args.args[4])

            for extra_args, expected_dry_run, status in (([], False, "delivered"), (["--dry-run"], True, "dry_run")):
                result = SimpleNamespace(status=status, as_dict=lambda status=status: {"status": status})
                with self.subTest(command="push-recommendations-webhook", extra_args=extra_args), patch.object(
                    sys,
                    "argv",
                    ["mal-updater", "--project-root", str(root), "push-recommendations-webhook", *extra_args],
                ), patch("mal_updater.cli.deliver_recommendations_via_openclaw", return_value=result) as deliver, redirect_stdout(io.StringIO()):
                    self.assertEqual(0, main())
                self.assertEqual(expected_dry_run, deliver.call_args.kwargs["dry_run"])

            with patch.object(
                sys,
                "argv",
                ["mal-updater", "--project-root", str(root), "service-run-once"],
            ), patch("mal_updater.cli.run_pending_tasks", return_value={"status": "ok"}) as run_once, redirect_stdout(io.StringIO()):
                self.assertEqual(0, main())
            run_once.assert_called_once()

    def test_provider_help_warns_against_whole_library_crawling(self) -> None:
        parser = build_parser()
        provider_action = next(action for action in parser._actions if getattr(action, "choices", None))
        provider_parser = provider_action.choices["provider-fetch-snapshot"]
        help_text = provider_parser.format_help()

        self.assertIn("account-scoped history/watchlist surfaces", help_text)
        self.assertIn("never crawl whole Crunchyroll/HIDIVE libraries", help_text)

    def test_crunchyroll_compatibility_wrappers_remain_parseable_and_dispatched(self) -> None:
        parser = build_parser()
        provider_action = next(action for action in parser._actions if getattr(action, "choices", None))
        self.assertIn("crunchyroll-auth-login", provider_action.choices)
        self.assertIn("crunchyroll-fetch-snapshot", provider_action.choices)
        self.assertEqual("crunchyroll-auth-login", parser.parse_args(["crunchyroll-auth-login"]).command)
        self.assertEqual("crunchyroll-fetch-snapshot", parser.parse_args(["crunchyroll-fetch-snapshot"]).command)

        with patch.object(sys, "argv", ["mal-updater", "crunchyroll-auth-login"]), patch(
            "mal_updater.cli._cmd_crunchyroll_auth_login", return_value=0
        ) as auth_wrapper:
            self.assertEqual(0, main())
        auth_wrapper.assert_called_once()

        with patch.object(sys, "argv", ["mal-updater", "crunchyroll-fetch-snapshot"]), patch(
            "mal_updater.cli._cmd_crunchyroll_fetch_snapshot", return_value=0
        ) as fetch_wrapper:
            self.assertEqual(0, main())
        fetch_wrapper.assert_called_once()

    def test_direct_cli_parser_import_registers_builtin_provider_choices_in_fresh_process(self) -> None:
        code = """
from mal_updater.cli_parser import build_parser

parser = build_parser()
provider_action = next(action for action in parser._actions if getattr(action, 'choices', None))
provider_parser = provider_action.choices['provider-fetch-snapshot']
provider_arg = next(action for action in provider_parser._actions if action.dest == 'provider')
assert list(provider_arg.choices) == ['crunchyroll', 'hidive'], provider_arg.choices
"""
        subprocess.run([sys.executable, "-c", code], check=True)

    def test_recommend_metadata_help_documents_paced_mal_refreshes(self) -> None:
        parser = build_parser()
        provider_action = next(action for action in parser._actions if getattr(action, "choices", None))
        recommend_parser = provider_action.choices["recommend-refresh-metadata"]
        help_text = recommend_parser.format_help()

        self.assertIn("MAL refreshes are paced by client throttling", help_text)
        self.assertIn("spread over time", help_text)

    def test_dead_sync_placeholder_is_absent_but_real_sync_commands_remain(self) -> None:
        parser = build_parser()
        provider_action = next(action for action in parser._actions if getattr(action, "choices", None))
        commands = set(provider_action.choices)

        self.assertNotIn("sync", provider_action.choices)
        self.assertNotIn("sync", commands)
        for command in ("dry-run-sync", "apply-sync", "exact-approved-sync-cycle"):
            with self.subTest(command=command):
                self.assertIn(command, commands)
                self.assertEqual(command, parser.parse_args([command]).command)
        with patch("sys.stderr", new_callable=io.StringIO), self.assertRaises(SystemExit) as raised:
            parser.parse_args(["sync"])
        self.assertEqual(2, raised.exception.code)


if __name__ == "__main__":
    unittest.main()
