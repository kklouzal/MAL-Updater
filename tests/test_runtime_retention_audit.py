from __future__ import annotations

import io
import json
import os
import sys
import tempfile
import unittest
from contextlib import contextmanager, redirect_stderr, redirect_stdout
from pathlib import Path
from typing import Iterator
from unittest.mock import patch

from mal_updater import cli as cli_module
from mal_updater.cli import main
from mal_updater.config import _load_config_unchecked
from mal_updater.runtime_retention_audit import (
    POLICY_MARKER,
    AuditCaps,
    AuditOptions,
    WarningThresholds,
    build_runtime_retention_audit_payload,
    render_runtime_retention_audit_summary,
)


@contextmanager
def _isolated_runtime_env(*, workspace_root: Path, runtime_root: Path) -> Iterator[None]:
    managed_keys = [key for key in os.environ if key.startswith("MAL_UPDATER_")]
    managed_keys.append("OPENCLAW_WORKSPACE_DIR")
    previous = {key: os.environ.get(key) for key in managed_keys}
    for key in managed_keys:
        os.environ.pop(key, None)
    os.environ.update(
        {
            "MAL_UPDATER_WORKSPACE_DIR": str(workspace_root),
            "MAL_UPDATER_RUNTIME_ROOT": str(runtime_root),
            "MAL_UPDATER_SETTINGS_PATH": str(runtime_root / "config" / "settings.toml"),
        }
    )
    try:
        yield
    finally:
        for key in list(os.environ):
            if key.startswith("MAL_UPDATER_") or key == "OPENCLAW_WORKSPACE_DIR":
                os.environ.pop(key, None)
        for key, value in previous.items():
            if value is not None:
                os.environ[key] = value


def _create_expected_top_level(runtime_root: Path, *, skip: set[str] | None = None) -> None:
    skip = skip or set()
    for name in ("config", "secrets", "data", "state", "cache"):
        if name not in skip:
            (runtime_root / name).mkdir(parents=True, exist_ok=True)


def _write_repo_markers(project_root: Path) -> None:
    (project_root / "pyproject.toml").write_text("[project]\nname='example'\n", encoding="utf-8")
    (project_root / ".git").mkdir(exist_ok=True)
    (project_root / "src").mkdir(exist_ok=True)


def _load_temp_config(project_root: Path, workspace_root: Path, runtime_root: Path):
    with _isolated_runtime_env(workspace_root=workspace_root, runtime_root=runtime_root):
        return _load_config_unchecked(project_root)


def _run_cli(project_root: Path, workspace_root: Path, runtime_root: Path, *args: str) -> tuple[int, str, str]:
    stdout = io.StringIO()
    stderr = io.StringIO()
    with _isolated_runtime_env(workspace_root=workspace_root, runtime_root=runtime_root), patch.object(
        sys,
        "argv",
        ["mal-updater", "--project-root", str(project_root), *args],
    ), patch.object(cli_module, "load_config", _load_config_unchecked), redirect_stdout(stdout), redirect_stderr(stderr):
        rc = main()
    return rc, stdout.getvalue(), stderr.getvalue()


def _issue_codes(payload: dict[str, object]) -> set[str]:
    layout = payload["layout"]
    assert isinstance(layout, dict)
    issues = layout["issues"]
    assert isinstance(issues, list)
    return {str(issue["code"]) for issue in issues if isinstance(issue, dict)}


def _runtime_tree(root: Path) -> list[tuple[str, bool, bool]]:
    entries: list[tuple[str, bool, bool]] = []
    for current, dirs, files in os.walk(root, followlinks=False):
        current_path = Path(current)
        for name in sorted([*dirs, *files]):
            path = current_path / name
            entries.append((path.relative_to(root).as_posix(), path.is_dir(), path.is_symlink()))
    return sorted(entries)


class RuntimeRetentionAuditTests(unittest.TestCase):
    def test_clean_layout_json_default_is_diagnostic_only(self) -> None:
        with tempfile.TemporaryDirectory(prefix="mal-runtime-audit-clean-", dir="/tmp") as td:
            root = Path(td)
            project_root = root / "repo"
            workspace_root = root / "workspace"
            runtime_root = project_root / ".MAL-Updater"
            project_root.mkdir(parents=True)
            workspace_root.mkdir(parents=True)
            _create_expected_top_level(runtime_root)

            rc, stdout, stderr = _run_cli(project_root, workspace_root, runtime_root, "runtime-retention-audit")

            self.assertEqual(0, rc, stderr)
            payload = json.loads(stdout)
            self.assertEqual(POLICY_MARKER, payload["policy"])
            self.assertEqual("ok", payload["status"])
            self.assertEqual("ok", payload["layout"]["status"])
            self.assertEqual(0, payload["review_candidate_count"])
            self.assertEqual("high_value_manual_retention_review_only", payload["families"]["db_backups"]["manual_policy"])
            self.assertTrue(payload["families"]["db_backups"]["high_value"])
            self.assertEqual("diagnostic_only_no_delete_or_prune", payload["families"]["cache"]["policy"])

    def test_standard_project_runtime_child_with_repo_markers_is_strict_clean(self) -> None:
        with tempfile.TemporaryDirectory(prefix="mal-runtime-audit-standard-repo-child-", dir="/tmp") as td:
            root = Path(td)
            project_root = root / "repo"
            workspace_root = root / "workspace"
            runtime_root = project_root / ".MAL-Updater"
            project_root.mkdir(parents=True)
            workspace_root.mkdir(parents=True)
            _write_repo_markers(project_root)
            _create_expected_top_level(runtime_root)

            rc, stdout, stderr = _run_cli(project_root, workspace_root, runtime_root, "runtime-retention-audit", "--strict")

            self.assertEqual(0, rc, stderr)
            payload = json.loads(stdout)
            self.assertEqual("ok", payload["layout"]["status"])
            self.assertNotIn("repo_source_runtime_overlap", _issue_codes(payload))
            self.assertFalse(payload["strict"]["would_fail"])

    def test_state_backups_are_high_value_db_backups_counted_once(self) -> None:
        with tempfile.TemporaryDirectory(prefix="mal-runtime-audit-state-backups-", dir="/tmp") as td:
            root = Path(td)
            project_root = root / "repo"
            workspace_root = root / "workspace"
            runtime_root = project_root / ".MAL-Updater"
            project_root.mkdir(parents=True)
            workspace_root.mkdir(parents=True)
            _create_expected_top_level(runtime_root)
            backup_root = runtime_root / "state" / "backups"
            backup_root.mkdir(parents=True)
            (backup_root / "mal_updater.sqlite3.bak").write_text("backup\n", encoding="utf-8")

            rc, stdout, stderr = _run_cli(project_root, workspace_root, runtime_root, "runtime-retention-audit", "--warn-file-count", "1")

            self.assertEqual(0, rc, stderr)
            payload = json.loads(stdout)
            db_backups = payload["families"]["db_backups"]
            self.assertEqual(1, db_backups["file_count"])
            self.assertEqual(1, db_backups["count"])
            self.assertEqual(1, payload["retention_inventory"]["totals"]["file_count"])
            self.assertEqual("high_value_manual_retention_review_only", db_backups["manual_policy"])
            self.assertTrue(db_backups["high_value"])
            self.assertEqual("high_value_manual_policy", db_backups["candidate_class"])
            candidates = payload["review_candidates"]
            self.assertEqual(1, len(candidates))
            self.assertEqual("db_backups", candidates[0]["family"])
            self.assertEqual("file_count_threshold_exceeded", candidates[0]["reason_code"])
            self.assertEqual("high_value_manual_retention_review_only", candidates[0]["manual_policy"])
            self.assertTrue(candidates[0]["high_value"])
            self.assertEqual("high_value_manual_policy", candidates[0]["candidate_class"])

    def test_nested_runtime_root_is_detected(self) -> None:
        with tempfile.TemporaryDirectory(prefix="mal-runtime-audit-nested-", dir="/tmp") as td:
            root = Path(td)
            project_root = root / "repo"
            workspace_root = root / "workspace"
            runtime_root = project_root / ".MAL-Updater"
            project_root.mkdir(parents=True)
            workspace_root.mkdir(parents=True)
            _create_expected_top_level(runtime_root)
            (runtime_root / ".MAL-Updater").mkdir()
            config = _load_temp_config(project_root, workspace_root, runtime_root)

            payload = build_runtime_retention_audit_payload(config)

            self.assertIn("nested_runtime_root", _issue_codes(payload))
            self.assertEqual("error", payload["layout"]["status"])

    def test_repo_overlap_rejects_runtime_equal_to_project_root_with_repo_markers(self) -> None:
        with tempfile.TemporaryDirectory(prefix="mal-runtime-audit-project-root-overlap-", dir="/tmp") as td:
            root = Path(td)
            project_root = root / "repo"
            workspace_root = root / "workspace"
            runtime_root = project_root
            project_root.mkdir(parents=True)
            workspace_root.mkdir(parents=True)
            _write_repo_markers(project_root)
            _create_expected_top_level(runtime_root)

            rc, stdout, stderr = _run_cli(project_root, workspace_root, runtime_root, "runtime-retention-audit", "--strict")

            self.assertEqual(2, rc, stderr)
            payload = json.loads(stdout)
            self.assertIn("repo_source_runtime_overlap", _issue_codes(payload))
            self.assertTrue(payload["strict"]["would_fail"])

    def test_repo_overlap_rejects_runtime_ancestor_containing_project_with_repo_markers(self) -> None:
        with tempfile.TemporaryDirectory(prefix="mal-runtime-audit-ancestor-overlap-", dir="/tmp") as td:
            runtime_root = Path(td)
            project_root = runtime_root / "repo"
            workspace_root = runtime_root / "workspace"
            project_root.mkdir(parents=True)
            workspace_root.mkdir(parents=True)
            _write_repo_markers(project_root)
            _create_expected_top_level(runtime_root)

            rc, stdout, stderr = _run_cli(project_root, workspace_root, runtime_root, "runtime-retention-audit", "--strict")

            self.assertEqual(2, rc, stderr)
            payload = json.loads(stdout)
            self.assertIn("repo_source_runtime_overlap", _issue_codes(payload))
            self.assertTrue(payload["strict"]["would_fail"])

    def test_repo_overlap_rejects_source_root_or_descendant_with_repo_markers(self) -> None:
        cases = {
            "source_root": lambda project_root: project_root / "src",
            "source_descendant": lambda project_root: project_root / "src" / "runtime-state",
        }
        for label, runtime_factory in cases.items():
            with self.subTest(label=label):
                with tempfile.TemporaryDirectory(prefix=f"mal-runtime-audit-{label}-overlap-", dir="/tmp") as td:
                    root = Path(td)
                    project_root = root / "repo"
                    workspace_root = root / "workspace"
                    project_root.mkdir(parents=True)
                    workspace_root.mkdir(parents=True)
                    _write_repo_markers(project_root)
                    runtime_root = runtime_factory(project_root)
                    _create_expected_top_level(runtime_root)

                    rc, stdout, stderr = _run_cli(project_root, workspace_root, runtime_root, "runtime-retention-audit", "--strict")

                    self.assertEqual(2, rc, stderr)
                    payload = json.loads(stdout)
                    self.assertIn("repo_source_runtime_overlap", _issue_codes(payload))
                    self.assertTrue(payload["strict"]["would_fail"])

    def test_repo_overlap_rejects_nonstandard_child_inside_repo_with_repo_markers(self) -> None:
        with tempfile.TemporaryDirectory(prefix="mal-runtime-audit-nonstandard-child-overlap-", dir="/tmp") as td:
            root = Path(td)
            project_root = root / "repo"
            workspace_root = root / "workspace"
            runtime_root = project_root / "runtime-state"
            project_root.mkdir(parents=True)
            workspace_root.mkdir(parents=True)
            _write_repo_markers(project_root)
            _create_expected_top_level(runtime_root)

            rc, stdout, stderr = _run_cli(project_root, workspace_root, runtime_root, "runtime-retention-audit", "--strict")

            self.assertEqual(2, rc, stderr)
            payload = json.loads(stdout)
            self.assertIn("repo_source_runtime_overlap", _issue_codes(payload))
            self.assertTrue(payload["strict"]["would_fail"])

    def test_runtime_root_symlink_is_strict_layout_failure(self) -> None:
        with tempfile.TemporaryDirectory(prefix="mal-runtime-audit-root-symlink-", dir="/tmp") as td:
            root = Path(td)
            project_root = root / "repo"
            workspace_root = root / "workspace"
            target_runtime_root = root / "runtime-target" / ".MAL-Updater"
            runtime_root = root / "runtime-link"
            project_root.mkdir(parents=True)
            workspace_root.mkdir(parents=True)
            _create_expected_top_level(target_runtime_root)
            os.symlink(target_runtime_root, runtime_root)

            rc, stdout, stderr = _run_cli(project_root, workspace_root, runtime_root, "runtime-retention-audit", "--strict")

            self.assertEqual(2, rc, stderr)
            payload = json.loads(stdout)
            self.assertIn("runtime_root_is_symlink", _issue_codes(payload))
            self.assertTrue(payload["strict"]["would_fail"])

    def test_managed_top_level_symlink_escape_is_detected_and_not_followed(self) -> None:
        with tempfile.TemporaryDirectory(prefix="mal-runtime-audit-symlink-", dir="/tmp") as td:
            root = Path(td)
            project_root = root / "repo"
            workspace_root = root / "workspace"
            runtime_root = project_root / ".MAL-Updater"
            outside_root = root / "outside-cache"
            project_root.mkdir(parents=True)
            workspace_root.mkdir(parents=True)
            outside_root.mkdir()
            (outside_root / "escaped-cache-file.txt").write_text("outside\n", encoding="utf-8")
            _create_expected_top_level(runtime_root, skip={"cache"})
            os.symlink(outside_root, runtime_root / "cache")

            rc, stdout, stderr = _run_cli(project_root, workspace_root, runtime_root, "runtime-retention-audit")

            self.assertEqual(0, rc, stderr)
            payload = json.loads(stdout)
            self.assertIn("managed_top_level_symlink_escape", _issue_codes(payload))
            cache_family = payload["families"]["cache"]
            self.assertEqual(0, cache_family["file_count"])
            self.assertEqual(1, cache_family["scan_error_count"])
            self.assertEqual("root_outside_runtime_root_skipped", cache_family["scan_errors"][0]["code"])
            self.assertNotIn("escaped-cache-file.txt", stdout)

    def test_missing_and_non_directory_expected_top_level_paths_are_reported(self) -> None:
        with tempfile.TemporaryDirectory(prefix="mal-runtime-audit-missing-", dir="/tmp") as td:
            root = Path(td)
            project_root = root / "repo"
            workspace_root = root / "workspace"
            runtime_root = project_root / ".MAL-Updater"
            project_root.mkdir(parents=True)
            workspace_root.mkdir(parents=True)
            _create_expected_top_level(runtime_root, skip={"state", "cache"})
            (runtime_root / "state").write_text("not a directory\n", encoding="utf-8")
            config = _load_temp_config(project_root, workspace_root, runtime_root)

            payload = build_runtime_retention_audit_payload(config)

            codes = _issue_codes(payload)
            self.assertIn("expected_top_level_missing", codes)
            self.assertIn("expected_top_level_not_directory", codes)
            self.assertEqual("error", payload["layout"]["status"])

    def test_scan_caps_report_truncation(self) -> None:
        with tempfile.TemporaryDirectory(prefix="mal-runtime-audit-caps-", dir="/tmp") as td:
            root = Path(td)
            project_root = root / "repo"
            workspace_root = root / "workspace"
            runtime_root = project_root / ".MAL-Updater"
            project_root.mkdir(parents=True)
            workspace_root.mkdir(parents=True)
            _create_expected_top_level(runtime_root)
            for index in range(3):
                (runtime_root / "cache" / f"cache-{index}.json").write_text("{}\n", encoding="utf-8")

            rc, stdout, stderr = _run_cli(
                project_root,
                workspace_root,
                runtime_root,
                "runtime-retention-audit",
                "--max-files-per-family",
                "2",
            )

            self.assertEqual(0, rc, stderr)
            payload = json.loads(stdout)
            self.assertTrue(payload["truncated"])
            self.assertEqual(2, payload["families"]["cache"]["file_count"])
            self.assertTrue(payload["families"]["cache"]["truncated"])

    def test_threshold_candidates_are_review_only_and_do_not_fail_strict(self) -> None:
        with tempfile.TemporaryDirectory(prefix="mal-runtime-audit-threshold-", dir="/tmp") as td:
            root = Path(td)
            project_root = root / "repo"
            workspace_root = root / "workspace"
            runtime_root = project_root / ".MAL-Updater"
            project_root.mkdir(parents=True)
            workspace_root.mkdir(parents=True)
            _create_expected_top_level(runtime_root)
            (runtime_root / "cache" / "one.json").write_text("{}\n", encoding="utf-8")
            (runtime_root / "cache" / "two.json").write_text("{}\n", encoding="utf-8")

            rc, stdout, stderr = _run_cli(
                project_root,
                workspace_root,
                runtime_root,
                "runtime-retention-audit",
                "--strict",
                "--warn-file-count",
                "1",
            )

            self.assertEqual(0, rc, stderr)
            payload = json.loads(stdout)
            self.assertEqual("warning", payload["status"])
            self.assertFalse(payload["strict"]["would_fail"])
            candidates = payload["review_candidates"]
            self.assertEqual(["file_count_threshold_exceeded"], [candidate["reason_code"] for candidate in candidates])
            self.assertEqual("cache", candidates[0]["family"])
            self.assertNotIn("command", candidates[0])
            self.assertEqual(POLICY_MARKER, candidates[0]["policy"])

    def test_health_file_count_thresholds_follow_configured_hourly_retention(self) -> None:
        with tempfile.TemporaryDirectory(prefix="mal-runtime-audit-health-retention-", dir="/tmp") as td:
            root = Path(td)
            project_root = root / "repo"
            workspace_root = root / "workspace"
            runtime_root = project_root / ".MAL-Updater"
            project_root.mkdir(parents=True)
            workspace_root.mkdir(parents=True)
            _create_expected_top_level(runtime_root)
            config = _load_temp_config(project_root, workspace_root, runtime_root)
            config.service.health_every_seconds = 60 * 60
            config.service.health_history_retention_days = 90
            config.service.health_history_min_count = 168
            config.service.service_log_retained_generations = 5

            payload = build_runtime_retention_audit_payload(config)

            snapshots = payload["families"]["health_snapshots"]
            logs = payload["families"]["state_logs"]
            self.assertEqual(2163, snapshots["warning_thresholds"]["file_count"])
            self.assertEqual(2168, logs["warning_thresholds"]["file_count"])

    def test_explicit_file_count_override_still_applies_to_health_families(self) -> None:
        with tempfile.TemporaryDirectory(prefix="mal-runtime-audit-health-override-", dir="/tmp") as td:
            root = Path(td)
            project_root = root / "repo"
            workspace_root = root / "workspace"
            runtime_root = project_root / ".MAL-Updater"
            project_root.mkdir(parents=True)
            workspace_root.mkdir(parents=True)
            _create_expected_top_level(runtime_root)
            config = _load_temp_config(project_root, workspace_root, runtime_root)

            payload = build_runtime_retention_audit_payload(
                config,
                AuditOptions(warning_threshold_overrides=WarningThresholds(file_count=7)),
            )

            self.assertEqual(7, payload["families"]["health_snapshots"]["warning_thresholds"]["file_count"])
            self.assertEqual(7, payload["families"]["state_logs"]["warning_thresholds"]["file_count"])

    def test_summary_output_is_stable(self) -> None:
        with tempfile.TemporaryDirectory(prefix="mal-runtime-audit-summary-", dir="/tmp") as td:
            root = Path(td)
            project_root = root / "repo"
            workspace_root = root / "workspace"
            runtime_root = project_root / ".MAL-Updater"
            project_root.mkdir(parents=True)
            workspace_root.mkdir(parents=True)
            _create_expected_top_level(runtime_root)

            rc1, stdout1, stderr1 = _run_cli(project_root, workspace_root, runtime_root, "runtime-retention-audit", "--format", "summary")
            rc2, stdout2, stderr2 = _run_cli(project_root, workspace_root, runtime_root, "runtime-retention-audit", "--format", "summary")

            self.assertEqual(0, rc1, stderr1)
            self.assertEqual(0, rc2, stderr2)
            self.assertEqual(stdout1, stdout2)
            self.assertIn(f"policy={POLICY_MARKER}", stdout1)
            self.assertIn("family_db_backups_files=0", stdout1)
            self.assertIn("review_candidate_count=0", stdout1)

    def test_audit_does_not_mutate_runtime_tree(self) -> None:
        with tempfile.TemporaryDirectory(prefix="mal-runtime-audit-no-mutate-", dir="/tmp") as td:
            root = Path(td)
            project_root = root / "repo"
            workspace_root = root / "workspace"
            runtime_root = project_root / ".MAL-Updater"
            project_root.mkdir(parents=True)
            workspace_root.mkdir(parents=True)
            _create_expected_top_level(runtime_root)
            (runtime_root / "cache" / "cache.json").write_text("{}\n", encoding="utf-8")
            before = _runtime_tree(runtime_root)

            rc, stdout, stderr = _run_cli(project_root, workspace_root, runtime_root, "runtime-retention-audit", "--warn-file-count", "0")

            self.assertEqual(0, rc, stderr)
            self.assertEqual(before, _runtime_tree(runtime_root))
            payload = json.loads(stdout)
            self.assertTrue(payload["review_candidates"])
            for candidate in payload["review_candidates"]:
                self.assertNotIn("command", candidate)

    def test_secret_filenames_and_contents_are_not_disclosed(self) -> None:
        with tempfile.TemporaryDirectory(prefix="mal-runtime-audit-secret-", dir="/tmp") as td:
            root = Path(td)
            project_root = root / "repo"
            workspace_root = root / "workspace"
            runtime_root = project_root / ".MAL-Updater"
            project_root.mkdir(parents=True)
            workspace_root.mkdir(parents=True)
            _create_expected_top_level(runtime_root)
            secret_filename = "very_private_refresh_token_filename.txt"
            secret_content = "super-secret-token-value"
            (runtime_root / "secrets" / secret_filename).write_text(secret_content, encoding="utf-8")

            rc, stdout, stderr = _run_cli(project_root, workspace_root, runtime_root, "runtime-retention-audit")
            config = _load_temp_config(project_root, workspace_root, runtime_root)
            summary = render_runtime_retention_audit_summary(build_runtime_retention_audit_payload(config))

            self.assertEqual(0, rc, stderr)
            self.assertNotIn(secret_filename, stdout)
            self.assertNotIn(secret_content, stdout)
            self.assertNotIn(secret_filename, summary)
            self.assertNotIn(secret_content, summary)

    def test_direct_options_allow_bounded_payload_construction(self) -> None:
        with tempfile.TemporaryDirectory(prefix="mal-runtime-audit-direct-", dir="/tmp") as td:
            root = Path(td)
            project_root = root / "repo"
            workspace_root = root / "workspace"
            runtime_root = project_root / ".MAL-Updater"
            project_root.mkdir(parents=True)
            workspace_root.mkdir(parents=True)
            _create_expected_top_level(runtime_root)
            config = _load_temp_config(project_root, workspace_root, runtime_root)

            payload = build_runtime_retention_audit_payload(
                config,
                AuditOptions(
                    caps=AuditCaps(max_files_per_family=1, max_dirs_per_family=1, max_depth=0, max_scan_errors_per_family=0),
                    warning_threshold_overrides=WarningThresholds(file_count=0, total_bytes=0, oldest_days=0.0),
                ),
            )

            self.assertEqual(1, payload["scan_policy"]["max_files_per_family"])
            self.assertFalse(payload["scan_policy"]["follow_symlinks"])
            self.assertEqual(POLICY_MARKER, payload["policy"])


if __name__ == "__main__":
    unittest.main()
