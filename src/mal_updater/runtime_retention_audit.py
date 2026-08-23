from __future__ import annotations

import math
import os
import stat
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Protocol, cast

POLICY_MARKER = "diagnostic_only_no_delete_or_prune"
SCHEMA_VERSION = 1
DEFAULT_RUNTIME_DIR_NAME = ".MAL-Updater"

_BYTES_IN_MIB = 1024 * 1024
_BYTES_IN_GIB = 1024 * _BYTES_IN_MIB


class RuntimeAuditConfig(Protocol):
    project_root: Path
    workspace_root: Path
    runtime_root: Path
    config_dir: Path
    secrets_dir: Path
    data_dir: Path
    state_dir: Path
    cache_dir: Path
    service: "RuntimeAuditServiceConfig"

    @property
    def service_log_path(self) -> Path: ...

    @property
    def api_request_events_path(self) -> Path: ...

    @property
    def health_latest_json_path(self) -> Path: ...


class RuntimeAuditServiceConfig(Protocol):
    health_every_seconds: int
    health_history_retention_days: int
    health_history_min_count: int
    service_log_retained_generations: int


@dataclass(frozen=True, slots=True)
class AuditCaps:
    max_files_per_family: int = 10_000
    max_dirs_per_family: int = 2_000
    max_depth: int = 8
    max_scan_errors_per_family: int = 20


@dataclass(frozen=True, slots=True)
class WarningThresholds:
    file_count: int | None = None
    total_bytes: int | None = None
    oldest_days: float | None = None


@dataclass(frozen=True, slots=True)
class AuditOptions:
    caps: AuditCaps = AuditCaps()
    warning_threshold_overrides: WarningThresholds = WarningThresholds()
    strict: bool = False


@dataclass(frozen=True, slots=True)
class FamilyDefinition:
    name: str
    label: str
    root_labels: tuple[str, ...]
    default_thresholds: WarningThresholds
    manual_policy: str
    high_value: bool = False


@dataclass(slots=True)
class ScanSummary:
    file_count: int = 0
    total_bytes: int = 0
    oldest_mtime_epoch: float | None = None
    newest_mtime_epoch: float | None = None
    truncated: bool = False
    skipped_symlink_count: int = 0
    skipped_special_count: int = 0


FAMILY_DEFINITIONS: tuple[FamilyDefinition, ...] = (
    FamilyDefinition(
        name="db_backups",
        label="DB backups",
        root_labels=("data_db_backups", "data_backups", "state_db_backups", "state_backups", "runtime_backups"),
        default_thresholds=WarningThresholds(file_count=20, total_bytes=5 * _BYTES_IN_GIB, oldest_days=365.0),
        manual_policy="high_value_manual_retention_review_only",
        high_value=True,
    ),
    FamilyDefinition(
        name="health_snapshots",
        label="health snapshots",
        root_labels=("state_health",),
        default_thresholds=WarningThresholds(file_count=200, total_bytes=512 * _BYTES_IN_MIB, oldest_days=120.0),
        manual_policy="automatic_bounded_history_retention_plus_manual_review",
    ),
    FamilyDefinition(
        name="state_logs",
        label="state logs",
        root_labels=("state_logs",),
        default_thresholds=WarningThresholds(file_count=50, total_bytes=256 * _BYTES_IN_MIB, oldest_days=45.0),
        manual_policy="automatic_health_history_retention_and_size_rotation_plus_manual_review",
    ),
    FamilyDefinition(
        name="request_events",
        label="request events",
        root_labels=("api_request_events",),
        default_thresholds=WarningThresholds(file_count=5, total_bytes=256 * _BYTES_IN_MIB, oldest_days=45.0),
        manual_policy="disposableish_log_manual_review_only",
    ),
    FamilyDefinition(
        name="tmp",
        label="temporary runtime files",
        root_labels=("runtime_tmp", "state_tmp"),
        default_thresholds=WarningThresholds(file_count=100, total_bytes=1 * _BYTES_IN_GIB, oldest_days=7.0),
        manual_policy="disposableish_runtime_manual_review_only",
    ),
    FamilyDefinition(
        name="cache",
        label="cache",
        root_labels=("cache_dir",),
        default_thresholds=WarningThresholds(file_count=5_000, total_bytes=5 * _BYTES_IN_GIB, oldest_days=365.0),
        manual_policy="disposableish_runtime_manual_review_only",
    ),
    FamilyDefinition(
        name="artifacts",
        label="artifacts",
        root_labels=("runtime_artifacts", "data_artifacts", "state_artifacts"),
        default_thresholds=WarningThresholds(file_count=1_000, total_bytes=5 * _BYTES_IN_GIB, oldest_days=180.0),
        manual_policy="disposableish_runtime_manual_review_only",
    ),
)

_EXPECTED_TOP_LEVELS = ("config", "secrets", "data", "state", "cache")
_DISPOSABLEISH_FAMILIES = frozenset({"state_logs", "request_events", "tmp", "cache", "artifacts"})
_LAYOUT_ERROR_CODES = {
    "managed_top_level_outside_runtime_root",
    "managed_top_level_symlink_escape",
    "nested_runtime_root",
    "repo_source_runtime_overlap",
    "runtime_root_not_directory",
    "runtime_root_is_symlink",
    "runtime_root_symlink_escape",
    "expected_top_level_not_directory",
}


def _candidate_class(*, family: str, high_value: bool) -> str:
    if high_value:
        return "high_value_manual_policy"
    if family in _DISPOSABLEISH_FAMILIES:
        return "disposableish_manual_review"
    return "operational_history_manual_review"


def _is_relative_to(path: Path, base: Path) -> bool:
    try:
        path.relative_to(base)
    except ValueError:
        return False
    return True


def _safe_resolve(path: Path) -> Path | None:
    try:
        return path.resolve(strict=True)
    except OSError:
        return None


def _iso_from_mtime(timestamp: float | None) -> str | None:
    if timestamp is None:
        return None
    return datetime.fromtimestamp(timestamp, timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _first_env_value(*names: str) -> str | None:
    for name in names:
        value = os.getenv(name)
        if value:
            return value
    return None


def _payload_dict(value: object) -> dict[str, object]:
    if isinstance(value, dict):
        return cast("dict[str, object]", value)
    return {}


def _payload_list(value: object) -> list[object]:
    if isinstance(value, list):
        return value
    return []


def _payload_dict_list(value: object) -> list[dict[str, object]]:
    return [_payload_dict(item) for item in _payload_list(value) if isinstance(item, dict)]


def _payload_int(value: object, *, default: int = 0) -> int:
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    return default


def _payload_bool(value: object) -> bool:
    if isinstance(value, bool):
        return value
    return bool(value)


def _raw_runtime_root(config: RuntimeAuditConfig) -> Path:
    env_override = _first_env_value("MAL_UPDATER_RUNTIME_ROOT", "MAL_UPDATER_RUNTIME_DIR")
    if env_override:
        raw = Path(env_override).expanduser()
        if not raw.is_absolute():
            raw = Path.cwd() / raw
        return raw
    return config.workspace_root / DEFAULT_RUNTIME_DIR_NAME


def _top_level_paths(config: RuntimeAuditConfig) -> dict[str, Path]:
    return {
        "config": config.config_dir,
        "secrets": config.secrets_dir,
        "data": config.data_dir,
        "state": config.state_dir,
        "cache": config.cache_dir,
    }


def _family_roots(config: RuntimeAuditConfig) -> dict[str, Path]:
    return {
        "data_db_backups": config.data_dir / "db-backups",
        "data_backups": config.data_dir / "backups",
        "state_db_backups": config.state_dir / "db-backups",
        "state_backups": config.state_dir / "backups",
        "runtime_backups": config.runtime_root / "backups",
        "state_health": config.health_latest_json_path.parent,
        "state_logs": config.service_log_path.parent,
        "api_request_events": config.api_request_events_path,
        "runtime_tmp": config.runtime_root / "tmp",
        "state_tmp": config.state_dir / "tmp",
        "cache_dir": config.cache_dir,
        "runtime_artifacts": config.runtime_root / "artifacts",
        "data_artifacts": config.data_dir / "artifacts",
        "state_artifacts": config.state_dir / "artifacts",
    }


def _path_payload(path: Path, runtime_root: Path) -> dict[str, object]:
    resolved = _safe_resolve(path)
    within_runtime_root = resolved is not None and _is_relative_to(resolved, runtime_root)
    relative_path = None
    if within_runtime_root and resolved is not None:
        relative_path = "." if resolved == runtime_root else resolved.relative_to(runtime_root).as_posix()
    return {
        "exists": path.exists() or path.is_symlink(),
        "is_directory": path.is_dir(),
        "is_symlink": path.is_symlink(),
        "within_runtime_root": within_runtime_root,
        "runtime_relative_path": relative_path,
    }


def _layout_issue(code: str, detail: str, *, label: str | None = None) -> dict[str, object]:
    issue: dict[str, object] = {
        "code": code,
        "severity": "error" if code in _LAYOUT_ERROR_CODES else "warning",
        "detail": detail,
    }
    if label is not None:
        issue["label"] = label
    return issue


def _detect_runtime_root_symlink_escape(raw_runtime_root: Path, runtime_root: Path) -> dict[str, object] | None:
    if not raw_runtime_root.is_symlink():
        return None
    raw_parent = raw_runtime_root.parent.resolve()
    target = _safe_resolve(raw_runtime_root)
    if target is None:
        return _layout_issue("runtime_root_symlink_escape", "runtime root symlink target could not be resolved", label="runtime_root")
    if target != runtime_root:
        return _layout_issue("runtime_root_symlink_escape", "runtime root symlink target does not match the resolved runtime root", label="runtime_root")
    if not _is_relative_to(target, raw_parent):
        return _layout_issue("runtime_root_symlink_escape", "runtime root symlink target escapes its parent directory", label="runtime_root")
    return _layout_issue("runtime_root_is_symlink", "runtime root input path is a symlink", label="runtime_root")


def _detect_repo_source_overlap(config: RuntimeAuditConfig, runtime_root: Path) -> list[dict[str, object]]:
    issues: list[dict[str, object]] = []
    project_root = config.project_root.resolve()
    source_root = (project_root / "src").resolve()
    standard_runtime_root = (project_root / DEFAULT_RUNTIME_DIR_NAME).resolve()
    project_looks_like_repo = any((project_root / marker).exists() for marker in ("pyproject.toml", ".git", "src"))
    if runtime_root == project_root:
        issues.append(_layout_issue("repo_source_runtime_overlap", "runtime root resolves to the project root", label="runtime_root"))
    if _is_relative_to(project_root, runtime_root) and runtime_root != project_root:
        issues.append(_layout_issue("repo_source_runtime_overlap", "runtime root contains the project/source root", label="runtime_root"))
    if runtime_root == source_root or _is_relative_to(runtime_root, source_root):
        issues.append(_layout_issue("repo_source_runtime_overlap", "runtime root is inside the source tree", label="runtime_root"))
    if project_looks_like_repo and _is_relative_to(runtime_root, project_root) and runtime_root != standard_runtime_root:
        detail = f"runtime root is a nonstandard child inside the repository/project tree; use {DEFAULT_RUNTIME_DIR_NAME} or an external runtime root"
        issues.append(_layout_issue("repo_source_runtime_overlap", detail, label="runtime_root"))
    return issues


def _build_layout(config: RuntimeAuditConfig) -> dict[str, object]:
    runtime_root = config.runtime_root.resolve()
    raw_runtime_root = _raw_runtime_root(config)
    expected_paths = _top_level_paths(config)
    issues: list[dict[str, object]] = []

    if not runtime_root.exists():
        issues.append(_layout_issue("runtime_root_missing", "runtime root is missing; audit did not create it", label="runtime_root"))
    elif not runtime_root.is_dir():
        issues.append(_layout_issue("runtime_root_not_directory", "runtime root exists but is not a directory", label="runtime_root"))

    symlink_issue = _detect_runtime_root_symlink_escape(raw_runtime_root, runtime_root)
    if symlink_issue is not None:
        issues.append(symlink_issue)

    nested_runtime_root = runtime_root / DEFAULT_RUNTIME_DIR_NAME
    if nested_runtime_root.exists() or nested_runtime_root.is_symlink():
        issues.append(
            _layout_issue(
                "nested_runtime_root",
                f"nested {DEFAULT_RUNTIME_DIR_NAME} exists inside the configured runtime root",
                label=DEFAULT_RUNTIME_DIR_NAME,
            )
        )

    issues.extend(_detect_repo_source_overlap(config, runtime_root))

    expected_payload: dict[str, object] = {}
    for name in _EXPECTED_TOP_LEVELS:
        path = expected_paths[name]
        resolved = _safe_resolve(path)
        payload = _path_payload(path, runtime_root)
        expected_payload[name] = payload
        conventional_path = runtime_root / name
        if conventional_path.is_symlink() and conventional_path != path:
            conventional_target = _safe_resolve(conventional_path)
            if conventional_target is None or not _is_relative_to(conventional_target, runtime_root):
                issues.append(_layout_issue("managed_top_level_symlink_escape", "managed top-level symlink resolves outside runtime root", label=name))
            else:
                issues.append(_layout_issue("managed_top_level_symlink", "managed top-level path is a symlink and will be treated fail-closed", label=name))
        if path.is_symlink():
            if resolved is None or not _is_relative_to(resolved, runtime_root):
                issues.append(_layout_issue("managed_top_level_symlink_escape", "managed top-level symlink resolves outside runtime root", label=name))
            else:
                issues.append(_layout_issue("managed_top_level_symlink", "managed top-level path is a symlink and will be treated fail-closed", label=name))
            continue
        if not path.exists():
            issues.append(_layout_issue("expected_top_level_missing", "expected managed top-level path is missing", label=name))
            continue
        if not path.is_dir():
            issues.append(_layout_issue("expected_top_level_not_directory", "expected managed top-level path is not a directory", label=name))
            continue
        if resolved is None:
            issues.append(_layout_issue("managed_top_level_outside_runtime_root", "managed top-level path could not be resolved", label=name))
            continue
        if not _is_relative_to(resolved, runtime_root):
            issue_code = "managed_top_level_symlink_escape" if path.is_symlink() else "managed_top_level_outside_runtime_root"
            issues.append(_layout_issue(issue_code, "managed top-level path resolves outside runtime root", label=name))

    status = "ok"
    if any(issue.get("severity") == "error" for issue in issues):
        status = "error"
    elif issues:
        status = "warning"

    return {
        "status": status,
        "runtime_root": _path_payload(runtime_root, runtime_root),
        "expected_top_level": expected_payload,
        "issues": issues,
    }


def _retained_health_history_file_count(config: RuntimeAuditConfig) -> int:
    cadence_seconds = max(1, int(config.service.health_every_seconds))
    retention_seconds = max(1, int(config.service.health_history_retention_days)) * 86_400
    cadence_window_count = math.ceil(retention_seconds / cadence_seconds) + 1
    return max(int(config.service.health_history_min_count), cadence_window_count)


def _merged_thresholds(
    definition: FamilyDefinition,
    overrides: WarningThresholds,
    config: RuntimeAuditConfig,
) -> WarningThresholds:
    defaults = definition.default_thresholds
    default_file_count = defaults.file_count
    retained_health_history_count = _retained_health_history_file_count(config)
    if definition.name == "health_snapshots":
        # Allow the retained timestamped history plus the latest alias. The
        # threshold comparison is inclusive, so warn at one file above that.
        default_file_count = retained_health_history_count + 2
    elif definition.name == "state_logs":
        # Allow the matching retained health logs, active service log, and its
        # configured bounded generations before warning on file count.
        default_file_count = retained_health_history_count + int(config.service.service_log_retained_generations) + 2
    return WarningThresholds(
        file_count=overrides.file_count if overrides.file_count is not None else default_file_count,
        total_bytes=overrides.total_bytes if overrides.total_bytes is not None else defaults.total_bytes,
        oldest_days=overrides.oldest_days if overrides.oldest_days is not None else defaults.oldest_days,
    )


def _threshold_payload(thresholds: WarningThresholds) -> dict[str, object]:
    return {
        "file_count": thresholds.file_count,
        "total_bytes": thresholds.total_bytes,
        "oldest_days": thresholds.oldest_days,
    }


def _add_scan_error(errors: list[dict[str, object]], caps: AuditCaps, *, root_label: str, code: str, error_type: str | None = None) -> None:
    if len(errors) >= caps.max_scan_errors_per_family:
        return
    payload: dict[str, object] = {"root_label": root_label, "code": code}
    if error_type is not None:
        payload["error_type"] = error_type
    errors.append(payload)


def _append_file_stats(summary: ScanSummary, st: os.stat_result) -> None:
    summary.file_count += 1
    summary.total_bytes += max(0, int(st.st_size))
    if summary.oldest_mtime_epoch is None or st.st_mtime < summary.oldest_mtime_epoch:
        summary.oldest_mtime_epoch = st.st_mtime
    if summary.newest_mtime_epoch is None or st.st_mtime > summary.newest_mtime_epoch:
        summary.newest_mtime_epoch = st.st_mtime


def _entry_symlink_escapes(path: Path, scan_root: Path, runtime_root: Path) -> bool:
    target = _safe_resolve(path)
    if target is None:
        return True
    return not (_is_relative_to(target, scan_root) and _is_relative_to(target, runtime_root))


def _scan_directory(
    root: Path,
    *,
    root_label: str,
    runtime_root: Path,
    summary: ScanSummary,
    errors: list[dict[str, object]],
    caps: AuditCaps,
) -> None:
    stack: list[tuple[Path, int]] = [(root, 0)]
    scanned_dirs = 0
    root_resolved = root.resolve()
    while stack:
        if summary.file_count >= caps.max_files_per_family or scanned_dirs >= caps.max_dirs_per_family:
            summary.truncated = True
            return
        current, depth = stack.pop()
        scanned_dirs += 1
        try:
            entries = os.scandir(current)
        except OSError as exc:
            _add_scan_error(errors, caps, root_label=root_label, code="scandir_failed", error_type=type(exc).__name__)
            continue
        with entries:
            for entry in entries:
                if summary.file_count >= caps.max_files_per_family:
                    summary.truncated = True
                    return
                path = Path(entry.path)
                try:
                    if entry.is_symlink():
                        summary.skipped_symlink_count += 1
                        if _entry_symlink_escapes(path, root_resolved, runtime_root):
                            _add_scan_error(errors, caps, root_label=root_label, code="symlink_escape_skipped")
                        continue
                    st = entry.stat(follow_symlinks=False)
                except OSError as exc:
                    _add_scan_error(errors, caps, root_label=root_label, code="stat_failed", error_type=type(exc).__name__)
                    continue
                mode = st.st_mode
                if stat.S_ISDIR(mode):
                    if depth >= caps.max_depth:
                        summary.truncated = True
                        continue
                    stack.append((path, depth + 1))
                elif stat.S_ISREG(mode):
                    _append_file_stats(summary, st)
                else:
                    summary.skipped_special_count += 1


def _scan_root(
    root: Path,
    *,
    root_label: str,
    runtime_root: Path,
    summary: ScanSummary,
    root_payloads: list[dict[str, object]],
    errors: list[dict[str, object]],
    caps: AuditCaps,
) -> None:
    payload = _path_payload(root, runtime_root)
    payload["label"] = root_label
    root_payloads.append(payload)
    if root.is_symlink():
        resolved = _safe_resolve(root)
        payload["scanned"] = False
        if resolved is None or not _is_relative_to(resolved, runtime_root):
            payload["skip_reason"] = "symlink_escape"
            _add_scan_error(errors, caps, root_label=root_label, code="root_symlink_escape_skipped")
        else:
            payload["skip_reason"] = "symlink_skipped"
            _add_scan_error(errors, caps, root_label=root_label, code="root_symlink_skipped")
        return
    if not root.exists():
        payload["scanned"] = False
        payload["skip_reason"] = "missing"
        return
    resolved = _safe_resolve(root)
    if resolved is None or not _is_relative_to(resolved, runtime_root):
        payload["scanned"] = False
        payload["skip_reason"] = "outside_runtime_root"
        _add_scan_error(errors, caps, root_label=root_label, code="root_outside_runtime_root_skipped")
        return
    if root.is_file():
        payload["scanned"] = True
        try:
            _append_file_stats(summary, root.stat())
        except OSError as exc:
            _add_scan_error(errors, caps, root_label=root_label, code="stat_failed", error_type=type(exc).__name__)
        return
    if root.is_dir():
        payload["scanned"] = True
        _scan_directory(root, root_label=root_label, runtime_root=runtime_root, summary=summary, errors=errors, caps=caps)
        return
    payload["scanned"] = False
    payload["skip_reason"] = "not_file_or_directory"
    _add_scan_error(errors, caps, root_label=root_label, code="root_not_file_or_directory_skipped")


def _candidate(
    *,
    family: str,
    reason_code: str,
    observed: int | float,
    threshold: int | float,
    manual_policy: str,
    high_value: bool,
) -> dict[str, object]:
    return {
        "family": family,
        "reason_code": reason_code,
        "severity": "review",
        "observed": observed,
        "threshold": threshold,
        "policy": POLICY_MARKER,
        "manual_policy": manual_policy,
        "high_value": high_value,
        "candidate_class": _candidate_class(family=family, high_value=high_value),
    }


def _family_review_candidates(
    name: str,
    summary: ScanSummary,
    thresholds: WarningThresholds,
    definition: FamilyDefinition,
    *,
    now: datetime,
) -> list[dict[str, object]]:
    candidates: list[dict[str, object]] = []
    file_count = summary.file_count
    total_bytes = summary.total_bytes
    if thresholds.file_count is not None and file_count > 0 and file_count >= thresholds.file_count:
        candidates.append(
            _candidate(
                family=name,
                reason_code="file_count_threshold_exceeded",
                observed=file_count,
                threshold=thresholds.file_count,
                manual_policy=definition.manual_policy,
                high_value=definition.high_value,
            )
        )
    if thresholds.total_bytes is not None and total_bytes > 0 and total_bytes >= thresholds.total_bytes:
        candidates.append(
            _candidate(
                family=name,
                reason_code="total_bytes_threshold_exceeded",
                observed=total_bytes,
                threshold=thresholds.total_bytes,
                manual_policy=definition.manual_policy,
                high_value=definition.high_value,
            )
        )
    oldest_epoch = summary.oldest_mtime_epoch
    if thresholds.oldest_days is not None and oldest_epoch is not None:
        oldest_days = max(0.0, (now.timestamp() - float(oldest_epoch)) / 86_400.0)
        if oldest_days >= thresholds.oldest_days:
            candidates.append(
                _candidate(
                    family=name,
                    reason_code="oldest_mtime_threshold_exceeded",
                    observed=round(oldest_days, 3),
                    threshold=thresholds.oldest_days,
                    manual_policy=definition.manual_policy,
                    high_value=definition.high_value,
                )
            )
    return candidates


def _scan_family(
    definition: FamilyDefinition,
    *,
    config: RuntimeAuditConfig,
    root_paths: dict[str, Path],
    runtime_root: Path,
    options: AuditOptions,
    now: datetime,
) -> dict[str, object]:
    summary = ScanSummary()
    root_payloads: list[dict[str, object]] = []
    errors: list[dict[str, object]] = []
    seen_roots: set[Path] = set()
    for root_label in definition.root_labels:
        root = root_paths[root_label]
        root_key = root.resolve(strict=False)
        if root_key in seen_roots:
            continue
        seen_roots.add(root_key)
        _scan_root(
            root,
            root_label=root_label,
            runtime_root=runtime_root,
            summary=summary,
            root_payloads=root_payloads,
            errors=errors,
            caps=options.caps,
        )

    thresholds = _merged_thresholds(definition, options.warning_threshold_overrides, config)
    review_candidates = _family_review_candidates(definition.name, summary, thresholds, definition, now=now)
    payload: dict[str, object] = {
        "label": definition.label,
        "policy": POLICY_MARKER,
        "manual_policy": definition.manual_policy,
        "high_value": definition.high_value,
        "candidate_class": _candidate_class(family=definition.name, high_value=definition.high_value),
        "roots": root_payloads,
        "file_count": summary.file_count,
        "total_bytes": summary.total_bytes,
        "truncated": summary.truncated,
        "skipped_symlink_count": summary.skipped_symlink_count,
        "skipped_special_count": summary.skipped_special_count,
        "count": summary.file_count,
        "bytes": summary.total_bytes,
        "oldest_mtime": _iso_from_mtime(summary.oldest_mtime_epoch),
        "newest_mtime": _iso_from_mtime(summary.newest_mtime_epoch),
        "warning_thresholds": _threshold_payload(thresholds),
        "review_candidates": review_candidates,
        "scan_errors": errors,
        "scan_error_count": len(errors),
    }
    return payload


def _caps_payload(caps: AuditCaps) -> dict[str, object]:
    return {
        "max_files_per_family": caps.max_files_per_family,
        "max_dirs_per_family": caps.max_dirs_per_family,
        "max_depth": caps.max_depth,
        "max_scan_errors_per_family": caps.max_scan_errors_per_family,
        "follow_symlinks": False,
    }


def _status(layout_status: str, families: dict[str, dict[str, object]], review_candidates: list[dict[str, object]]) -> str:
    if layout_status == "error":
        return "error"
    if layout_status == "warning":
        return "warning"
    if review_candidates:
        return "warning"
    for family in families.values():
        if _payload_bool(family.get("truncated")) or _payload_int(family.get("scan_error_count")):
            return "warning"
    return "ok"


def build_runtime_retention_audit_payload(config: RuntimeAuditConfig, options: AuditOptions | None = None) -> dict[str, object]:
    effective_options = options or AuditOptions()
    runtime_root = config.runtime_root.resolve()
    now = datetime.now(timezone.utc)
    layout = _build_layout(config)
    root_paths = _family_roots(config)
    families = {
        definition.name: _scan_family(
            definition,
            config=config,
            root_paths=root_paths,
            runtime_root=runtime_root,
            options=effective_options,
            now=now,
        )
        for definition in FAMILY_DEFINITIONS
    }
    review_candidates = [
        candidate
        for family_name in sorted(families)
        for candidate in _payload_dict_list(families[family_name].get("review_candidates"))
    ]
    scan_error_count = sum(_payload_int(family.get("scan_error_count")) for family in families.values())
    truncated = any(_payload_bool(family.get("truncated")) for family in families.values())
    inventory_totals = {
        "file_count": sum(_payload_int(family.get("file_count")) for family in families.values()),
        "total_bytes": sum(_payload_int(family.get("total_bytes")) for family in families.values()),
        "scan_error_count": scan_error_count,
        "truncated_family_count": sum(1 for family in families.values() if _payload_bool(family.get("truncated"))),
    }
    strict_failure_codes = sorted(
        str(issue.get("code"))
        for issue in _payload_dict_list(layout.get("issues"))
        if issue.get("severity") == "error" and issue.get("code") is not None
    )
    return {
        "schema_version": SCHEMA_VERSION,
        "command": "runtime-retention-audit",
        "policy": POLICY_MARKER,
        "diagnostic_only_no_delete_or_prune": True,
        "mutation_policy": "read_only_no_archive_delete_prune_chmod_move",
        "status": _status(str(layout["status"]), families, review_candidates),
        "project_root": str(config.project_root),
        "workspace_root": str(config.workspace_root),
        "runtime_root": str(config.runtime_root),
        "runtime_root_input": str(_raw_runtime_root(config)),
        "layout": layout,
        "scan_policy": _caps_payload(effective_options.caps),
        "families": families,
        "retention_inventory": {
            "families": families,
            "totals": inventory_totals,
        },
        "review_candidates": review_candidates,
        "review_candidate_count": len(review_candidates),
        "review_guidance": {
            "diagnostic_only_no_delete_or_prune": True,
            "review_candidates_are_not_commands": True,
            "no_automatic_retention_policy_guess": True,
            "db_backups": "High-value/manual-policy family; verify backup/recovery needs before any separate retention action.",
            "disposableish_families": ["tmp", "cache", "state_logs", "request_events", "artifacts"],
            "health_snapshots": "Timestamped health JSON is automatically age/count bounded; audit retained history before any separate action.",
        },
        "truncated": truncated,
        "scan_error_count": scan_error_count,
        "strict": {
            "enabled": effective_options.strict,
            "would_fail": bool(strict_failure_codes),
            "failure_codes": strict_failure_codes,
        },
    }


def _candidate_summary_values(candidates: Iterable[dict[str, object]]) -> list[str]:
    values = []
    for candidate in candidates:
        family = candidate.get("family")
        reason = candidate.get("reason_code")
        if isinstance(family, str) and isinstance(reason, str):
            values.append(f"{family}:{reason}")
    return sorted(values)


def render_runtime_retention_audit_summary(payload: dict[str, object]) -> str:
    layout = _payload_dict(payload.get("layout"))
    layout_issues = _payload_dict_list(layout.get("issues"))
    layout_codes = sorted(
        str(issue.get("code"))
        for issue in layout_issues
        if issue.get("code") is not None
    )
    families = _payload_dict(payload.get("families"))
    review_candidates = _payload_dict_list(payload.get("review_candidates"))
    lines = [
        f"diagnostic_only_no_delete_or_prune={bool(payload.get('diagnostic_only_no_delete_or_prune'))}",
        f"policy={payload.get('policy')}",
        f"status={payload.get('status')}",
        f"runtime_root={payload.get('runtime_root')}",
        f"runtime_root_input={payload.get('runtime_root_input')}",
        f"layout_status={layout.get('status')}",
        f"layout_issue_count={len(layout_codes)}",
        "layout_issues=" + ",".join(layout_codes),
        f"truncated={bool(payload.get('truncated'))}",
        f"scan_error_count={payload.get('scan_error_count', 0)}",
        f"review_candidate_count={payload.get('review_candidate_count', 0)}",
    ]
    for family_name in sorted(families):
        family = _payload_dict(families.get(family_name))
        candidates = _payload_dict_list(family.get("review_candidates"))
        lines.extend(
            [
                f"family_{family_name}_files={family.get('file_count', 0)}",
                f"family_{family_name}_bytes={family.get('total_bytes', 0)}",
                f"family_{family_name}_oldest_mtime={family.get('oldest_mtime') or ''}",
                f"family_{family_name}_newest_mtime={family.get('newest_mtime') or ''}",
                f"family_{family_name}_truncated={bool(family.get('truncated'))}",
                f"family_{family_name}_scan_errors={family.get('scan_error_count', 0)}",
                f"family_{family_name}_review_candidates={len(candidates)}",
            ]
        )
    for index, value in enumerate(_candidate_summary_values(review_candidates), start=1):
        lines.append(f"review_candidate_{index}={value}")
    return "\n".join(lines) + "\n"
