from __future__ import annotations

import os
import re
import stat
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .config import AppConfig

_HEALTH_HISTORY_NAME = re.compile(r"^health-check-(\d{8}T\d{6}Z)\.json$")
_HEALTH_LOG_NAME = re.compile(r"^health-check-(\d{8}T\d{6}Z)\.log$")
_LATEST_HEALTH_NAME = "latest-health-check.json"


@dataclass(frozen=True, slots=True)
class HousekeepingReport:
    values: dict[str, Any]

    def as_dict(self) -> dict[str, Any]:
        return dict(self.values)


def _service_log_generations(path: Path) -> tuple[list[tuple[int, Path, int]], dict[str, Any] | None]:
    pattern = re.compile(rf"^{re.escape(path.name)}\.(\d+)$")
    generations: list[tuple[int, Path, int]] = []
    try:
        entries = list(os.scandir(path.parent))
    except FileNotFoundError:
        return [], None
    except OSError as exc:
        return [], {"reason": "service_log_directory_scan_failed", "error_type": type(exc).__name__}
    for entry in entries:
        match = pattern.fullmatch(entry.name)
        if match is None:
            continue
        try:
            entry_stat = entry.stat(follow_symlinks=False)
        except OSError as exc:
            return [], {"reason": "service_log_generation_stat_failed", "error_type": type(exc).__name__}
        if entry.is_symlink() or not stat.S_ISREG(entry_stat.st_mode):
            return [], {"reason": "unsafe_service_log_generation"}
        generation = int(match.group(1))
        if generation < 1:
            return [], {"reason": "unsafe_service_log_generation"}
        generations.append((generation, Path(entry.path), max(0, int(entry_stat.st_size))))
    return sorted(generations), None


def _now_iso(epoch: float) -> str:
    return datetime.fromtimestamp(epoch, timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def prune_health_history(config: AppConfig, *, now: float | None = None) -> HousekeepingReport:
    current = datetime.now(timezone.utc).timestamp() if now is None else float(now)
    health_dir = config.health_latest_json_path.parent
    retention_days = max(1, int(config.service.health_history_retention_days))
    min_count = max(1, int(config.service.health_history_min_count))
    batch_size = max(1, int(config.service.health_history_prune_batch_size))
    base: dict[str, Any] = {
        "label": "health_history_retention",
        "attempted_at": _now_iso(current),
        "retention_days": retention_days,
        "min_count": min_count,
        "batch_size": batch_size,
        "scanned_count": 0,
        "scanned_bytes": 0,
        "eligible_count": 0,
        "eligible_bytes": 0,
        "deleted_count": 0,
        "deleted_bytes": 0,
        "remaining_count": 0,
        "remaining_bytes": 0,
        "snapshot_deleted_count": 0,
        "snapshot_remaining_count": 0,
        "log_deleted_count": 0,
        "log_remaining_count": 0,
        "latest_preserved": True,
    }
    try:
        directory_stat = os.lstat(health_dir)
    except FileNotFoundError:
        entries: list[os.DirEntry[str]] = []
    except OSError as exc:
        return HousekeepingReport({**base, "status": "blocked", "reason": "health_directory_stat_failed", "error_type": type(exc).__name__})
    else:
        if stat.S_ISLNK(directory_stat.st_mode) or not stat.S_ISDIR(directory_stat.st_mode):
            return HousekeepingReport({**base, "status": "blocked", "reason": "unsafe_health_directory"})
        try:
            entries = list(os.scandir(health_dir))
        except OSError as exc:
            return HousekeepingReport({**base, "status": "blocked", "reason": "health_directory_scan_failed", "error_type": type(exc).__name__})

    snapshot_candidates: list[tuple[float, str, Path, int]] = []
    for entry in entries:
        if entry.name == _LATEST_HEALTH_NAME:
            try:
                latest_stat = entry.stat(follow_symlinks=False)
            except OSError as exc:
                return HousekeepingReport({**base, "status": "blocked", "reason": "latest_health_stat_failed", "error_type": type(exc).__name__})
            if entry.is_symlink() or not stat.S_ISREG(latest_stat.st_mode):
                return HousekeepingReport({**base, "status": "blocked", "reason": "unsafe_latest_health_artifact"})
            continue
        match = _HEALTH_HISTORY_NAME.fullmatch(entry.name)
        if match is None:
            return HousekeepingReport({**base, "status": "blocked", "reason": "unsafe_health_history_name", "unsafe_entry_count": 1})
        try:
            entry_stat = entry.stat(follow_symlinks=False)
        except OSError as exc:
            return HousekeepingReport({**base, "status": "blocked", "reason": "health_history_stat_failed", "error_type": type(exc).__name__})
        if entry.is_symlink() or not stat.S_ISREG(entry_stat.st_mode):
            return HousekeepingReport({**base, "status": "blocked", "reason": "unsafe_health_history_entry"})
        try:
            timestamp = datetime.strptime(match.group(1), "%Y%m%dT%H%M%SZ").replace(tzinfo=timezone.utc).timestamp()
        except ValueError:
            return HousekeepingReport({**base, "status": "blocked", "reason": "unsafe_health_history_timestamp"})
        snapshot_candidates.append((timestamp, entry.name, Path(entry.path), max(0, int(entry_stat.st_size))))

    log_dir = config.state_dir / "logs"
    try:
        log_directory_stat = os.lstat(log_dir)
    except FileNotFoundError:
        log_entries: list[os.DirEntry[str]] = []
    except OSError as exc:
        return HousekeepingReport({**base, "status": "blocked", "reason": "health_log_directory_stat_failed", "error_type": type(exc).__name__})
    else:
        if stat.S_ISLNK(log_directory_stat.st_mode) or not stat.S_ISDIR(log_directory_stat.st_mode):
            return HousekeepingReport({**base, "status": "blocked", "reason": "unsafe_health_log_directory"})
        try:
            log_entries = list(os.scandir(log_dir))
        except OSError as exc:
            return HousekeepingReport({**base, "status": "blocked", "reason": "health_log_directory_scan_failed", "error_type": type(exc).__name__})

    log_candidates: list[tuple[float, str, Path, int]] = []
    for entry in log_entries:
        match = _HEALTH_LOG_NAME.fullmatch(entry.name)
        if match is None:
            continue
        try:
            entry_stat = entry.stat(follow_symlinks=False)
        except OSError as exc:
            return HousekeepingReport({**base, "status": "blocked", "reason": "health_log_stat_failed", "error_type": type(exc).__name__})
        if entry.is_symlink() or not stat.S_ISREG(entry_stat.st_mode):
            return HousekeepingReport({**base, "status": "blocked", "reason": "unsafe_health_log_entry"})
        try:
            timestamp = datetime.strptime(match.group(1), "%Y%m%dT%H%M%SZ").replace(tzinfo=timezone.utc).timestamp()
        except ValueError:
            return HousekeepingReport({**base, "status": "blocked", "reason": "unsafe_health_log_timestamp"})
        log_candidates.append((timestamp, entry.name, Path(entry.path), max(0, int(entry_stat.st_size))))

    snapshot_candidates.sort(key=lambda item: (item[0], item[1]), reverse=True)
    log_candidates.sort(key=lambda item: (item[0], item[1]), reverse=True)
    candidates = [*snapshot_candidates, *log_candidates]
    base["scanned_count"] = len(candidates)
    base["scanned_bytes"] = sum(item[3] for item in candidates)
    cutoff = current - retention_days * 86_400
    snapshot_protected_names = {item[1] for item in snapshot_candidates[:min_count]}
    log_protected_names = {item[1] for item in log_candidates[:min_count]}
    snapshot_eligible = [item for item in reversed(snapshot_candidates) if item[0] < cutoff and item[1] not in snapshot_protected_names]
    log_eligible = [item for item in reversed(log_candidates) if item[0] < cutoff and item[1] not in log_protected_names]
    # Preserve the configured per-run deletion bound while allowing both
    # families to make progress when an old snapshot and log backlog coexist.
    eligible: list[tuple[float, str, Path, int]] = []
    for index in range(max(len(snapshot_eligible), len(log_eligible))):
        if index < len(snapshot_eligible):
            eligible.append(snapshot_eligible[index])
        if index < len(log_eligible):
            eligible.append(log_eligible[index])
    base["eligible_count"] = len(eligible)
    base["eligible_bytes"] = sum(item[3] for item in eligible)
    deleted: list[tuple[float, str, Path, int]] = []
    snapshot_deleted: list[tuple[float, str, Path, int]] = []
    log_deleted: list[tuple[float, str, Path, int]] = []
    for item in eligible[:batch_size]:
        is_snapshot = item in snapshot_eligible
        family = "snapshot" if is_snapshot else "log"
        try:
            item[2].unlink()
        except OSError as exc:
            remaining = [candidate for candidate in candidates if candidate not in deleted]
            return HousekeepingReport({
                **base,
                "status": "blocked",
                "reason": f"health_{family}_delete_failed",
                "error_type": type(exc).__name__,
                "deleted_count": len(deleted),
                "deleted_bytes": sum(candidate[3] for candidate in deleted),
                "snapshot_deleted_count": len(snapshot_deleted),
                "log_deleted_count": len(log_deleted),
                "remaining_count": len(remaining),
                "remaining_bytes": sum(candidate[3] for candidate in remaining),
            })
        deleted.append(item)
        (snapshot_deleted if is_snapshot else log_deleted).append(item)
    remaining = [candidate for candidate in candidates if candidate not in deleted]
    return HousekeepingReport({
        **base,
        "status": "pruned" if deleted else "no_change",
        "deleted_count": len(deleted),
        "deleted_bytes": sum(item[3] for item in deleted),
        "remaining_count": len(remaining),
        "remaining_bytes": sum(item[3] for item in remaining),
        "snapshot_deleted_count": len(snapshot_deleted),
        "snapshot_remaining_count": len(snapshot_candidates) - len(snapshot_deleted),
        "snapshot_remaining_eligible_count": max(0, len(snapshot_eligible) - len(snapshot_deleted)),
        "log_deleted_count": len(log_deleted),
        "log_remaining_count": len(log_candidates) - len(log_deleted),
        "log_remaining_eligible_count": max(0, len(log_eligible) - len(log_deleted)),
        "remaining_eligible_count": max(0, len(eligible) - len(deleted)),
    })


def rotate_service_log(config: AppConfig, *, incoming_bytes: int = 0, now: float | None = None) -> HousekeepingReport:
    current = datetime.now(timezone.utc).timestamp() if now is None else float(now)
    path = config.service_log_path
    max_bytes = max(1, int(config.service.service_log_max_bytes))
    retained_generations = max(1, int(config.service.service_log_retained_generations))
    base: dict[str, Any] = {
        "label": "service_log_retention",
        "attempted_at": _now_iso(current),
        "max_bytes": max_bytes,
        "retained_generations": retained_generations,
        "incoming_bytes": max(0, int(incoming_bytes)),
        "rotated": False,
        "deleted_generations": 0,
        "current_bytes_before": 0,
        "current_bytes_after": 0,
        "generation_count": 0,
        "generation_bytes": 0,
    }
    generations, generation_error = _service_log_generations(path)
    if generation_error is not None:
        return HousekeepingReport({**base, "status": "blocked", **generation_error})
    for candidate in [path]:
        try:
            candidate_stat = os.lstat(candidate)
        except FileNotFoundError:
            continue
        except OSError as exc:
            return HousekeepingReport({**base, "status": "blocked", "reason": "service_log_stat_failed", "error_type": type(exc).__name__})
        if stat.S_ISLNK(candidate_stat.st_mode) or not stat.S_ISREG(candidate_stat.st_mode):
            return HousekeepingReport({**base, "status": "blocked", "reason": "unsafe_service_log_path"})
        if candidate == path:
            base["current_bytes_before"] = max(0, int(candidate_stat.st_size))
    if int(base["current_bytes_before"]) + max(0, int(incoming_bytes)) <= max_bytes:
        base["generation_count"] = len(generations)
        base["generation_bytes"] = sum(item[2] for item in generations)
        base["current_bytes_after"] = int(base["current_bytes_before"])
        return HousekeepingReport({**base, "status": "no_change"})
    if not path.exists():
        return HousekeepingReport({**base, "status": "no_change", "reason": "service_log_missing"})
    try:
        for generation, generation_path, _size in reversed(generations):
            if generation >= retained_generations:
                generation_path.unlink()
                base["deleted_generations"] = int(base["deleted_generations"]) + 1
        for index in range(retained_generations - 1, 0, -1):
            source = path.with_name(f"{path.name}.{index}")
            destination = path.with_name(f"{path.name}.{index + 1}")
            if source.exists():
                os.replace(source, destination)
        os.replace(path, path.with_name(f"{path.name}.1"))
    except OSError as exc:
        return HousekeepingReport({**base, "status": "blocked", "reason": "service_log_rotation_failed", "error_type": type(exc).__name__})
    generations, generation_error = _service_log_generations(path)
    if generation_error is not None:
        return HousekeepingReport({**base, "status": "blocked", **generation_error})
    base.update({
        "status": "rotated",
        "rotated": True,
        "current_bytes_after": 0,
        "generation_count": len(generations),
        "generation_bytes": sum(item[2] for item in generations),
    })
    return HousekeepingReport(base)


def inspect_service_log(config: AppConfig, *, now: float | None = None) -> HousekeepingReport:
    current = datetime.now(timezone.utc).timestamp() if now is None else float(now)
    path = config.service_log_path
    retained_generations = max(1, int(config.service.service_log_retained_generations))
    payload: dict[str, Any] = {
        "label": "service_log_retention",
        "attempted_at": _now_iso(current),
        "status": "ok",
        "max_bytes": max(1, int(config.service.service_log_max_bytes)),
        "retained_generations": retained_generations,
        "current_bytes": 0,
        "generation_count": 0,
        "generation_bytes": 0,
        "rotation_due": False,
    }
    for candidate in [path]:
        try:
            candidate_stat = os.lstat(candidate)
        except FileNotFoundError:
            continue
        except OSError as exc:
            return HousekeepingReport({**payload, "status": "blocked", "reason": "service_log_stat_failed", "error_type": type(exc).__name__})
        if stat.S_ISLNK(candidate_stat.st_mode) or not stat.S_ISREG(candidate_stat.st_mode):
            return HousekeepingReport({**payload, "status": "blocked", "reason": "unsafe_service_log_path"})
        payload["current_bytes"] = max(0, int(candidate_stat.st_size))
    generations, generation_error = _service_log_generations(path)
    if generation_error is not None:
        return HousekeepingReport({**payload, "status": "blocked", **generation_error})
    payload["generation_count"] = len(generations)
    payload["generation_bytes"] = sum(item[2] for item in generations)
    payload["rotation_due"] = int(payload["current_bytes"]) >= int(payload["max_bytes"])
    return HousekeepingReport(payload)
