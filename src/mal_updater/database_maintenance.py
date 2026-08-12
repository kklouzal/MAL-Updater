from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import fcntl
import os
import shutil
import sqlite3
import time
from pathlib import Path
from typing import Any

from .config import AppConfig
from .container_lifecycle import backup as create_backup, inspect as inspect_backup
from .redaction import sanitize_text


@dataclass(frozen=True, slots=True)
class DatabaseCompactionReport:
    status: str
    reason: str | None
    attempted_at: str
    db_path: str
    db_size_before: int
    db_size_after: int
    page_size: int
    page_count: int
    freelist_count: int
    freelist_bytes: int
    freelist_ratio: float
    min_freelist_bytes: int
    min_freelist_ratio: float
    min_interval_seconds: int
    last_success_at: str | None = None
    bytes_reclaimed: int = 0
    backup_archive: str | None = None
    backup_archive_sha256: str | None = None
    backup_file_count: int = 0
    last_success_epoch: float | None = None
    required_free_bytes: int = 0
    available_free_bytes: int = 0
    initial_available_free_bytes: int = 0
    post_backup_available_free_bytes: int = 0
    error_type: str | None = None

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


def _now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def database_lock_path(db_path: Path) -> Path:
    return db_path.parent / f".{db_path.name}.repo-writers.lock"


def acquire_database_lock(db_path: Path, *, exclusive: bool, blocking: bool = True):
    lock_path = database_lock_path(db_path)
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    handle = lock_path.open("a+", encoding="utf-8")
    flags = fcntl.LOCK_EX if exclusive else fcntl.LOCK_SH
    if not blocking:
        flags |= fcntl.LOCK_NB
    try:
        fcntl.flock(handle.fileno(), flags)
    except BaseException:
        handle.close()
        raise
    return handle


def release_database_lock(handle: Any) -> None:
    try:
        fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
    finally:
        handle.close()


def _read_stats(db_path: Path) -> tuple[int, int, int, int, int, float]:
    size = db_path.stat().st_size if db_path.exists() else 0
    with sqlite3.connect(f"file:{db_path}?mode=ro", uri=True) as conn:
        page_size = int(conn.execute("PRAGMA page_size").fetchone()[0])
        page_count = int(conn.execute("PRAGMA page_count").fetchone()[0])
        freelist_count = int(conn.execute("PRAGMA freelist_count").fetchone()[0])
    freelist_bytes = max(0, page_size * freelist_count)
    ratio = float(freelist_count / page_count) if page_count > 0 else 0.0
    return size, page_size, page_count, freelist_count, freelist_bytes, ratio


def _base_report(config: AppConfig, *, status: str, reason: str | None, attempted_at: str, stats: tuple[int, int, int, int, int, float], previous: dict[str, Any] | None = None, **extra: Any) -> DatabaseCompactionReport:
    size, page_size, page_count, freelist_count, freelist_bytes, ratio = stats
    last_success_at = previous.get("last_success_at") if isinstance(previous, dict) and isinstance(previous.get("last_success_at"), str) else None
    return DatabaseCompactionReport(
        status=status,
        reason=reason,
        attempted_at=attempted_at,
        db_path=str(config.db_path),
        db_size_before=size,
        db_size_after=int(extra.pop("db_size_after", size)),
        page_size=page_size,
        page_count=page_count,
        freelist_count=freelist_count,
        freelist_bytes=freelist_bytes,
        freelist_ratio=round(ratio, 6),
        min_freelist_bytes=int(config.service.db_compaction_min_freelist_bytes),
        min_freelist_ratio=float(config.service.db_compaction_min_freelist_ratio),
        min_interval_seconds=int(config.service.db_compaction_min_interval_seconds),
        last_success_at=last_success_at,
        bytes_reclaimed=int(extra.pop("bytes_reclaimed", 0)),
        backup_archive=extra.pop("backup_archive", None),
        backup_archive_sha256=extra.pop("backup_archive_sha256", None),
        backup_file_count=int(extra.pop("backup_file_count", 0)),
        last_success_epoch=extra.pop("last_success_epoch", None),
        required_free_bytes=int(extra.pop("required_free_bytes", 0)),
        available_free_bytes=int(extra.pop("available_free_bytes", 0)),
        initial_available_free_bytes=int(extra.pop("initial_available_free_bytes", 0)),
        post_backup_available_free_bytes=int(extra.pop("post_backup_available_free_bytes", 0)),
        error_type=extra.pop("error_type", None),
    )


def _previous_success_too_recent(previous: dict[str, Any] | None, now: float, min_interval: int) -> bool:
    if min_interval <= 0 or not isinstance(previous, dict):
        return False
    epoch = previous.get("last_success_epoch")
    return isinstance(epoch, (int, float)) and now - float(epoch) < min_interval


def compact_database_if_due(config: AppConfig, *, previous: dict[str, Any] | None = None, now: float | None = None) -> DatabaseCompactionReport:
    current = time.time() if now is None else float(now)
    attempted_at = datetime.fromtimestamp(current, tz=timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    if not config.db_path.exists():
        return _base_report(config, status="skipped", reason="database_missing", attempted_at=attempted_at, stats=(0, 0, 0, 0, 0, 0.0), previous=previous)
    if config.db_path.is_symlink():
        return _base_report(config, status="blocked", reason="database_symlink", attempted_at=attempted_at, stats=(0, 0, 0, 0, 0, 0.0), previous=previous)
    try:
        lock = acquire_database_lock(config.db_path, exclusive=True, blocking=False)
    except BlockingIOError:
        stats = _read_stats(config.db_path)
        return _base_report(config, status="skipped", reason="database_writer_lease_busy", attempted_at=attempted_at, stats=stats, previous=previous)
    try:
        stats = _read_stats(config.db_path)
        size, _page_size, _page_count, _freelist_count, freelist_bytes, ratio = stats
        if _previous_success_too_recent(previous, current, int(config.service.db_compaction_min_interval_seconds)):
            return _base_report(config, status="skipped", reason="min_interval_not_elapsed", attempted_at=attempted_at, stats=stats, previous=previous)
        if freelist_bytes < int(config.service.db_compaction_min_freelist_bytes):
            return _base_report(config, status="skipped", reason="freelist_bytes_below_threshold", attempted_at=attempted_at, stats=stats, previous=previous)
        if ratio < float(config.service.db_compaction_min_freelist_ratio):
            return _base_report(config, status="skipped", reason="freelist_ratio_below_threshold", attempted_at=attempted_at, stats=stats, previous=previous)
        required = size + int(config.service.db_compaction_free_space_margin_bytes)
        available = shutil.disk_usage(config.db_path.parent).free
        if available < required:
            return _base_report(config, status="blocked", reason="insufficient_database_volume_space", attempted_at=attempted_at, stats=stats, previous=previous, required_free_bytes=required, available_free_bytes=available, initial_available_free_bytes=available)
        # Use the canonical backup directory itself. container_lifecycle.backup
        # excludes its destination parent from runtime-state traversal, so this
        # prevents a new archive from embedding prior backup inventory.
        backup_dir = config.state_dir / "backups"
        backup_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
        archive = backup_dir / f"mal-updater-pre-vacuum-{int(current)}.tar.gz"
        try:
            created = create_backup(config.project_root, archive, reason="pre-sqlite-compaction")
            verified = inspect_backup(created, verify=True)
        except Exception as exc:  # fail closed before VACUUM
            return _base_report(config, status="blocked", reason="backup_verify_failed", attempted_at=attempted_at, stats=stats, previous=previous, required_free_bytes=required, available_free_bytes=available, initial_available_free_bytes=available, error_type=sanitize_text(type(exc).__name__, max_length=100))
        if not verified.get("valid") or not verified.get("verified"):
            return _base_report(config, status="blocked", reason="backup_verify_failed", attempted_at=attempted_at, stats=stats, previous=previous, backup_archive=str(created), backup_archive_sha256=str(verified.get("archive_sha256") or ""), required_free_bytes=required, available_free_bytes=available, initial_available_free_bytes=available)
        # The verified archive is intentionally retained, so re-check the DB
        # volume after backup creation before allowing SQLite's rewrite.
        post_backup_available = shutil.disk_usage(config.db_path.parent).free
        if post_backup_available < required:
            return _base_report(config, status="blocked", reason="insufficient_database_volume_space_after_backup", attempted_at=attempted_at, stats=stats, previous=previous, backup_archive=str(created), backup_archive_sha256=str(verified.get("archive_sha256") or ""), backup_file_count=len(verified.get("manifest", {}).get("files", [])) if isinstance(verified.get("manifest"), dict) else 0, required_free_bytes=required, available_free_bytes=post_backup_available, initial_available_free_bytes=available, post_backup_available_free_bytes=post_backup_available)
        with sqlite3.connect(config.db_path) as conn:
            conn.execute("PRAGMA foreign_keys = ON")
            conn.execute("VACUUM")
        after_size = config.db_path.stat().st_size
        return _base_report(config, status="compacted", reason=None, attempted_at=attempted_at, stats=stats, previous=previous, db_size_after=after_size, bytes_reclaimed=max(0, size - after_size), backup_archive=str(created), backup_archive_sha256=str(verified.get("archive_sha256") or ""), backup_file_count=len(verified.get("manifest", {}).get("files", [])) if isinstance(verified.get("manifest"), dict) else 0, last_success_epoch=current, required_free_bytes=required, available_free_bytes=post_backup_available, initial_available_free_bytes=available, post_backup_available_free_bytes=post_backup_available)
    finally:
        release_database_lock(lock)
