from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import os
import re
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import Any

from .config import AppConfig
from .container_lifecycle import backup as create_backup, inspect as inspect_backup
from .file_locks import lock_file, lock_inheritance_fds, unlock_file
from .redaction import sanitize_text


# Compaction report fields surfaced in service status and its CLI summary.
COMPACTION_REPORT_STATUS_FIELDS = (
    "db_size_before",
    "db_size_after",
    "freelist_bytes",
    "freelist_ratio",
    "bytes_reclaimed",
    "backup_archive",
    "backup_archive_sha256",
    "required_free_bytes",
    "available_free_bytes",
    "initial_available_free_bytes",
    "post_backup_available_free_bytes",
    "pruned_backup_archives",
    "backup_prune_error_type",
    "error_type",
)


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
    pruned_backup_archives: int = 0
    backup_prune_error_type: str | None = None

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


def database_lock_path(db_path: Path) -> Path:
    return db_path.parent / f".{db_path.name}.repo-writers.lock"


def acquire_database_lock(db_path: Path, *, exclusive: bool, blocking: bool = True):
    lock_path = database_lock_path(db_path)
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    handle = lock_path.open("a+", encoding="utf-8")
    try:
        lock_file(handle, exclusive=exclusive, blocking=blocking)
    except BaseException:
        handle.close()
        raise
    return handle


def release_database_lock(handle: Any) -> None:
    try:
        unlock_file(handle)
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
        pruned_backup_archives=int(extra.pop("pruned_backup_archives", 0)),
        backup_prune_error_type=extra.pop("backup_prune_error_type", None),
    )


def _previous_success_too_recent(previous: dict[str, Any] | None, now: float, min_interval: int) -> bool:
    if min_interval <= 0 or not isinstance(previous, dict):
        return False
    epoch = previous.get("last_success_epoch")
    return isinstance(epoch, (int, float)) and now - float(epoch) < min_interval


PRE_VACUUM_BACKUP_RETENTION_SECONDS = 30 * 24 * 60 * 60
_PRE_VACUUM_ARCHIVE_NAME = re.compile(r"mal-updater-pre-vacuum-(\d+)\.tar\.gz")


def _prune_pre_vacuum_archives(backup_dir: Path, *, keep: Path, now: float) -> tuple[int, str | None]:
    """Delete this task's own pre-vacuum archives older than the retention window.

    Only names this task generates are considered, the archive just written is
    always kept, and age comes from the epoch embedded in the name. It runs
    after a completed VACUUM, so a filesystem error stops pruning and is
    returned for the report instead of masking the compaction outcome.
    """
    removed = 0
    try:
        for entry in backup_dir.iterdir():
            match = _PRE_VACUUM_ARCHIVE_NAME.fullmatch(entry.name)
            if match is None or entry == keep or entry.is_symlink() or not entry.is_file():
                continue
            if now - int(match.group(1)) > PRE_VACUUM_BACKUP_RETENTION_SECONDS:
                entry.unlink()
                removed += 1
    except OSError as exc:
        return removed, sanitize_text(type(exc).__name__, max_length=100)
    return removed, None


_VACUUM_SCRIPT = (
    "import sqlite3, sys\n"
    "conn = sqlite3.connect(sys.argv[1])\n"
    "try:\n"
    "    conn.execute('VACUUM')\n"
    "finally:\n"
    "    conn.close()\n"
)


def _vacuum_on_database_volume(db_path: Path, *, lock_handle: Any) -> None:
    """VACUUM with SQLite's rewrite copy on the database volume.

    VACUUM builds the compacted copy in SQLite's temp directory, which a
    process resolves once from SQLITE_TMPDIR/TMPDIR. A container /tmp is
    often a small tmpfs (64 MB in production), far below the copy of a
    multi-GB database ("database or disk is full"), and the free-space
    precheck measures the database volume. A child process therefore runs the
    VACUUM with its temp directory beside the database (SQLite reads
    SQLITE_TMPDIR/TMPDIR on POSIX and TMP/TEMP on Windows). On POSIX the child
    inherits the caller's repo writer lock descriptor, so the lock stays held
    for as long as the VACUUM runs, even if the caller dies first; Windows
    cannot pass it, so there the caller holds it until the child exits.
    """
    with tempfile.TemporaryDirectory(prefix=".vacuum-tmp-", dir=db_path.parent) as temp_dir:
        env = {**os.environ, "SQLITE_TMPDIR": temp_dir, "TMPDIR": temp_dir, "TMP": temp_dir, "TEMP": temp_dir}
        completed = subprocess.run(
            [sys.executable, "-I", "-c", _VACUUM_SCRIPT, str(db_path)],
            env=env,
            pass_fds=lock_inheritance_fds(lock_handle),
            capture_output=True,
            text=True,
            check=False,
        )
    if completed.returncode != 0:
        detail = (completed.stderr.strip().splitlines() or ["no diagnostics"])[-1]
        raise sqlite3.OperationalError(f"VACUUM failed: {sanitize_text(detail, max_length=300)}")


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
        try:
            _vacuum_on_database_volume(config.db_path, lock_handle=lock)
        except (sqlite3.Error, OSError) as exc:
            # Reported as a task error so the scheduler's failure backoff
            # applies; raising would stop the service loop and repeat the full
            # pre-VACUUM backup on every restart. The verified archive is kept.
            return _base_report(config, status="error", reason="vacuum_failed", attempted_at=attempted_at, stats=stats, previous=previous, backup_archive=str(created), backup_archive_sha256=str(verified.get("archive_sha256") or ""), required_free_bytes=required, available_free_bytes=post_backup_available, initial_available_free_bytes=available, post_backup_available_free_bytes=post_backup_available, error_type=sanitize_text(type(exc).__name__, max_length=100))
        after_size = config.db_path.stat().st_size
        pruned_archives, prune_error_type = _prune_pre_vacuum_archives(backup_dir, keep=Path(created), now=current)
        return _base_report(config, status="compacted", pruned_backup_archives=pruned_archives, backup_prune_error_type=prune_error_type, reason=None, attempted_at=attempted_at, stats=stats, previous=previous, db_size_after=after_size, bytes_reclaimed=max(0, size - after_size), backup_archive=str(created), backup_archive_sha256=str(verified.get("archive_sha256") or ""), backup_file_count=len(verified.get("manifest", {}).get("files", [])) if isinstance(verified.get("manifest"), dict) else 0, last_success_epoch=current, required_free_bytes=required, available_free_bytes=post_backup_available, initial_available_free_bytes=available, post_backup_available_free_bytes=post_backup_available)
    finally:
        release_database_lock(lock)
