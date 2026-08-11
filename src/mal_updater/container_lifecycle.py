"""Offline, container-native lifecycle tooling."""
from __future__ import annotations
import argparse, hashlib, io, json, os, shutil, sqlite3, sys, tarfile, tempfile, time
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path, PurePosixPath
from typing import Any
from .config import ensure_directories, load_config

FORMAT = 1
MAX_ARCHIVE = 4 * 1024**3
MAX_MEMBERS = 10000
MAX_EXPANDED = 16 * 1024**3
MANAGED = ("config", "secrets", "state", "data")
_WORK_PREFIXES = (".mal-backup-", ".mal-verify-", ".mal-restore-")
_FREE_SPACE_MARGIN = 64 * 1024**2

def _sha(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for block in iter(lambda: f.read(1024 * 1024), b""): h.update(block)
    return h.hexdigest()

def _runtime(root: Path | None):
    c = load_config(root); ensure_directories(c); return c

def _safe_destination(dest: Path) -> Path:
    dest = dest.absolute()
    if dest.exists() and (dest.is_symlink() or not dest.is_file()): raise ValueError("destination must be a regular file path")
    dest.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    if dest.parent.is_symlink(): raise ValueError("destination parent may not be a symlink")
    return dest

def _inside(path: Path, parent: Path) -> bool:
    try:
        path.absolute().relative_to(parent.absolute())
    except ValueError:
        return False
    return True

def _excluded(path: Path, exclusions: tuple[Path, ...]) -> bool:
    return any(path.absolute() == excluded.absolute() or _inside(path, excluded) for excluded in exclusions)

def _copy_tree(source: Path, target: Path, *, exclusions: tuple[Path, ...] = ()) -> None:
    if source.is_symlink(): raise ValueError(f"managed path is a symlink: {source.name}")
    target.mkdir(parents=True, mode=0o700)
    for current_raw, dirs, files in os.walk(source, topdown=True, followlinks=False):
        current = Path(current_raw)
        retained_dirs = []
        for name in dirs:
            item = current / name
            if item.is_symlink(): raise ValueError(f"backup refuses symlink: {item.relative_to(source)}")
            if _excluded(item, exclusions) or name.startswith(_WORK_PREFIXES): continue
            retained_dirs.append(name)
            (target / item.relative_to(source)).mkdir(exist_ok=True, mode=0o700)
        dirs[:] = retained_dirs
        for name in files:
            item = current / name
            if item.is_symlink(): raise ValueError(f"backup refuses symlink: {item.relative_to(source)}")
            if _excluded(item, exclusions) or name.endswith((".lock", ".tmp")): continue
            out = target / item.relative_to(source)
            out.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
            shutil.copyfile(item, out)
            os.chmod(out, item.stat().st_mode & 0o700 or 0o600)

def _source_size(c: Any, exclusions: tuple[Path, ...]) -> int:
    total = c.db_path.stat().st_size if c.db_path.exists() else 0
    for source in (c.config_dir, c.secrets_dir, c.state_dir):
        if not source.exists(): continue
        for current_raw, dirs, files in os.walk(source, topdown=True, followlinks=False):
            current = Path(current_raw)
            dirs[:] = [name for name in dirs if not _excluded(current / name, exclusions) and not name.startswith(_WORK_PREFIXES)]
            for name in files:
                item = current / name
                if not _excluded(item, exclusions) and not name.endswith((".lock", ".tmp")) and not item.is_symlink():
                    total += item.stat().st_size
    return total

def _require_backup_space(c: Any, parent: Path, exclusions: tuple[Path, ...]) -> None:
    # The destination volume temporarily holds an online DB copy plus the archive.
    payload = _source_size(c, exclusions)
    required = payload * 2 + max(_FREE_SPACE_MARGIN, payload // 10)
    available = shutil.disk_usage(parent).free
    if available < required:
        raise OSError(f"insufficient backup destination space: need at least {required} bytes, have {available} bytes")

def backup(root: Path | None, dest: Path, reason: str = "manual") -> Path:
    c = _runtime(root); dest = _safe_destination(dest)
    if c.runtime_root == Path("/data") and not _inside(dest, c.runtime_root):
        raise ValueError("production backup destination must be under /data")
    destination_exclusion = dest.parent if _inside(dest, c.runtime_root) and dest.parent != c.runtime_root else dest
    base_exclusions = (destination_exclusion,)
    _require_backup_space(c, dest.parent, base_exclusions)
    with tempfile.TemporaryDirectory(prefix=".mal-backup-", dir=dest.parent) as raw:
        work = Path(raw); stage = work / "payload"; stage.mkdir(mode=0o700); files = []
        exclusions = (destination_exclusion, work)
        db = stage / "data" / c.db_path.name; db.parent.mkdir(parents=True, mode=0o700)
        if c.db_path.exists():
            if c.db_path.is_symlink(): raise ValueError("database may not be a symlink")
            with sqlite3.connect(f"file:{c.db_path}?mode=ro", uri=True) as src, sqlite3.connect(db) as out: src.backup(out)
            os.chmod(db, 0o600)
        for name, source in (("config", c.config_dir), ("secrets", c.secrets_dir), ("state", c.state_dir)):
            if source.exists(): _copy_tree(source, stage / name, exclusions=exclusions)
        for p in sorted(stage.rglob("*")):
            if p.is_file(): files.append({"path": p.relative_to(stage).as_posix(), "size": p.stat().st_size, "sha256": _sha(p), "mode": p.stat().st_mode & 0o777})
        manifest = {"format": FORMAT, "created_at": int(time.time()), "reason": reason, "files": files}
        manifest_path = stage / "manifest.json"; manifest_path.write_text(json.dumps(manifest, indent=2) + "\n"); os.chmod(manifest_path, 0o600)
        fd, tmp_raw = tempfile.mkstemp(prefix="." + dest.name + ".", suffix=".tmp", dir=dest.parent); os.close(fd); tmp = Path(tmp_raw)
        try:
            with tarfile.open(tmp, "w:gz") as tf: tf.add(stage, arcname="mal-updater-backup", recursive=True)
            os.chmod(tmp, 0o600); os.replace(tmp, dest)
        finally: tmp.unlink(missing_ok=True)
    return dest

def _members(tf: tarfile.TarFile) -> list[tarfile.TarInfo]:
    members = tf.getmembers()
    if len(members) > MAX_MEMBERS: raise ValueError("too many backup members")
    total = 0
    for member in members:
        path = PurePosixPath(member.name)
        total += max(0, member.size)
        if total > MAX_EXPANDED: raise ValueError("backup expands beyond limit")
        if not member.name or path.is_absolute() or ".." in path.parts or path.parts[0] != "mal-updater-backup": raise ValueError("unsafe backup member")
        if member.issym() or member.islnk() or member.isdev() or member.isfifo() or not (member.isfile() or member.isdir()): raise ValueError("unsupported backup member")
    return members

def _extract(path: Path, dest: Path) -> Path:
    if not path.is_file() or path.is_symlink() or path.stat().st_size > MAX_ARCHIVE: raise ValueError("invalid backup archive")
    with tarfile.open(path, "r:gz") as tf:
        _members(tf); tf.extractall(dest, filter="data")
    return dest / "mal-updater-backup"

def inspect(path: Path, verify: bool = False) -> dict[str, Any]:
    path = path.absolute()
    with tempfile.TemporaryDirectory(prefix=".mal-verify-", dir=path.parent) as raw:
        base = _extract(path, Path(raw)); manifest_path = base / "manifest.json"
        manifest = json.loads(manifest_path.read_text())
        if manifest.get("format") != FORMAT or not isinstance(manifest.get("files"), list): raise ValueError("unsupported backup format")
        declared: set[str] = set(); errors = []
        for item in manifest["files"]:
            if not isinstance(item, dict) or set(item) - {"path", "size", "sha256", "mode"}: raise ValueError("invalid manifest item")
            rel = PurePosixPath(str(item.get("path", "")))
            if rel.is_absolute() or ".." in rel.parts or not rel.parts or rel.parts[0] not in MANAGED or str(rel) in declared: raise ValueError("invalid manifest path")
            declared.add(str(rel)); p = base.joinpath(*rel.parts)
            if verify and (not p.is_file() or p.stat().st_size != item.get("size") or _sha(p) != item.get("sha256")): errors.append(str(rel))
        actual = {p.relative_to(base).as_posix() for p in base.rglob("*") if p.is_file() and p.name != "manifest.json"}
        if verify and actual != declared: errors.extend(sorted(actual ^ declared))
        return {"valid": not errors, "verified": verify, "manifest": manifest, "errors": sorted(set(errors)), "archive_sha256": _sha(path)}

def restore(root: Path | None, archive: Path, yes: bool = False, dry_run: bool = False) -> dict[str, Any]:
    report = inspect(archive, True)
    if not report["valid"]: raise ValueError("backup verification failed")
    c = _runtime(root)
    if c.runtime_root.is_symlink(): raise ValueError("runtime root may not be a symlink")
    if dry_run: return {"dry_run": True, "valid": True, "target": str(c.runtime_root), "files": len(report["manifest"]["files"])}
    if not yes: raise ValueError("restore requires --yes (run --dry-run first)")
    pre_dir = c.runtime_root / "backups"; pre = backup(root, pre_dir / f"mal-updater-pre-restore-{int(time.time())}.tar.gz", "pre-restore")
    with tempfile.TemporaryDirectory(prefix=".mal-restore-", dir=c.runtime_root) as raw:
        work = Path(raw); src = _extract(archive, work); incoming = work / "incoming"; incoming.mkdir(mode=0o700)
        manifest = report["manifest"]
        for item in manifest["files"]:
            rel = PurePosixPath(item["path"]); source = src.joinpath(*rel.parts); target = incoming.joinpath(*rel.parts)
            target.parent.mkdir(parents=True, exist_ok=True, mode=0o700); shutil.copyfile(source, target); os.chmod(target, min(int(item.get("mode", 0o600)), 0o700))
        replaced: list[tuple[Path, Path | None]] = []
        try:
            for name in MANAGED:
                target = c.runtime_root / name; replacement = incoming / name
                replacement.mkdir(exist_ok=True, mode=0o700)
                old = c.runtime_root / f".restore-old-{name}-{os.getpid()}" if target.exists() else None
                if old is not None: os.replace(target, old)
                try: os.replace(replacement, target)
                except Exception:
                    if old is not None: os.replace(old, target)
                    raise
                replaced.append((target, old))
        except Exception:
            for target, old in reversed(replaced):
                shutil.rmtree(target)
                if old is not None: os.replace(old, target)
            raise
        for _, old in replaced:
            if old is not None: shutil.rmtree(old)
    return {"restored": True, "pre_restore_backup": str(pre)}

def support(root: Path | None, dest: Path) -> Path:
    c = _runtime(root); dest = _safe_destination(dest)
    payload = {"version": about(), "db_exists": c.db_path.exists(), "paths": {n: (c.runtime_root / n).is_dir() for n in ("config", "secrets", "data", "state", "cache")}, "note": "No paths, secret contents, credentials, tokens, usernames, logs, settings values, or database rows are included."}
    fd, tmp_raw = tempfile.mkstemp(prefix="." + dest.name + ".", suffix=".tmp", dir=dest.parent); os.close(fd); tmp = Path(tmp_raw)
    try:
        with tarfile.open(tmp, "w:gz") as tf:
            data = (json.dumps(payload, indent=2) + "\n").encode(); info = tarfile.TarInfo("mal-updater-support/diagnostics.json"); info.size = len(data); info.mtime = int(time.time()); info.mode = 0o600; tf.addfile(info, io.BytesIO(data))
        os.chmod(tmp, 0o600); os.replace(tmp, dest)
    finally: tmp.unlink(missing_ok=True)
    return dest

def about():
    try: v = version("mal-updater")
    except PackageNotFoundError: v = "development"
    return {"product": "MAL-Updater", "version": v, "backup_format": FORMAT, "python": sys.version.split()[0]}

def main(argv=None) -> int:
    p = argparse.ArgumentParser(prog="mal-updater-tools"); p.add_argument("--project-root", type=Path); sub = p.add_subparsers(dest="cmd", required=True)
    b = sub.add_parser("backup"); b.add_argument("archive", type=Path)
    i = sub.add_parser("backup-inspect"); i.add_argument("archive", type=Path); i.add_argument("--verify", action="store_true")
    r = sub.add_parser("restore"); r.add_argument("archive", type=Path); r.add_argument("--dry-run", action="store_true"); r.add_argument("--yes", action="store_true")
    s = sub.add_parser("support-bundle"); s.add_argument("archive", type=Path); sub.add_parser("version"); args = p.parse_args(argv)
    if args.cmd == "backup": out = {"archive": str(backup(args.project_root, args.archive))}
    elif args.cmd == "backup-inspect": out = inspect(args.archive, args.verify)
    elif args.cmd == "restore": out = restore(args.project_root, args.archive, args.yes, args.dry_run)
    elif args.cmd == "support-bundle": out = {"archive": str(support(args.project_root, args.archive))}
    else: out = about()
    print(json.dumps(out, indent=2, sort_keys=True)); return 0
if __name__ == "__main__": raise SystemExit(main())
