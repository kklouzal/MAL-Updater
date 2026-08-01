from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
import json
import os
from pathlib import Path
import stat
from typing import Any, BinaryIO, Iterator
import uuid


DEFAULT_JSON_MAX_BYTES = 8 * 1024 * 1024
DEFAULT_JSONL_MAX_LINE_BYTES = 256 * 1024


class PersistentJsonError(Exception):
    """Safe, content-free JSON persistence read error."""

    def __init__(self, message: str) -> None:
        super().__init__(message)
        self.safe_message = message


class PersistentWriteError(Exception):
    """Safe, content-free persistence write/replace error."""

    def __init__(self, message: str) -> None:
        super().__init__(message)
        self.safe_message = message


@dataclass(frozen=True, slots=True)
class JsonLine:
    line_number: int
    value: Any | None = None
    error: str | None = None


def _safe_path_label(path: Path) -> str:
    return Path(path).name or "persistent file"


def _safe_os_error_message(path: Path, exc: OSError, *, operation: str) -> str:
    errno = exc.errno if isinstance(getattr(exc, "errno", None), int) else "unknown"
    return f"type={type(exc).__name__} operation={operation} file={_safe_path_label(path)} errno={errno}"


def _safe_unicode_error_message(path: Path, exc: UnicodeDecodeError) -> str:
    return f"type=UnicodeDecodeError file={_safe_path_label(path)} start={exc.start} end={exc.end}"


def _safe_json_error_message(path: Path, exc: json.JSONDecodeError) -> str:
    return f"type=JSONDecodeError file={_safe_path_label(path)} line={exc.lineno} column={exc.colno} position={exc.pos}"


def _decode_json_bytes(path: Path, data: bytes) -> Any:
    try:
        text = data.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise PersistentJsonError(_safe_unicode_error_message(path, exc)) from None
    try:
        return json.loads(text)
    except json.JSONDecodeError as exc:
        raise PersistentJsonError(_safe_json_error_message(path, exc)) from None


def read_json_bounded(path: Path, *, max_bytes: int = DEFAULT_JSON_MAX_BYTES) -> Any:
    """Read one JSON file with an explicit byte ceiling and safe errors."""

    path = Path(path)
    limit = max(1, int(max_bytes))
    try:
        with path.open("rb") as fh:
            data = fh.read(limit + 1)
    except OSError as exc:
        raise PersistentJsonError(_safe_os_error_message(path, exc, operation="read")) from None
    if len(data) > limit:
        raise PersistentJsonError(f"type=PersistentFileTooLarge file={_safe_path_label(path)} max_bytes={limit}") from None
    return _decode_json_bytes(path, data)


def read_json_dict_bounded(path: Path, *, max_bytes: int = DEFAULT_JSON_MAX_BYTES) -> dict[str, Any] | None:
    """Return a dict JSON payload, None when the file is absent, or raise a safe error."""

    path = Path(path)
    if not path.exists():
        return None
    payload = read_json_bounded(path, max_bytes=max_bytes)
    if not isinstance(payload, dict):
        raise PersistentJsonError(f"type=UnexpectedJsonType file={_safe_path_label(path)} expected=object") from None
    return payload


def _requested_file_mode(mode: int) -> int:
    return stat.S_IMODE(int(mode)) & 0o666


def _existing_file_mode(path: Path, *, default_mode: int) -> int:
    requested_mode = _requested_file_mode(default_mode)
    try:
        st = os.lstat(path)
    except FileNotFoundError:
        return requested_mode
    except OSError:
        return requested_mode
    if not stat.S_ISREG(st.st_mode):
        return requested_mode
    return (stat.S_IMODE(st.st_mode) & 0o666) & requested_mode


def _ensure_parent_dir(path: Path) -> None:
    parent = Path(path).parent
    if parent.exists():
        return
    missing: list[Path] = []
    current = parent
    while not current.exists():
        missing.append(current)
        if current.parent == current:
            break
        current = current.parent
    for directory in reversed(missing):
        try:
            os.mkdir(str(directory), 0o700)
        except FileExistsError:
            continue


def _fsync_parent(path: Path) -> None:
    try:
        dir_fd = os.open(str(path.parent), os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    except OSError:
        return
    try:
        os.fsync(dir_fd)
    finally:
        os.close(dir_fd)


def atomic_write_bytes(path: Path, data: bytes, *, mode: int = 0o600) -> None:
    """Write bytes by fsyncing a same-directory temp file then replacing the target."""

    path = Path(path)
    _ensure_parent_dir(path)
    target_mode = _existing_file_mode(path, default_mode=mode)
    tmp_path = path.with_name(f".{path.name}.{os.getpid()}.{uuid.uuid4().hex}.tmp")
    fd: int | None = None
    try:
        fd = os.open(str(tmp_path), os.O_WRONLY | os.O_CREAT | os.O_EXCL, target_mode)
        with os.fdopen(fd, "wb") as fh:
            fd = None
            os.fchmod(fh.fileno(), target_mode)
            fh.write(data)
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp_path, path)
        _fsync_parent(path)
    except OSError as exc:
        if fd is not None:
            try:
                os.close(fd)
            except OSError:
                pass
        try:
            tmp_path.unlink()
        except FileNotFoundError:
            pass
        raise PersistentWriteError(_safe_os_error_message(path, exc, operation="atomic_write")) from None
    except Exception:
        if fd is not None:
            try:
                os.close(fd)
            except OSError:
                pass
        try:
            tmp_path.unlink()
        except FileNotFoundError:
            pass
        raise


@contextmanager
def atomic_writer(path: Path, *, mode: int = 0o600) -> Iterator[BinaryIO]:
    """Yield a binary temp writer that fsyncs and atomically replaces path on success."""

    path = Path(path)
    _ensure_parent_dir(path)
    target_mode = _existing_file_mode(path, default_mode=mode)
    tmp_path = path.with_name(f".{path.name}.{os.getpid()}.{uuid.uuid4().hex}.tmp")
    fd: int | None = None
    committed = False
    try:
        fd = os.open(str(tmp_path), os.O_WRONLY | os.O_CREAT | os.O_EXCL, target_mode)
        with os.fdopen(fd, "wb") as fh:
            fd = None
            os.fchmod(fh.fileno(), target_mode)
            yield fh
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp_path, path)
        committed = True
        _fsync_parent(path)
    except OSError as exc:
        raise PersistentWriteError(_safe_os_error_message(path, exc, operation="atomic_write")) from None
    finally:
        if fd is not None:
            try:
                os.close(fd)
            except OSError:
                pass
        if not committed:
            try:
                tmp_path.unlink()
            except FileNotFoundError:
                pass


def atomic_write_text(path: Path, text: str, *, mode: int = 0o600, encoding: str = "utf-8") -> None:
    atomic_write_bytes(path, text.encode(encoding), mode=mode)


def atomic_write_json(path: Path, payload: Any, *, mode: int = 0o600, trailing_newline: bool = True, **dumps_kwargs: Any) -> None:
    text = json.dumps(payload, **dumps_kwargs)
    if trailing_newline:
        text += "\n"
    atomic_write_text(path, text, mode=mode)


def _discard_overlong_line(fh: Any, *, max_line_bytes: int) -> None:
    while True:
        chunk = fh.readline(max_line_bytes + 1)
        if not chunk or chunk.endswith(b"\n"):
            return


def iter_json_lines(path: Path, *, max_line_bytes: int = DEFAULT_JSONL_MAX_LINE_BYTES) -> Iterator[JsonLine]:
    """Stream JSONL records with bounded per-line reads and content-free diagnostics."""

    path = Path(path)
    limit = max(1, int(max_line_bytes))
    line_number = 0
    with path.open("rb") as fh:
        while True:
            raw = fh.readline(limit + 1)
            if not raw:
                return
            line_number += 1
            if len(raw) > limit:
                _discard_overlong_line(fh, max_line_bytes=limit)
                yield JsonLine(line_number=line_number, error=f"type=JsonLineTooLong line={line_number} max_bytes={limit}")
                continue
            stripped = raw.strip()
            if not stripped:
                continue
            try:
                text = stripped.decode("utf-8")
            except UnicodeDecodeError as exc:
                yield JsonLine(line_number=line_number, error=_safe_unicode_error_message(path, exc))
                continue
            try:
                yield JsonLine(line_number=line_number, value=json.loads(text))
            except json.JSONDecodeError as exc:
                yield JsonLine(line_number=line_number, error=_safe_json_error_message(path, exc))
