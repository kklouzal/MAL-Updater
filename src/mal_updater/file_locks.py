"""Advisory whole-file locks that work on POSIX and Windows.

POSIX uses ``flock``. Windows has no ``flock``; ``LockFileEx`` over the whole
file gives the same shared/exclusive and non-blocking semantics. Each lock
belongs to one open file object; another open of the same file, in this process
or another, conflicts with it. The lock files guarded here are never read or
written, so Windows' mandatory byte-range enforcement does not matter.

A non-blocking attempt on a busy lock raises ``BlockingIOError`` on every
platform.
"""

from __future__ import annotations

import sys
from typing import IO, Any

if sys.platform == "win32":
    import ctypes
    import errno
    import msvcrt
    from ctypes import wintypes

    class _Overlapped(ctypes.Structure):
        _fields_ = [
            ("Internal", ctypes.c_void_p),
            ("InternalHigh", ctypes.c_void_p),
            ("Offset", wintypes.DWORD),
            ("OffsetHigh", wintypes.DWORD),
            ("hEvent", wintypes.HANDLE),
        ]

    _kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    _LockFileEx = _kernel32.LockFileEx
    _LockFileEx.argtypes = [wintypes.HANDLE, wintypes.DWORD, wintypes.DWORD, wintypes.DWORD, wintypes.DWORD, ctypes.POINTER(_Overlapped)]
    _LockFileEx.restype = wintypes.BOOL
    _UnlockFileEx = _kernel32.UnlockFileEx
    _UnlockFileEx.argtypes = [wintypes.HANDLE, wintypes.DWORD, wintypes.DWORD, wintypes.DWORD, ctypes.POINTER(_Overlapped)]
    _UnlockFileEx.restype = wintypes.BOOL
    _LOCKFILE_FAIL_IMMEDIATELY = 0x1
    _LOCKFILE_EXCLUSIVE_LOCK = 0x2
    _ERROR_LOCK_VIOLATION = 33
    _WHOLE_FILE = 0xFFFFFFFF

    def lock_file(handle: IO[Any], *, exclusive: bool = True, blocking: bool = True) -> None:
        flags = (_LOCKFILE_EXCLUSIVE_LOCK if exclusive else 0) | (0 if blocking else _LOCKFILE_FAIL_IMMEDIATELY)
        overlapped = _Overlapped()
        if not _LockFileEx(msvcrt.get_osfhandle(handle.fileno()), flags, 0, _WHOLE_FILE, _WHOLE_FILE, ctypes.byref(overlapped)):
            error = ctypes.get_last_error()
            if error == _ERROR_LOCK_VIOLATION:
                raise BlockingIOError(errno.EAGAIN, "file lock is held by another handle")
            raise ctypes.WinError(error)

    def unlock_file(handle: IO[Any]) -> None:
        overlapped = _Overlapped()
        if not _UnlockFileEx(msvcrt.get_osfhandle(handle.fileno()), 0, _WHOLE_FILE, _WHOLE_FILE, ctypes.byref(overlapped)):
            raise ctypes.WinError(ctypes.get_last_error())

    def lock_inheritance_fds(handle: IO[Any]) -> tuple[int, ...]:
        """Windows cannot hand a lock to a child; the caller keeps holding it while the child runs."""
        del handle
        return ()

else:
    import fcntl

    def lock_file(handle: IO[Any], *, exclusive: bool = True, blocking: bool = True) -> None:
        flags = fcntl.LOCK_EX if exclusive else fcntl.LOCK_SH
        if not blocking:
            flags |= fcntl.LOCK_NB
        fcntl.flock(handle.fileno(), flags)

    def unlock_file(handle: IO[Any]) -> None:
        fcntl.flock(handle.fileno(), fcntl.LOCK_UN)

    def lock_inheritance_fds(handle: IO[Any]) -> tuple[int, ...]:
        """The descriptor to pass to a child so the lock outlives a crashed caller."""
        return (handle.fileno(),)
