from __future__ import annotations

import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from mal_updater.file_locks import lock_file, lock_inheritance_fds, unlock_file

_SRC = str(Path(__file__).resolve().parents[1] / "src")
_TRY_LOCK = """
import sys
from mal_updater.file_locks import lock_file
with open(sys.argv[1], "a+", encoding="utf-8") as handle:
    try:
        lock_file(handle, exclusive=sys.argv[2] == "x", blocking=False)
    except BlockingIOError:
        raise SystemExit(3)
"""


class FileLockTests(unittest.TestCase):
    """Runs natively on Linux, macOS and Windows (see the desktop workflow)."""

    def setUp(self) -> None:
        temp = tempfile.TemporaryDirectory(prefix="mal-file-locks-")
        self.addCleanup(temp.cleanup)
        self.path = Path(temp.name) / "test.lock"

    def _open(self):  # type: ignore[no-untyped-def]
        handle = self.path.open("a+", encoding="utf-8")
        self.addCleanup(handle.close)
        return handle

    def _other_process_can_lock(self, *, exclusive: bool) -> bool:
        env = {**os.environ, "PYTHONPATH": os.pathsep.join(filter(None, [_SRC, os.environ.get("PYTHONPATH")]))}
        result = subprocess.run([sys.executable, "-c", _TRY_LOCK, str(self.path), "x" if exclusive else "s"], env=env, check=False, timeout=60)
        self.assertIn(result.returncode, {0, 3})
        return result.returncode == 0

    def test_exclusive_lock_excludes_other_handles_until_unlocked(self) -> None:
        first, second = self._open(), self._open()
        lock_file(first)
        with self.assertRaises(BlockingIOError):
            lock_file(second, blocking=False)
        with self.assertRaises(BlockingIOError):
            lock_file(second, exclusive=False, blocking=False)
        self.assertFalse(self._other_process_can_lock(exclusive=False))
        unlock_file(first)
        lock_file(second, blocking=False)
        unlock_file(second)
        self.assertTrue(self._other_process_can_lock(exclusive=True))

    def test_shared_locks_coexist_but_exclude_an_exclusive_lock(self) -> None:
        first, second, writer = self._open(), self._open(), self._open()
        lock_file(first, exclusive=False)
        lock_file(second, exclusive=False, blocking=False)
        self.assertTrue(self._other_process_can_lock(exclusive=False))
        with self.assertRaises(BlockingIOError):
            lock_file(writer, blocking=False)
        self.assertFalse(self._other_process_can_lock(exclusive=True))
        unlock_file(first)
        unlock_file(second)
        lock_file(writer, blocking=False)
        unlock_file(writer)

    def test_lock_inheritance_is_posix_only(self) -> None:
        handle = self._open()
        expected = () if os.name == "nt" else (handle.fileno(),)
        self.assertEqual(expected, lock_inheritance_fds(handle))


if __name__ == "__main__":
    unittest.main()
