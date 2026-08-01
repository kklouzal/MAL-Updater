from __future__ import annotations

from pathlib import Path
import stat
import tempfile
import unittest
from unittest.mock import patch

from mal_updater.persistence import PersistentJsonError, PersistentWriteError, atomic_write_text, read_json_bounded, read_json_dict_bounded


class PersistenceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)
        self.root = Path(self.temp_dir.name)

    def test_json_decode_error_is_content_safe_and_suppresses_context(self) -> None:
        sentinel = "SENTINEL-json-error-credential-123456789"
        path = self.root / "service-state.json"
        path.write_text(f'{{"refresh_token":"{sentinel}",', encoding="utf-8")

        with self.assertRaises(PersistentJsonError) as raised:
            read_json_dict_bounded(path)

        message = str(raised.exception)
        self.assertIn("type=JSONDecodeError", message)
        self.assertIn("file=service-state.json", message)
        self.assertIn("line=", message)
        self.assertIn("column=", message)
        self.assertIn("position=", message)
        self.assertNotIn(sentinel, message)
        self.assertTrue(raised.exception.__suppress_context__)

    def test_unicode_and_os_errors_are_content_and_path_safe(self) -> None:
        sentinel = "SENTINEL-unicode-credential-123456789"
        bad_utf8 = self.root / "bad.json"
        bad_utf8.write_bytes(b"\xff" + sentinel.encode("utf-8"))

        with self.assertRaises(PersistentJsonError) as unicode_error:
            read_json_bounded(bad_utf8)
        unicode_message = str(unicode_error.exception)
        self.assertIn("type=UnicodeDecodeError", unicode_message)
        self.assertIn("file=bad.json", unicode_message)
        self.assertIn("start=", unicode_message)
        self.assertIn("end=", unicode_message)
        self.assertNotIn(sentinel, unicode_message)
        self.assertTrue(unicode_error.exception.__suppress_context__)

        with self.assertRaises(PersistentJsonError) as os_error:
            read_json_bounded(self.root)
        os_message = str(os_error.exception)
        self.assertIn("type=IsADirectoryError", os_message)
        self.assertIn("operation=read", os_message)
        self.assertIn(f"file={self.root.name}", os_message)
        self.assertIn("errno=", os_message)
        self.assertNotIn(str(self.root.parent), os_message)
        self.assertTrue(os_error.exception.__suppress_context__)

    def test_atomic_write_caps_permissive_modes_and_preserves_restrictive_modes(self) -> None:
        permissive = self.root / "permissive.json"
        permissive.write_text("old", encoding="utf-8")
        permissive.chmod(0o644)
        atomic_write_text(permissive, "new")
        self.assertEqual("new", permissive.read_text(encoding="utf-8"))
        self.assertEqual(0o600, stat.S_IMODE(permissive.stat().st_mode))

        restrictive = self.root / "restrictive.json"
        restrictive.write_text("old", encoding="utf-8")
        restrictive.chmod(0o400)
        atomic_write_text(restrictive, "new")
        self.assertEqual("new", restrictive.read_text(encoding="utf-8"))
        self.assertEqual(0o400, stat.S_IMODE(restrictive.stat().st_mode))

    def test_atomic_write_creates_missing_parents_conservatively(self) -> None:
        path = self.root / "new" / "nested" / "state.json"
        atomic_write_text(path, "{}")

        self.assertEqual("{}", path.read_text(encoding="utf-8"))
        self.assertEqual(0, stat.S_IMODE((self.root / "new").stat().st_mode) & 0o077)
        self.assertEqual(0, stat.S_IMODE((self.root / "new" / "nested").stat().st_mode) & 0o077)
        self.assertEqual(0o600, stat.S_IMODE(path.stat().st_mode))

    def test_atomic_replace_failure_preserves_original_and_cleans_temp_file(self) -> None:
        sentinel = "SENTINEL-replace-credential-123456789"
        path = self.root / "state.json"
        path.write_text("original", encoding="utf-8")

        with patch("mal_updater.persistence.os.replace", side_effect=OSError(5, f"raw {sentinel}")):
            with self.assertRaises(PersistentWriteError) as raised:
                atomic_write_text(path, "replacement")

        message = str(raised.exception)
        self.assertIn("type=OSError", message)
        self.assertIn("operation=atomic_write", message)
        self.assertIn("file=state.json", message)
        self.assertIn("errno=5", message)
        self.assertNotIn(sentinel, message)
        self.assertEqual("original", path.read_text(encoding="utf-8"))
        self.assertEqual([], list(path.parent.glob(f".{path.name}.*.tmp")))


if __name__ == "__main__":
    unittest.main()
