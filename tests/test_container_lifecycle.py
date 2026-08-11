from __future__ import annotations

import io
import os
import sqlite3
import tarfile
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from mal_updater.config import ensure_directories, load_config
from mal_updater.container_lifecycle import about, backup, inspect, main, restore, support
from mal_updater.db import bootstrap_database

ROOT = Path(__file__).resolve().parents[1]


class LifecycleTests(unittest.TestCase):
    def setUp(self) -> None:
        self.t = tempfile.TemporaryDirectory(dir="/tmp")
        self.base = Path(self.t.name)
        self.runtime = self.base / "runtime"
        self.tiny_tmp = self.base / "tiny-tmp"
        self.tiny_tmp.mkdir()
        self.env = patch.dict(
            os.environ,
            {
                "MAL_UPDATER_RUNTIME_ROOT": str(self.runtime),
                "MAL_UPDATER_SETTINGS_PATH": str(self.runtime / "config/settings.toml"),
                "TMPDIR": str(self.tiny_tmp),
            },
            clear=False,
        )
        self.env.start()
        self.config = load_config(ROOT)
        ensure_directories(self.config)
        bootstrap_database(self.config.db_path)
        (self.config.secrets_dir / "mal_client_id.txt").write_text("fake-secret")

    def tearDown(self) -> None:
        self.env.stop()
        self.t.cleanup()

    def test_backup_verify_restore_and_support_redaction(self) -> None:
        archive = self.runtime / "backups/backup.tar.gz"
        archive.parent.mkdir(parents=True)
        (archive.parent / "older.tar.gz").write_bytes(b"must not recurse")
        backup(ROOT, archive)
        report = inspect(archive, True)
        self.assertTrue(report["valid"])
        self.assertTrue(any(item["path"].endswith("mal_client_id.txt") for item in report["manifest"]["files"]))
        self.assertFalse(any("backups/" in item["path"] for item in report["manifest"]["files"]))
        self.assertTrue(restore(ROOT, archive, dry_run=True)["dry_run"])
        (self.config.secrets_dir / "mal_client_id.txt").write_text("changed")
        output = restore(ROOT, archive, yes=True)
        pre_restore = Path(output["pre_restore_backup"])
        self.assertTrue(pre_restore.is_file())
        self.assertTrue(inspect(pre_restore, True)["valid"])
        self.assertEqual("fake-secret", (self.config.secrets_dir / "mal_client_id.txt").read_text())
        support_archive = self.runtime / "state/support/support.tar.gz"
        support(ROOT, support_archive)
        with tarfile.open(support_archive) as tf:
            extracted = tf.extractfile("mal-updater-support/diagnostics.json")
            assert extracted is not None
            data = extracted.read().decode()
        self.assertNotIn("fake-secret", data)

    def test_backup_uses_destination_volume_not_tmpdir_for_large_database(self) -> None:
        with sqlite3.connect(self.config.db_path) as conn:
            conn.execute("CREATE TABLE backup_size_probe(payload BLOB NOT NULL)")
            conn.execute("INSERT INTO backup_size_probe VALUES (randomblob(8388608))")
        archive = self.runtime / "state/backups/large.tar.gz"

        real_temporary_directory = tempfile.TemporaryDirectory

        def guarded_temporary_directory(*args, **kwargs):  # type: ignore[no-untyped-def]
            directory = Path(kwargs.get("dir") or tempfile.gettempdir())
            self.assertNotEqual(self.tiny_tmp, directory)
            return real_temporary_directory(*args, **kwargs)

        with patch("mal_updater.container_lifecycle.tempfile.TemporaryDirectory", side_effect=guarded_temporary_directory):
            backup(ROOT, archive)
            report = inspect(archive, True)

        self.assertTrue(report["valid"])
        db_item = next(item for item in report["manifest"]["files"] if item["path"].endswith(self.config.db_path.name))
        self.assertGreater(db_item["size"], 8 * 1024 * 1024)
        self.assertEqual([], list(self.tiny_tmp.iterdir()))

    def test_backup_failure_removes_partial_archive_and_preserves_existing_destination(self) -> None:
        archive = self.runtime / "backups/atomic.tar.gz"
        archive.parent.mkdir(parents=True)
        archive.write_bytes(b"previous archive")
        with patch("mal_updater.container_lifecycle.tarfile.open", side_effect=OSError("simulated archive failure")):
            with self.assertRaisesRegex(OSError, "simulated archive failure"):
                backup(ROOT, archive)
        self.assertEqual(b"previous archive", archive.read_bytes())
        self.assertEqual([], list(archive.parent.glob(f".{archive.name}.*.tmp")))
        self.assertEqual([], list(archive.parent.glob(".mal-backup-*")))

    def test_production_backup_destination_must_be_under_data(self) -> None:
        with patch.object(self.config, "runtime_root", Path("/data")):
            with patch("mal_updater.container_lifecycle._runtime", return_value=self.config):
                with self.assertRaisesRegex(ValueError, "must be under /data"):
                    backup(ROOT, self.base / "outside.tar.gz")

    def test_backup_rejects_insufficient_destination_space_before_staging(self) -> None:
        archive = self.runtime / "backups/no-space.tar.gz"
        usage = SimpleNamespace(free=0)
        with patch("mal_updater.container_lifecycle.shutil.disk_usage", return_value=usage):
            with self.assertRaisesRegex(OSError, "insufficient backup destination space"):
                backup(ROOT, archive)
        self.assertFalse(archive.exists())

    def test_rejects_traversal_symlink_and_undeclared_payload(self) -> None:
        bad = self.base / "bad.tar.gz"
        with tarfile.open(bad, "w:gz") as tf:
            info = tarfile.TarInfo("../escape")
            info.size = 1
            tf.addfile(info, io.BytesIO(b"x"))
        with self.assertRaises(ValueError):
            inspect(bad, True)
        link = self.config.state_dir / "bad-link"
        link.symlink_to("/etc/passwd")
        with self.assertRaises(ValueError):
            backup(ROOT, self.base / "link.tar.gz")

    def test_about_and_version_cli(self) -> None:
        self.assertEqual("MAL-Updater", about()["product"])
        self.assertEqual(0, main(["--project-root", str(ROOT), "version"]))

    def test_admin_reset_command_is_removed(self) -> None:
        with self.assertRaises(SystemExit):
            main(["--project-root", str(ROOT), "admin-reset", "--yes"])


if __name__ == "__main__":
    unittest.main()
