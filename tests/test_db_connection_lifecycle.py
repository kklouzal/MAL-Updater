from __future__ import annotations

import sqlite3
import tempfile
import unittest
from pathlib import Path

from mal_updater.db import connect


class DbConnectionLifecycleTests(unittest.TestCase):
    def test_context_manager_commits_and_closes_connection(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            db_path = Path(td) / "lifecycle.db"

            with connect(db_path) as conn:
                self.assertIsInstance(conn, sqlite3.Connection)
                conn.execute("CREATE TABLE sample (id INTEGER PRIMARY KEY, name TEXT NOT NULL)")
                conn.execute("INSERT INTO sample(name) VALUES (?)", ("committed",))

            with self.assertRaises(sqlite3.ProgrammingError):
                conn.execute("SELECT 1")

            raw = sqlite3.connect(db_path)
            try:
                row = raw.execute("SELECT name FROM sample").fetchone()
            finally:
                raw.close()
            self.assertEqual(row, ("committed",))

    def test_context_manager_rolls_back_and_closes_connection_on_error(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            db_path = Path(td) / "rollback.db"
            setup = sqlite3.connect(db_path)
            try:
                setup.execute("CREATE TABLE sample (id INTEGER PRIMARY KEY, name TEXT NOT NULL)")
                setup.commit()
            finally:
                setup.close()

            with self.assertRaises(RuntimeError):
                with connect(db_path) as conn:
                    conn.execute("INSERT INTO sample(name) VALUES (?)", ("rolled-back",))
                    raise RuntimeError("force rollback")

            with self.assertRaises(sqlite3.ProgrammingError):
                conn.execute("SELECT 1")

            raw = sqlite3.connect(db_path)
            try:
                count = raw.execute("SELECT COUNT(*) FROM sample").fetchone()[0]
            finally:
                raw.close()
            self.assertEqual(count, 0)

    def test_direct_connection_callers_can_close_explicitly(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            db_path = Path(td) / "direct.db"
            conn = connect(db_path)
            self.assertIsInstance(conn, sqlite3.Connection)
            conn.close()
            with self.assertRaises(sqlite3.ProgrammingError):
                conn.execute("SELECT 1")


if __name__ == "__main__":
    unittest.main()
