from __future__ import annotations

import unittest
from datetime import datetime, timezone

from mal_updater.cli import _age_seconds_from_timestamp, _parse_sqlite_timestamp, _stale_row_age_days


class CliSqliteTimestampTests(unittest.TestCase):
    def test_parse_sqlite_timestamp_canonical_formats(self) -> None:
        self.assertEqual(
            _parse_sqlite_timestamp("2026-07-23 00:02:03"),
            datetime(2026, 7, 23, 0, 2, 3, tzinfo=timezone.utc),
        )
        self.assertEqual(
            _parse_sqlite_timestamp("2026-07-23T00:02:03Z"),
            datetime(2026, 7, 23, 0, 2, 3, tzinfo=timezone.utc),
        )
        self.assertIsNone(_parse_sqlite_timestamp("not-a-timestamp"))

    def test_existing_age_helpers_use_remaining_parser(self) -> None:
        self.assertEqual(
            _stale_row_age_days(
                datetime(2026, 7, 24, 0, 2, 3, tzinfo=timezone.utc),
                "2026-07-23 00:02:03",
            ),
            1.0,
        )
        self.assertIsNotNone(_age_seconds_from_timestamp("2026-07-23T00:02:03Z"))


if __name__ == "__main__":
    unittest.main()
