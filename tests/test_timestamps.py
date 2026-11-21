from __future__ import annotations

import unittest
from datetime import datetime, timezone

from mal_updater.timestamps import parse_utc_timestamp


class ParseUtcTimestampTests(unittest.TestCase):
    def test_sqlite_iso_and_offset_forms_become_aware_utc(self) -> None:
        expected = datetime(2026, 7, 23, 0, 2, 3, tzinfo=timezone.utc)
        for value in ("2026-07-23 00:02:03", "2026-07-23T00:02:03Z", " 2026-07-23T00:02:03+00:00 ", "2026-07-23T02:02:03+02:00"):
            self.assertEqual(expected, parse_utc_timestamp(value), value)

    def test_unparseable_and_out_of_range_values_give_none(self) -> None:
        for value in (None, 17, "", "   ", "not-a-date", "9999-12-31T23:59:59-23:59", "0001-01-01T00:00:00+23:59"):
            self.assertIsNone(parse_utc_timestamp(value), value)


if __name__ == "__main__":
    unittest.main()
