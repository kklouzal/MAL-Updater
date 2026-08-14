from __future__ import annotations

import contextlib
import io
import json
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from mal_updater.cli import _cmd_provider_fetch_snapshot
from mal_updater.config import load_config
from mal_updater.contracts import CrunchyrollSnapshot, EpisodeProgress, ProviderSnapshot, SeriesRef, WatchlistEntry
from mal_updater.contracts.crunchyroll import CrunchyrollSnapshot as CrunchyrollSnapshotCompatAlias
from mal_updater.db import bootstrap_database
from mal_updater.ingestion import ingest_snapshot_payload
from mal_updater.provider_types import ProviderFetchResult
from mal_updater.validation import SnapshotValidationError, validate_snapshot_payload


def sample_snapshot() -> dict:
    return {
        "contract_version": "1.0",
        "generated_at": "2026-03-14T21:00:00Z",
        "provider": "crunchyroll",
        "account_id_hint": None,
        "series": [
            {
                "provider_series_id": "series-123",
                "title": "Example Show",
                "season_title": "Example Show Season 1",
                "season_number": 1,
            }
        ],
        "progress": [
            {
                "provider_episode_id": "episode-456",
                "provider_series_id": "series-123",
                "episode_number": 3,
                "episode_title": "Example Episode",
                "playback_position_ms": 1300000,
                "duration_ms": 1440000,
                "completion_ratio": 0.95,
                "last_watched_at": "2026-03-14T20:55:00Z",
                "audio_locale": "en-US",
                "subtitle_locale": None,
                "rating": None,
            }
        ],
        "watchlist": [
            {
                "provider_series_id": "series-123",
                "added_at": "2026-03-10T12:00:00Z",
                "status": "watching",
                "list_id": "favorites",
                "list_name": "Favorites",
                "list_kind": "system",
            }
        ],
        "raw": {},
    }


class ValidationTests(unittest.TestCase):
    def test_crunchyroll_contract_alias_remains_compatible(self) -> None:
        self.assertIs(CrunchyrollSnapshot, ProviderSnapshot)
        self.assertIs(CrunchyrollSnapshotCompatAlias, ProviderSnapshot)

    def test_validate_snapshot_payload_returns_dataclass_model(self) -> None:
        snapshot = validate_snapshot_payload(sample_snapshot())
        self.assertEqual(snapshot.provider, "crunchyroll")
        self.assertEqual(snapshot.contract_version, "1.0")
        self.assertEqual(snapshot.series[0].provider_series_id, "series-123")
        self.assertEqual(snapshot.progress[0].completion_ratio, 0.95)
        self.assertEqual(snapshot.watchlist[0].status, "watching")
        self.assertEqual(snapshot.watchlist[0].list_id, "favorites")
        self.assertEqual(snapshot.watchlist[0].list_kind, "system")

    def test_validate_snapshot_payload_rejects_invalid_ratio(self) -> None:
        payload = sample_snapshot()
        payload["progress"][0]["completion_ratio"] = 1.5
        with self.assertRaisesRegex(SnapshotValidationError, "progress\\[0\\]\\.completion_ratio must be between 0 and 1"):
            validate_snapshot_payload(payload)

    def test_validate_snapshot_payload_rejects_missing_progress_keys_with_stable_message(self) -> None:
        payload = sample_snapshot()
        del payload["progress"][0]["rating"]
        with self.assertRaisesRegex(SnapshotValidationError, "progress\\[0\\] is missing keys: rating"):
            validate_snapshot_payload(payload)

    def test_validate_snapshot_payload_rejects_extra_watchlist_keys_with_stable_message(self) -> None:
        payload = sample_snapshot()
        payload["watchlist"][0]["unexpected"] = "value"
        with self.assertRaisesRegex(SnapshotValidationError, "watchlist\\[0\\] contains unexpected keys: unexpected"):
            validate_snapshot_payload(payload)

    def test_validate_snapshot_payload_rejects_progress_unknown_series(self) -> None:
        payload = sample_snapshot()
        payload["progress"][0]["provider_series_id"] = "series-missing"
        with self.assertRaises(SnapshotValidationError):
            validate_snapshot_payload(payload)

    def test_validate_snapshot_payload_rejects_duplicate_episode_ids(self) -> None:
        payload = sample_snapshot()
        payload["progress"].append({**payload["progress"][0]})
        with self.assertRaises(SnapshotValidationError):
            validate_snapshot_payload(payload)

    def test_validate_snapshot_payload_rejects_duplicate_watchlist_ids(self) -> None:
        payload = sample_snapshot()
        payload["watchlist"].append({**payload["watchlist"][0]})
        with self.assertRaises(SnapshotValidationError):
            validate_snapshot_payload(payload)

    def test_validate_snapshot_payload_allows_cross_list_duplicate_membership(self) -> None:
        payload = sample_snapshot()
        payload["watchlist"][0].update({"provider_item_type": "VOD_SEASON", "provider_item_id": "season-1", "position": 0})
        payload["watchlist"].append(
            {
                **payload["watchlist"][0],
                "list_id": "watchlist:custom",
                "list_name": "Custom",
                "list_kind": "custom",
            }
        )

        snapshot = validate_snapshot_payload(payload)

        self.assertEqual(2, len(snapshot.watchlist))
        self.assertEqual(["favorites", "watchlist:custom"], [entry.list_id for entry in snapshot.watchlist])

class _FakeProvider:
    slug = "crunchyroll"
    display_name = "Crunchyroll"

    def __init__(self) -> None:
        self.full_refresh_requests: list[bool] = []

    def fetch_snapshot(self, config, *, profile: str = "default", full_refresh: bool = False, **_: object) -> ProviderFetchResult:
        self.full_refresh_requests.append(full_refresh)
        return ProviderFetchResult(
            snapshot=ProviderSnapshot(
                contract_version="1.0",
                generated_at="2026-03-14T21:00:00Z",
                provider="crunchyroll",
                account_id_hint=None,
                series=[
                    SeriesRef(
                        provider_series_id="series-123",
                        title="Example Show",
                        season_title="Example Show Season 1",
                        season_number=1,
                    )
                ],
                progress=[
                    EpisodeProgress(
                        provider_episode_id="episode-456",
                        provider_series_id="series-123",
                        episode_number=3,
                        episode_title="Example Episode",
                        playback_position_ms=1300000,
                        duration_ms=1440000,
                        completion_ratio=0.95,
                        last_watched_at="2026-03-14T20:55:00Z",
                        audio_locale="en-US",
                    )
                ],
                watchlist=[
                    WatchlistEntry(
                        provider_series_id="series-123",
                        added_at="2026-03-10T12:00:00Z",
                        status="watching",
                        list_id="favorites",
                        list_name="Favorites",
                        list_kind="system",
                    )
                ],
                raw={"sync_boundary_mode": "full_refresh"},
            )
        )

    def write_snapshot_file(self, path: Path, snapshot: ProviderSnapshot) -> Path:
        raise AssertionError("write_snapshot_file should not be called without --out")


class IngestionTests(unittest.TestCase):
    def _provider_table_rows(self, db_path: Path) -> dict[str, list[tuple[object, ...]]]:
        with contextlib.closing(sqlite3.connect(db_path)) as conn:
            return {
                "provider_series": conn.execute(
                    "SELECT * FROM provider_series ORDER BY provider, provider_series_id"
                ).fetchall(),
                "provider_episode_progress": conn.execute(
                    "SELECT * FROM provider_episode_progress ORDER BY provider, provider_episode_id"
                ).fetchall(),
                "provider_watchlist": conn.execute(
                    """
                    SELECT *
                    FROM provider_watchlist
                    ORDER BY provider, list_id, provider_series_id, provider_item_type, provider_item_id
                    """
                ).fetchall(),
            }

    def test_ingest_snapshot_payload_writes_rows_and_summary(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / ".MAL-Updater" / "config").mkdir(parents=True)
            config = load_config(root)

            summary = ingest_snapshot_payload(sample_snapshot(), config)

            self.assertEqual(summary.provider, "crunchyroll")
            self.assertEqual(summary.series_count, 1)
            self.assertEqual(summary.progress_count, 1)
            self.assertEqual(summary.watchlist_count, 1)
            self.assertIsNotNone(summary.sync_run_id)

            import sqlite3

            with contextlib.closing(sqlite3.connect(config.db_path)) as conn:
                self.assertEqual(conn.execute("SELECT COUNT(*) FROM provider_series").fetchone()[0], 1)
                self.assertEqual(conn.execute("SELECT COUNT(*) FROM provider_episode_progress").fetchone()[0], 1)
                self.assertEqual(conn.execute("SELECT COUNT(*) FROM provider_watchlist").fetchone()[0], 1)
                self.assertEqual(conn.execute("SELECT COUNT(*) FROM sync_runs WHERE status = 'completed'").fetchone()[0], 1)

    def test_ingest_snapshot_payload_stamps_account_provenance_and_preserves_catalog_timestamp(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / ".MAL-Updater" / "config").mkdir(parents=True)
            config = load_config(root)
            payload = sample_snapshot()
            bootstrap_database(config.db_path)
            with contextlib.closing(sqlite3.connect(config.db_path)) as conn:
                conn.execute("PRAGMA foreign_keys = ON")
                conn.execute(
                    """
                    INSERT INTO provider_series(
                        provider, provider_series_id, title, season_title, raw_json,
                        last_seen_at, catalog_observed_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        "crunchyroll",
                        "series-123",
                        "Catalog Title",
                        "Catalog Season",
                        json.dumps({"source": "catalog"}),
                        "2026-01-01T00:00:00Z",
                        "2026-01-02T00:00:00Z",
                    ),
                )
                conn.commit()

            ingest_snapshot_payload(payload, config)

            with contextlib.closing(sqlite3.connect(config.db_path)) as conn:
                conn.row_factory = sqlite3.Row
                row = conn.execute(
                    """
                    SELECT title, season_title, account_observed_at, catalog_observed_at
                    FROM provider_series
                    WHERE provider = 'crunchyroll' AND provider_series_id = 'series-123'
                    """
                ).fetchone()
            self.assertEqual("Example Show", row["title"])
            self.assertEqual("Example Show Season 1", row["season_title"])
            self.assertIsNotNone(row["account_observed_at"])
            self.assertEqual("2026-01-02T00:00:00Z", row["catalog_observed_at"])

    def test_ingest_snapshot_payload_rolls_back_provider_rows_when_watchlist_upsert_fails(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / ".MAL-Updater" / "config").mkdir(parents=True)
            config = load_config(root)
            ingest_snapshot_payload(sample_snapshot(), config)
            before_provider_rows = self._provider_table_rows(config.db_path)

            payload = sample_snapshot()
            payload["series"][0]["title"] = "Updated Show"
            payload["progress"][0]["playback_position_ms"] = 42
            payload["watchlist"][0]["status"] = "planned"
            payload["watchlist"].append(
                {
                    **payload["watchlist"][0],
                    "list_id": "watchlist:custom",
                    "list_name": "Custom",
                    "list_kind": "custom",
                    "position": 1,
                }
            )

            import mal_updater.ingestion as ingestion_module

            original_entry_json = ingestion_module._entry_json
            watchlist_entries_seen = 0

            def fail_on_second_watchlist_entry(entry: object) -> str:
                nonlocal watchlist_entries_seen
                if isinstance(entry, WatchlistEntry):
                    watchlist_entries_seen += 1
                    if watchlist_entries_seen == 2:
                        raise RuntimeError("injected watchlist upsert failure")
                return original_entry_json(entry)

            with patch("mal_updater.ingestion._entry_json", side_effect=fail_on_second_watchlist_entry):
                with self.assertRaisesRegex(RuntimeError, "injected watchlist upsert failure"):
                    ingest_snapshot_payload(payload, config, mode="hot")

            self.assertEqual(before_provider_rows, self._provider_table_rows(config.db_path))

            with contextlib.closing(sqlite3.connect(config.db_path)) as conn:
                conn.row_factory = sqlite3.Row
                runs = conn.execute("SELECT status, mode, summary_json FROM sync_runs ORDER BY id").fetchall()
                self.assertEqual(2, len(runs))
                self.assertEqual("completed", runs[0]["status"])
                self.assertEqual("failed", runs[1]["status"])
                self.assertEqual("hot", runs[1]["mode"])
                self.assertEqual(0, conn.execute("SELECT COUNT(*) FROM sync_runs WHERE status = 'running'").fetchone()[0])
                failure_summary = json.loads(runs[1]["summary_json"])
                self.assertIn("injected watchlist upsert failure", failure_summary["error"])

    def test_ingest_snapshot_payload_persists_cross_list_watchlist_memberships(self) -> None:
        payload = sample_snapshot()
        payload["watchlist"][0].update({"provider_item_type": "VOD_SEASON", "provider_item_id": "season-1", "position": 0})
        payload["watchlist"].append(
            {
                **payload["watchlist"][0],
                "list_id": "watchlist:custom",
                "list_name": "Custom",
                "list_kind": "custom",
                "position": 7,
            }
        )
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / ".MAL-Updater" / "config").mkdir(parents=True)
            config = load_config(root)

            summary = ingest_snapshot_payload(payload, config)

            self.assertEqual(2, summary.watchlist_count)
            import sqlite3

            with contextlib.closing(sqlite3.connect(config.db_path)) as conn:
                rows = conn.execute(
                    "SELECT list_id, provider_series_id, provider_item_type, provider_item_id, position FROM provider_watchlist ORDER BY list_id"
                ).fetchall()
            self.assertEqual(
                [("favorites", "series-123", "VOD_SEASON", "season-1", 0), ("watchlist:custom", "series-123", "VOD_SEASON", "season-1", 7)],
                rows,
            )

    def test_ingest_snapshot_payload_preserves_explicit_sync_mode(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / ".MAL-Updater" / "config").mkdir(parents=True)
            config = load_config(root)

            ingest_snapshot_payload(sample_snapshot(), config, mode="full_refresh")

            import sqlite3

            with contextlib.closing(sqlite3.connect(config.db_path)) as conn:
                self.assertEqual(conn.execute("SELECT mode FROM sync_runs").fetchone()[0], "full_refresh")

    def test_complete_watchlist_generation_deactivates_absent_but_partial_does_not(self) -> None:
        def complete_payload(series_id: str, account: str = "acct-1") -> dict:
            payload = sample_snapshot()
            payload["account_id_hint"] = account
            payload["series"][0]["provider_series_id"] = series_id
            payload["progress"] = []
            payload["watchlist"][0]["provider_series_id"] = series_id
            payload["raw"] = {
                "partial": False, "sync_boundary_refresh_kind": "explicit_full_refresh",
                "watchlist_start": 0, "watchlist_partial": False,
            }
            return payload

        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / ".MAL-Updater" / "config").mkdir(parents=True)
            config = load_config(root)
            ingest_snapshot_payload(complete_payload("series-x"), config, mode="full_refresh")
            partial = complete_payload("series-y")
            partial["raw"]["partial"] = True
            partial["raw"]["watchlist_partial"] = True
            ingest_snapshot_payload(partial, config, mode="hot")
            with sqlite3.connect(config.db_path) as conn:
                self.assertEqual(1, conn.execute("SELECT is_active FROM provider_watchlist WHERE provider_series_id='series-x'").fetchone()[0])
            ingest_snapshot_payload(complete_payload("series-y"), config, mode="full_refresh")
            with sqlite3.connect(config.db_path) as conn:
                states = dict(conn.execute("SELECT provider_series_id, is_active FROM provider_watchlist"))
            self.assertEqual({"series-x": 0, "series-y": 1}, states)

    def test_account_mismatch_cannot_deactivate_existing_membership(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / ".MAL-Updater" / "config").mkdir(parents=True)
            config = load_config(root)
            first = sample_snapshot()
            first["account_id_hint"] = "acct-a"
            first["raw"] = {"partial": False, "sync_boundary_refresh_kind": "explicit_full_refresh", "watchlist_start": 0, "watchlist_partial": False}
            ingest_snapshot_payload(first, config, mode="full_refresh")
            second = sample_snapshot()
            second["account_id_hint"] = "acct-b"
            second["watchlist"] = []
            second["raw"] = dict(first["raw"])
            ingest_snapshot_payload(second, config, mode="full_refresh")
            with sqlite3.connect(config.db_path) as conn:
                self.assertEqual(1, conn.execute("SELECT is_active FROM provider_watchlist").fetchone()[0])

    def test_provider_fetch_snapshot_ingest_records_full_refresh_mode(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / ".MAL-Updater" / "config").mkdir(parents=True)
            provider = _FakeProvider()

            with patch("mal_updater.cli.get_provider", return_value=provider), contextlib.redirect_stdout(io.StringIO()):
                exit_code = _cmd_provider_fetch_snapshot(
                    root,
                    "crunchyroll",
                    "default",
                    None,
                    ingest=True,
                    full_refresh=True,
                )

            self.assertEqual(exit_code, 0)
            self.assertEqual(provider.full_refresh_requests, [True])

            import sqlite3

            config = load_config(root)
            with contextlib.closing(sqlite3.connect(config.db_path)) as conn:
                self.assertEqual(conn.execute("SELECT mode FROM sync_runs").fetchone()[0], "full_refresh")


if __name__ == "__main__":
    unittest.main()
