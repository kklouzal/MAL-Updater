from __future__ import annotations

import contextlib
import io
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from mal_updater.cli import _cmd_provider_fetch_snapshot
from mal_updater.config import load_config
from mal_updater.contracts import EpisodeProgress, ProviderSnapshot, SeriesRef, WatchlistEntry
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
        with self.assertRaises(SnapshotValidationError):
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


class _FakeProvider:
    slug = "crunchyroll"
    display_name = "Crunchyroll"

    def __init__(self) -> None:
        self.full_refresh_requests: list[bool] = []

    def fetch_snapshot(self, config, *, profile: str = "default", full_refresh: bool = False) -> ProviderFetchResult:
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

            conn = sqlite3.connect(config.db_path)
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM provider_series").fetchone()[0], 1)
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM provider_episode_progress").fetchone()[0], 1)
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM provider_watchlist").fetchone()[0], 1)
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM sync_runs WHERE status = 'completed'").fetchone()[0], 1)

    def test_ingest_snapshot_payload_preserves_explicit_sync_mode(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / ".MAL-Updater" / "config").mkdir(parents=True)
            config = load_config(root)

            ingest_snapshot_payload(sample_snapshot(), config, mode="full_refresh")

            import sqlite3

            conn = sqlite3.connect(config.db_path)
            self.assertEqual(conn.execute("SELECT mode FROM sync_runs").fetchone()[0], "full_refresh")
            conn.close()

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
            conn = sqlite3.connect(config.db_path)
            self.assertEqual(conn.execute("SELECT mode FROM sync_runs").fetchone()[0], "full_refresh")
            conn.close()


if __name__ == "__main__":
    unittest.main()
