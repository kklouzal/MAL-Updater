from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from mal_updater.cli import _filter_review_queue_items, _review_queue_item_label, _summarize_review_queue
from mal_updater.config import load_config
from mal_updater.db import bootstrap_database, connect, get_provider_series_title_map_by_keys, list_review_queue_entries


class ReviewQueueMultiProviderTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.project_root = Path(self.temp_dir.name)
        (self.project_root / ".MAL-Updater" / "config").mkdir(parents=True, exist_ok=True)
        self.config = load_config(self.project_root)
        bootstrap_database(self.config.db_path)

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def _insert_series(self, provider: str, provider_series_id: str, *, title: str, season_title: str | None = None) -> None:
        with connect(self.config.db_path) as conn:
            conn.execute(
                """
                INSERT INTO provider_series (provider, provider_series_id, title, season_title, raw_json)
                VALUES (?, ?, ?, ?, '{}')
                """,
                (provider, provider_series_id, title, season_title),
            )
            conn.commit()

    def _insert_review_queue_entry(self, provider: str, provider_series_id: str, *, issue_type: str = "mapping_review") -> None:
        with connect(self.config.db_path) as conn:
            conn.execute(
                """
                INSERT INTO review_queue (
                    provider,
                    provider_series_id,
                    issue_type,
                    severity,
                    payload_json,
                    status
                ) VALUES (?, ?, ?, 'warning', ?, 'open')
                """,
                (
                    provider,
                    provider_series_id,
                    issue_type,
                    '{"decision":"needs_manual_match","reasons":["ambiguous_title"]}',
                ),
            )
            conn.commit()

    def test_review_queue_item_label_uses_matching_provider_series_title(self) -> None:
        self._insert_series("hidive", "hidive-show", title="Fallback HIDIVE Title", season_title="Fallback HIDIVE Season")
        self._insert_review_queue_entry("hidive", "hidive-show")

        items = list_review_queue_entries(self.config.db_path, status="open", issue_type="mapping_review")
        provider_series_titles = get_provider_series_title_map_by_keys(
            self.config.db_path,
            provider_series_keys=[(item.provider, item.provider_series_id) for item in items if item.provider_series_id],
        )

        label = _review_queue_item_label(items[0], provider_series_titles=provider_series_titles)

        self.assertEqual("Fallback HIDIVE Season", label["title"])

    def test_review_queue_filters_can_cluster_hidive_titles(self) -> None:
        self._insert_series("hidive", "hidive-show", title="Hidden Franchise", season_title="Hidden Franchise Season 2")
        self._insert_review_queue_entry("hidive", "hidive-show")

        items = list_review_queue_entries(self.config.db_path, status="open", issue_type="mapping_review")
        provider_series_titles = get_provider_series_title_map_by_keys(
            self.config.db_path,
            provider_series_keys=[(item.provider, item.provider_series_id) for item in items if item.provider_series_id],
        )

        filtered = _filter_review_queue_items(
            items,
            provider_series_titles=provider_series_titles,
            title_cluster="Hidden Franchise",
        )

        self.assertEqual(1, len(filtered))
        self.assertEqual("hidive-show", filtered[0].provider_series_id)

    def test_review_queue_summary_exposes_hidive_title_labels(self) -> None:
        self._insert_series("hidive", "hidive-show", title="Hidden Franchise", season_title="Hidden Franchise Season 2")
        self._insert_review_queue_entry("hidive", "hidive-show")

        items = list_review_queue_entries(self.config.db_path, status="open", issue_type="mapping_review")
        provider_series_titles = get_provider_series_title_map_by_keys(
            self.config.db_path,
            provider_series_keys=[(item.provider, item.provider_series_id) for item in items if item.provider_series_id],
        )

        summary = _summarize_review_queue(
            items,
            status="open",
            issue_type="mapping_review",
            provider_series_titles=provider_series_titles,
        )

        self.assertEqual("Hidden Franchise Season 2", summary["top_title_clusters"][0]["label"])
        self.assertEqual("hidive-show", summary["top_title_clusters"][0]["refresh_provider_series_ids"][0])


if __name__ == "__main__":
    unittest.main()
