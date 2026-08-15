from __future__ import annotations

import sqlite3
import tempfile
import unittest
from pathlib import Path

from mal_updater.config import load_config
from mal_updater.db import bootstrap_database
from mal_updater.hidive_snapshot import _dedupe_progress, _history_item_to_progress
from mal_updater.ingestion import ingest_snapshot_payload
from mal_updater.recommendations import _build_new_episode_recommendations
from mal_updater.sync_planner import _plan_status_update, load_provider_series_states


class HidiveProgressProvenanceTests(unittest.TestCase):
    def _config(self, root: Path):
        (root / ".MAL-Updater" / "config").mkdir(parents=True)
        config = load_config(root)
        config.db_path = root / ".MAL-Updater" / "data" / "test.sqlite3"
        return config

    def _payload(self, progress: list[dict]) -> dict:
        return {
            "contract_version": "1.0", "generated_at": "2026-08-15T00:00:00Z", "provider": "hidive",
            "account_id_hint": "acct", "series": [{"provider_series_id": "s1", "title": "Show (English Dub)", "season_title": "Show (English Dub)", "season_number": 1}],
            "progress": progress, "watchlist": [], "raw": {},
        }

    def _progress(self, **overrides):
        item = {
            "provider_episode_id": "e1", "provider_series_id": "s1", "episode_number": 1,
            "episode_title": "One", "playback_position_ms": None, "duration_ms": 1440000,
            "completion_ratio": None, "last_watched_at": "2026-08-15T00:00:00Z",
            "audio_locale": None, "subtitle_locale": None, "rating": None,
            "progress_source_surface": "hidive_history", "progress_observation_kind": "history_membership",
            "completion_assertion": "unknown", "normalization_logic_version": "hidive_progress_v2",
        }
        item.update(overrides)
        return item

    def test_history_membership_normalizes_activity_without_completion(self):
        progress = _history_item_to_progress({"id": 9, "duration": 1440, "watchedAt": 1770000000000, "episodeInformation": {"episodeNumber": 1, "seriesInformation": {"id": 7}}})
        self.assertIsNone(progress.playback_position_ms)
        self.assertIsNone(progress.completion_ratio)
        self.assertEqual("history_membership", progress.progress_observation_kind)
        self.assertEqual("unknown", progress.completion_assertion)

    def test_measured_continue_evidence_outranks_newer_history_membership(self):
        from mal_updater.contracts import EpisodeProgress
        weak = EpisodeProgress(**{k: v for k, v in self._progress(last_watched_at="2026-08-16T00:00:00Z").items()})
        strong = EpisodeProgress(**{k: v for k, v in self._progress(playback_position_ms=600000, completion_ratio=.4, progress_source_surface="hidive_continue_watching", progress_observation_kind="position", last_watched_at="2026-08-15T00:00:00Z").items()})
        self.assertIs(_dedupe_progress([strong, weak])[0], strong)

    def test_history_only_and_legacy_synthetic_rows_cannot_advance_completion(self):
        with tempfile.TemporaryDirectory() as td:
            config = self._config(Path(td))
            ingest_snapshot_payload(self._payload([self._progress()]), config)
            with sqlite3.connect(config.db_path) as conn:
                conn.execute("INSERT INTO provider_episode_progress (provider, provider_episode_id, provider_series_id, episode_number, playback_position_ms, duration_ms, completion_ratio, last_watched_at, raw_json) VALUES ('hidive','legacy','s1',2,1440000,1440000,1.0,'2026-08-16T00:00:00Z','{}')")
                conn.commit()
            state = load_provider_series_states(config, provider="hidive")[0]
            self.assertEqual(0, state.completed_episode_count)
            self.assertEqual(1, state.completion_audit["completion_uncertain_by"]["completion_unknown_history_membership"])
            self.assertEqual(1, state.completion_audit["completion_uncertain_by"]["legacy_unproven_hidive_synthetic_completion"])
            self.assertEqual([], _build_new_episode_recommendations([state]))
            proposal = _plan_status_update(
                state,
                {"id": 123, "title": "Show", "num_episodes": 12, "my_list_status": {"status": "watching", "num_episodes_watched": 0}},
                "mapped", 1.0, mapping_source="test", persisted_mapping_approved=True,
            )
            self.assertEqual("skip", proposal.decision)
            self.assertIn("completion_unknown_history_membership=1", proposal.reasons)

    def test_mixed_evidence_preserves_confirmed_completion_without_uncertain_tail_or_freshness(self):
        with tempfile.TemporaryDirectory() as td:
            config = self._config(Path(td))
            measured = {
                "playback_position_ms": 1440000,
                "completion_ratio": 1.0,
                "progress_source_surface": "hidive_continue_watching",
                "progress_observation_kind": "position",
                "completion_assertion": "confirmed",
            }
            ingest_snapshot_payload(
                self._payload(
                    [
                        self._progress(provider_episode_id="e1", episode_number=1, **measured),
                        self._progress(provider_episode_id="e2", episode_number=2, **measured),
                        self._progress(
                            provider_episode_id="e3",
                            episode_number=3,
                            playback_position_ms=240000,
                            completion_ratio=1 / 6,
                            progress_source_surface="hidive_continue_watching",
                            progress_observation_kind="position",
                            completion_assertion="unknown",
                            last_watched_at="2026-08-15T12:00:00Z",
                        ),
                        self._progress(provider_episode_id="history-tail", episode_number=99, last_watched_at="2026-08-17T00:00:00Z"),
                    ]
                ),
                config,
            )
            with sqlite3.connect(config.db_path) as conn:
                conn.execute(
                    "INSERT INTO provider_episode_progress (provider, provider_episode_id, provider_series_id, episode_number, playback_position_ms, duration_ms, completion_ratio, last_watched_at, raw_json) "
                    "VALUES ('hidive','legacy-tail','s1',100,1440000,1440000,1.0,'2026-08-18T00:00:00Z','{}')"
                )
                conn.commit()

            state = load_provider_series_states(config, provider="hidive")[0]
            self.assertEqual(2, state.completed_episode_count)
            self.assertEqual(2, state.max_completed_episode_number)
            self.assertEqual(3, state.max_episode_number)
            self.assertEqual("2026-08-15T12:00:00Z", state.last_watched_at)
            self.assertEqual(1, state.completion_audit["completion_uncertain_by"]["completion_unknown_history_membership"])
            self.assertEqual(1, state.completion_audit["completion_uncertain_by"]["legacy_unproven_hidive_synthetic_completion"])

            recommendations = _build_new_episode_recommendations([state])
            self.assertEqual(1, len(recommendations))
            self.assertEqual(1, recommendations[0].context["contiguous_tail_gap"])
            self.assertEqual("2026-08-15T12:00:00Z", recommendations[0].context["last_watched_at"])

            partial = _plan_status_update(
                state,
                {"id": 123, "title": "Show", "num_episodes": 3, "my_list_status": {"status": "watching", "num_episodes_watched": 0}},
                "mapped", 1.0, mapping_source="test", persisted_mapping_approved=True,
            )
            self.assertEqual("propose_update", partial.decision)
            self.assertEqual({"status": "watching", "num_watched_episodes": 2}, partial.proposed_my_list_status)

            completed = _plan_status_update(
                state,
                {"id": 123, "title": "Show", "num_episodes": 2, "my_list_status": {"status": "watching", "num_episodes_watched": 0}},
                "mapped", 1.0, mapping_source="test", persisted_mapping_approved=True,
            )
            self.assertEqual("propose_update", completed.decision)
            self.assertEqual("completed", completed.proposed_my_list_status["status"])
            self.assertEqual(2, completed.proposed_my_list_status["num_watched_episodes"])

    def test_uncertain_later_row_cannot_unlock_inferred_completion(self):
        with tempfile.TemporaryDirectory() as td:
            config = self._config(Path(td))
            ingest_snapshot_payload(
                self._payload(
                    [
                        self._progress(
                            provider_episode_id="e1",
                            episode_number=1,
                            playback_position_ms=1296000,
                            completion_ratio=0.9,
                            progress_source_surface="hidive_continue_watching",
                            progress_observation_kind="position",
                            completion_assertion="unknown",
                        ),
                        self._progress(provider_episode_id="history-e2", episode_number=2, last_watched_at="2026-08-16T00:00:00Z"),
                    ]
                ),
                config,
            )

            state = load_provider_series_states(config, provider="hidive")[0]
            self.assertEqual(0, state.completed_episode_count)
            self.assertIsNone(state.max_completed_episode_number)
            self.assertEqual(1, state.max_episode_number)
            self.assertEqual([], _build_new_episode_recommendations([state]))

    def test_migration_preserves_legacy_values_and_sets_nullable_provenance(self):
        with tempfile.TemporaryDirectory() as td:
            db_path = Path(td) / "db.sqlite3"
            bootstrap_database(db_path)
            with sqlite3.connect(db_path) as conn:
                conn.execute("INSERT INTO provider_series(provider,provider_series_id,title,raw_json) VALUES('hidive','s','Show','{}')")
                conn.execute("INSERT INTO provider_episode_progress(provider,provider_episode_id,provider_series_id,playback_position_ms,duration_ms,completion_ratio,raw_json) VALUES('hidive','e','s',10,20,.5,'{}')")
                row = conn.execute("SELECT playback_position_ms,duration_ms,completion_ratio,progress_source_surface,progress_observation_kind,completion_assertion,normalization_logic_version FROM provider_episode_progress").fetchone()
            self.assertEqual((10, 20, .5, None, None, None, None), row)


if __name__ == "__main__":
    unittest.main()
