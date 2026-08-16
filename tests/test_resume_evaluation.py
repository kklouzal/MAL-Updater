from __future__ import annotations

from copy import deepcopy
from contextlib import closing
from datetime import datetime, timedelta, timezone
import json
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from mal_updater.config import load_config
from mal_updater.db import bootstrap_database
from mal_updater.evaluation.resume import ReplayQuery, ResumePolicy, evaluate_resume
from mal_updater.ingestion import ingest_snapshot_payload
from tests.test_validation_ingestion import sample_snapshot

UTC = timezone.utc


class ResumeEvaluationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.runtime_root = self.root / ".MAL-Updater"
        self.db_path = self.runtime_root / "data" / "resume-evaluation.sqlite3"
        settings_path = self.runtime_root / "config" / "settings.toml"
        settings_path.parent.mkdir(parents=True)
        # TemporaryDirectory may live below an operator workspace marker. Pin
        # every discovery input so a test can never inherit or discover a live
        # workspace/runtime/database from cwd or an ancestor directory.
        self.config_env = mock.patch.dict(
            "os.environ",
            {
                "MAL_UPDATER_WORKSPACE_DIR": str(self.root),
                "OPENCLAW_WORKSPACE_DIR": str(self.root),
                "MAL_UPDATER_RUNTIME_ROOT": str(self.runtime_root),
                "MAL_UPDATER_RUNTIME_DIR": str(self.runtime_root),
                "MAL_UPDATER_SETTINGS_PATH": str(settings_path),
                "MAL_UPDATER_CONFIG": str(settings_path),
                "MAL_UPDATER_CONFIG_DIR": str(self.runtime_root / "config"),
                "MAL_UPDATER_SECRETS_DIR": str(self.runtime_root / "secrets"),
                "MAL_UPDATER_DATA_DIR": str(self.runtime_root / "data"),
                "MAL_UPDATER_STATE_DIR": str(self.runtime_root / "state"),
                "MAL_UPDATER_CACHE_DIR": str(self.runtime_root / "cache"),
                "MAL_UPDATER_DB_PATH": str(self.db_path),
            },
        )
        self.config_env.start()
        self.config = load_config(self.root)
        self.assertEqual(self.config.project_root, self.root.resolve())
        self.assertEqual(self.config.runtime_root, self.runtime_root.resolve())
        self.assertEqual(self.config.db_path, self.db_path.resolve())
        self.assertTrue(self.config.db_path.is_relative_to(self.root.resolve()))

    def tearDown(self) -> None:
        self.config_env.stop()
        self.temp.cleanup()

    def _payload(self, generated: str, progress: list[dict]) -> dict:
        payload = sample_snapshot()
        payload["generated_at"] = generated
        payload["progress"] = progress
        payload["watchlist"] = []
        return payload

    @staticmethod
    def _progress(episode: int, *, ratio: float | None, position: int | None,
                  watched: str, assertion: str | None = None, provider_episode_id: str | None = None) -> dict:
        row = deepcopy(sample_snapshot()["progress"][0])
        row.update({"provider_episode_id": provider_episode_id or f"episode-{episode}",
                    "episode_number": episode, "completion_ratio": ratio,
                    "playback_position_ms": position, "last_watched_at": watched})
        if assertion is not None:
            row.update({"progress_source_surface": "history", "progress_observation_kind": "position" if position is not None else "history_membership",
                        "completion_assertion": assertion, "normalization_logic_version": "provider/v1"})
        return row

    def _query(self) -> ReplayQuery:
        cutoff = datetime(2026, 3, 15, tzinfo=UTC)
        return ReplayQuery("q1", "local-default", cutoff, cutoff + timedelta(days=30))

    def test_migration_preserves_operational_rows_and_adds_empty_event_table(self) -> None:
        bootstrap_database(self.config.db_path)
        with closing(sqlite3.connect(self.config.db_path)) as conn:
            conn.execute("INSERT INTO provider_series(provider,provider_series_id,title) VALUES('crunchyroll','legacy','Legacy')")
            conn.commit()
        bootstrap_database(self.config.db_path)
        with closing(sqlite3.connect(self.config.db_path)) as conn:
            self.assertEqual(conn.execute("SELECT title FROM provider_series WHERE provider_series_id='legacy'").fetchone()[0], "Legacy")
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM evaluation_events").fetchone()[0], 0)
            self.assertIn("027_evaluation_events.sql", {row[0] for row in conn.execute("SELECT version FROM schema_migrations")})

    def test_append_is_idempotent_privacy_safe_and_old_new_payload_compatible(self) -> None:
        old = self._payload("2026-03-14T21:00:00Z", [self._progress(3, ratio=.4, position=500000, watched="2026-03-14T20:00:00Z")])
        ingest_snapshot_payload(old, self.config)
        ingest_snapshot_payload(old, self.config)
        new = self._payload("2026-03-14T22:00:00Z", [self._progress(3, ratio=.5, position=600000, watched="2026-03-14T21:00:00Z", assertion="unknown")])
        ingest_snapshot_payload(new, self.config)
        with closing(sqlite3.connect(self.config.db_path)) as conn:
            conn.row_factory = sqlite3.Row
            plays = conn.execute("SELECT * FROM evaluation_events WHERE event_type='provider_play' ORDER BY observed_at").fetchall()
            self.assertEqual(len(plays), 2)
            self.assertEqual(plays[0]["normalization_version"], "resume-observation/v1")
            serialized = json.dumps([dict(row) for row in plays])
            self.assertNotIn("account_id_hint", serialized)
            self.assertNotIn("raw", serialized)
            self.assertEqual(conn.execute("SELECT completion_assertion FROM provider_episode_progress").fetchone()[0], "unknown")

    def test_cutoff_latest_episode_false_start_and_future_leakage(self) -> None:
        evidence = self._payload("2026-03-14T23:00:00Z", [
            self._progress(1, ratio=.4, position=500000, watched="2026-03-13T20:00:00Z"),
            self._progress(2, ratio=.3, position=400000, watched="2026-03-14T20:00:00Z"),
            self._progress(3, ratio=.01, position=1000, watched="2026-03-14T21:00:00Z"),
        ])
        ingest_snapshot_payload(evidence, self.config)
        future = self._payload("2026-03-16T00:00:00Z", [self._progress(9, ratio=.5, position=600000, watched="2026-03-16T00:00:00Z")])
        ingest_snapshot_payload(future, self.config)
        report = evaluate_resume(self.config.db_path, self._query())
        self.assertEqual([item["features"]["episode_number"] for item in report["candidates"]], [2])
        self.assertEqual(report["exclusion_reasons"]["false_start_below_minimum_progress"], 1)
        self.assertLessEqual(report["leakage_audit"]["max_evidence_observed_at"], "2026-03-15T00:00:00Z")
        self.assertNotIn("episode-9", json.dumps(report["candidates"]))

    def test_event_at_cutoff_is_evidence_and_later_observation_is_label_only(self) -> None:
        at_cutoff = self._payload("2026-03-15T00:00:00Z", [self._progress(2, ratio=.3, position=400000, watched="2026-03-15T00:00:00Z")])
        ingest_snapshot_payload(at_cutoff, self.config)
        later = self._payload("2026-03-16T00:00:00Z", [self._progress(2, ratio=.4, position=500000, watched="2026-03-16T00:00:00Z")])
        ingest_snapshot_payload(later, self.config)
        report = evaluate_resume(self.config.db_path, self._query())
        self.assertEqual(report["coverage"]["evidence_events"], 1)
        self.assertEqual(report["labels"], [{"item_id": "crunchyroll:episode:episode-2", "resumed": True}])

    def test_uncertain_hidive_history_without_measurement_is_not_completion_or_candidate(self) -> None:
        payload = self._payload("2026-03-14T23:00:00Z", [self._progress(2, ratio=None, position=None, watched="2026-03-14T20:00:00Z", assertion="unknown")])
        payload["provider"] = "hidive"
        ingest_snapshot_payload(payload, self.config)
        report = evaluate_resume(self.config.db_path, self._query())
        self.assertEqual(report["candidates"], [])
        self.assertEqual(report["exclusion_reasons"], {"no_measured_progress": 1})

    def test_evaluation_is_read_only_and_metrics_unavailable_without_future_labels(self) -> None:
        ingest_snapshot_payload(self._payload("2026-03-14T23:00:00Z", [self._progress(2, ratio=.3, position=400000, watched="2026-03-14T20:00:00Z")]), self.config)
        before = self.config.db_path.read_bytes()
        report = evaluate_resume(self.config.db_path, self._query(), ResumePolicy(minimum_progress_ratio=.1))
        after = self.config.db_path.read_bytes()
        self.assertEqual(before, after)
        self.assertIsNone(report["metrics"]["precision_at_5"])
        self.assertTrue(report["read_only"])
        self.assertTrue(report["metrics_unavailable"])
        with closing(sqlite3.connect(f"file:{self.config.db_path.resolve()}?mode=ro", uri=True)) as conn:
            with self.assertRaises(sqlite3.OperationalError):
                conn.execute("DELETE FROM evaluation_events")


class BundleValidationTests(unittest.TestCase):
    def _bundle(self, root: Path, *, mutation: str | None = None) -> Path:
        from mal_updater.evaluation.events import canonical_json, sha256_json
        bundle = root / "bundle"
        (bundle / "predictions").mkdir(parents=True)
        event = {"schema_version": "mal-eval-event/v1", "event_id": "e1", "user_id": "u", "event_type": "provider_play",
                 "source": "fixture", "source_event_id": "s1", "source_revision": 1,
                 "occurred_at": "2026-03-14T00:00:00Z", "observed_at": "2026-03-14T00:00:00Z",
                 "effective_from": "2026-03-14T00:00:00Z", "effective_to": None, "supersedes_event_id": None,
                 "entity": {"entity_type": "episode", "entity_id": "x", "mal_anime_id": None, "provider": "fixture",
                            "provider_series_id": "s", "provider_episode_id": "x"}, "payload": {"completion_ratio": .5}}
        event["payload_sha256"] = sha256_json(event["payload"])
        candidate = {"schema_version": "mal-eval-candidate/v1", "query_id": "q", "user_id": "u",
                     "cutoff_at": "2026-03-15T00:00:00Z", "horizon_end_at": "2026-04-14T00:00:00Z", "objective": "resume",
                     "item_id": "x", "mal_anime_id": None, "provider_item_ids": ["x"], "eligible": True,
                     "eligibility_reasons": ["partial"], "sources": [{"source_type": "provider_watch_history", "source_item_id": "x",
                     "target_item_id": "x", "provider": "fixture", "weight": None, "votes": None,
                     "as_of": "2026-03-14T00:00:00Z", "evidence_event_ids": ["e1"]}], "availability": [], "english_dub": [],
                     "features": {"completion_ratio": .5}, "feature_version": "v1", "evidence_event_ids": ["e1"],
                     "max_observed_at": "2026-03-14T00:00:00Z"}
        prediction = {"schema_version": "mal-eval-prediction/v1", "query_id": "q", "objective": "resume", "item_id": "x",
                      "rank": 1, "score": .5, "probability": None, "policy_id": "production-v1", "policy_version": "1",
                      "policy_artifact_sha256": "0" * 64, "contributions": [{"feature": "completion_ratio", "value": .5,
                      "source_event_ids": ["e1"]}]}
        if mutation == "future_observation":
            event["observed_at"] = "2026-03-16T00:00:00Z"
            candidate["max_observed_at"] = event["observed_at"]
        elif mutation == "future_occurrence":
            event["occurred_at"] = "2026-03-16T00:00:00Z"
        elif mutation == "expired_fact":
            event["effective_to"] = "2026-03-15T00:00:00Z"
        elif mutation == "candidate_not_in_universe":
            prediction["item_id"] = "other"
        elif mutation == "missing_attribution":
            candidate["evidence_event_ids"] = []
        elif mutation == "label_in_feature":
            candidate["features"]["label"] = 1
        files = {"events.jsonl": event, "candidates.jsonl": candidate, "predictions/production-v1.jsonl": prediction}
        metadata = {}
        for name, row in files.items():
            data = canonical_json(row) + "\n"
            (bundle / name).write_text(data, encoding="utf-8")
            import hashlib
            metadata[name] = {"path": name, "sha256": hashlib.sha256(data.encode()).hexdigest(), "records": 1}
        manifest = {"schema_version": "mal-eval-manifest/v1", "bundle_id": "golden", "created_at": "2026-03-15T00:00:00Z",
                    "reconstruction_quality": "exact", "time_range": {"from": "2026-03-01T00:00:00Z", "until": "2026-04-15T00:00:00Z"},
                    "cutoffs": ["2026-03-15T00:00:00Z"], "objectives": ["resume"],
                    "horizons": {"discovery": "90d", "resume": "30d", "backlog": "30d", "new_season": "90d", "new_episode": "14d"},
                    "files": metadata, "hash_algorithm": "sha256"}
        (bundle / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
        return bundle

    def test_golden_bundle_and_stable_leakage_reason_codes(self) -> None:
        from mal_updater.evaluation.bundle import validate_bundle
        with tempfile.TemporaryDirectory() as td:
            self.assertTrue(validate_bundle(self._bundle(Path(td))).valid)
        for reason in ("future_observation", "future_occurrence", "expired_fact", "candidate_not_in_universe", "missing_attribution", "label_in_feature"):
            with self.subTest(reason=reason), tempfile.TemporaryDirectory() as td:
                report = validate_bundle(self._bundle(Path(td), mutation=reason))
                self.assertFalse(report.valid)
                self.assertIn(reason, {item["reason_code"] for item in report.violations})


if __name__ == "__main__":
    unittest.main()
