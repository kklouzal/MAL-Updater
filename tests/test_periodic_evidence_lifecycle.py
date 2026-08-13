from __future__ import annotations

import json
import os
import subprocess
import sys
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

from mal_updater.periodic_evidence_lifecycle import (
    periodic_evidence_is_due,
    periodic_evidence_refresh_due_at,
    periodic_evidence_schedule_key,
    stable_periodic_evidence_jitter_days,
)


class PeriodicEvidenceLifecycleTests(unittest.TestCase):
    def test_canonical_schedule_is_deterministic_across_hash_seeds(self) -> None:
        kwargs = {"surface": "mal_detail", "identity": {"mal_anime_id": 42, "fields": "id,title"}}
        local = stable_periodic_evidence_jitter_days(**kwargs)
        code = (
            "import json; from mal_updater.periodic_evidence_lifecycle import "
            "stable_periodic_evidence_jitter_days as f; "
            f"print(json.dumps(f(**{kwargs!r})))"
        )
        outputs = []
        for seed in ("1", "999"):
            env = dict(os.environ, PYTHONHASHSEED=seed, PYTHONPATH=str(Path(__file__).resolve().parents[1] / "src"))
            outputs.append(json.loads(subprocess.check_output([sys.executable, "-c", code], text=True, env=env)))
        self.assertEqual([local, local], outputs)
        self.assertTrue(periodic_evidence_schedule_key(**kwargs).startswith("sha256:"))

    def test_default_boundary_is_within_105_to_135_days(self) -> None:
        anchor = datetime(2026, 1, 1, tzinfo=timezone.utc)
        due = periodic_evidence_refresh_due_at(
            successful_at=anchor,
            surface="complete_public_userrecs_harvest",
            identity={"source_mal_anime_id": 42},
        )
        assert due is not None
        delta = datetime.fromisoformat(due.replace("Z", "+00:00")) - anchor
        self.assertGreaterEqual(delta, timedelta(days=105))
        self.assertLessEqual(delta, timedelta(days=135))

    def test_due_boundary_and_zero_disable_are_explicit(self) -> None:
        anchor = "2026-01-01T00:00:00Z"
        identity = {"cache_key": "abc"}
        due = periodic_evidence_refresh_due_at(
            successful_at=anchor,
            surface="mal_search_positive",
            identity=identity,
            target_days=120,
            jitter_days=15,
        )
        assert due is not None
        before = datetime.fromisoformat(due.replace("Z", "+00:00")) - timedelta(seconds=1)
        self.assertFalse(periodic_evidence_is_due(
            successful_at=anchor, surface="mal_search_positive", identity=identity, now=before
        ))
        self.assertTrue(periodic_evidence_is_due(
            successful_at=anchor, surface="mal_search_positive", identity=identity, now=due
        ))
        self.assertIsNone(periodic_evidence_refresh_due_at(
            successful_at=anchor, surface="mal_search_positive", identity=identity, target_days=0
        ))
        self.assertTrue(periodic_evidence_is_due(
            successful_at=anchor, surface="mal_search_positive", identity=identity, target_days=0
        ))

    def test_malformed_success_timestamp_fails_due_not_usable(self) -> None:
        self.assertTrue(periodic_evidence_is_due(
            successful_at="not-a-time", surface="provider_detail", identity={"provider_series_id": "x"}
        ))
