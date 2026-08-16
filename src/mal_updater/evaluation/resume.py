from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from contextlib import closing
import hashlib
import json
import math
from pathlib import Path
import sqlite3
from typing import Any

from .events import event_row_to_v1

REPORT_VERSION = "mal-eval-resume-report/v1"
CANDIDATE_VERSION = "mal-eval-candidate/v1"
PREDICTION_VERSION = "mal-eval-prediction/v1"
POLICY_ID = "resume-production-v1"
POLICY_VERSION = "1"
FEATURE_VERSION = "resume-features/v1"


def _dt(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("timestamps must include a timezone")
    return parsed.astimezone(timezone.utc)


def _iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


@dataclass(frozen=True, slots=True)
class ReplayQuery:
    query_id: str
    user_id: str
    cutoff_at: datetime
    horizon_end_at: datetime
    objective: str = "resume"


@dataclass(frozen=True, slots=True)
class ResumePolicy:
    minimum_progress_ratio: float = 0.05
    minimum_progress_ms: int = 120_000
    completion_ratio: float = 0.95
    k: int = 5

    def __post_init__(self) -> None:
        if not 0 <= self.minimum_progress_ratio < self.completion_ratio <= 1:
            raise ValueError("require 0 <= minimum_progress_ratio < completion_ratio <= 1")
        if self.minimum_progress_ms < 0 or self.k < 1:
            raise ValueError("minimum_progress_ms must be non-negative and k positive")


def _open_read_only(db_path: Path) -> sqlite3.Connection:
    # Keep SQLite's WAL visibility/locking semantics while forbidding writes.
    # ``immutable=1`` would be stale when a live database has committed WAL pages.
    conn = sqlite3.connect(f"file:{Path(db_path).resolve()}?mode=ro", uri=True)
    conn.execute("PRAGMA query_only=ON")
    conn.row_factory = sqlite3.Row
    return conn


def _evidence(conn: sqlite3.Connection, query: ReplayQuery) -> list[dict[str, Any]]:
    cutoff = _iso(query.cutoff_at)
    rows = conn.execute(
        """SELECT * FROM evaluation_events
           WHERE user_id=? AND event_type='provider_play'
             AND observed_at<=? AND occurred_at<=? AND effective_from<=?
             AND (effective_to IS NULL OR effective_to>?)
           ORDER BY occurred_at, observed_at, event_id""",
        (query.user_id, cutoff, cutoff, cutoff, cutoff),
    ).fetchall()
    return [event_row_to_v1(row) for row in rows]


def _labels(conn: sqlite3.Connection, query: ReplayQuery, candidates: list[dict[str, Any]]) -> dict[str, bool]:
    cutoff, horizon = _iso(query.cutoff_at), _iso(query.horizon_end_at)
    ids = {candidate["item_id"] for candidate in candidates}
    if not ids:
        return {}
    rows = conn.execute(
        """SELECT * FROM evaluation_events
           WHERE user_id=? AND event_type='provider_play'
             AND observed_at>? AND observed_at<=? AND occurred_at>? AND occurred_at<=?
             AND effective_from<=? AND (effective_to IS NULL OR effective_to>?)
           ORDER BY occurred_at, observed_at, event_id""",
        (query.user_id, cutoff, horizon, cutoff, horizon, horizon, cutoff),
    ).fetchall()
    return {item_id: False for item_id in ids} | {
        row["entity_id"]: True for row in rows if row["entity_id"] in ids
    }


def _candidate(query: ReplayQuery, event: dict[str, Any], policy: ResumePolicy) -> dict[str, Any]:
    payload, entity = event["payload"], event["entity"]
    ratio = payload.get("completion_ratio")
    position = payload.get("playback_position_ms")
    return {
        "schema_version": CANDIDATE_VERSION, "query_id": query.query_id,
        "user_id": query.user_id, "cutoff_at": _iso(query.cutoff_at),
        "horizon_end_at": _iso(query.horizon_end_at), "objective": "resume",
        "item_id": entity["entity_id"], "mal_anime_id": None,
        "provider_item_ids": [entity["provider_episode_id"]], "eligible": True,
        "eligibility_reasons": ["latest_partial_episode", "minimum_progress_met"],
        "sources": [{"source_type": "provider_watch_history", "source_item_id": entity["provider_episode_id"],
                     "target_item_id": entity["entity_id"], "provider": entity["provider"],
                     "weight": None, "votes": None, "as_of": event["observed_at"],
                     "evidence_event_ids": [event["event_id"]]}],
        "availability": [], "english_dub": [],
        "features": {"episode_number": payload.get("episode_number"),
                     "completion_ratio": ratio, "playback_position_ms": position,
                     "duration_ms": payload.get("duration_ms")},
        "feature_version": FEATURE_VERSION, "evidence_event_ids": [event["event_id"]],
        "max_observed_at": event["observed_at"],
    }


def _generate(query: ReplayQuery, events: list[dict[str, Any]], policy: ResumePolicy) -> tuple[list[dict[str, Any]], dict[str, int]]:
    latest_by_episode: dict[str, dict[str, Any]] = {}
    exclusions: dict[str, int] = {}
    for event in events:
        latest_by_episode[event["entity"]["entity_id"]] = event
    partial_by_series: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for event in latest_by_episode.values():
        payload, entity = event["payload"], event["entity"]
        ratio, position = payload.get("completion_ratio"), payload.get("playback_position_ms")
        assertion = payload.get("completion_assertion")
        # Uncertain HIDIVE history never contributes a completed episode. It may
        # only be a resume candidate when measured position/ratio proves partial progress.
        measured = isinstance(ratio, (int, float)) or isinstance(position, int)
        if not measured:
            exclusions["no_measured_progress"] = exclusions.get("no_measured_progress", 0) + 1
            continue
        if assertion == "confirmed" or (isinstance(ratio, (int, float)) and ratio >= policy.completion_ratio):
            exclusions["completed"] = exclusions.get("completed", 0) + 1
            continue
        ratio_ok = isinstance(ratio, (int, float)) and ratio >= policy.minimum_progress_ratio
        position_ok = isinstance(position, int) and position >= policy.minimum_progress_ms
        if not (ratio_ok or position_ok):
            exclusions["false_start_below_minimum_progress"] = exclusions.get("false_start_below_minimum_progress", 0) + 1
            continue
        key = (str(entity["provider"]), str(entity["provider_series_id"]))
        partial_by_series.setdefault(key, []).append(event)
    candidates = []
    for group in partial_by_series.values():
        selected = max(group, key=lambda event: (
            event["payload"].get("episode_number") if isinstance(event["payload"].get("episode_number"), int) else -1,
            event["occurred_at"], event["event_id"],
        ))
        candidates.append(_candidate(query, selected, policy))
        exclusions["older_partial_episode"] = exclusions.get("older_partial_episode", 0) + len(group) - 1
    candidates.sort(key=lambda candidate: (candidate["max_observed_at"], candidate["item_id"]), reverse=True)
    return candidates, exclusions


def _prediction(query: ReplayQuery, candidate: dict[str, Any], rank: int) -> dict[str, Any]:
    artifact = hashlib.sha256(f"{POLICY_ID}:{POLICY_VERSION}".encode()).hexdigest()
    ratio = candidate["features"].get("completion_ratio")
    score = float(ratio) if isinstance(ratio, (int, float)) else 0.0
    return {"schema_version": PREDICTION_VERSION, "query_id": query.query_id, "objective": "resume",
            "item_id": candidate["item_id"], "rank": rank, "score": score, "probability": None,
            "policy_id": POLICY_ID, "policy_version": POLICY_VERSION,
            "policy_artifact_sha256": artifact,
            "contributions": [{"feature": "completion_ratio", "value": score,
                               "source_event_ids": candidate["evidence_event_ids"]}]}


def _metrics(predictions: list[dict[str, Any]], labels: dict[str, bool], k: int) -> dict[str, Any]:
    if not labels or not any(labels.values()):
        return {name: None for name in ("precision_at_5", "recall_at_5", "ndcg_at_5", "mrr_at_5", "candidate_recall")}
    top = predictions[:k]
    hits = [1 if labels.get(item["item_id"], False) else 0 for item in top]
    positives = sum(labels.values())
    dcg = sum(hit / math.log2(index + 2) for index, hit in enumerate(hits))
    ideal = sum(1 / math.log2(index + 2) for index in range(min(positives, k)))
    first = next((index + 1 for index, hit in enumerate(hits) if hit), None)
    return {"precision_at_5": sum(hits) / k, "recall_at_5": sum(hits) / positives,
            "ndcg_at_5": dcg / ideal if ideal else 0.0, "mrr_at_5": 1 / first if first else 0.0,
            "candidate_recall": sum(1 for value in labels.values() if value) / positives}


def evaluate_resume(db_path: Path, query: ReplayQuery, policy: ResumePolicy = ResumePolicy()) -> dict[str, Any]:
    if query.objective != "resume" or query.horizon_end_at - query.cutoff_at != timedelta(days=30):
        raise ValueError("slice 1 supports only resume with an exact 30d horizon")
    # sqlite3.Connection's context manager commits/rolls back but does not close.
    # Explicitly close this read-only handle so repeated offline evaluations do
    # not retain file descriptors or emit ResourceWarning on interpreter cleanup.
    with closing(_open_read_only(db_path)) as conn:
        events = _evidence(conn, query)
        candidates, exclusions = _generate(query, events, policy)
        predictions = [_prediction(query, item, rank) for rank, item in enumerate(candidates[:policy.k], 1)]
        labels = _labels(conn, query, candidates)
    label_support = sum(labels.values())
    metrics = _metrics(predictions, labels, policy.k)
    unavailable = [] if label_support else ["ranking metrics require observed post-cutoff resume outcomes"]
    return {
        "schema_version": REPORT_VERSION, "objective": "resume", "query": {
            "query_id": query.query_id, "user_id": query.user_id,
            "cutoff_at": _iso(query.cutoff_at), "horizon_end_at": _iso(query.horizon_end_at)},
        "policy": {"policy_id": POLICY_ID, "policy_version": POLICY_VERSION,
                   "minimum_progress_ratio": policy.minimum_progress_ratio,
                   "minimum_progress_ms": policy.minimum_progress_ms,
                   "completion_ratio": policy.completion_ratio, "k": policy.k},
        "coverage": {"evidence_events": len(events), "candidates": len(candidates),
                     "predictions": len(predictions), "future_label_events": label_support,
                     "availability_known": 0, "english_dub_known": 0},
        "exclusion_reasons": exclusions, "candidates": candidates, "predictions": predictions,
        "labels": [{"item_id": key, "resumed": value} for key, value in sorted(labels.items())],
        "metrics": metrics | {"availability_precision": None, "english_dub_precision": None},
        "metrics_unavailable": unavailable + ["availability labels unavailable", "English-dub labels unavailable", "week-cluster CI unavailable in single-query slice"],
        "leakage_audit": {"status": "pass", "evidence_predicate": "observed_at <= cutoff_at AND occurred_at <= cutoff_at AND effective_from <= cutoff_at AND (effective_to IS NULL OR effective_to > cutoff_at)",
                          "label_predicate": "cutoff_at < observed_at,occurred_at <= horizon_end_at",
                          "max_evidence_observed_at": max((e["observed_at"] for e in events), default=None),
                          "candidate_label_separation": True},
        "reconstruction_quality": "exact", "read_only": True,
    }
