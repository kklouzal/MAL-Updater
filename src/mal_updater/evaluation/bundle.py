from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
from typing import Any, Iterable, Protocol, Sequence

from .resume import ReplayQuery


class EventStore(Protocol):
    def evidence_as_of(self, query: ReplayQuery) -> Iterable[dict[str, Any]]: ...
    def outcomes_after(self, query: ReplayQuery) -> Iterable[dict[str, Any]]: ...


class CandidateGenerator(Protocol):
    def generate(self, query: ReplayQuery, evidence: Iterable[dict[str, Any]]) -> Iterable[dict[str, Any]]: ...


class RankingPolicy(Protocol):
    policy_id: str
    policy_version: str
    def rank(self, query: ReplayQuery, candidates: Sequence[dict[str, Any]], *, k: int) -> Sequence[dict[str, Any]]: ...


@dataclass(frozen=True, slots=True)
class ValidationReport:
    valid: bool
    violations: tuple[dict[str, str], ...]
    record_counts: dict[str, int]


@dataclass(frozen=True, slots=True)
class RunManifest:
    files: dict[str, str]


@dataclass(frozen=True, slots=True)
class LabelPolicyV1:
    policy_id: str = "label-policy/v1"


@dataclass(frozen=True, slots=True)
class EvalConfigV1:
    k: int = 5


@dataclass(frozen=True, slots=True)
class EvaluationReportV1:
    payload: dict[str, Any]


def _dt(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("timezone is required")
    return parsed.astimezone(timezone.utc)


def _jsonl(path: Path) -> list[dict[str, Any]]:
    rows = []
    for number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        if not line.strip():
            continue
        value = json.loads(line)
        if not isinstance(value, dict):
            raise ValueError(f"line {number} is not an object")
        rows.append(value)
    return rows


def _violation(code: str, location: str, detail: str) -> dict[str, str]:
    return {"reason_code": code, "location": location, "detail": detail}


def validate_bundle(path: Path, *, exact_required: bool = True) -> ValidationReport:
    bundle = Path(path)
    violations: list[dict[str, str]] = []
    counts: dict[str, int] = {}
    try:
        manifest = json.loads((bundle / "manifest.json").read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        return ValidationReport(False, (_violation("invalid_manifest", "manifest.json", type(exc).__name__),), counts)
    if manifest.get("schema_version") != "mal-eval-manifest/v1":
        violations.append(_violation("unknown_schema_version", "manifest.json", "expected mal-eval-manifest/v1"))
    if exact_required and manifest.get("reconstruction_quality") != "exact":
        violations.append(_violation("inexact_reconstruction", "manifest.json", "exact reconstruction required"))
    expected_versions = {"events.jsonl": "mal-eval-event/v1", "candidates.jsonl": "mal-eval-candidate/v1"}
    records: dict[str, list[dict[str, Any]]] = {}
    for name, version in expected_versions.items():
        try:
            records[name] = _jsonl(bundle / name)
        except (OSError, ValueError, json.JSONDecodeError) as exc:
            violations.append(_violation("invalid_jsonl", name, type(exc).__name__))
            records[name] = []
        counts[name] = len(records[name])
        for index, row in enumerate(records[name], 1):
            if row.get("schema_version") != version:
                violations.append(_violation("unknown_schema_version", f"{name}:{index}", f"expected {version}"))
    predictions: list[dict[str, Any]] = []
    prediction_dir = bundle / "predictions"
    if prediction_dir.exists():
        for file in sorted(prediction_dir.glob("*.jsonl")):
            try:
                rows = _jsonl(file)
            except (OSError, ValueError, json.JSONDecodeError) as exc:
                violations.append(_violation("invalid_jsonl", str(file.relative_to(bundle)), type(exc).__name__))
                continue
            counts[str(file.relative_to(bundle))] = len(rows)
            for index, row in enumerate(rows, 1):
                if row.get("schema_version") != "mal-eval-prediction/v1":
                    violations.append(_violation("unknown_schema_version", f"{file.name}:{index}", "expected mal-eval-prediction/v1"))
            predictions.extend(rows)
    event_ids = {row.get("event_id") for row in records["events.jsonl"]}
    candidates = {(row.get("query_id"), row.get("item_id")): row for row in records["candidates.jsonl"]}
    event_by_id = {row.get("event_id"): row for row in records["events.jsonl"]}
    for index, candidate in enumerate(records["candidates.jsonl"], 1):
        location = f"candidates.jsonl:{index}"
        try:
            cutoff = _dt(candidate["cutoff_at"])
            max_observed = _dt(candidate["max_observed_at"])
        except (KeyError, TypeError, ValueError):
            violations.append(_violation("invalid_candidate", location, "missing or invalid cutoff/max_observed_at"))
            continue
        if max_observed > cutoff:
            violations.append(_violation("future_observation", location, "max_observed_at exceeds cutoff"))
        evidence = candidate.get("evidence_event_ids")
        if not isinstance(evidence, list) or not evidence or any(item not in event_ids for item in evidence):
            violations.append(_violation("missing_attribution", location, "unknown or empty evidence_event_ids"))
        else:
            for event_id in evidence:
                event = event_by_id[event_id]
                try:
                    if _dt(event["observed_at"]) > cutoff:
                        violations.append(_violation("future_observation", location, f"event {event_id} observed after cutoff"))
                    if _dt(event["occurred_at"]) > cutoff:
                        violations.append(_violation("future_occurrence", location, f"event {event_id} occurred after cutoff"))
                    if _dt(event["effective_from"]) > cutoff or (event.get("effective_to") and cutoff >= _dt(event["effective_to"])):
                        violations.append(_violation("expired_fact", location, f"event {event_id} is not effective at cutoff"))
                except (KeyError, TypeError, ValueError):
                    violations.append(_violation("invalid_event", location, f"event {event_id} has invalid temporal fields"))
        if any(key.lower() in {"label", "binary_relevance", "graded_relevance", "outcome"} for key in candidate.get("features", {})):
            violations.append(_violation("label_in_feature", location, "label-like feature present"))
    for index, event in enumerate(records["events.jsonl"], 1):
        location = f"events.jsonl:{index}"
        try:
            if _dt(event["effective_to"]) <= _dt(event["effective_from"]) if event.get("effective_to") else False:
                violations.append(_violation("expired_fact", location, "empty effective interval"))
        except (KeyError, TypeError, ValueError):
            violations.append(_violation("invalid_event", location, "invalid temporal fields"))
        payload = event.get("payload")
        expected = hashlib.sha256(json.dumps(payload, ensure_ascii=False, separators=(",", ":"), sort_keys=True).encode()).hexdigest()
        if event.get("payload_sha256") != expected:
            violations.append(_violation("payload_hash_mismatch", location, "payload_sha256 mismatch"))
    for index, prediction in enumerate(predictions, 1):
        if (prediction.get("query_id"), prediction.get("item_id")) not in candidates:
            violations.append(_violation("candidate_not_in_universe", f"predictions:{index}", "prediction lacks candidate"))
    files = manifest.get("files", {})
    for metadata in files.values() if isinstance(files, dict) else []:
        if not isinstance(metadata, dict) or not isinstance(metadata.get("path"), str):
            continue
        file = bundle / metadata["path"]
        try:
            digest = hashlib.sha256(file.read_bytes()).hexdigest()
        except OSError:
            violations.append(_violation("missing_file", metadata["path"], "manifest file missing"))
        else:
            if metadata.get("sha256") != digest:
                violations.append(_violation("file_hash_mismatch", metadata["path"], "SHA-256 mismatch"))
    return ValidationReport(not violations, tuple(violations), counts)


def materialize_candidates(store: EventStore, queries: Sequence[ReplayQuery], generator: CandidateGenerator) -> RunManifest:
    raise NotImplementedError("bundle materialization is outside resume slice 1")


def build_labels(store: EventStore, queries: Sequence[ReplayQuery], candidates: Iterable[dict[str, Any]], policy: LabelPolicyV1) -> Iterable[dict[str, Any]]:
    for query in queries:
        outcomes = list(store.outcomes_after(query))
        for candidate in candidates:
            if candidate.get("query_id") != query.query_id:
                continue
            matches = [event for event in outcomes if event.get("entity", {}).get("entity_id") == candidate.get("item_id")]
            yield {"schema_version": "mal-eval-label/v1", "query_id": query.query_id,
                   "item_id": candidate["item_id"], "binary_relevance": 1 if matches else 0,
                   "graded_relevance": 2 if matches else 0,
                   "label_state": "positive" if matches else "unobserved",
                   "first_positive_at": min((event["occurred_at"] for event in matches), default=None),
                   "outcome_event_ids": sorted({event["event_id"] for event in matches}),
                   "observable_days": 30, "exposure_known": False,
                   "reason_codes": ["provider_play_started"] if matches else ["insufficient_observability"]}


def evaluate(predictions: Iterable[dict[str, Any]], labels: Iterable[dict[str, Any]], *, config: EvalConfigV1) -> EvaluationReportV1:
    from .resume import _metrics
    prediction_rows, label_rows = list(predictions), list(labels)
    known = {row["item_id"]: row.get("binary_relevance") == 1 for row in label_rows}
    return EvaluationReportV1({"schema_version": "mal-eval-report/v1", "objective": "resume",
                               "metrics": _metrics(prediction_rows, known, config.k),
                               "counts": {"predictions": len(prediction_rows), "labels": len(label_rows), "positives": sum(known.values())}})


def compare(baseline: Any, contender: Any, *, bootstrap: Any) -> dict[str, Any]:
    raise NotImplementedError("policy comparison is outside resume slice 1")
