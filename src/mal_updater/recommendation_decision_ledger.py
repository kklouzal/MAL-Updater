from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

from .db import connect

RECOMMENDATION_POLICY_ID = "mal-updater-local-recommendations"
RECOMMENDATION_POLICY_VERSION = "2026-08-16.1"
RECOMMENDATION_SURFACE = "cli:recommend"
RECOMMENDATION_OBJECTIVE = "rank_local_recommendations"

_POLICY_ARTIFACT = {
    "candidate_order": (
        "candidate_ordinal records the complete eligible recommendation-builder order: "
        "priority_desc,provider_count_desc,title_casefold,provider_series_id"
    ),
    "eligibility": "recommendation-builder-output-after-suppression-and-provider-actionability-policy",
    "exposure_order": {
        "flat": "output list order after applying the positive output limit;unbounded emits all eligible candidates",
        "grouped": (
            "group sections,apply fair section-interleaved selection for a positive output limit,then assign exposure_rank "
            "in emitted section order and emitted item order;unbounded emits all grouped items in that same order"
        ),
    },
    "identity": "mal:<mal_anime_id> when available, otherwise provider:<provider>:<provider_series_id>, namespaced by kind",
    "policy_id": RECOMMENDATION_POLICY_ID,
    "policy_version": RECOMMENDATION_POLICY_VERSION,
    "score": "scorecard.total when present;priority is the production ordering value",
}
RECOMMENDATION_POLICY_ARTIFACT_SHA256 = hashlib.sha256(
    json.dumps(_POLICY_ARTIFACT, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
).hexdigest()

_EVIDENCE_TIME_KEYS = frozenset(
    {
        "account_observed_at",
        "catalog_observed_at",
        "fetched_at",
        "last_progress_seen_at",
        "last_seen_at",
        "last_verified_at",
        "last_watched_at",
        "observed_at",
        "updated_at",
    }
)


@dataclass(frozen=True, slots=True)
class RecommendationDecisionLedgerRun:
    run_id: str
    cutoff_at: str
    surface: str
    objective: str
    policy_id: str
    policy_version: str
    policy_artifact_sha256: str
    maximum_evidence_at: str | None
    output_limit: int | None
    candidate_count: int
    selected_count: int


@dataclass(frozen=True, slots=True)
class RecommendationDecisionLedgerItem:
    run_id: str
    item_identity: str
    candidate_ordinal: int
    exposure_rank: int | None
    selected: bool
    eligibility_state: str
    exposure_state: str
    kind: str
    provider: str | None
    provider_series_id: str | None
    mal_anime_id: int | None
    title: str
    priority: int | None
    score: float | None
    scorecard: dict[str, Any] | None
    reasons: list[Any]
    context: dict[str, Any] | None
    feature_evidence_payload_hash: str
    maximum_evidence_at: str | None


def _canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _utc_datetime(value: str, *, field: str) -> datetime:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field} must be a non-empty ISO-8601 timestamp")
    normalized = value.strip().replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise ValueError(f"{field} must be an ISO-8601 timestamp") from exc
    if parsed.tzinfo is None:
        raise ValueError(f"{field} must include a timezone")
    return parsed.astimezone(timezone.utc)


def _canonical_timestamp(value: str, *, field: str) -> str:
    return _utc_datetime(value, field=field).isoformat().replace("+00:00", "Z")


def _walk_evidence_timestamps(value: Any, *, key: str | None = None) -> Iterable[str]:
    if key in _EVIDENCE_TIME_KEYS and isinstance(value, str) and value.strip():
        yield value.strip()
    if isinstance(value, dict):
        for child_key, child in value.items():
            yield from _walk_evidence_timestamps(child, key=str(child_key))
    elif isinstance(value, list):
        for child in value:
            yield from _walk_evidence_timestamps(child)


def _maximum_evidence_at(row: dict[str, Any]) -> str | None:
    evidence = list(_walk_evidence_timestamps(row))
    if not evidence:
        return None
    parsed = [(_utc_datetime(value, field="evidence timestamp"), value) for value in evidence]
    return _canonical_timestamp(max(parsed, key=lambda item: item[0])[1], field="maximum_evidence_at")


def _scorecard(row: dict[str, Any]) -> dict[str, Any] | None:
    direct = row.get("scorecard")
    if isinstance(direct, dict):
        return direct
    context = row.get("context")
    if isinstance(context, dict) and isinstance(context.get("scorecard"), dict):
        return context["scorecard"]
    return None


def _mal_anime_id(row: dict[str, Any]) -> int | None:
    value = row.get("mal_anime_id")
    context = row.get("context")
    if value in (None, "") and isinstance(context, dict):
        value = context.get("mal_anime_id")
    try:
        return None if value in (None, "") else int(value)
    except (TypeError, ValueError):
        return None


def _float(value: Any) -> float | None:
    try:
        return None if value in (None, "") else float(value)
    except (TypeError, ValueError):
        return None


def _int(value: Any) -> int | None:
    try:
        return None if value in (None, "") else int(value)
    except (TypeError, ValueError):
        return None


def stable_recommendation_item_identity(row: dict[str, Any]) -> str:
    kind = str(row.get("kind") or "unknown").strip().lower()
    mal_anime_id = _mal_anime_id(row)
    if mal_anime_id is not None:
        return f"{kind}:mal:{mal_anime_id}"
    provider = str(row.get("provider") or "unknown").strip().lower()
    provider_series_id = str(row.get("provider_series_id") or "").strip()
    if provider_series_id:
        return f"{kind}:provider:{provider}:{provider_series_id}"
    raise ValueError("recommendation candidate lacks a stable MAL or provider-series identity")


def feature_evidence_payload(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "availability_providers": row.get("available_via_providers") or row.get("providers") or [],
        "context": row.get("context") if isinstance(row.get("context"), dict) else None,
        "kind": row.get("kind"),
        "mal_anime_id": _mal_anime_id(row),
        "provider": row.get("provider"),
        "provider_series_id": row.get("provider_series_id"),
        "reasons": row.get("reasons") if isinstance(row.get("reasons"), list) else [],
        "scorecard": _scorecard(row),
    }


def insert_recommendation_decision_ledger(
    db_path: Path,
    rows: Iterable[dict[str, Any]],
    *,
    run_id: str,
    cutoff_at: str,
    output_limit: int | None,
    selected_item_identities_in_exposure_order: Iterable[str] | None = None,
    surface: str = RECOMMENDATION_SURFACE,
    objective: str = RECOMMENDATION_OBJECTIVE,
    policy_id: str = RECOMMENDATION_POLICY_ID,
    policy_version: str = RECOMMENDATION_POLICY_VERSION,
    policy_artifact_sha256: str = RECOMMENDATION_POLICY_ARTIFACT_SHA256,
) -> RecommendationDecisionLedgerRun:
    cutoff = _canonical_timestamp(cutoff_at, field="cutoff_at")
    cutoff_datetime = _utc_datetime(cutoff, field="cutoff_at")
    normalized_limit = None if output_limit is None or int(output_limit) <= 0 else int(output_limit)
    explicit_exposure_order = (
        None
        if selected_item_identities_in_exposure_order is None
        else list(selected_item_identities_in_exposure_order)
    )
    if explicit_exposure_order is not None and len(explicit_exposure_order) != len(set(explicit_exposure_order)):
        raise ValueError("selected recommendation identities must be unique in exposure order")
    exposure_rank_by_identity = (
        None
        if explicit_exposure_order is None
        else {identity: rank for rank, identity in enumerate(explicit_exposure_order, start=1)}
    )
    prepared: list[tuple[Any, ...]] = []
    identities: set[str] = set()
    maximum_evidence: datetime | None = None
    maximum_evidence_text: str | None = None

    for ordinal, source in enumerate(rows, start=1):
        if not isinstance(source, dict):
            raise ValueError("recommendation decision candidates must be dictionaries")
        row = dict(source)
        identity = stable_recommendation_item_identity(row)
        if identity in identities:
            raise ValueError(f"duplicate recommendation decision candidate identity: {identity}")
        identities.add(identity)
        evidence_at = _maximum_evidence_at(row)
        if evidence_at is not None:
            evidence_datetime = _utc_datetime(evidence_at, field="maximum_evidence_at")
            if evidence_datetime > cutoff_datetime:
                raise ValueError(
                    f"recommendation evidence is newer than cutoff: item={identity}, evidence={evidence_at}, cutoff={cutoff}"
                )
            if maximum_evidence is None or evidence_datetime > maximum_evidence:
                maximum_evidence = evidence_datetime
                maximum_evidence_text = evidence_at
        exposure_rank = (
            exposure_rank_by_identity.get(identity)
            if exposure_rank_by_identity is not None
            else ordinal if normalized_limit is None or ordinal <= normalized_limit else None
        )
        selected = exposure_rank is not None
        scorecard = _scorecard(row)
        context = row.get("context") if isinstance(row.get("context"), dict) else None
        reasons = row.get("reasons") if isinstance(row.get("reasons"), list) else []
        evidence_payload = feature_evidence_payload(row)
        evidence_hash = hashlib.sha256(_canonical_json(evidence_payload).encode("utf-8")).hexdigest()
        score = _float(row.get("scorecard_total") or row.get("score") or (scorecard or {}).get("total"))
        prepared.append(
            (
                run_id,
                identity,
                ordinal,
                exposure_rank,
                int(selected),
                "eligible",
                "selected" if selected else "eligible_not_selected_output_limit",
                str(row.get("kind") or "unknown"),
                row.get("provider"),
                row.get("provider_series_id"),
                _mal_anime_id(row),
                str(row.get("title") or ""),
                _int(row.get("priority")),
                score,
                None if scorecard is None else _canonical_json(scorecard),
                _canonical_json(reasons),
                None if context is None else _canonical_json(context),
                evidence_hash,
                evidence_at,
            )
        )

    candidate_count = len(prepared)
    selected_count = sum(int(row[4]) for row in prepared)
    if explicit_exposure_order is not None:
        unknown_selected_identities = set(explicit_exposure_order) - identities
        if unknown_selected_identities:
            raise ValueError(
                "selected recommendation identities are absent from the eligible candidate set: "
                + ", ".join(sorted(unknown_selected_identities))
            )
        if normalized_limit is not None and selected_count > normalized_limit:
            raise ValueError("selected recommendation count exceeds the positive output limit")
    if len(policy_artifact_sha256) != 64 or any(character not in "0123456789abcdef" for character in policy_artifact_sha256):
        raise ValueError("policy_artifact_sha256 must be a lowercase SHA-256 hex digest")
    run = RecommendationDecisionLedgerRun(
        run_id=run_id,
        cutoff_at=cutoff,
        surface=surface,
        objective=objective,
        policy_id=policy_id,
        policy_version=policy_version,
        policy_artifact_sha256=policy_artifact_sha256,
        maximum_evidence_at=maximum_evidence_text,
        output_limit=normalized_limit,
        candidate_count=candidate_count,
        selected_count=selected_count,
    )
    with connect(db_path) as conn:
        conn.execute("BEGIN IMMEDIATE")
        conn.execute(
            """
            INSERT INTO recommendation_decision_ledger_runs (
                run_id, cutoff_at, surface, objective, policy_id, policy_version,
                policy_artifact_sha256, maximum_evidence_at, output_limit,
                candidate_count, selected_count
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                run.run_id,
                run.cutoff_at,
                run.surface,
                run.objective,
                run.policy_id,
                run.policy_version,
                run.policy_artifact_sha256,
                run.maximum_evidence_at,
                run.output_limit,
                run.candidate_count,
                run.selected_count,
            ),
        )
        conn.executemany(
            """
            INSERT INTO recommendation_decision_ledger_items (
                run_id, item_identity, candidate_ordinal, exposure_rank, selected,
                eligibility_state, exposure_state, kind, provider, provider_series_id,
                mal_anime_id, title, priority, score, scorecard_json, reasons_json,
                context_json, feature_evidence_payload_hash, maximum_evidence_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            prepared,
        )
        conn.commit()
    return run


def list_recommendation_decision_ledger_items(
    db_path: Path,
    *,
    run_id: str,
) -> list[RecommendationDecisionLedgerItem]:
    with connect(db_path) as conn:
        rows = conn.execute(
            "SELECT * FROM recommendation_decision_ledger_items WHERE run_id = ? ORDER BY candidate_ordinal",
            (run_id,),
        ).fetchall()
    return [
        RecommendationDecisionLedgerItem(
            run_id=str(row["run_id"]),
            item_identity=str(row["item_identity"]),
            candidate_ordinal=int(row["candidate_ordinal"]),
            exposure_rank=None if row["exposure_rank"] is None else int(row["exposure_rank"]),
            selected=bool(row["selected"]),
            eligibility_state=str(row["eligibility_state"]),
            exposure_state=str(row["exposure_state"]),
            kind=str(row["kind"]),
            provider=row["provider"],
            provider_series_id=row["provider_series_id"],
            mal_anime_id=row["mal_anime_id"],
            title=str(row["title"]),
            priority=row["priority"],
            score=row["score"],
            scorecard=json.loads(row["scorecard_json"]) if row["scorecard_json"] else None,
            reasons=json.loads(row["reasons_json"]),
            context=json.loads(row["context_json"]) if row["context_json"] else None,
            feature_evidence_payload_hash=str(row["feature_evidence_payload_hash"]),
            maximum_evidence_at=row["maximum_evidence_at"],
        )
        for row in rows
    ]
