from __future__ import annotations

import json
import re
import shlex
from collections import Counter

from .mapping import normalize_title


def _review_queue_item_label(
    item: object,
    *,
    provider_series_titles: dict[tuple[str, str], dict[str, str | None]] | None = None,
) -> dict[str, object]:
    payload = getattr(item, "payload", None)
    title = None
    if isinstance(payload, dict):
        for key in ("title", "provider_title", "crunchyroll_title", "season_title", "mal_title", "suggested_mal_title"):
            value = payload.get(key)
            if isinstance(value, str) and value.strip():
                title = value.strip()
                break
    provider = getattr(item, "provider", None)
    provider_series_id = getattr(item, "provider_series_id", None)
    series_row = (
        provider_series_titles.get((provider, provider_series_id))
        if provider_series_titles and isinstance(provider, str) and isinstance(provider_series_id, str)
        else None
    )
    if title is None and isinstance(series_row, dict):
        for key in ("season_title", "title"):
            value = series_row.get(key)
            if isinstance(value, str) and value.strip():
                title = value.strip()
                break
    return {
        "provider_series_id": provider_series_id,
        "issue_type": getattr(item, "issue_type", None),
        "severity": getattr(item, "severity", None),
        "title": title,
        "created_at": getattr(item, "created_at", None),
    }

_TRAILING_INSTALLMENT_CLUSTER_RE = re.compile(
    r"(?:\b(?:season|part|cour)\s*\d+\b|\b\d+(?:st|nd|rd|th)\s+season\b|\b(?:final|last)\s+season\b|\b(?:part|cour)\s+[ivx]+\b|\b[ivx]+\b)$",
    re.IGNORECASE,
)

def _review_queue_title_cluster_key(title: str | None) -> str | None:
    if not isinstance(title, str) or not title.strip():
        return None
    value = title.strip()
    previous = None
    while previous != value:
        previous = value
        value = _TRAILING_INSTALLMENT_CLUSTER_RE.sub("", value).strip(" -:()[]")
    normalized = normalize_title(value)
    return normalized or None

def _review_queue_reason_family(reason: str | None) -> str | None:
    if not isinstance(reason, str) or not reason.strip():
        return None
    normalized_reason = reason.strip()
    if "=" in normalized_reason:
        return normalized_reason.split("=", 1)[0].strip() or normalized_reason
    return normalized_reason

def _review_queue_fix_strategy_key(payload: dict[str, object], *, canonicalize_reasons: bool = False) -> str | None:
    decision = payload.get("decision")
    if not isinstance(decision, str) or not decision.strip():
        return None
    parts = [decision.strip()]
    reasons = payload.get("reasons")
    normalized_reasons: list[str] = []
    if isinstance(reasons, list):
        normalized_reason_values: set[str] = set()
        for reason in reasons:
            if not isinstance(reason, str) or not reason.strip():
                continue
            normalized_reason = reason.strip()
            if canonicalize_reasons:
                normalized_reason = _review_queue_reason_family(normalized_reason) or normalized_reason
            normalized_reason_values.add(normalized_reason)
        normalized_reasons = sorted(normalized_reason_values)
    parts.extend(normalized_reasons)
    return " | ".join(parts)

def _review_queue_cluster_strategy_key(
    title: str | None,
    payload: dict[str, object],
    *,
    canonicalize_reasons: bool = False,
) -> dict[str, str] | None:
    cluster = _review_queue_title_cluster_key(title)
    strategy = _review_queue_fix_strategy_key(payload, canonicalize_reasons=canonicalize_reasons)
    if cluster is None or strategy is None:
        return None
    return {
        "cluster": cluster,
        "strategy": strategy,
        "key": f"{cluster} || {strategy}",
    }

def _review_queue_command_args(
    command: str,
    *,
    status: str,
    issue_type: str | None,
    title_cluster: str | None = None,
    fix_strategy: str | None = None,
    cluster_strategy: str | None = None,
    decision: str | None = None,
    reason: str | None = None,
    reason_family: str | None = None,
    fix_strategy_family: str | None = None,
    cluster_strategy_family: str | None = None,
    limit: int | None = None,
) -> list[str]:
    args: list[str] = [command]
    status_value = status if command == "list-review-queue" and status != "open" else None
    option_pairs = [
        ("--status", status_value),
        ("--issue-type", issue_type),
        ("--title-cluster", title_cluster),
        ("--fix-strategy", fix_strategy),
        ("--cluster-strategy", cluster_strategy),
        ("--decision", decision),
        ("--reason", reason),
        ("--reason-family", reason_family),
        ("--fix-strategy-family", fix_strategy_family),
        ("--cluster-strategy-family", cluster_strategy_family),
    ]
    seen_pairs: set[tuple[str, str]] = set()
    for flag, value in option_pairs:
        if not isinstance(value, str):
            continue
        normalized_value = value.strip()
        if not normalized_value:
            continue
        pair = (flag, normalized_value)
        if pair in seen_pairs:
            continue
        args.extend([flag, normalized_value])
        seen_pairs.add(pair)
    if command == "resolve-review-queue" and isinstance(limit, int):
        args.extend(["--limit", str(limit)])
    return args

def _review_queue_drilldown_args(
    *,
    status: str,
    issue_type: str | None,
    title_cluster: str | None = None,
    fix_strategy: str | None = None,
    cluster_strategy: str | None = None,
    decision: str | None = None,
    reason: str | None = None,
    reason_family: str | None = None,
    fix_strategy_family: str | None = None,
    cluster_strategy_family: str | None = None,
) -> list[str]:
    return _review_queue_command_args(
        "list-review-queue",
        status=status,
        issue_type=issue_type,
        title_cluster=title_cluster,
        fix_strategy=fix_strategy,
        cluster_strategy=cluster_strategy,
        decision=decision,
        reason=reason,
        reason_family=reason_family,
        fix_strategy_family=fix_strategy_family,
        cluster_strategy_family=cluster_strategy_family,
    )

def _review_queue_status_action_command(status: str) -> str:
    return "reopen-review-queue" if status == "resolved" else "resolve-review-queue"

def _review_queue_status_action_args(
    *,
    status: str,
    issue_type: str | None,
    title_cluster: str | None = None,
    fix_strategy: str | None = None,
    cluster_strategy: str | None = None,
    decision: str | None = None,
    reason: str | None = None,
    reason_family: str | None = None,
    fix_strategy_family: str | None = None,
    cluster_strategy_family: str | None = None,
    limit: int = 20,
) -> list[str]:
    command = _review_queue_status_action_command(status)
    return _review_queue_command_args(
        command,
        status="resolved" if command == "reopen-review-queue" else "open",
        issue_type=issue_type,
        title_cluster=title_cluster,
        fix_strategy=fix_strategy,
        cluster_strategy=cluster_strategy,
        decision=decision,
        reason=reason,
        reason_family=reason_family,
        fix_strategy_family=fix_strategy_family,
        cluster_strategy_family=cluster_strategy_family,
        limit=limit if command == "resolve-review-queue" else None,
    )

def _review_queue_resolve_args(
    *,
    issue_type: str | None,
    title_cluster: str | None = None,
    fix_strategy: str | None = None,
    cluster_strategy: str | None = None,
    decision: str | None = None,
    reason: str | None = None,
    reason_family: str | None = None,
    fix_strategy_family: str | None = None,
    cluster_strategy_family: str | None = None,
    limit: int = 20,
) -> list[str]:
    return _review_queue_status_action_args(
        status="open",
        issue_type=issue_type,
        title_cluster=title_cluster,
        fix_strategy=fix_strategy,
        cluster_strategy=cluster_strategy,
        decision=decision,
        reason=reason,
        reason_family=reason_family,
        fix_strategy_family=fix_strategy_family,
        cluster_strategy_family=cluster_strategy_family,
        limit=limit,
    )

def _review_queue_status_action_fields(status: str, args: list[str]) -> dict[str, object]:
    command = _review_queue_status_action_command(status)
    action = "reopen" if command == "reopen-review-queue" else "resolve"
    payload: dict[str, object] = {
        "action": action,
        "action_args": args,
        "action_command": _build_review_queue_command(args),
    }
    if action == "resolve":
        payload["resolve_args"] = args
        payload["resolve_command"] = payload["action_command"]
    else:
        payload["reopen_args"] = args
        payload["reopen_command"] = payload["action_command"]
    return payload

def _review_queue_refresh_args(
    *,
    issue_type: str | None,
    provider_series_ids: list[str],
    mapping_limit: int = 5,
) -> list[str] | None:
    if issue_type != "mapping_review":
        return None
    normalized_ids = sorted(
        {
            value.strip()
            for value in provider_series_ids
            if isinstance(value, str) and value.strip()
        }
    )
    if not normalized_ids:
        return None
    args = ["refresh-mapping-review-queue"]
    for provider_series_id in normalized_ids:
        args.extend(["--provider-series-id", provider_series_id])
    if mapping_limit != 5:
        args.extend(["--mapping-limit", str(mapping_limit)])
    return args

def _review_queue_refresh_fields(
    *,
    issue_type: str | None,
    provider_series_ids: list[str],
    mapping_limit: int = 5,
) -> dict[str, object]:
    args = _review_queue_refresh_args(
        issue_type=issue_type,
        provider_series_ids=provider_series_ids,
        mapping_limit=mapping_limit,
    )
    if args is None:
        return {}
    return {
        "refresh_provider_series_ids": provider_series_ids,
        "refresh_args": args,
        "refresh_command": _build_review_queue_command(args),
    }

def _review_queue_apply_worklist_args(
    *,
    status: str,
    issue_type: str | None,
    limit: int,
    per_bucket_limit: int,
    title_cluster: str | None = None,
    fix_strategy: str | None = None,
    cluster_strategy: str | None = None,
    decision: str | None = None,
    reason: str | None = None,
    reason_family: str | None = None,
    fix_strategy_family: str | None = None,
    cluster_strategy_family: str | None = None,
) -> list[str]:
    args = _review_queue_command_args(
        "review-queue-apply-worklist",
        status=status,
        issue_type=issue_type,
        title_cluster=title_cluster,
        fix_strategy=fix_strategy,
        cluster_strategy=cluster_strategy,
        decision=decision,
        reason=reason,
        reason_family=reason_family,
        fix_strategy_family=fix_strategy_family,
        cluster_strategy_family=cluster_strategy_family,
    )
    args.extend(["--limit", str(limit), "--per-bucket-limit", str(per_bucket_limit)])
    return args

def _review_queue_refresh_worklist_args(
    *,
    status: str,
    issue_type: str | None,
    limit: int,
    per_bucket_limit: int,
    mapping_limit: int,
    title_cluster: str | None = None,
    fix_strategy: str | None = None,
    cluster_strategy: str | None = None,
    decision: str | None = None,
    reason: str | None = None,
    reason_family: str | None = None,
    fix_strategy_family: str | None = None,
    cluster_strategy_family: str | None = None,
) -> list[str]:
    args = _review_queue_command_args(
        "review-queue-refresh-worklist",
        status=status,
        issue_type=issue_type,
        title_cluster=title_cluster,
        fix_strategy=fix_strategy,
        cluster_strategy=cluster_strategy,
        decision=decision,
        reason=reason,
        reason_family=reason_family,
        fix_strategy_family=fix_strategy_family,
        cluster_strategy_family=cluster_strategy_family,
    )
    args.extend(["--limit", str(limit), "--per-bucket-limit", str(per_bucket_limit), "--mapping-limit", str(mapping_limit)])
    return args

def _filter_review_queue_items(
    items: list[object],
    *,
    provider_series_titles: dict[str, dict[str, str | None]] | None = None,
    title_cluster: str | None = None,
    fix_strategy: str | None = None,
    cluster_strategy: str | None = None,
    decision: str | None = None,
    reason: str | None = None,
    reason_family: str | None = None,
    fix_strategy_family: str | None = None,
    cluster_strategy_family: str | None = None,
) -> list[object]:
    normalized_title_cluster = _review_queue_title_cluster_key(title_cluster) if title_cluster else None
    normalized_fix_strategy = fix_strategy.strip() if isinstance(fix_strategy, str) and fix_strategy.strip() else None
    normalized_cluster_strategy = cluster_strategy.strip() if isinstance(cluster_strategy, str) and cluster_strategy.strip() else None
    normalized_decision = decision.strip() if isinstance(decision, str) and decision.strip() else None
    normalized_reason = reason.strip() if isinstance(reason, str) and reason.strip() else None
    normalized_reason_family = _review_queue_reason_family(reason_family) if isinstance(reason_family, str) and reason_family.strip() else None
    normalized_fix_strategy_family = fix_strategy_family.strip() if isinstance(fix_strategy_family, str) and fix_strategy_family.strip() else None
    normalized_cluster_strategy_family = cluster_strategy_family.strip() if isinstance(cluster_strategy_family, str) and cluster_strategy_family.strip() else None
    if (
        normalized_title_cluster is None
        and normalized_fix_strategy is None
        and normalized_cluster_strategy is None
        and normalized_decision is None
        and normalized_reason is None
        and normalized_reason_family is None
        and normalized_fix_strategy_family is None
        and normalized_cluster_strategy_family is None
    ):
        return items
    filtered: list[object] = []
    for item in items:
        payload = getattr(item, "payload", None)
        if not isinstance(payload, dict):
            continue
        label = _review_queue_item_label(item, provider_series_titles=provider_series_titles)
        if normalized_title_cluster is not None:
            if _review_queue_title_cluster_key(label.get("title")) != normalized_title_cluster:
                continue
        if normalized_fix_strategy is not None and _review_queue_fix_strategy_key(payload) != normalized_fix_strategy:
            continue
        if normalized_cluster_strategy is not None:
            cluster_strategy_parts = _review_queue_cluster_strategy_key(label.get("title"), payload)
            if cluster_strategy_parts is None or cluster_strategy_parts["key"] != normalized_cluster_strategy:
                continue
        if normalized_decision is not None:
            payload_decision = payload.get("decision")
            if not isinstance(payload_decision, str) or payload_decision.strip() != normalized_decision:
                continue
        if normalized_reason is not None or normalized_reason_family is not None:
            reasons = payload.get("reasons")
            normalized_reasons = {
                item_reason.strip() for item_reason in reasons if isinstance(item_reason, str) and item_reason.strip()
            } if isinstance(reasons, list) else set()
            if normalized_reason is not None and normalized_reason not in normalized_reasons:
                continue
            if normalized_reason_family is not None:
                normalized_reason_families = {_review_queue_reason_family(item_reason) for item_reason in normalized_reasons}
                normalized_reason_families.discard(None)
                if normalized_reason_family not in normalized_reason_families:
                    continue
        if normalized_fix_strategy_family is not None:
            if _review_queue_fix_strategy_key(payload, canonicalize_reasons=True) != normalized_fix_strategy_family:
                continue
        if normalized_cluster_strategy_family is not None:
            cluster_strategy_family_parts = _review_queue_cluster_strategy_key(
                label.get("title"),
                payload,
                canonicalize_reasons=True,
            )
            if cluster_strategy_family_parts is None or cluster_strategy_family_parts["key"] != normalized_cluster_strategy_family:
                continue
        filtered.append(item)
    return filtered

def _summarize_review_queue(
    items: list[object],
    *,
    status: str,
    issue_type: str | None,
    provider_series_titles: dict[str, dict[str, str | None]] | None = None,
    title_cluster_filter: str | None = None,
    fix_strategy_filter: str | None = None,
    cluster_strategy_filter: str | None = None,
    decision_filter: str | None = None,
    reason_filter: str | None = None,
    reason_family_filter: str | None = None,
    fix_strategy_family_filter: str | None = None,
    cluster_strategy_family_filter: str | None = None,
) -> dict[str, object]:
    by_issue_type = Counter(getattr(item, "issue_type", None) for item in items)
    by_severity = Counter(getattr(item, "severity", None) for item in items)
    by_decision: Counter[str] = Counter()
    by_reason: Counter[str] = Counter()
    by_reason_family: Counter[str] = Counter()
    by_title_cluster: Counter[str] = Counter()
    by_fix_strategy: Counter[str] = Counter()
    by_fix_strategy_family: Counter[str] = Counter()
    by_cluster_strategy: Counter[str] = Counter()
    by_cluster_strategy_family: Counter[str] = Counter()
    decision_examples: dict[str, list[dict[str, object]]] = {}
    reason_examples: dict[str, list[dict[str, object]]] = {}
    reason_family_examples: dict[str, list[dict[str, object]]] = {}
    title_cluster_examples: dict[str, list[dict[str, object]]] = {}
    title_cluster_labels: dict[str, str] = {}
    fix_strategy_examples: dict[str, list[dict[str, object]]] = {}
    fix_strategy_family_examples: dict[str, list[dict[str, object]]] = {}
    cluster_strategy_examples: dict[str, list[dict[str, object]]] = {}
    cluster_strategy_family_examples: dict[str, list[dict[str, object]]] = {}
    cluster_strategy_parts: dict[str, dict[str, str]] = {}
    cluster_strategy_family_parts: dict[str, dict[str, str]] = {}
    decision_provider_series_ids: dict[str, list[str]] = {}
    reason_provider_series_ids: dict[str, list[str]] = {}
    reason_family_provider_series_ids: dict[str, list[str]] = {}
    title_cluster_provider_series_ids: dict[str, list[str]] = {}
    fix_strategy_provider_series_ids: dict[str, list[str]] = {}
    fix_strategy_family_provider_series_ids: dict[str, list[str]] = {}
    cluster_strategy_provider_series_ids: dict[str, list[str]] = {}
    cluster_strategy_family_provider_series_ids: dict[str, list[str]] = {}

    def add_provider_series_id(target: dict[str, list[str]], key: str, provider_series_id: str | None) -> None:
        if not isinstance(provider_series_id, str) or not provider_series_id.strip():
            return
        values = target.setdefault(key, [])
        normalized_id = provider_series_id.strip()
        if normalized_id in values or len(values) >= 20:
            return
        values.append(normalized_id)

    for item in items:
        payload = getattr(item, "payload", None)
        if not isinstance(payload, dict):
            continue
        label = _review_queue_item_label(item, provider_series_titles=provider_series_titles)
        provider_series_id = getattr(item, "provider_series_id", None)
        decision = payload.get("decision")
        if isinstance(decision, str) and decision.strip():
            normalized_decision = decision.strip()
            by_decision[normalized_decision] += 1
            examples = decision_examples.setdefault(normalized_decision, [])
            if len(examples) < 3:
                examples.append(label)
            add_provider_series_id(decision_provider_series_ids, normalized_decision, provider_series_id)
        title_cluster_key = _review_queue_title_cluster_key(label.get("title"))
        if title_cluster_key is not None:
            by_title_cluster[title_cluster_key] += 1
            title_cluster_labels.setdefault(title_cluster_key, str(label.get("title") or title_cluster_key))
            examples = title_cluster_examples.setdefault(title_cluster_key, [])
            if len(examples) < 3:
                examples.append(label)
            add_provider_series_id(title_cluster_provider_series_ids, title_cluster_key, provider_series_id)
        fix_strategy_key = _review_queue_fix_strategy_key(payload)
        if fix_strategy_key is not None:
            by_fix_strategy[fix_strategy_key] += 1
            examples = fix_strategy_examples.setdefault(fix_strategy_key, [])
            if len(examples) < 3:
                examples.append(label)
            add_provider_series_id(fix_strategy_provider_series_ids, fix_strategy_key, provider_series_id)
        fix_strategy_family_key = _review_queue_fix_strategy_key(payload, canonicalize_reasons=True)
        if fix_strategy_family_key is not None:
            by_fix_strategy_family[fix_strategy_family_key] += 1
            examples = fix_strategy_family_examples.setdefault(fix_strategy_family_key, [])
            if len(examples) < 3:
                examples.append(label)
            add_provider_series_id(fix_strategy_family_provider_series_ids, fix_strategy_family_key, provider_series_id)
        cluster_strategy = _review_queue_cluster_strategy_key(label.get("title"), payload)
        if cluster_strategy is not None:
            cluster_strategy_key = cluster_strategy["key"]
            by_cluster_strategy[cluster_strategy_key] += 1
            cluster_strategy_parts.setdefault(cluster_strategy_key, cluster_strategy)
            examples = cluster_strategy_examples.setdefault(cluster_strategy_key, [])
            if len(examples) < 3:
                examples.append(label)
            add_provider_series_id(cluster_strategy_provider_series_ids, cluster_strategy_key, provider_series_id)
        cluster_strategy_family = _review_queue_cluster_strategy_key(
            label.get("title"),
            payload,
            canonicalize_reasons=True,
        )
        if cluster_strategy_family is not None:
            cluster_strategy_family_key = cluster_strategy_family["key"]
            by_cluster_strategy_family[cluster_strategy_family_key] += 1
            cluster_strategy_family_parts.setdefault(cluster_strategy_family_key, cluster_strategy_family)
            examples = cluster_strategy_family_examples.setdefault(cluster_strategy_family_key, [])
            if len(examples) < 3:
                examples.append(label)
            add_provider_series_id(cluster_strategy_family_provider_series_ids, cluster_strategy_family_key, provider_series_id)
        reasons = payload.get("reasons")
        if isinstance(reasons, list):
            for reason in reasons:
                if not isinstance(reason, str) or not reason.strip():
                    continue
                normalized_reason = reason.strip()
                by_reason[normalized_reason] += 1
                examples = reason_examples.setdefault(normalized_reason, [])
                if len(examples) < 3:
                    examples.append(label)
                add_provider_series_id(reason_provider_series_ids, normalized_reason, provider_series_id)
                reason_family = _review_queue_reason_family(normalized_reason)
                if reason_family is None:
                    continue
                by_reason_family[reason_family] += 1
                family_examples = reason_family_examples.setdefault(reason_family, [])
                if len(family_examples) < 3:
                    family_examples.append(label)
                add_provider_series_id(reason_family_provider_series_ids, reason_family, provider_series_id)

    effective_refresh_issue_type = issue_type
    visible_issue_types = {key for key, value in by_issue_type.items() if key and value}
    if effective_refresh_issue_type is None and visible_issue_types == {"mapping_review"}:
        effective_refresh_issue_type = "mapping_review"

    return {
        "status": status,
        "issue_type_filter": issue_type,
        "title_cluster_filter": _review_queue_title_cluster_key(title_cluster_filter) if title_cluster_filter else None,
        "fix_strategy_filter": fix_strategy_filter.strip() if isinstance(fix_strategy_filter, str) and fix_strategy_filter.strip() else None,
        "cluster_strategy_filter": cluster_strategy_filter.strip() if isinstance(cluster_strategy_filter, str) and cluster_strategy_filter.strip() else None,
        "decision_filter": decision_filter.strip() if isinstance(decision_filter, str) and decision_filter.strip() else None,
        "reason_filter": reason_filter.strip() if isinstance(reason_filter, str) and reason_filter.strip() else None,
        "reason_family_filter": _review_queue_reason_family(reason_family_filter) if isinstance(reason_family_filter, str) and reason_family_filter.strip() else None,
        "fix_strategy_family_filter": fix_strategy_family_filter.strip() if isinstance(fix_strategy_family_filter, str) and fix_strategy_family_filter.strip() else None,
        "cluster_strategy_family_filter": cluster_strategy_family_filter.strip() if isinstance(cluster_strategy_family_filter, str) and cluster_strategy_family_filter.strip() else None,
        "count": len(items),
        "by_issue_type": dict(sorted((key, value) for key, value in by_issue_type.items() if key)),
        "by_severity": dict(sorted((key, value) for key, value in by_severity.items() if key)),
        "by_decision": dict(sorted(by_decision.items())),
        "decision_examples": {key: value for key, value in sorted(decision_examples.items())},
        "decision_drilldowns": {
            key: _review_queue_drilldown_args(
                status=status,
                issue_type=issue_type,
                title_cluster=title_cluster_filter,
                fix_strategy=fix_strategy_filter,
                cluster_strategy=cluster_strategy_filter,
                decision=key,
                reason=reason_filter,
            )
            for key in sorted(by_decision)
        },
        "decision_actions": {
            key: {
                **_review_queue_status_action_fields(
                    status,
                    _review_queue_status_action_args(
                        status=status,
                        issue_type=issue_type,
                        title_cluster=title_cluster_filter,
                        fix_strategy=fix_strategy_filter,
                        cluster_strategy=cluster_strategy_filter,
                        decision=key,
                        reason=reason_filter,
                    ),
                ),
                **_review_queue_refresh_fields(
                    issue_type=effective_refresh_issue_type,
                    provider_series_ids=decision_provider_series_ids.get(key, []),
                ),
            }
            for key in sorted(by_decision)
        },
        "decision_resolutions": {
            key: _review_queue_resolve_args(
                issue_type=issue_type,
                title_cluster=title_cluster_filter,
                fix_strategy=fix_strategy_filter,
                cluster_strategy=cluster_strategy_filter,
                decision=key,
                reason=reason_filter,
            )
            for key in sorted(by_decision)
            if status == "open"
        },
        "top_reasons": [
            {
                "reason": reason,
                "count": count,
                "examples": reason_examples.get(reason, []),
                "drilldown_args": _review_queue_drilldown_args(
                    status=status,
                    issue_type=issue_type,
                    title_cluster=title_cluster_filter,
                    fix_strategy=fix_strategy_filter,
                    cluster_strategy=cluster_strategy_filter,
                    decision=decision_filter,
                    reason=reason,
                ),
                **_review_queue_status_action_fields(
                    status,
                    _review_queue_status_action_args(
                        status=status,
                        issue_type=issue_type,
                        title_cluster=title_cluster_filter,
                        fix_strategy=fix_strategy_filter,
                        cluster_strategy=cluster_strategy_filter,
                        decision=decision_filter,
                        reason=reason,
                    ),
                ),
                **_review_queue_refresh_fields(
                    issue_type=effective_refresh_issue_type,
                    provider_series_ids=reason_provider_series_ids.get(reason, []),
                ),
            }
            for reason, count in by_reason.most_common(10)
        ],
        "top_reason_families": [
            {
                "reason_family": reason_family,
                "count": count,
                "examples": reason_family_examples.get(reason_family, []),
                "drilldown_args": _review_queue_drilldown_args(
                    status=status,
                    issue_type=issue_type,
                    title_cluster=title_cluster_filter,
                    fix_strategy=fix_strategy_filter,
                    cluster_strategy=cluster_strategy_filter,
                    decision=decision_filter,
                    reason=reason_filter,
                    reason_family=reason_family,
                ),
                **_review_queue_status_action_fields(
                    status,
                    _review_queue_status_action_args(
                        status=status,
                        issue_type=issue_type,
                        title_cluster=title_cluster_filter,
                        fix_strategy=fix_strategy_filter,
                        cluster_strategy=cluster_strategy_filter,
                        decision=decision_filter,
                        reason=reason_filter,
                        reason_family=reason_family,
                    ),
                ),
                **_review_queue_refresh_fields(
                    issue_type=effective_refresh_issue_type,
                    provider_series_ids=reason_family_provider_series_ids.get(reason_family, []),
                ),
            }
            for reason_family, count in by_reason_family.most_common(10)
        ],
        "top_title_clusters": [
            {
                "cluster": cluster,
                "label": title_cluster_labels.get(cluster, cluster),
                "count": count,
                "examples": title_cluster_examples.get(cluster, []),
                "drilldown_args": _review_queue_drilldown_args(
                    status=status,
                    issue_type=issue_type,
                    title_cluster=cluster,
                    fix_strategy=fix_strategy_filter,
                    cluster_strategy=cluster_strategy_filter,
                    decision=decision_filter,
                    reason=reason_filter,
                ),
                **_review_queue_status_action_fields(
                    status,
                    _review_queue_status_action_args(
                        status=status,
                        issue_type=issue_type,
                        title_cluster=cluster,
                        fix_strategy=fix_strategy_filter,
                        cluster_strategy=cluster_strategy_filter,
                        decision=decision_filter,
                        reason=reason_filter,
                    ),
                ),
                **_review_queue_refresh_fields(
                    issue_type=effective_refresh_issue_type,
                    provider_series_ids=title_cluster_provider_series_ids.get(cluster, []),
                ),
            }
            for cluster, count in by_title_cluster.most_common(10)
        ],
        "top_fix_strategies": [
            {
                "strategy": strategy,
                "count": count,
                "examples": fix_strategy_examples.get(strategy, []),
                "drilldown_args": _review_queue_drilldown_args(
                    status=status,
                    issue_type=issue_type,
                    title_cluster=title_cluster_filter,
                    fix_strategy=strategy,
                    cluster_strategy=cluster_strategy_filter,
                    decision=decision_filter,
                    reason=reason_filter,
                ),
                **_review_queue_status_action_fields(
                    status,
                    _review_queue_status_action_args(
                        status=status,
                        issue_type=issue_type,
                        title_cluster=title_cluster_filter,
                        fix_strategy=strategy,
                        cluster_strategy=cluster_strategy_filter,
                        decision=decision_filter,
                        reason=reason_filter,
                    ),
                ),
                **_review_queue_refresh_fields(
                    issue_type=effective_refresh_issue_type,
                    provider_series_ids=fix_strategy_provider_series_ids.get(strategy, []),
                ),
            }
            for strategy, count in by_fix_strategy.most_common(10)
        ],
        "top_fix_strategy_families": [
            {
                "strategy_family": strategy_family,
                "count": count,
                "examples": fix_strategy_family_examples.get(strategy_family, []),
                "drilldown_args": _review_queue_drilldown_args(
                    status=status,
                    issue_type=issue_type,
                    title_cluster=title_cluster_filter,
                    fix_strategy=fix_strategy_filter,
                    cluster_strategy=cluster_strategy_filter,
                    decision=decision_filter,
                    reason=reason_filter,
                    fix_strategy_family=strategy_family,
                ),
                **_review_queue_status_action_fields(
                    status,
                    _review_queue_status_action_args(
                        status=status,
                        issue_type=issue_type,
                        title_cluster=title_cluster_filter,
                        fix_strategy=fix_strategy_filter,
                        cluster_strategy=cluster_strategy_filter,
                        decision=decision_filter,
                        reason=reason_filter,
                        fix_strategy_family=strategy_family,
                    ),
                ),
                **_review_queue_refresh_fields(
                    issue_type=effective_refresh_issue_type,
                    provider_series_ids=fix_strategy_family_provider_series_ids.get(strategy_family, []),
                ),
            }
            for strategy_family, count in by_fix_strategy_family.most_common(10)
        ],
        "top_cluster_strategies": [
            {
                "cluster_strategy": cluster_strategy_key,
                "cluster": cluster_strategy_parts.get(cluster_strategy_key, {}).get("cluster"),
                "strategy": cluster_strategy_parts.get(cluster_strategy_key, {}).get("strategy"),
                "label": title_cluster_labels.get(
                    cluster_strategy_parts.get(cluster_strategy_key, {}).get("cluster", ""),
                    cluster_strategy_parts.get(cluster_strategy_key, {}).get("cluster"),
                ),
                "count": count,
                "examples": cluster_strategy_examples.get(cluster_strategy_key, []),
                "drilldown_args": _review_queue_drilldown_args(
                    status=status,
                    issue_type=issue_type,
                    title_cluster=title_cluster_filter,
                    fix_strategy=fix_strategy_filter,
                    cluster_strategy=cluster_strategy_key,
                    decision=decision_filter,
                    reason=reason_filter,
                ),
                **_review_queue_status_action_fields(
                    status,
                    _review_queue_status_action_args(
                        status=status,
                        issue_type=issue_type,
                        title_cluster=title_cluster_filter,
                        fix_strategy=fix_strategy_filter,
                        cluster_strategy=cluster_strategy_key,
                        decision=decision_filter,
                        reason=reason_filter,
                    ),
                ),
                **_review_queue_refresh_fields(
                    issue_type=effective_refresh_issue_type,
                    provider_series_ids=cluster_strategy_provider_series_ids.get(cluster_strategy_key, []),
                ),
            }
            for cluster_strategy_key, count in by_cluster_strategy.most_common(10)
        ],
        "top_cluster_strategy_families": [
            {
                "cluster_strategy_family": cluster_strategy_family_key,
                "cluster": cluster_strategy_family_parts.get(cluster_strategy_family_key, {}).get("cluster"),
                "strategy_family": cluster_strategy_family_parts.get(cluster_strategy_family_key, {}).get("strategy"),
                "label": title_cluster_labels.get(
                    cluster_strategy_family_parts.get(cluster_strategy_family_key, {}).get("cluster", ""),
                    cluster_strategy_family_parts.get(cluster_strategy_family_key, {}).get("cluster"),
                ),
                "count": count,
                "examples": cluster_strategy_family_examples.get(cluster_strategy_family_key, []),
                "drilldown_args": _review_queue_drilldown_args(
                    status=status,
                    issue_type=issue_type,
                    title_cluster=title_cluster_filter,
                    fix_strategy=fix_strategy_filter,
                    cluster_strategy=cluster_strategy_filter,
                    decision=decision_filter,
                    reason=reason_filter,
                    cluster_strategy_family=cluster_strategy_family_key,
                ),
                **_review_queue_status_action_fields(
                    status,
                    _review_queue_status_action_args(
                        status=status,
                        issue_type=issue_type,
                        title_cluster=title_cluster_filter,
                        fix_strategy=fix_strategy_filter,
                        cluster_strategy=cluster_strategy_filter,
                        decision=decision_filter,
                        reason=reason_filter,
                        cluster_strategy_family=cluster_strategy_family_key,
                    ),
                ),
                **_review_queue_refresh_fields(
                    issue_type=effective_refresh_issue_type,
                    provider_series_ids=cluster_strategy_family_provider_series_ids.get(cluster_strategy_family_key, []),
                ),
            }
            for cluster_strategy_family_key, count in by_cluster_strategy_family.most_common(10)
        ],
    }

_REVIEW_QUEUE_NEXT_BUCKET_ORDER = {
    "cluster-strategy": "top_cluster_strategies",
    "cluster-strategy-family": "top_cluster_strategy_families",
    "fix-strategy": "top_fix_strategies",
    "fix-strategy-family": "top_fix_strategy_families",
    "title-cluster": "top_title_clusters",
    "reason": "top_reasons",
    "reason-family": "top_reason_families",
    "decision": "by_decision",
}

_REVIEW_QUEUE_AUTO_BUCKET_ORDER = [
    "cluster-strategy-family",
    "cluster-strategy",
    "fix-strategy-family",
    "fix-strategy",
    "title-cluster",
    "reason-family",
    "reason",
    "decision",
]

def _review_queue_bucket_candidates(summary: dict[str, object], *, bucket: str) -> list[dict[str, object]]:
    if bucket == "decision":
        decision_counts = summary.get("by_decision")
        if not isinstance(decision_counts, dict) or not decision_counts:
            return []
        drilldowns = summary.get("decision_drilldowns")
        ordered = sorted(
            ((str(key), value) for key, value in decision_counts.items() if isinstance(value, int)),
            key=lambda item: (-item[1], item[0]),
        )
        candidates: list[dict[str, object]] = []
        for decision_name, count in ordered:
            drilldown_args = drilldowns.get(decision_name) if isinstance(drilldowns, dict) else None
            if not isinstance(drilldown_args, list):
                drilldown_args = _review_queue_drilldown_args(
                    status=summary.get("status") if isinstance(summary.get("status"), str) else "open",
                    issue_type=summary.get("issue_type_filter") if isinstance(summary.get("issue_type_filter"), str) else None,
                    title_cluster=summary.get("title_cluster_filter") if isinstance(summary.get("title_cluster_filter"), str) else None,
                    fix_strategy=summary.get("fix_strategy_filter") if isinstance(summary.get("fix_strategy_filter"), str) else None,
                    cluster_strategy=summary.get("cluster_strategy_filter") if isinstance(summary.get("cluster_strategy_filter"), str) else None,
                    decision=decision_name,
                    reason=summary.get("reason_filter") if isinstance(summary.get("reason_filter"), str) else None,
                )
            action_args = _review_queue_status_action_args(
                status=summary.get("status") if isinstance(summary.get("status"), str) else "open",
                issue_type=summary.get("issue_type_filter") if isinstance(summary.get("issue_type_filter"), str) else None,
                title_cluster=summary.get("title_cluster_filter") if isinstance(summary.get("title_cluster_filter"), str) else None,
                fix_strategy=summary.get("fix_strategy_filter") if isinstance(summary.get("fix_strategy_filter"), str) else None,
                cluster_strategy=summary.get("cluster_strategy_filter") if isinstance(summary.get("cluster_strategy_filter"), str) else None,
                decision=decision_name,
                reason=summary.get("reason_filter") if isinstance(summary.get("reason_filter"), str) else None,
            )
            decision_actions = summary.get("decision_actions") if isinstance(summary.get("decision_actions"), dict) else {}
            action_fields = decision_actions.get(decision_name) if isinstance(decision_actions, dict) else None
            if not isinstance(action_fields, dict):
                action_fields = _review_queue_status_action_fields(
                    summary.get("status") if isinstance(summary.get("status"), str) else "open",
                    action_args,
                )
            candidates.append(
                {
                    "bucket_type": "decision",
                    "bucket_key": decision_name,
                    "count": count,
                    "drilldown_args": drilldown_args,
                    "drilldown_command": _build_review_queue_command(drilldown_args),
                    **action_fields,
                }
            )
        return candidates

    summary_key = _REVIEW_QUEUE_NEXT_BUCKET_ORDER[bucket]
    entries = summary.get(summary_key)
    if not isinstance(entries, list):
        return []
    key_field = {
        "cluster-strategy": "cluster_strategy",
        "cluster-strategy-family": "cluster_strategy_family",
        "fix-strategy": "strategy",
        "fix-strategy-family": "strategy_family",
        "title-cluster": "cluster",
        "reason": "reason",
        "reason-family": "reason_family",
    }[bucket]
    candidates: list[dict[str, object]] = []
    for chosen in entries:
        if not isinstance(chosen, dict):
            continue
        drilldown_args = chosen.get("drilldown_args")
        if not isinstance(drilldown_args, list):
            continue
        action_fields = {
            key: value
            for key, value in chosen.items()
            if key in {
                "action",
                "action_args",
                "action_command",
                "resolve_args",
                "resolve_command",
                "reopen_args",
                "reopen_command",
                "refresh_provider_series_ids",
                "refresh_args",
                "refresh_command",
            }
        }
        candidates.append(
            {
                "bucket_type": bucket,
                "bucket_key": chosen.get(key_field),
                "count": chosen.get("count"),
                "label": chosen.get("label"),
                "examples": chosen.get("examples"),
                "drilldown_args": drilldown_args,
                "drilldown_command": _build_review_queue_command(drilldown_args),
                **action_fields,
            }
        )
    return candidates

def _build_review_queue_worklist(
    summary: dict[str, object],
    *,
    bucket_order: list[str],
    limit: int,
) -> list[dict[str, object]]:
    if limit <= 0:
        return []
    selected: list[dict[str, object]] = []
    seen_commands: set[str] = set()
    for bucket in bucket_order:
        for candidate in _review_queue_bucket_candidates(summary, bucket=bucket):
            command = candidate.get("drilldown_command")
            if not isinstance(command, str) or not command:
                continue
            if command in seen_commands:
                continue
            selected.append(candidate)
            seen_commands.add(command)
            if len(selected) >= limit:
                return selected
    return selected

def _build_shell_command(args: list[str]) -> str:
    return " ".join(shlex.quote(part) for part in args)

def _build_review_queue_command(args: list[str]) -> str:
    return "PYTHONPATH=src python3 -m mal_updater.cli " + " ".join(
        json.dumps(part) if any(char.isspace() for char in part) else part
        for part in args
    )

def _select_review_queue_next_bucket(summary: dict[str, object], *, bucket: str) -> dict[str, object] | None:
    candidates = _review_queue_bucket_candidates(summary, bucket=bucket)
    return candidates[0] if candidates else None

def _review_queue_bucket_filter_kwargs(candidate: dict[str, object]) -> dict[str, str]:
    bucket_type = candidate.get("bucket_type")
    bucket_key = candidate.get("bucket_key")
    if not isinstance(bucket_type, str) or not isinstance(bucket_key, str) or not bucket_key.strip():
        return {}
    if bucket_type == "cluster-strategy":
        return {"cluster_strategy": bucket_key.strip()}
    if bucket_type == "cluster-strategy-family":
        return {"cluster_strategy_family": bucket_key.strip()}
    if bucket_type == "fix-strategy":
        return {"fix_strategy": bucket_key.strip()}
    if bucket_type == "fix-strategy-family":
        return {"fix_strategy_family": bucket_key.strip()}
    if bucket_type == "title-cluster":
        return {"title_cluster": bucket_key.strip()}
    if bucket_type == "reason":
        return {"reason": bucket_key.strip()}
    if bucket_type == "reason-family":
        return {"reason_family": bucket_key.strip()}
    if bucket_type == "decision":
        return {"decision": bucket_key.strip()}
    return {}
