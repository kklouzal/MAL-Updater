from __future__ import annotations

from datetime import datetime, timezone
from html import escape
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
from pathlib import Path
import sqlite3
from typing import Any, Iterable
from urllib.parse import parse_qs, urlparse

from .config import DEFAULT_SERVICE_RECOMMENDATION_FULL_HARVEST_STALE_AFTER_DAYS, DEFAULT_SERVICE_TASK_EXECUTE_LIMITS, load_config
from .crunchyroll_auth import load_crunchyroll_credentials
from .db import (
    bootstrap_database,
    connect,
    get_mal_recommendation_harvest_coverage,
    get_operational_snapshot,
    get_public_userrecs_diagnostics,
    list_latest_recommendation_snapshot_rows,
    list_recommendation_provider_eligibility_evidence_for_mal_ids,
    unknown_public_userrecs_diagnostics,
)
from .hidive_auth import load_hidive_credentials

from .recommendation_actionability import (
    is_strict_provider_eligibility_actionable,
    strict_provider_actionability_failure_reasons,
)
from .recommendations import Recommendation, build_recommendations
from .recommendation_enrichment import build_provider_enrichment_diagnostics, unknown_provider_enrichment_diagnostics

DASHBOARD_MIN_RECOMMENDATION_LIMIT = 1
DASHBOARD_MAX_RECOMMENDATION_LIMIT = 500
DASHBOARD_DEFAULT_RECOMMENDATION_LIMIT = 120
STRICT_DISCOVERY_DIAGNOSTIC_COMMAND = "PYTHONPATH=src python3 -m mal_updater.cli recommend --include-dormant --limit 120"

_PROVIDER_LABELS = {"crunchyroll": "Crunchyroll", "hidive": "HIDIVE", "mal": "MyAnimeList", "unknown": "Unknown"}

_SECTION_METADATA: dict[str, dict[str, str]] = {
    "discovery_available_now": {
        "label": "Watchable now",
        "description": "Actionable discovery titles require fresh verified Crunchyroll or HIDIVE availability plus explicit English dub evidence.",
        "title_label": "English title",
    },
    "discovery_high_confidence": {
        "label": "Ranked discovery recommendations",
        "description": "Ranked MAL recommendation-graph discovery candidates. Provider availability and English-dub evidence may be unknown or unverified; these rows are not watch-now/actionable unless they also appear in Watchable now.",
        "title_label": "English title",
        "diagnostic_only": "true",
    },
    "discovery_candidate": {
        "label": "Title recommendations / discovery",
        "description": "All fresh MAL user-recommendation-backed discovery candidates, with provider availability evidence when known.",
        "title_label": "English title",
    },
    "resume_backlog": {
        "label": "Resume backlog",
        "description": "Known provider or MAL-list titles that look ready to resume from existing watch progress.",
        "title_label": "Title",
    },
}

def _section_metadata_for(kind: str) -> dict[str, str]:
    meta = _SECTION_METADATA.get(kind)
    if meta is None:
        label = kind.replace("_", " ").strip().title() if kind else "Unknown"
        meta = {"label": label, "description": "Recommendation rows from the latest persisted scoring snapshot.", "title_label": "Title"}
    return {"kind": kind, **meta}


def _compact_list(value: Any, *, limit: int = 6) -> list[Any]:
    if value is None:
        return []
    if isinstance(value, (str, int, float)) and not isinstance(value, bool):
        return [value]
    if isinstance(value, dict):
        value = value.values()
    if not isinstance(value, Iterable):
        return []
    result: list[Any] = []
    for item in value:
        if item is None or isinstance(item, bool):
            continue
        if isinstance(item, (str, int, float)):
            result.append(item)
        elif isinstance(item, dict):
            for key in ("mal_anime_id", "seed_mal_anime_id", "title", "name", "id"):
                if item.get(key) is not None:
                    result.append(item[key])
                    break
        if len(result) >= limit:
            break
    return result


def _compact_text(value: Any, *, limit: int = 220) -> str:
    text = " ".join(str(value or "").split())
    if len(text) <= limit:
        return text
    return text[: max(0, limit - 1)].rstrip() + "…"


def _provider_label(provider: Any) -> str:
    key = str(provider or "").strip().lower()
    return _PROVIDER_LABELS.get(key, key.replace("_", " ").title() if key else "Unknown")


def _provider_badges(providers: Iterable[Any]) -> list[dict[str, str]]:
    seen: set[str] = set()
    badges: list[dict[str, str]] = []
    for provider in providers:
        key = str(provider or "").strip().lower()
        if not key or key in seen:
            continue
        seen.add(key)
        badges.append({"provider": key, "label": _provider_label(key)})
    return badges


def _format_scorecard(scorecard: Any) -> str:
    if not isinstance(scorecard, dict):
        return ""
    parts: list[str] = []
    total = scorecard.get("total")
    if isinstance(total, int | float) and not isinstance(total, bool):
        parts.append(f"total {total:g}")
    components = scorecard.get("components")
    if isinstance(components, dict):
        for key in ("consensus", "affinity", "quality", "availability", "dub_watchable", "confidence"):
            value = components.get(key)
            if isinstance(value, int | float) and not isinstance(value, bool):
                parts.append(f"{key.replace('_', ' ')} {value:g}")
    return "; ".join(parts)


def _supporting_seed_details(context: dict[str, Any]) -> list[dict[str, Any]]:
    raw = context.get("supporting_seed_details") or context.get("top_supporting_seed_titles")
    if isinstance(raw, list):
        details: list[dict[str, Any]] = []
        for item in raw[:5]:
            if isinstance(item, dict):
                details.append(
                    {
                        "mal_anime_id": item.get("mal_anime_id") or item.get("seed_mal_anime_id"),
                        "title": item.get("title") or item.get("name") or "MAL seed",
                        "num_recommendation_votes": item.get("num_recommendation_votes") or item.get("recommendation_votes") or item.get("votes"),
                        "user_score": item.get("user_score"),
                        "status": item.get("status") or item.get("mal_watch_status"),
                    }
                )
            elif item is not None:
                details.append({"title": str(item)})
        if details:
            return details
    titles = _compact_list(context.get("supporting_seed_titles") or context.get("seed_titles"), limit=5)
    ids = _compact_list(context.get("supporting_seed_ids") or context.get("supporting_seed_mal_anime_ids") or context.get("seed_mal_anime_ids") or context.get("seed_ids"), limit=5)
    return [{"mal_anime_id": ids[index] if index < len(ids) else None, "title": str(title)} for index, title in enumerate(titles or ids)]


def _format_seed_label(seed: dict[str, Any]) -> str:
    parts = [str(seed.get("title") or seed.get("mal_anime_id") or "MAL seed")]
    details: list[str] = []
    votes = seed.get("num_recommendation_votes")
    if votes is not None:
        details.append(f"{votes} MAL vote{'s' if votes != 1 else ''}")
    score = seed.get("user_score")
    if score is not None:
        details.append(f"score {score}")
    status = seed.get("status")
    if status:
        details.append(str(status))
    if details:
        parts.append("(" + ", ".join(details) + ")")
    return " ".join(parts)


def _availability_evidence_details(context: dict[str, Any]) -> list[dict[str, Any]]:
    raw = context.get("provider_eligibility_evidence")
    if not isinstance(raw, list):
        return []
    details: list[dict[str, Any]] = []
    for item in raw:
        if not isinstance(item, dict):
            continue
        provider = str(item.get("provider") or "").strip().lower()
        details.append(
            {
                "provider": provider,
                "label": _provider_label(provider),
                "provider_series_id": item.get("provider_series_id"),
                "provider_title": item.get("provider_title"),
                "provider_url": item.get("provider_url"),
                "identity_match_kind": item.get("identity_match_kind"),
                "match_confidence": item.get("match_confidence"),
                "review_status": item.get("review_status"),
                "catalog_status": item.get("catalog_status"),
                "english_dub_status": item.get("english_dub_status"),
                "explicit_dub_evidence_source": item.get("explicit_dub_evidence_source"),
                "fetched_at": item.get("fetched_at"),
                "last_verified_at": item.get("last_verified_at"),
                "expires_at": item.get("expires_at"),
                "refresh_due_at": item.get("refresh_due_at"),
                "last_successful_positive_at": item.get("last_successful_positive_at"),
                "invalidated_at": item.get("invalidated_at"),
                "invalidation_reason": item.get("invalidation_reason"),
                "refresh_status": item.get("refresh_status"),
                "failure_count": item.get("failure_count"),
                "next_retry_at": item.get("next_retry_at"),
                "refresh_due": item.get("refresh_due"),
                "refresh_overdue": item.get("refresh_overdue"),
                "refresh_failed": item.get("refresh_failed"),
                "last_known_good_preserved": item.get("last_known_good_preserved"),
                "fresh": item.get("fresh"),
                "expired": item.get("expired"),
            }
        )
    return details


def _format_evidence_freshness(details: list[dict[str, Any]]) -> str:
    labels = []
    for item in details:
        provider = item.get("label") or _provider_label(item.get("provider"))
        verified = item.get("last_verified_at") or item.get("fetched_at") or "unknown verification time"
        expires = item.get("expires_at") or "unknown expiry"
        if item.get("invalidated_at"):
            state = "invalidated"
        elif item.get("refresh_failed"):
            state = "refresh failed; last-known-good preserved" if item.get("last_known_good_preserved") else "refresh failed"
        elif item.get("refresh_overdue"):
            state = "refresh overdue"
        elif item.get("refresh_due"):
            state = "refresh due"
        else:
            state = "expired" if item.get("expired") else "fresh" if item.get("fresh") else "unknown freshness"
        labels.append(f"{provider}: {state}; verified {verified}; expires {expires}")
    return "; ".join(labels)


def _watch_status_from_context(context: dict[str, Any]) -> str:
    mal_status = context.get("mal_watch_status") or ""
    mal_watched = _number(context.get("mal_num_episodes_watched"))
    mal_total = _number(context.get("mal_num_episodes"))
    if mal_watched is not None:
        return f"{mal_status or 'unknown'} ({mal_watched}/{mal_total if mal_total is not None else '?'})"
    if context.get("mal_watch_metadata_uncertain"):
        return "unknown/partial"
    return str(mal_status or "")


def _snapshot_evidence(row: dict[str, Any]) -> dict[str, Any]:
    context = row.get("context") if isinstance(row.get("context"), dict) else {}
    merged = {**context, **{k: v for k, v in row.items() if v is not None}}
    providers = row.get("availability_providers") or merged.get("available_via_providers") or merged.get("providers") or []
    if isinstance(providers, str):
        providers = [providers]
    elif not isinstance(providers, list):
        providers = []
    seed_details = _supporting_seed_details(merged)
    seed_ids = _compact_list(merged.get("supporting_seed_ids") or merged.get("supporting_seed_mal_anime_ids") or merged.get("seed_mal_anime_ids") or merged.get("seed_ids"))
    seed_titles = _compact_list(merged.get("supporting_seed_titles") or merged.get("seed_titles"))
    seed_count = _number(merged.get("supporting_source_count")) or _number(merged.get("source_count")) or len(seed_ids) or len(seed_titles) or 0
    votes = _number(merged.get("aggregated_recommendation_votes")) or _number(merged.get("total_votes")) or _number(merged.get("recommendation_votes")) or 0
    provider_series = merged.get("available_provider_series") if isinstance(merged.get("available_provider_series"), list) else []
    eligibility_details = _availability_evidence_details(merged)
    match_kinds = _compact_list(merged.get("availability_match_kinds") or [series.get("availability_match_kind") for series in provider_series if isinstance(series, dict)])
    if not match_kinds and eligibility_details:
        match_kinds = _compact_list([item.get("identity_match_kind") for item in eligibility_details])
    match_sources = _compact_list([series.get("mapping_source") for series in provider_series if isinstance(series, dict)])
    match_confidences = _compact_list([series.get("mapping_confidence") for series in provider_series if isinstance(series, dict)])
    if not match_confidences and eligibility_details:
        match_confidences = _compact_list([item.get("match_confidence") for item in eligibility_details])
    dub_status = _dub_status(row.get("dub_signal") or merged.get("english_dub") or merged.get("dub_signal") or merged.get("english_dub_signal"))
    review_needed = _review_needed(merged)
    provider_badges = [
        {
            "provider": item.get("provider", ""),
            "label": str(item.get("label") or _provider_label(item.get("provider"))),
            "url": str(item.get("provider_url") or ""),
            "title": str(item.get("provider_title") or ""),
        }
        for item in eligibility_details
        if item.get("provider")
    ] or _provider_badges(providers)
    identity_label = ", ".join(str(value) for value in match_kinds)
    review_labels = _compact_list([item.get("review_status") for item in eligibility_details])
    catalog_labels = _compact_list([item.get("catalog_status") for item in eligibility_details])
    english_dub_labels = _compact_list([item.get("english_dub_status") for item in eligibility_details])
    if dub_status == "unknown" and english_dub_labels:
        normalized_dub_labels = {str(value).strip().lower() for value in english_dub_labels}
        if "present" in normalized_dub_labels:
            dub_status = "present"
        elif normalized_dub_labels <= {"absent", "none"}:
            dub_status = "none"
    verification_label = "; ".join(
        part
        for part in (
            f"identity {identity_label}" if identity_label else "",
            f"review {', '.join(str(value) for value in review_labels)}" if review_labels else "",
            f"catalog {', '.join(str(value) for value in catalog_labels)}" if catalog_labels else "",
            f"English dub {', '.join(str(value) for value in english_dub_labels)}" if english_dub_labels else "",
        )
        if part
    )
    scorecard = row.get("scorecard") or merged.get("scorecard")
    return {
        "mal_recommendation_votes": votes,
        "seed_count": seed_count,
        "seed_ids": seed_ids,
        "seed_titles": seed_titles,
        "compact_seeds": ", ".join(str(x) for x in (seed_titles or seed_ids)),
        "top_supporting_seeds": seed_details,
        "supporting_seed_label": "; ".join(_format_seed_label(seed) for seed in seed_details),
        "availability_providers": list(providers) if isinstance(providers, list) else [],
        "availability_provider_label": ", ".join(str(x) for x in providers) if isinstance(providers, list) else "",
        "provider_badges": provider_badges,
        "availability_match_kinds": match_kinds,
        "availability_match_kind_label": ", ".join(str(x) for x in match_kinds),
        "availability_match_sources": match_sources,
        "availability_match_source_label": ", ".join(str(x) for x in match_sources),
        "availability_match_confidences": match_confidences,
        "availability_match_confidence_label": ", ".join(str(x) for x in match_confidences),
        "availability_confidence": merged.get("availability_confidence"),
        "availability_confidence_label": merged.get("availability_confidence_label") or merged.get("availability_evidence_label"),
        "provider_eligibility_evidence": eligibility_details,
        "verification_label": verification_label,
        "evidence_freshness_label": _format_evidence_freshness(eligibility_details),
        "dub_signal": dub_status,
        "dub_status": dub_status,
        "english_dub_present": dub_status == "present",
        "review_needed": review_needed,
        "review_label": "review needed" if review_needed else "",
        "mal_watch_status": _watch_status_from_context(merged),
        "why_recommended": _compact_text(merged.get("why_recommended") or "; ".join(str(value) for value in row.get("reasons", []) if value)),
        "scorecard_summary": _format_scorecard(scorecard),
    }


def _provider_eligibility_evidence_for_actionability(row: dict[str, Any]) -> list[Any]:
    context = row.get("context") if isinstance(row.get("context"), dict) else {}
    raw_details = context.get("provider_eligibility_evidence")
    if isinstance(raw_details, list):
        return raw_details
    evidence = row.get("evidence") if isinstance(row.get("evidence"), dict) else _snapshot_evidence(row)
    details = evidence.get("provider_eligibility_evidence")
    return details if isinstance(details, list) else []


def _overlay_current_eligibility_evidence(db_path: Path, rows: list[dict[str, Any]]) -> None:
    """Overlay authoritative DB lifecycle state onto snapshot-embedded evidence."""
    mal_ids = {
        int(context["mal_anime_id"])
        for row in rows
        if isinstance((context := row.get("context")), dict)
        and isinstance(context.get("mal_anime_id"), int)
    }
    if not mal_ids:
        return
    current_by_key = {
        (item.mal_anime_id, item.provider, item.provider_series_id): item
        for item in list_recommendation_provider_eligibility_evidence_for_mal_ids(db_path, mal_ids)
    }
    now = _utc_now_z()
    for row in rows:
        context = row.get("context") if isinstance(row.get("context"), dict) else {}
        mal_id = context.get("mal_anime_id")
        embedded = context.get("provider_eligibility_evidence")
        if not isinstance(mal_id, int) or not isinstance(embedded, list):
            continue
        overlaid: list[dict[str, Any]] = []
        for item in embedded:
            if not isinstance(item, dict):
                continue
            key = (mal_id, str(item.get("provider") or "").lower(), str(item.get("provider_series_id") or ""))
            current = current_by_key.get(key)
            if current is None:
                # Old snapshots may predate DB-backed lifecycle evidence. Absence
                # alone is not affirmative invalidation; current rows are the
                # authoritative overlay whenever present.
                overlaid.append(item)
                continue
            overlaid.append(
                {
                    **item,
                    "verification_outcome": current.verification_outcome,
                    "refresh_due_at": current.refresh_due_at,
                    "refresh_status": current.refresh_status,
                    "failure_count": current.failure_count,
                    "next_retry_at": current.next_retry_at,
                    "last_successful_positive_at": current.last_successful_positive_at,
                    "invalidated_at": current.invalidated_at,
                    "invalidation_reason": current.invalidation_reason,
                    "refresh_due": current.refresh_due_at is not None and current.refresh_due_at <= now,
                    "refresh_overdue": current.refresh_due_at is not None and current.refresh_due_at < now,
                    "refresh_failed": current.refresh_status == "failed",
                    "last_known_good_preserved": bool(current.last_successful_positive_at and not current.invalidated_at),
                }
            )
        context["provider_eligibility_evidence"] = overlaid
        row["context"] = context


def _strict_actionability_failure_reasons(row: dict[str, Any]) -> list[str]:
    return strict_provider_actionability_failure_reasons(_provider_eligibility_evidence_for_actionability(row))


def _mark_discovery_row_visibility(row: dict[str, Any]) -> dict[str, Any]:
    if row.get("kind") != "discovery_candidate":
        row["actionable"] = True
        row["diagnostic_only"] = False
        row["visibility_label"] = "actionable"
        return row
    actionable = _is_displayable_discovery(row)
    row["actionable"] = actionable
    row["diagnostic_only"] = not actionable
    if actionable:
        row["visibility_label"] = "Watchable now — verified provider+dub proof"
        row["strict_actionability"] = {"eligible": True, "missing": []}
    else:
        missing = _strict_actionability_failure_reasons(row)
        row["visibility_label"] = "Discovery only — provider/dub unverified; not actionable"
        row["strict_actionability"] = {"eligible": False, "missing": missing}
        row["unverified_evidence_label"] = "; ".join(missing)
        if row.get("provider_evidence"):
            row["provider_evidence"] = f"{row['provider_evidence']} (unverified)"
        else:
            row["provider_evidence"] = "unknown/unverified"
        badges = row.get("provider_badges")
        if isinstance(badges, list) and badges:
            unverified_badges = []
            for badge in badges:
                if not isinstance(badge, dict):
                    continue
                label = str(badge.get("label") or _provider_label(badge.get("provider")))
                unverified_badges.append({**badge, "label": f"{label} (unverified)", "url": ""})
            row["provider_badges"] = unverified_badges
            row["provider_evidence_html"] = _provider_evidence_html(unverified_badges)
            evidence = row.get("evidence")
            if isinstance(evidence, dict):
                evidence["provider_badges"] = unverified_badges
        if row.get("english_dub_evidence") in (None, "", "unknown"):
            row["english_dub_evidence"] = "unknown/unverified"
        elif row.get("english_dub_evidence") == "present":
            row["english_dub_evidence"] = "present (unverified)"
    return row


def _dub_status(value: Any) -> str:
    if isinstance(value, bool):
        return "present" if value else "none"
    normalized = str(value or "").strip().lower()
    if normalized in {"present", "yes", "true", "english dub", "dubbed"}:
        return "present"
    if normalized in {"none", "no", "false", "subonly", "sub only"}:
        return "none"
    return "unknown"


def _review_needed(context: dict[str, Any]) -> bool:
    for key in ("review_needed", "availability_review_needed", "mapping_review_needed", "needs_review"):
        if context.get(key) is True:
            return True
    if context.get("availability_match_kinds") == ["review_needed"]:
        return True
    return False


def _truthy_provider(providers: Iterable[str], name: str) -> str:
    normalized = {value.strip().lower() for value in providers if isinstance(value, str)}
    return "yes" if name.lower() in normalized else ""


def _english_dub_status(item: Recommendation) -> str:
    raw = item.context.get("english_dub")
    signal = item.context.get("english_dub_signal")
    if isinstance(raw, bool):
        return "yes" if raw else ""
    normalized_signal = str(signal).strip().lower()
    if normalized_signal in {"present", "yes", "true", "english dub"}:
        return "yes"
    if normalized_signal == "unknown":
        return "unknown"
    haystack = " ".join(value for value in (item.title, item.season_title or "") if value)
    return "yes" if "english dub" in haystack.lower() or "(dub)" in haystack.lower() else ""


def _availability_details(context: dict[str, Any]) -> dict[str, str]:
    provider_series = context.get("available_provider_series") if isinstance(context.get("available_provider_series"), list) else []
    match_kinds = _compact_list(context.get("availability_match_kinds") or [series.get("availability_match_kind") for series in provider_series if isinstance(series, dict)])
    sources = _compact_list([series.get("mapping_source") for series in provider_series if isinstance(series, dict)])
    confidences = _compact_list([series.get("mapping_confidence") for series in provider_series if isinstance(series, dict)])
    confidence_label = context.get("availability_confidence_label") or context.get("availability_evidence_label")
    confidence = context.get("availability_confidence")
    return {
        "availability_match_kind": ", ".join(str(value) for value in match_kinds),
        "availability_match_source": ", ".join(str(value) for value in sources),
        "availability_confidence": str(confidence_label if confidence_label is not None else confidence if confidence is not None else ""),
        "availability_confidence_label": str(confidence_label or ""),
        "mapping_confidence": ", ".join(str(value) for value in confidences),
        "review_needed": "yes" if _review_needed(context) else "",
    }


def _provider_evidence_html(badges: list[dict[str, Any]]) -> str:
    if not badges:
        return ""
    parts: list[str] = []
    for badge in badges:
        label = escape(str(badge.get("label") or _provider_label(badge.get("provider"))))
        css_provider = escape(str(badge.get("provider") or "unknown"))
        url = str(badge.get("url") or "").strip()
        body = f'<span class="provider-badge provider-{css_provider}">{label}</span>'
        if url:
            accessible_label = f"Open {badge.get('label') or _provider_label(badge.get('provider'))} provider proof"
            body = (
                f'<a class="provider-link" href="{escape(url, quote=True)}" rel="noreferrer noopener" '
                f'aria-label="{escape(str(accessible_label), quote=True)}">{body}</a>'
            )
        parts.append(body)
    return "<br>".join(parts)


def _number(value: Any) -> int | float | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int | float):
        return value
    return None


def _row(item: Recommendation) -> dict[str, Any]:
    context = item.context
    providers = item.available_providers()
    source_count = _number(context.get("supporting_source_count")) or _number(context.get("source_count")) or 0
    total_votes = _number(context.get("aggregated_recommendation_votes")) or _number(context.get("total_votes")) or 0
    mal_mean = _number(context.get("mean"))
    mal_popularity = _number(context.get("popularity"))
    completed = _number(context.get("completed_episode_count"))
    max_episode = _number(context.get("max_episode_number")) or _number(context.get("available_episode_count"))
    provider_progress = f"{completed}/{max_episode}" if completed is not None and max_episode is not None else (str(completed) if completed is not None else "")
    availability_details = _availability_details(context)
    mal_status = _watch_status_from_context(context)
    english_title = context.get("english_title") if isinstance(context.get("english_title"), str) else ""
    genres = context.get("genres") if isinstance(context.get("genres"), list) else []
    display_title = english_title.strip() or item.season_title or item.title
    if english_title and item.season_title and item.season_title != english_title:
        display_title = f"{english_title} ({item.season_title})"
    provider_evidence = _availability_evidence_details(context)
    provider_badges = [
        {"provider": item.get("provider"), "label": item.get("label"), "url": item.get("provider_url") or "", "title": item.get("provider_title") or ""}
        for item in provider_evidence
    ] or _provider_badges(providers)
    seed_details = _supporting_seed_details(context)
    identity_label = availability_details.get("availability_match_kind") or ", ".join(str(item.get("identity_match_kind")) for item in provider_evidence if item.get("identity_match_kind"))
    verification_label = "; ".join(
        part
        for part in (
            identity_label and f"identity {identity_label}",
            provider_evidence and "review " + ", ".join(str(item.get("review_status")) for item in provider_evidence if item.get("review_status")),
            provider_evidence and "catalog " + ", ".join(str(item.get("catalog_status")) for item in provider_evidence if item.get("catalog_status")),
        )
        if part
    )
    scorecard = context.get("scorecard") if isinstance(context.get("scorecard"), dict) else None
    why_recommended = _compact_text(context.get("why_recommended"))
    snapshot_evidence = _snapshot_evidence(
        {
            "kind": item.kind,
            "provider": item.provider,
            "title": item.title,
            "provider_series_id": item.provider_series_id,
            "priority": item.priority,
            "reasons": item.reasons,
            "context": context,
            "availability_providers": context.get("available_via_providers") if isinstance(context.get("available_via_providers"), list) else [],
        }
    )
    dub_status = str(snapshot_evidence.get("dub_status") or _dub_status(context.get("english_dub_signal") or context.get("english_dub")))
    row = {
        "title": display_title,
        "score": item.priority,
        "source_count": source_count,
        "total_votes": total_votes,
        "crunchyroll": _truthy_provider(providers, "crunchyroll"),
        "hidive": _truthy_provider(providers, "hidive"),
        "english_dub": _english_dub_status(item),
        "dub_status": dub_status,
        "provider_evidence": "; ".join(str(badge.get("label")) for badge in provider_badges),
        "provider_evidence_html": _provider_evidence_html(provider_badges),
        "provider_badges": provider_badges,
        "english_dub_evidence": "present" if dub_status == "present" else dub_status,
        "verification": verification_label,
        "evidence_freshness": _format_evidence_freshness(provider_evidence),
        "why_recommended": why_recommended,
        "scorecard_summary": _format_scorecard(scorecard),
        "seed_details": "; ".join(_format_seed_label(seed) for seed in seed_details),
        **availability_details,
        "mal_mean": mal_mean if mal_mean is not None else "",
        "mal_popularity": mal_popularity if mal_popularity is not None else "",
        "genres": ", ".join(str(value) for value in genres),
        "provider_progress": provider_progress,
        "mal_watch_status": mal_status,
        "reasons": "; ".join(item.reasons),
        "kind": item.kind,
        "providers": ", ".join(providers),
        "availability_providers": context.get("available_via_providers") if isinstance(context.get("available_via_providers"), list) else [],
        "provider_series_id": item.provider_series_id,
        "context": context,
        "evidence": snapshot_evidence,
    }
    return _mark_discovery_row_visibility(row)


# Presentation-only columns. Provider proof is rendered beneath the title; dub,
# provider-progress, and MAL-watch fields remain available in row/JSON evidence.
_DASHBOARD_COLUMNS: tuple[tuple[str, str, str], ...] = (
    ("title", "Title", "text"),
    ("score", "Score", "number"),
    ("source_count", "Source count", "number"),
    ("total_votes", "Total votes", "number"),
    ("availability_match_kind", "Availability match", "text"),
    ("availability_confidence", "Availability confidence", "text"),
    ("availability_match_source", "Match source", "text"),
    ("mapping_confidence", "Mapping confidence", "number"),
    ("review_needed", "Review", "text"),
    ("mal_mean", "MAL mean", "number"),
    ("mal_popularity", "MAL popularity", "number"),
    ("genres", "Genres", "text"),
    ("why_recommended", "Why recommended", "text"),
    ("scorecard_summary", "Scorecard", "text"),
    ("seed_details", "Top watched seeds", "text"),
)


_STATIC_SECTION_ORDER: tuple[str, ...] = ("discovery_available_now", "discovery_high_confidence", "resume_backlog")

_DASHBOARD_REQUIRED_TABLES: frozenset[str] = frozenset(
    {
        "mal_anime_metadata",
        "mal_anime_recommendations",
        "mal_anime_relations",
        "mal_recommendation_harvest_status",
        "mal_series_mapping",
        "mal_user_anime_list_cache",
        "provider_episode_progress",
        "provider_series",
        "provider_watchlist",
        "recommendation_provider_eligibility_evidence",
        "recommendation_score_snapshots",
        "review_queue",
        "schema_migrations",
        "sync_runs",
    }
)


def _utc_now_z() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _read_sqlite_table_names(db_path: Path) -> set[str]:
    uri = db_path.resolve().as_uri() + "?mode=ro"
    conn = sqlite3.connect(uri, uri=True)
    try:
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA query_only = ON")
        rows = conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
    finally:
        conn.close()
    return {str(row["name"]) for row in rows}


def _dashboard_schema_unavailable_reason(db_path: Path) -> str | None:
    if not db_path.exists():
        return "dashboard database file is absent"
    tables = _read_sqlite_table_names(db_path)
    missing = sorted(_DASHBOARD_REQUIRED_TABLES - tables)
    if missing:
        preview = ", ".join(missing[:6])
        if len(missing) > 6:
            preview += f", … ({len(missing)} missing total)"
        return f"dashboard database schema is not initialized or requires migration; missing table(s): {preview}"
    return None


def _is_schema_unavailable_error(exc: sqlite3.OperationalError) -> bool:
    message = str(exc).lower()
    return "no such table" in message or "no such column" in message


def _empty_operational_snapshot() -> dict[str, Any]:
    return {
        "latest_sync_run": None,
        "latest_completed_sync_run": None,
        "provider_counts": {"series": 0, "progress": 0, "watchlist": 0},
        "provider_counts_by_provider": {},
        "provider_freshness": {"series_last_seen_at": None, "progress_last_seen_at": None, "watchlist_last_seen_at": None},
        "provider_freshness_by_provider": {},
        "review_queue": {},
        "mappings": {"total": 0, "approved": 0, "by_source": {}, "by_provider": {}},
        "provider_enrichment": unknown_provider_enrichment_diagnostics(reason="dashboard_schema_unavailable"),
    }


def _empty_mal_recommendation_harvest_coverage() -> dict[str, Any]:
    return {
        "summary": {
            "mapped_sources": 0,
            "watched_sources": 0,
            "positive_list_sources": 0,
            "confirmed_positive_sources": 0,
            "fresh": 0,
            "stale": 0,
            "failed": 0,
            "unharvested": 0,
            "complete_full_harvest_sources": 0,
            "official_detail_sources": 0,
            "total_edges": 0,
            "fresh_coverage_ratio": None,
        },
        "sources": [],
        "public_userrecs": unknown_public_userrecs_diagnostics(reason="dashboard_schema_unavailable"),
    }


def _unavailable_dashboard_payload(*, limit: int, reason: str, db_path: Path | None = None) -> dict[str, Any]:
    section_metadata = {kind: _section_metadata_for(kind) for kind in _STATIC_SECTION_ORDER}
    coverage_state = {
        "strict_actionable_count": 0,
        "dormant_candidate_count": 0,
        "evidence_pending_review_count": 0,
        "stale_evidence_count": 0,
        "strict_current_evidence_count": 0,
        "message": f"Dashboard data unavailable: {reason}. Run an explicit initialization/startup command to apply migrations; payload reads do not bootstrap schema.",
        "next_diagnostic_command": STRICT_DISCOVERY_DIAGNOSTIC_COMMAND,
    }
    coverage = _empty_mal_recommendation_harvest_coverage()
    if db_path is not None:
        coverage["public_userrecs"] = _public_userrecs_diagnostics_for_dashboard(db_path, reason="dashboard_schema_unavailable")
    return {
        "generated_at": _utc_now_z(),
        "snapshot": None,
        "recommendations": {
            "mode": "strict_actionable",
            "strict_default": True,
            "items": [],
            "sections": {},
            "section_totals": {},
            "section_metadata": section_metadata,
            "coverage_state": coverage_state,
            "diagnostic_source_snapshot": None,
            "limit": limit,
            "limit_scope": "per_section",
        },
        "coverage": coverage,
        "operational": _empty_operational_snapshot(),
        "recent_sync_runs": [],
        "indicators": [
            {"level": "warning", "message": "No persisted recommendation snapshot is available yet."},
            {"level": "warning", "message": coverage_state["message"]},
        ],
    }


def _is_displayable_discovery(row: dict[str, Any]) -> bool:
    return any(
        is_strict_provider_eligibility_actionable(item)
        for item in _provider_eligibility_evidence_for_actionability(row)
        if isinstance(item, dict)
    )


def _static_section_key(row: dict[str, Any]) -> str | None:
    kind = str(row.get("kind") or "unknown")
    if kind == "discovery_candidate":
        if row.get("force_diagnostic_only") is True:
            return "discovery_high_confidence"
        return "discovery_available_now" if _is_displayable_discovery(row) else "discovery_high_confidence"
    return kind


def _static_sections(rows: list[dict[str, Any]]) -> list[tuple[dict[str, str], list[dict[str, Any]], int]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        section_key = _static_section_key(row)
        if section_key is not None:
            grouped.setdefault(section_key, []).append(row)
    ordered_keys = list(_STATIC_SECTION_ORDER)
    ordered_keys.extend(sorted(key for key in grouped if key not in _STATIC_SECTION_ORDER))
    return [(_section_metadata_for(key), grouped.get(key, []), len(grouped.get(key, []))) for key in ordered_keys]


def normalize_dashboard_limit(value: Any, *, default: int = DASHBOARD_DEFAULT_RECOMMENDATION_LIMIT) -> int:
    """Coerce dashboard row limits to the documented safe per-section range.

    Invalid query values fall back to the request/server default; numeric values
    clamp to ``DASHBOARD_MIN_RECOMMENDATION_LIMIT`` ..
    ``DASHBOARD_MAX_RECOMMENDATION_LIMIT``.
    """
    try:
        default_value = int(default)
    except (TypeError, ValueError):
        default_value = DASHBOARD_DEFAULT_RECOMMENDATION_LIMIT
    default_value = min(DASHBOARD_MAX_RECOMMENDATION_LIMIT, max(DASHBOARD_MIN_RECOMMENDATION_LIMIT, default_value))
    if value is None:
        return default_value
    if isinstance(value, str) and not value.strip():
        return default_value
    try:
        candidate = int(value)
    except (TypeError, ValueError):
        return default_value
    return min(DASHBOARD_MAX_RECOMMENDATION_LIMIT, max(DASHBOARD_MIN_RECOMMENDATION_LIMIT, candidate))


def _section_display_budget(limit: int) -> int:
    return normalize_dashboard_limit(limit)


def _cap_sections(
    sections: list[tuple[dict[str, str], list[dict[str, Any]], int]], *, limit: int | None
) -> list[tuple[dict[str, str], list[dict[str, Any]], int]]:
    if limit is None:
        return sections
    budget = _section_display_budget(limit)
    return [(meta, rows[:budget], total) for meta, rows, total in sections]


def _section_count_label(displayed: int, total: int) -> str:
    if displayed < total:
        return f"{displayed} of {total}"
    return str(total)


def _recommendation_table(rows: list[dict[str, Any]], table_id: str, head_cells: str | None = None, *, title_label: str = "Title") -> str:
    if head_cells is None:
        head_cells = _table_head_cells(title_label=title_label)
    body_rows = []
    for row in rows:
        cell_parts: list[str] = []
        for key, _, _ in _DASHBOARD_COLUMNS:
            value = row.get(key, "")
            if key == "title":
                providers = str(row.get("provider_evidence_html") or "")
                provider_block = f'<div class="title-providers" aria-label="Provider proof">{providers}</div>' if providers else '<div class="title-providers muted">Provider proof unavailable</div>'
                cell_parts.append(
                    f'<th scope="row" data-key="title" data-sort-value="{escape(str(value), quote=True)}">'
                    f'<span class="title-text">{escape(str(value))}</span>{provider_block}</th>'
                )
            else:
                cell_parts.append(f'<td data-key="{escape(key)}">{escape(str(value))}</td>')
        cells = "".join(cell_parts)
        body_rows.append(f'<tr data-kind="{escape(str(row.get("kind", "")))}" data-providers="{escape(str(row.get("providers", "")))}">{cells}</tr>')
    body = "\n".join(body_rows) or f'<tr class="table-empty"><td colspan="{len(_DASHBOARD_COLUMNS)}">No recommendations in this section.</td></tr>'
    caption = f'{escape(title_label)} recommendations; provider proof appears below each title.'
    return f"""<div class="table-scroll" tabindex="0" role="region" aria-label="{escape(title_label)} recommendations table">
  <table id="{escape(table_id)}" class="recommendations">
    <caption class="sr-only">{caption}</caption>
    <thead><tr>{head_cells}</tr></thead>
    <tbody>
{body}
    </tbody>
  </table>
  </div>"""


def _table_head_cells(*, title_label: str = "Title") -> str:
    return "".join(
        f'<th scope="col" data-key="{escape(key)}" data-type="{escape(kind)}" aria-sort="none"><button type="button" class="sort-button">{escape("Title" if key == "title" else label)}<span aria-hidden="true" class="sort-indicator"></span></button></th>'
        for key, label, kind in _DASHBOARD_COLUMNS
    )


def _dashboard_mode_banner(*, diagnostic_mode: bool) -> str:
    if diagnostic_mode:
        return (
            '<section class="banner warn"><strong>Diagnostic mode:</strong> '
            'Ranked discovery recommendations are included for operator inspection only. '
            'Rows without verified current Crunchyroll/HIDIVE availability plus explicit English-dub evidence are not actionable.</section>'
        )
    return (
        '<section class="banner good"><strong>Watchable now dashboard:</strong> '
        'Actionable discovery rows require fresh verified Crunchyroll/HIDIVE identity/catalog presence and explicit English-dub evidence. '
        'Use <code>--include-dormant</code> only for diagnostics.</section>'
    )


def _strict_empty_state_html(*, dormant_count: int = 0, pending_review_count: int = 0, stale_count: int = 0) -> str:
    return (
        '<section class="empty-state"><h2>No Watchable now discovery titles</h2>'
        '<p>Zero titles currently have verified current Crunchyroll/HIDIVE + English-dub evidence.</p>'
        f'<ul><li>Ranked unverified discovery candidates visible in diagnostics: {escape(str(dormant_count))}</li>'
        f'<li>Provider evidence pending review: {escape(str(pending_review_count))}</li>'
        f'<li>Stale or expired provider evidence: {escape(str(stale_count))}</li></ul>'
        f'<p class="meta">Bounded next diagnostic command: <code>{escape(STRICT_DISCOVERY_DIAGNOSTIC_COMMAND)}</code></p></section>'
    )


def render_recommendation_dashboard(items: Iterable[Recommendation], *, title: str = "MAL-Updater recommendations", limit: int | None = None, diagnostic_mode: bool = False) -> str:
    rows = [_row(item) for item in items]
    generated_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    strict_count = sum(1 for row in rows if _static_section_key(row) == "discovery_available_now")
    dormant_count = sum(1 for row in rows if _static_section_key(row) == "discovery_high_confidence")
    pending_review_count = sum(1 for row in rows if row.get("review_needed") == "yes")
    stale_count = sum(1 for row in rows if "expired" in str(row.get("evidence_freshness", "")).lower() or "stale" in str(row.get("verification", "")).lower())
    banner = _dashboard_mode_banner(diagnostic_mode=diagnostic_mode)
    if rows:
        sections = []
        if strict_count == 0:
            sections.append(_strict_empty_state_html(dormant_count=dormant_count, pending_review_count=pending_review_count, stale_count=stale_count))
        for index, (meta, section_rows, total_rows) in enumerate(_cap_sections(_static_sections(rows), limit=limit), start=1):
            description = f'<p class="meta">{escape(meta["description"])}</p>' if meta.get("description") else ""
            if meta.get("diagnostic_only") == "true":
                description += '<p class="warn"><strong>Discovery only:</strong> these rows lack strict actionable provider+dub proof and are not watch-now eligible.</p>'
            count_label = _section_count_label(len(section_rows), total_rows)
            table_or_empty = (
                _recommendation_table(section_rows, f"recommendations-{index}", title_label=meta.get("title_label", "Title"))
                if section_rows
                else '<p class="muted" role="status">No recommendations in this section.</p>'
            )
            sections.append(
                f'<section><h2>{escape(meta["label"])} ({escape(count_label)})</h2>{description}'
                f'{table_or_empty}</section>'
            )
        body = "\n  ".join(sections)
    else:
        body = _strict_empty_state_html() + _recommendation_table([], "recommendations")
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{escape(title)}</title>
  <style>
    * {{ box-sizing: border-box; }}
    body {{ font-family: system-ui, -apple-system, Segoe UI, sans-serif; margin: 0 auto; padding: 1.5rem; max-width: 110rem; background: #101418; color: #eef3f8; line-height: 1.45; }}
    section {{ margin-top: 2rem; }} h1, h2 {{ line-height: 1.2; }} a {{ color: #8cc8ff; }}
    .table-scroll {{ overflow-x: auto; margin: .75rem 0 1.5rem; border: 1px solid #2b3642; border-radius: .55rem; }}
    .table-scroll:focus-visible {{ outline: 3px solid #8cc8ff; outline-offset: 2px; }}
    table {{ border-collapse: collapse; width: 100%; background: #161d24; }}
    .table-scroll table.recommendations {{ min-width: 62rem; }}
    th, td {{ border-right: 1px solid #2b3642; border-bottom: 1px solid #2b3642; padding: .65rem .75rem; vertical-align: top; text-align: left; }}
    th:last-child, td:last-child {{ border-right: 0; }} tbody tr:last-child > * {{ border-bottom: 0; }}
    .recommendations thead th {{ position: sticky; top: 0; z-index: 1; background: #243140; }}
    .recommendations tbody th {{ min-width: 16rem; font-weight: 650; }} tbody tr:nth-child(even) {{ background: #121920; }} tbody tr:hover {{ background: #1c2732; }}
    .sort-button {{ all: unset; display: flex; align-items: center; gap: .35rem; width: 100%; cursor: pointer; font-weight: 700; }}
    .sort-button:focus-visible {{ outline: 2px solid #8cc8ff; outline-offset: 3px; }}
    th[aria-sort="ascending"] .sort-indicator::after {{ content: "▲"; }} th[aria-sort="descending"] .sort-indicator::after {{ content: "▼"; }}
    .title-text {{ display: block; }} .title-providers {{ display: block; margin-top: .45rem; font-size: .88rem; font-weight: 400; }}
    .meta, .muted {{ color: #aebccc; }} .warn {{ color: #ffd37a; }} .good {{ color: #b9f6ca; }}
    .banner, .empty-state {{ background: #161d24; border: 1px solid #2b3642; border-radius: .6rem; padding: 1rem; }}
    .provider-badge {{ display: inline-block; border: 1px solid #4b9fff; border-radius: 999px; padding: .08rem .5rem; font-weight: 700; }}
    .provider-link {{ display: inline-block; border-radius: 999px; text-decoration: none; }} .provider-link:hover .provider-badge {{ background: #243f5a; }}
    .provider-link:focus-visible {{ outline: 3px solid #8cc8ff; outline-offset: 2px; }}
    .table-empty {{ text-align: center; color: #aebccc; }} .sr-only {{ position: absolute; width: 1px; height: 1px; padding: 0; margin: -1px; overflow: hidden; clip: rect(0,0,0,0); white-space: nowrap; border: 0; }}
    @media (max-width: 48rem) {{ body {{ padding: 1rem; }} section {{ margin-top: 1.5rem; }} th, td {{ padding: .55rem .6rem; }} .banner, .empty-state {{ padding: .8rem; }} }}
  </style>
</head>
<body>
  <h1>{escape(title)}</h1>
  <p class="meta">Generated {escape(generated_at)} from local recommendation data. Click any column header to sort.</p>
  {banner}
  {body}
  <script>
  (() => {{
    const getValue = (row, key, type) => {{
      const cell = row.querySelector(`[data-key="${{key}}"]`);
      const text = cell?.dataset.sortValue ?? cell?.textContent.trim() ?? '';
      return type === 'number' ? (text === '' ? Number.NEGATIVE_INFINITY : Number(text)) : text.toLowerCase();
    }};
    document.querySelectorAll('table.recommendations').forEach(table => {{
      const tbody = table.tBodies[0];
      table.querySelectorAll('thead th').forEach(th => {{
        th.querySelector('button')?.addEventListener('click', () => {{
          const key = th.dataset.key;
          const type = th.dataset.type;
          const direction = th.getAttribute('aria-sort') === 'ascending' ? 'descending' : 'ascending';
          table.querySelectorAll('thead th').forEach(other => other.setAttribute('aria-sort', 'none'));
          th.setAttribute('aria-sort', direction);
          const rows = Array.from(tbody.rows).filter(row => !row.classList.contains('table-empty'));
          rows.sort((a, b) => {{
            const av = getValue(a, key, type);
            const bv = getValue(b, key, type);
            if (av < bv) return direction === 'ascending' ? -1 : 1;
            if (av > bv) return direction === 'ascending' ? 1 : -1;
            return 0;
          }});
          rows.forEach(row => tbody.appendChild(row));
        }});
      }});
    }});
  }})();
  </script>
</body>
</html>
"""


def write_recommendation_dashboard(path: Path, items: Iterable[Recommendation], *, title: str = "MAL-Updater recommendations", limit: int | None = None, diagnostic_mode: bool = False) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(render_recommendation_dashboard(items, title=title, limit=limit, diagnostic_mode=diagnostic_mode), encoding="utf-8")
    return path


def _availability_confidence_label(row: Any, context: dict[str, Any]) -> str | None:
    label = getattr(row, "availability_confidence_label", None)
    if label is None:
        label = context.get("availability_confidence_label") or context.get("availability_evidence_label")
    return None if label is None else str(label)


def recommendation_snapshot_row_base_payload(row: Any, *, collapse_discovery_candidate_reasons: bool = False) -> dict[str, Any]:
    context = getattr(row, "context", None) if isinstance(getattr(row, "context", None), dict) else {}
    reasons = getattr(row, "reasons", [])
    if collapse_discovery_candidate_reasons and getattr(row, "kind", None) == "discovery_candidate":
        reasons = []
    return {
        "id": row.id,
        "run_id": row.run_id,
        "generated_at": row.generated_at,
        "kind": row.kind,
        "provider": row.provider,
        "title": row.title,
        "provider_series_id": row.provider_series_id,
        "mal_anime_id": row.mal_anime_id,
        "score": row.score,
        "priority": row.priority,
        "reasons": reasons,
        "scorecard": row.scorecard,
        "context": row.context,
        "availability_providers": row.availability_providers,
        "dub_signal": row.dub_signal,
        "availability_confidence": row.availability_confidence,
        "availability_confidence_label": _availability_confidence_label(row, context),
    }


def recommendation_snapshot_availability_payload(row: Any, *, evidence: dict[str, Any] | None = None) -> dict[str, Any]:
    context = getattr(row, "context", None) if isinstance(getattr(row, "context", None), dict) else {}
    if evidence is not None:
        providers = evidence.get("availability_providers", [])
        match_kinds = evidence.get("availability_match_kinds", [])
        match_sources = evidence.get("availability_match_sources", [])
        match_confidences = evidence.get("availability_match_confidences", [])
        dub_status = evidence.get("dub_status", "unknown")
        review_needed = evidence.get("review_needed", False)
    else:
        provider_series = context.get("available_provider_series") if isinstance(context.get("available_provider_series"), list) else []
        match_kinds = context.get("availability_match_kinds")
        if not isinstance(match_kinds, list):
            match_kinds = [series.get("availability_match_kind") for series in provider_series if isinstance(series, dict) and series.get("availability_match_kind")]
        match_sources = [series.get("mapping_source") for series in provider_series if isinstance(series, dict) and series.get("mapping_source")]
        match_confidences = [series.get("mapping_confidence") for series in provider_series if isinstance(series, dict) and series.get("mapping_confidence") is not None]
        providers = getattr(row, "availability_providers", [])
        dub_status = _dub_status(getattr(row, "dub_signal", None) or context.get("english_dub_signal") or context.get("english_dub"))
        review_needed = _review_needed(context)
    return {
        "providers": providers,
        "match_kinds": match_kinds,
        "match_sources": match_sources,
        "match_confidences": match_confidences,
        "confidence": getattr(row, "availability_confidence", None),
        "confidence_label": _availability_confidence_label(row, context),
        "dub_status": dub_status,
        "review_needed": review_needed,
    }


def _snapshot_row_to_dict(row: Any) -> dict[str, Any]:
    payload = recommendation_snapshot_row_base_payload(row, collapse_discovery_candidate_reasons=True)
    context = payload.get("context") if isinstance(payload.get("context"), dict) else {}
    english_title = context.get("english_title")
    genres = context.get("genres")
    if isinstance(english_title, str) and english_title.strip():
        payload["english_title"] = english_title.strip()
        payload["display_title"] = f"{english_title.strip()} ({row.title})" if row.title and row.title != english_title.strip() else english_title.strip()
    else:
        payload["display_title"] = row.title
    payload["genres"] = genres if isinstance(genres, list) else []
    payload["evidence"] = _snapshot_evidence(payload)
    payload["provider_badges"] = payload["evidence"].get("provider_badges", [])
    payload["provider_evidence"] = payload["evidence"].get("availability_provider_label", "")
    payload["english_dub_evidence"] = "present" if payload["evidence"].get("english_dub_present") else payload["evidence"].get("dub_status", "unknown")
    payload["verification"] = payload["evidence"].get("verification_label", "")
    payload["evidence_freshness"] = payload["evidence"].get("evidence_freshness_label", "")
    payload["why_recommended"] = payload["evidence"].get("why_recommended", "")
    payload["scorecard_summary"] = payload["evidence"].get("scorecard_summary", "")
    payload["seed_details"] = payload["evidence"].get("supporting_seed_label", "")
    availability = recommendation_snapshot_availability_payload(row, evidence=payload["evidence"])
    availability.update(
        {
            "provider_badges": payload["evidence"].get("provider_badges", []),
            "verification": payload["evidence"].get("verification_label", ""),
            "freshness": payload["evidence"].get("evidence_freshness_label", ""),
        }
    )
    payload["availability"] = availability
    return _mark_discovery_row_visibility(payload)


def _latest_snapshot_summary(db_path: Path) -> dict[str, Any] | None:
    with connect(db_path) as conn:
        row = conn.execute(
            """
            SELECT run_id, MIN(generated_at) AS first_generated_at, MAX(generated_at) AS generated_at, COUNT(*) AS item_count
            FROM recommendation_score_snapshots
            WHERE run_id = (SELECT run_id FROM recommendation_score_snapshots ORDER BY generated_at DESC, id DESC LIMIT 1)
            GROUP BY run_id
            """
        ).fetchone()
    if row is None:
        return None
    return {"run_id": row["run_id"], "generated_at": row["generated_at"], "first_generated_at": row["first_generated_at"], "item_count": int(row["item_count"] or 0)}


def _recent_sync_runs(db_path: Path, *, limit: int = 8) -> list[dict[str, Any]]:
    with connect(db_path) as conn:
        rows = conn.execute(
            """
            SELECT id, provider, contract_version, mode, started_at, completed_at, status, summary_json
            FROM sync_runs ORDER BY started_at DESC, id DESC LIMIT ?
            """,
            (max(1, int(limit)),),
        ).fetchall()
    runs: list[dict[str, Any]] = []
    for row in rows:
        summary = None
        if row["summary_json"]:
            try:
                summary = json.loads(row["summary_json"])
            except json.JSONDecodeError:
                summary = {"_decode_error": True}
        runs.append({"id": int(row["id"]), "provider": row["provider"], "contract_version": row["contract_version"], "mode": row["mode"], "started_at": row["started_at"], "completed_at": row["completed_at"], "status": row["status"], "summary": summary})
    return runs


def _eligibility_coverage_counts(db_path: Path) -> dict[str, int]:
    with connect(db_path) as conn:
        rows = conn.execute(
            """
            SELECT provider, identity_match_kind, review_status, catalog_status, english_dub_status,
                   audio_locales_json, fetched_at, expires_at, last_verified_at,
                   last_successful_positive_at, invalidated_at
            FROM recommendation_provider_eligibility_evidence
            """
        ).fetchall()
    counts = {"total": len(rows), "strict_current": 0, "pending_review": 0, "stale": 0}
    for row in rows:
        try:
            audio_locales = json.loads(row["audio_locales_json"] or "[]")
        except json.JSONDecodeError:
            audio_locales = []
        evidence = {
            "provider": row["provider"],
            "identity_match_kind": row["identity_match_kind"],
            "review_status": row["review_status"],
            "catalog_status": row["catalog_status"],
            "english_dub_status": row["english_dub_status"],
            "audio_locales": audio_locales if isinstance(audio_locales, list) else [],
            "fetched_at": row["fetched_at"],
            "expires_at": row["expires_at"],
            "last_verified_at": row["last_verified_at"],
            "last_successful_positive_at": row["last_successful_positive_at"],
            "invalidated_at": row["invalidated_at"],
        }
        if is_strict_provider_eligibility_actionable(evidence):
            counts["strict_current"] += 1
        if row["review_status"] in {"unknown", "review-needed"} or row["catalog_status"] in {"unknown", "review-needed"} or row["english_dub_status"] in {"unknown", "review-needed"}:
            counts["pending_review"] += 1
        reasons = strict_provider_actionability_failure_reasons([evidence])
        expires_at = str(row["expires_at"] or "").strip().replace("Z", "+00:00")
        expired = False
        if expires_at:
            try:
                expired_at = datetime.fromisoformat(expires_at)
                if expired_at.tzinfo is None:
                    expired_at = expired_at.replace(tzinfo=timezone.utc)
                expired = expired_at.astimezone(timezone.utc) <= datetime.now(timezone.utc)
            except ValueError:
                expired = False
        verification_only_stale = reasons == ["current provider verification stale or missing"]
        last_known_positive = bool(row["last_successful_positive_at"] and not row["invalidated_at"])
        if row["review_status"] == "stale" or row["catalog_status"] == "stale" or row["english_dub_status"] == "stale" or ((expired or verification_only_stale) and not last_known_positive):
            counts["stale"] += 1
    return counts


def _project_root_from_db_path(db_path: Path) -> Path | None:
    resolved = db_path.resolve()
    for parent in (resolved.parent, *resolved.parents):
        if parent.name == ".MAL-Updater":
            return parent.parent
    return None


def _dashboard_config_from_db_path(db_path: Path) -> Any | None:
    project_root = _project_root_from_db_path(db_path)
    if project_root is None:
        return None
    try:
        config = load_config(project_root)
        if config.db_path.resolve() != db_path.resolve():
            return None
    except (AttributeError, OSError, ValueError, TypeError):
        return None
    return config


def _public_userrecs_policy_kwargs_from_db_path(db_path: Path) -> dict[str, int]:
    source_titles_per_hour = int(DEFAULT_SERVICE_TASK_EXECUTE_LIMITS.get("recommend_full_harvest", 3))
    max_pages_per_source_per_run = int(DEFAULT_SERVICE_TASK_EXECUTE_LIMITS.get("recommend_full_harvest_pages", 10))
    stale_after_days = int(DEFAULT_SERVICE_RECOMMENDATION_FULL_HARVEST_STALE_AFTER_DAYS)
    config = _dashboard_config_from_db_path(db_path)
    if config is not None and getattr(config, "service", None) is not None:
        try:
            source_limit = config.service.execute_limit_for("recommend_full_harvest")
            pages_limit = config.service.execute_limit_for("recommend_full_harvest_pages")
            if source_limit is not None:
                source_titles_per_hour = max(0, int(source_limit))
            if pages_limit is not None:
                max_pages_per_source_per_run = max(1, int(pages_limit))
            stale_after_days = max(1, int(config.service.recommendation_full_harvest_stale_after_days))
        except (AttributeError, TypeError, ValueError):
            pass
    return {
        "configured_source_titles_per_hour": source_titles_per_hour,
        "max_pages_per_source_per_run": max_pages_per_source_per_run,
        "stale_after_days": stale_after_days,
    }


def _public_userrecs_diagnostics_for_dashboard(db_path: Path, *, reason: str | None = None) -> dict[str, Any]:
    policy_kwargs = _public_userrecs_policy_kwargs_from_db_path(db_path)
    if reason is not None:
        payload = unknown_public_userrecs_diagnostics(reason=reason)
        payload["policy"].update(
            {
                "configured_source_titles_per_hour": policy_kwargs["configured_source_titles_per_hour"],
                "configured_source_titles_per_hour_source": "task_execute_limit",
                "max_pages_per_source_per_run": policy_kwargs["max_pages_per_source_per_run"],
                "stale_horizon_days": policy_kwargs["stale_after_days"],
                "stale_horizon_source": "configured_or_default",
            }
        )
        payload["sustainability"].update(
            {
                "configured_source_titles_per_hour": policy_kwargs["configured_source_titles_per_hour"],
                "configured_exceeds_authorized": policy_kwargs["configured_source_titles_per_hour"] > 2,
            }
        )
        return payload
    return get_public_userrecs_diagnostics(db_path, **policy_kwargs)


def _dashboard_configured_provider_slugs(config: Any) -> list[str]:
    providers: list[str] = []
    try:
        crunchyroll_credentials = load_crunchyroll_credentials(config)
        if crunchyroll_credentials.username and crunchyroll_credentials.password:
            providers.append("crunchyroll")
    except (AttributeError, OSError, ValueError, TypeError):
        pass
    try:
        hidive_credentials = load_hidive_credentials(config)
        if hidive_credentials.username and hidive_credentials.password:
            providers.append("hidive")
    except (AttributeError, OSError, ValueError, TypeError):
        pass
    return providers


def _provider_enrichment_diagnostics_for_dashboard(db_path: Path) -> dict[str, Any]:
    config = _dashboard_config_from_db_path(db_path)
    if config is None:
        return unknown_provider_enrichment_diagnostics(reason="dashboard_config_unavailable")
    try:
        return build_provider_enrichment_diagnostics(
            config,
            provider_slugs=_dashboard_configured_provider_slugs(config),
        )
    except (AttributeError, OSError, ValueError, TypeError) as exc:
        return unknown_provider_enrichment_diagnostics(
            reason=f"provider_enrichment_diagnostic_unavailable:{type(exc).__name__}"
        )


def _local_diagnostic_unavailable_source(reason: str) -> dict[str, Any]:
    return {
        "run_id": "local-diagnostic-unavailable",
        "generated_at": _utc_now_z(),
        "item_count": 0,
        "selection": "current local recommendation scorer unavailable; no writes attempted",
        "unavailable": True,
        "reason": reason,
    }


def _current_ranked_discovery_rows_from_local_state(db_path: Path, *, limit: int) -> tuple[list[dict[str, Any]], dict[str, Any] | None]:
    """Build current diagnostic discovery rows from local SQLite-backed state only.

    This intentionally uses the recommendation scorer over cached local state instead of any
    provider/MAL network calls, so page requests can expose discovery visibility without
    mutating strict persisted recommendation snapshots.
    """
    project_root = _project_root_from_db_path(db_path)
    if project_root is None:
        return [], None
    try:
        config = load_config(project_root)
        if config.db_path.resolve() != db_path.resolve():
            return [], None
        results = build_recommendations(
            config,
            limit=max(1, int(limit)),
            require_provider_availability=False,
            include_discovery_candidates_without_actionable_provider_evidence=True,
            read_only=True,
        )
    except sqlite3.OperationalError as exc:
        if _is_schema_unavailable_error(exc):
            return [], _local_diagnostic_unavailable_source(str(exc))
        raise
    rows: list[dict[str, Any]] = []
    for item in results:
        if item.kind != "discovery_candidate":
            continue
        row = _row(item)
        genres = item.context.get("genres")
        if isinstance(genres, list):
            row["genres"] = [str(value) for value in genres if value is not None]
        rows.append(row)
    if not rows:
        return [], None
    return rows, {
        "run_id": "local-diagnostic-current",
        "generated_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "item_count": len(rows),
        "selection": "current local recommendation scorer with --include-dormant semantics; no provider network calls",
    }


def build_dashboard_payload(db_path: Path, *, limit: int = DASHBOARD_DEFAULT_RECOMMENDATION_LIMIT, stale_after_days: int = 14) -> dict[str, Any]:
    """Return the current dashboard model directly from SQLite state."""
    display_limit = normalize_dashboard_limit(limit)
    unavailable_reason = _dashboard_schema_unavailable_reason(db_path)
    if unavailable_reason is not None:
        return _unavailable_dashboard_payload(limit=display_limit, reason=unavailable_reason, db_path=db_path)
    try:
        return _build_dashboard_payload_from_initialized_schema(
            db_path,
            display_limit=display_limit,
            stale_after_days=stale_after_days,
        )
    except sqlite3.OperationalError as exc:
        if _is_schema_unavailable_error(exc):
            return _unavailable_dashboard_payload(limit=display_limit, reason=str(exc), db_path=db_path)
        raise


def _build_dashboard_payload_from_initialized_schema(db_path: Path, *, display_limit: int, stale_after_days: int) -> dict[str, Any]:
    operational = get_operational_snapshot(db_path)
    operational["provider_enrichment"] = _provider_enrichment_diagnostics_for_dashboard(db_path)
    coverage = get_mal_recommendation_harvest_coverage(db_path, stale_after_days=stale_after_days)
    coverage["public_userrecs"] = _public_userrecs_diagnostics_for_dashboard(db_path)
    latest_snapshot = _latest_snapshot_summary(db_path)
    latest_run_id = latest_snapshot.get("run_id") if latest_snapshot else None
    latest_raw_rows = list_latest_recommendation_snapshot_rows(db_path, limit=None)
    rows = [_snapshot_row_to_dict(row) for row in latest_raw_rows]
    _overlay_current_eligibility_evidence(db_path, rows)
    latest_has_discovery = any(row.kind == "discovery_candidate" for row in latest_raw_rows)
    diagnostic_source_snapshot: dict[str, Any] | None = None
    if not latest_has_discovery:
        current_rows, current_source = _current_ranked_discovery_rows_from_local_state(db_path, limit=display_limit)
        if current_rows:
            diagnostic_source_snapshot = current_source
            for row in current_rows:
                rows.append(row)
        else:
            diagnostic_raw_rows = list_latest_recommendation_snapshot_rows(db_path, limit=None, kind="discovery_candidate")
            if diagnostic_raw_rows:
                diagnostic_source_snapshot = {
                    "run_id": diagnostic_raw_rows[0].run_id,
                    "generated_at": max(row.generated_at for row in diagnostic_raw_rows),
                    "item_count": len(diagnostic_raw_rows),
                    "selection": "latest persisted snapshot run containing discovery_candidate rows",
                }
                for raw_row in diagnostic_raw_rows:
                    row = _snapshot_row_to_dict(raw_row)
                    row["force_diagnostic_only"] = True
                    row["actionable"] = False
                    row["diagnostic_only"] = True
                    row["visibility_label"] = "Discovery only — provider/dub unverified; not actionable"
                    row["strict_actionability"] = {"eligible": False, "missing": ["not present in latest strict snapshot"]}
                    rows.append(row)
    sections: dict[str, list[dict[str, Any]]] = {}
    section_totals: dict[str, int] = {}
    for row in rows:
        section_key = _static_section_key(row)
        if section_key is None:
            continue
        section_totals[section_key] = section_totals.get(section_key, 0) + 1
        if len(sections.setdefault(section_key, [])) < display_limit:
            sections[section_key].append(row)
    eligibility_counts = _eligibility_coverage_counts(db_path)
    strict_actionable_count = section_totals.get("discovery_available_now", 0)
    dormant_count = section_totals.get("discovery_high_confidence", 0)
    coverage_state = {
        "strict_actionable_count": strict_actionable_count,
        "dormant_candidate_count": dormant_count,
        "evidence_pending_review_count": eligibility_counts["pending_review"],
        "stale_evidence_count": eligibility_counts["stale"],
        "strict_current_evidence_count": eligibility_counts["strict_current"],
        "message": "Zero titles currently have verified current Crunchyroll/HIDIVE + English-dub evidence." if strict_actionable_count == 0 else "Strict actionable discovery is populated from verified current provider+dub evidence.",
        "next_diagnostic_command": STRICT_DISCOVERY_DIAGNOSTIC_COMMAND,
    }
    latest_run = operational.get("latest_sync_run") or {}
    indicators: list[dict[str, str]] = []
    if latest_snapshot is None:
        indicators.append({"level": "warning", "message": "No persisted recommendation snapshot is available yet."})
    elif latest_snapshot.get("item_count", 0) == 0:
        indicators.append({"level": "warning", "message": "Latest recommendation snapshot has no items."})
    if latest_run and latest_run.get("status") not in (None, "completed"):
        indicators.append({"level": "error", "message": f"Latest provider sync run is {latest_run.get('status')}."})
    cov_summary = coverage.get("summary") or {}
    if cov_summary.get("unharvested") or cov_summary.get("stale") or cov_summary.get("failed"):
        indicators.append({"level": "warning", "message": "Recommendation harvest coverage is stale, failed, or incomplete."})
    if strict_actionable_count == 0:
        indicators.append({"level": "warning", "message": coverage_state["message"]})
    if dormant_count:
        indicators.append({"level": "warning", "message": "Dormant discovery diagnostic rows are present; they are not actionable without strict provider+dub proof."})
    if diagnostic_source_snapshot is not None and diagnostic_source_snapshot.get("run_id") != latest_run_id:
        if diagnostic_source_snapshot.get("run_id") == "local-diagnostic-current":
            message = "Ranked discovery recommendations are sourced from current local diagnostic scorer output, not persisted diagnostic data or the latest strict snapshot."
        else:
            message = "Ranked discovery recommendations are sourced from the latest persisted diagnostic discovery snapshot, not the latest strict snapshot."
        indicators.append({"level": "warning", "message": message})
    section_metadata = {kind: _section_metadata_for(kind) for kind in (*_STATIC_SECTION_ORDER, *sections.keys())}
    return {
        "generated_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "snapshot": latest_snapshot,
        "recommendations": {"mode": "diagnostic_snapshot" if dormant_count else "strict_actionable", "strict_default": True, "items": [row for section_rows in sections.values() for row in section_rows], "sections": sections, "section_totals": section_totals, "section_metadata": section_metadata, "coverage_state": coverage_state, "diagnostic_source_snapshot": diagnostic_source_snapshot, "limit": display_limit, "limit_scope": "per_section"},
        "coverage": coverage,
        "operational": operational,
        "recent_sync_runs": _recent_sync_runs(db_path),
        "indicators": indicators,
    }


def render_dynamic_dashboard_html(*, title: str = "MAL-Updater live dashboard", settings_href: str | None = None) -> str:
    template = """<!doctype html>
<html lang=\"en\"><head><meta charset=\"utf-8\"><meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">
<title>__TITLE__</title><style>
*{box-sizing:border-box}body{font-family:system-ui,-apple-system,Segoe UI,sans-serif;margin:0 auto;padding:1.5rem;max-width:110rem;background:#101418;color:#eef3f8;line-height:1.45}h1,h2,h3{line-height:1.2}a{color:#8cc8ff}.top-nav{float:right;margin:.5rem 0 1rem 1rem}.muted{color:#aebccc}.bad{color:#ff9b9b}.warn{color:#ffd37a}.good{color:#b9f6ca}.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(min(220px,100%),1fr));gap:1rem}.card,.banner,.empty-state{background:#161d24;border:1px solid #2b3642;border-radius:.6rem;padding:1rem}.banner{margin:1rem 0}.table-scroll,.ordinary-table-scroll{overflow-x:auto;margin:.75rem 0 1.5rem;border:1px solid #2b3642;border-radius:.55rem}.table-scroll:focus-visible,.ordinary-table-scroll:focus-visible{outline:3px solid #8cc8ff;outline-offset:2px}table{border-collapse:collapse;width:100%;background:#161d24}.table-scroll table{min-width:54rem}.ordinary-table-scroll table{min-width:36rem}th,td{border-right:1px solid #2b3642;border-bottom:1px solid #2b3642;padding:.65rem .75rem;vertical-align:top;text-align:left}th:last-child,td:last-child{border-right:0}tbody tr:last-child>*{border-bottom:0}.table-scroll thead th{position:sticky;top:0;z-index:1;background:#243140}.table-scroll tbody th{min-width:16rem;font-weight:650}tbody tr:nth-child(even){background:#121920}tbody tr:hover{background:#1c2732}.title-text{display:block}.title-providers{display:block;margin-top:.45rem;font-size:.88rem;font-weight:400}.provider-badge{display:inline-block;border:1px solid #4b9fff;border-radius:999px;padding:.08rem .5rem;font-weight:700;margin:.05rem .2rem .05rem 0}.provider-link{display:inline-block;border-radius:999px;text-decoration:none}.provider-link:hover .provider-badge{background:#243f5a}.provider-link:focus-visible{outline:3px solid #8cc8ff;outline-offset:2px}.diagnostic-row{opacity:.86}.diagnostic-label{color:#ffd37a;font-weight:700;margin-bottom:.25rem}.table-empty{text-align:center;color:#aebccc}.sr-only{position:absolute;width:1px;height:1px;padding:0;margin:-1px;overflow:hidden;clip:rect(0,0,0,0);white-space:nowrap;border:0}code{white-space:pre-wrap}@media(max-width:48rem){body{padding:1rem}.card,.banner,.empty-state{padding:.8rem}th,td{padding:.55rem .6rem}}
</style></head><body>__NAVIGATION__<h1>__TITLE__</h1><p class=\"muted\">Live local strict dashboard. Data is fetched from <code>/api/dashboard</code> on load and every 60 seconds.</p><div id=\"app\">Loading…</div><script>
const esc = value => String(value ?? '').replace(/[&<>\"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','\"':'&quot;',"'":'&#39;'}[c]));
const countValue = value => {
  if (value == null) return '';
  if (Array.isArray(value)) return value.length ? value.map(countValue).join(', ') : '<span class=\"muted\">none</span>';
  if (typeof value === 'object') {
    const entries = Object.entries(value);
    return entries.length ? entries.map(([key, nested]) => `<span class=\"muted\">${esc(key)}:</span> ${countValue(nested)}`).join(' · ') : '<span class=\"muted\">none</span>';
  }
  return esc(value);
};
const count = obj => Object.entries(obj || {}).map(([k,v]) => `<div><b>${esc(k)}:</b> ${countValue(v)}</div>`).join('') || '<span class=\"muted\">none</span>';
const providerBadges = r => { const badges = r.provider_badges || r.evidence?.provider_badges || []; if (!badges.length) return esc(r.provider_evidence || r.evidence?.availability_provider_label || (r.availability_providers || []).join(', ') || 'unknown/unverified'); return badges.map(b => { const provider = b.label || b.provider; const label = `<span class=\"provider-badge\">${esc(provider)}</span>`; return b.url ? `<a class=\"provider-link\" href=\"${esc(b.url)}\" rel=\"noreferrer noopener\" aria-label=\"${esc(`Open ${provider} provider proof`)}\">${label}</a>` : label; }).join('<br>'); };
const seedDetails = e => (e?.top_supporting_seeds || []).map(s => { const bits = []; if (s.num_recommendation_votes != null) bits.push(`${s.num_recommendation_votes} MAL vote${s.num_recommendation_votes === 1 ? '' : 's'}`); if (s.user_score != null) bits.push(`score ${s.user_score}`); if (s.status) bits.push(s.status); return `${esc(s.title || s.mal_anime_id || 'MAL seed')}${bits.length ? ` <span class=\"muted\">(${esc(bits.join(', '))})</span>` : ''}`; }).join('<br>') || esc(e?.compact_seeds || '');
const scorecard = r => esc(r.scorecard_summary || r.evidence?.scorecard_summary || '');
const genreText = r => Array.isArray(r.genres) ? r.genres.join(', ') : (r.genres ?? '');
function recTable(rows, meta = {}){ const titleLabel = meta.title_label || 'Title'; const diag = meta.diagnostic_only === 'true' ? '<p class=\"warn\"><strong>Discovery only:</strong> these rows lack strict provider+dub proof and are not watch-now eligible.</p>' : ''; if(!rows?.length) return `${diag}<p class=\"muted\" role=\"status\">No recommendations in this section.</p>`; const headings = ['Priority', 'Title', 'Why recommended', 'Scorecard', 'Top watched seeds', 'Genres']; const head = headings.map(label => `<th scope=\"col\">${esc(label)}</th>`).join(''); const body = rows.map(r => { const e = r.evidence || {}; const diagnostic = r.diagnostic_only ? ' diagnostic-row' : ''; const diagnosticLabel = r.diagnostic_only ? '<div class=\"diagnostic-label\">discovery only · unverified</div>' : ''; const title = esc(r.display_title || r.english_title || r.title); return `<tr class=\"${diagnostic}\"><td>${esc(r.priority ?? r.score)}</td><th scope=\"row\">${diagnosticLabel}<span class=\"title-text\">${title}</span><div class=\"title-providers\" aria-label=\"Provider proof\">${providerBadges(r)}</div></th><td>${esc(r.why_recommended || e.why_recommended || '')}</td><td>${scorecard(r)}</td><td>${seedDetails(e)}</td><td>${esc(genreText(r))}</td></tr>`; }).join(''); return `${diag}<div class=\"table-scroll\" tabindex=\"0\" role=\"region\" aria-label=\"${esc(titleLabel)} recommendations table\"><table><caption class=\"sr-only\">${esc(titleLabel)} recommendations; provider proof appears below each title.</caption><thead><tr>${head}</tr></thead><tbody>${body}</tbody></table></div>`; }
function syncTable(runs){ if(!runs?.length) return '<p class=\"muted\">No sync runs recorded.</p>'; return `<div class=\"ordinary-table-scroll\" tabindex=\"0\" role=\"region\" aria-label=\"Recent provider sync runs table\"><table><thead><tr><th>ID</th><th>Provider</th><th>Mode</th><th>Status</th><th>Started</th><th>Completed</th></tr></thead><tbody>${runs.map(r => `<tr><td>${esc(r.id)}</td><td>${esc(r.provider)}</td><td>${esc(r.mode)}</td><td>${esc(r.status)}</td><td>${esc(r.started_at)}</td><td>${esc(r.completed_at)}</td></tr>`).join('')}</tbody></table></div>`; }
const etaText = (hours, reasons = []) => hours == null ? `unknown${reasons.length ? ` (${esc(reasons.join(', '))})` : ''}` : (hours === 0 ? 'ready now' : `~${esc(hours)}h`);
function publicUserrecsSection(p){ if(!p) return ''; const c = p.coverage || {}; const o = p.open_generations || {}; const h = p.hourly_throughput || {}; const b = p.backlog || {}; const s = p.sustainability || {}; return `<section><h2>Public MAL userrecs crawl</h2><div class=\"grid\"><section class=\"card\"><h3>Crawl coverage</h3>${count({status:p.status, positive_seeds:p.positive_seed_count, complete:c.complete, fresh:c.fresh, stale:c.stale, failed:c.failed, unharvested:c.unharvested, fresh_ratio:c.fresh_ratio})}</section><section class=\"card\"><h3>Backlog/open</h3>${count({due_sources:b.due_sources, active:o.active, paused:o.paused, ready:o.ready, open_total:o.total, staged_pages:o.staged_pages, staged_edges:o.staged_edges})}</section><section class=\"card\"><h3>Hourly throughput</h3>${count({pages_fetched_last_hour:h.pages_fetched_last_hour, last_page:h.last_page_fetched_at, sources_started:h.sources_started_last_hour, sources_published:h.sources_published_last_hour})}</section><section class=\"card\"><h3>Source-start ETA</h3>${count({eta:etaText(b.conservative_source_start_eta_hours), authorized_per_hour:s.authorized_source_titles_per_hour, configured_per_hour:s.configured_source_titles_per_hour, within_authorized_rate:s.within_authorized_rate})}</section><section class=\"card\"><h3>Completion ETA</h3>${count({eta:etaText(b.completion_eta_hours, b.completion_eta_reason_codes || ['unknown_pages_per_source'])})}</section></div></section>`; }
function providerEnrichmentSection(e){ const providers = e?.providers || {}; const cards = Object.entries(providers).map(([slug,p]) => { const c = p.candidates || {}; const d = p.due || {}; const a = p.attempts || {}; const cur = p.cursor || {}; const eta = d.selection_eta || {}; const complete = p.availability_completion_eta || {}; return `<section class="card"><h3>${esc(slug)}</h3>${count({status:p.status, reasons:p.reason_codes, candidates:c.ranked_total, metadata_coverage:c.metadata_coverage_ratio, due:d.total, due_by_class:d.by_class, selected_now:d.current_limit_selection_count, selection_eta_seconds:eta.eta_seconds, selection_eta_reason:eta.reason_code, cursor_mal_id:cur.cursor_mal_anime_id, cursor_generation:cur.cursor_generation, attempted_last_hour:a.distinct_candidates_attempted_last_hour, recent_outcomes:a.recent_24h_outcome_counts, availability_completion_eta:complete.reason_code})}</section>`; }).join(''); if(!cards) return `<section><h2>Provider enrichment health</h2><p class="muted">${esc(e?.reason_codes?.join(', ') || 'No configured credentialed provider enrichment lanes detected.')}</p></section>`; return `<section><h2>Provider enrichment health</h2><div class="grid">${cards}</div></section>`; }
function emptyState(state){ if (!state || state.strict_actionable_count !== 0) return ''; return `<section class=\"empty-state\"><h2>No Watchable now discovery titles</h2><p>${esc(state.message)}</p><ul><li>Ranked discovery recommendations shown as unverified: ${esc(state.dormant_candidate_count)}</li><li>Evidence pending review: ${esc(state.evidence_pending_review_count)}</li><li>Stale/expired evidence: ${esc(state.stale_evidence_count)}</li></ul><p class=\"muted\">Bounded next diagnostic command: <code>${esc(state.next_diagnostic_command)}</code></p></section>`; }
async function refresh(){ const res = await fetch('/api/dashboard', {cache:'no-store'}); const data = await res.json(); const state = data.recommendations?.coverage_state || {}; const indicators = (data.indicators || []).map(i => `<li class=\"${i.level === 'error' ? 'bad' : 'warn'}\">${esc(i.message)}</li>`).join('') || '<li class=\"muted\">No stale/partial/failure indicators.</li>'; const mode = data.recommendations?.mode === 'diagnostic_snapshot' ? '<section class=\"banner warn\"><strong>Discovery visibility enabled:</strong> ranked recommendations may be shown with unknown/unverified provider or dub evidence; only Watchable now is actionable.</section>' : '<section class=\"banner good\"><strong>Watchable now dashboard:</strong> actionable discovery rows require verified current Crunchyroll/HIDIVE identity/catalog presence plus explicit English-dub evidence.</section>'; document.getElementById('app').innerHTML = `${mode}<div class=\"grid\"><section class=\"card\"><h2>Snapshot</h2><div><b>Run:</b> ${esc(data.snapshot?.run_id || 'none')}</div><div><b>Generated:</b> ${esc(data.snapshot?.generated_at || 'n/a')}</div><div><b>Items:</b> ${esc(data.snapshot?.item_count || 0)}</div></section><section class=\"card\"><h2>Strict coverage</h2>${count({watchable_now: state.strict_actionable_count, ranked_unverified_discovery: state.dormant_candidate_count, pending_review: state.evidence_pending_review_count, stale_or_expired: state.stale_evidence_count})}</section><section class=\"card\"><h2>MAL harvest coverage</h2>${count(data.coverage?.summary)}</section><section class=\"card\"><h2>Providers</h2>${count(data.operational?.provider_counts_by_provider)}</section><section class=\"card\"><h2>Review queue</h2>${count(data.operational?.review_queue)}</section></div>${publicUserrecsSection(data.coverage?.public_userrecs)}${providerEnrichmentSection(data.operational?.provider_enrichment)}${emptyState(state)}<section><h2>Indicators</h2><ul>${indicators}</ul></section><section><h2>Recommendations</h2>${Object.entries(data.recommendations?.sections || {}).map(([name, rows]) => { const meta = data.recommendations?.section_metadata?.[name] || {label:name, description:''}; const total = data.recommendations?.section_totals?.[name] ?? rows.length; const countLabel = rows.length < total ? `${rows.length} of ${total}` : `${total}`; return `<h3>${esc(meta.label || name)} (${esc(countLabel)})</h3>${meta.description ? `<p class=\"muted\">${esc(meta.description)}</p>` : ''}${recTable(rows, meta)}`; }).join('') || recTable([])}</section><section><h2>Recent provider sync runs</h2>${syncTable(data.recent_sync_runs)}</section><p class=\"muted\">Last refreshed ${esc(data.generated_at)} · <a href=\"/api/dashboard\">JSON</a></p>`; }
refresh().catch(err => document.getElementById('app').innerHTML = `<p class=\"bad\">${esc(err.message)}</p>`); setInterval(refresh, 60000);
</script></body></html>"""
    navigation = f'<nav class="top-nav"><a href="{escape(settings_href, quote=True)}">Settings</a></nav>' if settings_href else ""
    return template.replace("__TITLE__", escape(title)).replace("__NAVIGATION__", navigation)


def make_dashboard_handler(db_path: Path, *, limit: int = DASHBOARD_DEFAULT_RECOMMENDATION_LIMIT, stale_after_days: int = 14, settings_href: str | None = None) -> type[BaseHTTPRequestHandler]:
    default_limit = normalize_dashboard_limit(limit)

    class DashboardHandler(BaseHTTPRequestHandler):
        def log_message(self, format: str, *args: Any) -> None:  # noqa: A002
            return

        def _send_json(self, payload: Any, status: HTTPStatus = HTTPStatus.OK) -> None:
            body = json.dumps(payload, indent=2, sort_keys=True).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Cache-Control", "no-store")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def _send_html(self, body_text: str) -> None:
            body = body_text.encode("utf-8")
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Cache-Control", "no-store")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self) -> None:  # noqa: N802
            parsed = urlparse(self.path)
            query = parse_qs(parsed.query)
            request_limit = normalize_dashboard_limit(query.get("limit", [None])[0], default=default_limit)
            if parsed.path in ("/", "/dashboard"):
                self._send_html(render_dynamic_dashboard_html(settings_href=settings_href))
                return
            if parsed.path == "/api/dashboard":
                self._send_json(build_dashboard_payload(db_path, limit=request_limit, stale_after_days=stale_after_days))
                return
            self._send_json({"error": "not_found"}, HTTPStatus.NOT_FOUND)

    return DashboardHandler


def serve_dashboard(db_path: Path, *, host: str = "127.0.0.1", port: int = 8766, limit: int = DASHBOARD_DEFAULT_RECOMMENDATION_LIMIT) -> None:
    bootstrap_database(db_path)
    server = ThreadingHTTPServer((host, int(port)), make_dashboard_handler(db_path, limit=normalize_dashboard_limit(limit)))
    print(f"Serving MAL-Updater dashboard at http://{host}:{server.server_port}/")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
