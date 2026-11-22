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

from .config import AppConfig, DEFAULT_SERVICE_RECOMMENDATION_FULL_HARVEST_STALE_AFTER_DAYS, DEFAULT_SERVICE_TASK_EXECUTE_LIMITS, load_config
from .web_shell import FAVICON_LINK, SHELL_CSS, top_bar
from .db import (
    MAL_RECOMMENDATION_HARVEST_SUMMARY_COUNTERS,
    bootstrap_database,
    connect,
    get_mal_recommendation_harvest_coverage,
    get_operational_snapshot,
    get_public_userrecs_diagnostics,
    list_latest_recommendation_snapshot_rows,
    list_recommendation_provider_eligibility_evidence_for_mal_ids,
    list_recommendation_provider_eligibility_evidence_for_provider_series_keys,
    unknown_public_userrecs_diagnostics,
)
from .provider_registry import connected_source_providers

from .recommendation_actionability import (
    is_strict_provider_eligibility_actionable,
    strict_provider_actionability_failure_reasons,
)
from .recommendations import Recommendation, build_recommendations
from .recommendation_enrichment import build_provider_enrichment_diagnostics, unknown_provider_enrichment_diagnostics

DASHBOARD_MIN_RECOMMENDATION_LIMIT = 1
DASHBOARD_MAX_RECOMMENDATION_LIMIT = 500
DASHBOARD_DEFAULT_RECOMMENDATION_LIMIT = 120
GENRE_AFFINITY_DEFAULT_LIMIT = 10
GENRE_AFFINITY_WATCHING_WEIGHT = 0.5
STRICT_DISCOVERY_DIAGNOSTIC_COMMAND = "PYTHONPATH=src python3 -m mal_updater.cli recommend --include-dormant --limit 120"
DASHBOARD_FAVICON_SVG = b"""<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 64 64">
<rect width="64" height="64" rx="12" fill="#101418"/>
<path d="M13 46V18h8l11 15 11-15h8v28h-8V30L32 44 21 30v16z" fill="#8cc8ff"/>
<path d="M13 52h38" stroke="#b9f6ca" stroke-width="5" stroke-linecap="round"/>
</svg>"""

_PROVIDER_LABELS = {"crunchyroll": "Crunchyroll", "hidive": "HIDIVE", "mal": "MyAnimeList", "unknown": "Unknown"}

_SECTION_METADATA: dict[str, dict[str, str]] = {
    "discovery_available_now": {
        "label": "Watchable now",
        "description": "Recommended titles verified as currently streaming on your Crunchyroll or HIDIVE account. Use the Audio filter to show a dub language.",
        "title_label": "English title",
    },
    "discovery_high_confidence": {
        "label": "Ranked discovery recommendations",
        "description": "Ranked MAL recommendation-graph discovery candidates. Provider availability may be unknown or unverified; these rows are not watch-now/actionable unless they also appear in Watchable now.",
        "title_label": "English title",
        "diagnostic_only": "true",
    },
    "discovery_candidate": {
        "label": "Title recommendations / discovery",
        "description": "All fresh MAL user-recommendation-backed discovery candidates, with provider availability evidence when known.",
        "title_label": "English title",
    },
    "mal_sequel": {
        "label": "Continue a franchise",
        "description": "Sequels of titles you completed on MAL that are not on your MAL list yet. Specials, recaps and unaired entries are left out; where they stream is not checked.",
        "title_label": "Title",
    },
    "new_season": {
        "label": "New seasons",
        "description": "Later seasons of shows you finished on your provider.",
        "title_label": "Title",
    },
    "new_episode": {
        "label": "New episodes",
        "description": "New episodes after your progress in shows you watched recently, in the version (sub or dub) you watch.",
        "title_label": "Title",
    },
    "resume_backlog": {
        "label": "Resume backlog",
        "description": "Known provider or MAL-list titles that look ready to resume from existing watch progress.",
        "title_label": "Title",
    },
}

_MAL_ONLY_SECTION_METADATA: dict[str, dict[str, str]] = {
    "discovery_high_confidence": {
        "label": "Recommended from your MAL list",
        "description": "Ranked from your MAL list and MAL community recommendations. No streaming provider is connected, so where each title streams and in which languages are not checked.",
        "title_label": "English title",
    },
}
MAL_ONLY_MESSAGE = (
    "No Crunchyroll or HIDIVE account is connected, so recommendations are ranked from your MAL list and "
    "MAL community recommendations; where each title streams is not checked."
)


def _dashboard_source_providers(config: AppConfig | None) -> list[str] | None:
    """Connected watch-history providers, or None when the configuration is unknown."""
    if config is None:
        return None
    try:
        return connected_source_providers(config)
    except (AttributeError, OSError, ValueError, TypeError):
        return None


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
        for key in ("consensus", "affinity", "quality", "availability", "confidence"):
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
                "audio_locales": item.get("audio_locales") if isinstance(item.get("audio_locales"), list) else [],
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
    review_needed = _review_needed(merged)
    provider_badges = [
        {
            "provider": item.get("provider", ""),
            "label": str(item.get("label") or _provider_label(item.get("provider"))),
            "url": str(item.get("provider_url") or ""),
            "title": str(item.get("provider_title") or ""),
            "provider_series_id": str(item.get("provider_series_id") or ""),
        }
        for item in eligibility_details
        if item.get("provider")
    ] or _provider_badges(providers)
    identity_label = ", ".join(str(value) for value in match_kinds)
    review_labels = _compact_list([item.get("review_status") for item in eligibility_details])
    catalog_labels = _compact_list([item.get("catalog_status") for item in eligibility_details])
    verification_label = "; ".join(
        part
        for part in (
            f"identity {identity_label}" if identity_label else "",
            f"review {', '.join(str(value) for value in review_labels)}" if review_labels else "",
            f"catalog {', '.join(str(value) for value in catalog_labels)}" if catalog_labels else "",
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
        "audio_locales": _row_audio_locales(merged, eligibility_details),
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
        row["visibility_label"] = "Watchable now — verified provider availability"
        row["strict_actionability"] = {"eligible": True, "missing": []}
    else:
        missing = _strict_actionability_failure_reasons(row)
        row["visibility_label"] = "Discovery only — provider availability unverified; not actionable"
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
                unverified_badges.append({**badge, "label": f"{label} (unverified)"})
            row["provider_badges"] = unverified_badges
            row["provider_evidence_html"] = _provider_evidence_html(unverified_badges)
            evidence = row.get("evidence")
            if isinstance(evidence, dict):
                evidence["provider_badges"] = unverified_badges
    return row


def _row_audio_locales(context: dict[str, Any], eligibility_details: list[dict[str, Any]]) -> list[str]:
    """Audio languages for a row: the context's list (provider languages for
    discovery, the watched version for provider progress), else the evidence's."""
    raw = context.get("audio_locales")
    lists = [raw] if isinstance(raw, list) else [item.get("audio_locales") for item in eligibility_details]
    return sorted({str(value) for values in lists if isinstance(values, list) for value in values if isinstance(value, str) and value.strip()})


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
        star_rating = str(badge.get("star_rating") or "").strip()
        maturity_rating = str(badge.get("maturity_rating") or "").strip()
        if star_rating:
            body += f'<span class="provider-star-rating">{label} user rating: {escape(star_rating)}</span>'
        if maturity_rating:
            body += f'<span class="provider-maturity-rating">Maturity: {escape(maturity_rating)}</span>'
        parts.append(body)
    return "<br>".join(parts)


def _number(value: Any) -> int | float | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int | float):
        return value
    return None


def _episode_count(value: Any, *, allow_zero: bool = False) -> int | None:
    if isinstance(value, bool) or not isinstance(value, int | float) or int(value) != value:
        return None
    count = int(value)
    return count if count >= (0 if allow_zero else 1) else None


def _reliable_provider_episode_total(context: dict[str, Any]) -> int | None:
    # These fields represent an explicit series total. max_episode_number and
    # available_episode_count are intentionally excluded: both can merely be
    # the highest episode observed so far and caused misleading X/X+1 output.
    for key in (
        "provider_series_episode_count",
        "provider_total_episode_count",
        "provider_episode_count",
        "series_episode_count",
        "total_episode_count",
    ):
        total = _episode_count(context.get(key))
        if total is not None:
            return total
    return None


def _reliable_episode_total(context: dict[str, Any]) -> int | None:
    provider_total = _reliable_provider_episode_total(context)
    if provider_total is not None:
        return provider_total
    for key in ("mal_num_episodes", "num_episodes"):
        mal_total = _episode_count(context.get(key))
        if mal_total is not None:
            return mal_total
    return None


def _watched_progress(context: dict[str, Any]) -> str:
    # Provider progress is the watched numerator for provider continuation rows.
    # MAL contributes only a trustworthy series total; using MAL watched here can
    # make provider recommendations display stale progress from a different source.
    provider_watched = _episode_count(context.get("completed_episode_count"), allow_zero=True)
    total = _reliable_episode_total(context)
    if provider_watched is not None and total is not None and provider_watched <= total:
        return f"{provider_watched}/{total}"

    return f"{provider_watched if provider_watched is not None else '?'}/?"


def _provider_star_ratings(context: dict[str, Any], provider: Any) -> dict[str, str]:
    stored = context.get("provider_star_ratings")
    ratings = {
        str(key).strip().lower(): str(value).strip()
        for key, value in stored.items()
        if str(key).strip() and value is not None and str(value).strip()
    } if isinstance(stored, dict) else {}
    provider_key = str(provider or "").strip().lower()
    rating = str(context.get("provider_star_rating") or "").strip()
    if provider_key and rating:
        ratings.setdefault(provider_key, rating)
    return ratings


def _provider_maturity_ratings(context: dict[str, Any], provider: Any) -> dict[str, str]:
    stored = context.get("provider_maturity_ratings")
    ratings = {
        str(key).strip().lower(): str(value).strip()
        for key, value in stored.items()
        if str(key).strip() and value is not None and str(value).strip()
    } if isinstance(stored, dict) else {}
    provider_key = str(provider or "").strip().lower()
    rating = str(context.get("provider_maturity_rating") or context.get("provider_rating") or "").strip()
    if provider_key and rating:
        ratings.setdefault(provider_key, rating)
    return ratings


def _attach_provider_metadata(
    badges: list[dict[str, Any]],
    *,
    star_ratings: dict[str, str],
    maturity_ratings: dict[str, str],
) -> None:
    for badge in badges:
        provider = str(badge.get("provider") or "").strip().lower()
        if provider in star_ratings and not badge.get("star_rating"):
            badge["star_rating"] = star_ratings[provider]
        if provider in maturity_ratings and not badge.get("maturity_rating"):
            badge["maturity_rating"] = maturity_ratings[provider]


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
    display_title = english_title.strip() or item.title or item.season_title
    provider_evidence = _availability_evidence_details(context)
    provider_badges = [
        {
            "provider": item.get("provider"),
            "label": item.get("label"),
            "url": item.get("provider_url") or "",
            "title": item.get("provider_title") or "",
            "provider_series_id": item.get("provider_series_id") or "",
        }
        for item in provider_evidence
    ] or _provider_badges(providers)
    provider_star_ratings = _provider_star_ratings(context, item.provider)
    provider_maturity_ratings = _provider_maturity_ratings(context, item.provider)
    _attach_provider_metadata(
        provider_badges,
        star_ratings=provider_star_ratings,
        maturity_ratings=provider_maturity_ratings,
    )
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
    row = {
        "title": display_title,
        "score": item.priority,
        "source_count": source_count,
        "total_votes": total_votes,
        "crunchyroll": _truthy_provider(providers, "crunchyroll"),
        "hidive": _truthy_provider(providers, "hidive"),
        "audio_locales": snapshot_evidence["audio_locales"],
        "provider_evidence": "; ".join(str(badge.get("label")) for badge in provider_badges),
        "provider_evidence_html": _provider_evidence_html(provider_badges),
        "provider_badges": provider_badges,
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
        "watched_progress": _watched_progress(context),
        "episode_total": _reliable_episode_total(context),
        "image_url": str(context.get("cover_image_url") or ""),
        "provider_star_ratings": provider_star_ratings,
        "provider_maturity_ratings": provider_maturity_ratings,
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


# Presentation-only columns. Provider proof is rendered beneath the title; audio
# languages, provider-progress, and MAL-watch fields remain available in row/JSON evidence.
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


_STATIC_SECTION_ORDER: tuple[str, ...] = ("discovery_available_now", "mal_sequel", "discovery_high_confidence", "resume_backlog")

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
        "hidive_progress_provenance": {
            "confirmed_or_measured": 0,
            "history_only_completion_unknown": 0,
            "legacy_unproven_synthetic_completion": 0,
        },
        "provider_freshness": {"series_last_seen_at": None, "progress_last_seen_at": None, "watchlist_last_seen_at": None},
        "provider_freshness_by_provider": {},
        "review_queue": {},
        "mappings": {"total": 0, "approved": 0, "by_source": {}, "by_provider": {}},
        "provider_enrichment": unknown_provider_enrichment_diagnostics(reason="dashboard_schema_unavailable"),
    }


def _empty_mal_recommendation_harvest_coverage() -> dict[str, Any]:
    return {
        "summary": {**dict.fromkeys(MAL_RECOMMENDATION_HARVEST_SUMMARY_COUNTERS, 0), "fresh_coverage_ratio": None},
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
        "genre_affinity": aggregate_genre_affinity([]),
        "recommendations": {
            "mode": "strict_actionable",
            "strict_default": True,
            "items": [],
            "sections": {},
            "section_order": [],
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


def _cap_sections(
    sections: list[tuple[dict[str, str], list[dict[str, Any]], int]], *, limit: int | None
) -> list[tuple[dict[str, str], list[dict[str, Any]], int]]:
    if limit is None:
        return sections
    budget = normalize_dashboard_limit(limit)
    return [(meta, rows[:budget], total) for meta, rows, total in sections]


def _section_count_label(displayed: int, total: int) -> str:
    if displayed < total:
        return f"{displayed} of {total}"
    return str(total)


def _recommendation_table(
    rows: list[dict[str, Any]],
    table_id: str,
    head_cells: str | None = None,
    *,
    title_label: str = "Title",
    progress_section: bool = False,
) -> str:
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
                episode_block = "" if progress_section else f'<div class="title-episodes">Episodes: {escape(str(row.get("episode_total") or "?"))}</div>'
                cell_parts.append(
                    f'<th scope="row" data-key="title" data-sort-value="{escape(str(value), quote=True)}">'
                    f'<span class="title-text">{escape(str(value))}</span>{provider_block}{episode_block}</th>'
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
            'Rows without verified current Crunchyroll/HIDIVE availability are not actionable.</section>'
        )
    return (
        '<section class="banner good"><strong>Watchable now dashboard:</strong> '
        'Actionable discovery rows require fresh verified Crunchyroll/HIDIVE identity and catalog presence. '
        'Use <code>--include-dormant</code> only for diagnostics.</section>'
    )


def _strict_empty_state_html(*, dormant_count: int = 0, pending_review_count: int = 0, stale_count: int = 0) -> str:
    return (
        '<section class="empty-state"><h2>No Watchable now discovery titles</h2>'
        '<p>Zero titles currently have verified current Crunchyroll/HIDIVE availability.</p>'
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
                description += '<p class="warn"><strong>Discovery only:</strong> these rows lack strict actionable provider availability proof and are not watch-now eligible.</p>'
            count_label = _section_count_label(len(section_rows), total_rows)
            table_or_empty = (
                _recommendation_table(
                    section_rows,
                    f"recommendations-{index}",
                    title_label=meta.get("title_label", "Title"),
                    progress_section=meta.get("kind") in {"new_episode", "resume_backlog"},
                )
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
  <link rel="icon" href="/favicon.svg" type="image/svg+xml">
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
    .title-text {{ display: block; }} .title-providers {{ display: block; margin-top: .45rem; font-size: .88rem; font-weight: 400; }} .title-episodes {{ display: block; margin-top: .3rem; font-size: .88rem; font-weight: 400; color: #aebccc; }}
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
        audio_locales = evidence.get("audio_locales", [])
        review_needed = evidence.get("review_needed", False)
    else:
        provider_series = context.get("available_provider_series") if isinstance(context.get("available_provider_series"), list) else []
        match_kinds = context.get("availability_match_kinds")
        if not isinstance(match_kinds, list):
            match_kinds = [series.get("availability_match_kind") for series in provider_series if isinstance(series, dict) and series.get("availability_match_kind")]
        match_sources = [series.get("mapping_source") for series in provider_series if isinstance(series, dict) and series.get("mapping_source")]
        match_confidences = [series.get("mapping_confidence") for series in provider_series if isinstance(series, dict) and series.get("mapping_confidence") is not None]
        providers = getattr(row, "availability_providers", [])
        audio_locales = _row_audio_locales(context, _availability_evidence_details(context))
        review_needed = _review_needed(context)
    return {
        "providers": providers,
        "match_kinds": match_kinds,
        "match_sources": match_sources,
        "match_confidences": match_confidences,
        "confidence": getattr(row, "availability_confidence", None),
        "confidence_label": _availability_confidence_label(row, context),
        "audio_locales": audio_locales,
        "review_needed": review_needed,
    }


def _snapshot_row_to_dict(row: Any) -> dict[str, Any]:
    payload = recommendation_snapshot_row_base_payload(row, collapse_discovery_candidate_reasons=True)
    context = payload.get("context") if isinstance(payload.get("context"), dict) else {}
    english_title = context.get("english_title")
    genres = context.get("genres")
    if isinstance(english_title, str) and english_title.strip():
        payload["english_title"] = english_title.strip()
        payload["display_title"] = english_title.strip()
    else:
        payload["display_title"] = row.title
    payload["genres"] = genres if isinstance(genres, list) else []
    payload["watched_progress"] = _watched_progress(context)
    payload["episode_total"] = _reliable_episode_total(context)
    payload["image_url"] = str(context.get("cover_image_url") or "")
    payload["provider_star_ratings"] = _provider_star_ratings(context, payload.get("provider"))
    payload["provider_maturity_ratings"] = _provider_maturity_ratings(context, payload.get("provider"))
    payload["evidence"] = _snapshot_evidence(payload)
    payload["provider_badges"] = payload["evidence"].get("provider_badges", [])
    _attach_provider_metadata(
        payload["provider_badges"],
        star_ratings=payload["provider_star_ratings"],
        maturity_ratings=payload["provider_maturity_ratings"],
    )
    payload["provider_evidence"] = payload["evidence"].get("availability_provider_label", "")
    payload["audio_locales"] = payload["evidence"].get("audio_locales", [])
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


def _provider_catalog_metadata(source_evidence: dict[str, Any]) -> tuple[str, str, str]:
    match = source_evidence.get("match") if isinstance(source_evidence.get("match"), dict) else {}
    raw = match.get("raw") if isinstance(match.get("raw"), dict) else source_evidence
    if not isinstance(raw, dict):
        return "", "", ""

    image_url = ""
    images = raw.get("images")
    if isinstance(images, dict):
        poster_groups = images.get("poster_tall")
        if isinstance(poster_groups, list):
            posters = [poster for group in poster_groups for poster in (group if isinstance(group, list) else [group]) if isinstance(poster, dict)]
            preferred = next((poster for poster in posters if _number(poster.get("width")) == 120), None)
            preferred = preferred or next((poster for poster in posters if (_number(poster.get("width")) or 0) >= 128), None)
            preferred = preferred or (posters[0] if posters else None)
            if preferred:
                image_url = str(preferred.get("source") or "")
    if not image_url:
        image_url = str(raw.get("smallCoverUrl") or raw.get("coverUrl") or "")

    maturity_ratings: list[str] = []
    series_metadata = raw.get("series_metadata") if isinstance(raw.get("series_metadata"), dict) else {}
    maturity = series_metadata.get("maturity_ratings")
    if isinstance(maturity, list):
        maturity_ratings.extend(str(value) for value in maturity if value)
    regional_ratings = raw.get("ratings")
    if isinstance(regional_ratings, dict):
        preferred = regional_ratings.get("US")
        values = preferred if isinstance(preferred, list) else [preferred]
        maturity_ratings.extend(str(value) for value in values if value)

    star_rating = ""
    for source in (raw, series_metadata):
        for key in ("star_rating", "average_star_rating", "user_rating"):
            value = source.get(key)
            if value is not None and not isinstance(value, bool) and str(value).strip():
                star_rating = str(value).strip()
                break
        if star_rating:
            break
    return image_url, star_rating, ", ".join(dict.fromkeys(maturity_ratings))


def _provider_series_total(raw_json: Any) -> int | None:
    if isinstance(raw_json, str):
        try:
            raw_json = json.loads(raw_json)
        except json.JSONDecodeError:
            return None
    if not isinstance(raw_json, dict):
        return None
    candidates = [raw_json]
    match = raw_json.get("match")
    if isinstance(match, dict):
        match_raw = match.get("raw")
        if isinstance(match_raw, dict):
            candidates.append(match_raw)
    nested_raw = raw_json.get("raw")
    if isinstance(nested_raw, dict):
        candidates.append(nested_raw)
    for candidate in candidates:
        series_metadata = candidate.get("series_metadata")
        if isinstance(series_metadata, dict):
            total = _episode_count(series_metadata.get("episode_count"))
            if total is not None:
                return total
        total = _episode_count(candidate.get("episode_count"))
        if total is not None:
            return total
    return None


def _overlay_provider_catalog_metadata(db_path: Path, rows: list[dict[str, Any]]) -> None:
    keys: set[tuple[str, str]] = set()
    for row in rows:
        if row.get("provider") and row.get("provider_series_id"):
            keys.add((str(row["provider"]).strip().lower(), str(row["provider_series_id"]).strip()))
        badges = row.get("provider_badges") if isinstance(row.get("provider_badges"), list) else []
        keys.update(
            (str(badge.get("provider") or "").strip().lower(), str(badge.get("provider_series_id") or "").strip())
            for badge in badges
            if isinstance(badge, dict) and badge.get("provider") and badge.get("provider_series_id")
        )
    if not keys:
        return
    series_totals: dict[tuple[str, str], int] = {}
    with connect(db_path) as conn:
        for provider, provider_series_id in keys:
            stored = conn.execute(
                "SELECT raw_json FROM provider_series WHERE provider = ? AND provider_series_id = ?",
                (provider, provider_series_id),
            ).fetchone()
            total = _provider_series_total(stored["raw_json"]) if stored is not None else None
            if total is not None:
                series_totals[(provider, provider_series_id)] = total
    evidence_by_key: dict[tuple[str, str], Any] = {}
    for item in list_recommendation_provider_eligibility_evidence_for_provider_series_keys(db_path, keys):
        key = (item.provider.lower(), item.provider_series_id)
        current = evidence_by_key.get(key)
        if current is None or (not current.provider_url and item.provider_url):
            evidence_by_key[key] = item
        evidence_total = _provider_series_total(item.source_evidence)
        if evidence_total is not None:
            series_totals.setdefault(key, evidence_total)
    for row in rows:
        badges = row.get("provider_badges") if isinstance(row.get("provider_badges"), list) else []
        primary_key = (str(row.get("provider") or "").strip().lower(), str(row.get("provider_series_id") or "").strip())
        row_keys = [primary_key] if primary_key in evidence_by_key else []
        row_keys.extend(
            key
            for key in (
                (str(badge.get("provider") or "").strip().lower(), str(badge.get("provider_series_id") or "").strip())
                for badge in badges
                if isinstance(badge, dict)
            )
            if key in evidence_by_key and key not in row_keys
        )
        associated_totals = [series_totals[key] for key in (primary_key, *row_keys) if key in series_totals]
        reliable_provider_total = series_totals.get(primary_key)
        if reliable_provider_total is None and associated_totals and len(set(associated_totals)) == 1:
            reliable_provider_total = associated_totals[0]
        if reliable_provider_total is not None:
            context = row.get("context") if isinstance(row.get("context"), dict) else {}
            context["provider_series_episode_count"] = reliable_provider_total
            row["context"] = context
            row["watched_progress"] = _watched_progress(context)
            row["episode_total"] = _reliable_episode_total(context)
        provider_star_ratings = row.get("provider_star_ratings") if isinstance(row.get("provider_star_ratings"), dict) else {}
        provider_maturity_ratings = row.get("provider_maturity_ratings") if isinstance(row.get("provider_maturity_ratings"), dict) else {}
        for key in row_keys:
            evidence = evidence_by_key[key]
            image_url, star_rating, maturity_rating = _provider_catalog_metadata(evidence.source_evidence)
            row["image_url"] = row.get("image_url") or image_url
            if star_rating:
                provider_star_ratings.setdefault(evidence.provider, star_rating)
            if maturity_rating:
                provider_maturity_ratings.setdefault(evidence.provider, maturity_rating)
            matching_badge = next(
                (
                    badge
                    for badge in badges
                    if isinstance(badge, dict)
                    and str(badge.get("provider") or "").strip().lower() == evidence.provider.lower()
                    and (
                        not badge.get("provider_series_id")
                        or str(badge.get("provider_series_id")).strip() == evidence.provider_series_id
                    )
                ),
                None,
            )
            if matching_badge is not None:
                if star_rating and not matching_badge.get("star_rating"):
                    matching_badge["star_rating"] = star_rating
                if maturity_rating and not matching_badge.get("maturity_rating"):
                    matching_badge["maturity_rating"] = maturity_rating
                if evidence.provider_url:
                    matching_badge["url"] = evidence.provider_url
                    matching_badge["title"] = matching_badge.get("title") or evidence.provider_title or row.get("title") or ""
            elif evidence.provider_url:
                badges.append(
                    {
                        "provider": evidence.provider,
                        "label": _provider_label(evidence.provider),
                        "url": evidence.provider_url,
                        "title": evidence.provider_title or row.get("title") or "",
                        "provider_series_id": evidence.provider_series_id,
                        **({"star_rating": star_rating} if star_rating else {}),
                        **({"maturity_rating": maturity_rating} if maturity_rating else {}),
                    }
                )
        row["provider_star_ratings"] = provider_star_ratings
        row["provider_maturity_ratings"] = provider_maturity_ratings
        row["provider_badges"] = badges


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
            SELECT provider, identity_match_kind, review_status, catalog_status,
                   fetched_at, expires_at, last_verified_at,
                   last_successful_positive_at, invalidated_at
            FROM recommendation_provider_eligibility_evidence
            """
        ).fetchall()
    counts = {"total": len(rows), "strict_current": 0, "pending_review": 0, "stale": 0}
    for row in rows:
        evidence = {
            "provider": row["provider"],
            "identity_match_kind": row["identity_match_kind"],
            "review_status": row["review_status"],
            "catalog_status": row["catalog_status"],
            "fetched_at": row["fetched_at"],
            "expires_at": row["expires_at"],
            "last_verified_at": row["last_verified_at"],
            "last_successful_positive_at": row["last_successful_positive_at"],
            "invalidated_at": row["invalidated_at"],
        }
        if is_strict_provider_eligibility_actionable(evidence):
            counts["strict_current"] += 1
        if row["review_status"] in {"unknown", "review-needed"} or row["catalog_status"] in {"unknown", "review-needed"}:
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
        if row["review_status"] == "stale" or row["catalog_status"] == "stale" or ((expired or verification_only_stale) and not last_known_positive):
            counts["stale"] += 1
    return counts


def _project_root_from_db_path(db_path: Path) -> Path | None:
    resolved = db_path.resolve()
    for parent in (resolved.parent, *resolved.parents):
        if parent.name == ".MAL-Updater":
            return parent.parent
    return None


def _dashboard_config_from_db_path(db_path: Path) -> AppConfig | None:
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


def _dashboard_config(db_path: Path, config: AppConfig | None = None) -> AppConfig | None:
    if config is None:
        return _dashboard_config_from_db_path(db_path)
    try:
        if config.db_path.resolve() != db_path.resolve():
            return None
    except (AttributeError, OSError, TypeError, ValueError):
        return None
    return config


def _genre_names(raw: Any) -> set[str]:
    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except json.JSONDecodeError:
            return set()
    if not isinstance(raw, dict) or not isinstance(raw.get("genres"), list):
        return set()
    names: set[str] = set()
    for genre in raw["genres"]:
        name = genre.get("name") if isinstance(genre, dict) else genre
        if isinstance(name, str) and name.strip():
            names.add(name.strip())
    return names


def aggregate_genre_affinity(rows: Iterable[Any], *, limit: int = GENRE_AFFINITY_DEFAULT_LIMIT) -> dict[str, Any]:
    """Aggregate unique completed/watching MAL titles into a bounded chart model."""
    aggregates: dict[str, dict[str, float | int | str]] = {}
    represented_titles = 0
    for row in rows:
        status = str(row["list_status"] or "").strip().lower()
        weight = 1.0 if status == "completed" else GENRE_AFFINITY_WATCHING_WEIGHT if status == "watching" else 0.0
        genres = _genre_names(row["metadata_raw_json"])
        if not weight or not genres:
            continue
        represented_titles += 1
        for genre in genres:
            item = aggregates.setdefault(genre, {"genre": genre, "title_count": 0, "weighted_count": 0.0})
            item["title_count"] = int(item["title_count"]) + 1
            item["weighted_count"] = float(item["weighted_count"]) + weight
    ordered = sorted(aggregates.values(), key=lambda item: (-float(item["weighted_count"]), -int(item["title_count"]), str(item["genre"]).casefold()))
    bounded = ordered[: max(1, int(limit))]
    maximum = max((float(item["weighted_count"]) for item in bounded), default=0.0)
    axes = [
        {
            **item,
            "weighted_count": round(float(item["weighted_count"]), 2),
            "normalized": round(float(item["weighted_count"]) / maximum * 100, 1) if maximum else 0.0,
        }
        for item in bounded
    ]
    return {
        "axes": axes,
        "available_genre_count": len(ordered),
        "represented_title_count": represented_titles,
        "limit": max(1, int(limit)),
        "completed_weight": 1.0,
        "watching_weight": GENRE_AFFINITY_WATCHING_WEIGHT,
    }


def _genre_affinity_from_local_state(db_path: Path) -> dict[str, Any]:
    with connect(db_path) as conn:
        rows = conn.execute(
            """
            SELECT list.mal_anime_id, list.list_status, metadata.raw_json AS metadata_raw_json
            FROM mal_user_anime_list_cache AS list
            LEFT JOIN mal_anime_metadata AS metadata ON metadata.mal_anime_id = list.mal_anime_id
            WHERE list.list_status IN ('completed', 'watching')
            ORDER BY list.mal_anime_id
            """
        ).fetchall()
    return aggregate_genre_affinity(rows)


def _public_userrecs_policy_kwargs_from_db_path(db_path: Path, *, config: AppConfig | None = None) -> dict[str, int]:
    source_titles_per_hour = int(DEFAULT_SERVICE_TASK_EXECUTE_LIMITS.get("recommend_full_harvest", 3))
    max_pages_per_source_per_run = int(DEFAULT_SERVICE_TASK_EXECUTE_LIMITS.get("recommend_full_harvest_pages", 10))
    stale_after_days = int(DEFAULT_SERVICE_RECOMMENDATION_FULL_HARVEST_STALE_AFTER_DAYS)
    config = _dashboard_config(db_path, config)
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


def _public_userrecs_diagnostics_for_dashboard(db_path: Path, *, reason: str | None = None, config: AppConfig | None = None) -> dict[str, Any]:
    policy_kwargs = _public_userrecs_policy_kwargs_from_db_path(db_path, config=config)
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


def _provider_enrichment_diagnostics_for_dashboard(db_path: Path, *, config: AppConfig | None = None) -> dict[str, Any]:
    config = _dashboard_config(db_path, config)
    if config is None:
        return unknown_provider_enrichment_diagnostics(reason="dashboard_config_unavailable")
    try:
        return build_provider_enrichment_diagnostics(
            config,
            provider_slugs=connected_source_providers(config),
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


def _current_ranked_discovery_rows_from_local_state(db_path: Path, *, limit: int, config: AppConfig | None = None) -> tuple[list[dict[str, Any]], dict[str, Any] | None]:
    """Build current diagnostic discovery rows from local SQLite-backed state only.

    This intentionally uses the recommendation scorer over cached local state instead of any
    provider/MAL network calls, so page requests can expose discovery visibility without
    mutating strict persisted recommendation snapshots.
    """
    resolved_config = _dashboard_config(db_path, config)
    if resolved_config is None:
        return [], None
    try:
        results = build_recommendations(
            resolved_config,
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


def build_dashboard_payload(db_path: Path, *, limit: int = DASHBOARD_DEFAULT_RECOMMENDATION_LIMIT, stale_after_days: int = 14, config: AppConfig | None = None) -> dict[str, Any]:
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
            config=config,
        )
    except sqlite3.OperationalError as exc:
        if _is_schema_unavailable_error(exc):
            return _unavailable_dashboard_payload(limit=display_limit, reason=str(exc), db_path=db_path)
        raise


def _mal_cover_and_genres_from_raw(raw_json: Any) -> tuple[str, list[str]]:
    try:
        raw = json.loads(raw_json) if isinstance(raw_json, str) else raw_json
    except (TypeError, ValueError):
        return "", []
    if not isinstance(raw, dict):
        return "", []
    cover = ""
    picture = raw.get("main_picture")
    if isinstance(picture, dict):
        for key in ("large", "medium"):
            value = picture.get(key)
            if isinstance(value, str) and value.startswith("https://"):
                cover = value.strip()
                break
    genres = [
        str(genre["name"]).strip()
        for genre in raw.get("genres") or []
        if isinstance(genre, dict) and isinstance(genre.get("name"), str) and genre["name"].strip()
    ]
    return cover, genres


_MAL_CDN_PREFIX = "https://cdn.myanimelist.net/"


def _lacks_mal_cover(row: dict[str, Any]) -> bool:
    return not str(row.get("image_url") or "").startswith(_MAL_CDN_PREFIX)


def _fill_missing_mal_metadata(db_path: Path, rows: list[dict[str, Any]]) -> None:
    """Give rows without MAL cover art or genres (e.g. resume backlog) their cached MAL values.

    Provider catalog art is replaced when MAL has a cover: the container CSP
    only admits images from the MAL CDN, so provider hosts would not load.
    """
    missing: dict[int, list[dict[str, Any]]] = {}
    for row in rows:
        mal_id = _number(row.get("mal_anime_id"))
        if (_lacks_mal_cover(row) or not row.get("genres")) and isinstance(mal_id, int) and mal_id > 0:
            missing.setdefault(mal_id, []).append(row)
    if not missing:
        return
    ids = sorted(missing)
    with connect(db_path) as conn:
        found = conn.execute(
            f"SELECT mal_anime_id, raw_json FROM mal_anime_metadata WHERE mal_anime_id IN ({','.join('?' * len(ids))})",
            ids,
        ).fetchall()
    for mal_id, raw_json in found:
        cover, genres = _mal_cover_and_genres_from_raw(raw_json)
        for row in missing.get(int(mal_id), []):
            if cover and _lacks_mal_cover(row):
                row["image_url"] = cover
            if genres and not row.get("genres"):
                row["genres"] = genres


# Fields the live dashboard page reads from each recommendation row; the
# default /api/dashboard payload stays complete for API consumers.
_UI_ROW_FIELDS = (
    "kind", "priority", "score", "title", "display_title", "english_title", "mal_anime_id", "provider",
    "provider_series_id", "diagnostic_only", "image_url", "episode_total", "watched_progress", "genres",
    "provider_badges", "provider_evidence", "availability_providers", "audio_locales",
)


def _ui_why_points(row: dict[str, Any], context: dict[str, Any]) -> list[str]:
    # Snapshot rows carry a reason list; locally scored diagnostic rows
    # (``_row``) carry a joined string, so those fall back to the context text.
    raw_reasons = row.get("reasons")
    reasons = [str(value).strip() for value in raw_reasons if str(value).strip()] if isinstance(raw_reasons, list) else []
    if not reasons:
        text = str(context.get("why_recommended") or row.get("why_recommended") or "")
        reasons = [part.strip() for part in text.split("; ") if part.strip()]
    return [reason.rstrip(".") for reason in reasons]


# Without a connected provider these are constant placeholders on every row.
_MAL_ONLY_HIDDEN_ROW_FIELDS = ("diagnostic_only", "provider_evidence", "availability_providers")
_MAL_ONLY_HIDDEN_SCORE_COMPONENTS = ("availability",)


def _ui_recommendation_row(row: dict[str, Any], *, mal_only: bool = False) -> dict[str, Any]:
    slim = {key: row[key] for key in _UI_ROW_FIELDS if key in row}
    context = row.get("context")
    context = context if isinstance(context, dict) else {}
    scorecard = row.get("scorecard")
    if not isinstance(scorecard, dict):
        scorecard = context.get("scorecard")
    scorecard = scorecard if isinstance(scorecard, dict) else {}
    components = scorecard.get("components")
    evidence = row.get("evidence")
    slim["why_points"] = _ui_why_points(row, context)
    slim["score_total"] = scorecard.get("total")
    slim["score_components"] = components if isinstance(components, dict) else {}
    slim["seeds"] = (evidence.get("top_supporting_seeds") if isinstance(evidence, dict) else None) or []
    if mal_only:
        for key in _MAL_ONLY_HIDDEN_ROW_FIELDS:
            slim.pop(key, None)
        slim["why_points"] = [point for point in slim["why_points"] if not point.lower().startswith("provider availability")]
        slim["score_components"] = {
            key: value for key, value in slim["score_components"].items() if key not in _MAL_ONLY_HIDDEN_SCORE_COMPONENTS
        }
    return slim


def dashboard_ui_payload(payload: dict[str, Any]) -> dict[str, Any]:
    """The live page's view of a dashboard payload: only fields it renders.

    The full payload repeats every row under ``items`` and carries each
    row's complete scoring context (about 38 KB per row, 9 MB in total);
    the page re-fetches every minute and uses a few fields per row.
    """
    recommendations = dict(payload.get("recommendations") or {})
    recommendations.pop("items", None)
    coverage_state = recommendations.get("coverage_state")
    mal_only = isinstance(coverage_state, dict) and coverage_state.get("mal_only") is True
    recommendations["sections"] = {
        name: [_ui_recommendation_row(row, mal_only=mal_only) for row in rows]
        for name, rows in (recommendations.get("sections") or {}).items()
    }
    return {**payload, "recommendations": recommendations}


def _build_dashboard_payload_from_initialized_schema(db_path: Path, *, display_limit: int, stale_after_days: int, config: AppConfig | None = None) -> dict[str, Any]:
    operational = get_operational_snapshot(db_path)
    operational["provider_enrichment"] = _provider_enrichment_diagnostics_for_dashboard(db_path, config=config)
    coverage = get_mal_recommendation_harvest_coverage(db_path, stale_after_days=stale_after_days)
    coverage["public_userrecs"] = _public_userrecs_diagnostics_for_dashboard(db_path, config=config)
    latest_snapshot = _latest_snapshot_summary(db_path)
    latest_run_id = latest_snapshot.get("run_id") if latest_snapshot else None
    latest_raw_rows = list_latest_recommendation_snapshot_rows(db_path, limit=None)
    rows = [_snapshot_row_to_dict(row) for row in latest_raw_rows]
    _overlay_current_eligibility_evidence(db_path, rows)
    _overlay_provider_catalog_metadata(db_path, rows)
    latest_has_discovery = any(row.kind == "discovery_candidate" for row in latest_raw_rows)
    diagnostic_source_snapshot: dict[str, Any] | None = None
    if not latest_has_discovery:
        current_rows, current_source = _current_ranked_discovery_rows_from_local_state(db_path, limit=display_limit, config=config)
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
                    row["visibility_label"] = "Discovery only — provider availability unverified; not actionable"
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
    section_rank = {kind: index for index, kind in enumerate(_STATIC_SECTION_ORDER)}
    sections = dict(sorted(sections.items(), key=lambda item: (section_rank.get(item[0], len(section_rank)), item[0])))
    _fill_missing_mal_metadata(db_path, [row for section_rows in sections.values() for row in section_rows])
    eligibility_counts = _eligibility_coverage_counts(db_path)
    strict_actionable_count = section_totals.get("discovery_available_now", 0)
    dormant_count = section_totals.get("discovery_high_confidence", 0)
    coverage_state = {
        "strict_actionable_count": strict_actionable_count,
        "dormant_candidate_count": dormant_count,
        "evidence_pending_review_count": eligibility_counts["pending_review"],
        "stale_evidence_count": eligibility_counts["stale"],
        "strict_current_evidence_count": eligibility_counts["strict_current"],
        "message": "Zero titles currently have verified current Crunchyroll/HIDIVE availability." if strict_actionable_count == 0 else "Strict actionable discovery is populated from verified current provider availability.",
        "next_diagnostic_command": STRICT_DISCOVERY_DIAGNOSTIC_COMMAND,
    }
    source_providers = _dashboard_source_providers(_dashboard_config(db_path, config))
    mal_only = source_providers == []
    if mal_only:
        coverage_state["message"] = MAL_ONLY_MESSAGE
    coverage_state["source_providers"] = source_providers
    coverage_state["mal_only"] = mal_only
    latest_run = operational.get("latest_sync_run") or {}
    indicators: list[dict[str, str]] = []
    if latest_snapshot is None:
        indicators.append({"level": "warning", "message": "No persisted recommendation snapshot is available yet."})
    elif latest_snapshot.get("item_count", 0) == 0:
        indicators.append({"level": "warning", "message": "Latest recommendation snapshot has no items."})
    if not mal_only and latest_run and latest_run.get("status") not in (None, "completed"):
        indicators.append({"level": "error", "message": f"Latest provider sync run is {latest_run.get('status')}."})
    cov_summary = coverage.get("summary") or {}
    if cov_summary.get("unharvested") or cov_summary.get("stale") or cov_summary.get("failed"):
        indicators.append({"level": "warning", "message": "Recommendation harvest coverage is stale, failed, or incomplete."})
    if mal_only:
        indicators.append({"level": "info", "message": MAL_ONLY_MESSAGE})
    elif strict_actionable_count == 0:
        indicators.append({"level": "warning", "message": coverage_state["message"]})
    if dormant_count and not mal_only:
        indicators.append({"level": "warning", "message": "Dormant discovery diagnostic rows are present; they are not actionable without verified provider availability."})
    if not mal_only and diagnostic_source_snapshot is not None and diagnostic_source_snapshot.get("run_id") != latest_run_id:
        if diagnostic_source_snapshot.get("run_id") == "local-diagnostic-current":
            message = "Ranked discovery recommendations are sourced from current local diagnostic scorer output, not persisted diagnostic data or the latest strict snapshot."
        else:
            message = "Ranked discovery recommendations are sourced from the latest persisted diagnostic discovery snapshot, not the latest strict snapshot."
        indicators.append({"level": "warning", "message": message})
    section_metadata = {kind: _section_metadata_for(kind) for kind in (*_STATIC_SECTION_ORDER, *sections.keys())}
    if mal_only:
        for kind, override in _MAL_ONLY_SECTION_METADATA.items():
            section_metadata[kind] = {"kind": kind, **override}
    return {
        "generated_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "snapshot": latest_snapshot,
        "genre_affinity": _genre_affinity_from_local_state(db_path),
        "recommendations": {"mode": "diagnostic_snapshot" if dormant_count else "strict_actionable", "strict_default": True, "items": [row for section_rows in sections.values() for row in section_rows], "sections": sections, "section_order": list(sections), "section_totals": section_totals, "section_metadata": section_metadata, "coverage_state": coverage_state, "diagnostic_source_snapshot": diagnostic_source_snapshot, "limit": display_limit, "limit_scope": "per_section"},
        "coverage": coverage,
        "operational": operational,
        "recent_sync_runs": _recent_sync_runs(db_path),
        "indicators": indicators,
    }


_COMMON_PAGE_CSS = r"""
.page-status{margin:0;color:var(--muted);font-size:.9rem}
.page-status.bad{color:var(--bad)}
.print-report-meta{display:none}
.card,.alert,.empty-state{background:var(--surface);border:1px solid var(--border);border-radius:var(--radius);padding:1rem 1.15rem;min-width:0}
.card h2,.card h3{font-size:1.05rem;margin-bottom:.6rem}
.alert{margin:0 0 1rem;border-left:4px solid var(--warn)}
.alert.bad{border-left-color:var(--bad)}
.alert.good{border-left-color:var(--good)}
.alert.info{border-left-color:var(--accent)}
.alert h2{font-size:1rem;margin-bottom:.35rem}
.alert ul{margin:.25rem 0 0;padding-left:1.2rem}
.alert p{margin:0}
.empty-state{margin:0 0 1rem}
.empty-state h2{font-size:1.1rem;margin-bottom:.5rem}
.page-foot{margin:2rem 0 .5rem;color:var(--faint);font-size:.86rem}
"""

_PAGE_CSS = r"""
.section-title{display:flex;align-items:baseline;gap:.6rem;margin:1.75rem 0 .35rem;font-size:1.3rem}
.count{font-size:.85rem;font-weight:650;color:var(--muted);background:var(--surface-3);border-radius:999px;padding:.08rem .55rem}
.section-description{margin:0 0 .75rem;color:var(--muted);max-width:60rem}
/* genre affinity */
.affinity-card{margin:0 0 1.5rem}
.affinity-card h2{font-size:1.3rem;margin-bottom:.25rem}
.affinity-layout{display:grid;grid-template-columns:minmax(16rem,24rem) minmax(0,46rem);gap:1.25rem 3rem;align-items:center}
.affinity-chart{display:block;width:100%;height:auto}
.affinity-grid{fill:none;stroke:#3a4757;stroke-width:1}
.affinity-axis{stroke:#3a4757;stroke-width:1}
.affinity-area{fill:#4b9fff40;stroke:var(--accent);stroke-width:2}
.affinity-point{fill:var(--good);stroke:var(--bg);stroke-width:1.5}
.affinity-label{fill:var(--text);font-size:13px;font-weight:650}
.affinity-list{list-style:none;margin:0;padding:0;display:grid;grid-template-columns:auto auto minmax(3rem,1fr) auto;align-items:center;gap:.45rem .75rem}
.affinity-list li{display:contents}
.affinity-rank{color:var(--faint);text-align:right;font-variant-numeric:tabular-nums}
.affinity-name{font-weight:650}
.affinity-numbers{color:var(--muted);font-size:.86rem;font-variant-numeric:tabular-nums;white-space:nowrap}
.affinity-note{margin:.9rem 0 0;color:var(--muted);font-size:.88rem}
.bar{position:relative;display:block;height:.5rem;border-radius:999px;background:var(--surface-3);overflow:hidden}
.bar>span{position:absolute;inset:0 auto 0 0;border-radius:999px;background:var(--accent)}
/* toolbar */
.toolbar{display:flex;flex-wrap:wrap;align-items:center;gap:.6rem .9rem;margin:.75rem 0 .5rem}
.search input{font:inherit;color:var(--text);background:var(--surface);border:1px solid var(--border-strong);border-radius:.45rem;padding:.38rem .7rem;width:16rem;max-width:100%}
.search input::placeholder{color:var(--faint)}
.genre-filter{position:relative}
.genre-menu{position:absolute;z-index:25;top:calc(100% + .35rem);left:0;min-width:13rem;max-height:20rem;overflow-y:auto;padding:.35rem;background:var(--surface-2);border:1px solid var(--border-strong);border-radius:.55rem;box-shadow:0 .75rem 2rem #000a}
.genre-menu button{display:block;width:100%;text-align:left;margin:0;padding:.4rem .6rem;border:0;border-radius:.35rem;color:var(--text);background:transparent;font:inherit;cursor:pointer}
.genre-menu button:hover,.genre-menu button:focus-visible{background:var(--surface-3)}
.genre-chips{display:flex;align-items:center;flex-wrap:wrap;gap:.35rem}
.genre-chip{display:inline-flex;align-items:center;gap:.3rem;padding:.12rem .2rem .12rem .6rem;border:1px solid var(--border-strong);border-radius:999px;background:var(--surface-2);font-size:.9rem}
.genre-chip button{display:inline-grid;place-items:center;width:1.3rem;height:1.3rem;padding:0;border:0;border-radius:50%;color:var(--text);background:var(--surface-3);font:inherit;font-weight:700;line-height:1;cursor:pointer}
.toggle{display:inline-flex;align-items:center;gap:.4rem;color:var(--muted);cursor:pointer}
.audio-filter{display:inline-flex;align-items:center;gap:.4rem;color:var(--muted)}
.audio-filter select{font:inherit;color:var(--text);background:var(--surface);border:1px solid var(--border-strong);border-radius:.45rem;padding:.32rem .5rem}
.audio-languages{margin:.35rem 0 0;color:var(--muted);font-size:.8rem}
.audio-languages .match{color:var(--good);font-weight:700}
.toggle input{accent-color:var(--accent);width:1rem;height:1rem}
/* recommendation tables */
.rec-table{width:100%;border-collapse:separate;border-spacing:0;table-layout:fixed;background:var(--surface);border:1px solid var(--border);border-radius:var(--radius);margin:0 0 1rem}
.rec-table thead th{position:sticky;top:var(--topbar-h);z-index:5;background:var(--surface-3);color:var(--muted);font-size:.78rem;font-weight:700;letter-spacing:.04em;text-transform:uppercase;text-align:left;padding:.6rem .75rem;border-bottom:1px solid var(--border)}
.rec-table thead th:first-child{border-top-left-radius:var(--radius)}
.rec-table thead th:last-child{border-top-right-radius:var(--radius)}
.rec-table td,.rec-table tbody th{padding:.8rem .75rem;vertical-align:top;text-align:left;border-bottom:1px solid var(--border);overflow-wrap:break-word;font-weight:400}
.rec-table tbody tr:last-child>*{border-bottom:0}
.rec-table tbody tr:hover>*{background:#18202a}
.rec-table tr.hidden-recommendation>*{background:#1d242d;color:var(--muted)}
.rec-table tr.hidden-recommendation .cover{opacity:.45}
.rec-table tr.diagnostic-row>*{background:#181b17}
.col-rank{width:6rem}.col-cover{width:6.4rem}.col-title{width:17%}.col-why{width:21%}.col-score{width:12.5rem}
.col-genres{width:8.5rem}.col-actions{width:5.6rem}.col-progress{width:10rem}
/* Tables without the seeds column give its share to "Why recommended". */
.rec-table.no-seeds .col-why{width:auto}
.rank{display:inline-block;min-width:2.4rem;text-align:center;font-weight:750;font-variant-numeric:tabular-nums;background:var(--surface-3);border-radius:.4rem;padding:.12rem .4rem}
.cover-link{display:block;width:4.9rem}
.cover{position:relative;display:block;width:4.9rem;aspect-ratio:225/318;border-radius:.4rem;overflow:hidden;background:var(--surface-3)}
.cover img{display:block;width:100%;height:100%;object-fit:cover}
.cover-missing::after{content:"No cover";position:absolute;inset:0;display:grid;place-items:center;text-align:center;color:var(--faint);font-size:.72rem;padding:.3rem}
.title-link{display:block;color:var(--text);font-weight:700;font-size:1.02rem;text-decoration:none;line-height:1.3}
a.title-link:hover{color:var(--accent-strong);text-decoration:underline}
.diagnostic-label{display:inline-block;margin-bottom:.3rem;color:var(--warn);font-size:.78rem;font-weight:700;text-transform:uppercase;letter-spacing:.03em}
.title-meta{margin:.3rem 0 .45rem;color:var(--muted);font-size:.86rem}
.providers{display:flex;flex-wrap:wrap;align-items:center;gap:.3rem .45rem}
.provider-badge{display:inline-block;border:1px solid #4b8fd6;color:var(--accent-strong);border-radius:999px;padding:.02rem .55rem;font-size:.8rem;font-weight:700}
.provider-badge.unverified{border-color:var(--border-strong);color:var(--muted)}
.provider-link{text-decoration:none;border-radius:999px}
.provider-link:hover .provider-badge{background:#1d3550}
.provider-rating{color:var(--warn);font-size:.82rem}
.why{margin:0;padding:0;list-style:none;display:grid;gap:.3rem}
.why li{position:relative;padding-left:.9rem}
.why li::before{content:"";position:absolute;left:0;top:.62em;width:.32rem;height:.32rem;border-radius:50%;background:var(--faint)}
.score{display:grid;gap:.12rem .9rem;align-content:start}
.score-total{grid-column:1 / -1;margin-bottom:.3rem;font-size:1.35rem;font-weight:750;font-variant-numeric:tabular-nums;line-height:1}
.score-total small{font-size:.75rem;font-weight:600;color:var(--muted)}
.bar-row{display:grid;grid-template-columns:5.3rem minmax(2rem,1fr) 1.9rem;align-items:center;gap:.4rem;font-size:.8rem;line-height:1.45}
.bar-label{color:var(--muted)}
.bar-value{text-align:right;font-variant-numeric:tabular-nums}
.seeds{margin:0;padding:0;list-style:none;display:grid;gap:.4rem}
.seeds a{color:var(--text);text-decoration:none}
.seeds a:hover{color:var(--accent-strong);text-decoration:underline}
.seed-meta{display:block;color:var(--muted);font-size:.82rem}
.more{margin-top:.4rem}
.more summary{cursor:pointer;color:var(--accent);font-size:.86rem}
.more .seeds{margin-top:.4rem}
.tags{margin:0;padding:0;list-style:none;display:flex;flex-direction:column;align-items:flex-start;gap:.25rem}
.tags li{font-size:.82rem;line-height:1.25;background:var(--surface-3);border-radius:.35rem;padding:.12rem .45rem}
.progress-text{font-weight:700;font-variant-numeric:tabular-nums}
.progress-cell .bar{margin-top:.35rem}
@media screen and (max-width:62rem){
.affinity-layout{grid-template-columns:1fr}
.affinity-chart-wrap{max-width:24rem;margin:0 auto;width:100%}
.rec-table,.rec-table tbody{display:block;border:0;background:transparent}
.rec-table colgroup,.rec-table thead{display:none}
.rec-table tr{display:grid;grid-template-columns:4.9rem minmax(0,1fr) auto;gap:.55rem .9rem;background:var(--surface);border:1px solid var(--border);border-radius:var(--radius);padding:.9rem;margin:0 0 .8rem}
.rec-table td,.rec-table tbody th{display:block;padding:0;border:0;background:transparent!important}
.rec-table .c-cover{grid-column:1;grid-row:1 / span 2}
.rec-table .c-rank{grid-column:2;grid-row:1}
.rec-table .c-actions{grid-column:3;grid-row:1}
.rec-table .c-title{grid-column:2 / -1;grid-row:2}
.rec-table .score{grid-template-columns:1fr 1fr}
.rec-table .c-why,.rec-table .c-progress,.rec-table .c-score,.rec-table .c-seeds,.rec-table .c-genres{grid-column:1 / -1}
.rec-table td[data-label]::before{content:attr(data-label);display:block;margin-bottom:.25rem;color:var(--faint);font-size:.72rem;font-weight:700;letter-spacing:.04em;text-transform:uppercase}
.rec-table .c-rank::before{display:inline!important;margin-right:.4rem}
.tags{flex-direction:row;flex-wrap:wrap}
.search input{width:100%}
.search{flex:1 1 14rem}
}
@media screen and (max-width:40rem){.export-pdf-button{display:none}.affinity-list{grid-template-columns:auto auto minmax(0,1fr) auto;gap:.45rem .5rem}.affinity-numbers{white-space:normal;text-align:right}}
@media screen and (max-width:24rem){.rec-table .score{grid-template-columns:1fr}}
@media print{
@page{size:landscape;margin:.45in}
*{-webkit-print-color-adjust:exact;print-color-adjust:exact}
html,body{background:#fff!important;color:#111!important}
body{font-size:8.5pt}
.page{max-width:none;padding:0}
.page-status,.toolbar,.dashboard-interactive-only,.screen-only,.more summary{display:none!important}
.print-report-meta{display:block;margin:0 0 .8rem;color:#333}
.card,.alert,.empty-state,.rec-table{background:#fff!important;color:#111!important;border-color:#aaa!important;box-shadow:none}
.muted,.title-meta,.seed-meta,.bar-label,.affinity-numbers,.section-description,.count{color:#444!important}
.count{background:#eee!important}
.bad{color:#8b0000!important}.warn{color:#704d00!important}.good{color:#075f25!important}
a{color:#1a4f9c!important}
span.title-link{color:#111!important}
a.title-link,.seeds a,.page-foot a{text-decoration:underline;text-decoration-thickness:.5pt;text-underline-offset:1.5pt}
h2,h3{break-after:avoid}
.affinity-card,.alert{break-inside:avoid}
.affinity-layout{grid-template-columns:16rem minmax(0,1fr)}
.affinity-label{fill:#111}
.rec-table thead th{position:static;background:#e8edf2!important;color:#222!important}
.rec-table tr,.rec-table tr>*{background:#fff!important;color:#111!important}
.rec-table td,.rec-table tbody th{padding:.3rem .35rem;border-color:#bbb!important}
.rec-table tr{break-inside:avoid}
.col-rank{width:3rem}.col-cover{width:3.6rem}.col-title{width:16%}.col-why{width:19%}.col-score{width:10rem}
.col-genres{width:6.5rem}.col-progress{width:7rem}.col-actions{width:0}
.cover,.cover-link{width:2.8rem}
.rank{min-width:0;padding:0 .2rem}
.title-link{font-size:9.5pt}
.title-meta,.seed-meta,.tags li,.provider-badge,.provider-rating,.more summary,.section-description{font-size:7.5pt}
.bar-row{grid-template-columns:4.3rem minmax(1rem,1fr) 1.6rem;font-size:7pt}
.tags li,.rank,.bar{background:#eee!important}
.bar>span{background:#4a78a8!important}
.provider-badge{color:#111!important;border-color:#555!important}
.provider-link .provider-badge{color:#1a4f9c!important;border-color:#1a4f9c!important}
.more[open] .seeds,.more .seeds{display:grid}
.more{display:block}
}
"""

_DEBUG_CSS = r"""
.debug-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(min(17rem,100%),1fr));gap:1rem;margin:0 0 1.5rem}
.debug-section-title{font-size:1.25rem;margin:1.75rem 0 .75rem}
.kv{display:grid;grid-template-columns:fit-content(58%) minmax(0,1fr);gap:.3rem .9rem;margin:0;font-size:.9rem}
.kv dt{color:var(--muted)}
.kv dd{margin:0;overflow-wrap:break-word;min-width:0;font-variant-numeric:tabular-nums}
.kv-inline{display:flex;flex-wrap:wrap;gap:.1rem .7rem}
.kv-inline>span{min-width:0;overflow-wrap:break-word}
.card h3.sub{margin-top:1rem}
.status-chip{display:inline-block;border-radius:999px;padding:.02rem .55rem;font-size:.8rem;font-weight:700;background:var(--surface-3)}
.status-chip.good{color:var(--good)}.status-chip.warn{color:var(--warn)}.status-chip.bad{color:var(--bad)}
.data-table{width:100%;border-collapse:separate;border-spacing:0;background:var(--surface);border:1px solid var(--border);border-radius:var(--radius);font-size:.9rem}
.data-table th{background:var(--surface-3);color:var(--muted);font-size:.78rem;text-transform:uppercase;letter-spacing:.04em;text-align:left;padding:.55rem .75rem}
.data-table th:first-child{border-top-left-radius:var(--radius)}.data-table th:last-child{border-top-right-radius:var(--radius)}
.data-table td{padding:.5rem .75rem;border-top:1px solid var(--border);font-variant-numeric:tabular-nums}
.table-scroll{overflow-x:auto}
.indicator-list{margin:0;padding-left:1.2rem}
"""

_COMMON_JS = r"""
const esc = value => String(value ?? '').replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const plural = (n, word, many) => `${Number(n).toLocaleString()} ${Number(n) === 1 ? word : (many || `${word}s`)}`;
const ISO_RE = /^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}(?::\d{2}(?:\.\d+)?)?(?:Z|[+-]\d{2}:?\d{2})?$/;
function parseTime(value){
  if (typeof value !== 'string' || !ISO_RE.test(value.trim())) return null;
  let text = value.trim().replace(' ', 'T');
  if (!/(?:Z|[+-]\d{2}:?\d{2})$/.test(text)) text += 'Z';
  const date = new Date(text);
  return Number.isNaN(date.getTime()) ? null : date;
}
const timeFormat = new Intl.DateTimeFormat(undefined, {dateStyle: 'medium', timeStyle: 'short'});
const dateFormat = new Intl.DateTimeFormat(undefined, {dateStyle: 'medium'});
const formatTime = value => { const date = parseTime(value); return date ? timeFormat.format(date) : String(value ?? ''); };
const timeTag = value => { const date = parseTime(value); return date ? `<time datetime="${esc(date.toISOString())}" title="${esc(date.toISOString())}">${esc(timeFormat.format(date))}</time>` : esc(value); };
let lastSignature = null;
function setPageStatus(text, level){ const el = document.getElementById('page-status'); if (!el) return; el.textContent = text; el.className = `page-status${level ? ` ${level}` : ''}`; }
async function refresh(){
  let data;
  try {
    const response = await fetch(DATA_URL, {cache: 'no-store'});
    if (!response.ok) throw new Error(`HTTP ${response.status}`);
    data = await response.json();
  } catch (error) {
    if (lastSignature === null) {
      const app = document.getElementById('app');
      app.innerHTML = `<div class="alert bad" role="alert"><p><strong>Could not load dashboard data.</strong> ${esc(error.message)}</p></div>`;
      app.removeAttribute('aria-busy');
    }
    setPageStatus(`Refresh failed (${error.message}); retrying in a minute`, 'bad');
    return;
  }
  const signature = pageSignature(data);
  if (signature !== lastSignature) {
    lastSignature = signature;
    renderPage(data);
    document.getElementById('app').removeAttribute('aria-busy');
  }
  setPageStatus(`Updated ${formatTime(data.generated_at)} · refreshes every minute`);
}
"""

_BOOT_JS = r"""
refresh();
setInterval(() => { if (!document.hidden) refresh(); }, 60000);
document.addEventListener('visibilitychange', () => { if (!document.hidden) refresh(); });
"""

_DASHBOARD_JS = r"""
const dismissedTitlesKey = ['malupdater', 'dismissedTitles', 'v1'].join('.');
const audioLanguageKey = ['malupdater', 'audioLanguage', 'v1'].join('.');
let selectedGenres = [];
let audioLanguage = '';
try { audioLanguage = String(localStorage.getItem(audioLanguageKey) || ''); } catch (_) { audioLanguage = ''; }
function persistAudioLanguage(){ try { if (audioLanguage) localStorage.setItem(audioLanguageKey, audioLanguage); else localStorage.removeItem(audioLanguageKey); } catch (_) {} }
let genreMenuOpen = false;
let showHiddenTitles = false;
let titleQuery = '';
let hiddenTitles = new Set();
let latestDashboardData = null;
try {
  const storedHiddenTitles = JSON.parse(localStorage.getItem(dismissedTitlesKey) || '[]');
  if (Array.isArray(storedHiddenTitles)) hiddenTitles = new Set(storedHiddenTitles.filter(value => typeof value === 'string'));
} catch (_) { hiddenTitles = new Set(); }
function persistHiddenTitles(){ try { localStorage.setItem(dismissedTitlesKey, JSON.stringify([...hiddenTitles].sort())); } catch (_) {} }
const recommendationIdentity = r => {
  if (r.mal_anime_id != null && String(r.mal_anime_id).trim()) return `mal:${String(r.mal_anime_id).trim()}`;
  if (r.provider && r.provider_series_id) return `provider:${String(r.provider).trim().toLowerCase()}:${String(r.provider_series_id).trim()}`;
  const title = String(r.display_title || r.english_title || r.title || 'untitled').trim().toLowerCase().replace(/\s+/g, ' ');
  return `fallback:${encodeURIComponent(`${r.kind || 'recommendation'}|${title}`)}`;
};
const malUrl = id => `https://myanimelist.net/anime/${encodeURIComponent(String(id).trim())}`;
// Payload URLs come from provider catalogs; only http(s) may reach href/src.
const webUrl = value => { const text = String(value ?? '').trim(); return /^https?:\/\//i.test(text) ? text : ''; };
const externalLink = (href, html, cls) => `<a${cls ? ` class="${cls}"` : ''} href="${esc(href)}" target="_blank" rel="noopener noreferrer">${html}</a>`;
const SEASON_RE = /^(?:Winter|Spring|Summer|Fall) \d{4}$/;
const MAL_SCORE_RE = /^MAL (\d+(?:\.\d+)?)$/;
// Presentation wording for the recommendation engine's reason strings.
const REASON_REWRITES = [
  [/^recommended from (\d+) watched seed title\(s\)$/i, m => `Recommended by ${plural(m[1], 'title')} you watched`],
  [/^(\d+) MAL recommendation vote\(s\)$/i, m => plural(m[1], 'MAL recommendation vote')],
  [/^watchable as (.+) on ([^;]+)$/i, m => `Watchable on ${m[2]} as “${m[1]}”`],
  [/^watchable on ([^;]+)$/i, m => `Watchable on ${m[1]}`],
  [/^(\d+) contiguous episode\(s\) appear beyond your completed tail progress$/i, m => `${plural(m[1], 'more episode')} after where you stopped`],
  [/^this looks more like a backlog continuation than a fresh release alert$/i, () => null],
  [/^most recent watch activity: (.+)$/i, m => { const date = parseTime(m[1]); return date ? `Last watched ${dateFormat.format(date)}` : `Last watched ${m[1]}`; }],
  [/^MAL watch status: ([\w ]+) \((\S+)\/(\S+) MAL episodes watched\)$/i, m => `On your MAL list: ${m[1].replace(/_/g, ' ')} (${m[2]}/${m[3]} episodes)`],
  [/^MAL watch status: ([\w ]+)$/i, m => `On your MAL list: ${m[1].replace(/_/g, ' ')}`],
];
function tidyReason(text){
  const value = String(text ?? '').trim().replace(/[.…]+$/, '');
  if (!value) return null;
  for (const [pattern, rewrite] of REASON_REWRITES) { const match = value.match(pattern); if (match) return rewrite(match); }
  const generic = value.replace(/(\d+)(\D*?)\b(\w+)\(s\)/g, (_, n, middle, word) => `${n}${middle}${Number(n) === 1 ? word : `${word}s`}`);
  return generic.charAt(0).toUpperCase() + generic.slice(1);
}
function rowPresentation(r){
  const meta = [];
  const why = [];
  for (const point of (Array.isArray(r.why_points) ? r.why_points : [])) {
    const text = String(point).trim();
    if (SEASON_RE.test(text)) { meta.push(text); continue; }
    const score = text.match(MAL_SCORE_RE);
    if (score) { meta.push(`MAL ${score[1]}`); continue; }
    const tidy = tidyReason(text);
    if (tidy) why.push(tidy);
  }
  return {meta, why};
}
function providerBadges(r){
  const badges = Array.isArray(r.provider_badges) ? r.provider_badges : [];
  if (!badges.length) {
    const label = r.provider_evidence || (r.availability_providers || []).join(', ');
    return label ? `<span class="provider-badge unverified">${esc(label)}</span>` : '<span class="muted">Provider unverified</span>';
  }
  return badges.map(b => {
    const name = b.label || b.provider;
    const chip = `<span class="provider-badge">${esc(name)}</span>`;
    const rating = b.star_rating ? `<span class="provider-rating" title="${esc(`${name} user rating`)}">★ ${esc(b.star_rating)}</span>` : '';
    const url = webUrl(b.url);
    return `${url ? `<a class="provider-link" href="${esc(url)}" target="_blank" rel="noopener noreferrer" aria-label="${esc(`Open on ${name}`)}">${chip}</a>` : chip}${rating}`;
  }).join('');
}
function coverCell(r){
  const src = webUrl(r.image_url);
  const image = src ? `<img src="${esc(src)}" alt="" width="225" height="318" loading="lazy" decoding="async">` : '';
  const cover = `<span class="cover${image ? '' : ' cover-missing'}">${image}</span>`;
  return r.mal_anime_id ? `<a class="cover-link" href="${esc(malUrl(r.mal_anime_id))}" target="_blank" rel="noopener noreferrer" tabindex="-1" aria-hidden="true">${cover}</a>` : cover;
}
// Audio languages as the providers report them (BCP 47, e.g. ja-JP, es-419).
let languageNames = null;
try { languageNames = new Intl.DisplayNames(undefined, {type: 'language'}); } catch (_) { languageNames = null; }
const baseLanguage = locale => String(locale).split('-')[0].toLowerCase();
const rowLanguages = r => Array.isArray(r.audio_locales) ? r.audio_locales.filter(value => typeof value === 'string' && value.trim()) : [];
function displayLanguage(locale, ambiguousBases){
  let name = null;
  try { name = languageNames?.of(ambiguousBases?.has(baseLanguage(locale)) ? locale : baseLanguage(locale)); } catch (_) { name = null; }
  return name || locale;
}
function dashboardLanguages(data){
  const locales = new Set(Object.values(data.recommendations?.sections || {}).flat().flatMap(rowLanguages));
  if (audioLanguage) locales.add(audioLanguage);
  const variants = new Map();
  for (const locale of locales) variants.set(baseLanguage(locale), (variants.get(baseLanguage(locale)) || 0) + 1);
  const ambiguous = new Set([...variants].filter(([, count]) => count > 1).map(([base]) => base));
  return [...locales].map(locale => [locale, displayLanguage(locale, ambiguous)]).sort((a, b) => a[1].localeCompare(b[1]));
}
let languageLabels = new Map();
const languageLabel = locale => languageLabels.get(locale) || displayLanguage(locale);
function audioLine(r){
  const languages = rowLanguages(r);
  if (!languages.length) return audioLanguage && r.kind !== 'mal_sequel' && !latestDashboardData?.recommendations?.coverage_state?.mal_only ? '<div class="audio-languages">Audio not checked</div>' : '';
  const label = r.kind === 'new_episode' || r.kind === 'resume_backlog' ? 'You watch in' : 'Audio';
  const names = languages.map(locale => [locale, languageLabel(locale)]).sort((a, b) => a[1].localeCompare(b[1]))
    .map(([locale, name]) => locale === audioLanguage ? `<span class="match">${esc(name)}</span>` : esc(name));
  return `<div class="audio-languages">${label}: ${names.join(' · ')}</div>`;
}
function titleCell(r, extraMeta, progressSection){
  const rawTitle = r.display_title || r.english_title || r.title || 'Untitled';
  const name = r.mal_anime_id ? externalLink(malUrl(r.mal_anime_id), esc(rawTitle), 'title-link') : `<span class="title-link">${esc(rawTitle)}</span>`;
  const maturity = [...new Set((r.provider_badges || []).map(b => b.maturity_rating).filter(Boolean))];
  const meta = [...maturity];
  if (!progressSection && r.episode_total) meta.push(plural(r.episode_total, 'episode'));
  meta.push(...extraMeta);
  const label = r.diagnostic_only ? '<span class="diagnostic-label">Discovery only · unverified</span>' : '';
  const providers = latestDashboardData?.recommendations?.coverage_state?.mal_only || r.kind === 'mal_sequel' ? '' : `<div class="providers" aria-label="Where to watch">${providerBadges(r)}</div>`;
  return `${label}${name}${meta.length ? `<div class="title-meta">${esc(meta.join(' · '))}</div>` : ''}${providers}${audioLine(r)}`;
}
const SCORE_PARTS = [['consensus', 'Consensus'], ['affinity', 'Affinity'], ['quality', 'Quality'], ['availability', 'Availability'], ['confidence', 'Confidence']];
function scoreCell(r){
  const total = Number(r.score_total);
  const parts = r.score_components || {};
  const bars = SCORE_PARTS.filter(([key]) => parts[key] != null).map(([key, label]) => {
    const value = Math.max(0, Math.min(100, Number(parts[key]) || 0));
    return `<div class="bar-row"><span class="bar-label">${label}</span><span class="bar" aria-hidden="true"><span style="width:${value}%"></span></span><span class="bar-value">${Math.round(value)}</span></div>`;
  }).join('');
  if (!Number.isFinite(total) && !bars) return '<span class="muted">—</span>';
  return `<div class="score">${Number.isFinite(total) ? `<div class="score-total">${esc(total.toFixed(1))} <small>/ 100</small></div>` : ''}${bars}</div>`;
}
function seedItem(seed){
  const bits = [];
  if (seed.num_recommendation_votes != null) bits.push(plural(seed.num_recommendation_votes, 'vote'));
  if (seed.user_score != null) bits.push(`you scored ${seed.user_score}`);
  if (seed.status) bits.push(String(seed.status).replace(/_/g, ' '));
  const label = esc(seed.title || 'MAL title');
  const name = seed.mal_anime_id ? externalLink(malUrl(seed.mal_anime_id), label) : label;
  return `<li>${name}${bits.length ? `<span class="seed-meta">${esc(bits.join(' · '))}</span>` : ''}</li>`;
}
function seedsCell(r){
  const seeds = Array.isArray(r.seeds) ? r.seeds : [];
  if (!seeds.length) return '<span class="muted">—</span>';
  const rest = seeds.slice(3);
  return `<ul class="seeds">${seeds.slice(0, 3).map(seedItem).join('')}</ul>${rest.length ? `<details class="more"><summary>${esc(plural(rest.length, 'more title'))}</summary><ul class="seeds">${rest.map(seedItem).join('')}</ul></details>` : ''}`;
}
function progressCell(r){
  const match = String(r.watched_progress || '').match(/^(\d+)\s*\/\s*(\d+)$/);
  if (!match) return `<span class="progress-text">${esc(r.watched_progress || '?/?')}</span>`;
  const percent = Number(match[2]) > 0 ? Math.min(100, Math.round(Number(match[1]) / Number(match[2]) * 100)) : 0;
  return `<div class="progress-cell"><span class="progress-text">${esc(match[1])} / ${esc(match[2])}</span> <span class="muted">episodes</span><span class="bar" aria-hidden="true"><span style="width:${percent}%"></span></span></div>`;
}
const genreList = r => { const genres = Array.isArray(r.genres) ? r.genres : []; return genres.length ? `<ul class="tags">${genres.map(genre => `<li>${esc(genre)}</li>`).join('')}</ul>` : '<span class="muted">—</span>'; };
function recRow(r, progressSection, sequelSection = false){
  const identity = recommendationIdentity(r);
  const hidden = hiddenTitles.has(identity);
  const {meta, why} = rowPresentation(r);
  const classes = [r.diagnostic_only ? 'diagnostic-row' : '', hidden ? 'hidden-recommendation' : ''].filter(Boolean).join(' ');
  const whyHtml = why.length ? `<ul class="why">${why.map(point => `<li>${esc(point)}</li>`).join('')}</ul>` : '<span class="muted">—</span>';
  const cells = [
    `<td class="c-rank" data-label="Priority"><span class="rank" title="Recommendation priority">${esc(r.priority ?? r.score ?? '')}</span></td>`,
    `<td class="c-cover">${coverCell(r)}</td>`,
    `<th scope="row" class="c-title">${titleCell(r, meta, progressSection)}</th>`,
    progressSection ? `<td class="c-progress" data-label="Progress">${progressCell(r)}</td>` : '',
    `<td class="c-why" data-label="Why recommended">${whyHtml}</td>`,
    progressSection || sequelSection ? '' : `<td class="c-score" data-label="Score">${scoreCell(r)}</td><td class="c-seeds" data-label="Top watched seeds">${seedsCell(r)}</td>`,
    `<td class="c-genres" data-label="Genres">${genreList(r)}</td>`,
    `<td class="c-actions dashboard-interactive-only"><button type="button" class="button small row-visibility-button" data-recommendation-id="${esc(identity)}" aria-pressed="${hidden}">${hidden ? 'Unhide' : 'Hide'}</button></td>`,
  ];
  return `<tr${classes ? ` class="${classes}"` : ''} data-recommendation-id="${esc(identity)}">${cells.join('')}</tr>`;
}
function recTable(rows, meta){
  const progressSection = meta.kind === 'new_episode' || meta.kind === 'resume_backlog';
  const sequelSection = meta.kind === 'mal_sequel';
  const columns = progressSection
    ? [['rank', 'Priority'], ['cover', '<span class="sr-only">Cover</span>'], ['title', 'Title'], ['progress', 'Progress'], ['why', 'Why recommended'], ['genres', 'Genres'], ['actions', '<span class="sr-only">Visibility</span>']]
    : sequelSection
    ? [['rank', 'Priority'], ['cover', '<span class="sr-only">Cover</span>'], ['title', 'Title'], ['why', 'Why recommended'], ['genres', 'Genres'], ['actions', '<span class="sr-only">Visibility</span>']]
    : [['rank', 'Priority'], ['cover', '<span class="sr-only">Cover</span>'], ['title', 'Title'], ['why', 'Why recommended'], ['score', 'Score'], ['seeds', 'Top watched seeds'], ['genres', 'Genres'], ['actions', '<span class="sr-only">Visibility</span>']];
  const colgroup = columns.map(([key]) => `<col class="col-${key}">`).join('');
  const head = columns.map(([key, label]) => `<th scope="col"${key === 'actions' ? ' class="dashboard-interactive-only"' : ''}>${label}</th>`).join('');
  const tableClass = columns.some(([key]) => key === 'seeds') ? 'rec-table' : 'rec-table no-seeds';
  return `<table class="${tableClass}"><caption class="sr-only">${esc(meta.label || 'Recommendations')}</caption><colgroup>${colgroup}</colgroup><thead><tr>${head}</tr></thead><tbody>${rows.map(r => recRow(r, progressSection, sequelSection)).join('')}</tbody></table>`;
}
function visibleRows(rows){
  const query = titleQuery.trim().toLowerCase();
  return rows.filter(r => {
    if (selectedGenres.length && !(Array.isArray(r.genres) && selectedGenres.every(genre => r.genres.includes(genre)))) return false;
    // Titles whose audio languages were not reported stay visible (labelled "Audio not checked").
    if (audioLanguage && rowLanguages(r).length && !rowLanguages(r).includes(audioLanguage)) return false;
    if (!showHiddenTitles && hiddenTitles.has(recommendationIdentity(r))) return false;
    if (query && ![r.display_title, r.english_title, r.title].some(value => String(value || '').toLowerCase().includes(query))) return false;
    return true;
  });
}
function sectionsHtml(data){
  // JSON responses sort keys, so the server sends the display order separately.
  const byName = data.recommendations?.sections || {};
  const order = (data.recommendations?.section_order || []).filter(name => name in byName);
  const sections = [...order, ...Object.keys(byName).filter(name => !order.includes(name))].map(name => [name, byName[name]]);
  if (!sections.length) return '<p class="muted" role="status">No recommendations yet.</p>';
  return sections.map(([name, rows]) => {
    const meta = {kind: name, label: name, ...(data.recommendations?.section_metadata?.[name] || {})};
    const shown = visibleRows(rows);
    const countLabel = shown.length === rows.length ? `${rows.length}` : `${shown.length} of ${rows.length}`;
    const description = meta.description ? `<p class="section-description">${esc(meta.description)}</p>` : '';
    const diagnostic = meta.diagnostic_only === 'true' ? '<p class="alert"><strong>Discovery only:</strong> these titles lack verified provider availability, so they are not watchable-now recommendations.</p>' : '';
    const body = shown.length ? recTable(shown, meta) : '<p class="muted" role="status">No recommendations match the current filters in this section.</p>';
    return `<section aria-labelledby="section-${esc(name)}"><h3 class="section-title" id="section-${esc(name)}">${esc(meta.label || name)} <span class="count">${esc(countLabel)}</span></h3>${description}${diagnostic}${body}</section>`;
  }).join('');
}
function dashboardGenres(data){ return [...new Set(Object.values(data.recommendations?.sections || {}).flat().flatMap(r => Array.isArray(r.genres) ? r.genres : []).filter(g => typeof g === 'string' && g.trim()).map(g => g.trim()))].sort((a, b) => a.localeCompare(b)); }
function genreControlsHtml(data){
  const availableGenres = dashboardGenres(data).filter(genre => !selectedGenres.includes(genre));
  const options = availableGenres.map(genre => `<button type="button" role="menuitem" class="genre-option" data-genre="${esc(genre)}">${esc(genre)}</button>`).join('') || '<span class="muted" role="status">All available genres are selected.</span>';
  const menu = genreMenuOpen ? `<div id="genre-menu" class="genre-menu" role="menu" aria-label="Available genres">${options}</div>` : '';
  return `<button type="button" id="genre-menu-trigger" class="button genre-menu-trigger" aria-haspopup="menu" aria-controls="genre-menu" aria-expanded="${genreMenuOpen}">Add genre filter</button>${menu}`;
}
function audioControlHtml(data){
  const languages = dashboardLanguages(data);
  languageLabels = new Map(languages);
  const options = [['', 'Any language'], ...languages].map(([value, name]) => `<option value="${esc(value)}"${value === audioLanguage ? ' selected' : ''}>${esc(name)}</option>`).join('');
  return `<span>Audio</span><select id="audio-language" aria-label="Audio language">${options}</select>`;
}
const chipsHtml = () => selectedGenres.map(genre => `<span class="genre-chip"><span>${esc(genre)}</span><button type="button" class="genre-remove" data-genre="${esc(genre)}" aria-label="${esc(`Remove ${genre} genre filter`)}">×</button></span>`).join('');
function genreAffinitySection(model){
  const axes = Array.isArray(model?.axes) ? model.axes : [];
  if (axes.length < 3) return `<section id="genre-affinity" class="card affinity-card"><h2>Genre affinity</h2><p class="muted" role="status">Not enough completed/currently-watching titles with genre metadata to draw a radar chart.</p></section>`;
  const centerX = 240, centerY = 220, radius = 130;
  const angleFor = index => -Math.PI / 2 + index * Math.PI * 2 / axes.length;
  const point = (index, scale = 1) => { const angle = angleFor(index); return [centerX + Math.cos(angle) * radius * scale, centerY + Math.sin(angle) * radius * scale]; };
  const polygon = scale => axes.map((_, index) => point(index, scale).join(',')).join(' ');
  const radialScale = axis => Math.sqrt(Math.max(0, Math.min(100, Number(axis.normalized || 0))) / 100);
  const grid = [0.25, 0.5, 0.75, 1].map(scale => `<polygon class="affinity-grid" points="${polygon(scale)}"/>`).join('');
  const axisLines = axes.map((_, index) => { const [x, y] = point(index); return `<line class="affinity-axis" x1="${centerX}" y1="${centerY}" x2="${x}" y2="${y}"/>`; }).join('');
  const labels = axes.map((axis, index) => { const angle = angleFor(index); const [x, y] = point(index, 1.16); const horizontal = Math.cos(angle), vertical = Math.sin(angle); const anchor = horizontal > 0.25 ? 'start' : horizontal < -0.25 ? 'end' : 'middle'; const baseline = vertical > 0.4 ? 'hanging' : vertical < -0.4 ? 'auto' : 'middle'; return `<text class="affinity-label" x="${x}" y="${y}" text-anchor="${anchor}" dominant-baseline="${baseline}">${esc(axis.genre)}</text>`; }).join('');
  const values = axes.map((axis, index) => point(index, radialScale(axis)).join(',')).join(' ');
  const points = axes.map((axis, index) => { const [x, y] = point(index, radialScale(axis)); return `<circle class="affinity-point" cx="${x}" cy="${y}" r="5"><title>${esc(`${axis.genre}: ${axis.title_count} titles, ${axis.weighted_count} weighted`)}</title></circle>`; }).join('');
  const list = axes.map((axis, index) => `<li><span class="affinity-rank">${index + 1}</span><span class="affinity-name">${esc(axis.genre)}</span><span class="bar" aria-hidden="true"><span style="width:${Math.max(0, Math.min(100, Number(axis.normalized) || 0))}%"></span></span><span class="affinity-numbers">${esc(plural(axis.title_count, 'title'))} · ${esc(axis.normalized)}%</span></li>`).join('');
  const watchingWeight = Number(model.watching_weight);
  const weighting = Number.isFinite(watchingWeight) && watchingWeight !== 1 ? ` Titles you are still watching count ${watchingWeight === 0.5 ? 'half' : `×${watchingWeight}`}; percentages are relative to your top genre.` : '';
  return `<section id="genre-affinity" class="card affinity-card" aria-labelledby="genre-affinity-title"><h2 id="genre-affinity-title">Genre affinity</h2><div class="affinity-layout"><div class="affinity-chart-wrap"><svg class="affinity-chart" viewBox="-60 0 600 440" role="img" aria-labelledby="genre-affinity-svg-title genre-affinity-svg-desc"><title id="genre-affinity-svg-title">Genre affinity radar chart</title><desc id="genre-affinity-svg-desc">Normalized affinity for ${esc(axes.map(axis => axis.genre).join(', '))}. Exact values follow the chart.</desc>${grid}${axisLines}<polygon class="affinity-area" points="${values}"/>${points}${labels}</svg></div><div><h3 class="sr-only">Exact values</h3><ol class="affinity-list">${list}</ol><p class="affinity-note">${esc(plural(model.represented_title_count, 'watched title'))} contributed genre data.${esc(weighting)}</p></div></div></section>`;
}
function alertsHtml(data){
  const errors = (data.indicators || []).filter(i => i.level === 'error').map(i => i.message);
  const failedHarvests = Number(data.coverage?.summary?.failed || 0);
  let html = '';
  if (errors.length) html += `<section class="alert bad" role="alert" aria-labelledby="dashboard-failures"><h2 id="dashboard-failures" class="bad">Needs attention</h2><ul>${errors.map(message => `<li>${esc(message)}</li>`).join('')}</ul></section>`;
  if (failedHarvests > 0) html += `<section class="alert"><p><strong>${esc(plural(failedHarvests, 'MAL recommendation source'))} failed to harvest.</strong> They are retried automatically; see <a href="${esc(DEBUG_HREF)}">Debug</a> for details.</p></section>`;
  if (data.recommendations?.coverage_state?.mal_only) html += `<section class="alert info"><p><strong>MAL-only mode.</strong> ${esc(data.recommendations.coverage_state.message)} ${SETTINGS_HREF ? `Connect an account in <a href="${esc(SETTINGS_HREF)}">Settings</a> to see what you can watch now.` : ''}</p></section>`;
  else if (data.recommendations?.mode === 'diagnostic_snapshot') html += '<section class="alert"><p><strong>Discovery visibility enabled:</strong> ranked recommendations may be shown with unknown or unverified provider availability; only Watchable now is actionable.</p></section>';
  return html;
}
function emptyState(state){
  if (!state || state.mal_only || state.strict_actionable_count !== 0) return '';
  return `<section class="empty-state"><h2>No Watchable now discovery titles</h2><p>${esc(state.message)}</p><ul><li>Ranked discovery recommendations shown as unverified: ${esc(state.dormant_candidate_count)}</li><li>Evidence pending review: ${esc(state.evidence_pending_review_count)}</li><li>Stale/expired evidence: ${esc(state.stale_evidence_count)}</li></ul><p class="muted">Bounded next diagnostic command: <code>${esc(state.next_diagnostic_command)}</code></p></section>`;
}
function renderControls(){
  const data = latestDashboardData;
  document.getElementById('genre-filter').innerHTML = genreControlsHtml(data);
  document.getElementById('genre-chips').innerHTML = chipsHtml();
  document.getElementById('audio-filter').innerHTML = audioControlHtml(data);
  const hiddenCount = Object.values(data.recommendations?.sections || {}).flat().filter(r => hiddenTitles.has(recommendationIdentity(r))).length;
  document.getElementById('hidden-count').textContent = hiddenCount ? ` (${hiddenCount})` : '';
  document.getElementById('clear-filters').hidden = !(selectedGenres.length || titleQuery || audioLanguage);
}
function renderSections(){ document.getElementById('sections').innerHTML = sectionsHtml(latestDashboardData); }
function renderPage(data){
  latestDashboardData = data;
  const app = document.getElementById('app');
  if (!document.getElementById('sections')) {
    app.innerHTML = `<div id="alerts"></div><div id="empty"></div><div id="affinity"></div><section aria-labelledby="recommendations-title"><h2 id="recommendations-title" class="section-title">Recommendations</h2><div class="toolbar"><label class="search"><span class="sr-only">Search titles</span><input id="title-search" type="search" placeholder="Search titles" autocomplete="off"></label><div class="genre-filter" id="genre-filter" data-genre-control></div><div class="genre-chips" id="genre-chips" aria-label="Selected genre filters"></div><label class="audio-filter" id="audio-filter"></label><label class="toggle"><input id="show-hidden-titles" type="checkbox"> Show hidden titles<span id="hidden-count"></span></label><button type="button" id="clear-filters" class="button small" hidden>Clear filters</button></div><div id="sections"></div></section><p class="page-foot screen-only">Raw data: <a href="/api/dashboard">dashboard JSON</a></p>`;
    bindDashboardEvents();
  }
  document.getElementById('alerts').innerHTML = alertsHtml(data);
  document.getElementById('empty').innerHTML = emptyState(data.recommendations?.coverage_state);
  document.getElementById('affinity').innerHTML = genreAffinitySection(data.genre_affinity);
  renderControls();
  renderSections();
}
function closeGenreMenu(focusTrigger){ if (!genreMenuOpen) return; genreMenuOpen = false; renderControls(); if (focusTrigger) document.getElementById('genre-menu-trigger')?.focus(); }
function bindDashboardEvents(){
  const app = document.getElementById('app');
  app.addEventListener('click', event => {
    const target = event.target.closest('button');
    if (!target) return;
    if (target.id === 'genre-menu-trigger') { genreMenuOpen = !genreMenuOpen; renderControls(); (genreMenuOpen ? document.querySelector('.genre-option') : document.getElementById('genre-menu-trigger'))?.focus(); return; }
    if (target.classList.contains('genre-option')) { const genre = target.dataset.genre; if (genre && !selectedGenres.includes(genre)) selectedGenres = [...selectedGenres, genre]; genreMenuOpen = false; renderControls(); renderSections(); document.getElementById('genre-menu-trigger')?.focus(); return; }
    if (target.classList.contains('genre-remove')) { selectedGenres = selectedGenres.filter(genre => genre !== target.dataset.genre); renderControls(); renderSections(); document.getElementById('genre-menu-trigger')?.focus(); return; }
    if (target.id === 'clear-filters') { selectedGenres = []; titleQuery = ''; audioLanguage = ''; persistAudioLanguage(); document.getElementById('title-search').value = ''; renderControls(); renderSections(); document.getElementById('title-search').focus(); return; }
    if (target.classList.contains('row-visibility-button')) {
      const identity = target.dataset.recommendationId;
      if (!identity) return;
      if (hiddenTitles.has(identity)) hiddenTitles.delete(identity); else hiddenTitles.add(identity);
      persistHiddenTitles();
      renderControls();
      renderSections();
    }
  });
  document.getElementById('title-search').addEventListener('input', event => { titleQuery = event.target.value; renderControls(); renderSections(); });
  document.getElementById('show-hidden-titles').addEventListener('change', event => { showHiddenTitles = event.target.checked; renderSections(); });
  app.addEventListener('change', event => { if (event.target.id !== 'audio-language') return; audioLanguage = event.target.value; persistAudioLanguage(); renderControls(); renderSections(); document.getElementById('audio-language')?.focus(); });
  app.addEventListener('keydown', event => {
    if (!genreMenuOpen || !event.target.classList?.contains('genre-option') || !['ArrowDown', 'ArrowUp'].includes(event.key)) return;
    const options = [...document.querySelectorAll('.genre-option')];
    const index = options.indexOf(event.target);
    options[(index + (event.key === 'ArrowDown' ? 1 : options.length - 1)) % options.length]?.focus();
    event.preventDefault();
  });
}
document.addEventListener('keydown', event => { if (event.key === 'Escape' && genreMenuOpen) closeGenreMenu(true); });
// composedPath() is captured at dispatch, so it still holds the genre control
// when the click re-rendered (detached) the button that was clicked.
document.addEventListener('click', event => { if (genreMenuOpen && !event.composedPath().some(node => node instanceof Element && node.matches('[data-genre-control]'))) closeGenreMenu(false); });
document.addEventListener('error', event => { const image = event.target; if (image instanceof HTMLImageElement && image.parentElement?.classList.contains('cover')) { image.parentElement.classList.add('cover-missing'); image.remove(); } }, true);
function pageSignature(data){ return JSON.stringify([data.recommendations, data.genre_affinity, data.indicators, data.coverage?.summary?.failed]); }
function updateExportMetadata(){
  const timestamp = document.getElementById('export-timestamp');
  if (timestamp) timestamp.textContent = timeFormat.format(new Date());
  const summary = document.getElementById('export-filter-summary');
  if (summary) summary.textContent = [selectedGenres.length ? `Genres: ${selectedGenres.join(', ')}` : 'All genres', audioLanguage ? `Audio: ${languageLabel(audioLanguage)}` : 'Any audio language', titleQuery ? `Search: “${titleQuery}”` : '', `Hidden titles ${showHiddenTitles ? 'shown' : 'excluded'}`].filter(Boolean).join(' · ');
}
document.getElementById('export-pdf')?.addEventListener('click', () => { closeGenreMenu(false); document.querySelectorAll('details.more').forEach(details => { details.open = true; }); updateExportMetadata(); window.print(); });
window.addEventListener('beforeprint', updateExportMetadata);
"""

_DEBUG_JS = r"""
const PROVIDER_NAMES = {crunchyroll: 'Crunchyroll', hidive: 'HIDIVE', mal: 'MyAnimeList'};
const humanKey = key => { const text = String(key).replace(/_/g, ' ').trim(); return text.charAt(0).toUpperCase() + text.slice(1); };
function formatValue(key, value){
  if (value == null || value === '') return '<span class="muted">—</span>';
  if (typeof value === 'boolean') return value ? 'yes' : 'no';
  if (typeof value === 'number') return /ratio|coverage/i.test(key) && value >= 0 && value <= 1 ? `${(value * 100).toFixed(1)}%` : esc(value.toLocaleString());
  if (Array.isArray(value)) return value.length ? value.map(item => formatValue(key, item)).join(', ') : '<span class="muted">none</span>';
  if (typeof value === 'object') { const entries = Object.entries(value); return entries.length ? `<span class="kv-inline">${entries.map(([k, v]) => `<span><span class="muted">${esc(humanKey(PROVIDER_NAMES[k] || k))}</span> ${formatValue(k, v)}</span>`).join('')}</span>` : '<span class="muted">none</span>'; }
  const text = String(value);
  if (parseTime(text)) return timeTag(text);
  if (/^[a-z0-9]+(?:_[a-z0-9]+)+$/.test(text)) return esc(text.replace(/_/g, ' '));
  return esc(text);
}
const kv = object => { const entries = Object.entries(object || {}); return entries.length ? `<dl class="kv">${entries.map(([key, value]) => `<dt>${esc(humanKey(PROVIDER_NAMES[key] || key))}</dt><dd>${formatValue(key, value)}</dd>`).join('')}</dl>` : '<p class="muted">None</p>'; };
const card = (title, body, level = 'h3') => `<section class="card"><${level}>${esc(title)}</${level}>${body}</section>`;
function providerEnrichmentSection(e){
  const providers = e?.providers || {};
  const cards = Object.entries(providers).map(([slug, p]) => {
    const c = p.candidates || {}, d = p.due || {}, a = p.attempts || {}, cur = p.cursor || {}, eta = d.selection_eta || {}, complete = p.availability_completion_eta || {};
    return card(PROVIDER_NAMES[slug] || slug, kv({status: p.status, reasons: p.reason_codes, candidates: c.ranked_total, metadata_coverage: c.metadata_coverage_ratio, due: d.total, due_by_class: d.by_class, selected_now: d.current_limit_selection_count, selection_eta_seconds: eta.eta_seconds, selection_eta_reason: eta.reason_code, cursor_mal_id: cur.cursor_mal_anime_id, cursor_generation: cur.cursor_generation, attempted_last_hour: a.distinct_candidates_attempted_last_hour, recent_outcomes: a.recent_24h_outcome_counts, availability_completion_eta: complete.reason_code}));
  }).join('');
  const body = cards ? `<div class="debug-grid">${cards}</div>` : `<p class="muted">${esc(e?.reason_codes?.join(', ') || 'No configured credentialed provider enrichment lanes detected.')}</p>`;
  return `<section><h2 class="debug-section-title">Provider enrichment health</h2>${body}</section>`;
}
const etaText = (hours, reasons = []) => hours == null ? `unknown${reasons.length ? ` (${reasons.join(', ').replace(/_/g, ' ')})` : ''}` : (hours === 0 ? 'ready now' : `~${hours}h`);
function publicUserrecsSection(p){
  if (!p) return '';
  const c = p.coverage || {}, o = p.open_generations || {}, h = p.hourly_throughput || {}, b = p.backlog || {}, s = p.sustainability || {};
  return `<section><h2 class="debug-section-title">Public MAL userrecs crawl</h2><div class="debug-grid">${card('Crawl coverage', kv({status: p.status, positive_seeds: p.positive_seed_count, complete: c.complete, fresh: c.fresh, stale: c.stale, failed: c.failed, unharvested: c.unharvested, fresh_ratio: c.fresh_ratio}))}${card('Backlog and open crawls', kv({due_sources: b.due_sources, active: o.active, paused: o.paused, ready: o.ready, open_total: o.total, staged_pages: o.staged_pages, staged_edges: o.staged_edges}))}${card('Hourly throughput', kv({pages_fetched_last_hour: h.pages_fetched_last_hour, last_page: h.last_page_fetched_at, sources_started: h.sources_started_last_hour, sources_published: h.sources_published_last_hour}))}${card('Source-start ETA', kv({eta: etaText(b.conservative_source_start_eta_hours), authorized_per_hour: s.authorized_source_titles_per_hour, configured_per_hour: s.configured_source_titles_per_hour, within_authorized_rate: s.within_authorized_rate}))}${card('Completion ETA', kv({eta: etaText(b.completion_eta_hours, b.completion_eta_reason_codes || ['unknown_pages_per_source'])}))}</div></section>`;
}
const statusChip = status => { const text = String(status || 'unknown'); const level = text === 'completed' ? 'good' : /fail|error/.test(text) ? 'bad' : 'warn'; return `<span class="status-chip ${level}">${esc(text.replace(/_/g, ' '))}</span>`; };
function syncTable(runs){
  if (!runs?.length) return '<p class="muted">No sync runs recorded.</p>';
  return `<div class="table-scroll" tabindex="0" role="region" aria-label="Recent provider sync runs table"><table class="data-table"><thead><tr><th scope="col">ID</th><th scope="col">Provider</th><th scope="col">Mode</th><th scope="col">Status</th><th scope="col">Started</th><th scope="col">Completed</th></tr></thead><tbody>${runs.map(r => `<tr><td>${esc(r.id)}</td><td>${esc(PROVIDER_NAMES[r.provider] || r.provider)}</td><td>${esc(r.mode)}</td><td>${statusChip(r.status)}</td><td>${timeTag(r.started_at)}</td><td>${timeTag(r.completed_at)}</td></tr>`).join('')}</tbody></table></div>`;
}
function pageSignature(data){ const {generated_at, ...rest} = data; return JSON.stringify(rest); }
function renderPage(data){
  const state = data.recommendations?.coverage_state || {};
  const indicators = (data.indicators || []).map(i => `<li class="${i.level === 'error' ? 'bad' : i.level === 'info' ? 'muted' : 'warn'}">${esc(i.message)}</li>`).join('') || '<li class="muted">No indicators.</li>';
  const snapshot = data.snapshot || {};
  const overview = [
    card('Snapshot', kv({run: snapshot.run_id || 'none', generated: snapshot.generated_at, items: snapshot.item_count || 0}), 'h2'),
    card('Strict coverage', kv({watchable_now: state.strict_actionable_count, ranked_unverified_discovery: state.dormant_candidate_count, pending_review: state.evidence_pending_review_count, stale_or_expired: state.stale_evidence_count}), 'h2'),
    card('MAL harvest coverage', kv(data.coverage?.summary), 'h2'),
    `<section class="card"><h2>Providers</h2>${kv(data.operational?.provider_counts_by_provider)}<h3 class="sub">HIDIVE progress evidence</h3>${kv(data.operational?.hidive_progress_provenance)}</section>`,
    card('Review queue', kv(data.operational?.review_queue), 'h2'),
  ].join('');
  document.getElementById('app').innerHTML = `<section class="card alert"><h2>Indicators</h2><ul class="indicator-list">${indicators}</ul></section><div class="debug-grid">${overview}</div>${providerEnrichmentSection(data.operational?.provider_enrichment)}${publicUserrecsSection(data.coverage?.public_userrecs)}<section><h2 class="debug-section-title">Recent provider sync runs</h2>${syncTable(data.recent_sync_runs)}</section><p class="page-foot">Raw data: <a href="/api/dashboard">dashboard JSON</a></p>`;
}
"""


def _render_dynamic_dashboard_page(
    *,
    page: str,
    title: str,
    settings_href: str | None,
    dashboard_href: str = "/",
    debug_href: str = "/debug",
) -> str:
    is_dashboard = page == "dashboard"
    export_control = (
        '<button type="button" id="export-pdf" class="button primary export-pdf-button" '
        'aria-label="Export dashboard as PDF using the browser print dialog" '
        'title="Opens the print dialog. Choose Save as PDF as the destination to keep links clickable; system PDF printers such as Microsoft Print to PDF drop them.">Export PDF</button>'
        if is_dashboard
        else ""
    )
    header = top_bar(
        "dashboard" if is_dashboard else "debug",
        dashboard_href=dashboard_href,
        debug_href=debug_href,
        settings_href=settings_href,
        actions=export_control,
    )
    print_metadata = (
        '<p class="print-report-meta">Dashboard report · Exported <time id="export-timestamp">when printed</time><br>'
        'View: <span id="export-filter-summary">All genres · Hidden titles excluded</span><br>'
        'Titles, covers and seed titles link to MyAnimeList; provider badges link to Crunchyroll or HIDIVE.</p>'
        if is_dashboard
        else ""
    )
    page_css = _PAGE_CSS if is_dashboard else _DEBUG_CSS
    page_js = _DASHBOARD_JS if is_dashboard else _DEBUG_JS
    config_js = (
        f"const DATA_URL = '/api/dashboard?view=ui';\nconst DEBUG_HREF = {json.dumps(debug_href)};\n"
        f"const SETTINGS_HREF = {json.dumps(settings_href or '')};\n"
    )
    return (
        '<!doctype html>\n<html lang="en"><head><meta charset="utf-8">'
        '<meta name="viewport" content="width=device-width, initial-scale=1"><meta name="color-scheme" content="dark">'
        f"{FAVICON_LINK}<title>{escape(title)}</title><style>{SHELL_CSS}{_COMMON_PAGE_CSS}{page_css}</style></head><body>"
        f"{header}<main class=\"page\" id=\"main\"><div class=\"page-head\"><h1>{escape(title)}</h1>"
        '<p class="page-status" id="page-status">Loading…</p></div>'
        f'{print_metadata}<div id="app" aria-busy="true"><p class="muted">Loading…</p></div></main>'
        f"<script>\n{config_js}{_COMMON_JS}{page_js}{_BOOT_JS}</script></body></html>"
    )


def render_dynamic_dashboard_html(*, title: str = "MAL-Updater live dashboard", settings_href: str | None = None) -> str:
    return _render_dynamic_dashboard_page(page="dashboard", title=title, settings_href=settings_href)


def render_dynamic_debug_html(*, title: str = "MAL-Updater debug", settings_href: str | None = None) -> str:
    return _render_dynamic_dashboard_page(page="debug", title=title, settings_href=settings_href)


def make_dashboard_handler(db_path: Path, *, limit: int = DASHBOARD_DEFAULT_RECOMMENDATION_LIMIT, stale_after_days: int = 14, settings_href: str | None = None, config: AppConfig | None = None) -> type[BaseHTTPRequestHandler]:
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

        def _send_favicon(self) -> None:
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", "image/svg+xml")
            self.send_header("Cache-Control", "public, max-age=86400")
            self.send_header("X-Content-Type-Options", "nosniff")
            self.send_header("Content-Length", str(len(DASHBOARD_FAVICON_SVG)))
            self.end_headers()
            self.wfile.write(DASHBOARD_FAVICON_SVG)

        def do_GET(self) -> None:  # noqa: N802
            parsed = urlparse(self.path)
            query = parse_qs(parsed.query)
            request_limit = normalize_dashboard_limit(query.get("limit", [None])[0], default=default_limit)
            if parsed.path in ("/", "/dashboard"):
                self._send_html(render_dynamic_dashboard_html(settings_href=settings_href))
                return
            if parsed.path == "/debug":
                self._send_html(render_dynamic_debug_html(settings_href=settings_href))
                return
            if parsed.path == "/favicon.svg":
                self._send_favicon()
                return
            if parsed.path == "/api/dashboard":
                payload = build_dashboard_payload(db_path, limit=request_limit, stale_after_days=stale_after_days, config=config)
                if query.get("view", [""])[0] == "ui":
                    payload = dashboard_ui_payload(payload)
                self._send_json(payload)
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
