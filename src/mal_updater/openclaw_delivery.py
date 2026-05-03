from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from .config import AppConfig, load_openclaw_recommendations_hook_token
from .recommendations import build_recommendations, group_recommendations, trim_grouped_recommendations


class OpenClawDeliveryError(RuntimeError):
    """Raised when recommendation delivery to OpenClaw cannot proceed safely."""


@dataclass(slots=True)
class OpenClawRecommendationDeliveryResult:
    status: str
    request_url: str | None
    payload: dict[str, object]
    http_status: int | None = None
    response_text: str | None = None
    reason: str | None = None
    token_path: str | None = None
    request_id: str | None = None

    def as_dict(self) -> dict[str, object]:
        return {
            "status": self.status,
            "request_url": self.request_url,
            "payload": self.payload,
            "http_status": self.http_status,
            "response_text": self.response_text,
            "reason": self.reason,
            "token_path": self.token_path,
            "request_id": self.request_id,
        }


_DELIVERY_MODE_SECTION_KEYS = {
    "fresh": {"continue_next", "fresh_dubbed_episodes"},
    "digest": {"discovery_candidates", "resume_backlog"},
    "all": None,
}

_SECTION_DELIVERY_TIERS = {
    "continue_next": "fresh",
    "fresh_dubbed_episodes": "fresh",
    "discovery_candidates": "digest",
    "resume_backlog": "digest",
    "other": "digest",
}


def _utcnow_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _normalize_delivery_mode(value: str | None) -> str:
    normalized = str(value or "").strip().lower()
    if normalized in _DELIVERY_MODE_SECTION_KEYS:
        return normalized
    return "fresh"


def _section_delivery_tier(section_key: str) -> str:
    return _SECTION_DELIVERY_TIERS.get(section_key, "digest")


def _include_section_for_delivery(section_key: str, *, delivery_mode: str) -> bool:
    allowed = _DELIVERY_MODE_SECTION_KEYS.get(delivery_mode)
    if allowed is None:
        return True
    return section_key in allowed


def _trimmed_delivery_sections(config: AppConfig, sections: list[dict[str, object]], *, delivery_mode: str) -> list[dict[str, object]]:
    limits = config.openclaw.recommendations_webhook_section_limits
    trimmed_sections: list[dict[str, object]] = []
    for section in sections:
        section_key = str(section.get("key") or "other")
        if not _include_section_for_delivery(section_key, delivery_mode=delivery_mode):
            continue
        items = [item for item in (section.get("items") or []) if isinstance(item, dict)]
        limit = max(0, int(limits.get(section_key, limits.get("other", len(items)))))
        if limit == 0:
            continue
        visible_items = items[:limit]
        if not visible_items:
            continue
        trimmed_section = dict(section)
        trimmed_section["items"] = visible_items
        trimmed_section["count"] = len(visible_items)
        trimmed_section["total_count"] = len(items)
        trimmed_section["truncated"] = len(visible_items) < len(items)
        trimmed_section["delivery_tier"] = _section_delivery_tier(section_key)
        trimmed_section["delivery_limit"] = limit
        trimmed_sections.append(trimmed_section)
    return trimmed_sections


def recommendation_delivery_item_fingerprints(payload: dict[str, object]) -> list[str]:
    sections = payload.get("sections") if isinstance(payload.get("sections"), list) else []
    fingerprints: list[str] = []
    for section in sections:
        if not isinstance(section, dict):
            continue
        section_key = str(section.get("key") or "other")
        for item in section.get("items") or []:
            if not isinstance(item, dict):
                continue
            basis = json.dumps(
                {
                    "section": section_key,
                    "kind": item.get("kind"),
                    "provider_series_id": item.get("provider_series_id"),
                    "title": item.get("title"),
                    "season_title": item.get("season_title"),
                    "provider": item.get("provider"),
                },
                sort_keys=True,
                separators=(",", ":"),
            )
            fingerprints.append(hashlib.sha256(basis.encode("utf-8")).hexdigest()[:24])
    return sorted(set(fingerprints))


def build_recommendation_delivery_payload(
    config: AppConfig,
    *,
    limit: int | None,
    include_dormant: bool,
    delivery_mode: str | None = None,
) -> dict[str, object]:
    normalized_mode = _normalize_delivery_mode(delivery_mode or config.openclaw.recommendations_webhook_delivery_mode)
    items = build_recommendations(
        config,
        limit=0,
        require_provider_availability=not include_dormant,
    )
    grouped_sections = trim_grouped_recommendations(group_recommendations(items), limit)
    delivery_sections = _trimmed_delivery_sections(config, grouped_sections, delivery_mode=normalized_mode)
    item_count = sum(int(section.get("count", 0)) for section in delivery_sections if isinstance(section, dict))
    fresh_item_count = sum(
        int(section.get("count", 0))
        for section in delivery_sections
        if isinstance(section, dict) and str(section.get("delivery_tier") or "") == "fresh"
    )
    digest_item_count = max(0, item_count - fresh_item_count)
    fingerprints = recommendation_delivery_item_fingerprints({"sections": delivery_sections})
    return {
        "event": "mal_updater.recommendations",
        "generated_at": _utcnow_iso(),
        "include_dormant": include_dormant,
        "limit": limit,
        "delivery_mode": normalized_mode,
        "section_count": len(delivery_sections),
        "item_count": item_count,
        "fresh_item_count": fresh_item_count,
        "digest_item_count": digest_item_count,
        "interruptive": fresh_item_count > 0,
        "section_limits": dict(config.openclaw.recommendations_webhook_section_limits),
        "item_fingerprints": fingerprints,
        "sections": delivery_sections,
    }


def _build_openclaw_hook_request_payload(config: AppConfig, payload: dict[str, object]) -> dict[str, object]:
    delivery_mode = str(payload.get("delivery_mode") or "fresh")
    if delivery_mode == "fresh":
        headline = "Focus on fresh/new watch-now availability."
    elif delivery_mode == "digest":
        headline = "Treat this as a low-noise digest, not an interruptive alert."
    else:
        headline = "Blend fresh items first, then brief digest material if present."
    return {
        "message": (
            "MAL-Updater recommendation webhook event.\n"
            "Create a concise Discord update for Schwi using the structured payload below as the source of truth.\n"
            f"Delivery posture: {headline}\n"
            "Rules:\n"
            "- mention only non-empty sections\n"
            "- treat `delivery_tier=fresh` sections as watch-now / higher-urgency items\n"
            "- treat `delivery_tier=digest` sections as quiet backlog/discovery material\n"
            "- prefer short bullets grouped by section title\n"
            "- keep the update concise but useful\n"
            "- use provider labels when helpful\n"
            "- respect each section's visible item count; do not re-expand truncated sections\n"
            "- do not invent titles, reasons, or availability\n"
            "Structured payload:\n"
            f"{json.dumps(payload, indent=2)}"
        ),
        "deliver": True,
        "channel": config.openclaw.recommendations_webhook_channel,
        "to": config.openclaw.recommendations_webhook_to,
        "thinking": "off",
        "timeoutSeconds": 45,
    }


def deliver_recommendations_via_openclaw(
    config: AppConfig,
    *,
    limit: int | None,
    include_dormant: bool,
    delivery_mode: str | None = None,
    dry_run: bool = False,
) -> OpenClawRecommendationDeliveryResult:
    payload = build_recommendation_delivery_payload(
        config,
        limit=limit,
        include_dormant=include_dormant,
        delivery_mode=delivery_mode,
    )
    token, token_path = load_openclaw_recommendations_hook_token(config)
    request_url = (config.openclaw.recommendations_webhook_url or "").strip()

    if payload.get("item_count", 0) == 0:
        return OpenClawRecommendationDeliveryResult(
            status="no_recommendations",
            request_url=request_url or None,
            payload=payload,
            reason="no_recommendations",
            token_path=str(token_path),
        )

    if not config.openclaw.recommendations_webhook_enabled:
        raise OpenClawDeliveryError("OpenClaw recommendation webhook delivery is disabled")
    if not request_url:
        raise OpenClawDeliveryError("OpenClaw recommendation webhook URL is not configured")
    if not token:
        raise OpenClawDeliveryError(f"OpenClaw hook token is missing ({token_path})")
    if not (config.openclaw.recommendations_webhook_to or "").strip():
        raise OpenClawDeliveryError("OpenClaw recommendation webhook target is not configured")

    stable_id_basis = json.dumps(
        {
            "event": payload.get("event"),
            "include_dormant": payload.get("include_dormant"),
            "limit": payload.get("limit"),
            "delivery_mode": payload.get("delivery_mode"),
            "sections": payload.get("sections"),
        },
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    request_id = hashlib.sha256(stable_id_basis).hexdigest()[:32]
    hook_request_payload = _build_openclaw_hook_request_payload(config, payload)
    result_payload = {
        "structured_payload": payload,
        "hook_request": hook_request_payload,
    }

    if dry_run:
        return OpenClawRecommendationDeliveryResult(
            status="dry_run",
            request_url=request_url,
            payload=result_payload,
            token_path=str(token_path),
            request_id=request_id,
        )

    body = json.dumps(hook_request_payload).encode("utf-8")
    request = Request(
        request_url,
        data=body,
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {token}",
            "x-openclaw-idempotency-key": request_id,
        },
        method="POST",
    )
    timeout_seconds = max(1.0, float(config.openclaw.recommendations_webhook_timeout_seconds))
    try:
        with urlopen(request, timeout=timeout_seconds) as response:
            response_text = response.read().decode("utf-8", errors="replace")
            status_code = getattr(response, "status", None) or response.getcode()
    except HTTPError as exc:
        error_text = exc.read().decode("utf-8", errors="replace") if exc.fp is not None else str(exc)
        return OpenClawRecommendationDeliveryResult(
            status="http_error",
            request_url=request_url,
            payload=result_payload,
            http_status=exc.code,
            response_text=error_text,
            reason=str(exc),
            token_path=str(token_path),
            request_id=request_id,
        )
    except URLError as exc:
        raise OpenClawDeliveryError(f"OpenClaw recommendation webhook request failed: {exc}") from exc

    return OpenClawRecommendationDeliveryResult(
        status="delivered",
        request_url=request_url,
        payload=result_payload,
        http_status=int(status_code) if status_code is not None else None,
        response_text=response_text,
        token_path=str(token_path),
        request_id=request_id,
    )
