from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from .config import AppConfig, load_openclaw_recommendations_hook_token
from .recommendations import build_recommendations, group_recommendations


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


def _utcnow_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def build_recommendation_delivery_payload(
    config: AppConfig,
    *,
    limit: int | None,
    include_dormant: bool,
) -> dict[str, object]:
    items = build_recommendations(
        config,
        limit=limit,
        require_provider_availability=not include_dormant,
    )
    sections = group_recommendations(items)
    item_count = sum(int(section.get("count", 0)) for section in sections if isinstance(section, dict))
    return {
        "event": "mal_updater.recommendations",
        "generated_at": _utcnow_iso(),
        "include_dormant": include_dormant,
        "limit": limit,
        "section_count": len(sections),
        "item_count": item_count,
        "sections": sections,
    }


def deliver_recommendations_via_openclaw(
    config: AppConfig,
    *,
    limit: int | None,
    include_dormant: bool,
    dry_run: bool = False,
) -> OpenClawRecommendationDeliveryResult:
    payload = build_recommendation_delivery_payload(config, limit=limit, include_dormant=include_dormant)
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
            "sections": payload.get("sections"),
        },
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    request_id = hashlib.sha256(stable_id_basis).hexdigest()[:32]
    hook_request_payload = {
        "message": (
            "MAL-Updater recommendation webhook event.\n"
            "Create a concise Discord update for Schwi using the structured payload below as the source of truth.\n"
            "Rules:\n"
            "- mention only non-empty sections\n"
            "- prefer short bullets grouped by section title\n"
            "- keep the update concise but useful\n"
            "- use provider labels when helpful\n"
            "- highlight only the strongest few items per section unless the section is very small\n"
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

    if dry_run:
        return OpenClawRecommendationDeliveryResult(
            status="dry_run",
            request_url=request_url,
            payload=hook_request_payload,
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
            payload=hook_request_payload,
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
        payload=hook_request_payload,
        http_status=int(status_code) if status_code is not None else None,
        response_text=response_text,
        token_path=str(token_path),
        request_id=request_id,
    )
