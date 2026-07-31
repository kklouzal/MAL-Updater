from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any
from urllib.parse import quote, urlencode

try:
    from curl_cffi import requests as curl_requests
except ModuleNotFoundError:  # pragma: no cover - dependency health is checked elsewhere
    curl_requests = None

from ..config import AppConfig
from ..contracts import ProviderSnapshot
from ..hidive_urls import canonical_hidive_series_url
from ..hidive_snapshot import fetch_snapshot as fetch_hidive_snapshot
from ..hidive_snapshot import write_snapshot_file as write_hidive_snapshot_file
from ..provider_registry import register_provider
from ..provider_types import ProviderCapabilities, ProviderFetchResult, ProviderSearchResult
from ..request_tracking import record_api_request_event
from ..provider_niceness import ProviderRequestGate, response_retry_after, retry_delay_seconds

HIDIVE_ALGOLIA_APP_ID = "H99XLDR8MJ"
HIDIVE_ALGOLIA_API_KEY = "e55ccb3db0399eabe2bfc37a0314c346"  # public frontend search-only key; never log
HIDIVE_ALGOLIA_REALM = "dce.hidive"
HIDIVE_ALGOLIA_INDEX = f"prod-{HIDIVE_ALGOLIA_REALM}-livestreaming-events"
HIDIVE_ALGOLIA_ENDPOINT = f"https://{HIDIVE_ALGOLIA_APP_ID}-dsn.algolia.net/1/indexes/{quote(HIDIVE_ALGOLIA_INDEX, safe='')}/query"
HIDIVE_ALGOLIA_SERIES_FILTER = "type:VOD_SERIES"


def _reject_unsupported_snapshot_page_controls(
    *,
    max_history_pages: int | None,
    max_watchlist_pages: int | None,
    history_start_page: int,
    watchlist_start: int,
) -> None:
    requested: list[str] = []
    if max_history_pages is not None:
        requested.append("max_history_pages")
    if max_watchlist_pages is not None:
        requested.append("max_watchlist_pages")
    if history_start_page != 1:
        requested.append("history_start_page")
    if watchlist_start != 0:
        requested.append("watchlist_start")
    if requested:
        raise ValueError("HIDIVE snapshot fetch does not support page chunk controls: " + ", ".join(requested))


class HidiveProvider:
    slug = "hidive"
    display_name = "HIDIVE"
    capabilities = ProviderCapabilities(
        history=True,
        continue_watching=True,
        watchlists=True,
        favourites=True,
        rich_progress=True,
        token_refresh=True,
        incremental_boundaries=True,
    )

    def fetch_snapshot(
        self,
        config: AppConfig,
        *,
        profile: str = "default",
        full_refresh: bool = False,
        max_history_pages: int | None = None,
        max_watchlist_pages: int | None = None,
        history_start_page: int = 1,
        watchlist_start: int = 0,
    ) -> ProviderFetchResult:
        _reject_unsupported_snapshot_page_controls(
            max_history_pages=max_history_pages,
            max_watchlist_pages=max_watchlist_pages,
            history_start_page=history_start_page,
            watchlist_start=watchlist_start,
        )
        result = fetch_hidive_snapshot(
            config,
            profile=profile,
            use_incremental_boundary=not full_refresh,
        )
        return ProviderFetchResult(
            snapshot=result.snapshot,
            metadata={
                "provider": self.slug,
                "history_count": result.history_count,
                "continue_count": result.continue_count,
                "favourite_count": result.favourite_count,
                "custom_watchlist_count": getattr(result, "custom_watchlist_count", 0),
                "full_refresh_requested": full_refresh,
            },
        )

    def write_snapshot_file(self, path: Path, snapshot: ProviderSnapshot) -> Path:
        return write_hidive_snapshot_file(path, snapshot)

    def search_title(self, config: AppConfig, query: str, *, limit: int = 10):
        return _search_title(config, query, limit=limit)

    def fetch_search_result_detail(self, config: AppConfig, match):
        # HIDIVE Algolia VOD_SERIES hits already carry the explicit Audio|... tags
        # used as conservative catalog/dub evidence; no broader detail crawl here.
        return match


provider = HidiveProvider()
register_provider(provider)


def _localized_title(hit: dict) -> str | None:
    localisations = hit.get("localisations")
    if isinstance(localisations, dict):
        for key in ("en_US", "en-US", "en", "en_us"):
            value = localisations.get(key)
            if isinstance(value, dict):
                title = value.get("title") or value.get("name")
                if title:
                    return str(title)
    title = hit.get("title") or hit.get("name")
    return str(title) if title else None


def _normalize_hidive_audio_tag_language(language: str) -> str | None:
    normalized = " ".join(language.strip().replace("_", "-").split()).lower()
    if normalized in {"english", "en", "en-us", "en-gb"}:
        return "en-US"
    if normalized in {"japanese", "ja", "ja-jp"}:
        return "ja-JP"
    return None


def _audio_locales_from_hidive_tags(tags: Any) -> list[str]:
    if not isinstance(tags, list):
        return []
    locales: list[str] = []
    seen: set[str] = set()
    for tag in tags:
        if not isinstance(tag, str):
            continue
        prefix, sep, language = tag.partition("|")
        if not sep or prefix.strip().casefold() != "audio":
            continue
        locale = _normalize_hidive_audio_tag_language(language)
        if locale and locale not in seen:
            seen.add(locale)
            locales.append(locale)
    return locales


def _series_from_algolia_hit(hit: dict) -> ProviderSearchResult | None:
    if not isinstance(hit, dict) or hit.get("type") != "VOD_SERIES":
        return None
    provider_series_id = hit.get("id") or str(hit.get("objectID") or "").removeprefix("VOD_SERIES_")
    title = _localized_title(hit)
    if not provider_series_id or not title:
        return None
    # HIDIVE frontend routes generic VOD_SERIES/title hits through /series/{id};
    # /season/{id-or-slug} is for VOD_SEASON and leaves title-search/dashboard
    # links stranded on the web app.
    url = canonical_hidive_series_url(provider_series_id)
    return ProviderSearchResult(
        provider_series_id=str(provider_series_id),
        title=title,
        season_title=title,
        url=str(url) if url else None,
        audio_locales=_audio_locales_from_hidive_tags(hit.get("tags")),
        raw={**hit, "catalog_status": "present", "catalog_evidence_source": "hidive_algolia_vod_series"},
    )


def _normalize_algolia_hits(payload: dict, *, limit: int) -> list[ProviderSearchResult]:
    hits = payload.get("hits") if isinstance(payload, dict) else None
    if not isinstance(hits, list):
        raise ValueError("Unexpected HIDIVE Algolia search response shape")
    results: list[ProviderSearchResult] = []
    seen_ids: set[str] = set()
    for hit in hits:
        summary = _series_from_algolia_hit(hit)
        if summary is None or summary.provider_series_id in seen_ids:
            continue
        seen_ids.add(summary.provider_series_id)
        results.append(summary)
        if len(results) >= limit:
            break
    return results


def _search_title(config, query: str, *, limit: int = 10):
    """Bounded read-only HIDIVE title search via frontend Algolia.

    The HIDIVE web frontend searches Algolia index
    ``prod-dce.hidive-livestreaming-events``. For safe reverse mapping, this
    adapter requests only ``VOD_SERIES`` hits and intentionally ignores episode
    (``VOD_VIDEO``) results instead of attempting risky episode-to-series grouping.
    """
    normalized_limit = max(1, min(int(limit or 10), 10))
    q = str(query or "").strip()
    if not q:
        return []
    if curl_requests is None:
        raise RuntimeError("HIDIVE title search requires curl_cffi browser-TLS transport; install project dependencies with `python3 -m pip install -e .`.")
    timeout = float(getattr(config, "request_timeout_seconds", 30.0) or 30.0)
    params = urlencode({"query": q, "hitsPerPage": normalized_limit, "filters": HIDIVE_ALGOLIA_SERIES_FILTER})
    body = {"params": params}
    niceness = getattr(config, "hidive", None)
    state_dir = Path(getattr(config, "state_dir", Path(getattr(config, "api_request_events_path", Path.cwd())).parent))
    gate = ProviderRequestGate(
        provider="hidive",
        state_dir=state_dir,
        spacing_seconds=float(getattr(niceness, "request_spacing_seconds", 0.0) or 0.0),
        jitter_seconds=float(getattr(niceness, "request_spacing_jitter_seconds", 0.0) or 0.0),
    )
    attempts = max(1, int(getattr(niceness, "retry_max_attempts", 2) or 2))
    response = None
    for attempt in range(1, attempts + 1):
        gate.wait_turn()
        try:
            response = curl_requests.post(
                HIDIVE_ALGOLIA_ENDPOINT,
                data=json.dumps(body),
                headers={
                    "x-algolia-application-id": HIDIVE_ALGOLIA_APP_ID,
                    "x-algolia-api-key": HIDIVE_ALGOLIA_API_KEY,
                    "content-type": "application/json",
                    "accept": "application/json",
                    "user-agent": "MAL-Updater/1.0 HIDIVE title search",
                },
                timeout=(timeout, timeout),
                impersonate="chrome124",
            )
        except Exception as exc:
            record_api_request_event("hidive", "algolia_search", url=HIDIVE_ALGOLIA_ENDPOINT, method="POST", outcome="request_error", error=type(exc).__name__, config=config)
            if attempt < attempts and ("timeout" in type(exc).__name__.lower() or "connection" in type(exc).__name__.lower()):
                delay = retry_delay_seconds(
                    attempt,
                    base_seconds=float(getattr(niceness, "retry_backoff_base_seconds", 1.0) or 0.0),
                    jitter_seconds=float(getattr(niceness, "retry_backoff_jitter_seconds", 0.25) or 0.0),
                    cap_seconds=float(getattr(niceness, "retry_after_cap_seconds", 60.0) or 0.0),
                )
                if delay > 0:
                    time.sleep(delay)
                continue
            raise
        status_code = int(getattr(response, "status_code", 200))
        record_api_request_event(
            "hidive", "algolia_search", url=HIDIVE_ALGOLIA_ENDPOINT, method="POST",
            outcome="ok" if status_code < 400 else "http_error", status_code=status_code, config=config,
        )
        if status_code in {429, 500, 502, 503, 504} and attempt < attempts:
            delay = retry_delay_seconds(
                attempt,
                retry_after=response_retry_after(response),
                base_seconds=float(getattr(niceness, "retry_backoff_base_seconds", 1.0) or 0.0),
                jitter_seconds=float(getattr(niceness, "retry_backoff_jitter_seconds", 0.25) or 0.0),
                cap_seconds=float(getattr(niceness, "retry_after_cap_seconds", 60.0) or 0.0),
            )
            if delay > 0:
                time.sleep(delay)
            continue
        response.raise_for_status()
        break
    assert response is not None
    payload = response.json()
    return _normalize_algolia_hits(payload, limit=normalized_limit)
