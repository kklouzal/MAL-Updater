from __future__ import annotations

from pathlib import Path
from typing import Any

from ..config import AppConfig
from ..contracts import ProviderSnapshot
from ..crunchyroll_snapshot import fetch_snapshot as fetch_crunchyroll_snapshot
from ..crunchyroll_snapshot import write_snapshot_file as write_crunchyroll_snapshot_file
from ..provider_registry import register_provider
from ..provider_types import ProviderCapabilities, ProviderFetchResult, ProviderSearchResult


class CrunchyrollProvider:
    slug = "crunchyroll"
    display_name = "Crunchyroll"
    capabilities = ProviderCapabilities(
        history=True,
        watchlists=True,
        rich_progress=True,
        incremental_boundaries=True,
        token_refresh=True,
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
        result = fetch_crunchyroll_snapshot(
            config,
            profile=profile,
            use_incremental_boundary=not full_refresh,
            max_history_pages=max_history_pages,
            max_watchlist_pages=max_watchlist_pages,
            history_start_page=history_start_page,
            watchlist_start=watchlist_start,
        )
        return ProviderFetchResult(
            snapshot=result.snapshot,
            metadata={
                "provider": self.slug,
                "used_incremental_boundary": not full_refresh,
                "account_email": result.account_email,
                "state_paths": {
                    "root": str(result.state_paths.root),
                    "refresh_token_path": str(result.state_paths.refresh_token_path),
                    "device_id_path": str(result.state_paths.device_id_path),
                    "session_state_path": str(result.state_paths.session_state_path),
                    "sync_boundary_path": str(result.state_paths.sync_boundary_path),
                },
            },
        )

    def write_snapshot_file(self, path: Path, snapshot: ProviderSnapshot) -> Path:
        return write_crunchyroll_snapshot_file(path, snapshot)

    def search_title(self, config: AppConfig, query: str, *, limit: int = 10):
        return _search_title(config, query, limit=limit)

    def fetch_search_result_detail(self, config: AppConfig, match):
        return _fetch_search_result_detail(config, match)


provider = CrunchyrollProvider()
register_provider(provider)


def _normalize_audio_locales(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    normalized: list[str] = []
    seen: set[str] = set()
    for raw_locale in value:
        if not isinstance(raw_locale, str):
            continue
        locale = raw_locale.strip().replace("_", "-")
        if not locale:
            continue
        parts = [part for part in locale.split("-") if part]
        if not parts:
            continue
        language = parts[0].lower()
        if len(parts) == 1:
            canonical = language
        else:
            canonical = "-".join([language, *[part.upper() if len(part) == 2 else part for part in parts[1:]]])
        if canonical not in seen:
            seen.add(canonical)
            normalized.append(canonical)
    return normalized


def _audio_locales_from_crunchyroll_item(item: dict[str, Any]) -> list[str]:
    top_level = _normalize_audio_locales(item.get("audio_locales"))
    if top_level:
        return top_level
    series_metadata = item.get("series_metadata")
    if isinstance(series_metadata, dict):
        return _normalize_audio_locales(series_metadata.get("audio_locales"))
    return []


def _series_id_from_crunchyroll_item(item: dict[str, Any]) -> str | None:
    series_id = item.get("id") or item.get("series_id") or item.get("external_id")
    if series_id is None and isinstance(item.get("series_metadata"), dict):
        series_id = item["series_metadata"].get("series_id") or item["series_metadata"].get("id")
    return str(series_id) if series_id else None


def _crunchyroll_catalog_status(item: dict[str, Any]) -> str:
    status = str(item.get("availability_status") or "").strip().lower()
    metadata = item.get("series_metadata")
    if isinstance(metadata, dict) and not status:
        status = str(metadata.get("availability_status") or "").strip().lower()
    if status in {"available", "available_now"}:
        return "present"
    if status in {"not_available", "unavailable", "expired", "deleted"}:
        return "absent"
    # A series-like object returned by authenticated Crunchyroll search/CMS is a
    # defensible catalog-presence signal even when the availability field is absent.
    return "present"


def _series_from_crunchyroll_item(item):
    if not isinstance(item, dict):
        return None
    item_type = str(item.get("type") or "").lower()
    if item_type and item_type not in {"series", "movie_listing"}:
        return None
    series_id = _series_id_from_crunchyroll_item(item)
    title = item.get("title") or item.get("name")
    if not series_id or not title:
        return None
    slug = item.get("slug_title")
    url = item.get("url") or item.get("link")
    if not url and slug:
        url = f"https://www.crunchyroll.com/series/{series_id}/{slug}"
    return {
        "provider_series_id": str(series_id),
        "title": str(title),
        "season_title": item.get("season_title") or item.get("title"),
        "url": url,
        "audio_locales": _audio_locales_from_crunchyroll_item(item),
        "catalog_status": _crunchyroll_catalog_status(item),
        "raw": item,
    }


def _search_title(config, query: str, *, limit: int = 10):
    """Bounded read-only Crunchyroll title search.

    Uses Crunchyroll's authenticated content discover search endpoint and returns only
    normalized summary rows for series-like items. This is intentionally a single
    limited query, not a catalog crawl.
    """
    from ..crunchyroll_snapshot import CrunchyrollSnapshotError, _CrunchyrollRequestPacer, _start_auth_session

    normalized_limit = max(1, min(int(limit or 10), 10))
    q = str(query or "").strip()
    if not q:
        return []
    pacer = _CrunchyrollRequestPacer(
        spacing_seconds=max(0.0, float(getattr(config.crunchyroll, "request_spacing_seconds", 0.0) or 0.0)),
        jitter_seconds=max(0.0, float(getattr(config.crunchyroll, "request_jitter_seconds", 0.0) or 0.0)),
    )
    session = _start_auth_session(
        config,
        profile="default",
        timeout_seconds=float(getattr(config.crunchyroll, "request_timeout_seconds", 30.0) or 30.0),
        pacer=pacer,
    )
    endpoint = "https://www.crunchyroll.com/content/v2/discover/search"
    payload = session.authorized_json_get(
        endpoint,
        params={
            "q": q,
            "n": normalized_limit,
            "locale": getattr(config.crunchyroll, "locale", "en-US") or "en-US",
            "type": "series,movie_listing",
        },
        phase="title-search",
    )
    data = payload.get("data") if isinstance(payload, dict) else None
    if not isinstance(data, list):
        raise CrunchyrollSnapshotError("Unexpected Crunchyroll search response shape")
    results = []
    seen_ids: set[str] = set()
    # Crunchyroll returns grouped buckets, not a flat item list. Prefer title-level
    # buckets and ignore episode hits for reverse MAL-series matching.
    for bucket_type in ("top_results", "series", "movie_listing"):
        for bucket in data:
            if not isinstance(bucket, dict) or bucket.get("type") != bucket_type:
                continue
            items = bucket.get("items")
            if not isinstance(items, list):
                continue
            for item in items:
                summary = _series_from_crunchyroll_item(item)
                if summary is None:
                    continue
                provider_series_id = summary.get("provider_series_id")
                if provider_series_id in seen_ids:
                    continue
                seen_ids.add(provider_series_id)
                results.append(summary)
                if len(results) >= normalized_limit:
                    return results
    return results


def _provider_series_id_from_match(match: Any) -> str | None:
    if isinstance(match, dict):
        value = match.get("provider_series_id") or match.get("id") or match.get("series_id") or match.get("external_id")
    else:
        value = getattr(match, "provider_series_id", None) or getattr(match, "id", None)
    return str(value) if value else None


def _merge_search_result_with_detail(match: Any, detail: dict[str, Any]) -> ProviderSearchResult | dict[str, Any]:
    summary = _series_from_crunchyroll_item(detail)
    if summary is None:
        return match
    base = dict(match) if isinstance(match, dict) else {
        "provider_series_id": getattr(match, "provider_series_id", None),
        "title": getattr(match, "title", None),
        "season_title": getattr(match, "season_title", None),
        "url": getattr(match, "url", None),
        "audio_locales": getattr(match, "audio_locales", []),
        "raw": getattr(match, "raw", {}),
    }
    raw = base.get("raw") if isinstance(base.get("raw"), dict) else {}
    original_title = base.get("title")
    original_season_title = base.get("season_title")
    return {
        **base,
        "provider_series_id": summary.get("provider_series_id") or base.get("provider_series_id"),
        "title": original_title or summary.get("title"),
        "season_title": original_season_title or summary.get("season_title") or original_title,
        "url": summary.get("url") or base.get("url"),
        "audio_locales": summary.get("audio_locales") or (base.get("audio_locales") if isinstance(base.get("audio_locales"), list) else []),
        "catalog_status": summary.get("catalog_status") or base.get("catalog_status") or "present",
        "raw": {**raw, "detail": detail},
        "detail_evidence_source": "crunchyroll_cms_series",
    }


def _fetch_search_result_detail(config: AppConfig, match: Any):
    """Fetch a bounded authenticated Crunchyroll CMS series detail payload.

    Crunchyroll discover search sometimes omits audio locales from grouped search
    hits. The CMS series object is still read-only/account-authenticated and
    exposes the explicit ``audio_locales`` field needed for English-dub evidence.
    """
    from ..crunchyroll_snapshot import _CrunchyrollRequestPacer, _start_auth_session

    provider_series_id = _provider_series_id_from_match(match)
    if not provider_series_id:
        return match
    pacer = _CrunchyrollRequestPacer(
        spacing_seconds=max(0.0, float(getattr(config.crunchyroll, "request_spacing_seconds", 0.0) or 0.0)),
        jitter_seconds=max(0.0, float(getattr(config.crunchyroll, "request_jitter_seconds", 0.0) or 0.0)),
    )
    session = _start_auth_session(
        config,
        profile="default",
        timeout_seconds=float(getattr(config.crunchyroll, "request_timeout_seconds", 30.0) or 30.0),
        pacer=pacer,
    )
    payload = session.authorized_json_get(
        f"https://www.crunchyroll.com/content/v2/cms/series/{provider_series_id}",
        params={"locale": getattr(config.crunchyroll, "locale", "en-US") or "en-US"},
        phase="title-detail",
    )
    data = payload.get("data") if isinstance(payload, dict) else None
    detail = data[0] if isinstance(data, list) and data else data if isinstance(data, dict) else None
    if not isinstance(detail, dict):
        return match
    return _merge_search_result_with_detail(match, detail)
