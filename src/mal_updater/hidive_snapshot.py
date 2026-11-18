from __future__ import annotations

import json
import hashlib
import math
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import quote

from .config import AppConfig
from .persistence import atomic_write_json
from .contracts import EpisodeProgress, ProviderSnapshot, SeriesRef, WatchlistEntry
from .fetch_provenance import FetchProvenance
from .provider_snapshot import snapshot_to_dict as _snapshot_to_dict
from .provider_snapshot import write_snapshot_file as _write_snapshot_file
from .hidive_auth import HidiveAuthError, HidiveSession, HidiveStatePaths, start_hidive_session
from .season_catalog import (
    ProviderSeasonCatalog,
    catalog_candidate_season_keys,
    season_keys_needing_catalog,
    upsert_provider_season_catalog,
)


HIDIVE_CATALOG_SEASONS_PER_RUN = 10
HIDIVE_CATALOG_MAX_PAGES = 20


def _season_catalog_from_pages(season_key: str, pages: list[dict[str, Any]]) -> ProviderSeasonCatalog:
    """Summarize /api/v4/season/{id} (probed 2026-11-18).

    Episode publishedDate is HIDIVE's own release, never earlier than the
    original broadcast (a 2023 season published 2024-01), so the dates are
    provider_publish, not original_air.
    """
    episodes = [episode for page in pages for episode in (page.get("episodes") or []) if isinstance(episode, dict)]
    numbered = [
        episode
        for episode in episodes
        if isinstance((episode.get("episodeInformation") or {}).get("episodeNumber"), int)
    ]
    dated = numbered or episodes
    dates = sorted(str(episode["publishedDate"])[:10] for episode in dated if episode.get("publishedDate"))
    first = pages[0] if pages else {}
    declared = _safe_int(first.get("episodeCount"))
    return ProviderSeasonCatalog(
        provider="hidive",
        provider_season_key=season_key,
        title=str(first.get("title")) if first.get("title") else None,
        numbered_episode_count=len(numbered),
        total_episode_count=len(episodes),
        first_date=dates[0] if dates else None,
        last_date=dates[-1] if dates else None,
        date_kind="provider_publish",
        complete=declared is None or declared <= len(episodes),
        fetched_at="",
    )


def _refresh_season_catalog(session: HidiveSession) -> dict[str, Any]:
    """Catalog a few seasons per run that season-to-MAL matching may need.

    Only numeric HIDIVE season ids have a catalog route. A failure never fails
    the snapshot: catalog facts only add matching evidence.
    """
    db_path = session.config.db_path
    report: dict[str, Any] = {"requested": 0, "stored": 0, "failed": 0}
    if not db_path.exists():
        return report
    candidates = [key for key in catalog_candidate_season_keys(db_path, provider="hidive") if key.isdigit()]
    pending = season_keys_needing_catalog(db_path, provider="hidive", season_keys=candidates)
    report["pending"] = len(pending)
    for season_key in pending[:HIDIVE_CATALOG_SEASONS_PER_RUN]:
        report["requested"] += 1
        pages: list[dict[str, Any]] = []
        params: dict[str, Any] = {"rpp": HIDIVE_HOME_ITEMS_PER_BUCKET}
        try:
            for _ in range(HIDIVE_CATALOG_MAX_PAGES):
                page = session.json_get(f"/api/v4/season/{quote(season_key, safe='')}", params=params)
                if not isinstance(page, dict):
                    raise HidiveSnapshotError("HIDIVE season payload was not a JSON object")
                pages.append(page)
                paging = page.get("paging") if isinstance(page.get("paging"), dict) else {}
                if not paging.get("moreDataAvailable") or paging.get("lastSeen") is None:
                    break
                params = {"rpp": HIDIVE_HOME_ITEMS_PER_BUCKET, "lastSeen": paging["lastSeen"]}
        except (HidiveAuthError, HidiveSnapshotError) as exc:
            report["failed"] += 1
            report.setdefault("errors", []).append(f"{season_key}: {str(exc)[:160]}")
            continue
        upsert_provider_season_catalog(db_path, _season_catalog_from_pages(season_key, pages))
        report["stored"] += 1
    return report


class HidiveSnapshotError(RuntimeError):
    pass


SYNC_BOUNDARY_SCHEMA_VERSION = 1
HISTORY_BOUNDARY_MARKER_LIMIT = 25
CONTINUE_BOUNDARY_MARKER_LIMIT = 25
FAVOURITE_BOUNDARY_MARKER_LIMIT = 25
INCREMENTAL_BACKFILL_PAGE_LIMIT = 1
HIDIVE_HISTORY_PAGE_LIMIT = 1000
# /customer/history/vod pages with ``rpp`` (results per page) and ``p`` (1-based
# page).  Production probes (2026-11-18): ``size``/``page`` are silently ignored
# (default 5 per page, always page 1); rpp=25 is accepted; rpp=50 and rpp=100
# are rejected with HTTP 400.
HIDIVE_HISTORY_RPP = 25
# Hot mode pages history until the stored front-boundary marker is seen.  If it
# is not reached within this budget the run escalates to a full refresh so the
# boundary is rebuilt instead of staying unreachable forever.
HIDIVE_HOT_HISTORY_CATCHUP_PAGE_LIMIT = 20
HIDIVE_PROGRESS_NORMALIZATION_VERSION = "hidive_progress_v4"
HIDIVE_HOME_BUCKETS_PER_PAGE = 5
HIDIVE_HOME_ITEMS_PER_BUCKET = 25
HIDIVE_FAVOURITE_PAGE_LIMIT = 1000
HIDIVE_CUSTOM_WATCHLIST_COLLECTION_PAGE_LIMIT = 1000
HIDIVE_CUSTOM_WATCHLIST_DETAIL_PAGE_LIMIT = 1000
HIDIVE_CUSTOM_WATCHLIST_RPP = 25


@dataclass(slots=True)
class _SyncBoundary:
    generated_at: str | None
    account_id_hint: str | None
    history_markers: list[str]
    continue_markers: list[str]
    favourite_markers: list[str]
    favourite_backfill_markers: list[str]


@dataclass(slots=True)
class HidiveFetchResult:
    snapshot: ProviderSnapshot
    history_count: int
    continue_count: int
    favourite_count: int
    custom_watchlist_count: int = 0
    _ingestion_authority: object | None = None


def _now_string() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _iso_from_epoch_ms(value: Any) -> str | None:
    if value is None:
        return None
    if not isinstance(value, (int, float)):
        return None
    return datetime.fromtimestamp(float(value) / 1000.0, tz=timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _safe_int(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    try:
        return int(str(value))
    except (TypeError, ValueError):
        return None


def _episode_number_and_label(value: Any) -> tuple[int | None, str | None]:
    """Split a provider episode number into an integral number and display label.

    Non-integral numbers (e.g. 12.5 recaps) keep ``episode_number`` None so they
    cannot collide with the integral episode they would truncate to.
    """
    if isinstance(value, bool) or value is None:
        return None, None
    if isinstance(value, int):
        return value, str(value)
    if isinstance(value, float):
        if not math.isfinite(value):
            return None, None
        if value.is_integer():
            return int(value), str(int(value))
        return None, repr(value)
    text = str(value).strip()
    if not text:
        return None, None
    try:
        return int(text), text
    except ValueError:
        pass
    try:
        numeric = float(text)
    except ValueError:
        return None, text
    if math.isfinite(numeric) and numeric.is_integer():
        return int(numeric), text
    return None, text


def _season_number(value: Any) -> int | None:
    number, _label = _episode_number_and_label(value)
    return number if number is not None and number >= 0 else None


def _season_identity(episode_information: dict[str, Any]) -> tuple[str | None, int | None, str | None]:
    """Return (provider_season_key, season_number, season_title).

    The key is HIDIVE's season id (``episodeInformation.season``) as a string.
    When a payload lacks it, the deterministic fallback is
    ``season-number:<seasonNumber>`` (distinct from any numeric id).  HIDIVE
    carries dubs as audio tracks of one VOD, so no dub collapse is needed.
    """
    season_number = _season_number(episode_information.get("seasonNumber"))
    raw_title = episode_information.get("seasonTitle")
    season_title = raw_title if isinstance(raw_title, str) and raw_title.strip() else None
    season_id = episode_information.get("season")
    key: str | None = None
    if isinstance(season_id, int) and not isinstance(season_id, bool):
        key = str(season_id)
    elif isinstance(season_id, str) and season_id.strip():
        key = season_id.strip()
    elif season_number is not None:
        key = f"season-number:{season_number}"
    return key, season_number, season_title


def _safe_float(value: Any) -> float | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    try:
        return float(str(value))
    except (TypeError, ValueError):
        return None


def _history_item_fingerprint(item: dict[str, Any]) -> str | None:
    episode_information = item.get("episodeInformation") or {}
    series_information = episode_information.get("seriesInformation") or {}
    provider_series_id = series_information.get("id")
    provider_episode_id = item.get("id") or item.get("externalAssetId")
    watched_at = item.get("watchedAt")
    if provider_series_id is None or provider_episode_id is None:
        return None
    return "|".join([str(provider_series_id), str(provider_episode_id), str(watched_at or "")])


def _continue_item_fingerprint(item: dict[str, Any]) -> str | None:
    episode_information = item.get("episodeInformation") or {}
    series_information = episode_information.get("seriesInformation") or {}
    provider_series_id = series_information.get("id")
    provider_episode_id = item.get("id") or item.get("externalAssetId")
    watched_at = item.get("watchedAt")
    watch_progress = item.get("watchProgress")
    if provider_series_id is None or provider_episode_id is None:
        return None
    return "|".join([str(provider_series_id), str(provider_episode_id), str(watched_at or ""), str(watch_progress or "")])


def _favourite_item_fingerprint(item: dict[str, Any]) -> str | None:
    provider_series_id = item.get("id")
    title = item.get("title")
    if provider_series_id is None or title is None:
        return None
    return "|".join([str(provider_series_id), str(title)])


def _stable_json_fingerprint(value: Any) -> str:
    encoded = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _page_fingerprint(items: list[dict[str, Any]], fingerprint_func) -> str:
    markers: list[str] = []
    for item in items:
        marker = fingerprint_func(item)
        markers.append(marker if marker is not None else _stable_json_fingerprint(item))
    return _stable_json_fingerprint(markers)


def _diagnostic(code: str, *, surface: str, detail: str, severity: str = "warning", **extra: Any) -> dict[str, Any]:
    return {
        "code": code,
        "surface": surface,
        "severity": severity,
        "detail": detail,
        **{key: value for key, value in extra.items() if value is not None},
    }


def _paging_info(payload: dict[str, Any]) -> dict[str, Any]:
    paging = payload.get("pagingInfo")
    return paging if isinstance(paging, dict) else {}


def _paging_more(payload: dict[str, Any]) -> bool:
    return _paging_info(payload).get("moreDataAvailable") is True


def _paging_last_seen(payload: dict[str, Any]) -> tuple[bool, Any]:
    paging = _paging_info(payload)
    if "lastSeen" not in paging:
        return False, None
    return True, paging.get("lastSeen")


def _load_sync_boundary(state_paths: HidiveStatePaths) -> _SyncBoundary | None:
    path = state_paths.sync_boundary_path
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError, TypeError):
        return None
    if not isinstance(payload, dict):
        return None
    if int(payload.get("schema_version") or 0) != SYNC_BOUNDARY_SCHEMA_VERSION:
        return None
    history = payload.get("history") if isinstance(payload.get("history"), dict) else {}
    continue_watching = payload.get("continue_watching") if isinstance(payload.get("continue_watching"), dict) else {}
    favourites = payload.get("favourites") if isinstance(payload.get("favourites"), dict) else {}
    return _SyncBoundary(
        generated_at=str(payload.get("generated_at")) if payload.get("generated_at") else None,
        account_id_hint=str(payload.get("account_id_hint")) if payload.get("account_id_hint") else None,
        history_markers=[str(item) for item in history.get("first_seen", []) if item],
        continue_markers=[str(item) for item in continue_watching.get("first_seen", []) if item],
        favourite_markers=[str(item) for item in favourites.get("first_seen", []) if item],
        favourite_backfill_markers=[str(item) for item in favourites.get("backfill_seen", []) if item],
    )


def _unique_fingerprints(entries: list[dict[str, Any]], fingerprint_func, limit: int) -> list[str]:
    markers: list[str] = []
    for item in entries:
        fp = fingerprint_func(item)
        if fp and fp not in markers:
            markers.append(fp)
        if len(markers) >= limit:
            break
    return markers


def _write_sync_boundary(
    *,
    state_paths: HidiveStatePaths,
    generated_at: str,
    account_id_hint: str | None,
    history_items: list[dict[str, Any]],
    continue_items: list[dict[str, Any]],
    favourite_items: list[dict[str, Any]],
    favourite_backfill_items: list[dict[str, Any]] | None = None,
    favourite_markers_override: list[str] | None = None,
    favourite_backfill_markers_override: list[str] | None = None,
) -> None:
    state_paths.root.mkdir(parents=True, exist_ok=True)
    history_markers = _unique_fingerprints(history_items, _history_item_fingerprint, HISTORY_BOUNDARY_MARKER_LIMIT)
    continue_markers = _unique_fingerprints(continue_items, _continue_item_fingerprint, CONTINUE_BOUNDARY_MARKER_LIMIT)
    favourite_markers = (
        list(favourite_markers_override[:FAVOURITE_BOUNDARY_MARKER_LIMIT])
        if favourite_markers_override is not None
        else _unique_fingerprints(favourite_items, _favourite_item_fingerprint, FAVOURITE_BOUNDARY_MARKER_LIMIT)
    )
    favourite_backfill_markers = (
        list(favourite_backfill_markers_override[:FAVOURITE_BOUNDARY_MARKER_LIMIT])
        if favourite_backfill_markers_override is not None
        else _unique_fingerprints(favourite_backfill_items or [], _favourite_item_fingerprint, FAVOURITE_BOUNDARY_MARKER_LIMIT)
    )
    payload = {
        "schema_version": SYNC_BOUNDARY_SCHEMA_VERSION,
        "generated_at": generated_at,
        "account_id_hint": account_id_hint,
        "history": {"marker_limit": HISTORY_BOUNDARY_MARKER_LIMIT, "retained_count": len(history_markers), "first_seen": history_markers},
        "continue_watching": {"marker_limit": CONTINUE_BOUNDARY_MARKER_LIMIT, "retained_count": len(continue_markers), "first_seen": continue_markers},
        "favourites": {"marker_limit": FAVOURITE_BOUNDARY_MARKER_LIMIT, "retained_count": len(favourite_markers), "first_seen": favourite_markers, "backfill_seen": favourite_backfill_markers, "backfill_retained_count": len(favourite_backfill_markers)},
    }
    atomic_write_json(state_paths.sync_boundary_path, payload, indent=2)


def _normalize_hidive_watch_progress(raw_progress: Any, duration_seconds: int | None) -> tuple[int | None, float | None]:
    """Return (playback_seconds, completion_ratio) from Continue Watching.

    Production ``watchProgress`` values are integer seconds (e.g. 1137 of a
    1488 s episode, or 16 s), so integral values are always seconds; only a
    non-integral value below 1 is read as a ratio.  A 1-second position must
    never read as "100% watched".
    """
    numeric = _safe_float(raw_progress)
    if numeric is None or not math.isfinite(numeric) or numeric < 0:
        return None, None
    known_duration = duration_seconds if duration_seconds is not None and duration_seconds > 0 else None
    if not numeric.is_integer() and numeric < 1.0:
        playback_seconds = int(known_duration * numeric) if known_duration is not None else None
        return playback_seconds, numeric
    playback_seconds = int(numeric)
    if known_duration is None:
        return playback_seconds, None
    return playback_seconds, min(1.0, playback_seconds / known_duration)


def _extract_series_ref_from_episode_info(item: dict[str, Any]) -> SeriesRef | None:
    episode_information = item.get("episodeInformation") or {}
    series_information = episode_information.get("seriesInformation") or {}
    provider_series_id = series_information.get("id")
    title = series_information.get("title")
    if provider_series_id is None or not title:
        return None
    return SeriesRef(
        provider_series_id=str(provider_series_id),
        title=str(title),
        season_title=(str(episode_information.get("seasonTitle")) if episode_information.get("seasonTitle") is not None else None),
        season_number=_season_number(episode_information.get("seasonNumber")),
    )


def _history_item_to_progress(item: dict[str, Any]) -> EpisodeProgress | None:
    episode_information = item.get("episodeInformation") or {}
    series_information = episode_information.get("seriesInformation") or {}
    provider_episode_id = item.get("id") or item.get("externalAssetId")
    provider_series_id = series_information.get("id")
    if provider_episode_id is None or provider_series_id is None:
        return None
    duration_seconds = _safe_int(item.get("duration"))
    duration_ms = duration_seconds * 1000 if duration_seconds is not None else None
    episode_number, episode_label = _episode_number_and_label(episode_information.get("episodeNumber"))
    season_key, season_number, season_title = _season_identity(episode_information)
    # HIDIVE keeps a resume point (watchProgress, seconds) only while an
    # episode is unfinished and clears it once the episode is finished
    # (probed 2026-11-18: all 20 of 401 history items carrying progress were
    # partial plays). A history item without progress is a finished episode.
    if item.get("watchProgress") is not None:
        playback_seconds, completion_ratio = _normalize_hidive_watch_progress(item.get("watchProgress"), duration_seconds)
        playback_position_ms = playback_seconds * 1000 if playback_seconds is not None else None
        observation_kind = "position" if playback_position_ms is not None else "history_membership"
        completion_assertion = "confirmed" if completion_ratio is not None and completion_ratio >= 1.0 else "unknown"
    else:
        playback_position_ms = None
        completion_ratio = None
        observation_kind = "explicit_completed"
        completion_assertion = "inferred"
    return EpisodeProgress(
        provider_episode_id=str(provider_episode_id),
        provider_series_id=str(provider_series_id),
        episode_number=episode_number,
        episode_title=str(item.get("title")) if item.get("title") is not None else None,
        playback_position_ms=playback_position_ms,
        duration_ms=duration_ms,
        completion_ratio=completion_ratio,
        last_watched_at=_iso_from_epoch_ms(item.get("watchedAt")),
        audio_locale=None,
        subtitle_locale=None,
        rating=str(item.get("rating")) if item.get("rating") is not None else None,
        progress_source_surface="hidive_history",
        progress_observation_kind=observation_kind,
        completion_assertion=completion_assertion,
        normalization_logic_version=HIDIVE_PROGRESS_NORMALIZATION_VERSION,
        provider_season_key=season_key,
        season_number=season_number,
        season_title=season_title,
        episode_label=episode_label,
        # HIDIVE numbers episodes per season (E1 restarts each season).
        season_episode_ordinal=episode_number,
    )


def _continue_item_to_progress(item: dict[str, Any]) -> EpisodeProgress | None:
    episode_information = item.get("episodeInformation") or {}
    series_ref = _extract_series_ref_from_episode_info(item)
    if series_ref is None:
        return None
    provider_episode_id = item.get("id") or item.get("externalAssetId")
    if provider_episode_id is None:
        return None
    duration_seconds = _safe_int(item.get("duration"))
    duration_ms = duration_seconds * 1000 if duration_seconds is not None else None
    playback_seconds, watch_progress = _normalize_hidive_watch_progress(item.get("watchProgress"), duration_seconds)
    playback_position_ms = playback_seconds * 1000 if playback_seconds is not None else None
    if playback_position_ms is not None:
        observation_kind = "position"
    elif watch_progress is not None:
        observation_kind = "ratio"
    else:
        # Bucket membership without any measurement is no stronger than history
        # membership and must not outrank measured evidence.
        observation_kind = "history_membership"
    episode_number, episode_label = _episode_number_and_label(episode_information.get("episodeNumber"))
    season_key, season_number, season_title = _season_identity(episode_information)
    return EpisodeProgress(
        provider_episode_id=str(provider_episode_id),
        provider_series_id=series_ref.provider_series_id,
        episode_number=episode_number,
        episode_title=str(item.get("title")) if item.get("title") is not None else None,
        playback_position_ms=playback_position_ms,
        duration_ms=duration_ms,
        completion_ratio=watch_progress,
        last_watched_at=_iso_from_epoch_ms(item.get("watchedAt")),
        audio_locale=None,
        subtitle_locale=None,
        rating=str(item.get("rating")) if item.get("rating") is not None else None,
        progress_source_surface="hidive_continue_watching",
        progress_observation_kind=observation_kind,
        completion_assertion="confirmed" if watch_progress is not None and watch_progress >= 1.0 else "unknown",
        normalization_logic_version=HIDIVE_PROGRESS_NORMALIZATION_VERSION,
        provider_season_key=season_key,
        season_number=season_number,
        season_title=season_title,
        episode_label=episode_label,
        # HIDIVE numbers episodes per season (E1 restarts each season).
        season_episode_ordinal=episode_number,
    )


@dataclass(slots=True)
class _HistoryFetch:
    items: list[dict[str, Any]]
    pages_fetched: int
    # True when paging stopped before the declared terminal page for any reason
    # other than reaching the front boundary (guard, cap, non-advancing page).
    stopped_early: bool
    # True when the declared terminal page was read: every item was observed.
    traversal_complete: bool
    front_boundary_seen: bool
    catchup_cap_hit: bool
    next_page: int | None
    diagnostics: list[dict[str, Any]]

    @property
    def front_boundary_complete(self) -> bool:
        # Reading the whole history proves nothing newer was skipped even if the
        # stored markers vanished (e.g. every marker episode was rewatched).
        return self.front_boundary_seen or self.traversal_complete


def _fetch_history_items(
    session: HidiveSession,
    *,
    history_markers: set[str] | None = None,
    max_pages: int | None = None,
    start_page: int = 1,
) -> _HistoryFetch:
    page = start_page
    items: list[dict[str, Any]] = []
    pages_fetched = 0
    stopped_early = False
    traversal_complete = False
    catchup_cap_hit = False
    next_page: int | None = None
    diagnostics: list[dict[str, Any]] = []
    seen_page_fingerprints: dict[str, int] = {}
    front_boundary_seen = not history_markers
    while True:
        if pages_fetched >= HIDIVE_HISTORY_PAGE_LIMIT:
            stopped_early = True
            diagnostics.append(_diagnostic(
                "history_page_guard_hit", surface="history",
                detail=f"HIDIVE history exceeded page guard ({HIDIVE_HISTORY_PAGE_LIMIT})",
                pages_fetched=pages_fetched,
            ))
            break
        payload = session.json_get("/customer/history/vod", params={"rpp": HIDIVE_HISTORY_RPP, "p": page})
        pages_fetched += 1
        if not isinstance(payload, dict):
            raise HidiveSnapshotError("HIDIVE history payload was not a JSON object")
        vods = payload.get("vods")
        if not isinstance(vods, list):
            raise HidiveSnapshotError("HIDIVE history payload did not include a vods list")
        batch = [item for item in vods if isinstance(item, dict)]
        batch_fingerprint = _page_fingerprint(batch, _history_item_fingerprint)
        previous_page = seen_page_fingerprints.get(batch_fingerprint)
        echoed_page = _safe_int(payload.get("page"))
        if (batch and previous_page is not None) or (echoed_page is not None and echoed_page != page):
            # The server echoes the page it served; a mismatch or a repeated
            # page means the paging parameters were ignored.
            stopped_early = True
            diagnostics.append(
                _diagnostic(
                    "history_pagination_non_advancing",
                    surface="history",
                    detail="HIDIVE history served a repeated or different page than requested; stopped to avoid repeated non-advancing requests",
                    previous_page=previous_page,
                    requested_page=page,
                    echoed_page=echoed_page,
                    pages_fetched=pages_fetched,
                    batch_count=len(batch),
                    declared_total_pages=_safe_int(payload.get("totalPages")),
                    declared_total_results=_safe_int(payload.get("totalResults")),
                )
            )
            break
        seen_page_fingerprints[batch_fingerprint] = page
        items.extend(batch)
        if history_markers and not front_boundary_seen:
            batch_markers = {marker for item in batch if (marker := _history_item_fingerprint(item))}
            front_boundary_seen = bool(history_markers.intersection(batch_markers))
        total_pages = _safe_int(payload.get("totalPages"))
        if not batch or (total_pages is not None and page >= total_pages) or (total_pages is None and len(batch) < HIDIVE_HISTORY_RPP):
            traversal_complete = True
            break
        if history_markers and front_boundary_seen:
            break
        if max_pages is not None and pages_fetched >= max_pages:
            stopped_early = True
            catchup_cap_hit = True
            next_page = page + 1
            diagnostics.append(_diagnostic(
                "history_page_cap_hit", surface="history", severity="info",
                detail="HIDIVE history stopped at the page budget before the stored front boundary",
                pages_fetched=pages_fetched,
            ))
            break
        page += 1
    return _HistoryFetch(
        items=items,
        pages_fetched=pages_fetched,
        stopped_early=stopped_early,
        traversal_complete=traversal_complete,
        front_boundary_seen=front_boundary_seen,
        catchup_cap_hit=catchup_cap_hit,
        next_page=next_page,
        diagnostics=diagnostics,
    )


def _fetch_continue_items(session: HidiveSession, *, continue_markers: set[str] | None = None) -> tuple[list[dict[str, Any]], bool, dict[str, Any]]:
    # Production probes (2026-11-18): /content/home pages buckets with bpp
    # (buckets per page) and items per bucket with rpp, which accepts at most
    # 25 (rpp=50 -> HTTP 400); size/page are ignored (5 items). With rpp=25 the
    # whole Continue Watching bucket (19 items) arrives in one response. A
    # bucket larger than 25 has no verified item paging and is reported partial.
    payload = session.json_get("/content/home", params={"bpp": HIDIVE_HOME_BUCKETS_PER_PAGE, "rpp": HIDIVE_HOME_ITEMS_PER_BUCKET})
    if not isinstance(payload, dict):
        raise HidiveSnapshotError("HIDIVE home payload was not a JSON object")
    buckets = payload.get("buckets")
    if not isinstance(buckets, list):
        raise HidiveSnapshotError("HIDIVE home payload did not include a buckets list")
    metadata: dict[str, Any] = {
        "pages_fetched": 1,
        "partial": False,
        "unpageable": False,
        "diagnostics": [],
    }
    for bucket in buckets:
        if not isinstance(bucket, dict):
            continue
        if str(bucket.get("name") or "").strip().upper() != "CONTINUE WATCHING":
            continue
        bucket_total_pages = _safe_int(bucket.get("totalPages"))
        bucket_total_results = _safe_int(bucket.get("totalResults"))
        metadata.update(
            {
                "bucket_page": _safe_int(bucket.get("page")),
                "bucket_results_per_page": _safe_int(bucket.get("resultsPerPage")),
                "bucket_total_pages": bucket_total_pages,
                "bucket_total_results": bucket_total_results,
            }
        )
        content_list = bucket.get("contentList")
        if isinstance(content_list, list):
            items = [item for item in content_list if isinstance(item, dict)]
            stopped_early = bool(continue_markers and any((_continue_item_fingerprint(item) in continue_markers) for item in items))
            partial = bool(
                (isinstance(bucket_total_pages, int) and bucket_total_pages > 1)
                or (isinstance(bucket_total_results, int) and bucket_total_results > len(items))
            )
            metadata["partial"] = partial
            metadata["unpageable"] = partial
            metadata["item_count"] = len(items)
            if partial:
                metadata["diagnostics"] = [
                    _diagnostic(
                        "continue_watching_partial_unpageable",
                        surface="continue_watching",
                        severity="info",
                        detail=f"HIDIVE Continue Watching holds more than {HIDIVE_HOME_ITEMS_PER_BUCKET} items (the per-bucket maximum) and has no verified item paging; the first {HIDIVE_HOME_ITEMS_PER_BUCKET} are retained",
                        item_count=len(items),
                        declared_total_pages=bucket_total_pages,
                        declared_total_results=bucket_total_results,
                    )
                ]
            return items, stopped_early, metadata
        return [], False, metadata
    return [], False, metadata


def _fetch_favourite_items(
    session: HidiveSession,
    *,
    favourite_markers: set[str] | None = None,
    backfill_markers: set[str] | None = None,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], int, int, bool, bool]:
    page = 1
    size = 100
    items: list[dict[str, Any]] = []
    backfill_items: list[dict[str, Any]] = []
    pages_fetched = 0
    backfill_pages_fetched = 0
    stopped_early = False
    backfill_exhausted = False
    front_boundary_seen = not favourite_markers
    backfill_cursor_seen = not backfill_markers
    seen_pages: set[str] = set()
    while True:
        if pages_fetched >= HIDIVE_FAVOURITE_PAGE_LIMIT:
            stopped_early = True
            break
        payload = session.json_get("/favourite/vods", params={"size": size, "page": page})
        pages_fetched += 1
        if not isinstance(payload, dict):
            raise HidiveSnapshotError("HIDIVE favourites payload was not a JSON object")
        # HIDIVE currently returns favourites under `events`
        events = payload.get("events") or payload.get("vods") or []
        if not isinstance(events, list):
            raise HidiveSnapshotError("HIDIVE favourites payload did not include an events/vods list")
        batch = [item for item in events if isinstance(item, dict)]
        page_fingerprint = _page_fingerprint(batch, _favourite_item_fingerprint)
        if page_fingerprint in seen_pages:
            stopped_early = True
            break
        seen_pages.add(page_fingerprint)
        items.extend(batch)
        batch_markers = {marker for item in batch if (marker := _favourite_item_fingerprint(item))}
        if not front_boundary_seen and favourite_markers and favourite_markers.intersection(batch_markers):
            front_boundary_seen = True
        elif front_boundary_seen and not backfill_cursor_seen and backfill_markers and backfill_markers.intersection(batch_markers):
            backfill_cursor_seen = True
        elif front_boundary_seen and backfill_cursor_seen and favourite_markers:
            backfill_items.extend(batch)
            backfill_pages_fetched += 1
            if backfill_pages_fetched >= INCREMENTAL_BACKFILL_PAGE_LIMIT:
                stopped_early = True
                break
        total_pages = _safe_int(payload.get("totalPages")) or 1
        if page >= total_pages or not events:
            if front_boundary_seen:
                backfill_exhausted = True
            break
        page += 1
    return items, backfill_items, pages_fetched, backfill_pages_fetched, stopped_early, backfill_exhausted


def _favourite_item_to_series(item: dict[str, Any]) -> SeriesRef | None:
    provider_series_id = item.get('id')
    title = item.get('title')
    if provider_series_id is None or not title:
        return None
    return SeriesRef(
        provider_series_id=str(provider_series_id),
        title=str(title),
        season_title=None,
        season_number=None,
    )


def _favourite_item_to_watchlist(item: dict[str, Any]) -> WatchlistEntry | None:
    provider_series_id = item.get('id')
    if provider_series_id is None:
        return None
    return WatchlistEntry(
        provider_series_id=str(provider_series_id),
        added_at=_iso_from_epoch_ms(item.get('watchedAt') or item.get('publishedDate')),
        status='favorite',
        list_id='favorites',
        list_name='Favorites',
        list_kind='system',
    )


def _custom_watchlist_external_id(item: dict[str, Any]) -> str | None:
    value = item.get("watchlistExternalId")
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _custom_watchlist_list_id(item: dict[str, Any]) -> str | None:
    external_id = _custom_watchlist_external_id(item)
    return f"watchlist:{external_id}" if external_id else None


def _custom_watchlist_name(item: dict[str, Any]) -> str | None:
    value = item.get("name")
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _custom_watchlist_kind(item: dict[str, Any]) -> str:
    if item.get("systemDefinedType") is not None:
        return "system"
    ownership = str(item.get("ownership") or "").strip().upper()
    if ownership == "OWNED":
        return "custom"
    if ownership in {"SHARED", "SAVED"}:
        return "shared"
    return "custom"


def _localized_content_title(item: dict[str, Any]) -> str | None:
    localisations = item.get("localisations")
    if isinstance(localisations, dict):
        for key in ("en_US", "en-US", "en", "en_us"):
            value = localisations.get(key)
            if isinstance(value, dict):
                title = value.get("title") or value.get("name")
                if title:
                    return str(title)
    for key in ("title", "name"):
        value = item.get(key)
        if value:
            return str(value)
    return None


def _custom_content_series_id(item: dict[str, Any]) -> str | None:
    series = item.get("series") if isinstance(item.get("series"), dict) else {}
    for value in (
        series.get("seriesId") if isinstance(series, dict) else None,
        series.get("id") if isinstance(series, dict) else None,
        item.get("seriesId"),
        item.get("seriesID"),
    ):
        if value is not None:
            text = str(value).strip()
            if text:
                return text
    content_type = str(item.get("type") or item.get("contentType") or "").strip().upper()
    if content_type in {"VOD_SERIES", "SERIES"} and item.get("id") is not None:
        return str(item.get("id"))
    return None


def _custom_content_series_title(item: dict[str, Any]) -> str | None:
    series = item.get("series") if isinstance(item.get("series"), dict) else {}
    if isinstance(series, dict):
        title = _localized_content_title(series)
        if title:
            return title
    return _localized_content_title(item)


def _custom_content_item_type(item: dict[str, Any]) -> str:
    value = item.get("type") or item.get("contentType") or "unknown"
    return str(value).strip().upper() or "unknown"


def _custom_content_item_id(item: dict[str, Any], provider_series_id: str) -> str:
    for key in ("id", "contentId", "externalAssetId"):
        value = item.get(key)
        if value is not None:
            text = str(value).strip()
            if text:
                return text
    return provider_series_id


def _custom_content_to_series(item: dict[str, Any]) -> SeriesRef | None:
    provider_series_id = _custom_content_series_id(item)
    title = _custom_content_series_title(item)
    if not provider_series_id or not title:
        return None
    return SeriesRef(
        provider_series_id=provider_series_id,
        title=title,
        season_title=_localized_content_title(item),
        season_number=_safe_int(item.get("seasonNumber") or item.get("season")),
    )


def _custom_content_to_watchlist(
    item: dict[str, Any],
    *,
    list_payload: dict[str, Any],
    position: int,
) -> WatchlistEntry | None:
    provider_series_id = _custom_content_series_id(item)
    if not provider_series_id:
        return None
    item_type = _custom_content_item_type(item)
    item_id = _custom_content_item_id(item, provider_series_id)
    return WatchlistEntry(
        provider_series_id=provider_series_id,
        added_at=None,
        status="in_watchlist",
        list_id=_custom_watchlist_list_id(list_payload),
        list_name=_custom_watchlist_name(list_payload),
        list_kind=_custom_watchlist_kind(list_payload),
        provider_item_id=item_id,
        provider_item_type=item_type,
        position=position,
    )


def _fetch_custom_watchlist_collection(session: HidiveSession) -> tuple[list[dict[str, Any]], int, list[dict[str, Any]], bool]:
    params: dict[str, Any] = {"rpp": HIDIVE_CUSTOM_WATCHLIST_RPP}
    items: list[dict[str, Any]] = []
    pages_fetched = 0
    diagnostics: list[dict[str, Any]] = []
    seen_page_fingerprints: dict[str, int] = {}
    seen_cursors: set[str] = set()
    while True:
        if pages_fetched >= HIDIVE_CUSTOM_WATCHLIST_COLLECTION_PAGE_LIMIT:
            diagnostics.append(_diagnostic(
                "custom_watchlist_collection_page_guard_hit", surface="watchlists",
                detail=f"HIDIVE custom watchlist collection exceeded page guard ({HIDIVE_CUSTOM_WATCHLIST_COLLECTION_PAGE_LIMIT})",
            ))
            return items, pages_fetched, diagnostics, True
        payload = session.json_get("/api/v3/user/watchlist", params=params)
        pages_fetched += 1
        if not isinstance(payload, dict):
            raise HidiveSnapshotError("HIDIVE custom watchlist collection payload was not a JSON object")
        watchlists = payload.get("watchlists")
        if not isinstance(watchlists, list):
            raise HidiveSnapshotError("HIDIVE custom watchlist collection payload did not include a watchlists list")
        batch = [item for item in watchlists if isinstance(item, dict)]
        fingerprint = _page_fingerprint(batch, lambda item: _custom_watchlist_external_id(item))
        previous_page = seen_page_fingerprints.get(fingerprint)
        if previous_page is not None:
            diagnostics.append(
                _diagnostic(
                    "custom_watchlist_collection_pagination_non_advancing",
                    surface="watchlists",
                    detail="HIDIVE custom watchlist collection returned a duplicate page fingerprint; stopped to avoid repeated non-advancing requests",
                    previous_page=previous_page,
                    repeated_page=pages_fetched,
                )
            )
            return items, pages_fetched, diagnostics, True
        seen_page_fingerprints[fingerprint] = pages_fetched
        items.extend(batch)
        if not _paging_more(payload):
            return items, pages_fetched, diagnostics, False
        has_cursor, cursor = _paging_last_seen(payload)
        if not has_cursor:
            diagnostics.append(
                _diagnostic(
                    "custom_watchlist_collection_missing_cursor",
                    surface="watchlists",
                    detail="HIDIVE custom watchlist collection reported moreDataAvailable without lastSeen; stopped before guessing a cursor",
                    page=pages_fetched,
                )
            )
            return items, pages_fetched, diagnostics, True
        cursor_key = json.dumps(cursor, sort_keys=True, default=str)
        if cursor_key in seen_cursors:
            diagnostics.append(
                _diagnostic(
                    "custom_watchlist_collection_cursor_non_advancing",
                    surface="watchlists",
                    detail="HIDIVE custom watchlist collection returned a repeated lastSeen cursor; stopped to avoid repeated non-advancing requests",
                    page=pages_fetched,
                )
            )
            return items, pages_fetched, diagnostics, True
        seen_cursors.add(cursor_key)
        params = {"rpp": HIDIVE_CUSTOM_WATCHLIST_RPP, "lastSeen": cursor}


def _fetch_custom_watchlist_detail(
    session: HidiveSession,
    list_item: dict[str, Any],
) -> tuple[dict[str, Any], list[dict[str, Any]], int, list[dict[str, Any]], bool]:
    external_id = _custom_watchlist_external_id(list_item)
    if not external_id:
        return list_item, [], 0, [
            _diagnostic(
                "custom_watchlist_missing_id",
                surface="watchlists",
                detail="HIDIVE custom watchlist collection row omitted watchlistExternalId; skipped detail fetch",
            )
        ], True
    params: dict[str, Any] = {"rpp": HIDIVE_CUSTOM_WATCHLIST_RPP}
    path = f"/api/v4/user/watchlist/{quote(external_id, safe='')}"
    detail_payload: dict[str, Any] = {**list_item}
    content_items: list[dict[str, Any]] = []
    pages_fetched = 0
    diagnostics: list[dict[str, Any]] = []
    seen_page_fingerprints: dict[str, int] = {}
    seen_cursors: set[str] = set()
    while True:
        if pages_fetched >= HIDIVE_CUSTOM_WATCHLIST_DETAIL_PAGE_LIMIT:
            diagnostics.append(_diagnostic(
                "custom_watchlist_detail_page_guard_hit", surface="watchlists",
                detail=f"HIDIVE custom watchlist detail exceeded page guard ({HIDIVE_CUSTOM_WATCHLIST_DETAIL_PAGE_LIMIT})",
                list_id=_custom_watchlist_list_id(detail_payload),
            ))
            return detail_payload, content_items, pages_fetched, diagnostics, True
        payload = session.json_get(path, params=params)
        pages_fetched += 1
        if not isinstance(payload, dict):
            raise HidiveSnapshotError("HIDIVE custom watchlist detail payload was not a JSON object")
        detail_payload.update({key: value for key, value in payload.items() if key != "content"})
        content = payload.get("content")
        if not isinstance(content, list):
            raise HidiveSnapshotError("HIDIVE custom watchlist detail payload did not include a content list")
        batch = [item for item in content if isinstance(item, dict)]
        fingerprint = _page_fingerprint(batch, lambda item: "|".join([_custom_content_item_type(item), _custom_content_item_id(item, _custom_content_series_id(item) or "")]))
        previous_page = seen_page_fingerprints.get(fingerprint)
        if previous_page is not None:
            diagnostics.append(
                _diagnostic(
                    "custom_watchlist_detail_pagination_non_advancing",
                    surface="watchlists",
                    detail="HIDIVE custom watchlist detail returned a duplicate page fingerprint; stopped to avoid repeated non-advancing requests",
                    list_id=_custom_watchlist_list_id(detail_payload),
                    previous_page=previous_page,
                    repeated_page=pages_fetched,
                )
            )
            return detail_payload, content_items, pages_fetched, diagnostics, True
        seen_page_fingerprints[fingerprint] = pages_fetched
        content_items.extend(batch)
        if not _paging_more(payload):
            return detail_payload, content_items, pages_fetched, diagnostics, False
        has_cursor, cursor = _paging_last_seen(payload)
        if not has_cursor:
            diagnostics.append(
                _diagnostic(
                    "custom_watchlist_detail_missing_cursor",
                    surface="watchlists",
                    detail="HIDIVE custom watchlist detail reported moreDataAvailable without lastSeen; stopped before guessing a cursor",
                    list_id=_custom_watchlist_list_id(detail_payload),
                    page=pages_fetched,
                )
            )
            return detail_payload, content_items, pages_fetched, diagnostics, True
        cursor_key = json.dumps(cursor, sort_keys=True, default=str)
        if cursor_key in seen_cursors:
            diagnostics.append(
                _diagnostic(
                    "custom_watchlist_detail_cursor_non_advancing",
                    surface="watchlists",
                    detail="HIDIVE custom watchlist detail returned a repeated lastSeen cursor; stopped to avoid repeated non-advancing requests",
                    list_id=_custom_watchlist_list_id(detail_payload),
                    page=pages_fetched,
                )
            )
            return detail_payload, content_items, pages_fetched, diagnostics, True
        seen_cursors.add(cursor_key)
        params = {"rpp": HIDIVE_CUSTOM_WATCHLIST_RPP, "lastSeen": cursor}


def _fetch_custom_watchlist_items(session: HidiveSession) -> tuple[list[SeriesRef], list[WatchlistEntry], dict[str, Any], list[dict[str, Any]], bool]:
    collection_items, collection_pages, diagnostics, partial = _fetch_custom_watchlist_collection(session)
    series_entries: list[SeriesRef] = []
    watchlist_entries: list[WatchlistEntry] = []
    list_summaries: list[dict[str, Any]] = []
    detail_pages_fetched = 0
    skipped_unknown_content = 0
    duplicate_within_list = 0
    for collection_item in collection_items:
        detail_payload, content_items, pages_fetched, detail_diagnostics, detail_partial = _fetch_custom_watchlist_detail(session, collection_item)
        diagnostics.extend(detail_diagnostics)
        partial = partial or detail_partial
        detail_pages_fetched += pages_fetched
        list_seen_memberships: set[tuple[str, str]] = set()
        emitted_for_list = 0
        for item_index, item in enumerate(content_items):
            series = _custom_content_to_series(item)
            entry = _custom_content_to_watchlist(item, list_payload=detail_payload, position=item_index)
            if series is None or entry is None:
                skipped_unknown_content += 1
                continue
            membership_key = (entry.provider_item_type or "series", entry.provider_item_id or entry.provider_series_id)
            if membership_key in list_seen_memberships:
                duplicate_within_list += 1
                continue
            list_seen_memberships.add(membership_key)
            series_entries.append(series)
            watchlist_entries.append(entry)
            emitted_for_list += 1
        list_summaries.append(
            {
                "list_id": _custom_watchlist_list_id(detail_payload),
                "list_name": _custom_watchlist_name(detail_payload),
                "list_kind": _custom_watchlist_kind(detail_payload),
                "content_count": len(content_items),
                "emitted_membership_count": emitted_for_list,
                "detail_pages_fetched": pages_fetched,
                "partial": detail_partial,
            }
        )
    metadata = {
        "collection_pages_fetched": collection_pages,
        "detail_pages_fetched": detail_pages_fetched,
        "list_count": len(collection_items),
        "membership_count": len(watchlist_entries),
        "skipped_unknown_content_count": skipped_unknown_content,
        "duplicate_within_list_count": duplicate_within_list,
        "lists": list_summaries,
    }
    return series_entries, watchlist_entries, metadata, diagnostics, partial


def _dedupe_series(entries: list[SeriesRef]) -> list[SeriesRef]:
    by_id: dict[str, SeriesRef] = {}
    for entry in entries:
        by_id.setdefault(entry.provider_series_id, entry)
    return list(by_id.values())


def _progress_evidence_rank(entry: EpisodeProgress) -> int:
    if entry.progress_observation_kind == "explicit_completed":
        return 4
    if entry.progress_observation_kind in {"position", "ratio", "inferred_later_episode"}:
        return 3
    if entry.progress_observation_kind == "history_membership":
        return 1
    return 2  # legacy payload: preserve compatibility without outranking measured evidence


def _dedupe_progress(entries: list[EpisodeProgress]) -> list[EpisodeProgress]:
    by_id: dict[str, EpisodeProgress] = {}
    for entry in entries:
        previous = by_id.get(entry.provider_episode_id)
        if previous is None:
            by_id[entry.provider_episode_id] = entry
            continue
        candidate_key = (_progress_evidence_rank(entry), entry.last_watched_at or "")
        previous_key = (_progress_evidence_rank(previous), previous.last_watched_at or "")
        if candidate_key >= previous_key:
            by_id[entry.provider_episode_id] = entry
    return list(by_id.values())


def _fetch_snapshot(
    config: AppConfig,
    *,
    profile: str = "default",
    timeout_seconds: float | None = None,
    use_incremental_boundary: bool = True,
) -> HidiveFetchResult:
    try:
        session = start_hidive_session(config, profile=profile, timeout_seconds=timeout_seconds)
        boundary_file_present = session.state_paths.sync_boundary_path.exists()
        stored_boundary = _load_sync_boundary(session.state_paths)
        loaded_boundary = stored_boundary if use_incremental_boundary else None
        boundary = loaded_boundary
        boundary_account_status = "not_requested" if not use_incremental_boundary else "missing"
        if loaded_boundary is not None:
            if not session.token.account_id:
                boundary = None
                boundary_account_status = "current_account_unproven"
            elif not loaded_boundary.account_id_hint:
                boundary = None
                boundary_account_status = "boundary_account_unproven"
            elif loaded_boundary.account_id_hint != session.token.account_id:
                boundary = None
                boundary_account_status = "account_mismatch"
            else:
                boundary_account_status = "account_match"
        # HIDIVE's hot path pages history only down to the stored front-boundary
        # marker and skips account-scope favourites/custom watchlists.  Only use
        # that abbreviated mode once we have a matching local boundary;
        # first-run or account-swapped snapshots need a full account refresh.
        hot_mode = use_incremental_boundary and boundary is not None
        escalated_to_full_refresh = False
        if hot_mode and boundary is not None:
            history = _fetch_history_items(
                session,
                history_markers=set(boundary.history_markers),
                max_pages=HIDIVE_HOT_HISTORY_CATCHUP_PAGE_LIMIT,
            )
            if history.catchup_cap_hit and not history.front_boundary_complete and history.next_page is not None:
                # The boundary is unreachable within the hot budget.  Staying hot
                # would never advance it, so finish this run as a full refresh
                # (remaining history plus watchlist surfaces) and rebuild it.
                remainder = _fetch_history_items(session, start_page=history.next_page)
                history = _HistoryFetch(
                    items=[*history.items, *remainder.items],
                    pages_fetched=history.pages_fetched + remainder.pages_fetched,
                    stopped_early=remainder.stopped_early,
                    traversal_complete=remainder.traversal_complete,
                    front_boundary_seen=remainder.front_boundary_seen,
                    catchup_cap_hit=True,
                    next_page=None,
                    diagnostics=[*history.diagnostics, *remainder.diagnostics],
                )
                hot_mode = False
                escalated_to_full_refresh = True
        else:
            history = _fetch_history_items(session)
        history_items = history.items
        history_pages_fetched = history.pages_fetched
        history_stopped_early = history.stopped_early
        history_diagnostics = history.diagnostics
        if hot_mode:
            continue_items, continue_stopped_early, continue_metadata = _fetch_continue_items(
                session,
                continue_markers=set(boundary.continue_markers) if boundary else None,
            )
            favourite_items, favourite_backfill_items, favourite_pages_fetched, favourite_backfill_pages_fetched, favourite_stopped_early, favourite_backfill_exhausted = [], [], 0, 0, False, False
            custom_series, custom_watchlist_entries, custom_watchlist_metadata, custom_watchlist_diagnostics, custom_watchlist_partial = [], [], {
                "collection_pages_fetched": 0,
                "detail_pages_fetched": 0,
                "list_count": 0,
                "membership_count": 0,
                "lists": [],
            }, [], False
        else:
            # Full refresh (including an escalated hot run) traverses every
            # surface from the start; stored markers must not cut it short.
            continue_items, continue_stopped_early, continue_metadata = _fetch_continue_items(session)
            favourite_items, favourite_backfill_items, favourite_pages_fetched, favourite_backfill_pages_fetched, favourite_stopped_early, favourite_backfill_exhausted = _fetch_favourite_items(session)
            custom_series, custom_watchlist_entries, custom_watchlist_metadata, custom_watchlist_diagnostics, custom_watchlist_partial = _fetch_custom_watchlist_items(session)
    except HidiveAuthError as exc:
        raise
    except Exception as exc:
        raise HidiveSnapshotError(str(exc)) from exc

    history_series = [series for item in history_items if (series := _extract_series_ref_from_episode_info(item)) is not None]
    continue_series = [series for item in continue_items if (series := _extract_series_ref_from_episode_info(item)) is not None]
    favourite_series = [series for item in favourite_items if (series := _favourite_item_to_series(item)) is not None]
    history_progress = [progress for item in history_items if (progress := _history_item_to_progress(item)) is not None]
    continue_progress = [progress for item in continue_items if (progress := _continue_item_to_progress(item)) is not None]
    favourite_watchlist_entries = [entry for item in favourite_items if (entry := _favourite_item_to_watchlist(item)) is not None]
    watchlist_entries = [*favourite_watchlist_entries, *custom_watchlist_entries]
    diagnostics = [*history_diagnostics, *(continue_metadata.get("diagnostics") if isinstance(continue_metadata.get("diagnostics"), list) else []), *custom_watchlist_diagnostics]
    history_non_advancing_detected = any(item.get("code") == "history_pagination_non_advancing" for item in diagnostics if isinstance(item, dict))
    continue_partial = continue_metadata.get("partial") is True
    history_front_boundary_complete = history.front_boundary_complete
    history_full_complete = history.traversal_complete
    continue_complete = not continue_partial
    watchlist_complete = bool(
        not hot_mode
        and not favourite_stopped_early
        and not custom_watchlist_partial
    )
    stored_boundary_account_mismatch = bool(
        stored_boundary is not None
        and stored_boundary.account_id_hint
        and session.token.account_id
        and stored_boundary.account_id_hint != session.token.account_id
    )
    # A hot run with a proven history front rewrites the history/continue
    # markers (favourite markers are carried over unchanged below).  Without
    # this the markers age until hot mode can no longer reach them.
    boundary_rewrite_eligible = bool(
        session.token.account_id
        and history_front_boundary_complete
        and (watchlist_complete or hot_mode)
        and not stored_boundary_account_mismatch
    )
    partial = bool(
        history_non_advancing_detected
        or history_stopped_early
        or continue_partial
        or custom_watchlist_partial
        or favourite_stopped_early
        or (hot_mode and not history_front_boundary_complete)
    )

    generated_at = _now_string()
    snapshot = ProviderSnapshot(
        contract_version=config.contract_version,
        generated_at=generated_at,
        provider="hidive",
        account_id_hint=session.token.account_id,
        series=_dedupe_series([*history_series, *continue_series, *favourite_series, *custom_series]),
        progress=_dedupe_progress([*history_progress, *continue_progress]),
        watchlist=watchlist_entries,
        fetch_provenance=[
            FetchProvenance(
                surface="history", completeness="complete" if history_full_complete else "partial",
                collected_count=len(history_items), pages_fetched=history_pages_fetched,
                observed_at=generated_at, route="history",
            ),
            FetchProvenance(
                surface="continue_watching", completeness="complete" if continue_complete else "partial",
                collected_count=len(continue_items), pages_fetched=continue_metadata.get("pages_fetched"),
                observed_at=generated_at, route="continue_watching",
            ),
            FetchProvenance(
                surface="watchlist", completeness="complete" if watchlist_complete else "partial",
                collected_count=len(watchlist_entries),
                pages_fetched=favourite_pages_fetched + int(custom_watchlist_metadata.get("collection_pages_fetched") or 0),
                observed_at=generated_at, route="watchlists",
            ),
        ],
        raw={
            "partial": partial,
            "diagnostics": diagnostics,
            "season_catalog": _refresh_season_catalog(session),
            "snapshot_producer": "mal_updater.hidive_snapshot",
            "surface_authority_schema_version": 1,
            "history_count": len(history_items),
            "history_pages_fetched": history_pages_fetched,
            "history_stopped_early": history_stopped_early,
            "history_non_advancing_detected": history_non_advancing_detected,
            "history_traversal_complete": history.traversal_complete,
            "history_catchup_page_limit": HIDIVE_HOT_HISTORY_CATCHUP_PAGE_LIMIT,
            "history_results_per_page": HIDIVE_HISTORY_RPP,
            "sync_boundary_escalated_to_full_refresh": escalated_to_full_refresh,
            # Surface authority is intentionally split: a hot run proves the
            # history front (down to the stored marker) without traversing the
            # full history.
            "history_boundary_complete": history_front_boundary_complete,
            "history_front_boundary_complete": history_front_boundary_complete,
            "history_full_complete": history_full_complete,
            "continue_count": len(continue_items),
            "continue_stopped_early": continue_stopped_early,
            "continue_pages_fetched": continue_metadata.get("pages_fetched"),
            "continue_partial": continue_partial,
            "continue_complete": continue_complete,
            "continue_unpageable": continue_metadata.get("unpageable") is True,
            "continue_bucket_total_pages": continue_metadata.get("bucket_total_pages"),
            "continue_bucket_total_results": continue_metadata.get("bucket_total_results"),
            "favourite_count": len(favourite_items),
            "favourite_pages_fetched": favourite_pages_fetched,
            "favourite_stopped_early": favourite_stopped_early,
            "favourite_backfill_pages_fetched": favourite_backfill_pages_fetched,
            "favourite_backfill_exhausted": favourite_backfill_exhausted,
            "custom_watchlist_count": len(custom_watchlist_entries),
            "custom_watchlist_list_count": custom_watchlist_metadata.get("list_count"),
            "custom_watchlist_collection_pages_fetched": custom_watchlist_metadata.get("collection_pages_fetched"),
            "custom_watchlist_detail_pages_fetched": custom_watchlist_metadata.get("detail_pages_fetched"),
            "custom_watchlist_partial": custom_watchlist_partial,
            "watchlist_complete": watchlist_complete,
            "custom_watchlist_lists": custom_watchlist_metadata.get("lists"),
            "custom_watchlist_skipped_unknown_content_count": custom_watchlist_metadata.get("skipped_unknown_content_count", 0),
            "custom_watchlist_duplicate_within_list_count": custom_watchlist_metadata.get("duplicate_within_list_count", 0),
            # Compatibility: present means the state file exists, even when it
            # is unusable.  Consumers needing trust should use usable/status.
            "sync_boundary_present": boundary_file_present,
            "sync_boundary_usable": boundary is not None,
            "sync_boundary_loaded": loaded_boundary is not None,
            "sync_boundary_account_status": boundary_account_status,
            "sync_boundary_rewrite_eligible": boundary_rewrite_eligible,
            "sync_boundary_rewrite_blocked_by_account_mismatch": stored_boundary_account_mismatch,
            "sync_boundary_mode": "hot" if hot_mode else "full_refresh",
            "sync_boundary_refresh_kind": (
                "hot_boundary_refresh"
                if hot_mode
                else "hot_boundary_unreachable_full_refresh"
                if escalated_to_full_refresh
                else "explicit_full_refresh"
                if not use_incremental_boundary
                else "account_repair_full_refresh"
            ),
            "hot_surface_only": hot_mode,
            "sync_boundary_schema_version": SYNC_BOUNDARY_SCHEMA_VERSION,
            "sync_boundary_path": str(session.state_paths.sync_boundary_path),
            "request_spacing_seconds": config.hidive.request_spacing_seconds,
            "request_spacing_jitter_seconds": config.hidive.request_spacing_jitter_seconds,
            "retry_max_attempts": config.hidive.retry_max_attempts,
            "retry_after_cap_seconds": config.hidive.retry_after_cap_seconds,
            "niceness_policy": "local_host_process_gate",
            "supports": {
                "history": True,
                "continue_watching": True,
                "watchlists": not hot_mode,
            },
            "surface_authority": {
                "history_front_boundary_complete": history_front_boundary_complete,
                "history_full_complete": history_full_complete,
                "continue_complete": continue_complete,
                "watchlist_complete": watchlist_complete,
                "account_identity_proven": bool(session.token.account_id),
            },
        },
    )
    if boundary_rewrite_eligible:
        _write_sync_boundary(
            state_paths=session.state_paths,
            generated_at=generated_at,
            account_id_hint=session.token.account_id,
            history_items=history_items,
            continue_items=continue_items,
            favourite_items=favourite_items,
            favourite_backfill_items=[] if favourite_backfill_exhausted else favourite_backfill_items,
            favourite_markers_override=boundary.favourite_markers if hot_mode and boundary else None,
            favourite_backfill_markers_override=boundary.favourite_backfill_markers if hot_mode and boundary else None,
        )
    return HidiveFetchResult(
        snapshot=snapshot,
        history_count=len(history_items),
        continue_count=len(continue_items),
        favourite_count=len(favourite_items),
        custom_watchlist_count=len(custom_watchlist_entries),
    )


def _bind_fetch_authority(fetch_impl):
    """Bind producer authority to this concrete fetch implementation only."""
    authority = object()

    def authoritative_fetch(*args, **kwargs) -> HidiveFetchResult:
        result = fetch_impl(*args, **kwargs)
        result._ingestion_authority = authority
        return result

    def accepts(candidate: Any) -> bool:
        return candidate is authority

    return authoritative_fetch, accepts


fetch_snapshot, has_hidive_snapshot_authority = _bind_fetch_authority(_fetch_snapshot)
del _bind_fetch_authority


def snapshot_to_dict(snapshot: ProviderSnapshot) -> dict[str, Any]:
    return _snapshot_to_dict(snapshot)


def write_snapshot_file(path: Path, snapshot: ProviderSnapshot) -> Path:
    return _write_snapshot_file(path, snapshot)
