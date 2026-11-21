from __future__ import annotations

from urllib.parse import quote

HIDIVE_WEB_BASE_URL = "https://www.hidive.com"


def canonical_hidive_series_url(provider_series_id: object) -> str | None:
    """Return the stable HIDIVE VOD_SERIES route for a provider series id."""
    text = str(provider_series_id or "").strip()
    if not text:
        return None
    # HIDIVE VOD_SERIES ids observed from Algolia/front-end route tables are
    # numeric ids used under /series/{seriesId}.  Keep this intentionally simple
    # and do not fabricate /season URLs from series ids or slugs.
    return f"{HIDIVE_WEB_BASE_URL}/series/{quote(text, safe='')}"
