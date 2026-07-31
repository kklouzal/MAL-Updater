from __future__ import annotations

from urllib.parse import quote, urlparse

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


def is_hidive_generated_web_url(url: object) -> bool:
    """True for HIDIVE web URLs that are safe to replace with canonical routes."""
    if not isinstance(url, str) or not url.strip():
        return True
    try:
        parsed = urlparse(url.strip())
    except ValueError:
        return False
    host = parsed.netloc.casefold()
    if host not in {"hidive.com", "www.hidive.com"}:
        return False
    return parsed.path.startswith("/season/") or parsed.path.startswith("/series/")
