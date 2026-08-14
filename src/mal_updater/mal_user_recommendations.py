from __future__ import annotations

from dataclasses import dataclass, field
import hashlib
from html import unescape
from html.parser import HTMLParser
import json
import re
import time
from socket import timeout as SocketTimeout
from typing import Any, Callable
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qsl, quote, urlencode, unquote, urljoin, urlparse, urlunparse
from urllib.request import HTTPRedirectHandler, Request, build_opener

from .config import AppConfig
from .mal_client import MalApiError, _read_http_error_detail
from .provider_niceness import ProviderRequestGate, retry_delay_seconds
from .request_tracking import record_api_request_event

DEFAULT_PUBLIC_USER_RECS_MAX_PAGES = 10
DEFAULT_PUBLIC_USER_RECS_MAX_BODY_BYTES = 4 * 1024 * 1024
DEFAULT_PUBLIC_USER_RECS_MAX_REDIRECTS = 2
PUBLIC_USER_RECS_USER_AGENT = (
    "MAL-Updater/0.1 public-user-recommendation-harvest "
    "(read-only; no recommendation prose or usernames retained)"
)

_RETRYABLE_HTTP_STATUSES = frozenset({429, 500, 502, 503, 504})
_USER_RECS_PATH_RE_TEMPLATE = r"^/anime/{source_id}(?:/[^/?#]+)?/userrecs/?$"
_ANIME_PATH_RE = re.compile(r"^/anime/(\d+)(?:/[^/?#]+)?/?$")
_MORE_USERS_RE = re.compile(r"Read\s+recommendations?\s+by\s+([\d,]+)\s+more\s+users?", re.IGNORECASE)
_RECOMMENDED_BY_RE = re.compile(r"\bRecommended\s+by\b", re.IGNORECASE)
_NO_USER_RECOMMENDATIONS_EMPTY_STATE_RE = re.compile(
    r"\b(?:no|there\s+(?:are|is)\s+no)\s+(?:user\s+)?recommendations?\b"
    r"|\b(?:recommendations?)\s+(?:have|has)\s+not\s+been\s+(?:made|written|submitted)\b"
    r"|\b(?:recommendations?)\s+(?:haven't|hasn't)\s+been\s+(?:made|written|submitted)\b",
    re.IGNORECASE,
)
_USER_RECS_HEADING_TEXTS = frozenset({"recommendations", "user recommendations", "anime recommendations"})


class PublicMalUserRecommendationsError(MalApiError):
    """Safe, non-mutating failure while reading/parsing public MAL user recs."""


class _NoRedirectHandler(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):  # type: ignore[override]
        return None


@dataclass(slots=True)
class PublicMalRecommendationEdge:
    target_mal_anime_id: int
    target_title: str | None
    num_recommendations: int
    page_url: str

    def as_edge_payload(self, *, source_url: str, page_count: int) -> dict[str, Any]:
        return {
            "target_mal_anime_id": self.target_mal_anime_id,
            "target_title": self.target_title,
            "num_recommendations": self.num_recommendations,
            # Store only aggregate/public-link provenance. Do not retain MAL
            # recommendation prose or usernames from the public HTML surface.
            "raw": {
                "source": "public_mal_userrecs",
                "target_mal_anime_id": self.target_mal_anime_id,
                "target_title": self.target_title,
                "num_recommendations": self.num_recommendations,
            },
            "provenance": {
                "source": "public_mal_userrecs",
                "source_url": source_url,
                "page_url": self.page_url,
                "page_count": int(page_count),
                "retained_fields": ["target_mal_anime_id", "target_title", "num_recommendations"],
                "privacy": "recommendation prose and usernames are parsed only for aggregate counts and are not persisted",
            },
        }


@dataclass(slots=True)
class ParsedPublicUserRecommendationsPage:
    edges: list[PublicMalRecommendationEdge]
    next_url: str | None
    explicit_empty: bool = False
    document_complete: bool = False
    terminal_evidence: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class PublicUserRecommendationsPageFetchResult:
    source_mal_anime_id: int
    requested_url: str
    final_url: str
    next_url: str | None
    page_fingerprint: str
    anchor: dict[str, Any]
    edges: list[PublicMalRecommendationEdge]
    explicit_empty: bool = False
    document_complete: bool = False
    terminal_evidence: dict[str, Any] = field(default_factory=dict)

    def edge_payloads(self, *, source_url: str, page_count: int) -> list[dict[str, Any]]:
        return [edge.as_edge_payload(source_url=source_url, page_count=page_count) for edge in self.edges]


@dataclass(slots=True)
class PublicUserRecommendationsHarvestResult:
    source_mal_anime_id: int
    source_title: str | None
    status: str
    complete: bool
    partial: bool
    edges: list[PublicMalRecommendationEdge] = field(default_factory=list)
    pages_fetched: int = 0
    source_url: str | None = None
    fetched_urls: list[str] = field(default_factory=list)
    error: str | None = None

    def as_dict(self) -> dict[str, Any]:
        return {
            "source_mal_anime_id": self.source_mal_anime_id,
            "source_title": self.source_title,
            "status": self.status,
            "complete": self.complete,
            "partial": self.partial,
            "edge_count": len(self.edges),
            "pages_fetched": self.pages_fetched,
            "source_url": self.source_url,
            "fetched_urls": list(self.fetched_urls),
            "error": self.error,
        }


@dataclass
class _Anchor:
    href: str | None
    rel: str
    class_name: str
    aria_label: str
    text_parts: list[str] = field(default_factory=list)


@dataclass
class _SurfaceTextCapture:
    tag: str
    class_name: str
    element_id: str
    role: str
    aria_label: str
    text_parts: list[str] = field(default_factory=list)


@dataclass
class _RecommendationBlock:
    target_titles_by_id: dict[int, str | None] = field(default_factory=dict)
    text_parts: list[str] = field(default_factory=list)

    def observe_target(self, target_id: int, title: str | None) -> None:
        existing = self.target_titles_by_id.get(target_id)
        normalized_title = " ".join((title or "").split()).strip() or None
        if existing is None and normalized_title:
            self.target_titles_by_id[target_id] = normalized_title
        else:
            self.target_titles_by_id.setdefault(target_id, existing)


class _UserRecommendationsHTMLParser(HTMLParser):
    def __init__(self, *, source_mal_anime_id: int, page_url: str, public_base_url: str):
        super().__init__(convert_charrefs=True)
        self.source_mal_anime_id = int(source_mal_anime_id)
        self.page_url = page_url
        self.public_base_url = public_base_url
        self.edges: list[PublicMalRecommendationEdge] = []
        self.next_hrefs: list[str] = []
        self._blocks: list[_RecommendationBlock] = []
        self._anchors: list[_Anchor] = []
        self._surface_text_captures: list[_SurfaceTextCapture] = []
        self._saw_userrecs_surface = False
        self._saw_explicit_empty = False
        self._saw_html_start = False
        self._saw_html_end = False
        self._saw_body_start = False
        self._saw_body_end = False
        self._recommendation_row_count = 0

    @property
    def saw_recommendation_surface(self) -> bool:
        return self._saw_userrecs_surface

    @property
    def saw_explicit_empty(self) -> bool:
        return self._saw_explicit_empty

    @property
    def document_complete(self) -> bool:
        return self._saw_html_start and self._saw_html_end and self._saw_body_start and self._saw_body_end

    @property
    def recommendation_row_count(self) -> int:
        return self._recommendation_row_count

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        attrs_dict = {name.lower(): (value or "") for name, value in attrs}
        lowered = tag.lower()
        if lowered == "html":
            self._saw_html_start = True
        elif lowered == "body":
            self._saw_body_start = True
        if lowered == "tr":
            self._blocks.append(_RecommendationBlock())
            self._recommendation_row_count += 1
        elif lowered == "a":
            self._anchors.append(
                _Anchor(
                    href=unescape(attrs_dict.get("href", "")).strip() or None,
                    rel=attrs_dict.get("rel", ""),
                    class_name=attrs_dict.get("class", ""),
                    aria_label=attrs_dict.get("aria-label", ""),
                )
            )
        elif lowered == "body":
            # Presence of a normal body is not sufficient for a valid rec page,
            # but helps distinguish an empty userrecs page once recommendation
            # page-specific copy is seen in text data.
            pass
        if _should_capture_surface_text(lowered, attrs_dict):
            self._surface_text_captures.append(
                _SurfaceTextCapture(
                    tag=lowered,
                    class_name=attrs_dict.get("class", ""),
                    element_id=attrs_dict.get("id", ""),
                    role=attrs_dict.get("role", ""),
                    aria_label=attrs_dict.get("aria-label", ""),
                )
            )

    def handle_endtag(self, tag: str) -> None:
        lowered = tag.lower()
        if lowered == "html":
            self._saw_html_end = True
        elif lowered == "body":
            self._saw_body_end = True
        if lowered == "a" and self._anchors:
            anchor = self._anchors.pop()
            text = " ".join("".join(anchor.text_parts).split()).strip()
            if anchor.href:
                target_id = _target_anime_id_from_href(anchor.href, base_url=self.page_url)
                if target_id is not None and self._blocks and target_id != self.source_mal_anime_id:
                    self._blocks[-1].observe_target(target_id, text)
                if _looks_like_next_anchor(anchor, text):
                    self.next_hrefs.append(anchor.href)
            return
        if lowered == "tr" and self._blocks:
            block = self._blocks.pop()
            edge = self._edge_from_block(block)
            if edge is not None:
                self.edges.append(edge)
        if self._surface_text_captures and self._surface_text_captures[-1].tag == lowered:
            capture = self._surface_text_captures.pop()
            text = " ".join("".join(capture.text_parts).split()).strip()
            if _looks_like_userrecs_page_surface(capture, text):
                self._saw_userrecs_surface = True
            attrs = f"{capture.class_name} {capture.element_id} {capture.role} {capture.aria_label}".casefold()
            dedicated_empty = (
                "userrecs-empty" in attrs
                or "recommendations-empty" in attrs
                or "empty-state" in attrs
                or "no-recommendations" in attrs
            )
            if dedicated_empty and _NO_USER_RECOMMENDATIONS_EMPTY_STATE_RE.search(text):
                self._saw_explicit_empty = True

    def handle_data(self, data: str) -> None:
        if self._anchors:
            self._anchors[-1].text_parts.append(data)
        if self._blocks:
            self._blocks[-1].text_parts.append(data)
        for capture in self._surface_text_captures:
            capture.text_parts.append(data)

    def _edge_from_block(self, block: _RecommendationBlock) -> PublicMalRecommendationEdge | None:
        if not block.target_titles_by_id:
            return None
        text = " ".join("".join(block.text_parts).split())
        more_counts = []
        for match in _MORE_USERS_RE.finditer(text):
            try:
                more_counts.append(int(match.group(1).replace(",", "")))
            except ValueError:
                continue
        recommended_by_count = len(_RECOMMENDED_BY_RE.findall(text))
        if more_counts:
            num_recommendations = max([recommended_by_count, *(count + 1 for count in more_counts)])
        elif recommended_by_count:
            num_recommendations = recommended_by_count
        else:
            return None
        target_id = sorted(block.target_titles_by_id)[0]
        if target_id == self.source_mal_anime_id:
            return None
        return PublicMalRecommendationEdge(
            target_mal_anime_id=target_id,
            target_title=block.target_titles_by_id.get(target_id),
            num_recommendations=max(1, int(num_recommendations)),
            page_url=self.page_url,
        )


def _collapse_slug(title: str | None, fallback: str) -> str:
    raw = str(title or fallback)
    cleaned = re.sub(r"[^0-9A-Za-z]+", "_", raw).strip("_")
    return cleaned or fallback


def build_public_user_recs_url(public_base_url: str, *, source_mal_anime_id: int, source_title: str | None) -> str:
    base = public_base_url.rstrip("/")
    slug = quote(_collapse_slug(source_title, str(int(source_mal_anime_id))))
    return f"{base}/anime/{int(source_mal_anime_id)}/{slug}/userrecs"


def _same_public_origin(url: str, public_base_url: str) -> bool:
    parsed = urlparse(url)
    base = urlparse(public_base_url)
    if parsed.scheme != "https" or base.scheme != "https":
        return False
    if parsed.hostname != "myanimelist.net" or base.hostname != "myanimelist.net":
        return False
    if not parsed.netloc or parsed.username or parsed.password:
        return False
    return parsed.scheme == base.scheme and parsed.netloc == base.netloc


def validate_public_user_recs_url(url: str, *, public_base_url: str, source_mal_anime_id: int) -> str:
    absolute = urljoin(public_base_url.rstrip("/") + "/", str(url))
    if not _same_public_origin(absolute, public_base_url):
        raise PublicMalUserRecommendationsError("public MAL userrecs URL points outside configured HTTPS MAL origin")
    parsed = urlparse(absolute)
    decoded_path = unquote(parsed.path)
    pattern = re.compile(_USER_RECS_PATH_RE_TEMPLATE.format(source_id=int(source_mal_anime_id)))
    if not pattern.match(decoded_path):
        raise PublicMalUserRecommendationsError("public MAL userrecs URL path is outside the source anime /userrecs surface")
    if parsed.params:
        raise PublicMalUserRecommendationsError("public MAL userrecs URL path parameters are not retained")
    if parsed.fragment:
        raise PublicMalUserRecommendationsError("public MAL userrecs URL fragments are not retained")
    query_pairs = parse_qsl(parsed.query, keep_blank_values=True)
    normalized_query = ""
    if query_pairs:
        if len(query_pairs) != 1 or query_pairs[0][0] != "p":
            raise PublicMalUserRecommendationsError("public MAL userrecs URL query is outside the supported page cursor")
        try:
            page_number = int(query_pairs[0][1])
        except ValueError as exc:
            raise PublicMalUserRecommendationsError("public MAL userrecs page cursor must be a positive integer") from exc
        if page_number < 1:
            raise PublicMalUserRecommendationsError("public MAL userrecs page cursor must be a positive integer")
        normalized_query = urlencode({"p": page_number})
    return urlunparse(parsed._replace(query=normalized_query, fragment=""))


def _target_anime_id_from_href(href: str, *, base_url: str) -> int | None:
    absolute = urljoin(base_url, unescape(href))
    parsed = urlparse(absolute)
    base = urlparse(base_url)
    if (
        parsed.scheme != "https"
        or parsed.hostname != "myanimelist.net"
        or parsed.netloc != base.netloc
        or parsed.username
        or parsed.password
    ):
        return None
    match = _ANIME_PATH_RE.match(unquote(parsed.path))
    if match is None:
        return None
    try:
        return int(match.group(1))
    except ValueError:
        return None


def _looks_like_next_anchor(anchor: _Anchor, text: str) -> bool:
    rel_tokens = {part.strip().lower() for part in anchor.rel.split() if part.strip()}
    if "next" in rel_tokens:
        return True
    normalized_text = " ".join(text.casefold().split())
    if normalized_text in {"next", "next ›", "next >", "›", ">", "next page"}:
        return True
    aria = " ".join(anchor.aria_label.casefold().split())
    class_tokens = {part.strip().lower() for part in anchor.class_name.split() if part.strip()}
    return aria in {"next", "next page"} and bool(class_tokens & {"next", "link", "pagination"})


def _attr_tokens(value: str) -> set[str]:
    return {part.strip().casefold() for part in re.split(r"\s+", value or "") if part.strip()}


def _normalized_surface_text(text: str) -> str:
    return " ".join((text or "").casefold().split())


def _should_capture_surface_text(tag: str, attrs: dict[str, str]) -> bool:
    if tag in {"h1", "h2", "h3", "p", "li"}:
        return True
    if tag not in {"div", "section", "td", "span"}:
        return False
    attr_blob = " ".join(
        attrs.get(name, "")
        for name in ("id", "class", "role", "aria-label", "data-testid", "data-cy")
    ).casefold()
    return "recommend" in attr_blob or "userrecs" in attr_blob or "empty" in attr_blob


def _looks_like_userrecs_page_surface(capture: _SurfaceTextCapture, text: str) -> bool:
    """Recognize page-specific public /userrecs surfaces, not generic nav copy.

    A generic MAL/global page can mention recommendations in navigation, footer,
    or challenge/error text. Empty harvests are destructive, so only accept an
    empty page after seeing the MAL userrecs content heading/container or a
    strong empty-state sentence that says there are no recommendations.
    """
    normalized = _normalized_surface_text(text)
    if not normalized:
        return False
    class_tokens = _attr_tokens(capture.class_name)
    attr_blob = " ".join(
        [capture.class_name, capture.element_id, capture.role, capture.aria_label]
    ).casefold()
    if _NO_USER_RECOMMENDATIONS_EMPTY_STATE_RE.search(text) and any(
        marker in attr_blob
        for marker in ("userrecs-empty", "recommendations-empty", "empty-state", "no-recommendations")
    ):
        return True
    if capture.tag in {"h1", "h2", "h3"} and normalized in _USER_RECS_HEADING_TEXTS:
        if normalized != "recommendations":
            return True
        if "h2_overwrite" in class_tokens:
            return True
        if "userrecs" in attr_blob or "recommend" in attr_blob:
            return True
        # MAL's public /anime/<id>/<slug>/userrecs body heading is an h2 named
        # "Recommendations". A bare paragraph/nav/footer mention is not enough,
        # but a page-content heading is still a recognizable page surface even
        # if MAL drops its legacy h2_overwrite class.
        return capture.tag == "h2"
    if "userrecs" in attr_blob and "recommend" in normalized:
        return True
    if "recommend" in attr_blob and normalized in _USER_RECS_HEADING_TEXTS:
        return True
    return False


def _dedupe_edges(edges: list[PublicMalRecommendationEdge]) -> list[PublicMalRecommendationEdge]:
    """Deduplicate targets while preserving observed public page order.

    Staged resumable crawls use page fingerprints/anchors as coherence signals.
    Keeping the parser's observed order lets a same-content MAL page reorder
    produce a different safe page fingerprint instead of being silently mixed
    into an existing staged chain. Terminal publication/ranking can still sort
    the aggregate later.
    """
    by_target: dict[int, PublicMalRecommendationEdge] = {}
    ordered_target_ids: list[int] = []
    for edge in edges:
        existing = by_target.get(edge.target_mal_anime_id)
        if existing is None:
            by_target[edge.target_mal_anime_id] = edge
            ordered_target_ids.append(edge.target_mal_anime_id)
            continue
        if edge.num_recommendations > existing.num_recommendations:
            by_target[edge.target_mal_anime_id] = PublicMalRecommendationEdge(
                target_mal_anime_id=edge.target_mal_anime_id,
                target_title=edge.target_title or existing.target_title,
                num_recommendations=edge.num_recommendations,
                page_url=edge.page_url,
            )
        elif edge.num_recommendations == existing.num_recommendations and not existing.target_title and edge.target_title:
            by_target[edge.target_mal_anime_id] = PublicMalRecommendationEdge(
                target_mal_anime_id=existing.target_mal_anime_id,
                target_title=edge.target_title,
                num_recommendations=existing.num_recommendations,
                page_url=existing.page_url,
            )
    return [by_target[target_id] for target_id in ordered_target_ids]


def _sort_edges_for_recommendation_rank(edges: list[PublicMalRecommendationEdge]) -> list[PublicMalRecommendationEdge]:
    return sorted(edges, key=lambda edge: (-edge.num_recommendations, edge.target_mal_anime_id))


def public_user_recs_page_anchor(edges: list[PublicMalRecommendationEdge]) -> dict[str, Any]:
    """Return a deterministic privacy-safe page anchor for staged crawl coherence.

    The anchor intentionally contains only aggregate target identifiers/titles that
    are already retained by the public-userrecs graph. Recommendation prose,
    usernames, profile links, and HTML are never included.
    """
    ordered = list(edges)
    anchor: dict[str, Any] = {
        "target_mal_anime_ids": [int(edge.target_mal_anime_id) for edge in ordered],
    }
    if ordered:
        first = ordered[0]
        last = ordered[-1]
        anchor["first_target_mal_anime_id"] = int(first.target_mal_anime_id)
        anchor["last_target_mal_anime_id"] = int(last.target_mal_anime_id)
        if first.target_title:
            anchor["first_target_title"] = first.target_title
        if last.target_title:
            anchor["last_target_title"] = last.target_title
    return anchor


def public_user_recs_page_fingerprint(
    *,
    final_url: str,
    next_url: str | None,
    edges: list[PublicMalRecommendationEdge],
) -> str:
    """Return a stable privacy-safe, order-sensitive fingerprint for one page.

    The fingerprint is a chain/coherence guard for resumable staging, not just a
    target-set content hash. It deliberately includes only retained aggregate
    fields, but preserves observed page order so page-order drift cannot be
    masked by sorting.
    """
    payload = {
        "final_url": final_url,
        "next_url": next_url,
        "edges": [
            {
                "target_mal_anime_id": int(edge.target_mal_anime_id),
                "target_title": edge.target_title,
                "num_recommendations": int(edge.num_recommendations),
            }
            for edge in edges
        ],
    }
    encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def parse_public_user_recommendations_page(
    html: str,
    *,
    source_mal_anime_id: int,
    page_url: str,
    public_base_url: str,
) -> ParsedPublicUserRecommendationsPage:
    validated_page_url = validate_public_user_recs_url(
        page_url,
        public_base_url=public_base_url,
        source_mal_anime_id=int(source_mal_anime_id),
    )
    parser = _UserRecommendationsHTMLParser(
        source_mal_anime_id=source_mal_anime_id,
        page_url=validated_page_url,
        public_base_url=public_base_url,
    )
    try:
        parser.feed(html)
        parser.close()
    except Exception as exc:  # HTMLParser can surface malformed entity edge cases.
        raise PublicMalUserRecommendationsError(f"malformed public MAL userrecs HTML: {exc}") from exc

    validated_candidates: list[str] = []
    if parser.next_hrefs:
        for href in parser.next_hrefs:
            candidate = validate_public_user_recs_url(
                urljoin(validated_page_url, href),
                public_base_url=public_base_url,
                source_mal_anime_id=source_mal_anime_id,
            )
            validated_candidates.append(candidate)
    distinct_next = list(dict.fromkeys(validated_candidates))
    if len(distinct_next) > 1:
        raise PublicMalUserRecommendationsError("public MAL userrecs page advertised conflicting next links")
    validated_next = distinct_next[0] if distinct_next else None

    edges = _dedupe_edges(parser.edges)
    if not parser.saw_recommendation_surface:
        raise PublicMalUserRecommendationsError("public MAL userrecs HTML did not contain a recognizable recommendation surface")
    if not parser.document_complete:
        raise PublicMalUserRecommendationsError("public MAL userrecs HTML document was incomplete or truncated")
    if validated_next is None and not edges and not parser.saw_explicit_empty:
        raise PublicMalUserRecommendationsError(
            "public MAL userrecs terminal empty state lacked dedicated structural evidence; manual review required"
        )
    terminal_evidence = {
        "document_complete": parser.document_complete,
        "recommendation_surface": parser.saw_recommendation_surface,
        "recommendation_row_count": parser.recommendation_row_count,
        "next_candidate_count": len(parser.next_hrefs),
        "next_links_consistent": len(distinct_next) <= 1,
        "explicit_empty": bool(parser.saw_explicit_empty and not edges and validated_next is None),
        "terminal": validated_next is None,
    }
    return ParsedPublicUserRecommendationsPage(
        edges=edges,
        next_url=validated_next,
        explicit_empty=bool(parser.saw_explicit_empty and not edges and validated_next is None),
        document_complete=parser.document_complete,
        terminal_evidence=terminal_evidence,
    )


class PublicMalUserRecommendationsClient:
    def __init__(
        self,
        config: AppConfig,
        *,
        opener: Any | None = None,
        sleep: Callable[[float], None] | None = None,
        clock: Callable[[], float] | None = None,
    ) -> None:
        self.config = config
        self._opener = opener or build_opener(_NoRedirectHandler())
        self._sleep = sleep or time.sleep
        self._request_gate = ProviderRequestGate(
            provider="mal",
            state_dir=config.state_dir,
            spacing_seconds=config.mal.request_spacing_seconds,
            jitter_seconds=config.mal.request_spacing_jitter_seconds,
            clock=clock or time.monotonic,
            sleep=self._sleep,
        )

    def harvest(
        self,
        source_mal_anime_id: int,
        *,
        source_title: str | None,
        max_pages: int = DEFAULT_PUBLIC_USER_RECS_MAX_PAGES,
        max_body_bytes: int = DEFAULT_PUBLIC_USER_RECS_MAX_BODY_BYTES,
    ) -> PublicUserRecommendationsHarvestResult:
        normalized_max_pages = max(1, int(max_pages))
        normalized_max_body = max(1024, int(max_body_bytes))
        start_url = validate_public_user_recs_url(
            build_public_user_recs_url(
                self.config.mal.public_base_url,
                source_mal_anime_id=int(source_mal_anime_id),
                source_title=source_title,
            ),
            public_base_url=self.config.mal.public_base_url,
            source_mal_anime_id=int(source_mal_anime_id),
        )
        fetched_urls: list[str] = []
        visited: set[str] = set()
        all_edges: list[PublicMalRecommendationEdge] = []
        next_url: str | None = start_url
        pages = 0
        while next_url:
            if next_url in visited:
                return PublicUserRecommendationsHarvestResult(
                    source_mal_anime_id=int(source_mal_anime_id),
                    source_title=source_title,
                    status="failed",
                    complete=False,
                    partial=True,
                    edges=[],
                    pages_fetched=pages,
                    source_url=start_url,
                    fetched_urls=fetched_urls,
                    error="public MAL userrecs pagination loop detected before completion",
                )
            if pages >= normalized_max_pages:
                return PublicUserRecommendationsHarvestResult(
                    source_mal_anime_id=int(source_mal_anime_id),
                    source_title=source_title,
                    status="failed",
                    complete=False,
                    partial=True,
                    edges=[],
                    pages_fetched=pages,
                    source_url=start_url,
                    fetched_urls=fetched_urls,
                    error="max_pages reached before public MAL userrecs pagination completed; existing edges preserved",
                )
            visited.add(next_url)
            html, final_url = self._fetch_html(
                next_url,
                source_mal_anime_id=int(source_mal_anime_id),
                max_body_bytes=normalized_max_body,
            )
            pages += 1
            fetched_urls.append(final_url)
            parsed = parse_public_user_recommendations_page(
                html,
                source_mal_anime_id=int(source_mal_anime_id),
                page_url=final_url,
                public_base_url=self.config.mal.public_base_url,
            )
            all_edges.extend(parsed.edges)
            next_url = parsed.next_url
        return PublicUserRecommendationsHarvestResult(
            source_mal_anime_id=int(source_mal_anime_id),
            source_title=source_title,
            status="ok",
            complete=True,
            partial=False,
            edges=_sort_edges_for_recommendation_rank(_dedupe_edges(all_edges)),
            pages_fetched=pages,
            source_url=start_url,
            fetched_urls=fetched_urls,
        )

    def fetch_page(
        self,
        source_mal_anime_id: int,
        *,
        page_url: str,
        max_body_bytes: int = DEFAULT_PUBLIC_USER_RECS_MAX_BODY_BYTES,
        max_attempts: int | None = None,
    ) -> PublicUserRecommendationsPageFetchResult:
        """Fetch and parse exactly one validated public MAL /userrecs page.

        This resumable-crawl primitive accepts a persisted cursor URL and
        returns only aggregate edges plus deterministic fingerprint/anchor data.
        It deliberately does not follow the advertised next link.
        """
        normalized_max_body = max(1024, int(max_body_bytes))
        validated_url = validate_public_user_recs_url(
            page_url,
            public_base_url=self.config.mal.public_base_url,
            source_mal_anime_id=int(source_mal_anime_id),
        )
        html, final_url = self._fetch_html(
            validated_url,
            source_mal_anime_id=int(source_mal_anime_id),
            max_body_bytes=normalized_max_body,
            max_attempts=max_attempts,
        )
        parsed = parse_public_user_recommendations_page(
            html,
            source_mal_anime_id=int(source_mal_anime_id),
            page_url=final_url,
            public_base_url=self.config.mal.public_base_url,
        )
        return PublicUserRecommendationsPageFetchResult(
            source_mal_anime_id=int(source_mal_anime_id),
            requested_url=validated_url,
            final_url=final_url,
            next_url=parsed.next_url,
            page_fingerprint=public_user_recs_page_fingerprint(
                final_url=final_url,
                next_url=parsed.next_url,
                edges=parsed.edges,
            ),
            anchor=public_user_recs_page_anchor(parsed.edges),
            edges=parsed.edges,
            explicit_empty=parsed.explicit_empty,
            document_complete=parsed.document_complete,
            terminal_evidence=parsed.terminal_evidence,
        )

    def _is_timeout_error(self, exc: BaseException) -> bool:
        if isinstance(exc, (TimeoutError, SocketTimeout)):
            return True
        if isinstance(exc, URLError):
            reason = exc.reason
            if isinstance(reason, (TimeoutError, SocketTimeout)):
                return True
            reason_text = str(reason).lower()
            return "timed out" in reason_text or "timeout" in reason_text
        return False

    def _fetch_html(
        self,
        url: str,
        *,
        source_mal_anime_id: int,
        max_body_bytes: int,
        max_attempts: int | None = None,
    ) -> tuple[str, str]:
        current_url = validate_public_user_recs_url(
            url,
            public_base_url=self.config.mal.public_base_url,
            source_mal_anime_id=source_mal_anime_id,
        )
        redirects = 0
        # Direct/legacy harvests retain configured retry behavior. Durable
        # orchestrators pass max_attempts=1 because they charge each invocation
        # as one hard request-attempt budget unit.
        attempts = (
            max(1, int(max_attempts))
            if max_attempts is not None
            else max(1, int(self.config.mal.retry_max_attempts))
        )
        while True:
            for attempt in range(1, attempts + 1):
                request = Request(
                    current_url,
                    headers={
                        "User-Agent": PUBLIC_USER_RECS_USER_AGENT,
                        "Accept": "text/html,application/xhtml+xml;q=0.9,*/*;q=0.1",
                    },
                    method="GET",
                )
                self._request_gate.wait_turn()
                try:
                    with self._opener.open(request, timeout=self.config.request_timeout_seconds) as response:
                        final_url = validate_public_user_recs_url(
                            response.geturl(),
                            public_base_url=self.config.mal.public_base_url,
                            source_mal_anime_id=source_mal_anime_id,
                        )
                        body = response.read(max_body_bytes + 1)
                        if len(body) > max_body_bytes:
                            record_api_request_event(
                                "mal",
                                "public_userrecs",
                                url=current_url,
                                method="GET",
                                outcome="body_oversize",
                                status_code=getattr(response, "status", None),
                                error=f"body exceeded {max_body_bytes} bytes",
                                config=self.config,
                            )
                            raise PublicMalUserRecommendationsError(
                                f"public MAL userrecs body exceeded {max_body_bytes} bytes"
                            )
                        charset = None
                        try:
                            charset = response.headers.get_content_charset()
                        except AttributeError:
                            charset = None
                        record_api_request_event(
                            "mal",
                            "public_userrecs",
                            url=current_url,
                            method="GET",
                            outcome="ok",
                            status_code=getattr(response, "status", None),
                            config=self.config,
                        )
                        return body.decode(charset or "utf-8", errors="replace"), final_url
                except HTTPError as exc:
                    if 300 <= exc.code < 400:
                        location = exc.headers.get("Location") if exc.headers is not None else None
                        exc.close()
                        if not location:
                            raise PublicMalUserRecommendationsError("public MAL userrecs redirect lacked Location header") from exc
                        redirects += 1
                        if redirects > DEFAULT_PUBLIC_USER_RECS_MAX_REDIRECTS:
                            raise PublicMalUserRecommendationsError("public MAL userrecs redirect limit exceeded") from exc
                        current_url = validate_public_user_recs_url(
                            urljoin(current_url, location),
                            public_base_url=self.config.mal.public_base_url,
                            source_mal_anime_id=source_mal_anime_id,
                        )
                        record_api_request_event(
                            "mal",
                            "public_userrecs",
                            url=current_url,
                            method="GET",
                            outcome="redirect",
                            status_code=exc.code,
                            config=self.config,
                        )
                        break
                    detail = _read_http_error_detail(exc)
                    record_api_request_event(
                        "mal",
                        "public_userrecs",
                        url=current_url,
                        method="GET",
                        outcome="http_error",
                        status_code=exc.code,
                        error=f"HTTP {exc.code}",
                        config=self.config,
                    )
                    if exc.code in _RETRYABLE_HTTP_STATUSES and attempt < attempts:
                        delay = retry_delay_seconds(
                            attempt,
                            retry_after=exc.headers.get("Retry-After") if exc.headers is not None else None,
                            base_seconds=self.config.mal.retry_backoff_base_seconds,
                            jitter_seconds=self.config.mal.retry_backoff_jitter_seconds,
                            cap_seconds=self.config.mal.retry_after_cap_seconds,
                        )
                        if delay > 0:
                            self._sleep(delay)
                        continue
                    raise PublicMalUserRecommendationsError(
                        f"public MAL userrecs request failed: HTTP {exc.code}: {detail}"
                    ) from exc
                except (URLError, TimeoutError, SocketTimeout) as exc:
                    outcome = "timeout" if self._is_timeout_error(exc) else "url_error"
                    record_api_request_event(
                        "mal",
                        "public_userrecs",
                        url=current_url,
                        method="GET",
                        outcome=outcome,
                        error=type(exc).__name__,
                        config=self.config,
                    )
                    if self._is_timeout_error(exc) and attempt < attempts:
                        delay = retry_delay_seconds(
                            attempt,
                            base_seconds=self.config.mal.retry_backoff_base_seconds,
                            jitter_seconds=self.config.mal.retry_backoff_jitter_seconds,
                            cap_seconds=self.config.mal.retry_after_cap_seconds,
                        )
                        if delay > 0:
                            self._sleep(delay)
                        continue
                    raise PublicMalUserRecommendationsError(f"public MAL userrecs request failed: {exc}") from exc
            else:
                break
        raise PublicMalUserRecommendationsError("public MAL userrecs request failed without a captured exception")
