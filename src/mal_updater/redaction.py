"""Dependency-light shared sanitizers for telemetry and diagnostic data.

The public APIs are :func:`sanitize_url`, :func:`sanitize_text`, and
:func:`sanitize_value`.  They preserve useful diagnostic shape while replacing
credentials with :data:`REDACTED`.  Every configured bound has an explicit,
stable truncation suffix or metadata sentinel; output is never silently clipped.
"""

from __future__ import annotations

from collections.abc import Mapping
import re
import shlex
from typing import Any
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

REDACTED = "<redacted>"
NEUTRAL_URL_VALUE = "<value>"
INVALID_URL = "<invalid-url>"
TRUNCATED_TEXT_SUFFIX = "<truncated>"
TRUNCATED_ITEMS_KEY = "__sanitizer_truncated_items__"
TRUNCATED_DEPTH_KEY = "__sanitizer_truncated_depth__"

DEFAULT_TEXT_LIMIT = 2_000
DEFAULT_COLLECTION_LIMIT = 100
DEFAULT_DEPTH_LIMIT = 8

_SENSITIVE_LABELS = frozenset(
    {
        "token",
        "access_token",
        "refresh_token",
        "authorization",
        "auth",
        "password",
        "passwd",
        "secret",
        "client_secret",
        "api_key",
        "x_api_key",
        "cookie",
        "set_cookie",
        "session",
        "sessionid",
        "code",
        "credential",
    }
)
_ACCOUNT_SENSITIVE_LABELS = frozenset(
    {
        "account",
        "email",
        "login",
        "user",
        "username",
    }
)
# q/query were already private in request telemetry before this module existed.
# They remain URL/text-only compatibility labels, not structured credential keys.
_PRIVATE_QUERY_LABELS = frozenset({"q", "query"})

_SENSITIVE_LABEL_PATTERN = (
    r"access[_-]?token|refresh[_-]?token|authorization|client[_-]?secret|"
    r"x[_-]?api[_-]?key|api[_-]?key|set[_-]?cookie|password|passwd|"
    r"credential|sessionid|session|secret|cookie|token|auth|code"
)
_ACCOUNT_LABEL_PATTERN = r"user[_-]?name|account(?:[_-]?name)?|email|login|user"
_BOUND_TEXT_LABEL_PATTERN = rf"(?:{_SENSITIVE_LABEL_PATTERN}|{_ACCOUNT_LABEL_PATTERN}|query|q)"
_BOUND_TEXT_KEY = (
    rf'(?:"{_BOUND_TEXT_LABEL_PATTERN}"|\'{_BOUND_TEXT_LABEL_PATTERN}\'|{_BOUND_TEXT_LABEL_PATTERN})'
)
_BOUND_TEXT_PREFIX = (
    rf"(?P<prefix>(?<![A-Za-z0-9_-]){_BOUND_TEXT_KEY}(?![A-Za-z0-9_-])\s*[:=]\s*)"
)
_QUOTED_BOUND_VALUE = re.compile(
    rf"{_BOUND_TEXT_PREFIX}(?P<quote>[\"'])(?P<value>.*?)(?P=quote)",
    re.IGNORECASE,
)
_UNQUOTED_BOUND_VALUE = re.compile(
    rf"{_BOUND_TEXT_PREFIX}(?P<value>[^\s,;&}}\]]+)",
    re.IGNORECASE,
)
_CLI_BOUND_VALUE = re.compile(
    rf"(?P<prefix>--(?:{_SENSITIVE_LABEL_PATTERN})(?![A-Za-z0-9_-])\s+)"
    rf"(?P<value>\"[^\"]*\"|'[^']*'|[^\s,;&]+)",
    re.IGNORECASE,
)
_CLI_EQUALS_VALUE = re.compile(
    rf"(?P<prefix>--(?:{_SENSITIVE_LABEL_PATTERN})(?![A-Za-z0-9_-])=)"
    rf"(?P<value>\"[^\"]*\"|'[^']*'|[^\s,;&]+)",
    re.IGNORECASE,
)
_AUTHORIZATION_BOUND_VALUE = re.compile(
    r"(?P<prefix>(?<![A-Za-z0-9_-])(?:authorization|auth)(?![A-Za-z0-9_-])\s*[:=]\s*)"
    r"(?P<scheme>bearer|basic)(?P<space>\s+)(?P<value>[^\s,;&}\]]+)",
    re.IGNORECASE,
)
_BEARER_VALUE = re.compile(
    r"(?<![A-Za-z0-9_-])(?P<scheme>bearer)(?P<space>\s+)(?P<value>[A-Za-z0-9._~+/=-]+)",
    re.IGNORECASE,
)
_BASIC_VALUE = re.compile(
    r"(?<![A-Za-z0-9_-])(?P<scheme>basic)(?P<space>\s+)(?P<value>[A-Za-z0-9._~+/=-]{8,})",
    re.IGNORECASE,
)
_JWT_VALUE = re.compile(
    r"(?<![A-Za-z0-9_-])eyJ[A-Za-z0-9_-]{5,}\.[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}(?![A-Za-z0-9_-])"
)
_URL_IN_TEXT = re.compile(r"https?://[^\s<>\"']+", re.IGNORECASE)
_URL_TRAILING_PUNCTUATION = ".,;:!?)]}"
_VALID_SCHEME = re.compile(r"^[A-Za-z][A-Za-z0-9+.-]*$")
_INVALID_PERCENT_ESCAPE = re.compile(r"%(?![0-9A-Fa-f]{2})")


def _normalized_label(value: object) -> str:
    return str(value).strip().casefold().replace("-", "_")


def is_sensitive_field(field_name: object) -> bool:
    """Return whether ``field_name`` explicitly binds a credential value."""

    return _normalized_label(field_name) in _SENSITIVE_LABELS


def is_account_sensitive_field(field_name: object) -> bool:
    """Return whether ``field_name`` explicitly binds account identity data."""

    return _normalized_label(field_name) in _ACCOUNT_SENSITIVE_LABELS


def _is_private_query_field(field_name: str) -> bool:
    normalized = _normalized_label(field_name)
    return normalized in _SENSITIVE_LABELS or normalized in _ACCOUNT_SENSITIVE_LABELS or normalized in _PRIVATE_QUERY_LABELS


def _validated_text_limit(value: int, *, parameter: str) -> int:
    limit = int(value)
    if limit < len(TRUNCATED_TEXT_SUFFIX):
        raise ValueError(f"{parameter} must be at least {len(TRUNCATED_TEXT_SUFFIX)}")
    return limit


def _truncate_text(value: str, *, limit: int) -> str:
    if len(value) <= limit:
        return value
    visible_length = limit - len(TRUNCATED_TEXT_SUFFIX)
    return value[:visible_length] + TRUNCATED_TEXT_SUFFIX


def sanitize_url(value: object, *, max_length: int = DEFAULT_TEXT_LIMIT) -> str:
    """Sanitize an absolute URL while preserving its non-secret structure.

    Userinfo and fragments are removed.  Query order, repeated keys, and blank
    parameters are retained: sensitive values become ``<redacted>`` and every
    other value becomes ``<value>``.  Malformed URLs return ``<invalid-url>``.
    If the result exceeds ``max_length``, it ends with ``<truncated>``.
    """

    limit = _validated_text_limit(max_length, parameter="max_length")
    try:
        raw = str(value)
        parsed = urlsplit(raw)
        hostname = parsed.hostname
        port = parsed.port  # Access validates malformed and out-of-range ports.
        if not parsed.scheme or not _VALID_SCHEME.fullmatch(parsed.scheme) or not hostname:
            return INVALID_URL
        if any(character.isspace() or ord(character) < 32 for character in hostname):
            return INVALID_URL
        if _INVALID_PERCENT_ESCAPE.search(parsed.path) or _INVALID_PERCENT_ESCAPE.search(parsed.query):
            return INVALID_URL

        authority = hostname
        if ":" in authority and not authority.startswith("["):
            authority = f"[{authority}]"
        if port is not None:
            authority = f"{authority}:{port}"
        query = urlencode(
            [
                (key, REDACTED if _is_private_query_field(key) else NEUTRAL_URL_VALUE)
                for key, _query_value in parse_qsl(parsed.query, keep_blank_values=True)
            ]
        )
        sanitized = urlunsplit((parsed.scheme, authority, parsed.path, query, ""))
    except (TypeError, ValueError):
        # Parsing is lazy: malformed brackets and ports can raise only while
        # accessing parsed components or reconstructing the sanitized URL.
        return INVALID_URL
    return _truncate_text(sanitized, limit=limit)


def _sanitize_url_match(match: re.Match[str]) -> str:
    candidate = match.group(0)
    suffix = ""
    while candidate and candidate[-1] in _URL_TRAILING_PUNCTUATION:
        suffix = candidate[-1] + suffix
        candidate = candidate[:-1]
    return sanitize_url(candidate, max_length=DEFAULT_TEXT_LIMIT) + suffix


def sanitize_text(
    value: object,
    *,
    max_length: int = DEFAULT_TEXT_LIMIT,
    field_name: object | None = None,
) -> str:
    """Redact explicitly bound secrets/account values and bound the result.

    Bindings are case-insensitive and support ``=``, ``:``, query-string ``&``,
    and JSON-like quoted keys/values.  Bearer/Basic authorization payloads,
    credentialed URLs, and obvious compact JWTs are also redacted.  Ordinary
    prose, anime titles, and unbound email addresses are retained.  If
    ``field_name`` itself is sensitive, the complete value is redacted.
    """

    limit = _validated_text_limit(max_length, parameter="max_length")
    raw = str(value)
    if field_name is not None and (
        is_sensitive_field(field_name) or is_account_sensitive_field(field_name)
    ):
        return REDACTED

    # Hold sanitized URLs out of the label pass so URL path/userinfo text cannot
    # accidentally be interpreted as a free-standing ``user: value`` binding.
    sanitized_urls: list[str] = []

    def hold_url(match: re.Match[str]) -> str:
        sanitized_urls.append(_sanitize_url_match(match))
        return f"\ufff0{len(sanitized_urls) - 1}\ufff1"

    sanitized = _URL_IN_TEXT.sub(hold_url, raw)
    sanitized = _AUTHORIZATION_BOUND_VALUE.sub(
        lambda match: f"{match.group('prefix')}{match.group('scheme')}{match.group('space')}{REDACTED}",
        sanitized,
    )
    sanitized = _QUOTED_BOUND_VALUE.sub(
        lambda match: f"{match.group('prefix')}{match.group('quote')}{REDACTED}{match.group('quote')}",
        sanitized,
    )
    sanitized = _UNQUOTED_BOUND_VALUE.sub(
        lambda match: f"{match.group('prefix')}{REDACTED}",
        sanitized,
    )
    sanitized = _CLI_BOUND_VALUE.sub(
        lambda match: f"{match.group('prefix')}{REDACTED}",
        sanitized,
    )
    sanitized = _CLI_EQUALS_VALUE.sub(
        lambda match: f"{match.group('prefix')}{REDACTED}",
        sanitized,
    )
    sanitized = _BEARER_VALUE.sub(
        lambda match: f"{match.group('scheme')}{match.group('space')}{REDACTED}",
        sanitized,
    )
    sanitized = _BASIC_VALUE.sub(
        lambda match: f"{match.group('scheme')}{match.group('space')}{REDACTED}",
        sanitized,
    )
    sanitized = _JWT_VALUE.sub(REDACTED, sanitized)
    for index, sanitized_url in enumerate(sanitized_urls):
        sanitized = sanitized.replace(f"\ufff0{index}\ufff1", sanitized_url)
    return _truncate_text(sanitized, limit=limit)


def _validated_collection_limit(value: int) -> int:
    limit = int(value)
    if limit < 0:
        raise ValueError("max_items must be non-negative")
    return limit


def _validated_depth_limit(value: int) -> int:
    limit = int(value)
    if limit < 0:
        raise ValueError("max_depth must be non-negative")
    return limit


def _truncation_metadata_key(result: Mapping[Any, Any]) -> str:
    candidate = TRUNCATED_ITEMS_KEY
    suffix = 2
    while candidate in result:
        candidate = f"{TRUNCATED_ITEMS_KEY}_{suffix}"
        suffix += 1
    return candidate


def sanitize_value(
    value: Any,
    *,
    max_depth: int = DEFAULT_DEPTH_LIMIT,
    max_items: int = DEFAULT_COLLECTION_LIMIT,
    max_string: int = DEFAULT_TEXT_LIMIT,
) -> Any:
    """Recursively sanitize dict/list/tuple data within explicit bounds.

    Dict keys are preserved, while values bound to sensitive credential or
    account keys are replaced wholesale.  Lists and tuples retain their type,
    and ``None``/boolean/numeric scalars retain their value and type.  A bounded
    collection receives stable omitted-item metadata; a collection beyond
    ``max_depth`` becomes ``{"__sanitizer_truncated_depth__": True}``.
    """

    depth_limit = _validated_depth_limit(max_depth)
    item_limit = _validated_collection_limit(max_items)
    text_limit = _validated_text_limit(max_string, parameter="max_string")

    def walk(current: Any, depth: int) -> Any:
        if isinstance(current, str):
            return sanitize_text(current, max_length=text_limit)
        if current is None or isinstance(current, (bool, int, float, complex)):
            return current
        if isinstance(current, Mapping):
            if depth >= depth_limit:
                return {TRUNCATED_DEPTH_KEY: True}
            items = list(current.items())
            result: dict[Any, Any] = {}
            for key, item in items[:item_limit]:
                safe_key = sanitize_text(key, max_length=min(200, text_limit)) if isinstance(key, str) else key
                if safe_key in result and safe_key != key:
                    collision_index = 2
                    candidate = f"{safe_key}_{collision_index}"
                    while candidate in result:
                        collision_index += 1
                        candidate = f"{safe_key}_{collision_index}"
                    safe_key = candidate
                result[safe_key] = (
                    REDACTED
                    if is_sensitive_field(key) or is_account_sensitive_field(key)
                    else walk(item, depth + 1)
                )
            omitted = len(items) - min(len(items), item_limit)
            if omitted:
                result[_truncation_metadata_key(result)] = omitted
            return result
        if isinstance(current, list):
            if depth >= depth_limit:
                return {TRUNCATED_DEPTH_KEY: True}
            result = [walk(item, depth + 1) for item in current[:item_limit]]
            omitted = len(current) - len(result)
            if omitted:
                result.append({TRUNCATED_ITEMS_KEY: omitted})
            return result
        if isinstance(current, tuple):
            if depth >= depth_limit:
                return {TRUNCATED_DEPTH_KEY: True}
            result = tuple(walk(item, depth + 1) for item in current[:item_limit])
            omitted = len(current) - len(result)
            if omitted:
                result += ({TRUNCATED_ITEMS_KEY: omitted},)
            return result
        return current

    return walk(value, 0)


def sanitize_command(
    args: list[str] | tuple[str, ...],
    *,
    max_length: int = DEFAULT_TEXT_LIMIT,
) -> str:
    """Render shell arguments while redacting sensitive flags and URL values."""

    rendered: list[str] = []
    redact_next = False
    for raw_argument in args:
        argument = str(raw_argument)
        if redact_next:
            rendered.append(REDACTED)
            redact_next = False
            continue
        if argument.startswith("--") and is_sensitive_field(argument[2:]):
            rendered.append(argument)
            redact_next = True
            continue
        if "=" in argument:
            raw_key, _raw_value = argument.split("=", 1)
            key = raw_key[2:] if raw_key.startswith("--") else raw_key
            if is_sensitive_field(key):
                rendered.append(f"{raw_key}={REDACTED}")
                continue
        rendered.append(sanitize_text(argument, max_length=max_length))
    limit = _validated_text_limit(max_length, parameter="max_length")
    return _truncate_text(shlex.join(rendered), limit=limit)


__all__ = [
    "DEFAULT_COLLECTION_LIMIT",
    "DEFAULT_DEPTH_LIMIT",
    "DEFAULT_TEXT_LIMIT",
    "INVALID_URL",
    "NEUTRAL_URL_VALUE",
    "REDACTED",
    "TRUNCATED_DEPTH_KEY",
    "TRUNCATED_ITEMS_KEY",
    "TRUNCATED_TEXT_SUFFIX",
    "is_account_sensitive_field",
    "is_sensitive_field",
    "sanitize_command",
    "sanitize_text",
    "sanitize_url",
    "sanitize_value",
]
