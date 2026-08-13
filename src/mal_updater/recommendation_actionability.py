from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

STRICT_PROVIDER_ELIGIBILITY_PROVIDERS = frozenset({"crunchyroll", "hidive"})

APPROVED_MAPPING_IDENTITY_KIND = "approved_mapping"
MANUAL_VERIFIED_IDENTITY_KIND = "manual_verified"
USER_EXACT_IDENTITY_KIND = "user_exact"
AUTO_EXACT_IDENTITY_KIND = "auto_exact"
PROVIDER_TITLE_SEARCH_EXACT_IDENTITY_KIND = "provider_title_search_exact"
PROVIDER_FRANCHISE_SHELL_CHILD_IDENTITY_KIND = "provider_franchise_shell_child_match"

STRICT_PROVIDER_IDENTITY_MATCH_KINDS = frozenset(
    {
        APPROVED_MAPPING_IDENTITY_KIND,
        MANUAL_VERIFIED_IDENTITY_KIND,
        USER_EXACT_IDENTITY_KIND,
        AUTO_EXACT_IDENTITY_KIND,
        PROVIDER_TITLE_SEARCH_EXACT_IDENTITY_KIND,
        PROVIDER_FRANCHISE_SHELL_CHILD_IDENTITY_KIND,
    }
)

_NO_PROVIDER_EVIDENCE_REASONS = (
    "provider availability unverified",
    "English-dub evidence unknown",
)
_GENERIC_INCOMPLETE_REASON = "strict provider+dub proof incomplete"


@dataclass(frozen=True, slots=True)
class StrictProviderActionability:
    eligible: bool
    missing: tuple[str, ...] = ()


def normalize_audio_locale(value: Any) -> str | None:
    text = str(value or "").strip().lower().replace("_", "-")
    if not text:
        return None
    return text


def normalized_audio_locales(locales: Iterable[Any] | None) -> tuple[str, ...]:
    if locales is None:
        return ()
    seen: set[str] = set()
    normalized: list[str] = []
    for value in locales:
        locale = normalize_audio_locale(value)
        if locale is None or locale in seen:
            continue
        seen.add(locale)
        normalized.append(locale)
    return tuple(normalized)


def audio_locale_is_english(value: Any) -> bool:
    locale = normalize_audio_locale(value)
    return bool(locale == "en" or (locale is not None and locale.startswith("en-")))


def provider_audio_locales_have_english(locales: Iterable[Any] | None) -> bool:
    return any(audio_locale_is_english(value) for value in locales or ())


def _value(source: Any, key: str, default: Any = None) -> Any:
    if isinstance(source, Mapping):
        return source.get(key, default)
    return getattr(source, key, default)


def _normalized_text(value: Any) -> str:
    return str(value or "").strip().lower()


def _coerce_datetime(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        dt = value
    else:
        text = str(value).strip()
        if not text:
            return None
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        try:
            dt = datetime.fromisoformat(text)
        except ValueError:
            return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _coerce_now(now: Any) -> datetime:
    coerced = _coerce_datetime(now)
    if coerced is not None:
        return coerced
    return datetime.now(timezone.utc)


def _evidence_is_current(evidence: Any, *, now: datetime) -> bool:
    if _value(evidence, "expired") is True:
        return False
    expires_at = _coerce_datetime(_value(evidence, "expires_at"))
    if expires_at is not None:
        return expires_at > now
    if _value(evidence, "fresh") is True:
        return True
    return False


def _has_non_stale_last_verified_at(evidence: Any, *, now: datetime) -> bool:
    if _value(evidence, "verification_outcome") == "positive" and _coerce_datetime(_value(evidence, "last_successful_positive_at")) is not None:
        return _coerce_datetime(_value(evidence, "invalidated_at")) is None
    if _coerce_datetime(_value(evidence, "last_verified_at")) is None:
        return False
    return _evidence_is_current(evidence, now=now)


def strict_provider_eligibility_actionability(evidence: Any, *, now: Any = None) -> StrictProviderActionability:
    current = _coerce_now(now)
    missing: list[str] = []

    if _coerce_datetime(_value(evidence, "invalidated_at")) is not None or _normalized_text(
        _value(evidence, "verification_outcome")
    ) == "negative":
        missing.append("provider eligibility explicitly invalidated")

    provider = _normalized_text(_value(evidence, "provider"))
    identity_match_kind = _normalized_text(_value(evidence, "identity_match_kind"))
    if provider not in STRICT_PROVIDER_ELIGIBILITY_PROVIDERS or identity_match_kind not in STRICT_PROVIDER_IDENTITY_MATCH_KINDS:
        missing.append("Crunchyroll/HIDIVE identity unverified")

    if _normalized_text(_value(evidence, "review_status")) != "verified":
        missing.append("provider identity review unverified")

    if _normalized_text(_value(evidence, "catalog_status")) != "present":
        missing.append("current provider catalog presence unverified")

    if _normalized_text(_value(evidence, "english_dub_status")) != "present":
        missing.append("English-dub evidence unknown")

    raw_locales = _value(evidence, "audio_locales")
    locales = raw_locales if isinstance(raw_locales, Iterable) and not isinstance(raw_locales, (str, bytes, dict)) else ()
    if not provider_audio_locales_have_english(locales):
        missing.append("English audio-locales missing/unverified")

    if not _has_non_stale_last_verified_at(evidence, now=current):
        missing.append("current provider verification stale or missing")

    return StrictProviderActionability(eligible=not missing, missing=tuple(missing))


def is_strict_provider_eligibility_actionable(evidence: Any, *, now: Any = None) -> bool:
    return strict_provider_eligibility_actionability(evidence, now=now).eligible


def strict_provider_last_verified_at_for_persistence(evidence: Any, *, verified_at: Any, now: Any = None) -> str | None:
    """Return the persisted ``last_verified_at`` timestamp only for strict actionable evidence."""
    candidate: dict[str, Any]
    if isinstance(evidence, Mapping):
        candidate = dict(evidence)
    else:
        candidate = {
            key: _value(evidence, key)
            for key in (
                "provider",
                "identity_match_kind",
                "review_status",
                "catalog_status",
                "english_dub_status",
                "audio_locales",
                "expires_at",
                "fresh",
                "expired",
            )
        }
    if verified_at is None:
        return None
    candidate["last_verified_at"] = verified_at
    if not is_strict_provider_eligibility_actionable(candidate, now=now if now is not None else verified_at):
        return None
    if isinstance(verified_at, datetime):
        return verified_at.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    text = str(verified_at).strip()
    return text or None


def strict_provider_actionability_failure_reasons(evidence_rows: Iterable[Any] | None, *, now: Any = None) -> list[str]:
    rows = [row for row in evidence_rows or [] if row is not None]
    if not rows:
        return list(_NO_PROVIDER_EVIDENCE_REASONS)
    results = [strict_provider_eligibility_actionability(row, now=now) for row in rows]
    if any(result.eligible for result in results):
        return []
    missing_sets = [result.missing for result in results if result.missing]
    if not missing_sets:
        return [_GENERIC_INCOMPLETE_REASON]
    best = min(missing_sets, key=lambda values: (len(values), tuple(values)))
    return list(best or (_GENERIC_INCOMPLETE_REASON,))
