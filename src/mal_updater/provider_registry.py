from __future__ import annotations

from importlib import import_module
from collections.abc import Iterable

from .provider_types import ProviderModule


_PROVIDER_REGISTRY: dict[str, ProviderModule] = {}
_BUILTIN_PROVIDER_MODULES = (
    "mal_updater.providers.crunchyroll",
    "mal_updater.providers.hidive",
)


def ensure_builtin_providers_registered() -> None:
    for module_name in _BUILTIN_PROVIDER_MODULES:
        import_module(module_name)


def register_provider(provider: ProviderModule) -> None:
    _PROVIDER_REGISTRY[provider.slug] = provider


def get_provider(slug: str) -> ProviderModule:
    ensure_builtin_providers_registered()
    try:
        return _PROVIDER_REGISTRY[slug]
    except KeyError as exc:  # pragma: no cover - defensive branch
        available = ", ".join(sorted(_PROVIDER_REGISTRY)) or "<none>"
        raise KeyError(f"unknown provider '{slug}' (available: {available})") from exc


def list_providers() -> list[ProviderModule]:
    ensure_builtin_providers_registered()
    return [_PROVIDER_REGISTRY[key] for key in sorted(_PROVIDER_REGISTRY)]


def list_provider_slugs() -> Iterable[str]:
    ensure_builtin_providers_registered()
    return sorted(_PROVIDER_REGISTRY)
