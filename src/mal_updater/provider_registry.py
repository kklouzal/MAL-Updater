from __future__ import annotations

from importlib import import_module
from collections.abc import Iterable
from typing import TYPE_CHECKING

from .provider_types import ProviderModule

if TYPE_CHECKING:
    from .config import AppConfig


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


def list_provider_slugs() -> Iterable[str]:
    ensure_builtin_providers_registered()
    return sorted(_PROVIDER_REGISTRY)


def connected_source_providers(config: AppConfig) -> list[str]:
    """Watch-history providers that can run unattended: Crunchyroll with a linked
    session or both credentials, HIDIVE with both credentials.

    MAL-only installs return an empty list; every provider-backed lane keys off it.
    """
    from .crunchyroll_auth import crunchyroll_auth_material_present
    from .hidive_auth import load_hidive_credentials

    providers: list[str] = []
    if crunchyroll_auth_material_present(config):
        providers.append("crunchyroll")
    hidive_credentials = load_hidive_credentials(config)
    if hidive_credentials.username and hidive_credentials.password:
        providers.append("hidive")
    return providers


def started_source_providers(config: AppConfig) -> list[str]:
    """Watch-history providers with any credential or session file staged.

    A superset of :func:`connected_source_providers`: only a started provider
    can be incompletely configured, so setup guidance and warnings key off it.
    """
    from .crunchyroll_auth import load_crunchyroll_credentials, resolve_crunchyroll_state_paths
    from .hidive_auth import load_hidive_credentials, resolve_hidive_state_paths

    providers: list[str] = []
    crunchyroll_credentials = load_crunchyroll_credentials(config)
    crunchyroll_state = resolve_crunchyroll_state_paths(config)
    if (
        crunchyroll_credentials.username
        or crunchyroll_credentials.password
        or crunchyroll_state.refresh_token_path.exists()
        or crunchyroll_state.device_id_path.exists()
    ):
        providers.append("crunchyroll")
    hidive_credentials = load_hidive_credentials(config)
    hidive_state = resolve_hidive_state_paths(config)
    if (
        hidive_credentials.username
        or hidive_credentials.password
        or hidive_state.access_token_path.exists()
        or hidive_state.refresh_token_path.exists()
    ):
        providers.append("hidive")
    return providers
