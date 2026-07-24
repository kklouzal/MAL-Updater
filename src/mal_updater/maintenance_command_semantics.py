from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass

from .auth_failure_signals import auth_failure_remediation


_PROVIDER_HEALTH_LABELS = {
    "crunchyroll": "Crunchyroll",
    "hidive": "HIDIVE",
}


def _provider_health_label(provider: str) -> str:
    return _PROVIDER_HEALTH_LABELS.get(provider, provider)


def _provider_bootstrap_title(provider: str) -> str:
    # Preserve the existing bootstrap-audit status wording: Crunchyroll, Hidive.
    return provider.capitalize()


def auth_failure_label(auth_issue: Mapping[str, object] | None) -> str:
    if not isinstance(auth_issue, Mapping):
        return "auth looks degraded"
    label = auth_issue.get("auth_failure_label") or auth_issue.get("label")
    if isinstance(label, str) and label.strip() and label.strip().lower() != "none":
        return label.strip()
    return "auth looks degraded"


def _auth_failure_kind(auth_issue: Mapping[str, object] | None) -> str | None:
    if not isinstance(auth_issue, Mapping):
        return None
    value = auth_issue.get("auth_failure_kind") or auth_issue.get("kind")
    return str(value) if isinstance(value, str) and value else None


def _auth_issue_text(auth_issue: Mapping[str, object] | None, field: str) -> str | None:
    if not isinstance(auth_issue, Mapping):
        return None
    value = auth_issue.get(field)
    return value if isinstance(value, str) and value else None


@dataclass(frozen=True)
class AuthRemediationCommand:
    target: str
    command_args: tuple[str, ...]
    bootstrap_command_args: tuple[str, ...]
    reason_code: str
    automation_safe: bool
    requires_auth_interaction: bool
    auth_failure_kind: str | None = None
    auth_failure_label: str | None = None
    auth_remediation_kind: str | None = None
    auth_remediation_detail: str | None = None
    failure_reason: str | None = None

    @property
    def is_rebootstrap(self) -> bool:
        return self.reason_code.startswith("rebootstrap_")

    @property
    def provider(self) -> str | None:
        return None if self.target == "mal" else self.target

    def command_args_list(self) -> list[str]:
        return list(self.command_args)

    def bootstrap_command_args_list(self) -> list[str]:
        return list(self.bootstrap_command_args)

    def metadata(self) -> dict[str, object]:
        payload: dict[str, object] = {
            "reason_code": self.reason_code,
            "automation_safe": self.automation_safe,
            "requires_auth_interaction": self.requires_auth_interaction,
        }
        if isinstance(self.auth_failure_kind, str) and self.auth_failure_kind:
            payload["auth_failure_kind"] = self.auth_failure_kind
        if isinstance(self.auth_remediation_kind, str) and self.auth_remediation_kind:
            payload["auth_remediation_kind"] = self.auth_remediation_kind
        return payload

    def health_detail(self) -> str:
        if self.target == "mal":
            if not self.is_rebootstrap:
                return "Complete MAL OAuth and persist fresh access/refresh tokens"
            detail = (
                "Complete MAL OAuth again after repeated unattended MAL token refresh failures "
                f"({self.auth_failure_label or 'auth looks degraded'})"
            )
        else:
            provider_label = _provider_health_label(self.target)
            if not self.is_rebootstrap:
                return f"Re-bootstrap {provider_label} auth state from the staged local credentials"
            detail = (
                f"Re-bootstrap {provider_label} auth state after repeated auth-style unattended fetch failures "
                f"({self.auth_failure_label or 'auth looks degraded'})"
            )
        if isinstance(self.auth_remediation_detail, str) and self.auth_remediation_detail:
            detail += f"; {self.auth_remediation_detail}"
        if isinstance(self.failure_reason, str) and self.failure_reason:
            detail += f": {self.failure_reason}"
        return detail

    def health_command_payload(self, command_builder: Callable[[list[str]], str]) -> dict[str, object]:
        args = self.command_args_list()
        payload = {
            "reason_code": self.reason_code,
            "detail": self.health_detail(),
            "command_args": args,
            "command": command_builder(args),
            "automation_safe": self.automation_safe,
            "requires_auth_interaction": self.requires_auth_interaction,
        }
        if isinstance(self.auth_failure_kind, str) and self.auth_failure_kind:
            payload["auth_failure_kind"] = self.auth_failure_kind
        if isinstance(self.auth_remediation_kind, str) and self.auth_remediation_kind:
            payload["auth_remediation_kind"] = self.auth_remediation_kind
        return payload

    def bootstrap_operation_details(self) -> str:
        if self.target == "mal":
            if not self.is_rebootstrap:
                return "MyAnimeList OAuth tokens are not staged yet; complete MAL OAuth before treating unattended sync as ready."
            details = (
                "MyAnimeList OAuth tokens are staged, but repeated unattended token refresh failures suggest MAL auth should be completed again before trusting unattended sync "
                f"({self.auth_failure_label or 'auth looks degraded'})."
            )
        else:
            title = _provider_bootstrap_title(self.target)
            if not self.is_rebootstrap:
                return f"{title} credentials are staged but provider session state is still missing; finish provider bootstrap before expecting unattended fetches."
            details = (
                f"{title} credentials and session state are staged, but auth looks degraded ({self.auth_failure_label or 'auth looks degraded'}); "
                "re-bootstrap this provider before treating unattended fetches as healthy."
            )
        if self.is_rebootstrap and isinstance(self.auth_remediation_detail, str) and self.auth_remediation_detail:
            details += f" Recommended posture: {self.auth_remediation_detail}."
        if self.is_rebootstrap and isinstance(self.failure_reason, str) and self.failure_reason.strip():
            details += f" Latest signal: {self.failure_reason.strip()}"
        return details

    def bootstrap_guidance_command_fields(self, command: str | None) -> dict[str, object]:
        return guidance_command_fields(
            command=command,
            reason_code=self.reason_code,
            automation_safe=self.automation_safe,
            requires_auth_interaction=self.requires_auth_interaction,
            auth_failure_kind=self.auth_failure_kind,
            auth_remediation_kind=self.auth_remediation_kind,
        )

    def bootstrap_remediation_fields(self) -> dict[str, object]:
        return {
            "remediation_kind": self.auth_remediation_kind,
            "remediation_detail": self.auth_remediation_detail,
        }

    def bootstrap_onboarding_step_fields(self, *, command: str | None) -> dict[str, object]:
        payload: dict[str, object]
        if self.target == "mal":
            if self.is_rebootstrap:
                details = (
                    "Complete MAL OAuth again because repeated unattended MAL token refresh failures suggest the staged refresh material is no longer healthy "
                    f"({self.auth_failure_label or 'auth looks degraded'})."
                )
                if isinstance(self.failure_reason, str) and self.failure_reason.strip():
                    details += f" Latest signal: {self.failure_reason.strip()}"
                payload = {
                    "step": "rebootstrap-mal-oauth",
                    "details": details,
                    "user_action_required": True,
                    "applies_to": "mal",
                }
            else:
                payload = {
                    "step": "complete-mal-oauth",
                    "details": "Run `mal-updater mal-auth-login` after the MAL app exists so the skill can persist access and refresh tokens.",
                    "user_action_required": True,
                    "applies_to": "mal",
                }
        else:
            provider = self.target
            provider_label = _provider_health_label(provider)
            if self.is_rebootstrap:
                details = (
                    f"Re-bootstrap {provider_label} auth because staged session state looks degraded for unattended fetches "
                    f"({self.auth_failure_label or 'auth looks degraded'})."
                )
                if isinstance(self.failure_reason, str) and self.failure_reason.strip():
                    details += f" Latest signal: {self.failure_reason.strip()}"
                payload = {
                    "step": f"rebootstrap-{provider}-session",
                    "details": details,
                    "user_action_required": False,
                    "applies_to": provider,
                }
            else:
                details_by_provider = {
                    "crunchyroll": "Run `mal-updater provider-auth-login --provider crunchyroll` to mint and stage the long-lived Crunchyroll refresh token/device id pair.",
                    "hidive": "Run `mal-updater provider-auth-login --provider hidive` to mint and stage HIDIVE authorisation/refresh tokens.",
                }
                payload = {
                    "step": f"bootstrap-{provider}-session",
                    "details": details_by_provider.get(
                        provider,
                        f"Run `mal-updater provider-auth-login --provider {provider}` to mint and stage provider auth state.",
                    ),
                    "user_action_required": False,
                    "applies_to": provider,
                }
        payload.update(
            {
                "command": command,
                "command_args": self.bootstrap_command_args_list(),
                "reason_code": self.reason_code,
                "automation_safe": self.automation_safe,
                "requires_auth_interaction": self.requires_auth_interaction,
            }
        )
        if isinstance(self.auth_failure_kind, str) and self.auth_failure_kind:
            payload["auth_failure_kind"] = self.auth_failure_kind
        if isinstance(self.auth_remediation_kind, str) and self.auth_remediation_kind:
            payload["auth_remediation_kind"] = self.auth_remediation_kind
        return payload


def _descriptor_from_auth_issue(
    *,
    target: str,
    command_args: tuple[str, ...],
    bootstrap_command_args: tuple[str, ...],
    reason_code_prefix: str,
    requires_auth_interaction: bool,
    auth_issue: Mapping[str, object] | None,
) -> AuthRemediationCommand:
    failure_kind = _auth_failure_kind(auth_issue)
    remediation = auth_failure_remediation({"kind": failure_kind or "", "label": auth_failure_label(auth_issue)})
    remediation_kind = _auth_issue_text(auth_issue, "auth_remediation_kind")
    remediation_detail = _auth_issue_text(auth_issue, "auth_remediation_detail") or remediation.get("detail")
    return AuthRemediationCommand(
        target=target,
        command_args=command_args,
        bootstrap_command_args=bootstrap_command_args,
        reason_code=f"{reason_code_prefix}_{failure_kind or 'auth_failures'}",
        automation_safe=False,
        requires_auth_interaction=requires_auth_interaction,
        auth_failure_kind=failure_kind,
        auth_failure_label=auth_failure_label(auth_issue),
        auth_remediation_kind=remediation_kind,
        auth_remediation_detail=remediation_detail if isinstance(remediation_detail, str) and remediation_detail else None,
        failure_reason=_auth_issue_text(auth_issue, "reason"),
    )


def mal_missing_auth_command() -> AuthRemediationCommand:
    return AuthRemediationCommand(
        target="mal",
        command_args=("mal-auth-login",),
        bootstrap_command_args=("PYTHONPATH=src", "python3", "-m", "mal_updater.cli", "mal-auth-login"),
        reason_code="missing_mal_auth_material",
        automation_safe=False,
        requires_auth_interaction=True,
    )


def mal_rebootstrap_auth_command(auth_issue: Mapping[str, object] | None) -> AuthRemediationCommand:
    return _descriptor_from_auth_issue(
        target="mal",
        command_args=("mal-auth-login",),
        bootstrap_command_args=("PYTHONPATH=src", "python3", "-m", "mal_updater.cli", "mal-auth-login"),
        reason_code_prefix="rebootstrap_mal_auth_after",
        requires_auth_interaction=True,
        auth_issue=auth_issue,
    )


def provider_missing_state_command(provider: str) -> AuthRemediationCommand:
    return AuthRemediationCommand(
        target=provider,
        command_args=("provider-auth-login", "--provider", provider),
        bootstrap_command_args=("PYTHONPATH=src", "python3", "-m", "mal_updater.cli", "provider-auth-login", "--provider", provider),
        reason_code=f"missing_{provider}_state",
        automation_safe=False,
        requires_auth_interaction=False,
    )


def provider_rebootstrap_auth_command(provider: str, auth_issue: Mapping[str, object] | None) -> AuthRemediationCommand:
    return _descriptor_from_auth_issue(
        target=provider,
        command_args=("provider-auth-login", "--provider", provider),
        bootstrap_command_args=("PYTHONPATH=src", "python3", "-m", "mal_updater.cli", "provider-auth-login", "--provider", provider),
        reason_code_prefix=f"rebootstrap_{provider}_auth_after",
        requires_auth_interaction=False,
        auth_issue=auth_issue,
    )


def guidance_command_fields(
    *,
    command: str | None,
    reason_code: str | None = None,
    automation_safe: bool | None = None,
    requires_auth_interaction: bool | None = None,
    auth_failure_kind: str | None = None,
    auth_remediation_kind: str | None = None,
) -> dict[str, object]:
    payload: dict[str, object] = {"next_command": command}
    if not isinstance(command, str) or not command:
        return payload
    if isinstance(reason_code, str) and reason_code:
        payload["next_command_reason_code"] = reason_code
    if isinstance(automation_safe, bool):
        payload["next_command_automation_safe"] = automation_safe
    if isinstance(requires_auth_interaction, bool):
        payload["next_command_requires_auth_interaction"] = requires_auth_interaction
    if isinstance(auth_failure_kind, str) and auth_failure_kind:
        payload["next_command_auth_failure_kind"] = auth_failure_kind
    if isinstance(auth_remediation_kind, str) and auth_remediation_kind:
        payload["next_command_auth_remediation_kind"] = auth_remediation_kind
    return payload


def provider_from_refresh_command_args(command_args: object) -> str | None:
    if not isinstance(command_args, list) or not command_args:
        return None
    if command_args[0] == "crunchyroll-fetch-snapshot":
        return "crunchyroll"
    if len(command_args) >= 2 and command_args[0] == "sync-source" and isinstance(command_args[1], str):
        return str(command_args[1])
    if command_args[0] == "provider-fetch-snapshot":
        for index, part in enumerate(command_args[:-1]):
            if part == "--provider" and isinstance(command_args[index + 1], str):
                return str(command_args[index + 1])
    return None


def normalized_provider_fetch_command_args(provider: str, command_args: list[str]) -> list[str]:
    """Return provider-generic fetch args while accepting legacy persisted forms."""
    normalized = ["provider-fetch-snapshot", "--provider", provider]
    if not command_args:
        return normalized
    first = command_args[0]
    if first == "provider-fetch-snapshot":
        index = 1
        while index < len(command_args):
            part = command_args[index]
            if part == "--provider" and index + 1 < len(command_args):
                index += 2
                continue
            normalized.append(part)
            index += 1
        return normalized
    if first == "crunchyroll-fetch-snapshot":
        normalized.extend(command_args[1:])
        return normalized
    if first == "sync-source":
        normalized.extend(command_args[2:])
        return normalized
    normalized.extend(command_args[1:])
    return normalized
