from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass

from .auth_failure_signals import auth_failure_remediation

_CLI_PREFIX = ("PYTHONPATH=src", "python3", "-m", "mal_updater.cli")
_PROVIDER_HEALTH_LABELS = {
    "crunchyroll": "Crunchyroll",
    "hidive": "HIDIVE",
}
_PROVIDER_BOOTSTRAP_SESSION_DETAILS = {
    "crunchyroll": "Run `mal-updater provider-auth-login --provider crunchyroll` to mint and stage the long-lived Crunchyroll refresh token/device id pair.",
    "hidive": "Run `mal-updater provider-auth-login --provider hidive` to mint and stage HIDIVE authorisation/refresh tokens.",
}


def _provider_health_label(provider: str) -> str:
    return _PROVIDER_HEALTH_LABELS.get(provider, provider)


def _provider_bootstrap_title(provider: str) -> str:
    return provider.capitalize()


def _text(value: object) -> str | None:
    return value if isinstance(value, str) and value else None


def _auth_failure_kind(auth_issue: Mapping[str, object] | None) -> str | None:
    if not isinstance(auth_issue, Mapping):
        return None
    value = auth_issue.get("auth_failure_kind") or auth_issue.get("kind")
    if not value:
        return None
    text = str(value)
    return text if text else None


def _auth_failure_label(auth_issue: Mapping[str, object] | None) -> str:
    if not isinstance(auth_issue, Mapping):
        return "auth looks degraded"
    value = auth_issue.get("auth_failure_label") or auth_issue.get("label")
    if value is None:
        return "auth looks degraded"
    text = str(value).strip()
    if text and text.lower() != "none":
        return text
    return "auth looks degraded"


def _auth_issue_text(auth_issue: Mapping[str, object] | None, field: str) -> str | None:
    if not isinstance(auth_issue, Mapping):
        return None
    return _text(auth_issue.get(field))


def _bootstrap_remediation_text(auth_issue: Mapping[str, object] | None, field: str) -> str | None:
    failure_kind = str(auth_issue.get("auth_failure_kind")) if isinstance(auth_issue, Mapping) else ""
    failure_label = str(auth_issue.get("auth_failure_label")) if isinstance(auth_issue, Mapping) else ""
    remediation = auth_failure_remediation({"kind": failure_kind, "label": failure_label})
    if field == "auth_remediation_kind":
        return _text(remediation.get("remediation_kind"))
    if field == "auth_remediation_detail":
        return _text(remediation.get("detail"))
    return None


@dataclass(frozen=True)
class AuthRemediationDescriptor:
    target: str
    command_args: tuple[str, ...]
    reason_code: str
    automation_safe: bool
    requires_auth_interaction: bool
    auth_failure_kind: str | None = None
    auth_failure_label: str | None = None
    auth_remediation_kind: str | None = None
    auth_remediation_detail: str | None = None
    health_auth_remediation_kind: str | None = None
    health_auth_remediation_detail: str | None = None
    failure_reason: str | None = None

    @property
    def is_rebootstrap(self) -> bool:
        return self.reason_code.startswith("rebootstrap_")

    @property
    def bootstrap_command_args(self) -> list[str]:
        return [*_CLI_PREFIX, *self.command_args]

    @property
    def bootstrap_command(self) -> str:
        return " ".join(self.bootstrap_command_args)

    @property
    def command(self) -> str:
        return self.bootstrap_command

    def command_args_list(self) -> list[str]:
        return list(self.command_args)

    def maintenance_command_args(self) -> list[str]:
        return list(self.command_args)

    def cli_command_args(self) -> list[str]:
        return self.bootstrap_command_args

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
        if isinstance(self.health_auth_remediation_detail, str) and self.health_auth_remediation_detail:
            detail += f"; {self.health_auth_remediation_detail}"
        if isinstance(self.failure_reason, str) and self.failure_reason:
            detail += f": {self.failure_reason}"
        return detail

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
        if isinstance(self.auth_remediation_detail, str) and self.auth_remediation_detail:
            details += f" Recommended posture: {self.auth_remediation_detail}."
        if isinstance(self.failure_reason, str) and self.failure_reason.strip():
            details += f" Latest signal: {self.failure_reason.strip()}"
        return details

    def bootstrap_guidance_command_fields(self, command: str | None = None) -> dict[str, object]:
        return guidance_command_fields(
            command=self.bootstrap_command if command is None else command,
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

    def bootstrap_onboarding_step_fields(self, command: str | None = None) -> dict[str, object]:
        if self.target == "mal":
            if self.is_rebootstrap:
                details = (
                    "Complete MAL OAuth again because repeated unattended MAL token refresh failures suggest the staged refresh material is no longer healthy "
                    f"({self.auth_failure_label or 'auth looks degraded'})."
                )
                if isinstance(self.failure_reason, str) and self.failure_reason.strip():
                    details += f" Latest signal: {self.failure_reason.strip()}"
                payload: dict[str, object] = {
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
            provider_label = _provider_health_label(self.target)
            if self.is_rebootstrap:
                details = (
                    f"Re-bootstrap {provider_label} auth because staged session state looks degraded for unattended fetches "
                    f"({self.auth_failure_label or 'auth looks degraded'})."
                )
                if isinstance(self.failure_reason, str) and self.failure_reason.strip():
                    details += f" Latest signal: {self.failure_reason.strip()}"
                payload = {
                    "step": f"rebootstrap-{self.target}-session",
                    "details": details,
                    "user_action_required": False,
                    "applies_to": self.target,
                }
            else:
                payload = {
                    "step": f"bootstrap-{self.target}-session",
                    "details": _PROVIDER_BOOTSTRAP_SESSION_DETAILS.get(
                        self.target,
                        f"Run `mal-updater provider-auth-login --provider {self.target}` to mint and stage provider auth state.",
                    ),
                    "user_action_required": False,
                    "applies_to": self.target,
                }
        payload.update(
            {
                "command": self.bootstrap_command if command is None else command,
                "command_args": self.bootstrap_command_args,
                "reason_code": self.reason_code,
                "automation_safe": self.automation_safe,
                "requires_auth_interaction": self.requires_auth_interaction,
            }
        )
        if isinstance(self.auth_failure_kind, str) and self.auth_failure_kind:
            payload["auth_failure_kind"] = self.auth_failure_kind
        if isinstance(self.health_auth_remediation_kind, str) and self.health_auth_remediation_kind:
            payload["auth_remediation_kind"] = self.health_auth_remediation_kind
        return payload


def _descriptor_from_auth_issue(
    *,
    target: str,
    command_args: tuple[str, ...],
    reason_code_prefix: str,
    requires_auth_interaction: bool,
    auth_issue: Mapping[str, object] | None,
) -> AuthRemediationDescriptor:
    failure_kind = _auth_failure_kind(auth_issue)
    return AuthRemediationDescriptor(
        target=target,
        command_args=command_args,
        reason_code=f"{reason_code_prefix}_{failure_kind or 'auth_failures'}",
        automation_safe=False,
        requires_auth_interaction=requires_auth_interaction,
        auth_failure_kind=failure_kind,
        auth_failure_label=_auth_failure_label(auth_issue),
        auth_remediation_kind=_bootstrap_remediation_text(auth_issue, "auth_remediation_kind"),
        auth_remediation_detail=_bootstrap_remediation_text(auth_issue, "auth_remediation_detail"),
        health_auth_remediation_kind=_auth_issue_text(auth_issue, "auth_remediation_kind"),
        health_auth_remediation_detail=_auth_issue_text(auth_issue, "auth_remediation_detail"),
        failure_reason=_auth_issue_text(auth_issue, "reason"),
    )


def mal_missing_auth_descriptor() -> AuthRemediationDescriptor:
    return AuthRemediationDescriptor(
        target="mal",
        command_args=("mal-auth-login",),
        reason_code="missing_mal_auth_material",
        automation_safe=False,
        requires_auth_interaction=True,
    )


def mal_rebootstrap_auth_descriptor(auth_issue: Mapping[str, object] | None) -> AuthRemediationDescriptor:
    return _descriptor_from_auth_issue(
        target="mal",
        command_args=("mal-auth-login",),
        reason_code_prefix="rebootstrap_mal_auth_after",
        requires_auth_interaction=True,
        auth_issue=auth_issue,
    )


def provider_missing_state_descriptor(provider: str) -> AuthRemediationDescriptor:
    return AuthRemediationDescriptor(
        target=provider,
        command_args=("provider-auth-login", "--provider", provider),
        reason_code=f"missing_{provider}_state",
        automation_safe=False,
        requires_auth_interaction=False,
    )


def provider_rebootstrap_auth_descriptor(provider: str, auth_issue: Mapping[str, object] | None) -> AuthRemediationDescriptor:
    return _descriptor_from_auth_issue(
        target=provider,
        command_args=("provider-auth-login", "--provider", provider),
        reason_code_prefix=f"rebootstrap_{provider}_auth_after",
        requires_auth_interaction=False,
        auth_issue=auth_issue,
    )


# Private compatibility aliases for the in-flight health/bootstrap extraction.
AuthRemediationCommand = AuthRemediationDescriptor


def build_mal_auth_login_descriptor() -> AuthRemediationDescriptor:
    return mal_missing_auth_descriptor()


def build_crunchyroll_auth_login_descriptor() -> AuthRemediationDescriptor:
    return provider_missing_state_descriptor("crunchyroll")


def build_hidive_auth_login_descriptor() -> AuthRemediationDescriptor:
    return provider_missing_state_descriptor("hidive")


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


__all__ = [
    "AuthRemediationCommand",
    "AuthRemediationDescriptor",
    "build_crunchyroll_auth_login_descriptor",
    "build_hidive_auth_login_descriptor",
    "build_mal_auth_login_descriptor",
    "guidance_command_fields",
    "mal_missing_auth_descriptor",
    "mal_rebootstrap_auth_descriptor",
    "provider_missing_state_descriptor",
    "provider_rebootstrap_auth_descriptor",
]
