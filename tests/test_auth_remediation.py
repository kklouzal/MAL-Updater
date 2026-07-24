from __future__ import annotations

import unittest
from pathlib import Path

from mal_updater import bootstrap_guidance, health_report
from mal_updater.auth_remediation import (
    mal_missing_auth_descriptor,
    mal_rebootstrap_auth_descriptor,
    provider_missing_state_descriptor,
    provider_rebootstrap_auth_descriptor,
)

_REMEDIATION_DETAIL = (
    "stage fresh auth material because the existing refresh/auth token looks revoked, expired, or otherwise invalid"
)
_AUTH_ISSUE = {
    "auth_failure_kind": "invalid_grant",
    "auth_failure_label": "revoked or invalid refresh/auth token",
    "auth_remediation_kind": "refresh-token-invalidated",
    "auth_remediation_detail": _REMEDIATION_DETAIL,
    "reason": "MalApiError: invalid_grant from MAL token endpoint",
}


class AuthRemediationEquivalenceTests(unittest.TestCase):
    def _health_commands(self, **overrides: object) -> list[dict[str, object]]:
        options = {
            "crunchyroll_credentials_present": False,
            "crunchyroll_state_present": False,
            "hidive_credentials_present": False,
            "hidive_state_present": False,
            "mal_client_id_present": False,
            "mal_auth_present": False,
            "mal_auth_failure": None,
            "latest_sync_run": {},
            "latest_completed_sync_run": {},
            "latest_completed_age_seconds": 0.0,
            "stale_hours": 24.0,
            "crunchyroll_snapshot_output_path": Path(".MAL-Updater/cache/live-crunchyroll-snapshot.json"),
            "hidive_snapshot_output_path": Path(".MAL-Updater/cache/live-hidive-snapshot.json"),
            "provider_auth_failures": None,
        }
        options.update(overrides)
        return health_report._build_health_maintenance_commands(**options)  # type: ignore[arg-type]

    def assert_guidance_matches_health_command(
        self,
        health_command: dict[str, object],
        guidance: dict[str, object],
        *,
        expected_command_args: list[str],
    ) -> None:
        self.assertEqual(expected_command_args, health_command["command_args"])
        self.assertEqual(health_command["command"], guidance["next_command"])
        self.assertEqual(health_command["reason_code"], guidance["next_command_reason_code"])
        self.assertEqual(health_command["automation_safe"], guidance["next_command_automation_safe"])
        self.assertEqual(
            health_command["requires_auth_interaction"],
            guidance["next_command_requires_auth_interaction"],
        )
        if "auth_failure_kind" in health_command:
            self.assertEqual(health_command["auth_failure_kind"], guidance["next_command_auth_failure_kind"])
        else:
            self.assertNotIn("next_command_auth_failure_kind", guidance)
        if "auth_remediation_kind" in health_command:
            self.assertEqual(health_command["auth_remediation_kind"], guidance["next_command_auth_remediation_kind"])
        else:
            self.assertNotIn("next_command_auth_remediation_kind", guidance)

    def test_missing_mal_auth_equivalence(self) -> None:
        descriptor = mal_missing_auth_descriptor()
        commands = self._health_commands(mal_client_id_present=True, mal_auth_present=False)
        guidance = bootstrap_guidance._mal_bootstrap_guidance_status(
            client_id_present=True,
            oauth_present=False,
            auth_command=descriptor,
        )

        self.assertEqual(1, len(commands))
        self.assert_guidance_matches_health_command(commands[0], guidance, expected_command_args=["mal-auth-login"])
        self.assertEqual("Complete MAL OAuth and persist fresh access/refresh tokens", commands[0]["detail"])
        self.assertEqual(descriptor.bootstrap_operation_details(), guidance["details"])

    def test_missing_provider_auth_equivalence(self) -> None:
        for provider, health_label in (("crunchyroll", "Crunchyroll"), ("hidive", "HIDIVE")):
            with self.subTest(provider=provider):
                descriptor = provider_missing_state_descriptor(provider)
                commands = self._health_commands(
                    crunchyroll_credentials_present=provider == "crunchyroll",
                    crunchyroll_state_present=False,
                    hidive_credentials_present=provider == "hidive",
                    hidive_state_present=False,
                )
                guidance = bootstrap_guidance._provider_bootstrap_guidance_status(
                    provider_name=provider,
                    credentials_present=True,
                    session_present=False,
                    transport_ready=True,
                    bootstrap_command=descriptor,
                )

                self.assertEqual(1, len(commands))
                self.assert_guidance_matches_health_command(
                    commands[0],
                    guidance,
                    expected_command_args=["provider-auth-login", "--provider", provider],
                )
                self.assertEqual(
                    f"Re-bootstrap {health_label} auth state from the staged local credentials",
                    commands[0]["detail"],
                )
                self.assertEqual(descriptor.bootstrap_operation_details(), guidance["details"])

    def test_repeated_mal_auth_failure_equivalence(self) -> None:
        descriptor = mal_rebootstrap_auth_descriptor(_AUTH_ISSUE)
        commands = self._health_commands(
            mal_client_id_present=True,
            mal_auth_present=True,
            mal_auth_failure=_AUTH_ISSUE,
        )
        guidance = bootstrap_guidance._mal_bootstrap_guidance_status(
            client_id_present=True,
            oauth_present=True,
            auth_command=mal_missing_auth_descriptor(),
            auth_issue=_AUTH_ISSUE,
        )

        self.assertEqual(1, len(commands))
        self.assert_guidance_matches_health_command(commands[0], guidance, expected_command_args=["mal-auth-login"])
        self.assertEqual(descriptor.health_detail(), commands[0]["detail"])
        self.assertEqual(descriptor.bootstrap_operation_details(), guidance["details"])
        self.assertEqual(descriptor.bootstrap_remediation_fields()["remediation_kind"], guidance["remediation_kind"])

    def test_repeated_provider_auth_failure_equivalence(self) -> None:
        for provider in ("crunchyroll", "hidive"):
            with self.subTest(provider=provider):
                reason = "HTTP 401 from Crunchyroll" if provider == "crunchyroll" else "HIDIVE login failed: refresh token expired"
                auth_issue = {**_AUTH_ISSUE, "reason": reason}
                descriptor = provider_rebootstrap_auth_descriptor(provider, auth_issue)
                commands = self._health_commands(
                    crunchyroll_credentials_present=provider == "crunchyroll",
                    crunchyroll_state_present=provider == "crunchyroll",
                    hidive_credentials_present=provider == "hidive",
                    hidive_state_present=provider == "hidive",
                    provider_auth_failures={provider: auth_issue},
                )
                guidance = bootstrap_guidance._provider_bootstrap_guidance_status(
                    provider_name=provider,
                    credentials_present=True,
                    session_present=True,
                    transport_ready=True,
                    bootstrap_command=provider_missing_state_descriptor(provider),
                    auth_issue=auth_issue,
                )

                self.assertEqual(1, len(commands))
                self.assert_guidance_matches_health_command(
                    commands[0],
                    guidance,
                    expected_command_args=["provider-auth-login", "--provider", provider],
                )
                self.assertEqual(descriptor.health_detail(), commands[0]["detail"])
                self.assertEqual(descriptor.bootstrap_operation_details(), guidance["details"])
                self.assertEqual(descriptor.bootstrap_remediation_fields()["remediation_kind"], guidance["remediation_kind"])


if __name__ == "__main__":
    unittest.main()
