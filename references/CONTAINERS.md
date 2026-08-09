# Container architecture and operations

## Product shape

One image runs a credential-free trusted-LAN HTTP control plane and the existing scheduler as a supervised child. Automation is persisted disabled and cannot start until the MAL client ID and OAuth tokens exist. `tini` is PID 1. The Python supervisor applies bounded exponential restart backoff and forwards shutdown.

## Persistent data and container boundary

The single `/data` volume contains `config`, `secrets`, `data`, `state`, and disposable `cache`. The entrypoint reconciles volume ownership as root, then drops to the configured UID/GID (default 10001). The root filesystem is read-only; `/tmp` is a bounded noexec tmpfs. Never mount the canonical host `.MAL-Updater` runtime into this product.

Legacy `/data/secrets/container_auth.json` files from v0.2.2 are ignored. No migration, deletion, or reset is required, and new installs do not create that file.

## Health

`/healthz` is liveness. `/readyz` is 200 only after required MAL setup is complete and, when enabled, the child daemon is alive. Dashboard, status, settings, and recommendation read surfaces open directly without an installation claim or user session.

## Base image and dependencies

Release/local builds default to the same Python 3.13.7 Bookworm digest. Python dependencies are constrained by `constraints/ci.txt`; Debian runtime packages are version-pinned. Multi-architecture release builds must override the base with a verified multi-architecture digest if the recorded default digest is platform-specific.

## Trusted-LAN control plane and settings

A fresh `/data` volume opens directly to the dashboard and Settings. The runtime emits only a normal `container_starting` event: there is no first-run token, claim endpoint, login/logout, dashboard password, session cookie, password change/reset, or admin-reset lifecycle command.

This is intentionally a **trusted-LAN-only** product. Every client able to reach the control plane can read status and mutate settings. Do not expose it directly to the Internet/WAN. Put network access control or an authenticating trusted reverse proxy in front if client-level access control is required.

Mutations use a process-local synchronizer CSRF token. The UI obtains it from `GET /api/csrf` through the browser same-origin policy, retains it only in memory, and supplies `X-CSRF-Token`. The server also rejects mismatched `Origin` and cross-site `Sec-Fetch-Site` values. The token rotates on process restart and is neither a user credential nor persisted installation state. JSON bodies are capped at 64 KiB, mutation routes require `application/json`, Host values must be loopback/private IP or explicitly trusted, and responses use CSP, frame, MIME-sniffing, permissions, and referrer protections.

`GET /api/settings` returns presence booleans, never secret values. `POST /api/secrets` accepts only the documented MAL/Crunchyroll/HIDIVE fields and supports explicit replacement/removal. Non-secret settings are allowlisted and atomically regenerated; arbitrary TOML is rejected. Secret files remain mode 0600 and the secrets directory remains restrictive. Audit JSONL records event names and changed field names, never values.

MAL OAuth starts at `POST /api/oauth/mal/start`. The generated callback is exactly `<current trusted host>/oauth/mal/callback`; loopback/private-IP Host values or explicit trusted hostnames only are accepted to prevent Host-header callback poisoning. State and PKCE verifier are single-use, expire after ten minutes, and tokens are atomically persisted by the existing credential helper.

Provider connection tests remain deliberate network actions: saving credentials performs no network I/O. Connection tests are explicit, CSRF-protected, per-client rate-limited, timeout-bounded actions; secret values and provider error details are not returned. MAL OAuth and Crunchyroll/HIDIVE provider authentication and token/session secret handling are unchanged by removal of dashboard authentication.

Automation is enabled with CSRF-protected `POST /api/daemon` (`{"enabled":true}`) only after the MAL client ID and MAL OAuth tokens are present. The in-process supervisor observes the persisted flag and starts/stops the existing daemon without Compose environment edits. Onboarding never enables MAL writes; existing conservative approval/write controls remain authoritative.

## Product container lifecycle commands

The release bundle is designed so `docker compose up -d` starts the pinned GHCR image on port 80 with a persistent `mal-updater-data` volume. Use `compose.build.yaml` only for local development builds. Lifecycle tools run through the `tools` profile and never require host `.MAL-Updater` or systemd services.

See README for backup, verify, restore, support-bundle, upgrade, rollback, and health commands.
