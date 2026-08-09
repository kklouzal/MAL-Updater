# Container architecture and operations

## Product shape

One image runs an authenticated HTTP control plane and the existing scheduler as a supervised child. Automation is persisted disabled and cannot start until admin claim, MAL client ID, and OAuth tokens exist. `tini` is PID 1. The Python supervisor applies bounded exponential restart backoff and forwards shutdown.

## Persistent data and container boundary

The single `/data` volume contains `config`, `secrets`, `data`, `state`, and disposable `cache`. The entrypoint reconciles volume ownership as root, then drops to the configured UID/GID (default 10001). The root filesystem is read-only; `/tmp` is a bounded noexec tmpfs. Never mount the canonical host `.MAL-Updater` runtime into this product.

## Health

`/healthz` is unauthenticated liveness. `/readyz` is 200 only after setup is complete and, when enabled, the child daemon is alive. All other operational/status surfaces require authentication.

## Base image and dependencies

Release/local builds default to the same Python 3.13.7 Bookworm digest. Python dependencies are constrained by `constraints/ci.txt`; Debian runtime packages are version-pinned. Multi-architecture release builds must override the base with a verified multi-architecture digest if the recorded default digest is platform-specific.

## Authenticated first run and settings

A fresh `/data` volume is not an open dashboard. The root page is the setup wizard and the runtime prints one `first_run_setup_token` event to container logs. Claim it with `POST /api/setup/claim` and a strong admin password. Only an scrypt hash and random server material are persisted in `/data/secrets/container_auth.json` (0600). The claim token is held only in process memory and becomes unusable after claim.

All dashboard, status, settings, OAuth and write APIs require the `HttpOnly; SameSite=Strict` server-side session cookie; only `/healthz`, `/readyz`, and the unclaimed setup status/claim surface are public. Mutations require the per-session `X-CSRF-Token` returned by login. Login and claim attempts are rate-limited. JSON bodies are capped at 64 KiB, mutation routes require `application/json`, and responses use CSP, frame, MIME-sniffing and referrer protections. Browser state is not placed in localStorage. Set `MAL_UPDATER_COOKIE_SECURE=true` when accessed exclusively through a trusted TLS reverse proxy. This application accepts private LAN HTTP, but **must not be exposed directly to the Internet/WAN**.

`GET /api/settings` returns presence booleans, never secret values. `POST /api/secrets` accepts only the documented MAL/Crunchyroll/HIDIVE fields and supports explicit replacement/removal. Non-secret settings are allowlisted and atomically regenerated; arbitrary TOML is rejected. Audit JSONL records event names and changed field names, never values.

MAL OAuth starts at `POST /api/oauth/mal/start`. The generated callback is exactly `<current trusted host>/oauth/mal/callback`; loopback/private-IP Host values only are accepted to prevent Host-header callback poisoning. State and PKCE verifier are single-use, expire after ten minutes, and tokens are atomically persisted by the existing credential helper. Provider connection tests are deliberately not automatic: saving credentials does no network I/O. Connection tests are explicit, authenticated, rate-limited, timeout-bounded actions; secret values and provider error details are not returned.

Automation is enabled with authenticated `POST /api/daemon` (`{"enabled":true}`) only after admin claim, MAL client ID, and MAL OAuth tokens are present. The in-process supervisor observes the persisted flag and starts/stops the existing daemon without Compose environment edits. Onboarding never enables MAL writes; existing conservative approval/write controls remain authoritative. `healthz` is liveness; `readyz` means setup is complete and, when enabled, the daemon is alive.

Password recovery/reset is intentionally offline so an unauthenticated HTTP endpoint cannot seize the installation: stop the service, back up `/data`, remove `/data/secrets/container_auth.json` using the volume-mounted CLI/admin container, and restart to obtain a new one-time claim token. This invalidates prior in-memory sessions. Release tooling should add a purpose-built `container-reset-admin` CLI before stable release.


## Product container lifecycle commands

The release bundle is designed so `docker compose up -d` starts the pinned GHCR image on port 80 with a persistent `mal-updater-data` volume. Use `compose.build.yaml` only for local development builds. Lifecycle tools run through the `tools` profile and never require host `.MAL-Updater` or systemd services.

See README for backup, verify, restore, admin-reset, support-bundle, upgrade, rollback, health, and first-run claim-token commands.
