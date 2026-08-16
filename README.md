# MAL-Updater

Container-first trusted-LAN control plane for conservative provider-to-MyAnimeList sync and recommendations.

Periodic durable positive evidence uses a deterministic 120-day +/-15-day refresh window with stale-while-revalidate read/display semantics. Negative/no-match claims retain shorter cadences, provider-title search remains identity-only, and stale evidence never authorizes MAL/provider writes or strict availability by itself. Operational retention, auth/retry/backoff, health thresholds, backups, and explicit deletion remain separate policies.

## Quick start (container)

```bash
mkdir mal-updater && cd mal-updater
curl -O https://raw.githubusercontent.com/kklouzal/MAL-Updater/master/compose.yaml
curl -O https://raw.githubusercontent.com/kklouzal/MAL-Updater/master/.env.example
cp .env.example .env
# edit .env only for non-secret knobs such as image tag or port
docker compose up -d
```

Open **http://localhost/** (or `http://<lan-host>/` if you changed the port) for the database-backed recommendations/operations dashboard. Configuration controls are at **`/settings`**; both pages open directly and link to each other. There is no installation claim, dashboard account, password, or sign-in flow.

The image defaults to `ghcr.io/kklouzal/mal-updater:0.2.9`. For local development builds use:

```bash
docker compose -f compose.yaml -f compose.build.yaml up -d --build
```

## First-run setup

1. Open Settings and add MAL app credentials. Saving credentials never tests or calls MAL.
2. Use **Test connection** explicitly when desired. Tests are user-triggered, rate-limited, timeout-bounded, and redact errors.
3. Start MAL OAuth from Settings. Configure the MAL app callback as `http://localhost/oauth/mal/callback` for local access, or the exact trusted LAN/proxy URL you use.
4. Add Crunchyroll/HIDIVE credentials and test them only when intentionally requested.
5. Automation starts automatically after the MAL client ID and MAL OAuth tokens are present. Until then it remains safely blocked; removing either prerequisite stops it, and restoring the prerequisite starts it again without a toggle.

Dashboard access is deliberately credential-free for this trusted-LAN-only product. MAL OAuth and provider authentication remain unchanged and their secrets remain outside the UI response surface.

## Security and LAN deployment

Use this product only on a private trusted LAN or behind a trusted reverse proxy. Do not expose it directly to the WAN. If a reverse proxy is used, only enable forwarded headers with an explicit trusted proxy allowlist; otherwise Host/scheme are taken from the direct request. Because the dashboard has no user authentication, every LAN client that can reach it can view status and change settings.

Mutations require a process-local synchronizer CSRF token obtained by the dashboard through a same-origin request; explicit cross-origin and cross-site browser requests are rejected. This is CSRF protection, not an access credential. Trusted-host validation, 64 KiB JSON body limits, required JSON content types, CSP/security headers, connection-test rate limits, and secret redaction remain enforced.

Secrets live in the Docker volume under `/data/secrets` with restrictive permissions. The container runs non-root after startup, with a read-only root filesystem, no-new-privileges, a bounded tmpfs `/tmp`, and all capabilities dropped except the narrow startup set. `CAP_KILL` lets root `tini` forward shutdown signals to its different-UID child; the Python application remains UID/GID 10001 by default.

## Container tools

All lifecycle operations use the Compose tools profile and the same persistent volume:

```bash
docker compose --profile tools run --rm cli version
docker compose --profile tools run --rm cli backup /data/backups/manual.tar.gz
docker compose --profile tools run --rm cli backup-inspect --verify /data/backups/manual.tar.gz
docker compose --profile tools run --rm cli restore --dry-run /data/backups/manual.tar.gz
docker compose --profile tools run --rm cli restore --yes /data/backups/manual.tar.gz
docker compose --profile tools run --rm cli support-bundle /data/state/support/support.tar.gz
```

Backups contain a SQLite-consistent DB copy, config, secrets, state, a manifest, and SHA-256 checksums. Keep production backup destinations under `/data`; lifecycle work is staged on that persistent destination volume rather than bounded `/tmp`, and the destination subtree is excluded from its own archive. Restore verifies first, requires `--yes`, and creates an automatic pre-restore backup. Support bundles are deliberately redacted and do not include secret contents, usernames, database rows, tokens, or logs.

## Upgrades and rollback

Before upgrading, create and verify a backup. Pin the new image tag in `.env`, then run `docker compose pull && docker compose up -d`. Startup performs migrations before readiness turns green. Existing legacy `/data/secrets/container_auth.json` files are ignored and need not be removed. If startup fails, restore the pre-upgrade backup and pin the previous image tag. SQLite schema rollback requires restoring the backup taken before upgrade, not downgrading the live DB in place.

## Health semantics

- `/healthz`: liveness; returns 200 when the web process can answer.
- `/readyz`: readiness; returns 200 only while required MAL setup is complete and the automatically supervised scheduler is running. It returns 503 with blocked status while prerequisites are missing, startup is in progress, or the scheduler is degraded/restarting.

## Legacy/systemd advanced path

The historical user-systemd CLI path remains for advanced/manual installs from a source checkout. Container deployment is the supported product path for end users; do not mix the container volume with the host `.MAL-Updater` runtime.

## Troubleshooting

- Dashboard/settings unavailable: check `docker compose logs --tail=200 mal-updater` and confirm the requested hostname is loopback, private-IP, or explicitly listed in `MAL_UPDATER_TRUSTED_HOSTS`.
- Readiness degraded: complete MAL credentials/OAuth, or inspect logs if the automatically supervised scheduler is restarting.
- OAuth callback mismatch: update the MAL app callback to the exact browser URL plus `/oauth/mal/callback`.
- UI mutation reports CSRF failure after a restart: reload the page to obtain the new process-local token.
- Restart logs contain `Unexpected error when forwarding signal: 'Operation not permitted'`: upgrade to 0.2.6 or later. A healthy restart lets `tini` forward TERM to the non-root Python child and emits no `tini` fatal error.

## Release/support policy

MAL-Updater remains alpha software. Version 0.2.9 finishes the recommendation dashboard title/provider-proof polish without changing provider/MAL behavior; version 0.2.8 introduced the broader dashboard presentation improvements. Provider/MAL authentication, conservative MAL-write controls, and the control-plane hardening described above remain intact. Semver tags build multi-arch GHCR images, wheels/sdists, checksums, provenance/SBOM, a keyless Sigstore signature, and a curated release bundle. Back up before every upgrade. See `CHANGELOG.md` for release history and known limitations.

### Complete public MAL user recommendations

`recommend-refresh-full-userrecs` uses a durable strict-priority source queue and staged generations, with monotonic selection-sequence fairness and token/revision-fenced expiring claims. `max_pages` counts actual network attempts, including final revalidation of every staged page; orchestrated calls disable inner retries and zero returns before queue or generation mutation. Drift, truncation, unsafe/conflicting pagination, cross-page overlap, stale claims, or insufficient validation budget preserve last-known-good edges. Empty publication requires dedicated structural MAL empty-state proof plus final validation. See `references/OPERATIONS.md`.

### Durable MAL user-list refresh

`mal-list-refresh` persists an account/query generation and a fair per-status traversal queue. Every parsed page, staged row set, opaque same-origin cursor, ordered fingerprint, and anchor commits atomically. Bounded runs resume the exact cursor; they publish/prune only after every requested partition has explicit terminal proof and page-1/final-boundary revalidation. MAL exposes offset/rank-relative pagination, not immutable snapshot tokens, so concurrent insertion/deletion/reorder is detected and failed closed rather than silently accepted; repeated drift is quarantined and the published list remains last-known-good. `--max-pages 0` performs no network call and no refresh-state mutation.

### Manual MAL suggestions shadow audit

`mal-updater recommend-suggestions-audit [--limit 100] [--output report.json]` makes one explicitly invoked OAuth `GET /v2/anime/suggestions` request and never follows pagination. It reads the initialized operational SQLite database in query-only mode, does not migrate or persist recommendation candidates, and emits only cohort sizes, overlap/novelty counts and ratios, and bounded aggregate quality distributions. The artifact deliberately excludes suggestion IDs, titles, ranks, cursors, and the raw personalized payload. A configured MAL OAuth access token and an existing current-schema operational database are required. This comparison lane is descriptive evidence only and is not coupled to production ranking or maintenance automation.

### Manual Crunchyroll recommendation shadow audit

`MAL_UPDATER_CRUNCHYROLL_RECOMMENDATION_SHADOW_ENABLED=true mal-updater crunchyroll-recommendation-shadow-audit --access-token-file TOKEN --account-id-file ACCOUNT --output report.json` explicitly invokes bounded GET-only native-recommendation and home-feed probes. Both ephemeral input files must be mode `0600`. The audit opens the operational SQLite database query-only, verifies its bytes are unchanged, persists no candidates, and emits aggregate counts only. Home-feed rows with both attribution fields absent or both set to Crunchyroll's empty-string sentinel are counted as generic; partial, mixed, or malformed attribution remains a fail-closed schema error. This manual diagnostic is not part of provider refresh, ranking, or maintenance automation.
