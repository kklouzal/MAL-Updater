# MAL-Updater

Container-first trusted-LAN control plane for conservative provider-to-MyAnimeList sync and recommendations. It reads Crunchyroll and HIDIVE watch history, maps each provider season to its MyAnimeList entry, and fills in missing MAL progress without overwriting meaningful MAL data.

Crunchyroll and HIDIVE are optional. With only a MAL account connected (MAL-only mode), MAL-Updater still refreshes your MAL list, harvests MAL community recommendations, ranks recommendations from your list, and serves the dashboard, genre affinity and PDF export. What needs a provider is watch-history sync to MAL and the **Watchable now** check (verified streaming availability plus English dub).

## Quick start (container)

```bash
mkdir mal-updater && cd mal-updater
curl -O https://raw.githubusercontent.com/kklouzal/MAL-Updater/master/compose.yaml
curl -O https://raw.githubusercontent.com/kklouzal/MAL-Updater/master/.env.example
cp .env.example .env
# edit .env only for non-secret knobs such as image tag or port
docker compose up -d
```

Open **http://localhost/** (or `http://<lan-host>/` if you changed the port). The pages share one header:

- **Dashboard** (`/`): recommendations with MyAnimeList cover art, title search, and **Export PDF** (opens the browser print dialog; choose **Save as PDF** to keep links clickable).
- **Debug** (`/debug`): operational indicators, provider enrichment health, the public MAL userrecs crawl, and recent provider sync runs.
- **Settings** (`/settings`): credentials, MAL OAuth, Crunchyroll account linking, and bounded operational limits.

There is no installation claim, dashboard account, password, or sign-in flow.

The image defaults to `ghcr.io/kklouzal/mal-updater:0.2.9`. For local development builds use:

```bash
docker compose -f compose.yaml -f compose.build.yaml up -d --build
```

## First-run setup

1. Open Settings and add MAL app credentials. Saving credentials never tests or calls MAL.
2. Use **Test connection** explicitly when desired. Tests are user-triggered, rate-limited, timeout-bounded, and redact errors.
3. Start MAL OAuth from Settings. Configure the MAL app callback as `http://localhost/oauth/mal/callback` for local access, or the exact trusted LAN/proxy URL you use.
4. Optionally connect providers: **Link Crunchyroll account** (shows a TV activation code to approve at crunchyroll.com/activate; no password is stored) or Crunchyroll username/password, and HIDIVE username/password.
5. The scheduler starts automatically once the MAL client ID and MAL OAuth tokens are present, and stops if either is removed. It then fetches any connected providers, refreshes the MAL list, and builds recommendations.
6. Unattended MAL writes stay off until you opt in: set **Per-run execution bounds → sync_apply** in Settings to a positive number of writes per run (for example 8), then restart the container. Review a dry run first (see `references/OPERATIONS.md`).

MAL OAuth and provider secrets stay outside every UI response.

## Security and LAN deployment

Use this product only on a private trusted LAN or behind a trusted reverse proxy. Do not expose it directly to the WAN. Forwarded headers are honored only from proxies listed in `MAL_UPDATER_TRUSTED_PROXY_CIDRS`; otherwise Host/scheme come from the direct request. Because the dashboard has no user authentication, every LAN client that can reach it can view status and change settings.

Mutations require a process-local synchronizer CSRF token obtained through a same-origin request; cross-origin and cross-site browser requests are rejected. Trusted-host validation, 64 KiB JSON body limits, required JSON content types, CSP/security headers, connection-test rate limits, and secret redaction are enforced. See `references/CONTAINERS.md` for the full threat model.

Secrets live in the Docker volume under `/data/secrets` with restrictive permissions. The container runs non-root (UID/GID 10001 by default) with a read-only root filesystem, no-new-privileges, a bounded tmpfs `/tmp`, and all capabilities dropped except the narrow startup set.

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

Backups contain a SQLite-consistent DB copy, config, secrets, state, a manifest, and SHA-256 checksums. Keep backup destinations under `/data`; lifecycle work is staged on that volume rather than the bounded `/tmp`, and the destination subtree is excluded from its own archive. Restore verifies first, requires `--yes`, and creates an automatic pre-restore backup. Support bundles are redacted and do not include secret contents, usernames, database rows, tokens, or logs.

The application CLI (`references/cli-recipes.md`) runs inside the live container as the service user, for example `docker compose exec -u 10001:10001 mal-updater mal-updater health-check --format summary`.

## Upgrades and rollback

Before upgrading, create and verify a backup. Pin the new image tag in `.env`, then run `docker compose pull && docker compose up -d`. Startup performs migrations before readiness turns green. If startup fails, restore the pre-upgrade backup and pin the previous image tag. SQLite schema rollback requires restoring the backup taken before the upgrade, not downgrading the live DB in place. After an upgrade that adds the per-season sync migrations (032 and later), run a full provider refresh; see `references/OPERATIONS.md`.

## Health semantics

- `/healthz`: liveness; returns 200 when the web process can answer.
- `/readyz`: readiness; returns 200 only while required MAL setup is complete and the supervised scheduler is running. It returns 503 with blocked status while prerequisites are missing, startup is in progress, or the scheduler is degraded/restarting.

## Native user-systemd path

A source checkout can also run as a user-level systemd daemon (`references/AUTOMATION.md`; `SKILL.md` for OpenClaw hosts). The container is the supported end-user path; never mix the container volume with a host `.MAL-Updater` runtime.

## Troubleshooting

- Dashboard/settings unavailable: check `docker compose logs --tail=200 mal-updater` and confirm the requested hostname is loopback, private-IP, or listed in `MAL_UPDATER_TRUSTED_HOSTS`.
- Readiness degraded: complete MAL credentials/OAuth, or inspect logs if the scheduler is restarting.
- OAuth callback mismatch: update the MAL app callback to the exact browser URL plus `/oauth/mal/callback`.
- UI mutation reports CSRF failure after a restart: reload the page to obtain the new process-local token.
- Nothing is written to MAL: check that `sync_apply` is opted in (setup step 6), that no seasons wait in `season-mappings --unresolved`, and the `health-check` warnings.
- Restart logs contain `Unexpected error when forwarding signal: 'Operation not permitted'`: upgrade to 0.2.6 or later.

## Documentation map

| Topic | File |
| --- | --- |
| Container architecture, threat model, settings API | `references/CONTAINERS.md` |
| Scheduler lanes and defaults, housekeeping, sync and season behavior, MAL list/userrecs internals | `references/OPERATIONS.md` |
| Native user-systemd daemon and `service-status` | `references/AUTOMATION.md` |
| Command cookbook | `references/cli-recipes.md` |
| Native MAL OAuth | `references/MAL_OAUTH.md` |
| JSON contracts (snapshots, telemetry, command summaries) | `references/JSON_CONTRACT.md` |
| Configuration reference | `references/settings.toml.example`, `references/env.example` |
| Design decisions | `references/DECISIONS.md` |
| Repository layout | `ARCHITECTURE.md` |
| Quality gates | `references/QUALITY.md` |

## Release/support policy

MAL-Updater is alpha software. Semver tags build multi-arch GHCR images, wheels/sdists, checksums, provenance/SBOM, a keyless Sigstore signature, and a curated release bundle. Back up before every upgrade. See `CHANGELOG.md` for release history; `master` carries the changes listed under `Unreleased`.

Report bugs and feature requests at <https://github.com/kklouzal/MAL-Updater/issues>.
