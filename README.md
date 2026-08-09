# MAL-Updater

Container-first private LAN control plane for conservative provider-to-MyAnimeList sync and recommendations.

## Quick start (container)

```bash
mkdir mal-updater && cd mal-updater
curl -O https://raw.githubusercontent.com/kklouzal/MAL-Updater/master/compose.yaml
curl -O https://raw.githubusercontent.com/kklouzal/MAL-Updater/master/.env.example
cp .env.example .env
# edit .env only for non-secret knobs such as image tag or port
docker compose up -d
docker compose logs mal-updater | grep first_run_setup_token
```

Open: **http://localhost/** (or `http://<lan-host>/` if you changed the port). Paste the one-time claim token from the logs, set an admin password, then complete Settings.

The image defaults to the pinned first fully verified container release (`ghcr.io/kklouzal/mal-updater:0.2.2`). For local development builds use:

```bash
docker compose -f compose.yaml -f compose.build.yaml up -d --build
```

## First-run setup

1. Claim the fresh install with the log token.
2. Sign in with the local admin password. Sessions are server-side and are invalidated by container restart or password change.
3. Add MAL app credentials in Settings. Saving credentials never tests or calls MAL.
4. Use **Test connection** explicitly when desired. Tests are user-triggered, rate-limited, timeout-bounded, and redact errors.
5. Start MAL OAuth from Settings. Configure the MAL app callback as `http://localhost/oauth/mal/callback` for local access, or the exact trusted LAN/proxy URL you use.
6. Add provider credentials and test them only when intentionally requested.
7. Enable automation explicitly. The daemon will not start until setup is complete.

## Security and LAN deployment

Use this product on a private LAN or behind a trusted reverse proxy. Do not expose it directly to the WAN. If a reverse proxy is used, only enable forwarded headers with an explicit trusted proxy allowlist; otherwise Host/scheme are taken from the direct request. Secrets live in the Docker volume under `/data/secrets` with restrictive permissions. The container runs non-root after startup, with read-only root filesystem, dropped capabilities, no-new-privileges, and a tmpfs `/tmp`.

## Container tools

All lifecycle operations use the Compose tools profile and the same persistent volume:

```bash
docker compose --profile tools run --rm cli version
docker compose --profile tools run --rm cli backup /data/state/backups/manual.tar.gz
docker compose --profile tools run --rm cli backup-inspect --verify /data/state/backups/manual.tar.gz
docker compose --profile tools run --rm cli restore --dry-run /data/state/backups/manual.tar.gz
docker compose --profile tools run --rm cli restore --yes /data/state/backups/manual.tar.gz
docker compose --profile tools run --rm cli admin-reset --yes
docker compose --profile tools run --rm cli support-bundle /data/state/support/support.tar.gz
```

Backups contain a SQLite-consistent DB copy, config, secrets, state, a manifest, and SHA-256 checksums. Restore verifies first, requires `--yes`, and creates an automatic pre-restore backup. Support bundles are deliberately redacted and do not include secret contents, usernames, database rows, tokens, or logs.

## Upgrades and rollback

Before upgrading, create and verify a backup. Pin the new image tag in `.env`, then run `docker compose pull && docker compose up -d`. Startup performs migrations before readiness turns green. If startup fails, restore the pre-upgrade backup and pin the previous image tag. SQLite schema rollback is limited: once a newer migration mutates a DB, older binaries may not understand it, so rollback means restoring the backup taken before upgrade, not downgrading the live DB in place.

## Health semantics

- `/healthz`: liveness; returns 200 when the web process can answer.
- `/readyz`: readiness; returns 200 only when setup is complete and enabled automation is running. It returns 503 during first-run setup or daemon degradation.

## Legacy/systemd advanced path

The historical user-systemd CLI path remains for advanced/manual installs from a source checkout. Container deployment is the supported product path for end users; do not mix the container volume with the host `.MAL-Updater` runtime.

## Troubleshooting

- Lost admin password: run `docker compose --profile tools run --rm cli admin-reset --yes`, restart, then claim again with the new log token.
- Setup token not visible: `docker compose logs --tail=200 mal-updater`.
- Readiness degraded: check dashboard status and container logs; automation may be disabled or the child daemon may be restarting.
- OAuth callback mismatch: update the MAL app callback to the exact browser URL plus `/oauth/mal/callback`.

## Release/support policy

MAL-Updater remains alpha software. Version 0.2.2 is the first fully verified supported container release; the legacy source/systemd path is advanced-use only. Semver tags build multi-arch GHCR images, wheels/sdists, checksums, provenance/SBOM, a keyless Sigstore signature, and a curated release bundle containing compose/env/docs. Patch releases are intended to be safe upgrades within the same minor line; backup before every upgrade. See `CHANGELOG.md` for release history and known limitations.
