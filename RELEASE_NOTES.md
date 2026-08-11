# MAL-Updater 0.2.6

This alpha patch restores graceful container stop and restart. Compose still drops every capability by default, but now adds `CAP_KILL` to the narrow startup set so root `tini` PID 1 can forward shutdown signals to the different-UID Python child. The application remains non-root (UID/GID 10001 by default), the root filesystem remains read-only, and no-new-privileges remains enabled. MAL synchronization, recommendation retention, and provider behavior are unchanged.

## Install

Download and extract `mal-updater-v0.2.6.tar.gz`, copy `.env.example` to `.env`, then run:

```bash
docker compose up -d
```

The bundle pins `ghcr.io/kklouzal/mal-updater:0.2.6`. Open the dashboard and complete MAL configuration/OAuth in Settings; automation starts automatically after setup is complete.

Before upgrading, create and verify a backup under `/data`, for example:

```bash
docker compose --profile tools run --rm cli backup /data/backups/pre-0.2.6.tar.gz
docker compose --profile tools run --rm cli backup-inspect --verify /data/backups/pre-0.2.6.tar.gz
```

After upgrading, `docker compose restart mal-updater` should stop within the configured grace period, return healthy, and produce no `[FATAL tini` or `Unexpected error when forwarding signal` log entry.

## Included artifacts

- Curated Compose bundle with environment template, documentation, license, release metadata, and internal checksums
- Python 0.2.6 wheel and source distribution
- Top-level SHA-256 checksums
- AMD64/ARM64 GHCR image with build provenance and SBOM
- Keyless Sigstore signature for the immutable image digest

## Support status

Alpha; intended for a trusted private LAN or behind a trusted reverse proxy. The dashboard has no user authentication, so every client that can reach it can change settings. Do not expose it directly to the public Internet. Mutations retain process-local synchronizer CSRF and same-origin/cross-site checks, but network reachability is the access boundary. Container deployment is the supported end-user path. The source/user-systemd path remains advanced-use only, and its runtime must not be mixed with the container volume.

Back up before upgrading. Database rollback means restoring the backup created before migration, not running an older image against a newer database.
