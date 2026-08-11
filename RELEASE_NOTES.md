# MAL-Updater 0.2.5

This alpha patch hardens synchronization and long-running storage. MAL writes now fail closed if the live remote row has changed since planning, while validated non-textual fields reconcile safely into the cache. Recommendation score snapshots now default to a 14-day operational horizon while preserving the newest 30 runs per kind; bounded per-pass deletion and remaining-eligible telemetry drain existing excess safely over successive scheduler passes. Container lifecycle backups now support large SQLite databases by staging on the persistent destination volume instead of the 64 MiB `/tmp` tmpfs; online consistency, atomic publication, checksums/manifests, verification, restore compatibility, and archive safety controls remain intact.

## Install

Download and extract `mal-updater-v0.2.5.tar.gz`, copy `.env.example` to `.env`, then run:

```bash
docker compose up -d
```

The bundle pins `ghcr.io/kklouzal/mal-updater:0.2.5`. Open the dashboard and complete MAL configuration/OAuth in Settings; automation starts automatically after setup is complete.

Before upgrading, create and verify a backup under `/data`, for example:

```bash
docker compose --profile tools run --rm cli backup /data/backups/pre-0.2.5.tar.gz
docker compose --profile tools run --rm cli backup-inspect --verify /data/backups/pre-0.2.5.tar.gz
```

## Included artifacts

- Curated Compose bundle with environment template, documentation, license, release metadata, and internal checksums
- Python 0.2.5 wheel and source distribution
- Top-level SHA-256 checksums
- AMD64/ARM64 GHCR image with build provenance and SBOM
- Keyless Sigstore signature for the immutable image digest

## Support status

Alpha; intended for a trusted private LAN or behind a trusted reverse proxy. The dashboard has no user authentication, so every client that can reach it can change settings. Do not expose it directly to the public Internet. Mutations retain process-local synchronizer CSRF and same-origin/cross-site checks, but network reachability is the access boundary. Container deployment is the supported end-user path. The source/user-systemd path remains advanced-use only, and its runtime must not be mixed with the container volume.

Back up before upgrading. Database rollback means restoring the backup created before migration, not running an older image against a newer database.
