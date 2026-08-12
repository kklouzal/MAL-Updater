# MAL-Updater 0.2.7

This alpha patch adds conservative recurring housekeeping for long-running installations. A low-frequency SQLite compaction lane runs only when both freelist thresholds and the minimum interval are satisfied, obtains the repo-native writer lease, verifies a fresh backup, and confirms volume headroom before `VACUUM`. Daily health-history pruning is age/count/batch bounded, service logs rotate at append time with bounded generations, and the weekly runtime-retention inventory remains read-only. Backup deletion and ambiguous-runtime deletion remain human-gated.

Provider and MAL behavior is unchanged. Existing credential gates, pacing/backoff, ambiguity handling, stale-write reconciliation, and the unattended `sync_apply` limit of 8 remain in force. Housekeeping does not make provider or MAL network writes.

## Install

Download and extract `mal-updater-v0.2.7.tar.gz`, copy `.env.example` to `.env`, then run:

```bash
docker compose up -d
```

The bundle pins `ghcr.io/kklouzal/mal-updater:0.2.7`. Open the dashboard and complete MAL configuration/OAuth in Settings; automation starts automatically after setup is complete.

Before upgrading, create and verify a backup under `/data`, for example:

```bash
docker compose --profile tools run --rm cli backup /data/backups/pre-0.2.7.tar.gz
docker compose --profile tools run --rm cli backup-inspect --verify /data/backups/pre-0.2.7.tar.gz
```

After upgrading, wait for readiness and inspect service status/housekeeping evidence. The compaction lane normally skips until its interval and both freelist thresholds are satisfied; a skip is not a failure.

## Included artifacts

- Curated Compose bundle with environment template, documentation, license, release metadata, and internal checksums
- Python 0.2.7 wheel and source distribution
- Top-level SHA-256 checksums
- AMD64/ARM64 GHCR image with build provenance and SBOM
- Keyless Sigstore signature for the immutable image digest

## Support status

Alpha; intended for a trusted private LAN or behind a trusted reverse proxy. The dashboard has no user authentication, so every client that can reach it can change settings. Do not expose it directly to the public Internet. Mutations retain process-local synchronizer CSRF and same-origin/cross-site checks, but network reachability is the access boundary. Container deployment is the supported end-user path. The source/user-systemd path remains advanced-use only, and its runtime must not be mixed with the container volume.

Back up before upgrading. Database rollback means restoring the backup created before migration, not running an older image against a newer database.
