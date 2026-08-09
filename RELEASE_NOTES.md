# MAL-Updater 0.2.3

This alpha patch removes MAL-Updater's container dashboard claim/login/password surface for its trusted-LAN-only deployment model. The dashboard and Settings open directly, while MAL OAuth and Crunchyroll/HIDIVE provider authentication and secret handling remain intact. Legacy `container_auth.json` files are ignored.

## Install

Download and extract `mal-updater-v0.2.3.tar.gz`, copy `.env.example` to `.env`, then run:

```bash
docker compose up -d
```

The bundle pins `ghcr.io/kklouzal/mal-updater:0.2.3`. Open the dashboard and complete MAL configuration/OAuth in Settings; no claim or dashboard login is required.

## Included artifacts

- Curated Compose bundle with environment template, documentation, license, release metadata, and internal checksums
- Python 0.2.3 wheel and source distribution
- Top-level SHA-256 checksums
- AMD64/ARM64 GHCR image with build provenance and SBOM
- Keyless Sigstore signature for the immutable image digest

## Support status

Alpha; intended for a trusted private LAN or behind a trusted reverse proxy. The dashboard has no user authentication, so every client that can reach it can change settings. Do not expose it directly to the public Internet. Mutations retain process-local synchronizer CSRF and same-origin/cross-site checks, but network reachability is the access boundary. Container deployment is the supported end-user path. The source/user-systemd path remains advanced-use only, and its runtime must not be mixed with the container volume.

Back up before upgrading. Database rollback means restoring the backup created before migration, not running an older image against a newer database.
