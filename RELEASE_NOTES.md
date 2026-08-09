# MAL-Updater 0.2.0

This is the first supported **alpha container release** of MAL-Updater. The minor-version bump from the unpublished 0.1.4 development package reflects a materially expanded product surface: a hardened container runtime, authenticated setup/control UI, Compose deployment, lifecycle tooling, and a complete release supply chain.

## Install

Download and extract `mal-updater-v0.2.0.tar.gz`, copy `.env.example` to `.env`, then run:

```bash
docker compose up -d
docker compose logs mal-updater | grep first_run_setup_token
```

The bundle pins `ghcr.io/kklouzal/mal-updater:0.2.0`. Follow the bundled README for first-run claim and setup.

## Included artifacts

- Curated Compose bundle with environment template, documentation, license, release metadata, and internal checksums
- Python 0.2.0 wheel and source distribution
- Top-level SHA-256 checksums
- AMD64/ARM64 GHCR image with build provenance and SBOM
- Keyless Sigstore signature for the immutable image digest

## Support status

Alpha; intended for private LAN use or behind a trusted reverse proxy. Do not expose it directly to the public Internet. Container deployment is the supported end-user path. The source/user-systemd path remains advanced-use only, and its runtime must not be mixed with the container volume.

Back up before upgrading. Database rollback means restoring the backup created before migration, not running an older image against a newer database.
