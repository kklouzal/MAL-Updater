# Changelog

All notable product releases are documented here. MAL-Updater follows Semantic Versioning while it remains in alpha.

## [0.2.0] - 2026-08-09

### Added

- First supported container product: authenticated private-LAN setup UI, persistent Compose deployment, health/readiness endpoints, and supervised automation.
- Multi-architecture GHCR publication for AMD64 and ARM64 with provenance, SBOM, and keyless Sigstore signing.
- Container-native backup, verification, restore, admin reset, and redacted support-bundle tools.
- Curated release bundle, Python wheel and source distribution, and SHA-256 checksum manifests.

### Security and operations

- Non-root runtime with read-only root filesystem, dropped capabilities, no-new-privileges, bounded request bodies, CSRF protection, and restrictive secret storage.
- Explicit private-LAN/trusted-proxy support posture; direct public Internet exposure is unsupported.
- Provider connection checks and MAL OAuth remain explicit user actions. Fresh installs do not enable automation or MAL writes automatically.

### Compatibility

- The container is the supported end-user deployment path.
- The historical source/user-systemd workflow remains available for advanced users but must not share runtime state with a container deployment.

### Known alpha limitations

- Back up before every upgrade. Database rollback requires restoring a pre-upgrade backup rather than downgrading a migrated database in place.
- OAuth callback configuration must exactly match the trusted browser-facing URL.
- This release is alpha (`0.x`): configuration and operational interfaces may change in later minor releases.

## Earlier development builds

Versions through 0.1.4 were pre-container-development package versions and were not published as supported GitHub/GHCR releases.
