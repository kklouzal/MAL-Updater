# Changelog

All notable product releases are documented here. MAL-Updater follows Semantic Versioning while it remains in alpha.

## [0.2.3] - 2026-08-09

### Changed

- Removed the container dashboard installation claim, first-run token log, login/logout, dashboard password, server-side session, password-change/reset, and `admin-reset` lifecycle paths. Fresh and upgraded installations now open the dashboard and Settings directly on the trusted LAN.
- Setup completion/readiness and the daemon-enable gate now depend on actual MAL client/OAuth material rather than an administrator claim. Legacy `container_auth.json` files are harmless and ignored.
- Replaced per-session CSRF with a process-local same-origin bootstrap token, plus Origin and Fetch Metadata cross-site checks. Trusted-host validation, JSON type/size bounds, security headers, secret redaction/modes, rate-limited connection tests, and conservative daemon/MAL-write gates remain.
- Updated the pinned container/bundle version to 0.2.3. MAL OAuth and Crunchyroll/HIDIVE provider authentication are unchanged.

## [0.2.2] - 2026-08-09

### Added

- First supported container product: private-LAN setup UI, persistent Compose deployment, health/readiness endpoints, and supervised automation.
- Multi-architecture GHCR publication for AMD64 and ARM64 with provenance, SBOM, and keyless Sigstore signing.
- Container-native backup, verification, restore, admin reset, and redacted support-bundle tools. (The admin-reset command is removed in 0.2.3 with dashboard authentication.)
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

Versions through 0.1.4 were pre-container-development package versions and were not published as supported GitHub/GHCR releases. The v0.2.0 workflow published a signed image but failed before creating a GitHub Release. Version 0.2.1 published a release whose top-level checksums used unusable build-directory prefixes. Version 0.2.2 was the first fully verified supported container release; use 0.2.3 for the credential-free trusted-LAN dashboard.
