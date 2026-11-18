# Changelog

All notable product releases are documented here. MAL-Updater follows Semantic Versioning while it remains in alpha.

## [Unreleased]

### Added

- Added password-free Crunchyroll account linking with a TV activation code (Settings **Link Crunchyroll account** and `mal-updater crunchyroll-link`); a linked session renews itself and counts as fully configured.
- Added a `scheduled_task_failures_repeated` health warning for any scheduled task that fails three times in a row, so non-auth failures no longer stay visible only in the service log.
- Added 30-day retention for the recommendation decision ledger (migration 031 keeps rows younger than 7 days delete-protected) and for automatic pre-vacuum database backups.
- Added an accessible **Export PDF** control to the live dashboard using the browser print/save-to-PDF flow, with print-only report timing and a readable presentation that omits navigation and interactive filtering/visibility controls.
- Added bounded operational-limit controls to the trusted-LAN Settings page for hourly request budgets, per-run and pagination bounds, request pacing/retries, and learned-budget thresholds/backoff. Settings saves are strictly numeric/allowlisted, preserve unrelated TOML content and comments, and explicitly require a scheduler/container restart to apply.
- Added durable final-anchor validation for resumable public MAL user-recommendation crawls (migration 024), including bounded progress when only one page may be fetched per invocation.
- Added current-membership generations and account provenance for provider watchlists (migration 025), enabling conservative deactivation only after complete, account-proven full snapshots.
- Added staged Crunchyroll cold-bootstrap recovery and quarantine/reset controls that preserve the last-known-good boundary.
- Added per-season sync for provider series spanning several seasons: episodes record their canonical provider season (migration 032; Crunchyroll dubs collapse onto the original-language season), each season resolves to its own MAL entry (migration 033), and unresolved seasons go to review with an `unresolved_season_mappings` health warning and a `season-mappings` command to list/approve them.
- Added `apply-sync --provider` and `mal-refresh --force/--min-remaining-hours`.
- Added the in-season episode position (migration 034; Crunchyroll `sequence_number`, HIDIVE per-season number). Each MAL target uses the provider's episode numbers when they fit the entry (one MAL entry spanning several provider seasons) and the in-season position otherwise, so a season numbered 60-87 syncs as 1-28 (then split across MAL Part 1/Part 2).

### Changed

- Lowered the default health-history retention from 90 to 30 days and raised its per-run prune batch from 100 to 1000 files.
- Rotated the Crunchyroll OAuth client after the previous Android TV client was retired, and send the Android TV User-Agent that the token endpoint now requires.
- Hardened provider session, boundary, and snapshot JSON writes through the shared restrictive atomic persistence implementation.
- Aligned the secondary `recommend-maintain --mal-list-max-pages` default with the effective 10-page service budget.
- Tightened stale-running diagnostics so only an explicit released lease or elapsed timeout is terminal evidence.
- Expanded MAL/provider pacing, cache, ingestion, planner, migration, and validation coverage; nullable optional MAL detail containers remain accepted while missing/malformed requested fields fail closed.
- `apply-sync --limit` now bounds live revalidations/writes instead of the number of series inspected: every approved series is planned from cached MAL state and writes go most recently watched first, with the remainder reported as `deferred`. `apply-sync` exits non-zero when any item failed.
- Unattended `sync_apply` now applies each provider whose fetch succeeded since the previous apply instead of requiring every provider to succeed in the same scheduler pass.
- MAL token refresh renews only when the recorded access-token expiry is within 7 days (or unknown), and persists a rotated refresh token before the access token so an interrupted write cannot strand it.
- HIDIVE history items without `watchProgress` now count as finished episodes (HIDIVE clears the resume point once an episode is finished); partial plays keep their position.
- HIDIVE Continue Watching is read with `bpp=5`/`rpp=25`, which returns the whole bucket up to 25 items; it is reported partial only beyond that, and a hot snapshot with a complete Continue Watching bucket is healthy.
- Automatic season matches from older resolver versions are re-resolved (within the per-run search budget; the stored match keeps applying until then), and a provider season titled OVA/OAD/special/movie/recap never auto-matches a TV entry.
- The service MAL list refresh now requests `limit=1000` pages and prunes rows removed on MAL after a complete traversal; snapshot drift restarts in-run and is never terminal, and auth failures retry instead of quarantining.

### Fixed

- Fixed public MAL user-recommendation refreshes of already-published sources being rejected forever by the queue claim fence.
- Fixed watch progress of multi-season provider series being summed into one MAL entry (for example 88 episodes on a 16-episode season).
- Fixed unnumbered PVs, recaps, OVAs and movies counting as watched episodes, and watched counts exceeding the MAL episode count being written; episode numbers continuing across seasons are rebased when they exceed the MAL episode count, and a completion resting only on unobserved lower-numbered episodes goes to review.
- Fixed unattended `sync_apply` being skipped since late September because fetch lanes never all succeeded in one scheduler pass, and `apply-sync --limit` never reaching older backlog.
- Fixed Crunchyroll episodes flagged fully watched not completing when a playhead was also present.
- Fixed Crunchyroll specials ("SP1", "SP2") counting as episodes 1/2 of their season.
- Fixed HIDIVE Continue Watching positions of 0/1 seconds being read as 0%/100% watched, history paging (only the first 5 entries were read), and a hot sync boundary that could never be reached again; HIDIVE history items whose progress is unknown count as completed when the next episode of the same season was started at least 85% of the runtime later.
- Fixed the MAL list cache going stale after repeated drift quarantines caused by MAL flipping `main_picture` URLs; legacy drift quarantines resume automatically.
- Fixed Crunchyroll full refreshes failing validation when the offset-paged watchlist repeats a series across pages; repeats are dropped and the surface is marked partial.
- Fixed HIDIVE sync failing indefinitely once the saved refresh token expired (HIDIVE answers it with HTTP 404); an expired refresh token now triggers one credential re-login.
- Extended bounded health-history housekeeping to timestamped hourly health-check logs while preserving the configured recent floor and ignoring unrelated logs, backups, and runtime data.
- Derived health snapshot/log file-count review thresholds from the configured health cadence and retention window instead of fixed limits below the normal 90-day hourly volume.

### Release status

- These changes are unreleased and intentionally remain versioned as 0.2.9 in the dirty source tree. Existing 0.2.9 wheels, source archives, release bundles, and container images do **not** contain them; no publication or container-pin change is implied.

## [0.2.9] - 2026-08-12

### Changed

- Simplified the static and live recommendation table heading to `Title` and removed repeated provider-seen titles beside provider proof buttons, while retaining accessible proof links and the underlying evidence data.
- Updated the pinned container/bundle and Python package version to 0.2.9. Provider and MAL behavior remains unchanged, including credential gates, pacing/backoff, mapping and stale-write safeguards, and the unattended `sync_apply` limit of 8.

## [0.2.8] - 2026-08-12

### Changed

- Polished the static and live recommendation dashboards with scoped horizontal table scrolling, responsive spacing, sticky headers, clearer row highlighting, keyboard-visible focus states, accessible sort buttons, and explicit empty-section messages.
- Moved provider proof links beneath each title and removed diagnostic-only dub/provider-progress/MAL-watch columns from the presentation while retaining their row and JSON evidence for diagnostics.
- Updated the pinned container/bundle and Python package version to 0.2.8. Provider and MAL behavior remains unchanged, including credential gates, pacing/backoff, mapping and stale-write safeguards, and the unattended `sync_apply` limit of 8.

## [0.2.7] - 2026-08-12

### Added

- Added a low-frequency, threshold-gated `db_compaction` scheduler housekeeping lane that verifies a fresh backup, checks volume headroom, excludes repo-native DB writers, records skip/block/success evidence, and keeps hourly recommendation snapshot pruning logical-only.
- Added conservative recurring runtime housekeeping: daily age/count-bounded health-history retention, append-time size-bounded service-log rotation, and a weekly read-only retention inventory that keeps backup and ambiguous-runtime deletion human-gated.

### Changed

- Updated the pinned container/bundle and Python package version to 0.2.7. Conservative MAL/provider safeguards remain unchanged, including the unattended `sync_apply` limit of 8.

## [0.2.6] - 2026-08-11

### Fixed

- Restored graceful Compose stop/restart by retaining `CAP_KILL` for root `tini`, allowing PID 1 to forward signals to the different-UID Python child. The application still drops to UID/GID 10001 by default; all capabilities are still dropped before the narrow `CHOWN`, `DAC_OVERRIDE`, `KILL`, `SETGID`, and `SETUID` startup set is added, and the read-only root filesystem and no-new-privileges controls remain enabled.

### Changed

- Updated the pinned container/bundle and Python package version to 0.2.6. MAL synchronization, recommendation retention, and provider behavior are unchanged.

## [0.2.5] - 2026-08-11

### Fixed

- MAL synchronization now fails closed when the live remote row no longer matches the planned base row, and reconciles validated non-textual fields into the local cache without leaking raw network exception text.
- Recommendation score snapshots now default to a bounded 14-day operational horizon, preserve the newest 30 runs per kind, and expose capped per-pass deletion plus remaining-eligible telemetry so existing excess drains safely over successive passes.
- Container backups, verification, and restore pre-backups stage on the persistent destination volume rather than the bounded `/tmp` tmpfs. Large SQLite databases retain online-backup consistency, atomic archives, manifests/checksums, restore compatibility, symlink/traversal protections, and destination-recursion exclusion.

### Changed

- Updated the pinned container/bundle and Python package version to 0.2.5. Trusted-LAN and container hardening posture is unchanged.

## [0.2.4] - 2026-08-09

### Changed

- Made container automation always desired rather than user-toggleable. The scheduler starts automatically when the MAL client ID and OAuth tokens are present, stops safely if either prerequisite is lost, and starts again when prerequisites return.
- Removed the dashboard automation enable/disable button and `POST /api/daemon`. Legacy persisted `daemon_enabled:false` state is ignored so upgraded complete installations start without migration or another user action.
- Readiness now reports running only while the scheduler is actually alive and blocked/not-ready during missing prerequisites, startup, or restart backoff. Provider lanes remain independently credential-gated, and existing pacing/backoff, conservative MAL write, mapping ambiguity, CSRF/security-header, and secret-hygiene controls are unchanged.
- Updated the pinned container/bundle version to 0.2.4.

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
