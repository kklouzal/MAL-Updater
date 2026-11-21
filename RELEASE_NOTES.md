# MAL-Updater 0.3.0

This alpha release adds a standalone desktop app, MAL-only operation, per-season sync and **Continue a franchise** recommendations, redesigns the web pages, and fixes many Crunchyroll, HIDIVE and scheduler problems. `CHANGELOG.md` lists every change.

## Highlights

- **Desktop app (new):** run MAL-Updater on Windows, macOS or Linux without Docker or Python. Start the app, use the dashboard at `http://127.0.0.1:8765/`, and close its window to stop everything. See `references/DESKTOP.md`.
- **MAL-only mode:** Crunchyroll and HIDIVE are now optional; without them, recommendations come from your MAL list and MAL's recommendation graph.
- **Continue a franchise:** MAL sequels of titles you completed that are not on your list yet.
- **Per-season sync:** provider series spanning several seasons sync each season to its own MAL entry; unmatched seasons go to review (`season-mappings`).
- **Crunchyroll account linking** with a TV activation code, no password needed.
- **Redesigned dashboard, Debug and Settings pages**, with cover art, search and PDF export.

## Upgrade notes

- **Unattended MAL writes now default to off.** `service.task_execute_limits.sync_apply` changed from 8 to 0. If you relied on automatic MAL updates, set it to a positive number in Settings (or `settings.toml`) after upgrading. Manual `apply-sync` is unchanged.
- The database is migrated from schema 019 to 037 on first start. Migrations cannot be undone, so back up first.
- Health-history retention dropped from 90 to 30 days.

## Install

### Desktop app

Download the file for your system, then follow `references/DESKTOP.md` (the builds are not code-signed, so the first start needs one extra confirmation):

| System | File |
| --- | --- |
| Windows 10/11 (x64) | `MAL-Updater-windows-x64.zip` |
| macOS on Apple silicon | `MAL-Updater-macos-arm64.dmg` |
| macOS on Intel | `MAL-Updater-macos-x64.dmg` |
| Linux (x64, glibc 2.35 or newer) | `MAL-Updater-linux-x64.tar.gz` |

In your MAL API app, set the App Redirect URL to `http://127.0.0.1:8765/oauth/mal/callback`.

### Container

Download and extract `mal-updater-v0.3.0.tar.gz`, copy `.env.example` to `.env`, then run:

```bash
docker compose up -d
```

The bundle pins `ghcr.io/kklouzal/mal-updater:0.3.0`. Open the dashboard and complete MAL configuration/OAuth in Settings; automation starts automatically after setup is complete.

Before upgrading, create and verify a backup under `/data`, for example:

```bash
docker compose --profile tools run --rm cli backup /data/backups/pre-0.3.0.tar.gz
docker compose --profile tools run --rm cli backup-inspect --verify /data/backups/pre-0.3.0.tar.gz
```

After upgrading, wait for readiness and inspect the dashboard and service status.

## Included artifacts

- Desktop apps for Windows x64, macOS (Apple silicon and Intel) and Linux x64
- Curated Compose bundle with environment template, documentation, license, release metadata, and internal checksums
- Python 0.3.0 wheel and source distribution
- Top-level SHA-256 checksums and build provenance for every file
- AMD64/ARM64 GHCR image with build provenance and SBOM
- Keyless Sigstore signature for the immutable image digest

## Support status

Alpha; intended for a trusted private LAN, behind a trusted reverse proxy, or (desktop app) a computer you do not share with untrusted users. The dashboard has no user authentication, so every client that can reach it can change settings. Do not expose it directly to the public Internet. Mutations retain process-local synchronizer CSRF and same-origin/cross-site checks, but network reachability is the access boundary. The source/user-systemd path remains advanced-use only, and its runtime must not be mixed with the container volume.

Back up before upgrading. Database rollback means restoring the backup created before migration, not running an older image against a newer database.
