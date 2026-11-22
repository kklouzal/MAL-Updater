# MAL-Updater 0.4.0

This alpha release makes recommendations language-neutral. MAL-Updater no longer assumes you watch English dubs: subtitled shows and dubs in any language are recommended alike, and the dashboard's new **Audio** filter lets you narrow the list to the language you watch in. `CHANGELOG.md` lists every change.

## Highlights

- **Any language, sub or dub.** Watchable now needs only a verified match that is currently on Crunchyroll or HIDIVE. New-episode, new-season and backlog recommendations follow the version you actually watch, so subtitled viewers get them too.
- **Audio languages on every provider title.** Recommendations list the audio languages the provider reports (for your own shows, the language you watch in).
- **Audio filter.** Pick a language in the dashboard to hide titles that aren't available in it. Your browser remembers the choice, and titles whose languages weren't reported stay visible and say so.
- **Better matching for non-English dub titles.** Season titles such as "(Spanish Dub)" or "(Hindi Dub)" now match MAL entries like "(English Dub)" always did, and HIDIVE audio tags are read for many more languages.

## Upgrade notes

- **Migration 038** runs on first start. It removes the stored English-dub fields and accepts provider matches that were rejected only for lacking English audio, so Watchable now may grow. On a large database the migration can take a minute before the dashboard turns ready. Back up first.
- **Renamed:** the `new_dubbed_episode` recommendation type is now `new_episode`, and the webhook section `fresh_dubbed_episodes` is now `fresh_episodes`. If you set `fresh_dubbed_episodes` under `[openclaw.recommendations_webhook_section_limits]`, rename it. Recommendation JSON drops the English-dub fields and adds `audio_locales`.

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

Download and extract `mal-updater-v0.4.0.tar.gz`, copy `.env.example` to `.env`, then run:

```bash
docker compose up -d
```

The bundle pins `ghcr.io/kklouzal/mal-updater:0.4.0`. Open the dashboard and complete MAL configuration/OAuth in Settings; automation starts automatically after setup is complete.

Before upgrading, create and verify a backup under `/data`, for example:

```bash
docker compose --profile tools run --rm cli backup /data/backups/pre-0.4.0.tar.gz
docker compose --profile tools run --rm cli backup-inspect --verify /data/backups/pre-0.4.0.tar.gz
```

After upgrading, wait for readiness and inspect the dashboard and service status.

## Included artifacts

- Desktop apps for Windows x64, macOS (Apple silicon and Intel) and Linux x64
- Curated Compose bundle with environment template, documentation, license, release metadata, and internal checksums
- Python 0.4.0 wheel and source distribution
- Top-level SHA-256 checksums and build provenance for every file
- AMD64/ARM64 GHCR image with build provenance and SBOM
- Keyless Sigstore signature for the immutable image digest

## Support status

Alpha; intended for a trusted private LAN, behind a trusted reverse proxy, or (desktop app) a computer you do not share with untrusted users. The dashboard has no user authentication, so every client that can reach it can change settings. Do not expose it directly to the public Internet. Mutations retain process-local synchronizer CSRF and same-origin/cross-site checks, but network reachability is the access boundary. The source/user-systemd path remains advanced-use only, and its runtime must not be mixed with the container volume.

Back up before upgrading. Database rollback means restoring the backup created before migration, not running an older image against a newer database.
