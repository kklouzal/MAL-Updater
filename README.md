# MAL-Updater

Local Crunchyroll → MyAnimeList sync and recommendation integration for OpenClaw on the Orin.

## Planned architecture

- Python orchestration worker
- Rust Crunchyroll adapter (`crunchyroll-rs`)
- SQLite state database
- MAL official OAuth/API client
- Safe dry-run sync before live writes

## Status

Scaffolding and design phase.
