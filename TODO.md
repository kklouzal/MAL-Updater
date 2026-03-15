# TODO

## Immediate next build steps
- [x] Create Python project scaffold
- [x] Define normalized snapshot contract
- [x] Define SQLite schema
- [x] Decide config/secrets directory layout
- [x] Wire Python config loader to parse `config/settings.toml`
- [x] Add JSON schema validation before ingestion
- [x] Add Python ingestion path for snapshot -> SQLite
- [x] Verify a real Crunchyroll credential login + refresh-token bootstrap on this machine with staged credentials
- [x] Get the first real live Crunchyroll snapshot on this machine
- [x] Keep the working live Crunchyroll path in Python (`curl_cffi` transport when available)
- [x] Implement local MAL loopback OAuth flow + token persistence
- [x] Register MAL app / complete live OAuth with real credentials

## Sync policy
- [x] Add durable approved-mapping persistence in `mal_series_mapping`
- [x] Add operator review workflow for mapping approval / preservation
- [x] Add approved-mappings-only dry-run gate for the future live executor
- [x] Implement evidence-based credits-skipped completion handling (strict ratio + remaining-time window + later-episode evidence)
- [x] Implement missing-data-only merge rules
- [x] Implement conflict / ambiguity review queue generation from mapping/sync passes
- [x] Replace ad-hoc review output with persisted `review_queue` entries once the review shape settled enough for first use
- [x] Build the first guarded live MAL apply path for approved forward-safe proposals

## Recommendations
- [x] New dubbed episode alerts
- [x] New season alerts for completed shows
- [ ] Genre/studio/VA/taste feature ideas

## Later
- [ ] Re-stabilize fresh live Crunchyroll fetches after the current `watch-history` HTTP 401 regression (credential re-login succeeds, but full snapshot fetch still depends on `watch-history` surviving at least until the incremental overlap boundary is reached)
- [x] Add a conservative incremental Crunchyroll sync boundary/checkpoint so repeated runs can stop history/watchlist paging once already-synced overlap is detected
- [ ] Build OpenClaw skill wrapper
- [ ] Periodic health-check/maintenance loop
- [ ] Reduce the remaining same-franchise low-margin residue without weakening explainability (today: stronger single-episode special/OVA discrimination landed; still missing harder canonical-entry/side-story cases)
- [ ] Decompose combined Crunchyroll entries before treating aggregate episode counts as hard blockers for season/movie matching (today: aligned later-season matches can downgrade raw max-episode conflicts into `aggregated_episode_numbering_suspected` instead of a full blocker)
- [ ] Decide whether episode-title matching can be added from a trustworthy metadata source without pulling the project into brittle scraping or confidence theater

- [x] Add conservative Crunchyroll request spacing to the live fetch path
- [x] Add an exact-approved-only executor gate for unattended runs
- [x] Add a lock-protected exact-approved sync wrapper + checked-in user systemd timer units for a jittered ~8-hour-average local cadence
