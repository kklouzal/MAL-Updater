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
- [ ] New dubbed episode alerts
- [ ] New season alerts for completed shows
- [ ] Genre/studio/VA/taste feature ideas

## Later
- [ ] Build OpenClaw skill wrapper
- [ ] Periodic health-check/maintenance loop
- [ ] Reduce the remaining same-franchise low-margin residue without weakening explainability (OVA/recap/special discrimination, canonical-entry cues)
- [ ] Decompose combined Crunchyroll entries before treating aggregate episode counts as hard blockers for season/movie matching
