# TODO

## Immediate next build steps
- [x] Create Python project scaffold
- [x] Create Rust adapter scaffold
- [x] Define JSON contract between adapter and Python worker
- [x] Define SQLite schema
- [x] Decide config/secrets directory layout
- [x] Wire Python config loader to parse `config/settings.toml`
- [x] Add JSON schema validation before ingestion
- [x] Add Python ingestion path for adapter snapshot -> SQLite
- [x] Upgrade/install a newer Rust toolchain on the host so `crunchyroll-rs` can build
- [x] Integrate `crunchyroll-rs` into the Rust adapter
- [x] Add adapter-side Crunchyroll auth-material/state-dir conventions and CLI staging commands
- [x] Document the current Crunchyroll adapter auth-material flow and toolchain blocker
- [x] Verify a real Crunchyroll credential login + refresh-token bootstrap on this machine with staged credentials
- [ ] Get the first real Crunchyroll snapshot on this machine past the current Rust-side transport / Cloudflare blocker
- [ ] Decide whether to finish Crunchyroll fetches in Python (`curl_cffi` transport) or to rework the Rust HTTP client path
- [ ] Document the live Crunchyroll adapter auth/session flow once the first real snapshot implementation exists
- [x] Implement local MAL loopback OAuth flow + token persistence
- [ ] Register MAL app / complete live OAuth with real credentials

## Sync policy
- [x] Add durable approved-mapping persistence in `mal_series_mapping`
- [x] Add operator review workflow for mapping approval / preservation
- [x] Add approved-mappings-only dry-run gate for the future live executor
- [ ] Implement completion-threshold handling for credits-skipped episodes
- [ ] Implement missing-data-only merge rules
- [ ] Implement conflict / ambiguity review queue generation from ingestion/sync passes
- [ ] Replace ad-hoc review output with persisted `review_queue` entries once the review shape settles

## Recommendations
- [ ] New dubbed episode alerts
- [ ] New season alerts for completed shows
- [ ] Genre/studio/VA/taste feature ideas

## Later
- [ ] Build OpenClaw skill wrapper
- [ ] Periodic health-check/maintenance loop
