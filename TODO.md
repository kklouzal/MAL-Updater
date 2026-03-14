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
- [ ] Integrate `crunchyroll-rs` into the Rust adapter
- [ ] Document the Crunchyroll adapter auth/session flow once the first real implementation exists
- [x] Implement local MAL loopback OAuth flow + token persistence
- [ ] Register MAL app / complete live OAuth with real credentials

## Sync policy
- [ ] Implement completion-threshold handling for credits-skipped episodes
- [ ] Implement missing-data-only merge rules
- [ ] Implement conflict / ambiguity review queue generation from ingestion/sync passes

## Recommendations
- [ ] New dubbed episode alerts
- [ ] New season alerts for completed shows
- [ ] Genre/studio/VA/taste feature ideas

## Later
- [ ] Build OpenClaw skill wrapper
- [ ] Periodic health-check/maintenance loop
