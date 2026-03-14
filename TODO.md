# TODO

## Immediate next build steps
- [x] Create Python project scaffold
- [x] Create Rust adapter scaffold
- [x] Define JSON contract between adapter and Python worker
- [x] Define SQLite schema
- [x] Decide config/secrets directory layout
- [ ] Wire Python config loader to parse `config/settings.toml`
- [ ] Add JSON schema validation before ingestion
- [ ] Add Python ingestion path for adapter snapshot -> SQLite
- [ ] Integrate `crunchyroll-rs` into the Rust adapter
- [ ] Register MAL app / complete OAuth flow locally

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
