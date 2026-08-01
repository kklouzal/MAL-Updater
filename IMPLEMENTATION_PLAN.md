# Implementation Plan

## Phase 0 - Repo / project setup
- [x] Python project layout
- [x] shared JSON contract
- [x] config / secrets layout
- [x] SQLite bootstrap
- [x] public-repo anonymization baseline for tracked code/references/tests/history

## Phase 1 - Feasibility spike
- MAL OAuth works locally
- MAL API client can read and update test list entries
- Python source-provider auth works locally for supported account-scoped surfaces
- normalized live provider samples insert into SQLite

## Phase 2 - Read-only ingestion
- ingest source-provider watch history
- ingest source-provider watchlist / continue-watching / custom-list surfaces where the provider supports them
- normalize title/episode/progress data
- persist raw observations + normalized watch state
- no MAL writes yet

## Phase 3 - Mapping layer
- search MAL for candidate matches
- confidence scoring
- manual override support
- franchise/season/special handling
- dub/sub-aware metadata enrichment when possible

## Phase 4 - Dry-run sync
- generate proposed MAL mutations
- log to review queue
- render summaries for inspection
- no automatic writes yet

## Phase 5 - Guarded live sync
- enable one-way missing-data sync only
- never reduce progress
- never overwrite meaningful MAL data
- respect review queue for conflicts/ambiguity

## Phase 6 - Recommendation engine
- new season alerts for completed shows
- new dubbed episode alerts for currently-followed in-progress shows
- local taste model and feature extraction
- future experimentation with smarter ranking / learning

## Phase 7 - OpenClaw skill
- query sync state
- surface conflicts for approval
- recommendations / alerts
- periodic health checks

## Detached remediation-chain status

The original phase outline is historical. Current source work through the remediated chain is still review/landing/release pending; do not treat this detached source state as a pushed release or live deployment until the maintainer explicitly lands it.
