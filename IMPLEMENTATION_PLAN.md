# Implementation Plan

## Phase 0 - Repo / project setup
- Python project layout
- Rust adapter layout
- shared JSON contract between Rust and Python
- config / secrets layout
- SQLite bootstrap

## Phase 1 - Feasibility spike
- MAL OAuth works locally
- MAL API client can read and update test list entries
- Rust adapter can authenticate to Crunchyroll and fetch small watch sample
- normalized sample inserted into SQLite

## Phase 2 - Read-only ingestion
- ingest Crunchyroll watch history
- ingest Crunchyroll watchlist
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
