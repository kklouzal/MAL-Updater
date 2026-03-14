# Decisions

## 2026-03-14 - Core integration direction

### Decision
Build MAL-Updater as a local Orin-hosted integration using:
- Python worker/orchestrator
- Rust Crunchyroll adapter
- SQLite state database
- official MAL OAuth + REST API

### Why
- Crunchyroll access is the brittle/unofficial half and benefits from a stronger client library boundary.
- Python is better for orchestration, mapping logic, sync policy, and future recommendation work.
- SQLite is sufficient and simple for local state.

## 2026-03-14 - Sync direction

### Decision
One-way sync first: Crunchyroll -> MyAnimeList.

### Why
- Crunchyroll is the behavioral source of truth for watched progress.
- MAL should be updated conservatively as the public-facing tracking layer.
- Two-way reconciliation adds unnecessary risk early.

## 2026-03-14 - Sync policy

### Decision
This is a **missing-data-first** system.

### Rules
- Do not decrease MAL progress automatically.
- Do not overwrite meaningful existing MAL data automatically.
- Do not auto-resolve ambiguous mappings.
- Queue conflicts for review.
- Dry-run before live writes.

## 2026-03-14 - Completion semantics

### Decision
Episodes that are ~90-95% complete due only to skipped credits should count as watched.

### Working default
- completion threshold target: `0.90`

## 2026-03-14 - Recommendation priorities

### Highest priority alerts
1. New season released for an anime the user has completed.
2. New dubbed episode released for an in-progress anime the user is currently following.

### Hard recommendation filter
- Do not recommend anime or new episodes that lack English dubs.

## 2026-03-14 - Repo posture

### Decision
Keep `MAL-Updater` private for now.

### Why
Early auth/integration work is exactly where accidental exposure risk is highest. Public release can be considered later after a deliberate sanitization pass.

## 2026-03-14 - Project memory habit

### Decision
Use `docs/` as project-specific durable memory.

### Why
OpenClaw memory is useful, but project-specific knowledge should live with the project repo.
