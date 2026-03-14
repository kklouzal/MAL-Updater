# MAL-Updater Architecture

## Goal

Create a local Orin-hosted integration that:
- reads Crunchyroll watch history and watchlist
- syncs missing progress/status data into MyAnimeList
- never overwrites meaningful existing MAL data automatically
- records ambiguity/conflicts for user review
- later generates tailored anime recommendations
- eventually exposes the integration back into OpenClaw via a dedicated skill

## Core principles

- **One-way sync first:** Crunchyroll -> MAL
- **Missing-data first:** fill gaps, do not clobber existing MAL data
- **Safety before automation:** ambiguous mappings or conflicts go to review queue
- **Local-first:** run on the Orin, outbound-only
- **Python-only implementation:** Crunchyroll fetch, MAL sync logic, and persistence all live in one Python codebase
- **Recommendation quality comes after sync trustworthiness**

## Stack

### Python owns
- Crunchyroll credential bootstrap
- Crunchyroll session refresh and live fetches
- normalized snapshot generation
- orchestration
- sync policy
- MAL OAuth + API client
- SQLite state DB
- title mapping / normalization logic
- review queue generation
- recommendation engine
- future OpenClaw skill integration

## Initial sync policy

### Source of truth
Crunchyroll is the source of truth for what the user has watched.

### Sync target
MyAnimeList is the managed tracking layer.

### Allowed automatic writes (after dry-run phase)
- watched episode count
- status (`watching`, `completed`, etc.)
- dates if confidently inferable later
- score only if a future policy explicitly allows it and MAL score is missing

### Forbidden automatic writes
- decreasing watched episode count
- overwriting existing MAL score
- overwriting existing MAL dates unless MAL fields are empty
- resolving ambiguous mappings without review
- marking complete when data is weak/partial

## Crunchyroll completion semantics

If an episode is ~90-95% watched and the remainder is only credits, treat it as effectively watched.

This should be handled as a configurable completion threshold, likely defaulting to something like:
- `completion_threshold = 0.90`

## Review queue behavior

Any of these should be queued for user confirmation:
- ambiguous title mapping
- MAL existing data conflicts with Crunchyroll data
- uncertain completion state
- dub/sub uncertainty when relevant
- sequel/new season detection with uncertain ownership of prior season progress

## Recommendation priorities

### Highest priority
1. New season released for an anime the user completed
2. New dubbed episode released for a currently-followed in-progress anime

### Hard filters
- do not recommend anime without English dubs
- do not recommend newly released episodes unless English dub is available

### Future recommendation features
Potential signals:
- completion rate
- drop rate
- rewatch patterns
- genre affinity
- studio affinity
- English dub voice actor affinity if data is available
- sequel/continuation preference
- recency weighting
- explicit ratings

Recommendation sophistication should increase over time, but only after sync and mapping quality are stable.

## Future OpenClaw skill

Once the integration is stable, create a dedicated OpenClaw skill that can:
- query sync status
- surface conflicts/review items
- notify the user about high-priority new dubbed episodes / new seasons
- provide recommendations
- periodically health-check the integration

The skill should also help monitor drift, failures, and improvement opportunities over time.
