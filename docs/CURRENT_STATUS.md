# Current Status

## Working today

- Python worker scaffold exists
- Rust adapter scaffold exists
- versioned JSON contract exists
- SQLite bootstrap + initial schema exist
- local config/secrets/data directory conventions exist
- MAL OAuth flow is working end-to-end:
  - auth URL generation
  - callback listener
  - LAN callback reception on the Orin
  - token exchange
  - token persistence
  - `/users/@me` verification

## Not implemented yet

- real Crunchyroll login/session handling
- real Crunchyroll snapshot fetching
- title mapping to MAL IDs
- dry-run MAL sync proposals
- guarded live MAL writes
- recommendation engine
- OpenClaw skill wrapper for the integration

## Next practical milestone

Integrate the first real Crunchyroll data path into the Rust adapter and push a real snapshot into the Python ingestion pipeline.
