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
- Crunchyroll adapter local auth-material staging now exists:
  - profile-scoped state dir layout
  - `refresh_token.txt` / optional `device_id.txt` conventions
  - `session.json` adapter-side state tracking
  - `auth status` and `auth save-refresh-token` commands
  - `snapshot` now reports state-aware adapter metadata instead of a pure blind scaffold

## Not implemented yet

- real Crunchyroll login/session handling
- real Crunchyroll snapshot fetching
- title mapping to MAL IDs
- dry-run MAL sync proposals
- guarded live MAL writes
- recommendation engine
- OpenClaw skill wrapper for the integration

## Current Crunchyroll blocker

The intended next Rust step is `crunchyroll-rs`, but the current host toolchain is too old for the relevant crate versions:

- host Rust/Cargo: `1.75.0`
- current `crunchyroll-rs` path now requires newer Rust/Cargo (`edition = 2024`, recent releases declaring `rust_version` 1.85+)

That blocker was verified directly during implementation rather than assumed. See `docs/CRUNCHYROLL_ADAPTER.md` for the concrete notes.

## Next practical milestone

After a Rust toolchain upgrade, bind the staged adapter auth material to `crunchyroll-rs`, perform the first real Crunchyroll login, and push the first authenticated snapshot into the Python ingestion pipeline.
