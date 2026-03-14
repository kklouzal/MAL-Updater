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
- Crunchyroll Rust toolchain blocker is cleared:
  - user-local `rustup` toolchain `1.88.0` installed without sudo
  - repo-local `rust-toolchain.toml` pins the adapter to the required toolchain
  - `cargo build` now succeeds for the adapter on the Orin
- the adapter now attempts real Crunchyroll refresh-token login via `crunchyroll-rs`
- `snapshot` now honestly reports one of:
  - `auth_material_missing`
  - `auth_failed`
  - `ok`

## Not implemented yet

- fully verified live Crunchyroll snapshot against real staged credentials on this machine
- stronger device-identifier persistence beyond the current `device_id.txt` + device-type hint path
- title mapping to MAL IDs
- dry-run MAL sync proposals
- guarded live MAL writes
- recommendation engine
- OpenClaw skill wrapper for the integration

## Current Crunchyroll state

The adapter is no longer blocked on Rust/Cargo. The next real dependency is now auth material:

- a real staged Crunchyroll refresh token is required
- a matching device id may also be required for successful refresh-token login
- if login still fails, the next likely fix is storing the exact full device identifier used to mint the token

See `docs/CRUNCHYROLL_ADAPTER.md` for the concrete notes.

## Next practical milestone

Stage real Crunchyroll auth material, run the live adapter snapshot, and push the first authenticated snapshot into the Python ingestion pipeline.
