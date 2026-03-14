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
- Python-side Crunchyroll credential bootstrap now exists:
  - reads local `secrets/crunchyroll_username.txt` and `secrets/crunchyroll_password.txt`
  - stages adapter-compatible `state/crunchyroll/<profile>/refresh_token.txt`
  - stages matching `device_id.txt`
  - updates `session.json`
  - exposes `crunchyroll-auth-login` in the CLI
- Python-side live Crunchyroll snapshot fetching now exists:
  - refreshes the staged Crunchyroll token via the Python transport
  - uses browser-TLS impersonation when optional `curl_cffi` is installed
  - fetches `GET /accounts/v1/me`, `GET /content/v2/{account_id}/watch-history`, and `GET /content/v2/discover/{account_id}/watchlist`
  - normalizes the live responses into the existing `1.0` snapshot contract
  - exposes `crunchyroll-fetch-snapshot` in the CLI, with optional direct ingestion
- Crunchyroll Rust toolchain blocker is cleared:
  - user-local `rustup` toolchain `1.88.0` installed without sudo
  - repo-local `rust-toolchain.toml` pins the adapter to the required toolchain
  - `cargo build` now succeeds for the adapter on the Orin
- the adapter now attempts real Crunchyroll refresh-token login via `crunchyroll-rs`
- direct credential login is confirmed to exist in `crunchyroll-rs` (`login_with_credentials`)
- on this host, plain Python `requests` to Crunchyroll auth gets a Cloudflare 403 interstitial, while browser-TLS impersonation via optional `curl_cffi` successfully completes credential login and refresh-token minting
- `snapshot` now honestly reports one of:
  - `auth_material_missing`
  - `auth_failed`
  - `ok`

## Not implemented yet

- a transport path for the Rust adapter that survives Crunchyroll's current Cloudflare/anti-bot checks on this host
- Crunchyroll -> MAL decisioning / mapping / guarded MAL writeback
- title mapping to MAL IDs
- dry-run MAL sync proposals
- guarded live MAL writes
- recommendation engine
- OpenClaw skill wrapper for the integration

## Current Crunchyroll state

The adapter is no longer blocked on Rust/Cargo, and auth material is now real.

What was verified on this machine:

- Crunchyroll username/password secrets are staged locally
- Python credential login can mint a real refresh token when using browser-like TLS impersonation (`curl_cffi`)
- the minted refresh token also refreshes successfully through the same Python impersonated transport
- the new Python fetch path can retrieve real live Crunchyroll data on this host and normalize it honestly
- a live snapshot was validated and ingested into SQLite with:
  - `series_count=219`
  - `progress_count=4311`
  - `watchlist_count=10`
- the current Rust adapter still fails its refresh-token login attempt with `invalid_grant`

That strongly suggests the remaining blocker is the Rust-side transport / anti-bot path rather than just missing auth material or missing device id.

See `docs/CRUNCHYROLL_ADAPTER.md` for the concrete notes.

## Next practical milestone

The shortest path is now clear: keep using the proven Python impersonated transport for live Crunchyroll fetches, and treat the Rust adapter as a secondary/future path until its transport is fixed. The next milestone is MAL mapping + guarded write planning on top of the now-live local Crunchyroll dataset.
