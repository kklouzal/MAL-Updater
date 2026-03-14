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

- fully verified live Crunchyroll snapshot against real staged credentials on this machine
- a transport path for the Rust adapter that survives Crunchyroll's current Cloudflare/anti-bot checks on this host
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
- the current Rust adapter still fails its refresh-token login attempt with `invalid_grant`

That strongly suggests the remaining blocker is the Rust-side transport / anti-bot path rather than just missing auth material or missing device id.

See `docs/CRUNCHYROLL_ADAPTER.md` for the concrete notes.

## Next practical milestone

Either move Crunchyroll fetching onto the proven Python impersonated transport, or teach the Rust side to use an impersonation-capable HTTP client. After that, run the first live snapshot and push it into Python ingestion.
