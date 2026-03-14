# Crunchyroll adapter notes

## What exists now

The Rust adapter now has a real local state/auth-material convention and a real `crunchyroll-rs` build path instead of only a placeholder CLI:

- `rustup` toolchain `1.88.0` is installed in the user account (no sudo required)
- the adapter crate is pinned locally via `rust/crunchyroll_adapter/rust-toolchain.toml`
- `auth status` reports the resolved Crunchyroll state paths and whether auth material is present
- `auth save-refresh-token` stages a Crunchyroll refresh token into the adapter state dir
- optional `device_id.txt` staging is supported alongside the refresh token
- `snapshot` now attempts a live `crunchyroll-rs` refresh-token login when auth material is present
- session metadata is tracked in `session.json`

Default state layout relative to the adapter working directory:

- `state/crunchyroll/<profile>/refresh_token.txt`
- `state/crunchyroll/<profile>/device_id.txt`
- `state/crunchyroll/<profile>/session.json`

These files are intended to stay local-only.

## Toolchain outcome

The blocker was confirmed directly instead of guessed:

- host distro Rust/Cargo remains `1.75.0`
- `crunchyroll-rs = 0.17.0` declares `edition = "2024"` and `rust-version = "1.88.0"`
- distro packages are therefore not the clean path for this repo on the Orin
- the practical no-root fix is `rustup` in `$HOME` plus a repo-local toolchain pin

That path is now in place and `cargo build` succeeds for the adapter with the newer toolchain.

## Current live behavior

`snapshot` now has three honest modes:

1. `auth_material_missing`
   - no staged refresh token exists yet
   - snapshot returns an empty but valid payload with clear state metadata
2. `auth_failed`
   - staged refresh-token login was attempted through `crunchyroll-rs`
   - the adapter records the error in `session.json` and returns an empty payload with the failure surfaced in `raw`
3. `ok`
   - refresh-token login succeeded
   - the adapter fetches account info, watch history, and watchlist, then normalizes them into the current JSON contract

## Important current limitation

For refresh-token login, Crunchyroll can be picky about the device identity used when the token was created.

Right now the adapter persists:

- refresh token
- optional device id
- a device-type hint in `session.json` (defaulting to `ANDROIDTV`)

That is enough to make real login attempts, but a token created with a different device identity may still fail until the adapter stores the exact full device identifier more explicitly.

So the adapter now does **real** login attempts, but success still depends on the staged auth material matching what Crunchyroll expects.

## Next step

1. stage a real Crunchyroll refresh token (and matching device id if available)
2. run `cargo run -- snapshot --contract-version 1.0`
3. inspect whether login succeeds or an honest `auth_failed` reason is returned
4. if needed, extend staged device-identifier handling beyond `device_id.txt`
5. once a real snapshot exists, push it through Python ingestion and tighten normalization details
