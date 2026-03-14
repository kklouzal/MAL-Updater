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
- the Python worker now also has `crunchyroll-auth-login`, which can mint and stage adapter-compatible auth material from local username/password secrets

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

Two important live facts are now verified on this host:

- `crunchyroll-rs` really does support direct credential login (`login_with_credentials`), so credential-based programmatic auth is not imaginary
- Crunchyroll currently puts a Cloudflare interstitial in front of plain Python `requests` to `auth/v1/token`, but browser-TLS impersonation via optional `curl_cffi` succeeds for both password-grant login and refresh-token exchange

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

The next blocker no longer looks like missing credentials alone.

What was observed directly:

- Python `crunchyroll-auth-login` can mint a real refresh token and stage it locally
- the same staged refresh token successfully refreshes again when sent through Python `curl_cffi` impersonated transport
- the current Rust adapter still fails the refresh-token login path with `invalid_grant`

That points more strongly at transport / anti-bot behavior on the current Rust path than at simple device-id staging gaps.

The adapter currently persists:

- refresh token
- device id
- a device-type hint in `session.json` (defaulting to `ANDROIDTV`)

That is enough to bootstrap honest auth material, but not yet enough to guarantee the Rust-side transport is accepted by Crunchyroll on this host.

## Next step

The practical pivot is now real:

1. keep the Python credential bootstrap path as the honest way to mint/stage refresh-token auth material
2. use the Python impersonated transport as the current live snapshot path via `crunchyroll-fetch-snapshot`
3. treat the Rust adapter as a secondary path until its transport layer is made impersonation-capable
4. build MAL mapping / write planning on top of the now-live Python-ingested Crunchyroll dataset

### Verified pivot outcome

A live Python-side snapshot now succeeds on this host and was ingested into SQLite with real data:

- `series_count=219`
- `progress_count=4311`
- `watchlist_count=10`

That means the remaining blocker is no longer "can we get real Crunchyroll data locally?" — we can. The remaining work is downstream mapping/sync logic and, separately, whether the Rust transport is worth salvaging.
