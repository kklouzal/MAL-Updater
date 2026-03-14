# Crunchyroll adapter notes

## What exists now

The Rust adapter now has a real local state/auth-material convention instead of only a placeholder CLI:

- `auth status` reports the resolved Crunchyroll state paths and whether auth material is present
- `auth save-refresh-token` stages a Crunchyroll refresh token into the adapter state dir
- optional `device_id.txt` staging is supported alongside the refresh token
- `snapshot` now emits state-aware metadata showing whether Crunchyroll auth material has been staged
- session metadata is tracked in `session.json`

Default state layout relative to the adapter working directory:

- `state/crunchyroll/<profile>/refresh_token.txt`
- `state/crunchyroll/<profile>/device_id.txt`
- `state/crunchyroll/<profile>/session.json`

These files are intended to stay local-only.

## Why this was the first practical step

Before trying real watch-history extraction, the adapter needs a stable place for:

- Crunchyroll session material
- profile separation
- device-id persistence
- adapter-side status/debug metadata

That groundwork is now in place.

## Honest blocker

The next intended step is to bind the adapter to `crunchyroll-rs` and use the staged refresh token/device id for a real login + fetch path.

That is currently blocked on the host Rust toolchain:

- host `rustc`: `1.75.0`
- current `crunchyroll-rs` releases that are realistically relevant now require newer Cargo/Rust (`edition = 2024`, with recent versions declaring `rust_version` 1.85+)

I tried the direct path and confirmed the blocker instead of guessing:

- `crunchyroll-rs = 0.17.0` requires Rust 1.88 / edition 2024
- `crunchyroll-rs = 0.16.0` requires Rust 1.85 / edition 2024
- even older intermediate dependency attempts still hit edition-2024 manifests on this host

So the adapter does **not** claim live Crunchyroll auth or fetching yet.

## Next step once the blocker is cleared

1. upgrade/install a newer Rust toolchain on the host
2. add `crunchyroll-rs`
3. authenticate with the staged refresh token + device id
4. fetch the first real low-risk account/watch-history/watchlist payload
5. normalize that into the existing JSON contract for Python ingestion
