# MAL OAuth notes

This project is intentionally set up for a **local loopback OAuth flow** first.

## Current assumptions

- MAL OAuth is handled by the Python worker, not the Rust adapter.
- Redirect URI default: `http://127.0.0.1:8765/callback`
- PKCE is generated locally for each auth attempt.
- Secrets stay in `secrets/` (or env overrides), never in committed config.
- No browser automation is assumed yet.

## Secret file conventions

Default secret files under `secrets/`:

- `mal_client_id.txt`
- `mal_client_secret.txt`
- `mal_access_token.txt`
- `mal_refresh_token.txt`

Each file should contain exactly the relevant value with no extra metadata.
Environment variables can override every file path/value.

## Current CLI support

Generate an authorization URL and PKCE verifier:

```bash
PYTHONPATH=src python3 -m mal_updater.cli mal-auth-url
```

Machine-readable form:

```bash
PYTHONPATH=src python3 -m mal_updater.cli mal-auth-url --json
```

This is enough to prove config + URL generation wiring without pretending token exchange is already production-ready.

## Planned next step

Add a tiny loopback callback helper that:

1. binds to `127.0.0.1:<port>`
2. waits for a single `/callback?code=...`
3. exchanges the code for tokens
4. stores tokens back into `secrets/`
5. exits cleanly

That keeps the initial flow easy to reason about and avoids fake integrations.
