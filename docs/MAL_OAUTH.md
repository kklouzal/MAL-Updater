# MAL OAuth notes

This project uses a **local callback listener on the Orin** plus a browser-based user approval step.

## Current model

- MAL OAuth is handled by the Python worker, not the Rust adapter.
- The listener can bind to `0.0.0.0` so the Orin can receive callbacks from another machine on the LAN.
- The redirect URI registered in the MAL app must point to a **real reachable Orin address**, for example:
  - `http://192.168.1.117:8765/callback`
- `bind_host` may be `0.0.0.0`, but `redirect_host` must **not** be `0.0.0.0`.
- PKCE is generated locally for each auth attempt.
- MyAnimeList currently expects the **`plain`** PKCE method, not `S256`.
- Token exchange uses MAL's documented **HTTP Basic** auth scheme:
  - `client_id` as username
  - `client_secret` as password
  - empty password if no client secret exists
- Secrets stay in `secrets/` (or env overrides), never in committed config.

## Secret file conventions

Default secret files under `secrets/`:

- `mal_client_id.txt`
- `mal_client_secret.txt`
- `mal_access_token.txt`
- `mal_refresh_token.txt`

Each file should contain only the raw value.

## CLI support

Generate an authorization URL and PKCE verifier:

```bash
PYTHONPATH=src python3 -m mal_updater.cli mal-auth-url
```

Machine-readable form:

```bash
PYTHONPATH=src python3 -m mal_updater.cli mal-auth-url --json
```

Run the local callback login flow:

```bash
PYTHONPATH=src python3 -m mal_updater.cli mal-auth-login
```

Refresh a persisted token pair:

```bash
PYTHONPATH=src python3 -m mal_updater.cli mal-refresh
```

Confirm the current token works:

```bash
PYTHONPATH=src python3 -m mal_updater.cli mal-whoami
```

## Real login flow

`mal-auth-login` does the following:

1. generates a fresh PKCE verifier/challenge and random OAuth `state`
2. starts a one-shot HTTP listener on `<bind_host>:<port>`
3. prints the MAL authorization URL
4. user opens the URL in a browser that can reach the Orin callback host
5. MAL redirects to the configured callback URI
6. the worker validates `state`
7. the worker exchanges the code for tokens
8. tokens are persisted locally under `secrets/`
9. `GET /users/@me` is called to verify the token works

## Required conditions

- `mal_client_id.txt` must exist (or equivalent env override)
- the MAL app redirect URI must exactly match the configured reachable callback URI
- the user must complete the browser login and consent step

`mal_client_secret.txt` is optional for the current app type.

## Token persistence behavior

Tokens are stored in gitignored files under `secrets/`:

- access token → `mal_access_token.txt`
- refresh token → `mal_refresh_token.txt`

Writes are done atomically via temp-file + rename, and token files are chmod'd to `0600`.

## Lessons learned

- Console-only Orin means loopback-only browser auth is not enough; the callback listener must be LAN-reachable.
- MAL docs are authoritative here: `plain` PKCE and HTTP Basic token exchange matter.
- Browser auth path, LAN callback, token persistence, and `/users/@me` verification are now all proven working.
