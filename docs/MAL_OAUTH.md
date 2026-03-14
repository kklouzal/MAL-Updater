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

Run the local loopback login flow:

```bash
PYTHONPATH=src python3 -m mal_updater.cli mal-auth-login
```

Refresh an already-persisted token pair:

```bash
PYTHONPATH=src python3 -m mal_updater.cli mal-refresh
```

Confirm the current token works:

```bash
PYTHONPATH=src python3 -m mal_updater.cli mal-whoami
```

## Local login flow

`mal-auth-login` does the following for real:

1. generates a fresh PKCE verifier/challenge plus random OAuth `state`
2. binds a one-shot HTTP listener to `http://127.0.0.1:<port>/callback`
3. prints the MAL authorization URL for the user to open locally
4. waits for a single callback hit
5. validates `state`
6. exchanges the callback `code` for MAL tokens
7. writes returned tokens to the configured secret files under `secrets/`
8. calls `GET /users/@me` by default to confirm the token actually works

The helper is intentionally boring and local-first. No browser automation, no fake callbacks, no hidden secret storage.

## Credential expectations

Minimum local auth requirements:

- `mal_client_id.txt` must exist (or equivalent env override)
- the redirect URI registered with MAL must match `http://127.0.0.1:8765/callback` unless overridden in config
- the user still has to log in and approve the OAuth request in a browser

`mal_client_secret.txt` is optional in this scaffold. If present, it is included in token exchange/refresh requests. If absent, the client uses PKCE + `client_id` only.

## Token persistence behavior

Tokens are stored in separate gitignored files under `secrets/`:

- access token → `mal_access_token.txt`
- refresh token → `mal_refresh_token.txt`

Writes are done atomically via a temp file + rename, and token files are chmod'd to `0600`.

## Honest current boundary

The local MAL auth path is now wired end-to-end, but it still depends on two real-world things this repo cannot fake for you:

- a valid MAL application/client registration
- a human completing the browser login/consent step

That is the correct stopping point until real credentials are available.
