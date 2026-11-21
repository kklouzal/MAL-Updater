"""Smoke-test a packaged desktop build on the machine that built it.

usage: python scripts/smoke_desktop_build.py <path to the MAL-Updater executable>

Checks, against the frozen executable only (no source tree on the path):
1. it runs the CLI and inline code the way the scheduler invokes them;
2. it bundles sqlite3, curl_cffi and tkinter;
3. started headless with stand-in MAL credentials, it serves the pages, starts
   the scheduler, and the scheduler's tasks run through the same executable;
4. stopping it leaves no MAL-Updater process behind.
"""

from __future__ import annotations

import json
import os
import signal
import socket
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from urllib.error import URLError
from urllib.request import urlopen


def _fail(message: str) -> None:
    raise SystemExit(f"desktop smoke failed: {message}")


def _env() -> dict[str, str]:
    env = {key: value for key, value in os.environ.items() if not key.startswith("MAL_UPDATER_") and key not in {"PYTHONPATH", "PYTHONHOME"}}
    # Stand-in credentials must never reach MAL: point every MAL URL at a closed port.
    dead = "http://127.0.0.1:9"
    env.update(
        MAL_UPDATER_MAL_BASE_URL=dead,
        MAL_UPDATER_MAL_PUBLIC_BASE_URL=dead,
        MAL_UPDATER_MAL_TOKEN_URL=dead + "/token",
        MAL_UPDATER_MAL_AUTH_URL=dead + "/auth",
        MAL_UPDATER_SERVICE_STARTUP_GRACE_SECONDS="0",
    )
    return env


def _run(exe: Path, *args: str) -> str:
    result = subprocess.run([str(exe), *args], env=_env(), capture_output=True, text=True, timeout=180, check=False)
    if result.returncode != 0:
        _fail(f"{args[:2]} exited {result.returncode}: {result.stdout[-2000:]} {result.stderr[-2000:]}")
    return result.stdout


def _get(port: int, path: str) -> tuple[int, bytes]:
    with urlopen(f"http://127.0.0.1:{port}{path}", timeout=10) as response:
        return response.status, response.read()


def _free_port() -> int:
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        return int(probe.getsockname()[1])


def _wait(predicate, what: str, timeout: float) -> None:  # type: ignore[no-untyped-def]
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.5)
    _fail(f"timed out waiting for {what}")


def _status(port: int) -> dict[str, object]:
    try:
        return json.loads(_get(port, "/healthz")[1])
    except (URLError, OSError, ValueError):
        return {}


def _remaining_processes(exe: Path) -> list[str]:
    if os.name == "nt":
        listing = subprocess.run(["tasklist", "/FI", f"IMAGENAME eq {exe.name}", "/FO", "CSV", "/NH"], capture_output=True, text=True, check=False).stdout
        return [line for line in listing.splitlines() if exe.name.lower() in line.lower()]
    listing = subprocess.run(["ps", "-eo", "pid=,command="], capture_output=True, text=True, check=False).stdout
    return [line for line in listing.splitlines() if line.split(None, 1)[-1].startswith(str(exe))]


def main() -> int:
    if len(sys.argv) != 2:
        raise SystemExit(__doc__)
    exe = Path(sys.argv[1]).resolve()
    if not exe.is_file():
        _fail(f"{exe} is not a file")

    if "usage: mal-updater" not in _run(exe, "-m", "mal_updater.cli", "--help"):
        _fail("CLI help output missing")
    probe = _run(exe, "-I", "-c", "import json, sqlite3, curl_cffi, tkinter; print(json.dumps([sqlite3.sqlite_version, curl_cffi.__version__, tkinter.TkVersion]))")
    print("bundled sqlite/curl_cffi/tk:", probe.strip())

    with tempfile.TemporaryDirectory(prefix="mal-desktop-smoke-") as temp:
        home = Path(temp) / "home"
        secrets = home / "secrets"
        secrets.mkdir(parents=True)
        for name, value in {"mal_client_id.txt": "smoke-client", "mal_client_secret.txt": "smoke-secret", "mal_access_token.txt": "smoke-access", "mal_refresh_token.txt": "smoke-refresh"}.items():
            (secrets / name).write_text(value + "\n", encoding="utf-8")
        port = _free_port()
        app = subprocess.Popen([str(exe), "--no-window", "--no-browser", "--port", str(port), "--data-dir", str(home)], env=_env())
        try:
            _wait(lambda: bool(_status(port)), "the web server", 90)
            for path in ("/", "/settings", "/debug", "/api/dashboard"):
                code, _ = _get(port, path)
                if code != 200:
                    _fail(f"{path} returned {code}")
            _wait(lambda: _status(port).get("daemon_running") is True, "the scheduler", 90)
            service_log = home / "state" / "logs" / "service.log"
            _wait(lambda: service_log.exists() and "status=ok returncode=0" in service_log.read_text(encoding="utf-8", errors="replace"), "a scheduler task to finish", 240)
            print("scheduler tasks:", *[line for line in service_log.read_text(encoding="utf-8").splitlines() if "task=" in line][:6], sep="\n  ")
        finally:
            if os.name == "nt":
                app.terminate()  # closing the app's job object ends every process it started
            else:
                app.send_signal(signal.SIGTERM)
            try:
                app.wait(timeout=60)
            except subprocess.TimeoutExpired:
                app.kill()
                _fail("the app did not stop within 60 s")
        _wait(lambda: not _remaining_processes(exe), "every MAL-Updater process to exit", 30)
        if os.name != "nt" and app.returncode != 0:
            _fail(f"the app exited {app.returncode} after SIGTERM")
    print("desktop smoke passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
