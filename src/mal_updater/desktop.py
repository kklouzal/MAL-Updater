"""Desktop launcher: run MAL-Updater as an ordinary app, without Docker.

It serves the same web UI and scheduler as the container on 127.0.0.1, keeps
data in the user's application-data folder and opens the dashboard in the
browser. A small window shows that it is running; closing it stops the web
server, the scheduler and any task the scheduler started. Nothing is installed
as a service. Without a display (or with --no-window) it runs until Ctrl+C.

Packaged builds (PyInstaller) have no Python interpreter, yet the scheduler
starts its tasks as ``sys.executable -m mal_updater.cli ...`` and database
compaction as ``sys.executable -I -c <script>``. A frozen executable therefore
also accepts those two command forms; see frozen_python_dispatch().
"""

from __future__ import annotations

import argparse
import json
import os
import signal
import subprocess
import sys
import threading
import webbrowser
from importlib import resources
from pathlib import Path
from typing import IO, Any, Callable, Sequence
from urllib.error import URLError
from urllib.request import urlopen

APP_NAME = "MAL-Updater"
DEFAULT_PORT = 8765
_HOST = "127.0.0.1"
_DISPATCH_MODULES = {"mal_updater.cli", "mal_updater.container_runtime"}


def frozen_python_dispatch(argv: Sequence[str]) -> int | None:
    """Run ``-m <module> ...`` / ``[-I] -c <code> ...`` the way Python would, or return None."""
    args = list(argv)
    while args and args[0] == "-I":
        args.pop(0)
    if len(args) >= 2 and args[0] == "-m":
        module, rest = args[1], args[2:]
        if module not in _DISPATCH_MODULES:
            print(f"{APP_NAME}: module {module!r} is not available in this build", file=sys.stderr)
            return 2
        sys.argv = [module, *rest]
        if module == "mal_updater.cli":
            from .cli import main as cli_main

            return int(cli_main() or 0)
        from .container_runtime import main as container_main

        return int(container_main(rest) or 0)
    if len(args) >= 2 and args[0] == "-c":
        sys.argv = ["-c", *args[2:]]
        exec(compile(args[1], "<string>", "exec"), {"__name__": "__main__"})
        return 0
    return None


def default_data_dir() -> Path:
    override = os.getenv("MAL_UPDATER_DESKTOP_HOME")
    if override:
        return Path(override).expanduser()
    if sys.platform == "win32":
        base = os.getenv("LOCALAPPDATA") or os.getenv("APPDATA")
        return Path(base) / APP_NAME if base else Path.home() / "AppData" / "Local" / APP_NAME
    if sys.platform == "darwin":
        return Path.home() / "Library" / "Application Support" / APP_NAME
    xdg = os.getenv("XDG_DATA_HOME")
    return (Path(xdg) if xdg else Path.home() / ".local" / "share") / "mal-updater"


def configure_environment(home: Path) -> None:
    """Point the runtime, and every task it starts, at the chosen data folder.

    The container does the same with /data. These take precedence over their
    aliases (MAL_UPDATER_RUNTIME_DIR, MAL_UPDATER_CONFIG), so the folder chosen
    here always wins.
    """
    home.mkdir(parents=True, exist_ok=True)
    if sys.platform != "win32":
        os.umask(0o077)
        home.chmod(0o700)
    os.environ["MAL_UPDATER_WORKSPACE_DIR"] = str(home)
    os.environ["MAL_UPDATER_RUNTIME_ROOT"] = str(home)
    os.environ["MAL_UPDATER_SETTINGS_PATH"] = str(home / "config" / "settings.toml")


def running_instance(port: int) -> bool:
    """True when a MAL-Updater already answers on this port."""
    try:
        with urlopen(f"http://{_HOST}:{port}/healthz", timeout=2) as response:
            payload = json.loads(response.read(64 * 1024))
    except (URLError, OSError, ValueError):
        return False
    return isinstance(payload, dict) and "automation_state" in payload


def _open_log(home: Path) -> IO[str]:
    log_dir = home / "state" / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / "desktop.log"
    if log_path.exists():
        os.replace(log_path, log_dir / "desktop.log.1")
    return log_path.open("w", encoding="utf-8", buffering=1)


class _ProcessTree:
    """Stops everything the scheduler started when the app exits.

    The container gets this from its PID namespace. POSIX: the scheduler runs in
    its own session, and its process group is signalled at exit. Windows: the app
    joins a kill-on-close job object, so every descendant ends with it, even
    after a crash.
    """

    def __init__(self, log: IO[str]) -> None:
        self.log = log
        self.groups: list[int] = []
        self._job: Any = None
        if sys.platform == "win32":
            try:
                self._job = _kill_on_close_job()
            except OSError as exc:  # e.g. started inside a job that forbids nesting
                print(json.dumps({"event": "desktop_job_object_unavailable", "error": str(exc)[:200]}), file=log, flush=True)

    def popen(self, command: list[str], **kwargs: Any) -> subprocess.Popen[bytes]:
        kwargs["start_new_session"] = sys.platform != "win32"
        process = subprocess.Popen(command, stdout=self.log, stderr=subprocess.STDOUT, **kwargs)
        if sys.platform != "win32":
            self.groups.append(process.pid)
        return process

    def stop(self) -> None:
        if sys.platform == "win32":
            return
        for group in self.groups:
            try:
                os.killpg(group, signal.SIGTERM)
            except (ProcessLookupError, PermissionError):
                pass
        self.groups.clear()


if sys.platform == "win32":

    def _kill_on_close_job() -> Any:
        import ctypes
        from ctypes import wintypes

        class _BasicLimits(ctypes.Structure):
            _fields_ = [
                ("PerProcessUserTimeLimit", ctypes.c_int64),
                ("PerJobUserTimeLimit", ctypes.c_int64),
                ("LimitFlags", wintypes.DWORD),
                ("MinimumWorkingSetSize", ctypes.c_size_t),
                ("MaximumWorkingSetSize", ctypes.c_size_t),
                ("ActiveProcessLimit", wintypes.DWORD),
                ("Affinity", ctypes.c_size_t),
                ("PriorityClass", wintypes.DWORD),
                ("SchedulingClass", wintypes.DWORD),
            ]

        class _ExtendedLimits(ctypes.Structure):
            _fields_ = [
                ("BasicLimitInformation", _BasicLimits),
                ("IoInfo", ctypes.c_uint64 * 6),
                ("ProcessMemoryLimit", ctypes.c_size_t),
                ("JobMemoryLimit", ctypes.c_size_t),
                ("PeakProcessMemoryUsed", ctypes.c_size_t),
                ("PeakJobMemoryUsed", ctypes.c_size_t),
            ]

        kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
        kernel32.CreateJobObjectW.restype = wintypes.HANDLE
        kernel32.CreateJobObjectW.argtypes = [ctypes.c_void_p, wintypes.LPCWSTR]
        kernel32.SetInformationJobObject.argtypes = [wintypes.HANDLE, ctypes.c_int, ctypes.c_void_p, wintypes.DWORD]
        kernel32.GetCurrentProcess.restype = wintypes.HANDLE
        kernel32.AssignProcessToJobObject.argtypes = [wintypes.HANDLE, wintypes.HANDLE]
        job = kernel32.CreateJobObjectW(None, None)
        if not job:
            raise ctypes.WinError(ctypes.get_last_error())
        limits = _ExtendedLimits()
        limits.BasicLimitInformation.LimitFlags = 0x2000  # JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE
        if not kernel32.SetInformationJobObject(job, 9, ctypes.byref(limits), ctypes.sizeof(limits)):  # JobObjectExtendedLimitInformation
            raise ctypes.WinError(ctypes.get_last_error())
        if not kernel32.AssignProcessToJobObject(job, kernel32.GetCurrentProcess()):
            raise ctypes.WinError(ctypes.get_last_error())
        return job  # the handle stays open for the life of the process


def _scheduler_summary(runtime: Any) -> str:
    from .container_runtime import _status_payload

    status = _status_payload(config=runtime.config, daemon=runtime.daemon_ref[0], store=runtime.store)
    if status["ready"]:
        return "Running: syncing and recommendations are active."
    if "scheduler_not_running" in status["automation_blockers"]:
        return "Starting the scheduler…"
    return "Waiting for setup: add your MAL client ID and sign in to MAL in Settings."


def _run_window(url: str, home: Path, runtime: Any, stop: threading.Event) -> bool:
    """Show the control window until it is closed; False if no display is available."""
    try:
        import tkinter as tk
        from tkinter import ttk

        root = tk.Tk()
    except Exception:
        return False
    root.title(APP_NAME)
    with resources.as_file(resources.files("mal_updater") / "assets" / "icon.png") as icon_path:
        icon = tk.PhotoImage(file=str(icon_path))
    root.iconphoto(True, icon.subsample(max(1, icon.width() // 128)))
    root.resizable(False, False)
    frame = ttk.Frame(root, padding=16)
    frame.grid()
    ttk.Label(frame, text="MAL-Updater is running", font=("TkDefaultFont", 13, "bold")).grid(column=0, row=0, columnspan=3, sticky="w")
    ttk.Label(frame, text=url).grid(column=0, row=1, columnspan=3, sticky="w", pady=(4, 0))
    status = ttk.Label(frame, text="", wraplength=380)
    status.grid(column=0, row=2, columnspan=3, sticky="w", pady=(8, 0))
    ttk.Label(frame, text="Closing this window stops MAL-Updater.", foreground="gray").grid(column=0, row=3, columnspan=3, sticky="w", pady=(8, 12))
    ttk.Button(frame, text="Open dashboard", command=lambda: webbrowser.open(url)).grid(column=0, row=4, sticky="w")
    ttk.Button(frame, text="Open data folder", command=lambda: _open_folder(home)).grid(column=1, row=4, padx=8)
    ttk.Button(frame, text="Quit", command=root.destroy).grid(column=2, row=4, sticky="e")
    root.protocol("WM_DELETE_WINDOW", root.destroy)

    def tick() -> None:
        if stop.is_set():
            root.destroy()
            return
        try:
            status.configure(text=_scheduler_summary(runtime))
        except Exception as exc:  # status is informational; the log has the details
            status.configure(text=f"Status unavailable ({type(exc).__name__}).")
        root.after(2000, tick)

    tick()
    root.mainloop()
    return True


def _open_folder(path: Path) -> None:
    if sys.platform == "win32":
        os.startfile(path)  # type: ignore[attr-defined]  # Windows-only API
    else:
        subprocess.Popen(["open" if sys.platform == "darwin" else "xdg-open", str(path)])


def _show_error(message: str, console: IO[str] | None) -> None:
    print(f"{APP_NAME}: {message}", file=sys.stderr)
    if console is not None:
        print(f"{APP_NAME}: {message}", file=console, flush=True)
    try:
        import tkinter as tk
        from tkinter import messagebox

        root = tk.Tk()
        root.withdraw()
        messagebox.showerror(APP_NAME, message)
        root.destroy()
    except Exception:
        pass


def _parse_args(argv: Sequence[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="mal-updater-desktop", description="Run MAL-Updater as a desktop app (stops when closed).")
    parser.add_argument("--port", type=int, default=int(os.getenv("MAL_UPDATER_DESKTOP_PORT", DEFAULT_PORT)), help=f"local port (default {DEFAULT_PORT}); the MAL app redirect URL must use the same port")
    parser.add_argument("--data-dir", type=Path, default=None, help="where settings, secrets and the database are kept")
    parser.add_argument("--no-window", action="store_true", help="run without the control window (stop with Ctrl+C)")
    parser.add_argument("--no-browser", action="store_true", help="do not open the dashboard on start")
    args = parser.parse_args(list(argv))
    if not 1 <= args.port <= 65535:
        parser.error("--port must be between 1 and 65535")
    return args


def main(argv: Sequence[str] | None = None, *, open_browser: Callable[[str], Any] = webbrowser.open) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    argv = [arg for arg in argv if not arg.startswith("-psn_")]  # old macOS Finder launches add a process serial number
    if getattr(sys, "frozen", False):
        dispatched = frozen_python_dispatch(argv)
        if dispatched is not None:
            return dispatched
    args = _parse_args(argv)
    home = (args.data_dir or default_data_dir()).expanduser().resolve()
    url = f"http://{_HOST}:{args.port}/"
    if running_instance(args.port):
        if not args.no_browser:
            open_browser(url)
        print(f"{APP_NAME} is already running at {url}")
        return 0
    configure_environment(home)
    log = _open_log(home)
    console = sys.stdout  # None in windowed builds
    sys.stdout = sys.stderr = log  # request and error logs go to the log file, not the terminal
    tree = _ProcessTree(log)

    from .container_runtime import ContainerRuntime

    try:
        runtime = ContainerRuntime(project_root=home, host=_HOST, port=args.port, popen=tree.popen)
    except OSError as exc:
        _show_error(f"Could not listen on {url} ({exc.strerror or exc}). Another program may be using port {args.port}; start with --port <number> to pick another.", console)
        return 1
    print(json.dumps({"event": "desktop_started", "url": url, "data_dir": str(home)}), file=log, flush=True)
    stop = threading.Event()

    def serve() -> None:
        try:
            runtime.serve()
        finally:
            stop.set()

    server_thread = threading.Thread(target=serve, name="mal-updater-server", daemon=True)
    server_thread.start()
    previous = {sig: signal.signal(sig, lambda *_: stop.set()) for sig in (signal.SIGTERM, signal.SIGINT)}
    try:
        if not args.no_browser:
            open_browser(url)
        if args.no_window or not _run_window(url, home, runtime, stop):
            if console is not None:
                print(f"{APP_NAME} is running at {url} (data: {home}). Press Ctrl+C to stop.", file=console, flush=True)
            while not stop.wait(0.5):
                pass
    finally:
        runtime.request_stop()
        server_thread.join(timeout=30)
        tree.stop()
        for sig, handler in previous.items():
            signal.signal(sig, handler)
        print(json.dumps({"event": "desktop_stopped"}), file=log, flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
