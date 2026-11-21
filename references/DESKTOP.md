# Desktop app

The desktop app is MAL-Updater without Docker or Python. It is the same web UI and scheduler as the container, started by double-clicking an app and stopped by closing its window. Nothing is installed as a service, and nothing runs after you close it.

## Download

Each release has these files (the `latest/download` links always point to the newest release):

| System | File |
| --- | --- |
| Windows 10/11 (x64) | [`MAL-Updater-windows-x64.zip`](https://github.com/kklouzal/MAL-Updater/releases/latest/download/MAL-Updater-windows-x64.zip) |
| macOS on Apple silicon | [`MAL-Updater-macos-arm64.dmg`](https://github.com/kklouzal/MAL-Updater/releases/latest/download/MAL-Updater-macos-arm64.dmg) |
| macOS on Intel | [`MAL-Updater-macos-x64.dmg`](https://github.com/kklouzal/MAL-Updater/releases/latest/download/MAL-Updater-macos-x64.dmg) |
| Linux (x64, glibc 2.35 or newer) | [`MAL-Updater-linux-x64.tar.gz`](https://github.com/kklouzal/MAL-Updater/releases/latest/download/MAL-Updater-linux-x64.tar.gz) |

Builds of every `master` commit are also attached to the **desktop** workflow runs on GitHub Actions (sign-in required, kept for 30 days).

The builds are not code-signed, so the first start needs one extra step:

- **Windows**: unzip the folder anywhere and run `MAL-Updater.exe`. If SmartScreen says "Windows protected your PC", choose **More info → Run anyway**.
- **macOS**: open the `.dmg` and drag **MAL-Updater** to Applications. The first time, right-click the app and choose **Open**, then **Open** again (or allow it under System Settings → Privacy & Security). If macOS says the app "is damaged", run `xattr -dr com.apple.quarantine /Applications/MAL-Updater.app` once.
- **Linux**: `tar -xzf MAL-Updater-linux-x64.tar.gz` and run `MAL-Updater/MAL-Updater`. The control window needs a desktop session; without one the app runs in the terminal until Ctrl+C.

## Using it

Starting the app opens the dashboard in your browser at **http://127.0.0.1:8765/** and shows a small window with the scheduler status and **Open dashboard**, **Open data folder** and **Quit** buttons. Closing the window stops the web server, the scheduler and any task it is running. Starting the app again while it runs just opens the dashboard.

Setup is the same as for the container (README, *First-run setup*), with one difference: in your MAL API app, set the **App Redirect URL** to

```
http://127.0.0.1:8765/oauth/mal/callback
```

and open the app at `127.0.0.1`, not `localhost`, because MAL requires the callback to match exactly.

Syncing and recommendation refreshes run only while the app is open. When you start it again, overdue tasks run soon after start.

## Where data is kept

| System | Folder |
| --- | --- |
| Windows | `%LOCALAPPDATA%\MAL-Updater` |
| macOS | `~/Library/Application Support/MAL-Updater` |
| Linux | `$XDG_DATA_HOME/mal-updater` (default `~/.local/share/mal-updater`) |

The folder has the same layout as the container's `/data` volume (`config/`, `secrets/`, `data/`, `state/`, `cache/`), so you can move data between the container and the desktop app by copying it. The app's own log is `state/logs/desktop.log` (the previous run's is `desktop.log.1`); the scheduler log is `state/logs/service.log`. Uninstalling is deleting the app and this folder.

## Options

The executable accepts:

| Option | Meaning |
| --- | --- |
| `--port N` | Use another port (default 8765, or `MAL_UPDATER_DESKTOP_PORT`). The MAL redirect URL must use the same port. |
| `--data-dir PATH` | Keep data somewhere else (or set `MAL_UPDATER_DESKTOP_HOME`). |
| `--no-window` | Run without the control window; stop with Ctrl+C. |
| `--no-browser` | Do not open the dashboard on start. |

From a source checkout, `pip install .` provides the same launcher as `mal-updater-desktop`.

## Security

The app listens on 127.0.0.1 only, so other computers cannot reach it. Like the container, the dashboard has no sign-in, so any program or user on the same computer can use it; do not run it on a shared computer you do not trust. Host-header, CSRF and same-origin checks are the same as the container's (see `CONTAINERS.md`). On macOS and Linux the data folder is created private to your user (mode 700); on Windows it inherits your profile's permissions.

## How it works

- `mal_updater.desktop` points the runtime at the data folder (the same variables the container image sets), starts `ContainerRuntime` (the container's web server and scheduler supervisor) on 127.0.0.1 in a background thread, and runs the control window on the main thread.
- The scheduler starts each task as `<executable> -m mal_updater.cli …` and database compaction as `<executable> -I -c …`. A packaged build has no Python interpreter, so its executable accepts exactly these two forms itself (`frozen_python_dispatch`); like Python, an uncaught error prints a traceback and exits with status 1.
- Everything the scheduler starts is stopped with the app: on macOS and Linux the scheduler runs in its own process group, which the app signals on exit; on Windows the scheduler runs in a kill-on-close job object, so its tasks end with the app even after a crash. The browser and file manager the app opens are not part of either and keep running.
- File locks use `flock` on macOS and Linux and `LockFileEx` on Windows (`mal_updater.file_locks`). One difference: on Windows a lock cannot be handed to a child process, so if the scheduler itself is killed while a task runs, a restarted scheduler may start the same task again. Closing the app ends both, so this matters only after a crash.

## Building it yourself

Build on the system you are building for (PyInstaller does not cross-compile):

```bash
python -m pip install -c constraints/ci.txt ".[desktop]"
python -m PyInstaller --noconfirm packaging/mal-updater.spec
python scripts/smoke_desktop_build.py dist/MAL-Updater/MAL-Updater   # .exe on Windows; dist/MAL-Updater.app/Contents/MacOS/MAL-Updater on macOS
```

`.github/workflows/desktop.yml` does this for Windows, macOS (Apple silicon and Intel) and Linux on every push. On each OS it runs the cross-platform tests, builds the app, smoke-tests the build and uploads it. The smoke test starts the packaged app with stand-in MAL credentials pointed at a closed port, checks the pages, waits for a scheduler task to finish through the packaged executable, then checks that stopping the app leaves no process behind. Release tags attach the four files to the GitHub release (`release.yml`).
