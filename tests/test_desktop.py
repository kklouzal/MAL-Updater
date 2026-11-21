from __future__ import annotations

import io
import json
import os
import signal
import socket
import subprocess
import sys
import tempfile
import time
import unittest
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from unittest.mock import patch
from urllib.request import urlopen

from mal_updater import desktop

_SRC = str(Path(__file__).resolve().parents[1] / "src")


def _free_port() -> int:
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        return int(probe.getsockname()[1])


def _desktop_env() -> dict[str, str]:
    env = {key: value for key, value in os.environ.items() if not key.startswith("MAL_UPDATER_")}
    env["PYTHONPATH"] = os.pathsep.join(filter(None, [_SRC, os.environ.get("PYTHONPATH")]))
    # Keep the scheduler away from the real MAL API.
    env["MAL_UPDATER_MAL_BASE_URL"] = "http://127.0.0.1:9"
    return env


class FrozenDispatchTests(unittest.TestCase):
    def setUp(self) -> None:
        saved = list(sys.argv)
        self.addCleanup(setattr, sys, "argv", saved)

    def test_runs_the_cli_module_like_python_dash_m(self) -> None:
        with redirect_stdout(io.StringIO()) as out, self.assertRaises(SystemExit) as exit_info:
            desktop.frozen_python_dispatch(["-m", "mal_updater.cli", "--help"])
        self.assertEqual(0, exit_info.exception.code)
        self.assertIn("usage: mal-updater", out.getvalue())

    def test_runs_isolated_code_with_python_argv(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            target = Path(temp) / "out.json"
            code = "import json, sys; open(sys.argv[1], 'w').write(json.dumps(sys.argv))"
            self.assertEqual(0, desktop.frozen_python_dispatch(["-I", "-c", code, str(target), "extra"]))
            self.assertEqual(["-c", str(target), "extra"], json.loads(target.read_text()))

    def test_uncaught_errors_exit_1_with_a_traceback_like_python(self) -> None:
        with redirect_stderr(io.StringIO()) as err:
            self.assertEqual(1, desktop.frozen_python_dispatch(["-I", "-c", "raise RuntimeError('boom')"]))
        self.assertIn("RuntimeError: boom", err.getvalue())
        with patch("mal_updater.cli.main", side_effect=OSError("disk gone")), redirect_stderr(io.StringIO()) as err:
            self.assertEqual(1, desktop.frozen_python_dispatch(["-m", "mal_updater.cli", "health-check"]))
        self.assertIn("OSError: disk gone", err.getvalue())

    def test_refuses_unknown_modules_and_ignores_launcher_arguments(self) -> None:
        with patch("sys.stderr", io.StringIO()):
            self.assertEqual(2, desktop.frozen_python_dispatch(["-m", "http.server"]))
            self.assertEqual(2, desktop.frozen_python_dispatch(["-m", "mal_updater.container_runtime"]))
        self.assertIsNone(desktop.frozen_python_dispatch(["--port", "9000"]))
        self.assertIsNone(desktop.frozen_python_dispatch([]))


class DataDirectoryTests(unittest.TestCase):
    def test_explicit_home_wins(self) -> None:
        with patch.dict(os.environ, {"MAL_UPDATER_DESKTOP_HOME": "/srv/mal"}):
            self.assertEqual(Path("/srv/mal"), desktop.default_data_dir())

    def test_platform_defaults(self) -> None:
        with patch.dict(os.environ, {"LOCALAPPDATA": "C:/Users/a/AppData/Local"}), patch.object(sys, "platform", "win32"):
            os.environ.pop("MAL_UPDATER_DESKTOP_HOME", None)
            self.assertEqual(Path("C:/Users/a/AppData/Local/MAL-Updater"), desktop.default_data_dir())
        with patch.dict(os.environ, {}), patch.object(sys, "platform", "darwin"):
            os.environ.pop("MAL_UPDATER_DESKTOP_HOME", None)
            self.assertEqual(Path.home() / "Library" / "Application Support" / "MAL-Updater", desktop.default_data_dir())
        with patch.dict(os.environ, {"XDG_DATA_HOME": "/xdg"}), patch.object(sys, "platform", "linux"):
            os.environ.pop("MAL_UPDATER_DESKTOP_HOME", None)
            self.assertEqual(Path("/xdg/mal-updater"), desktop.default_data_dir())

    def test_configure_environment_points_every_task_at_the_data_folder(self) -> None:
        previous_umask = os.umask(0o022)
        os.umask(previous_umask)
        self.addCleanup(os.umask, previous_umask)
        with tempfile.TemporaryDirectory() as temp, patch.dict(os.environ, {"MAL_UPDATER_RUNTIME_ROOT": "/elsewhere"}):
            home = Path(temp) / "home"
            desktop.configure_environment(home)
            self.assertEqual(str(home), os.environ["MAL_UPDATER_RUNTIME_ROOT"])
            self.assertEqual(str(home), os.environ["MAL_UPDATER_WORKSPACE_DIR"])
            self.assertEqual(str(home / "config" / "settings.toml"), os.environ["MAL_UPDATER_SETTINGS_PATH"])
            if os.name != "nt":
                self.assertEqual(0o700, home.stat().st_mode & 0o777)


@unittest.skipIf(os.name == "nt", "process groups are POSIX; Windows uses the job object")
class ProcessTreeTests(unittest.TestCase):
    def test_signals_live_groups_and_forgets_ended_ones(self) -> None:
        with tempfile.TemporaryFile("w+", encoding="utf-8") as log:
            tree = desktop._ProcessTree(log)
            self.addCleanup(tree.stop)
            ended = tree.popen([sys.executable, "-c", "pass"])
            ended.wait(timeout=30)
            # A task left behind in a scheduler's group, as a crashed scheduler would leave it.
            leader = tree.popen([sys.executable, "-c", "import subprocess, sys; subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(60)'])"])
            leader.wait(timeout=30)
            deadline = time.monotonic() + 10
            while ended.pid in tree._groups and time.monotonic() < deadline:
                time.sleep(0.1)
            self.assertEqual([leader.pid], tree._groups)
            tree.stop()
            deadline = time.monotonic() + 10
            while desktop._group_exists(leader.pid) and time.monotonic() < deadline:
                time.sleep(0.1)
            self.assertFalse(desktop._group_exists(leader.pid), "the left-behind task survived stop()")


class DesktopProcessTests(unittest.TestCase):
    """Starts the launcher as a real process (headless), as a double-click would."""

    def setUp(self) -> None:
        temp = tempfile.TemporaryDirectory(prefix="mal-desktop-")
        self.addCleanup(temp.cleanup)
        self.home = Path(temp.name) / "home"
        self.port = _free_port()

    def _launch(self, *extra: str) -> subprocess.Popen[str]:
        command = [sys.executable, "-m", "mal_updater.desktop", "--no-window", "--no-browser", "--port", str(self.port), "--data-dir", str(self.home), *extra]
        process = subprocess.Popen(command, env=_desktop_env(), stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        self.addCleanup(lambda: process.poll() is None and (process.kill(), process.wait()))
        return process

    def _wait_ready(self, process: subprocess.Popen[str]) -> None:
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline:
            if process.poll() is not None:
                self.fail(f"launcher exited early: {process.stdout.read() if process.stdout else ''}")
            if desktop.running_instance(self.port):
                return
            time.sleep(0.2)
        self.fail("launcher did not become ready")

    def test_serves_the_app_reuses_a_running_instance_and_stops_cleanly(self) -> None:
        process = self._launch()
        self._wait_ready(process)
        for path in ("/", "/settings", "/api/status"):
            with urlopen(f"http://127.0.0.1:{self.port}{path}", timeout=10) as response:
                self.assertEqual(200, response.status)
        self.assertTrue((self.home / "data").is_dir())

        second = self._launch()
        output, _ = second.communicate(timeout=60)
        self.assertEqual(0, second.returncode)
        self.assertIn("already running", output)

        if os.name == "nt":
            process.terminate()  # Windows has no console signal for a detached child; the job object covers cleanup
            process.wait(timeout=30)
            return
        process.send_signal(signal.SIGTERM)
        self.assertEqual(0, process.wait(timeout=60))
        log = (self.home / "state" / "logs" / "desktop.log").read_text(encoding="utf-8")
        self.assertIn('"desktop_started"', log)
        self.assertIn('"desktop_stopped"', log)

    def test_reports_a_port_used_by_another_program(self) -> None:
        with socket.socket() as other:
            other.bind(("127.0.0.1", self.port))
            other.listen()
            process = self._launch()
            output, _ = process.communicate(timeout=60)
        self.assertEqual(1, process.returncode)
        self.assertIn(f"port {self.port}", output)


if __name__ == "__main__":
    unittest.main()
