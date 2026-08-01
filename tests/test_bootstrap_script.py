from __future__ import annotations

import os
import pty
import shutil
import subprocess
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
BOOTSTRAP = REPO_ROOT / "scripts" / "bootstrap.sh"


def _write_stub_python(bin_dir: Path) -> Path:
    python_path = bin_dir / "python3"
    python_path.write_text(
        f"#!{sys.executable}\n"
        """from __future__ import annotations

import os
import stat
import sys
from pathlib import Path

args = sys.argv[1:]
log_path = Path(os.environ["BOOTSTRAP_STUB_LOG"])

def log(message: str) -> None:
    with log_path.open("a", encoding="utf-8") as handle:
        handle.write(message + "\\n")

if args[:2] == ["-m", "venv"]:
    venv = Path(args[2])
    target = venv / "bin" / "python"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(Path(__file__).read_text(encoding="utf-8"), encoding="utf-8")
    target.chmod(target.stat().st_mode | stat.S_IXUSR)
    log(f"venv {venv}")
    raise SystemExit(0)

if args[:2] == ["-m", "pip"]:
    log("pip " + " ".join(args[2:]))
    raise SystemExit(0)

if args and args[0] == "-" and len(args) == 2 and Path(args[1]).is_dir():
    repo_root = Path(args[1])
    runtime = Path(os.environ["BOOTSTRAP_STUB_RUNTIME"])
    settings = runtime / "config" / "settings.toml"
    secrets = runtime / "secrets"
    print(f"RUNTIME_ROOT={runtime}")
    print(f"SETTINGS_PATH={settings}")
    print(f"SECRETS_DIR={secrets}")
    print(f"MAL_CLIENT_ID_PATH={secrets / 'mal_client_id.txt'}")
    print(f"MAL_CLIENT_SECRET_PATH={secrets / 'mal_client_secret.txt'}")
    print(f"CRUNCHYROLL_USERNAME_PATH={secrets / 'crunchyroll_username.txt'}")
    print(f"CRUNCHYROLL_PASSWORD_PATH={secrets / 'crunchyroll_password.txt'}")
    print(f"HIDIVE_USERNAME_PATH={secrets / 'hidive_username.txt'}")
    print(f"HIDIVE_PASSWORD_PATH={secrets / 'hidive_password.txt'}")
    log(f"resolve_paths {repo_root}")
    raise SystemExit(0)

if args and args[0] == "-" and len(args) == 2:
    host = args[1].lower()
    raise SystemExit(0 if host in {"127.0.0.1", "localhost", "ip6-localhost", "ip6-loopback"} else 1)

if args and args[0] == "-" and len(args) == 5:
    settings_path = Path(args[1])
    settings_path.parent.mkdir(parents=True, exist_ok=True)
    settings_path.write_text("[mal]\\nredirect_host = \\\"127.0.0.1\\\"\\n", encoding="utf-8")
    log("update_mal_runtime_settings")
    raise SystemExit(0)

if args[:2] == ["-m", "mal_updater.cli"]:
    cli_args = args[2:]
    log("cli " + " ".join(cli_args))
    if cli_args[:2] == ["bootstrap-audit", "--summary"]:
        print("bootstrap_ok=true")
    elif cli_args[:2] == ["health-check", "--format"]:
        print("health_ok=true")
    elif cli_args[:1] == ["status"]:
        print("mal.redirect_uri=http://127.0.0.1:8765/callback")
    raise SystemExit(0)

log("python " + " ".join(args))
raise SystemExit(0)
""",
        encoding="utf-8",
    )
    python_path.chmod(0o755)
    return python_path


def _make_bootstrap_repo(tmp_path: Path) -> tuple[Path, Path]:
    repo_root = tmp_path / "bootstrap-repo"
    scripts_dir = repo_root / "scripts"
    scripts_dir.mkdir(parents=True)
    bootstrap_path = scripts_dir / "bootstrap.sh"
    shutil.copyfile(BOOTSTRAP, bootstrap_path)
    bootstrap_path.chmod(0o755)
    install_script = scripts_dir / "install_user_systemd_units.sh"
    install_script.write_text(
        """#!/usr/bin/env bash
set -euo pipefail
printf 'install-systemd %s\n' "$*" >> "$BOOTSTRAP_STUB_LOG"
""",
        encoding="utf-8",
    )
    install_script.chmod(0o755)
    return repo_root, bootstrap_path


def _bootstrap_env(tmp_path: Path) -> tuple[dict[str, str], Path, Path]:
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    python_path = _write_stub_python(bin_dir)
    systemctl_path = bin_dir / "systemctl"
    systemctl_path.write_text(
        """#!/usr/bin/env bash
set -euo pipefail
printf 'systemctl %s\n' "$*" >> "$BOOTSTRAP_STUB_LOG"
""",
        encoding="utf-8",
    )
    systemctl_path.chmod(0o755)
    runtime = tmp_path / "runtime"
    log_path = tmp_path / "bootstrap.log"
    env = {
        "PATH": f"{bin_dir}:{os.environ.get('PATH', '')}",
        "PYTHON_BIN": str(python_path),
        "MAL_UPDATER_BOOTSTRAP_VENV": str(tmp_path / "venv"),
        "MAL_UPDATER_BOOTSTRAP_INSTALL_DEPS": "no",
        "MAL_UPDATER_BOOTSTRAP_RUN_AUTH_STEPS": "no",
        "MAL_UPDATER_BOOTSTRAP_REDIRECT_HOST": "127.0.0.1",
        "BOOTSTRAP_STUB_RUNTIME": str(runtime),
        "BOOTSTRAP_STUB_LOG": str(log_path),
        "HOME": str(tmp_path / "home"),
        "XDG_CONFIG_HOME": str(tmp_path / "xdg-config"),
    }
    return env, runtime, log_path


def _assert_secrets_not_echoed(result: subprocess.CompletedProcess[str], *secret_values: str) -> None:
    output = result.stdout + result.stderr
    for secret_value in secret_values:
        assert secret_value not in output


def _run_bootstrap(tmp_path: Path, env: dict[str, str]) -> subprocess.CompletedProcess[str]:
    repo_root, bootstrap_path = _make_bootstrap_repo(tmp_path)
    return subprocess.run(
        ["bash", str(bootstrap_path)],
        cwd=repo_root,
        text=True,
        capture_output=True,
        check=False,
        env=env,
        timeout=30,
    )


def _run_bootstrap_with_tty_input(tmp_path: Path, env: dict[str, str], input_text: str) -> subprocess.CompletedProcess[str]:
    repo_root, bootstrap_path = _make_bootstrap_repo(tmp_path)
    master_fd, slave_fd = pty.openpty()
    try:
        process = subprocess.Popen(
            ["bash", str(bootstrap_path)],
            cwd=repo_root,
            stdin=slave_fd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            env=env,
        )
        os.close(slave_fd)
        slave_fd = -1
        os.write(master_fd, input_text.encode())
        stdout, stderr = process.communicate(timeout=30)
        return subprocess.CompletedProcess(process.args, process.returncode, stdout, stderr)
    finally:
        if slave_fd >= 0:
            os.close(slave_fd)
        if master_fd >= 0:
            os.close(master_fd)


def test_bootstrap_noninteractive_accepts_client_id_only_without_optional_mal_client_secret(tmp_path: Path) -> None:
    env, runtime, log_path = _bootstrap_env(tmp_path)
    env.update(
        {
            "MAL_UPDATER_BOOTSTRAP_PROVIDERS": "none",
            "MAL_UPDATER_MAL_CLIENT_ID": "client-id",
        }
    )

    result = _run_bootstrap(tmp_path, env)

    assert result.returncode == 0, result.stderr + result.stdout
    secrets = runtime / "secrets"
    assert (secrets / "mal_client_id.txt").read_text(encoding="utf-8") == "client-id\n"
    assert not (secrets / "mal_client_secret.txt").exists()
    assert not (secrets / "crunchyroll_username.txt").exists()
    assert not (secrets / "crunchyroll_password.txt").exists()
    assert not (secrets / "hidive_username.txt").exists()
    assert not (secrets / "hidive_password.txt").exists()
    assert "Source provider bootstraps selected: none" in result.stdout
    assert "Optional MAL client secret" in result.stdout
    assert "provider-auth-login" not in log_path.read_text(encoding="utf-8")


def test_bootstrap_stages_environment_provided_optional_mal_client_secret(tmp_path: Path) -> None:
    env, runtime, _log_path = _bootstrap_env(tmp_path)
    env.update(
        {
            "MAL_UPDATER_BOOTSTRAP_PROVIDERS": "none",
            "MAL_UPDATER_MAL_CLIENT_ID": "client-id",
            "MAL_UPDATER_MAL_CLIENT_SECRET": "env-client-secret",
        }
    )

    result = _run_bootstrap(tmp_path, env)

    assert result.returncode == 0, result.stderr + result.stdout
    secrets = runtime / "secrets"
    assert (secrets / "mal_client_id.txt").read_text(encoding="utf-8") == "client-id\n"
    assert (secrets / "mal_client_secret.txt").read_text(encoding="utf-8") == "env-client-secret\n"
    assert "Staged MAL client secret" in result.stdout
    _assert_secrets_not_echoed(result, "env-client-secret")


def test_bootstrap_noninteractive_infers_existing_provider_and_preserves_secrets(tmp_path: Path) -> None:
    env, runtime, log_path = _bootstrap_env(tmp_path)
    secrets = runtime / "secrets"
    secrets.mkdir(parents=True)
    (secrets / "mal_client_id.txt").write_text("existing-client\n", encoding="utf-8")
    (secrets / "mal_client_secret.txt").write_text("existing-secret\n", encoding="utf-8")
    (secrets / "hidive_username.txt").write_text("existing-hidive-user\n", encoding="utf-8")
    (secrets / "hidive_password.txt").write_text("existing-hidive-secret\n", encoding="utf-8")

    result = _run_bootstrap(tmp_path, env)

    assert result.returncode == 0, result.stderr + result.stdout
    assert (secrets / "mal_client_id.txt").read_text(encoding="utf-8") == "existing-client\n"
    assert (secrets / "mal_client_secret.txt").read_text(encoding="utf-8") == "existing-secret\n"
    assert (secrets / "hidive_username.txt").read_text(encoding="utf-8") == "existing-hidive-user\n"
    assert (secrets / "hidive_password.txt").read_text(encoding="utf-8") == "existing-hidive-secret\n"
    assert "Source provider bootstraps selected: hidive" in result.stdout
    assert "Keeping existing value without showing it" in result.stdout
    assert "Skipping Crunchyroll credential prompts/auth" in result.stdout
    _assert_secrets_not_echoed(result, "existing-secret", "existing-hidive-secret")
    assert "provider-auth-login --provider" not in log_path.read_text(encoding="utf-8")


def test_bootstrap_interactive_allows_skipping_optional_mal_client_secret(tmp_path: Path) -> None:
    env, runtime, _log_path = _bootstrap_env(tmp_path)
    env.update(
        {
            "MAL_UPDATER_BOOTSTRAP_INSTALL_DEPS": "no",
            "MAL_UPDATER_BOOTSTRAP_PROVIDERS": "none",
        }
    )

    result = _run_bootstrap_with_tty_input(
        tmp_path,
        env,
        "\n"  # skip dependency install
        "interactive-client-id\n"
        "\n"  # skip optional MAL client secret
        "n\n",  # do not start service
    )

    assert result.returncode == 0, result.stderr + result.stdout
    secrets = runtime / "secrets"
    assert (secrets / "mal_client_id.txt").read_text(encoding="utf-8") == "interactive-client-id\n"
    assert not (secrets / "mal_client_secret.txt").exists()
    assert "Enter MAL client secret (optional; press Enter to skip):" in result.stderr
    assert "Skipping optional MAL client secret" in result.stdout
    _assert_secrets_not_echoed(result, "interactive-client-id")


def test_bootstrap_can_opt_into_selected_provider_auth_without_other_provider(tmp_path: Path) -> None:
    env, runtime, log_path = _bootstrap_env(tmp_path)
    env.update(
        {
            "MAL_UPDATER_BOOTSTRAP_PROVIDERS": "hidive",
            "MAL_UPDATER_BOOTSTRAP_RUN_AUTH_STEPS": "yes",
            "MAL_UPDATER_MAL_CLIENT_ID": "client-id",
            "MAL_UPDATER_MAL_CLIENT_SECRET": "client-secret",
            "MAL_UPDATER_HIDIVE_USERNAME": "hidive-user@example.invalid",
            "MAL_UPDATER_HIDIVE_PASSWORD": "hidive-secret",
        }
    )

    result = _run_bootstrap(tmp_path, env)

    assert result.returncode == 0, result.stderr + result.stdout
    log = log_path.read_text(encoding="utf-8")
    assert "cli mal-auth-login" in log
    assert "cli provider-auth-login --provider hidive" in log
    assert "provider-auth-login --provider crunchyroll" not in log
    assert not (runtime / "secrets" / "crunchyroll_username.txt").exists()
    _assert_secrets_not_echoed(result, "client-secret", "hidive-secret")


def test_bootstrap_service_start_no_does_not_pass_start_service(tmp_path: Path) -> None:
    env, _runtime, log_path = _bootstrap_env(tmp_path)
    env.update(
        {
            "MAL_UPDATER_BOOTSTRAP_PROVIDERS": "none",
            "MAL_UPDATER_BOOTSTRAP_SERVICE_START": "no",
            "MAL_UPDATER_MAL_CLIENT_ID": "client-id",
            "MAL_UPDATER_MAL_CLIENT_SECRET": "client-secret",
        }
    )

    result = _run_bootstrap(tmp_path, env)

    assert result.returncode == 0, result.stderr + result.stdout
    log = log_path.read_text(encoding="utf-8")
    assert "install-systemd " in log
    assert "--start-service" not in log
    assert "systemctl " not in log
    _assert_secrets_not_echoed(result, "client-secret")


def test_bootstrap_service_start_yes_passes_start_service_once_without_second_restart(
    tmp_path: Path,
) -> None:
    env, _runtime, log_path = _bootstrap_env(tmp_path)
    env.update(
        {
            "MAL_UPDATER_BOOTSTRAP_PROVIDERS": "none",
            "MAL_UPDATER_BOOTSTRAP_SERVICE_START": "yes",
            "MAL_UPDATER_MAL_CLIENT_ID": "client-id",
            "MAL_UPDATER_MAL_CLIENT_SECRET": "client-secret",
        }
    )

    result = _run_bootstrap(tmp_path, env)

    assert result.returncode == 0, result.stderr + result.stdout
    log = log_path.read_text(encoding="utf-8")
    install_lines = [line for line in log.splitlines() if line.startswith("install-systemd")]
    assert install_lines == ["install-systemd --start-service"]
    assert log.count("--start-service") == 1
    assert "systemctl --user status --no-pager --lines=20 mal-updater.service" in log
    assert "systemctl --user restart" not in log
    assert "systemctl restart" not in log
    _assert_secrets_not_echoed(result, "client-secret")


def test_bootstrap_invalid_service_start_policy_exits_without_installing_or_starting(
    tmp_path: Path,
) -> None:
    env, _runtime, log_path = _bootstrap_env(tmp_path)
    env.update(
        {
            "MAL_UPDATER_BOOTSTRAP_PROVIDERS": "none",
            "MAL_UPDATER_BOOTSTRAP_SERVICE_START": "later-please",
            "MAL_UPDATER_MAL_CLIENT_ID": "client-id",
            "MAL_UPDATER_MAL_CLIENT_SECRET": "client-secret",
        }
    )

    result = _run_bootstrap(tmp_path, env)

    assert result.returncode == 2
    assert "Invalid MAL_UPDATER_BOOTSTRAP_SERVICE_START value: later-please" in result.stderr
    log = log_path.read_text(encoding="utf-8") if log_path.exists() else ""
    assert "install-systemd" not in log
    assert "systemctl " not in log
    _assert_secrets_not_echoed(result, "client-secret")


def test_bootstrap_noninteractive_still_requires_mal_client_id(tmp_path: Path) -> None:
    env, runtime, _log_path = _bootstrap_env(tmp_path)
    env.update({"MAL_UPDATER_BOOTSTRAP_PROVIDERS": "none"})

    result = _run_bootstrap(tmp_path, env)

    assert result.returncode == 1
    assert "Missing MAL client id" in result.stderr
    assert not (runtime / "secrets" / "mal_client_id.txt").exists()
    assert not (runtime / "secrets" / "mal_client_secret.txt").exists()


def test_bootstrap_invalid_provider_selection_exits_before_secrets_install_or_auth(
    tmp_path: Path,
) -> None:
    env, runtime, log_path = _bootstrap_env(tmp_path)
    env.update(
        {
            "MAL_UPDATER_BOOTSTRAP_PROVIDERS": "crunchyroll,unknown-provider",
            "MAL_UPDATER_BOOTSTRAP_SERVICE_START": "yes",
            "MAL_UPDATER_MAL_CLIENT_ID": "client-id",
            "MAL_UPDATER_MAL_CLIENT_SECRET": "client-secret",
            "MAL_UPDATER_CRUNCHYROLL_USERNAME": "cr-user@example.invalid",
            "MAL_UPDATER_CRUNCHYROLL_PASSWORD": "cr-secret",
        }
    )

    result = _run_bootstrap(tmp_path, env)

    assert result.returncode == 2
    assert "Unknown MAL_UPDATER_BOOTSTRAP_PROVIDERS entry: unknown-provider" in result.stderr
    log = log_path.read_text(encoding="utf-8") if log_path.exists() else ""
    assert "provider-auth-login" not in log
    assert "install-systemd" not in log
    assert "systemctl " not in log
    assert not (runtime / "secrets" / "mal_client_secret.txt").exists()
    assert not (runtime / "secrets" / "crunchyroll_password.txt").exists()
    _assert_secrets_not_echoed(result, "client-secret", "cr-secret")
