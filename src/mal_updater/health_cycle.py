from __future__ import annotations

import json
import os
import shlex
import subprocess
from contextlib import contextmanager, redirect_stderr, redirect_stdout
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator, TextIO

from .config import AppConfig, ensure_directories


_SOURCE_ROOT = Path(__file__).resolve().parents[1]


class HealthCycleLockBusyError(RuntimeError):
    pass


class HealthCycleAutoRemediationError(RuntimeError):
    def __init__(self, message: str, *, returncode: int | None = None) -> None:
        super().__init__(message)
        self.returncode = returncode


@contextmanager
def _exclusive_lock(path: Path) -> Iterator[None]:
    import fcntl

    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a+", encoding="utf-8") as lock_file:
        try:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise HealthCycleLockBusyError(str(path)) from exc
        try:
            yield
        finally:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)


@contextmanager
def _tee_stdout_stderr(log_path: Path) -> Iterator[None]:
    class Tee:
        def __init__(self, *streams: TextIO) -> None:
            self._streams = streams

        def write(self, data: str) -> int:
            for stream in self._streams:
                stream.write(data)
                stream.flush()
            return len(data)

        def flush(self) -> None:
            for stream in self._streams:
                stream.flush()

    log_path.parent.mkdir(parents=True, exist_ok=True)
    with log_path.open("a", encoding="utf-8") as log_file:
        tee = Tee(log_file, __import__("sys").stdout)
        err_tee = Tee(log_file, __import__("sys").stderr)
        with redirect_stdout(tee), redirect_stderr(err_tee):
            yield


def _now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _print_prefixed(message: str) -> None:
    print(f"[{_now_iso()}] {message}")


def _health_check_command(
    config: AppConfig,
    *,
    stale_hours: float,
    strict: bool,
    output_format: str,
    review_issue_type: str | None,
    review_worklist_limit: int,
    mapping_coverage_threshold: float,
    maintenance_review_limit: int,
) -> list[str]:
    command = [
        "python3",
        "-m",
        "mal_updater.cli",
        "--project-root",
        str(config.project_root),
        "health-check",
        "--stale-hours",
        str(stale_hours),
        "--format",
        output_format,
        "--review-worklist-limit",
        str(review_worklist_limit),
        "--mapping-coverage-threshold",
        str(mapping_coverage_threshold),
        "--maintenance-review-limit",
        str(maintenance_review_limit),
    ]
    if review_issue_type:
        command.extend(["--review-issue-type", review_issue_type])
    if strict:
        command.append("--strict")
    return command


def _run_health_check_json(
    config: AppConfig,
    *,
    stale_hours: float,
    json_path: Path,
    review_issue_type: str | None,
    review_worklist_limit: int,
    mapping_coverage_threshold: float,
    maintenance_review_limit: int,
) -> tuple[int, dict[str, object]]:
    result = subprocess.run(
        _health_check_command(
            config,
            stale_hours=stale_hours,
            strict=False,
            output_format="json",
            review_issue_type=review_issue_type,
            review_worklist_limit=review_worklist_limit,
            mapping_coverage_threshold=mapping_coverage_threshold,
            maintenance_review_limit=maintenance_review_limit,
        ),
        check=False,
        capture_output=True,
        text=True,
        env={**os.environ, "PYTHONPATH": str(_SOURCE_ROOT)},
        cwd=config.project_root,
    )
    json_path.write_text(result.stdout, encoding="utf-8")
    payload = json.loads(result.stdout)
    if not isinstance(payload, dict):
        raise RuntimeError(f"Expected top-level object in {json_path}")
    config.health_latest_json_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return result.returncode, payload


def _select_auto_command(payload: dict[str, object], *, allow_reason_codes: set[str]) -> dict[str, object] | None:
    maintenance = payload.get("maintenance")
    if not isinstance(maintenance, dict):
        return None
    recommended_commands = maintenance.get("recommended_commands")
    if not isinstance(recommended_commands, list):
        return None
    for item in recommended_commands:
        if not isinstance(item, dict):
            continue
        reason_code = item.get("reason_code")
        command_args = item.get("command_args")
        automation_safe = item.get("automation_safe") is True
        requires_auth_interaction = item.get("requires_auth_interaction") is True
        if reason_code not in allow_reason_codes:
            continue
        if not automation_safe or requires_auth_interaction:
            continue
        if not isinstance(command_args, list) or not all(isinstance(part, str) and part for part in command_args):
            continue
        first_arg = command_args[0]
        execution_mode = "direct" if first_arg.startswith("/") or "/" in first_arg or first_arg.endswith(".sh") else "cli"
        return {
            "reason_code": reason_code,
            "command_args": command_args,
            "execution_mode": execution_mode,
        }
    return None


def _run_auto_command(selection: dict[str, object]) -> None:
    env = dict(os.environ)
    command_args = [str(part) for part in selection.get("command_args", [])]
    if selection.get("execution_mode") == "direct":
        command = command_args
        subprocess_env = env
        command_display = " ".join(shlex.quote(part) for part in command)
    else:
        command = ["python3", "-m", "mal_updater.cli", *command_args]
        subprocess_env = {**env, "PYTHONPATH": str(_SOURCE_ROOT)}
        command_display = "PYTHONPATH=src " + " ".join(shlex.quote(part) for part in command)
    print("auto_remediation=enabled")
    print("auto_remediation_reason_code=" + str(selection["reason_code"]))
    print("auto_remediation_command=" + command_display)
    result = subprocess.run(command, check=False, env=subprocess_env, cwd=None if selection.get("execution_mode") == "direct" else os.getcwd())
    if result.returncode != 0:
        raise HealthCycleAutoRemediationError(
            f"auto remediation failed with exit code {result.returncode}",
            returncode=result.returncode,
        )
    print("auto_remediation_result=completed")


def run_health_check_cycle(
    config: AppConfig,
    *,
    stale_hours: float = 72.0,
    strict: bool = False,
    auto_run_recommended: bool = False,
    auto_run_reason_codes: set[str] | None = None,
    review_issue_type: str | None = None,
    review_worklist_limit: int = 3,
    mapping_coverage_threshold: float = 0.8,
    maintenance_review_limit: int = 25,
) -> int:
    ensure_directories(config)
    lock_file = config.state_dir / "locks" / "health-check.lock"
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    run_log = config.state_dir / "logs" / f"health-check-{stamp}.log"
    run_json = config.health_latest_json_path.parent / f"health-check-{stamp}.json"
    allowed_reason_codes = auto_run_reason_codes or {"refresh_ingested_snapshot"}

    try:
        with _exclusive_lock(lock_file):
            with _tee_stdout_stderr(run_log):
                _print_prefixed("starting MAL-Updater health-check cycle")
                print(f"root={config.project_root}")
                print(f"runtime_root={config.runtime_root}")
                print(f"log={run_log}")
                print(f"health_json={run_json}")
                print(f"stale_hours={stale_hours:g}")
                print(f"auto_run_recommended={1 if auto_run_recommended else 0}")
                print("auto_run_reason_codes=" + ",".join(sorted(allowed_reason_codes)))
                print(f"review_issue_type={review_issue_type or ''}")
                print(f"review_worklist_limit={review_worklist_limit}")
                print(f"mapping_coverage_threshold={mapping_coverage_threshold:g}")
                print(f"maintenance_review_limit={maintenance_review_limit}")

                from .db import bootstrap_database

                bootstrap_database(config.db_path)
                _, payload = _run_health_check_json(
                    config,
                    stale_hours=stale_hours,
                    json_path=run_json,
                    review_issue_type=review_issue_type,
                    review_worklist_limit=review_worklist_limit,
                    mapping_coverage_threshold=mapping_coverage_threshold,
                    maintenance_review_limit=maintenance_review_limit,
                )

                selected = _select_auto_command(payload, allow_reason_codes=allowed_reason_codes) if auto_run_recommended else None
                if selected is None:
                    maintenance = payload.get("maintenance") if isinstance(payload.get("maintenance"), dict) else {}
                    recommended_command = maintenance.get("recommended_command") if isinstance(maintenance.get("recommended_command"), dict) else None
                    recommended_auto_command = maintenance.get("recommended_automation_command") if isinstance(maintenance.get("recommended_automation_command"), dict) else None
                    if auto_run_recommended:
                        print("auto_remediation=enabled")
                        if isinstance(recommended_command, dict) and recommended_command.get("reason_code"):
                            print("auto_remediation_recommended_reason_code=" + str(recommended_command["reason_code"]))
                        if isinstance(recommended_auto_command, dict) and recommended_auto_command.get("reason_code"):
                            print("auto_remediation_safe_candidate_reason_code=" + str(recommended_auto_command["reason_code"]))
                        if isinstance(recommended_auto_command, dict) and recommended_auto_command.get("reason_code") not in allowed_reason_codes:
                            print("auto_remediation_action=skipped_not_allowlisted")
                        else:
                            print("auto_remediation_action=none")
                    else:
                        print("auto_remediation=disabled")
                else:
                    _run_auto_command(selected)
                    _print_prefixed("re-running health-check after optional remediation")
                    _run_health_check_json(
                        config,
                        stale_hours=stale_hours,
                        json_path=run_json,
                        review_issue_type=review_issue_type,
                        review_worklist_limit=review_worklist_limit,
                        mapping_coverage_threshold=mapping_coverage_threshold,
                        maintenance_review_limit=maintenance_review_limit,
                    )

                summary = subprocess.run(
                    _health_check_command(
                        config,
                        stale_hours=stale_hours,
                        strict=strict,
                        output_format="summary",
                        review_issue_type=review_issue_type,
                        review_worklist_limit=review_worklist_limit,
                        mapping_coverage_threshold=mapping_coverage_threshold,
                        maintenance_review_limit=maintenance_review_limit,
                    ),
                    check=False,
                    capture_output=True,
                    text=True,
                    env={**os.environ, "PYTHONPATH": str(_SOURCE_ROOT)},
                    cwd=config.project_root,
                )
                if summary.stdout:
                    print(summary.stdout, end="")
                if summary.stderr:
                    print(summary.stderr, end="", file=__import__("sys").stderr)
                _print_prefixed("MAL-Updater health-check cycle completed")
                return summary.returncode
    except HealthCycleLockBusyError:
        _print_prefixed("health-check already running; skipping overlap")
        return 0
    except HealthCycleAutoRemediationError as exc:
        print(str(exc), file=__import__("sys").stderr)
        return exc.returncode or 1
