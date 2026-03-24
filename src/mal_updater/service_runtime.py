from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import json
import subprocess
import time
from typing import Any

from .auth import persist_token_response
from .config import AppConfig, ensure_directories, load_config, load_mal_secrets
from .crunchyroll_auth import load_crunchyroll_credentials, resolve_crunchyroll_state_paths
from .hidive_auth import load_hidive_credentials, resolve_hidive_state_paths
from .mal_client import MalApiError, MalClient
from .request_tracking import (
    estimate_budget_recovery_seconds,
    estimate_budget_recovery_seconds_for_ratio,
    prune_api_request_events,
    summarize_recent_api_usage,
)


@dataclass(slots=True)
class TaskSpec:
    name: str
    every_seconds: int
    budget_provider: str | None = None


_BUDGET_GATE_WINDOW_SECONDS = 3600
_FAILURE_BACKOFF_MIN_SECONDS = 300

_AUTH_STYLE_FAILURE_MARKERS = (
    "http 401",
    "http 403",
    "unauthor",
    "forbidden",
    "auth_failed",
    "invalid_grant",
    "invalid token",
    "expired token",
    "token refresh",
    "refresh token",
    "credential_rebootstrap",
    "login failed",
    "login did not return",
    "did not return both access_token and refresh_token",
    "did not return both authorisationtoken and refreshtoken",
    "did not return authorisationtoken",
    "did not return refreshtoken",
    "bearer",
)


def _now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _iso_after_seconds(seconds: int) -> str:
    return (datetime.now(timezone.utc) + timedelta(seconds=max(0, seconds))).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _load_state(config: AppConfig) -> dict[str, Any]:
    if not config.service_state_path.exists():
        return {"started_at": _now_iso(), "tasks": {}}
    return json.loads(config.service_state_path.read_text(encoding="utf-8"))


def _save_state(config: AppConfig, state: dict[str, Any]) -> None:
    config.service_state_path.write_text(json.dumps(state, indent=2, sort_keys=True), encoding="utf-8")


def _set_task_next_due(task_state: dict[str, Any], *, base_epoch: float, every_seconds: int) -> None:
    task_state["every_seconds"] = int(every_seconds)
    task_state["next_due_epoch"] = float(base_epoch) + int(every_seconds)
    task_state["next_due_at"] = _iso_after_seconds(int(task_state["next_due_epoch"] - time.time()))


def _append_log(config: AppConfig, message: str) -> None:
    with config.service_log_path.open("a", encoding="utf-8") as fh:
        fh.write(f"[{_now_iso()}] {message}\n")


def _mark_task_decision(task_state: dict[str, Any], *, decision_at: str | None = None) -> None:
    task_state["last_decision_at"] = decision_at or _now_iso()


def _record_task_timing(
    task_state: dict[str, Any],
    *,
    started_epoch: float,
    finished_epoch: float,
    started_at: str | None = None,
    finished_at: str | None = None,
) -> None:
    task_state["last_started_at"] = started_at or datetime.fromtimestamp(started_epoch, tz=timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    task_state["last_finished_at"] = finished_at or datetime.fromtimestamp(finished_epoch, tz=timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    task_state["last_duration_seconds"] = max(0.0, round(float(finished_epoch) - float(started_epoch), 3))
    _mark_task_decision(task_state, decision_at=task_state["last_finished_at"])


def _run_subprocess(config: AppConfig, args: list[str], *, label: str) -> dict[str, Any]:
    env = {
        **__import__("os").environ,
        "PYTHONPATH": str(config.project_root / "src"),
    }
    result = subprocess.run(args, cwd=config.project_root, text=True, capture_output=True, check=False, env=env)
    status = "ok" if result.returncode == 0 else "error"
    payload = {
        "status": status,
        "label": label,
        "returncode": result.returncode,
        "stdout": result.stdout,
        "stderr": result.stderr,
    }
    if result.returncode == 0:
        _append_log(config, f"task={label} status=ok")
    else:
        _append_log(config, f"task={label} status=error returncode={result.returncode} stderr={result.stderr.strip() or result.stdout.strip()}")
    return payload


def _refresh_mal_tokens(config: AppConfig) -> dict[str, Any]:
    secrets = load_mal_secrets(config)
    if not (secrets.client_id and secrets.refresh_token):
        return {"status": "skipped", "reason": "missing_mal_refresh_material"}
    client = MalClient(config, secrets)
    token = client.refresh_access_token()
    persisted = persist_token_response(token, secrets)
    return {
        "status": "ok",
        "access_token_path": str(persisted.access_token_path),
        "refresh_token_path": str(persisted.refresh_token_path),
    }


def _available_source_providers(config: AppConfig) -> list[str]:
    providers: list[str] = []
    crunchyroll_credentials = load_crunchyroll_credentials(config)
    if crunchyroll_credentials.username and crunchyroll_credentials.password:
        providers.append("crunchyroll")
    hidive_credentials = load_hidive_credentials(config)
    if hidive_credentials.username and hidive_credentials.password:
        providers.append("hidive")
    return providers



def _task_specs(config: AppConfig) -> list[TaskSpec]:
    specs = [TaskSpec("mal_refresh", config.service.mal_refresh_every_seconds, budget_provider="mal")]
    for provider in _available_source_providers(config):
        specs.append(TaskSpec(f"sync_fetch_{provider}", config.service.sync_every_seconds, budget_provider=provider))
    specs.append(TaskSpec("sync_apply", config.service.sync_every_seconds, budget_provider="mal"))
    specs.append(TaskSpec("health", config.service.health_every_seconds, budget_provider=None))
    return specs


def _provider_fetch_command(config: AppConfig, provider: str, *, full_refresh: bool = False) -> list[str]:
    command: list[str]
    if provider == "crunchyroll":
        snapshot_path = config.cache_dir / "live-crunchyroll-snapshot.json"
        command = [
            "python3",
            "-m",
            "mal_updater.cli",
            "crunchyroll-fetch-snapshot",
            "--out",
            str(snapshot_path),
            "--ingest",
        ]
    elif provider == "hidive":
        snapshot_path = config.cache_dir / "live-hidive-snapshot.json"
        command = [
            "python3",
            "-m",
            "mal_updater.cli",
            "provider-fetch-snapshot",
            "--provider",
            "hidive",
            "--out",
            str(snapshot_path),
            "--ingest",
        ]
    else:
        raise ValueError(f"unsupported provider fetch task: {provider}")
    if full_refresh:
        command.append("--full-refresh")
    return command



def _provider_fetch_requires_full_refresh(config: AppConfig, task_state: dict[str, Any], *, now: float) -> bool:
    interval = int(config.service.full_refresh_every_seconds)
    if interval <= 0:
        return False
    anchor_epoch = task_state.get("full_refresh_anchor_epoch")
    if not isinstance(anchor_epoch, (int, float)) or anchor_epoch <= 0:
        return False
    return float(now) - float(anchor_epoch) >= interval



def _provider_from_refresh_command_args(command_args: object) -> str | None:
    if not isinstance(command_args, list) or not command_args:
        return None
    if command_args[0] == "crunchyroll-fetch-snapshot":
        return "crunchyroll"
    if len(command_args) >= 3 and command_args[0] == "provider-fetch-snapshot" and command_args[1] == "--provider" and isinstance(command_args[2], str):
        return str(command_args[2])
    return None



def _provider_fetch_requested_by_health(config: AppConfig, provider: str, task_state: dict[str, Any]) -> bool:
    path = config.health_latest_json_path
    if not path.exists():
        return False
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return False
    if not isinstance(payload, dict):
        return False
    maintenance = payload.get("maintenance")
    if not isinstance(maintenance, dict):
        return False
    commands = maintenance.get("recommended_commands")
    if not isinstance(commands, list):
        return False
    try:
        health_mtime = path.stat().st_mtime
    except OSError:
        return False
    last_full_refresh_epoch = task_state.get("last_successful_full_refresh_epoch")
    if isinstance(last_full_refresh_epoch, (int, float)) and float(last_full_refresh_epoch) >= float(health_mtime):
        return False
    for command in commands:
        if not isinstance(command, dict):
            continue
        if command.get("reason_code") != "refresh_full_snapshot":
            continue
        if _provider_from_refresh_command_args(command.get("command_args")) == provider:
            return True
    return False



def _apply_sync_command() -> list[str]:
    return [
        "python3",
        "-m",
        "mal_updater.cli",
        "apply-sync",
        "--limit",
        "0",
        "--exact-approved-only",
        "--execute",
    ]



def _budget_gate(config: AppConfig, spec: TaskSpec) -> tuple[bool, str | None, dict[str, Any] | None]:
    provider = spec.budget_provider
    if provider is None:
        return True, None, None
    usage = summarize_recent_api_usage(provider=provider, window_seconds=_BUDGET_GATE_WINDOW_SECONDS, config=config).as_dict()
    limit = config.service.hourly_limit_for(provider, task_name=spec.name)
    ratio = 0.0 if limit <= 0 else float(usage.get("request_count", 0)) / float(limit)
    warn_recovery_seconds = estimate_budget_recovery_seconds_for_ratio(
        provider=provider,
        limit=limit,
        target_ratio=config.service.warn_ratio,
        window_seconds=_BUDGET_GATE_WINDOW_SECONDS,
        config=config,
    )
    recovery_seconds = estimate_budget_recovery_seconds(
        provider=provider,
        limit=limit,
        critical_ratio=config.service.critical_ratio,
        window_seconds=_BUDGET_GATE_WINDOW_SECONDS,
        config=config,
    )
    budget_scope = config.service.budget_scope_for(provider, task_name=spec.name)
    warn_floor_seconds = config.service.backoff_floor_seconds_for(provider, level="warn", task_name=spec.name)
    critical_floor_seconds = config.service.backoff_floor_seconds_for(provider, level="critical", task_name=spec.name)
    warn_cooldown_seconds = max(warn_recovery_seconds, warn_floor_seconds)
    critical_cooldown_seconds = max(recovery_seconds, critical_floor_seconds)
    usage["limit"] = limit
    usage["ratio"] = ratio
    usage["warn_ratio"] = config.service.warn_ratio
    usage["critical_ratio"] = config.service.critical_ratio
    usage["budget_scope"] = budget_scope
    usage["warn_recovery_seconds"] = warn_recovery_seconds
    usage["recovery_seconds"] = recovery_seconds
    usage["warn_backoff_floor_seconds"] = warn_floor_seconds
    usage["critical_backoff_floor_seconds"] = critical_floor_seconds
    usage["warn_cooldown_seconds"] = warn_cooldown_seconds
    usage["critical_cooldown_seconds"] = critical_cooldown_seconds
    if ratio >= config.service.critical_ratio:
        usage["backoff_level"] = "critical"
        usage["cooldown_seconds"] = critical_cooldown_seconds
        if critical_floor_seconds > recovery_seconds:
            usage["cooldown_source"] = f"{budget_scope}_floor"
        return False, f"{provider}_budget_critical ratio={ratio:.3f} cooldown={critical_cooldown_seconds}s", usage
    if ratio >= config.service.warn_ratio and warn_cooldown_seconds > 0:
        usage["backoff_level"] = "warn"
        usage["cooldown_seconds"] = warn_cooldown_seconds
        if warn_floor_seconds > warn_recovery_seconds:
            usage["cooldown_source"] = f"{budget_scope}_floor"
        return False, f"{provider}_budget_warn ratio={ratio:.3f} cooldown={warn_cooldown_seconds}s", usage
    return True, None, usage


def _looks_auth_style_failure(reason: str) -> bool:
    lowered = reason.lower()
    return any(marker in lowered for marker in _AUTH_STYLE_FAILURE_MARKERS)


def _failure_backoff_profile(config: AppConfig, spec: TaskSpec, reason: str) -> tuple[str, int]:
    provider = spec.budget_provider
    critical_floor = 0
    auth_floor = 0
    if provider:
        critical_floor = config.service.backoff_floor_seconds_for(provider, level="critical", task_name=spec.name)
        auth_floor = config.service.auth_failure_backoff_floor_seconds_for(provider, task_name=spec.name)
    classification = "auth" if provider and _looks_auth_style_failure(reason) else "generic"
    configured_floor = critical_floor
    if classification == "auth":
        configured_floor = max(configured_floor, auth_floor)
    return classification, configured_floor


def _failure_backoff_seconds(config: AppConfig, spec: TaskSpec, task_state: dict[str, Any], *, reason: str) -> tuple[int, str, int]:
    classification, configured_floor = _failure_backoff_profile(config, spec, reason)
    base_seconds = max(_FAILURE_BACKOFF_MIN_SECONDS, configured_floor)
    consecutive_failures = int(task_state.get("failure_backoff_consecutive_failures", 0)) + 1
    max_seconds = max(base_seconds, int(spec.every_seconds))
    cooldown_seconds = min(max_seconds, base_seconds * (2 ** max(0, consecutive_failures - 1)))
    return max(0, int(cooldown_seconds)), classification, configured_floor


def _clear_failure_backoff(task_state: dict[str, Any]) -> None:
    task_state.pop("failure_backoff_until_epoch", None)
    task_state.pop("failure_backoff_until", None)
    task_state.pop("failure_backoff_remaining_seconds", None)
    task_state.pop("failure_backoff_reason", None)
    task_state.pop("failure_backoff_consecutive_failures", None)
    task_state.pop("failure_backoff_class", None)
    task_state.pop("failure_backoff_floor_seconds", None)


def _set_failure_backoff(
    config: AppConfig,
    spec: TaskSpec,
    task_state: dict[str, Any],
    *,
    now: float,
    reason: str,
) -> dict[str, Any]:
    cooldown_seconds, failure_class, floor_seconds = _failure_backoff_seconds(config, spec, task_state, reason=reason)
    consecutive_failures = int(task_state.get("failure_backoff_consecutive_failures", 0)) + 1
    task_state["failure_backoff_consecutive_failures"] = consecutive_failures
    task_state["failure_backoff_reason"] = reason
    task_state["failure_backoff_class"] = failure_class
    task_state["failure_backoff_floor_seconds"] = floor_seconds
    task_state["failure_backoff_until_epoch"] = now + cooldown_seconds
    task_state["failure_backoff_until"] = _iso_after_seconds(cooldown_seconds)
    task_state["failure_backoff_remaining_seconds"] = cooldown_seconds
    return {
        "failure_backoff_until": task_state["failure_backoff_until"],
        "failure_backoff_remaining_seconds": cooldown_seconds,
        "failure_backoff_reason": reason,
        "failure_backoff_consecutive_failures": consecutive_failures,
        "failure_backoff_class": failure_class,
        "failure_backoff_floor_seconds": floor_seconds,
    }


def _summarize_task_failure(result: dict[str, Any]) -> str | None:
    reason = result.get("reason")
    if isinstance(reason, str) and reason.strip():
        return reason.strip()
    stderr = result.get("stderr")
    if isinstance(stderr, str) and stderr.strip():
        return stderr.strip().splitlines()[0]
    stdout = result.get("stdout")
    if isinstance(stdout, str) and stdout.strip():
        return stdout.strip().splitlines()[0]
    return None


def run_pending_tasks(config: AppConfig | None = None) -> dict[str, Any]:
    config = config or load_config()
    ensure_directories(config)
    state = _load_state(config)
    tasks_state = state.setdefault("tasks", {})
    now = time.time()
    results: list[dict[str, Any]] = []
    pruned = prune_api_request_events(retention_days=14, config=config)
    if pruned:
        _append_log(config, f"api_request_events_pruned={pruned}")

    for spec in _task_specs(config):
        task_state = tasks_state.setdefault(spec.name, {})
        task_state["budget_provider"] = spec.budget_provider
        task_state["budget_scope"] = config.service.budget_scope_for(spec.budget_provider, task_name=spec.name)
        task_state["every_seconds"] = int(spec.every_seconds)
        last_run = float(task_state.get("last_run_epoch", 0))
        if now - last_run < spec.every_seconds:
            _set_task_next_due(task_state, base_epoch=last_run, every_seconds=spec.every_seconds)
            continue
        backoff_until_epoch = float(task_state.get("budget_backoff_until_epoch", 0))
        if backoff_until_epoch > now:
            remaining = max(0, int(backoff_until_epoch - now))
            task_state.update(
                {
                    "last_status": "skipped",
                    "last_skipped_at": _now_iso(),
                    "last_skip_reason": f"budget_backoff_active remaining={remaining}s",
                    "budget_backoff_remaining_seconds": remaining,
                }
            )
            results.append(
                {
                    "task": spec.name,
                    "status": "skipped",
                    "reason": f"budget_backoff_active remaining={remaining}s",
                    "budget_backoff_until": task_state.get("budget_backoff_until"),
                    "budget_backoff_remaining_seconds": remaining,
                    "budget_backoff_level": task_state.get("budget_backoff_level"),
                    "budget_scope": task_state.get("budget_scope"),
                }
            )
            continue
        failure_backoff_until_epoch = float(task_state.get("failure_backoff_until_epoch", 0))
        if failure_backoff_until_epoch > now:
            remaining = max(0, int(failure_backoff_until_epoch - now))
            task_state.update(
                {
                    "last_status": "skipped",
                    "last_skipped_at": _now_iso(),
                    "last_skip_reason": f"failure_backoff_active remaining={remaining}s",
                    "failure_backoff_remaining_seconds": remaining,
                }
            )
            results.append(
                {
                    "task": spec.name,
                    "status": "skipped",
                    "reason": f"failure_backoff_active remaining={remaining}s",
                    "failure_backoff_until": task_state.get("failure_backoff_until"),
                    "failure_backoff_remaining_seconds": remaining,
                    "failure_backoff_reason": task_state.get("failure_backoff_reason"),
                    "failure_backoff_consecutive_failures": task_state.get("failure_backoff_consecutive_failures"),
                    "failure_backoff_class": task_state.get("failure_backoff_class"),
                    "failure_backoff_floor_seconds": task_state.get("failure_backoff_floor_seconds"),
                }
            )
            continue
        allowed, reason, usage = _budget_gate(config, spec)
        if not allowed:
            backoff_level = usage.get("backoff_level") if isinstance(usage, dict) else None
            recovery_seconds = int(usage.get("cooldown_seconds", 0)) if isinstance(usage, dict) else 0
            backoff_floor_seconds = 0
            cooldown_source = None
            if isinstance(usage, dict):
                floor_key = "warn_backoff_floor_seconds" if backoff_level == "warn" else "critical_backoff_floor_seconds"
                backoff_floor_seconds = int(usage.get(floor_key, 0))
                cooldown_source = usage.get("cooldown_source")
            skipped_at = _now_iso()
            task_state.update(
                {
                    "last_status": "skipped",
                    "last_skipped_at": skipped_at,
                    "last_skip_reason": reason,
                    "budget_backoff_level": backoff_level,
                    "budget_backoff_until_epoch": now + recovery_seconds,
                    "budget_backoff_until": _iso_after_seconds(recovery_seconds),
                    "budget_backoff_remaining_seconds": recovery_seconds,
                    "budget_backoff_floor_seconds": backoff_floor_seconds,
                    "budget_scope": task_state.get("budget_scope"),
                }
            )
            if isinstance(cooldown_source, str) and cooldown_source:
                task_state["budget_backoff_cooldown_source"] = cooldown_source
            else:
                task_state.pop("budget_backoff_cooldown_source", None)
            _mark_task_decision(task_state, decision_at=skipped_at)
            _set_task_next_due(task_state, base_epoch=now, every_seconds=spec.every_seconds)
            results.append(
                {
                    "task": spec.name,
                    "status": "skipped",
                    "reason": reason,
                    "api_usage": usage,
                    "budget_backoff_level": backoff_level,
                    "budget_backoff_until": task_state.get("budget_backoff_until"),
                    "budget_backoff_remaining_seconds": recovery_seconds,
                    "budget_backoff_floor_seconds": backoff_floor_seconds,
                    "budget_backoff_cooldown_source": task_state.get("budget_backoff_cooldown_source"),
                    "budget_scope": task_state.get("budget_scope"),
                }
            )
            _append_log(config, f"task={spec.name} status=skipped reason={reason}")
            continue
        started_epoch = time.time()
        started_at = datetime.fromtimestamp(started_epoch, tz=timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
        try:
            full_refresh_requested = False
            if spec.name == "mal_refresh":
                result = _refresh_mal_tokens(config)
            elif spec.name.startswith("sync_fetch_"):
                provider = spec.name.removeprefix("sync_fetch_")
                full_refresh_reasons: list[str] = []
                if _provider_fetch_requires_full_refresh(config, task_state, now=now):
                    full_refresh_reasons.append("periodic_cadence")
                if _provider_fetch_requested_by_health(config, provider, task_state):
                    full_refresh_reasons.append("health_recommended")
                full_refresh_requested = bool(full_refresh_reasons)
                result = _run_subprocess(
                    config,
                    _provider_fetch_command(config, provider, full_refresh=full_refresh_requested),
                    label=spec.name,
                )
                result["fetch_mode"] = "full_refresh" if full_refresh_requested else "incremental"
                if full_refresh_reasons:
                    result["full_refresh_reason"] = "+".join(full_refresh_reasons)
            elif spec.name == "sync_apply":
                result = _run_subprocess(config, _apply_sync_command(), label="sync_apply")
            elif spec.name == "health":
                result = _run_subprocess(config, [str(config.project_root / "scripts" / "run_health_check_cycle.sh")], label="health")
            else:
                result = {"status": "skipped", "reason": "unknown_task"}
            finished_epoch = time.time()
            finished_at = datetime.fromtimestamp(finished_epoch, tz=timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
            task_status = result.get("status", "ok")
            task_state.update({"last_run_epoch": now, "last_run_at": finished_at, "last_status": task_status, "last_result": result})
            _record_task_timing(task_state, started_epoch=started_epoch, finished_epoch=finished_epoch, started_at=started_at, finished_at=finished_at)
            _set_task_next_due(task_state, base_epoch=now, every_seconds=spec.every_seconds)
            if spec.name.startswith("sync_fetch_"):
                task_state["last_fetch_mode"] = "full_refresh" if full_refresh_requested else "incremental"
                task_state["last_fetch_mode_at"] = finished_at
                if full_refresh_requested:
                    task_state["last_full_refresh_reason"] = result.get("full_refresh_reason")
                    task_state["last_successful_full_refresh_epoch"] = finished_epoch
                    task_state["last_successful_full_refresh_at"] = finished_at
                    task_state["full_refresh_anchor_epoch"] = finished_epoch
                    task_state["full_refresh_anchor_at"] = finished_at
                else:
                    task_state.pop("last_full_refresh_reason", None)
                    if not isinstance(task_state.get("full_refresh_anchor_epoch"), (int, float)):
                        task_state["full_refresh_anchor_epoch"] = finished_epoch
                        task_state["full_refresh_anchor_at"] = finished_at
            task_state.pop("last_skip_reason", None)
            task_state.pop("last_skipped_at", None)
            task_state.pop("budget_backoff_level", None)
            task_state.pop("budget_backoff_until_epoch", None)
            task_state.pop("budget_backoff_until", None)
            task_state.pop("budget_backoff_remaining_seconds", None)
            task_state.pop("budget_backoff_floor_seconds", None)
            task_state.pop("budget_backoff_cooldown_source", None)
            if task_status == "error":
                failure_reason = _summarize_task_failure(result) or "subprocess_error"
                task_state["last_error"] = failure_reason
                failure_backoff = _set_failure_backoff(config, spec, task_state, now=now, reason=failure_reason)
                results.append({"task": spec.name, **result, **failure_backoff})
                _append_log(
                    config,
                    f"task={spec.name} status=error failure_backoff={failure_backoff['failure_backoff_remaining_seconds']}s reason={failure_reason}",
                )
                continue
            task_state.pop("last_error", None)
            _clear_failure_backoff(task_state)
            results.append({"task": spec.name, **result})
        except (MalApiError, OSError, subprocess.SubprocessError) as exc:
            finished_epoch = time.time()
            finished_at = datetime.fromtimestamp(finished_epoch, tz=timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
            task_state.update({"last_run_epoch": now, "last_run_at": finished_at, "last_status": "error", "last_error": f"{type(exc).__name__}: {exc}"})
            _record_task_timing(task_state, started_epoch=started_epoch, finished_epoch=finished_epoch, started_at=started_at, finished_at=finished_at)
            _set_task_next_due(task_state, base_epoch=now, every_seconds=spec.every_seconds)
            task_state.pop("last_skip_reason", None)
            task_state.pop("last_skipped_at", None)
            task_state.pop("budget_backoff_level", None)
            task_state.pop("budget_backoff_until_epoch", None)
            task_state.pop("budget_backoff_until", None)
            task_state.pop("budget_backoff_remaining_seconds", None)
            task_state.pop("budget_backoff_floor_seconds", None)
            task_state.pop("budget_backoff_cooldown_source", None)
            failure_backoff = _set_failure_backoff(config, spec, task_state, now=now, reason=f"{type(exc).__name__}: {exc}")
            results.append({"task": spec.name, "status": "error", "error": f"{type(exc).__name__}: {exc}", **failure_backoff})
            _append_log(
                config,
                f"task={spec.name} status=error error={type(exc).__name__}: {exc} failure_backoff={failure_backoff['failure_backoff_remaining_seconds']}s",
            )

    state["last_loop_at"] = _now_iso()
    tracked_providers = {"mal", "crunchyroll", *config.service.provider_hourly_limits.keys(), *_available_source_providers(config)}
    state["api_usage"] = {
        provider: summarize_recent_api_usage(provider=provider, window_seconds=_BUDGET_GATE_WINDOW_SECONDS, config=config).as_dict()
        for provider in sorted(tracked_providers)
    }
    _save_state(config, state)
    return {"status": "ok", "results": results, "state_file": str(config.service_state_path), "api_usage": state["api_usage"]}

def run_service_loop(config: AppConfig | None = None) -> int:
    config = config or load_config()
    ensure_directories(config)
    _append_log(config, "service loop starting")
    while True:
        run_pending_tasks(config)
        time.sleep(max(5, int(config.service.loop_sleep_seconds)))
