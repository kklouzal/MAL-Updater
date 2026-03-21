from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import json
import subprocess
import time
from typing import Any

from .auth import persist_token_response
from .config import AppConfig, ensure_directories, load_config, load_mal_secrets
from .mal_client import MalApiError, MalClient
from .request_tracking import estimate_budget_recovery_seconds, prune_api_request_events, summarize_recent_api_usage


@dataclass(slots=True)
class TaskSpec:
    name: str
    every_seconds: int
    budget_provider: str | None = None


_BUDGET_GATE_WINDOW_SECONDS = 3600


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


def _append_log(config: AppConfig, message: str) -> None:
    with config.service_log_path.open("a", encoding="utf-8") as fh:
        fh.write(f"[{_now_iso()}] {message}\n")


def _run_subprocess(config: AppConfig, args: list[str], *, label: str) -> dict[str, Any]:
    env = {
        **__import__("os").environ,
        "PYTHONPATH": str(config.project_root / "src"),
    }
    result = subprocess.run(args, cwd=config.project_root, text=True, capture_output=True, check=False, env=env)
    payload = {
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


def _task_specs(config: AppConfig) -> list[TaskSpec]:
    return [
        TaskSpec("mal_refresh", config.service.mal_refresh_every_seconds, budget_provider="mal"),
        TaskSpec("sync", config.service.sync_every_seconds, budget_provider="crunchyroll"),
        TaskSpec("health", config.service.health_every_seconds, budget_provider=None),
    ]


def _budget_gate(config: AppConfig, provider: str | None) -> tuple[bool, str | None, dict[str, Any] | None]:
    if provider is None:
        return True, None, None
    usage = summarize_recent_api_usage(provider=provider, window_seconds=_BUDGET_GATE_WINDOW_SECONDS, config=config).as_dict()
    limit = config.service.hourly_limit_for(provider)
    ratio = 0.0 if limit <= 0 else float(usage.get("request_count", 0)) / float(limit)
    recovery_seconds = estimate_budget_recovery_seconds(
        provider=provider,
        limit=limit,
        critical_ratio=config.service.critical_ratio,
        window_seconds=_BUDGET_GATE_WINDOW_SECONDS,
        config=config,
    )
    usage["limit"] = limit
    usage["ratio"] = ratio
    usage["warn_ratio"] = config.service.warn_ratio
    usage["critical_ratio"] = config.service.critical_ratio
    usage["recovery_seconds"] = recovery_seconds
    if ratio >= config.service.critical_ratio:
        return False, f"{provider}_budget_critical ratio={ratio:.3f} cooldown={recovery_seconds}s", usage
    return True, None, usage


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
        last_run = float(task_state.get("last_run_epoch", 0))
        if now - last_run < spec.every_seconds:
            continue
        backoff_until_epoch = float(task_state.get("budget_backoff_until_epoch", 0))
        if backoff_until_epoch > now:
            remaining = max(0, int(backoff_until_epoch - now))
            results.append(
                {
                    "task": spec.name,
                    "status": "skipped",
                    "reason": f"budget_backoff_active remaining={remaining}s",
                    "budget_backoff_until": task_state.get("budget_backoff_until"),
                    "budget_backoff_remaining_seconds": remaining,
                }
            )
            continue
        allowed, reason, usage = _budget_gate(config, spec.budget_provider)
        if not allowed:
            recovery_seconds = int(usage.get("recovery_seconds", 0)) if isinstance(usage, dict) else 0
            task_state.update(
                {
                    "last_skipped_at": _now_iso(),
                    "last_skip_reason": reason,
                    "budget_backoff_until_epoch": now + recovery_seconds,
                    "budget_backoff_until": _iso_after_seconds(recovery_seconds),
                }
            )
            results.append(
                {
                    "task": spec.name,
                    "status": "skipped",
                    "reason": reason,
                    "api_usage": usage,
                    "budget_backoff_until": task_state.get("budget_backoff_until"),
                    "budget_backoff_remaining_seconds": recovery_seconds,
                }
            )
            _append_log(config, f"task={spec.name} status=skipped reason={reason}")
            continue
        try:
            if spec.name == "mal_refresh":
                result = _refresh_mal_tokens(config)
            elif spec.name == "sync":
                result = _run_subprocess(config, [str(config.project_root / "scripts" / "run_exact_approved_sync_cycle.sh")], label="sync")
            elif spec.name == "health":
                result = _run_subprocess(config, [str(config.project_root / "scripts" / "run_health_check_cycle.sh")], label="health")
            else:
                result = {"status": "skipped", "reason": "unknown_task"}
            task_state.update({"last_run_epoch": now, "last_run_at": _now_iso(), "last_status": result.get("status", "ok"), "last_result": result})
            task_state.pop("last_error", None)
            task_state.pop("budget_backoff_until_epoch", None)
            task_state.pop("budget_backoff_until", None)
            results.append({"task": spec.name, **result})
        except (MalApiError, OSError, subprocess.SubprocessError) as exc:
            task_state.update({"last_run_epoch": now, "last_run_at": _now_iso(), "last_status": "error", "last_error": f"{type(exc).__name__}: {exc}"})
            results.append({"task": spec.name, "status": "error", "error": f"{type(exc).__name__}: {exc}"})
            _append_log(config, f"task={spec.name} status=error error={type(exc).__name__}: {exc}")

    state["last_loop_at"] = _now_iso()
    tracked_providers = {"mal", "crunchyroll", *config.service.provider_hourly_limits.keys()}
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
