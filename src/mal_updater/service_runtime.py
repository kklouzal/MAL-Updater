from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import fcntl
import json
import math
import os
import subprocess
import sys
import time
from typing import Any
import uuid

from .auth import persist_token_response
from .auth_failure_signals import looks_auth_style_failure
from .config import (
    AppConfig,
    DEFAULT_SERVICE_TASK_EXECUTE_LIMITS,
    DEFAULT_SERVICE_TASK_PROJECTED_REQUEST_COUNTS,
    DEFAULT_SERVICE_TASK_PROJECTED_REQUEST_COUNTS_BY_MODE,
    ensure_directories,
    load_config,
    load_mal_secrets,
)
from .crunchyroll_auth import load_crunchyroll_credentials
from .hidive_auth import load_hidive_credentials
from .mal_client import MalApiError, MalClient
from .openclaw_delivery import OpenClawDeliveryError, deliver_recommendations_via_openclaw
from .persistence import PersistentJsonError, atomic_write_json, read_json_dict_bounded
from .redaction import sanitize_command, sanitize_text, sanitize_url, sanitize_value
from .request_tracking import (
    begin_api_request_context,
    capture_api_event_boundary,
    count_api_events_since,
    current_api_request_context,
    end_api_request_context,
    estimate_budget_recovery_seconds,
    estimate_budget_recovery_seconds_for_ratio,
    prune_api_request_events_with_diagnostics,
    request_context_environment,
    summarize_recent_api_usage,
)
from .recommendation_snapshot_retention import prune_recommendation_score_snapshots


@dataclass(slots=True)
class TaskSpec:
    name: str
    every_seconds: int
    budget_provider: str | None = None
    initial_delay_seconds: int = 0


_RECOMMENDATIONS_WEBHOOK_REPEAT_COOLDOWN_SECONDS = 90 * 24 * 60 * 60
_BUDGET_GATE_WINDOW_SECONDS = 3600
_FAILURE_BACKOFF_MIN_SECONDS = 300
_AUTO_PROJECTED_REQUEST_PERCENTILE = 0.9
_AUTO_PROJECTED_REQUEST_BURST_MIN_HISTORY = 4
_AUTO_PROJECTED_REQUEST_BURST_RATIO = 2.0
_MAL_USER_LIST_REFRESH_MAX_PAGES = 3
_MAL_USER_LIST_INITIAL_DELAY_SECONDS = 15 * 60
_RECOMMENDATION_FULL_HARVEST_INITIAL_DELAY_SECONDS = 75 * 60
_RECOMMENDATION_PROVIDER_ELIGIBILITY_INITIAL_DELAY_SECONDS = 45 * 60
_RECOMMENDATION_PROVIDER_ELIGIBILITY_STAGGER_SECONDS = 15 * 60
_RECOMMENDATION_PROVIDER_ELIGIBILITY_REFRESH_LIMIT = 4
_RECOMMENDATION_PROVIDER_ELIGIBILITY_SEARCH_LIMIT = 5
_RECOMMENDATION_PROVIDER_ELIGIBILITY_QUERIES_PER_CANDIDATE = 1
_SERVICE_STATE_MAX_BYTES = 8 * 1024 * 1024
_LEASE_STATUS_MAX_BYTES = 256 * 1024


class ServiceStateLoadError(RuntimeError):
    def __init__(self, message: str) -> None:
        safe_message = sanitize_text(message, max_length=500)
        super().__init__(safe_message)
        self.safe_message = safe_message


def _now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _iso_after_seconds(seconds: int) -> str:
    return (datetime.now(timezone.utc) + timedelta(seconds=max(0, seconds))).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _load_state(config: AppConfig) -> dict[str, Any]:
    try:
        payload = read_json_dict_bounded(config.service_state_path, max_bytes=_SERVICE_STATE_MAX_BYTES)
    except PersistentJsonError as exc:
        raise ServiceStateLoadError(exc.safe_message) from None
    if payload is None:
        return {"started_at": _now_iso(), "tasks": {}}
    safe = sanitize_value(payload, max_depth=12, max_items=500, max_string=_SUBPROCESS_STREAM_LIMIT)
    if not isinstance(safe, dict):
        raise ServiceStateLoadError(f"type=UnexpectedJsonType file={config.service_state_path.name} expected=object")
    return safe


def _save_state(config: AppConfig, state: dict[str, Any]) -> None:
    safe_state = sanitize_value(state, max_depth=12, max_items=500, max_string=_SUBPROCESS_STREAM_LIMIT)
    if isinstance(safe_state, dict):
        safe_tasks = safe_state.get("tasks")
        if isinstance(safe_tasks, dict):
            for safe_task_state in safe_tasks.values():
                if isinstance(safe_task_state, dict) and isinstance(safe_task_state.get("last_result"), dict):
                    safe_task_state["last_result"] = _persistable_task_result(safe_task_state["last_result"])
    atomic_write_json(config.service_state_path, safe_state, indent=2, sort_keys=True)


def _read_json_dict(path: Any) -> dict[str, Any]:
    try:
        payload = read_json_dict_bounded(path, max_bytes=_LEASE_STATUS_MAX_BYTES)
    except PersistentJsonError:
        return {}
    if payload is None:
        return {}
    safe = sanitize_value(payload, max_depth=8, max_items=100, max_string=1_000)
    return safe if isinstance(safe, dict) else {}


class _ProcessLease:
    """A kernel-backed singleton lease with atomic operator-visible identity metadata."""

    def __init__(self, config: AppConfig, name: str) -> None:
        self.config = config
        self.name = name
        self.lock_path = config.service_leases_dir / f"{name}.lock"
        self.status_path = config.service_leases_dir / f"{name}.json"
        self.run_id = uuid.uuid4().hex
        self._handle: Any = None
        self.status: dict[str, Any] = {}

    def _write_status(self, payload: dict[str, Any]) -> None:
        safe_payload = sanitize_value(payload, max_depth=8, max_items=100, max_string=1_000)
        if not isinstance(safe_payload, dict):
            safe_payload = {}
        atomic_write_json(self.status_path, safe_payload, indent=2, sort_keys=True)
        self.status = safe_payload

    def try_acquire(self, *, phase: str) -> bool:
        self.lock_path.parent.mkdir(parents=True, exist_ok=True)
        handle = self.lock_path.open("a+", encoding="utf-8")
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            handle.close()
            holder = _read_json_dict(self.status_path)
            started_epoch = holder.get("started_epoch")
            age_seconds = max(0, int(time.time() - float(started_epoch))) if isinstance(started_epoch, (int, float)) else None
            self.status = {
                "status": "busy",
                "lease": self.name,
                "holder": holder,
                "holder_age_seconds": age_seconds,
                "holder_is_stale": age_seconds is not None and age_seconds >= int(self.config.service.lease_stale_after_seconds),
                "status_file": str(self.status_path),
            }
            return False

        self._handle = handle
        previous = _read_json_dict(self.status_path)
        now = time.time()
        previous_started = previous.get("started_epoch")
        previous_age = max(0, int(now - float(previous_started))) if isinstance(previous_started, (int, float)) else None
        recovered = bool(previous and previous.get("status") == "running")
        payload: dict[str, Any] = {
            "status": "running",
            "lease": self.name,
            "phase": phase,
            "pid": os.getpid(),
            "run_id": self.run_id,
            "started_epoch": now,
            "started_at": _now_iso(),
            "stale_after_seconds": int(self.config.service.lease_stale_after_seconds),
            "lock_file": str(self.lock_path),
        }
        if recovered:
            payload["recovered_previous_lease"] = True
            payload["previous_run_id"] = previous.get("run_id")
            payload["previous_pid"] = previous.get("pid")
            payload["previous_age_seconds"] = previous_age
            payload["previous_was_stale"] = previous_age is not None and previous_age >= int(self.config.service.lease_stale_after_seconds)
        self._write_status(payload)
        return True

    def update_phase(self, phase: str) -> None:
        if self._handle is None:
            return
        payload = dict(self.status)
        payload["phase"] = phase
        payload["updated_at"] = _now_iso()
        self._write_status(payload)

    @property
    def fileno(self) -> int:
        if self._handle is None:
            raise RuntimeError("lease is not acquired")
        return int(self._handle.fileno())

    def release(self) -> None:
        if self._handle is None:
            return
        payload = dict(self.status)
        payload.update({"status": "released", "released_at": _now_iso(), "released_epoch": time.time()})
        self._write_status(payload)
        fcntl.flock(self._handle.fileno(), fcntl.LOCK_UN)
        self._handle.close()
        self._handle = None


def _set_task_next_due(task_state: dict[str, Any], *, base_epoch: float, every_seconds: int) -> None:
    task_state["every_seconds"] = int(every_seconds)
    task_state["next_due_epoch"] = float(base_epoch) + int(every_seconds)
    task_state["next_due_at"] = _iso_after_seconds(int(task_state["next_due_epoch"] - time.time()))


def _append_log(config: AppConfig, message: str) -> None:
    with config.service_log_path.open("a", encoding="utf-8") as fh:
        fh.write(f"[{_now_iso()}] {sanitize_text(message, max_length=2_000)}\n")


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


def _normalized_request_delta_history(value: object) -> list[int]:
    if not isinstance(value, list):
        return []
    history: list[int] = []
    for item in value:
        if isinstance(item, int):
            history.append(max(0, int(item)))
    return history


def _trimmed_request_delta_history(history: list[int], *, limit: int) -> list[int]:
    normalized_limit = max(1, int(limit))
    if len(history) <= normalized_limit:
        return history
    return history[-normalized_limit:]


def _percentile_request_delta(history: list[int], percentile: float) -> int | None:
    if len(history) < 2:
        return None
    normalized = min(1.0, max(0.0, float(percentile)))
    if normalized <= 0.0:
        return None
    sorted_history = sorted(max(0, int(item)) for item in history)
    index = max(0, min(len(sorted_history) - 1, int(math.ceil(normalized * len(sorted_history))) - 1))
    return sorted_history[index]


def _smoothed_request_delta(history: list[int]) -> int | None:
    if len(history) < 2:
        return None
    return max(0, int(math.ceil(sum(history) / len(history))))


def _auto_projected_request_percentile(history: list[int]) -> float | None:
    if len(history) < _AUTO_PROJECTED_REQUEST_BURST_MIN_HISTORY:
        return None
    smoothed = _smoothed_request_delta(history)
    if smoothed is None or smoothed <= 0:
        return None
    highest = max(max(0, int(item)) for item in history)
    if highest < int(math.ceil(smoothed * _AUTO_PROJECTED_REQUEST_BURST_RATIO)):
        return None
    return _AUTO_PROJECTED_REQUEST_PERCENTILE


def _projected_request_delta_from_history(
    history: list[int],
    *,
    percentile: float | None,
) -> tuple[int | None, str | None, float | None, str | None]:
    chosen_percentile = percentile
    percentile_source = "configured" if chosen_percentile is not None else None
    label_prefix = ""
    if chosen_percentile is None:
        chosen_percentile = _auto_projected_request_percentile(history)
        if chosen_percentile is not None:
            percentile_source = "auto"
            label_prefix = "auto_"
    if chosen_percentile is not None:
        projected = _percentile_request_delta(history, chosen_percentile)
        if projected is not None:
            return projected, f"{label_prefix}p{int(round(chosen_percentile * 100))}", chosen_percentile, percentile_source
    projected = _smoothed_request_delta(history)
    if projected is not None:
        return projected, "smoothed", None, None
    return None, None, None, None


def _record_observed_request_delta(
    task_state: dict[str, Any],
    *,
    observed_request_delta: int,
    fetch_mode: str | None,
    finished_at: str,
    history_limit: int,
) -> None:
    normalized_delta = max(0, int(observed_request_delta))
    task_state["last_request_delta"] = normalized_delta
    task_state["last_request_delta_at"] = finished_at
    history = _normalized_request_delta_history(task_state.get("last_request_delta_history"))
    history.append(normalized_delta)
    task_state["last_request_delta_history"] = _trimmed_request_delta_history(history, limit=history_limit)
    if fetch_mode is None:
        return
    delta_by_mode = task_state.get("last_request_delta_by_mode")
    if not isinstance(delta_by_mode, dict):
        delta_by_mode = {}
    delta_by_mode[fetch_mode] = normalized_delta
    task_state["last_request_delta_by_mode"] = delta_by_mode
    history_by_mode = task_state.get("last_request_delta_history_by_mode")
    if not isinstance(history_by_mode, dict):
        history_by_mode = {}
    mode_history = _normalized_request_delta_history(history_by_mode.get(fetch_mode))
    mode_history.append(normalized_delta)
    history_by_mode[fetch_mode] = _trimmed_request_delta_history(mode_history, limit=history_limit)
    task_state["last_request_delta_history_by_mode"] = history_by_mode


def _finalize_run_request_delta(
    config: AppConfig,
    spec: TaskSpec,
    task_state: dict[str, Any],
    *,
    boundary: Any,
    run_id: str,
    fetch_mode: str | None,
    finished_at: str,
) -> dict[str, Any]:
    if spec.budget_provider is None:
        return {}
    observed_request_delta = count_api_events_since(
        boundary, provider=spec.budget_provider, task=spec.name, run_id=run_id, config=config
    )
    history_mode = fetch_mode if spec.name.startswith("sync_fetch_") else None
    _record_observed_request_delta(
        task_state,
        observed_request_delta=observed_request_delta,
        fetch_mode=history_mode,
        finished_at=finished_at,
        history_limit=config.service.projected_request_history_window_for(spec.name, provider=spec.budget_provider),
    )
    projected_request_count, projected_request_source = _refresh_projected_request_state(
        config, spec, task_state, fetch_mode=history_mode
    )
    payload: dict[str, Any] = {
        "request_delta": observed_request_delta,
        "request_run_id": run_id,
        "next_projected_request_count": projected_request_count,
    }
    if projected_request_source is not None:
        payload["next_projected_request_source"] = projected_request_source
    if history_mode is not None:
        payload["request_delta_by_mode"] = {history_mode: observed_request_delta}
    return payload


_SUBPROCESS_STREAM_LIMIT = 4_000
_MAINTENANCE_RESULT_LIMIT = 50
_SUBPROCESS_JSON_RESULT_FIELDS = {
    "cache_hits",
    "cache_misses",
    "candidates_considered",
    "considered",
    "discovery_considered",
    "discovery_refreshed",
    "eligibility_expired_retries",
    "eligibility_fresh_skips",
    "failed",
    "harvested",
    "provider_detail_probes",
    "provider_searches",
    "queries_selected",
    "refreshed",
    "seed_count",
    "skipped_fresh",
    "total_edges",
}


def _redacted_command(args: list[str]) -> str:
    return sanitize_command(args, max_length=2_000)


def _subprocess_stream(value: object) -> str:
    return sanitize_text(value or "", max_length=_SUBPROCESS_STREAM_LIMIT)


def _persistable_task_result(result: dict[str, Any]) -> dict[str, Any]:
    summary = {
        key: value
        for key, value in result.items()
        if key not in {"stdout", "stderr", "response_text", "payload", "hook_request"}
    }
    for stream_name in ("stdout", "stderr"):
        value = result.get(stream_name)
        if isinstance(value, str) and value.strip():
            summary[f"{stream_name}_snippet"] = sanitize_text(value.strip(), max_length=500)
    safe = sanitize_value(summary, max_depth=6, max_items=100, max_string=1_000)
    return safe if isinstance(safe, dict) else {}


def _project_subprocess_json_stdout(stdout: object) -> dict[str, Any]:
    if not isinstance(stdout, str) or not stdout.strip():
        return {}
    try:
        payload = json.loads(stdout)
    except json.JSONDecodeError:
        return {}
    if not isinstance(payload, dict):
        return {}
    return {
        key: max(0, int(value))
        for key in _SUBPROCESS_JSON_RESULT_FIELDS
        if isinstance((value := payload.get(key)), int) and not isinstance(value, bool)
    }


def _run_subprocess(config: AppConfig, args: list[str], *, label: str, run_id: str | None = None) -> dict[str, Any]:
    inherited_context = current_api_request_context()
    effective_run_id = run_id or inherited_context.run_id or str(uuid.uuid4())
    effective_task = inherited_context.task or label
    env = {
        **__import__("os").environ,
        "PYTHONPATH": str(config.project_root / "src"),
        **request_context_environment(task=effective_task, run_id=effective_run_id),
    }
    timeout_seconds = max(1, int(config.service.task_timeout_seconds))
    command = _redacted_command(args)
    lease = _ProcessLease(config, f"task-{label}")
    if not lease.try_acquire(phase="subprocess"):
        _append_log(config, f"task={label} status=skipped reason=lease_busy command={command}")
        return {
            "status": "skipped",
            "label": label,
            "reason": "lease_busy",
            "command": command,
            "lease": lease.status,
        }
    started = time.monotonic()
    _append_log(config, f"task={label} status=started timeout_seconds={timeout_seconds} command={command}")
    try:
        try:
            result = subprocess.run(
                args,
                cwd=config.project_root,
                text=True,
                capture_output=True,
                check=False,
                env=env,
                timeout=timeout_seconds,
                # If the scheduler is killed mid-task, the child retains the lock
                # until it exits, preventing a restarted daemon from duplicating it.
                pass_fds=(lease.fileno,),
            )
        except subprocess.TimeoutExpired as exc:
            duration_seconds = round(time.monotonic() - started, 3)
            stdout = exc.stdout.decode() if isinstance(exc.stdout, bytes) else (exc.stdout or "")
            stderr = exc.stderr.decode() if isinstance(exc.stderr, bytes) else (exc.stderr or "")
            payload = {
                "status": "error",
                "label": sanitize_text(label, max_length=200),
                "returncode": None,
                "stdout": _subprocess_stream(stdout),
                "stderr": _subprocess_stream(stderr),
                "timed_out": True,
                "timeout_seconds": timeout_seconds,
                "duration_seconds": duration_seconds,
                "command": command,
                "reason": "subprocess_timeout",
            }
            _append_log(config, f"task={label} status=error reason=subprocess_timeout timeout_seconds={timeout_seconds} duration_seconds={duration_seconds} command={command}")
            return payload
    finally:
        lease.release()
    duration_seconds = round(time.monotonic() - started, 3)
    status = "ok" if result.returncode == 0 else "error"
    payload = {
        "status": status,
        "label": sanitize_text(label, max_length=200),
        "returncode": result.returncode,
        "stdout": _subprocess_stream(result.stdout),
        "stderr": _subprocess_stream(result.stderr),
        "timed_out": False,
        "timeout_seconds": timeout_seconds,
        "duration_seconds": duration_seconds,
        "command": command,
    }
    # Parse the complete in-memory child JSON before bounding stdout, but only
    # retain the small operational counter projection used by scheduler lanes.
    payload.update(_project_subprocess_json_stdout(result.stdout))
    if result.returncode == 0:
        _append_log(config, f"task={label} status=ok returncode=0 duration_seconds={duration_seconds} command={command}")
    else:
        detail = (payload["stderr"].strip() or payload["stdout"].strip()).splitlines()[0:1]
        detail_text = detail[0] if detail else ""
        _append_log(config, f"task={sanitize_text(label, max_length=200)} status=error returncode={result.returncode} duration_seconds={duration_seconds} command={command} detail={detail_text}")
    return payload


def _mark_task_running(config: AppConfig, state: dict[str, Any], task_name: str, args: list[str]) -> tuple[float, str]:
    """Persist operator-visible task state before a blocking subprocess starts."""
    started_epoch = time.time()
    started_at = datetime.fromtimestamp(started_epoch, tz=timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    timeout_seconds = max(1, int(config.service.task_timeout_seconds))
    task_state = state.setdefault("tasks", {}).setdefault(task_name, {})
    task_state.update(
        {
            "execution_state": "running",
            "running_started_epoch": started_epoch,
            "running_started_at": started_at,
            "running_command": _redacted_command(args),
            "running_timeout_seconds": timeout_seconds,
            "last_started_at": started_at,
        }
    )
    state["last_loop_at"] = _now_iso()
    _save_state(config, state)
    return started_epoch, started_at


def _clear_task_running(task_state: dict[str, Any]) -> None:
    for key in ("running_started_epoch", "running_started_at", "running_command", "running_timeout_seconds"):
        task_state.pop(key, None)


def _parse_json_stdout(result: dict[str, Any]) -> dict[str, Any] | None:
    stdout = result.get("stdout")
    if not isinstance(stdout, str) or not stdout.strip():
        return None
    try:
        payload = json.loads(stdout)
    except json.JSONDecodeError:
        return None
    if not isinstance(payload, dict):
        return None
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
    providers = _available_source_providers(config)
    for provider in providers:
        specs.append(TaskSpec(f"sync_fetch_{provider}", config.service.sync_every_seconds, budget_provider=provider))
    specs.append(TaskSpec("sync_apply", config.service.sync_every_seconds, budget_provider="mal"))
    mal_secrets = load_mal_secrets(config)
    if int(config.service.mal_list_refresh_every_seconds) > 0 and bool(mal_secrets.access_token):
        specs.append(
            TaskSpec(
                "mal_list_refresh",
                config.service.mal_list_refresh_every_seconds,
                budget_provider="mal",
                initial_delay_seconds=_MAL_USER_LIST_INITIAL_DELAY_SECONDS,
            )
        )
    if int(config.service.recommendation_metadata_refresh_every_seconds) > 0:
        specs.append(TaskSpec("recommend_metadata_refresh", config.service.recommendation_metadata_refresh_every_seconds, budget_provider="mal"))
    if int(config.service.recommendation_full_harvest_every_seconds) > 0:
        specs.append(
            TaskSpec(
                "recommend_full_harvest",
                config.service.recommendation_full_harvest_every_seconds,
                budget_provider="mal",
                initial_delay_seconds=_RECOMMENDATION_FULL_HARVEST_INITIAL_DELAY_SECONDS,
            )
        )
    if int(config.service.provider_eligibility_refresh_every_seconds) > 0:
        for index, provider in enumerate(providers):
            specs.append(
                TaskSpec(
                    f"recommend_provider_eligibility_{provider}",
                    config.service.provider_eligibility_refresh_every_seconds,
                    budget_provider=provider,
                    initial_delay_seconds=(
                        _RECOMMENDATION_PROVIDER_ELIGIBILITY_INITIAL_DELAY_SECONDS
                        + index * _RECOMMENDATION_PROVIDER_ELIGIBILITY_STAGGER_SECONDS
                    ),
                )
            )
    if int(config.service.recommend_maintain_every_seconds) > 0:
        specs.append(TaskSpec("recommend_maintain", config.service.recommend_maintain_every_seconds, budget_provider=None))
    if int(config.service.recommendations_webhook_push_every_seconds) > 0 and config.openclaw.recommendations_webhook_enabled:
        specs.append(TaskSpec("push_recommendations_webhook", config.service.recommendations_webhook_push_every_seconds, budget_provider=None))
    specs.append(TaskSpec("health", config.service.health_every_seconds, budget_provider=None))
    return specs


def effective_niceness_policy(config: AppConfig) -> dict[str, Any]:
    """Return the effective operator-facing cadence, cache, and budget contract."""
    from .recommendation_enrichment import (
        PROVIDER_DETAIL_CACHE_LOGIC_VERSION,
        PROVIDER_ELIGIBILITY_EVIDENCE_TTL_DAYS,
        PROVIDER_SEARCH_CACHE_TTL_DAYS,
    )
    from .recommendation_metadata import (
        DEFAULT_COLD_METADATA_STALE_AFTER_DAYS,
        DEFAULT_HARVEST_STALE_AFTER_DAYS,
        DEFAULT_FULL_USER_RECOMMENDATION_HARVEST_STALE_AFTER_DAYS,
        DEFAULT_HOT_METADATA_STALE_AFTER_DAYS,
        DEFAULT_WARM_METADATA_STALE_AFTER_DAYS,
    )

    task_policies: dict[str, Any] = {}
    for spec in _task_specs(config):
        projected, projected_source = config.service.projected_request_count_for(spec.name)
        task_policy = {
            "every_seconds": int(spec.every_seconds),
            "initial_delay_seconds": int(spec.initial_delay_seconds),
            "budget_provider": spec.budget_provider,
            "budget_scope": config.service.budget_scope_for(spec.budget_provider, task_name=spec.name),
            "task_hourly_limit": (
                config.service.hourly_limit_for(spec.budget_provider, task_name=spec.name)
                if spec.budget_provider and spec.name in config.service.task_hourly_limits
                else None
            ),
            "provider_hourly_limit": (
                config.service.hourly_limit_for(spec.budget_provider) if spec.budget_provider else None
            ),
            "projected_requests": projected,
            "projected_request_source": projected_source,
            "projected_history_window": config.service.projected_request_history_window_for(
                spec.name, provider=spec.budget_provider
            ),
            "projected_percentile": config.service.projected_request_percentile_for(
                spec.name, provider=spec.budget_provider
            ),
        }
        if spec.budget_provider:
            task_policy.update(
                {
                    "warn_backoff_floor_seconds": config.service.backoff_floor_seconds_for(
                        spec.budget_provider, level="warn", task_name=spec.name
                    ),
                    "critical_backoff_floor_seconds": config.service.backoff_floor_seconds_for(
                        spec.budget_provider, level="critical", task_name=spec.name
                    ),
                    "auth_failure_backoff_floor_seconds": config.service.auth_failure_backoff_floor_seconds_for(
                        spec.budget_provider, task_name=spec.name
                    ),
                }
            )
        mode_projections = config.service.task_projected_request_counts_by_mode.get(spec.name)
        if isinstance(mode_projections, dict):
            task_policy["projected_requests_by_mode"] = dict(sorted(mode_projections.items()))
        task_policies[spec.name] = task_policy
    return {
        "policy_kind": "local_niceness_controls_not_external_provider_limits",
        "cadences": {
            "provider_hot_incremental_seconds": int(config.service.sync_every_seconds),
            "provider_cold_full_seconds": int(config.service.full_refresh_every_seconds),
            "provider_cold_full_policy": "weekly_page_bounded_crunchyroll_manual_only_hidive",
            "mal_token_refresh_seconds": int(config.service.mal_refresh_every_seconds),
            "mal_user_list_refresh_seconds": int(config.service.mal_list_refresh_every_seconds),
            "recommendation_metadata_refresh_seconds": int(config.service.recommendation_metadata_refresh_every_seconds),
            "recommendation_full_harvest_seconds": int(config.service.recommendation_full_harvest_every_seconds),
            "provider_eligibility_refresh_seconds": int(config.service.provider_eligibility_refresh_every_seconds),
            "recommendation_snapshot_health_seconds": int(config.service.recommend_maintain_every_seconds),
            "health_seconds": int(config.service.health_every_seconds),
        },
        "thresholds": {
            "warn_ratio": float(config.service.warn_ratio),
            "critical_ratio": float(config.service.critical_ratio),
            "task_and_provider_global_budgets_enforced": True,
        },
        "provider_hourly_budgets": {
            "mal": config.service.hourly_limit_for("mal"),
            "crunchyroll": config.service.hourly_limit_for("crunchyroll"),
            "hidive": config.service.hourly_limit_for("hidive"),
        },
        "request_start_spacing_seconds": {
            "mal": {
                "base": float(config.mal.request_spacing_seconds),
                "jitter": float(config.mal.request_spacing_jitter_seconds),
            },
            "crunchyroll": {
                "base": float(config.crunchyroll.request_spacing_seconds),
                "jitter": float(config.crunchyroll.request_spacing_jitter_seconds),
            },
            "hidive": {
                "base": float(config.hidive.request_spacing_seconds),
                "jitter": float(config.hidive.request_spacing_jitter_seconds),
            },
        },
        "retry_policy": {
            "mal_max_attempts": int(config.mal.retry_max_attempts),
            "crunchyroll_max_attempts": int(config.crunchyroll.retry_max_attempts),
            "hidive_max_attempts": int(config.hidive.retry_max_attempts),
            "mal_writes_retried": False,
        },
        "execute_limits": dict(sorted(config.service.task_execute_limits.items())),
        "task_policies": task_policies,
        "cold_refresh_bounds": {
            "crunchyroll_max_history_pages": int(config.service.crunchyroll_provider_max_history_pages),
            "crunchyroll_max_watchlist_pages": int(config.service.crunchyroll_provider_max_watchlist_pages),
            "hidive_unattended_full_refresh": False,
        },
        "cache_horizons_days": {
            "mal_search_positive": int(config.mal.search_cache_ttl_days),
            "mal_search_negative": int(config.mal.search_negative_cache_ttl_days),
            "mal_detail": int(config.mal.detail_cache_ttl_days),
            "provider_detail": int(config.mal.provider_detail_cache_ttl_days),
            "provider_search": PROVIDER_SEARCH_CACHE_TTL_DAYS,
            "provider_eligibility_evidence": PROVIDER_ELIGIBILITY_EVIDENCE_TTL_DAYS,
            "recommendation_harvest": DEFAULT_HARVEST_STALE_AFTER_DAYS,
            "recommendation_full_userrecs_harvest": int(getattr(config.service, "recommendation_full_harvest_stale_after_days", DEFAULT_FULL_USER_RECOMMENDATION_HARVEST_STALE_AFTER_DAYS)),
            "recommendation_metadata_hot": DEFAULT_HOT_METADATA_STALE_AFTER_DAYS,
            "recommendation_metadata_warm": DEFAULT_WARM_METADATA_STALE_AFTER_DAYS,
            "recommendation_metadata_cold": DEFAULT_COLD_METADATA_STALE_AFTER_DAYS,
        },
        "provider_detail_logic_version": PROVIDER_DETAIL_CACHE_LOGIC_VERSION,
    }


def _provider_fetch_command(config: AppConfig, provider: str, *, full_refresh: bool = False) -> list[str]:
    command: list[str]
    if provider == "crunchyroll":
        snapshot_path = config.cache_dir / "live-crunchyroll-snapshot.json"
        command = [
            sys.executable,
            "-m",
            "mal_updater.cli",
            "provider-fetch-snapshot",
            "--provider",
            "crunchyroll",
            "--out",
            str(snapshot_path),
            "--ingest",
        ]
    elif provider == "hidive":
        snapshot_path = config.cache_dir / "live-hidive-snapshot.json"
        command = [
            sys.executable,
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
    if provider == "crunchyroll":
        _append_positive_int_arg(command, "--max-history-pages", config.service.crunchyroll_provider_max_history_pages)
        _append_positive_int_arg(command, "--max-watchlist-pages", config.service.crunchyroll_provider_max_watchlist_pages)
    return command


def _append_positive_int_arg(command: list[str], flag: str, value: int | None) -> None:
    if value is not None and int(value) > 0:
        command.extend([flag, str(int(value))])


def _remove_flag_with_value(command: list[str], flag: str) -> None:
    while flag in command:
        index = command.index(flag)
        del command[index : index + 2]


def maintenance_cycle_plan(
    config: AppConfig,
    *,
    metadata_limit: int = 25,
    discovery_target_limit: int = 25,
    recommendation_limit: int = 100,
    mapping_limit: int = 25,
    mal_list_max_pages: int = _MAL_USER_LIST_REFRESH_MAX_PAGES,
    provider_max_history_pages: int | None = None,
    provider_max_watchlist_pages: int | None = None,
    include_provider_refresh: bool = True,
    local_only: bool = False,
) -> list[dict[str, Any]]:
    """Build the unattended, write-conservative maintenance command sequence.

    Commands are intentionally CLI-shaped so the daemon/service and tests exercise the same
    surface as an operator.  Provider chunk limits are passed through only where supported;
    the ingest command already refuses to record partial Crunchyroll chunks as completed full
    refreshes.
    """

    commands: list[dict[str, Any]] = []
    if include_provider_refresh and not local_only:
        for provider in _available_source_providers(config):
            command = _provider_fetch_command(config, provider, full_refresh=False)
            if provider == "crunchyroll":
                if provider_max_history_pages is not None:
                    _remove_flag_with_value(command, "--max-history-pages")
                    _append_positive_int_arg(command, "--max-history-pages", provider_max_history_pages)
                if provider_max_watchlist_pages is not None:
                    _remove_flag_with_value(command, "--max-watchlist-pages")
                    _append_positive_int_arg(command, "--max-watchlist-pages", provider_max_watchlist_pages)
            commands.append({"label": f"maintain_provider_refresh_{provider}", "args": command})

    if not local_only:
        commands.extend(
            [
            {
                "label": "maintain_safe_mapping_review",
                "args": [
                    sys.executable,
                    "-m",
                    "mal_updater.cli",
                    "apply-sync",
                    "--limit",
                    str(max(0, int(mapping_limit))),
                    "--exact-approved-only",
                ],
            },
            {
                "label": "maintain_mal_list_refresh",
                "args": [
                    sys.executable,
                    "-m",
                    "mal_updater.cli",
                    "mal-list-refresh",
                    "--max-pages",
                    str(max(1, int(mal_list_max_pages))),
                ],
            },
            {
                "label": "maintain_recommend_provider_eligibility",
                "args": [
                    sys.executable,
                    "-m",
                    "mal_updater.cli",
                    "recommend-enrich-provider-availability",
                    "--limit",
                    str(_RECOMMENDATION_PROVIDER_ELIGIBILITY_REFRESH_LIMIT),
                    "--search-limit",
                    str(_RECOMMENDATION_PROVIDER_ELIGIBILITY_SEARCH_LIMIT),
                    "--queries-per-candidate",
                    str(_RECOMMENDATION_PROVIDER_ELIGIBILITY_QUERIES_PER_CANDIDATE),
                ],
            },
            ]
        )
    commands.extend(
        [
            {
                "label": "maintain_recommend_snapshot",
                "args": [
                    sys.executable,
                    "-m",
                    "mal_updater.cli",
                    "recommend",
                    "--limit",
                    str(max(0, int(recommendation_limit))),
                    "--persist-snapshot",
                ],
            },
            {
                "label": "maintain_health",
                "args": [sys.executable, "-m", "mal_updater.cli", "health-check", "--format", "json"],
            },
        ]
    )
    return commands


def _run_maintenance_cycle_unlocked(
    config: AppConfig,
    *,
    dry_run: bool = False,
    metadata_limit: int = 25,
    discovery_target_limit: int = 25,
    recommendation_limit: int = 100,
    mapping_limit: int = 25,
    mal_list_max_pages: int = _MAL_USER_LIST_REFRESH_MAX_PAGES,
    provider_max_history_pages: int | None = None,
    provider_max_watchlist_pages: int | None = None,
    include_provider_refresh: bool = True,
    local_only: bool = False,
) -> dict[str, Any]:
    plan = maintenance_cycle_plan(
        config,
        metadata_limit=metadata_limit,
        discovery_target_limit=discovery_target_limit,
        recommendation_limit=recommendation_limit,
        mapping_limit=mapping_limit,
        mal_list_max_pages=mal_list_max_pages,
        provider_max_history_pages=provider_max_history_pages,
        provider_max_watchlist_pages=provider_max_watchlist_pages,
        include_provider_refresh=include_provider_refresh,
        local_only=local_only,
    )
    try:
        state = _load_state(config)
    except ServiceStateLoadError as exc:
        _append_log(config, f"task=recommend_maintain status=error reason=service_state_unavailable detail={exc.safe_message}")
        return {
            "status": "error",
            "reason": "service_state_unavailable",
            "service_state_parse_error": exc.safe_message,
            "state_file": str(config.service_state_path),
        }
    task_state = state.setdefault("tasks", {}).setdefault("recommend_maintain", {})
    started_epoch = time.time()
    started_at = datetime.fromtimestamp(started_epoch, tz=timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")

    if dry_run:
        task_state.update({"last_status": "dry_run", "last_plan_size": len(plan), "last_run_at": started_at})
        state["last_maintenance_cycle_at"] = started_at
        _save_state(config, state)
        return {"status": "dry_run", "commands": plan, "state_file": str(config.service_state_path)}

    results: list[dict[str, Any]] = []
    failures: list[dict[str, Any]] = []
    for step in plan:
        command_args = list(step["args"])
        _mark_task_running(config, state, "recommend_maintain", command_args)
        result = _run_subprocess(config, command_args, label=str(step["label"]))
        bounded_result = sanitize_value(result, max_depth=5, max_items=50, max_string=_SUBPROCESS_STREAM_LIMIT)
        if isinstance(bounded_result, dict):
            results.append(bounded_result)
        if result.get("status") != "ok":
            failures.append({
                "label": sanitize_text(step["label"], max_length=200),
                "returncode": result.get("returncode"),
                "reason": _summarize_task_failure(result) or "subprocess_error",
            })

    finished_epoch = time.time()
    finished_at = datetime.fromtimestamp(finished_epoch, tz=timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    status = "ok" if not failures else "partial_error"
    task_state.update({"last_status": status, "last_run_at": finished_at, "last_run_epoch": finished_epoch, "last_failure_count": len(failures), "execution_state": "idle"})
    _clear_task_running(task_state)
    _record_task_timing(task_state, started_epoch=started_epoch, finished_epoch=finished_epoch, started_at=started_at, finished_at=finished_at)
    if int(config.service.recommend_maintain_every_seconds) > 0:
        _set_task_next_due(task_state, base_epoch=finished_epoch, every_seconds=config.service.recommend_maintain_every_seconds)
    if failures:
        task_state["last_errors"] = failures[-_MAINTENANCE_RESULT_LIMIT:]
    else:
        task_state.pop("last_errors", None)
    state["last_maintenance_cycle_at"] = finished_at
    _save_state(config, state)
    return {
        "status": status,
        "results": results[-_MAINTENANCE_RESULT_LIMIT:],
        "failures": failures[-_MAINTENANCE_RESULT_LIMIT:],
        "state_file": str(config.service_state_path),
    }


def run_maintenance_cycle(config: AppConfig, **kwargs: Any) -> dict[str, Any]:
    ensure_directories(config)
    lease = _ProcessLease(config, "recommend-maintain")
    if not lease.try_acquire(phase="maintenance"):
        _append_log(config, "task=recommend_maintain status=skipped reason=lease_busy")
        return {"status": "skipped", "reason": "lease_busy", "lease": lease.status}
    try:
        result = _run_maintenance_cycle_unlocked(config, **kwargs)
        result["lease"] = {"run_id": lease.run_id, "status_file": str(lease.status_path)}
        return result
    finally:
        lease.release()



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
    if len(command_args) >= 2 and command_args[0] == "sync-source" and isinstance(command_args[1], str):
        return str(command_args[1])
    if command_args[0] == "provider-fetch-snapshot":
        for index, part in enumerate(command_args[:-1]):
            if part == "--provider" and isinstance(command_args[index + 1], str):
                return str(command_args[index + 1])
    return None



def _provider_fetch_health_request(config: AppConfig, provider: str, task_state: dict[str, Any]) -> dict[str, Any] | None:
    path = config.health_latest_json_path
    try:
        payload = read_json_dict_bounded(path, max_bytes=_SERVICE_STATE_MAX_BYTES)
    except PersistentJsonError:
        return None
    if payload is None:
        return None
    maintenance = payload.get("maintenance")
    if not isinstance(maintenance, dict):
        return None
    commands = maintenance.get("recommended_commands")
    if not isinstance(commands, list):
        return None
    try:
        health_mtime = path.stat().st_mtime
    except OSError:
        return None
    last_handled_mtime = task_state.get("last_health_request_handled_mtime")
    if isinstance(last_handled_mtime, (int, float)) and float(last_handled_mtime) >= float(health_mtime):
        return None
    for command in commands:
        if not isinstance(command, dict):
            continue
        reason_code = command.get("reason_code")
        if reason_code not in {"refresh_ingested_snapshot", "refresh_full_snapshot"}:
            continue
        if _provider_from_refresh_command_args(command.get("command_args")) != provider:
            continue
        if reason_code == "refresh_full_snapshot" and int(config.service.full_refresh_every_seconds) <= 0:
            continue
        if reason_code == "refresh_full_snapshot":
            last_full_refresh_epoch = task_state.get("last_successful_full_refresh_epoch")
            if isinstance(last_full_refresh_epoch, (int, float)) and float(last_full_refresh_epoch) >= float(health_mtime):
                continue
        return {
            "reason_code": reason_code,
            "mode": "full_refresh" if reason_code == "refresh_full_snapshot" else "hot",
            "health_mtime": float(health_mtime),
        }
    return None



def _apply_sync_command(config: AppConfig) -> list[str]:
    apply_limit = config.service.execute_limit_for("sync_apply")
    if apply_limit is None:
        apply_limit = DEFAULT_SERVICE_TASK_EXECUTE_LIMITS.get("sync_apply", 0)
    return [
        sys.executable,
        "-m",
        "mal_updater.cli",
        "apply-sync",
        "--limit",
        str(max(0, int(apply_limit))),
        "--exact-approved-only",
        "--execute",
    ]


def _recommendation_metadata_refresh_command(config: AppConfig) -> list[str]:
    seed_limit = config.service.execute_limit_for("recommend_metadata_refresh")
    if seed_limit is None:
        seed_limit = DEFAULT_SERVICE_TASK_EXECUTE_LIMITS.get("recommend_metadata_refresh", 0)
    discovery_target_limit = config.service.execute_limit_for("recommend_metadata_discovery_targets")
    if discovery_target_limit is None:
        discovery_target_limit = DEFAULT_SERVICE_TASK_EXECUTE_LIMITS.get("recommend_metadata_discovery_targets", 0)
    return [
        sys.executable,
        "-m",
        "mal_updater.cli",
        "recommend-refresh-metadata",
        "--limit",
        str(max(0, int(seed_limit))),
        "--include-discovery-targets",
        "--discovery-target-limit",
        str(max(0, int(discovery_target_limit))),
    ]


def _recommendation_full_harvest_command(config: AppConfig) -> list[str]:
    source_limit = config.service.execute_limit_for("recommend_full_harvest")
    if source_limit is None:
        source_limit = DEFAULT_SERVICE_TASK_EXECUTE_LIMITS.get("recommend_full_harvest", 0)
    max_pages = config.service.execute_limit_for("recommend_full_harvest_pages")
    if max_pages is None:
        max_pages = DEFAULT_SERVICE_TASK_EXECUTE_LIMITS.get("recommend_full_harvest_pages", 1)
    return [
        sys.executable,
        "-m",
        "mal_updater.cli",
        "recommend-refresh-full-userrecs",
        "--limit",
        str(max(0, int(source_limit))),
        "--stale-after-days",
        str(max(1, int(config.service.recommendation_full_harvest_stale_after_days))),
        "--max-pages",
        str(max(1, int(max_pages))),
    ]


def _mal_list_refresh_command(config: AppConfig) -> list[str]:
    max_pages = config.service.execute_limit_for("mal_list_refresh_pages")
    if max_pages is None:
        max_pages = DEFAULT_SERVICE_TASK_EXECUTE_LIMITS.get("mal_list_refresh_pages", _MAL_USER_LIST_REFRESH_MAX_PAGES)
    return [
        sys.executable,
        "-m",
        "mal_updater.cli",
        "mal-list-refresh",
        "--max-pages",
        str(max(1, int(max_pages))),
    ]


def _provider_eligibility_command(config: AppConfig, provider: str) -> list[str]:
    candidate_limit = config.service.execute_limit_for("recommend_provider_eligibility_candidates")
    if candidate_limit is None:
        candidate_limit = DEFAULT_SERVICE_TASK_EXECUTE_LIMITS.get(
            "recommend_provider_eligibility_candidates", _RECOMMENDATION_PROVIDER_ELIGIBILITY_REFRESH_LIMIT
        )
    search_limit = config.service.execute_limit_for("recommend_provider_eligibility_search_results")
    if search_limit is None:
        search_limit = DEFAULT_SERVICE_TASK_EXECUTE_LIMITS.get(
            "recommend_provider_eligibility_search_results", _RECOMMENDATION_PROVIDER_ELIGIBILITY_SEARCH_LIMIT
        )
    query_limit = config.service.execute_limit_for("recommend_provider_eligibility_queries_per_candidate")
    if query_limit is None:
        query_limit = DEFAULT_SERVICE_TASK_EXECUTE_LIMITS.get(
            "recommend_provider_eligibility_queries_per_candidate",
            _RECOMMENDATION_PROVIDER_ELIGIBILITY_QUERIES_PER_CANDIDATE,
        )
    return [
        sys.executable,
        "-m",
        "mal_updater.cli",
        "recommend-enrich-provider-availability",
        "--provider",
        provider,
        "--limit",
        str(max(0, int(candidate_limit))),
        "--search-limit",
        str(max(1, int(search_limit))),
        "--queries-per-candidate",
        str(max(1, int(query_limit))),
    ]


def _recommend_maintain_command(config: AppConfig) -> list[str]:
    recommendation_limit = config.service.execute_limit_for("recommendation_snapshot")
    if recommendation_limit is None:
        recommendation_limit = DEFAULT_SERVICE_TASK_EXECUTE_LIMITS.get("recommendation_snapshot", 100)
    mapping_limit = config.service.execute_limit_for("sync_apply")
    if mapping_limit is None:
        mapping_limit = DEFAULT_SERVICE_TASK_EXECUTE_LIMITS.get("sync_apply", 0)
    return [
        sys.executable,
        "-m",
        "mal_updater.cli",
        "recommend-maintain",
        "--recommendation-limit",
        str(max(0, int(recommendation_limit))),
        "--mapping-limit",
        str(max(0, int(mapping_limit))),
        # Network work belongs to the individually budgeted MAL/provider lanes.
        # This frequent maintenance pass materializes DB-local snapshot/health only.
        "--local-only",
    ]


def _recommendations_webhook_push_limit(config: AppConfig) -> int:
    push_limit = config.service.execute_limit_for("push_recommendations_webhook")
    if push_limit is None:
        push_limit = 20
    return max(0, int(push_limit))


def _delivery_fingerprint_history(task_state: dict[str, Any], *, now_epoch: float) -> dict[str, float]:
    raw_history = task_state.get("delivery_item_fingerprint_history")
    history: dict[str, float] = {}
    if isinstance(raw_history, dict):
        for fingerprint, delivered_epoch in raw_history.items():
            if not isinstance(fingerprint, str) or not fingerprint:
                continue
            if isinstance(delivered_epoch, (int, float)):
                history[fingerprint] = float(delivered_epoch)
    legacy_fingerprints = task_state.get("last_delivery_item_fingerprints")
    if isinstance(legacy_fingerprints, list):
        legacy_epoch = task_state.get("last_delivery_epoch") or task_state.get("last_run_epoch") or now_epoch
        if isinstance(legacy_epoch, (int, float)):
            for fingerprint in legacy_fingerprints:
                if isinstance(fingerprint, str) and fingerprint:
                    history.setdefault(fingerprint, float(legacy_epoch))
    return history


def _recent_delivery_fingerprints(task_state: dict[str, Any], *, now_epoch: float) -> set[str]:
    cutoff_epoch = now_epoch - _RECOMMENDATIONS_WEBHOOK_REPEAT_COOLDOWN_SECONDS
    return {
        fingerprint
        for fingerprint, delivered_epoch in _delivery_fingerprint_history(task_state, now_epoch=now_epoch).items()
        if delivered_epoch >= cutoff_epoch
    }


def _record_delivery_fingerprints(task_state: dict[str, Any], item_fingerprints: list[str], *, now_epoch: float) -> None:
    cutoff_epoch = now_epoch - _RECOMMENDATIONS_WEBHOOK_REPEAT_COOLDOWN_SECONDS
    history = {
        fingerprint: delivered_epoch
        for fingerprint, delivered_epoch in _delivery_fingerprint_history(task_state, now_epoch=now_epoch).items()
        if delivered_epoch >= cutoff_epoch
    }
    for fingerprint in item_fingerprints:
        if fingerprint:
            history[fingerprint] = now_epoch
    task_state["delivery_item_fingerprint_history"] = dict(sorted(history.items()))
    task_state["last_delivery_item_fingerprints"] = item_fingerprints
    task_state["last_delivery_epoch"] = now_epoch


def _push_recommendations_webhook_task(config: AppConfig, task_state: dict[str, Any]) -> dict[str, Any]:
    now_epoch = time.time()
    delivery_limit = _recommendations_webhook_push_limit(config)
    delivery_mode = config.openclaw.recommendations_webhook_delivery_mode
    suppressed_fingerprints = _recent_delivery_fingerprints(task_state, now_epoch=now_epoch)
    preview = deliver_recommendations_via_openclaw(
        config,
        limit=delivery_limit,
        include_dormant=False,
        delivery_mode=delivery_mode,
        suppress_item_fingerprints=suppressed_fingerprints,
        dry_run=True,
    )
    structured_payload = preview.payload.get("structured_payload") if isinstance(preview.payload, dict) else None
    item_fingerprints = []
    if isinstance(structured_payload, dict):
        raw_fingerprints = structured_payload.get("item_fingerprints")
        if isinstance(raw_fingerprints, list):
            item_fingerprints = [str(value) for value in raw_fingerprints if isinstance(value, str) and value]
    request_id = preview.request_id
    previous_request_id = task_state.get("last_delivery_request_id")
    if not item_fingerprints:
        return {
            "status": "ok",
            "label": "push_recommendations_webhook",
            "delivery_status": "unchanged_recent_items",
            "delivery_limit": delivery_limit,
            "delivery_mode": delivery_mode,
            "request_id": request_id,
            "request_url": sanitize_url(preview.request_url) if preview.request_url else None,
            "suppressed_recent_item_count": len(suppressed_fingerprints),
            "repeat_cooldown_days": 90,
        }
    if request_id and previous_request_id == request_id:
        return {
            "status": "ok",
            "label": "push_recommendations_webhook",
            "delivery_status": "unchanged",
            "delivery_limit": delivery_limit,
            "delivery_mode": delivery_mode,
            "request_id": request_id,
            "request_url": sanitize_url(preview.request_url) if preview.request_url else None,
            "suppressed_recent_item_count": len(suppressed_fingerprints),
            "repeat_cooldown_days": 90,
        }
    delivery = deliver_recommendations_via_openclaw(
        config,
        limit=delivery_limit,
        include_dormant=False,
        delivery_mode=delivery_mode,
        suppress_item_fingerprints=suppressed_fingerprints,
        dry_run=False,
    )
    result = {
        "status": "ok",
        "label": "push_recommendations_webhook",
        "delivery_status": delivery.status,
        "delivery_limit": delivery_limit,
        "delivery_mode": delivery_mode,
        "request_id": delivery.request_id,
        "request_url": sanitize_url(delivery.request_url) if delivery.request_url else None,
        "http_status": delivery.http_status,
        "suppressed_recent_item_count": len(suppressed_fingerprints),
        "repeat_cooldown_days": 90,
    }
    if delivery.reason is not None:
        result["reason"] = sanitize_text(delivery.reason, max_length=500)
    if delivery.status == "delivered" and delivery.request_id:
        task_state["last_delivery_request_id"] = delivery.request_id
        task_state["last_delivery_http_status"] = delivery.http_status
        _record_delivery_fingerprints(task_state, item_fingerprints, now_epoch=now_epoch)
    elif delivery.status == "no_recommendations":
        task_state.pop("last_delivery_request_id", None)
        task_state.pop("last_delivery_http_status", None)
        task_state.pop("last_delivery_item_fingerprints", None)
        task_state.pop("last_delivery_epoch", None)
    return result


def _task_execution_signature(config: AppConfig, spec: TaskSpec, *, fetch_mode: str | None = None) -> str | None:
    if spec.name == "mal_list_refresh":
        max_pages = config.service.execute_limit_for("mal_list_refresh_pages")
        return f"mal_list_refresh:max_pages={max_pages}"
    if spec.name == "sync_apply":
        apply_limit = config.service.execute_limit_for("sync_apply")
        if apply_limit is None:
            apply_limit = DEFAULT_SERVICE_TASK_EXECUTE_LIMITS.get("sync_apply", 0)
        return f"sync_apply:limit={max(0, int(apply_limit))}"
    if spec.name == "recommend_metadata_refresh":
        seed_limit = config.service.execute_limit_for("recommend_metadata_refresh")
        if seed_limit is None:
            seed_limit = DEFAULT_SERVICE_TASK_EXECUTE_LIMITS.get("recommend_metadata_refresh", 0)
        discovery_target_limit = config.service.execute_limit_for("recommend_metadata_discovery_targets")
        if discovery_target_limit is None:
            discovery_target_limit = DEFAULT_SERVICE_TASK_EXECUTE_LIMITS.get("recommend_metadata_discovery_targets", 0)
        return f"recommend_metadata_refresh:limit={max(0, int(seed_limit))}:discovery_target_limit={max(0, int(discovery_target_limit))}"
    if spec.name == "recommend_full_harvest":
        source_limit = config.service.execute_limit_for("recommend_full_harvest")
        if source_limit is None:
            source_limit = DEFAULT_SERVICE_TASK_EXECUTE_LIMITS.get("recommend_full_harvest", 0)
        max_pages = config.service.execute_limit_for("recommend_full_harvest_pages")
        if max_pages is None:
            max_pages = DEFAULT_SERVICE_TASK_EXECUTE_LIMITS.get("recommend_full_harvest_pages", 1)
        return (
            f"recommend_full_harvest:limit={max(0, int(source_limit))}"
            f":stale_after_days={max(1, int(config.service.recommendation_full_harvest_stale_after_days))}"
            f":max_pages={max(1, int(max_pages))}"
        )
    if spec.name.startswith("recommend_provider_eligibility_"):
        provider = spec.name.removeprefix("recommend_provider_eligibility_")
        return f"{spec.name}:provider={provider}:command={' '.join(_provider_eligibility_command(config, provider)[4:])}"
    if spec.name == "push_recommendations_webhook":
        return f"push_recommendations_webhook:limit={_recommendations_webhook_push_limit(config)}:mode={config.openclaw.recommendations_webhook_delivery_mode}"
    if spec.name.startswith("sync_fetch_"):
        return f"{spec.name}:mode={fetch_mode or 'hot'}"
    return None


def _maybe_reset_task_projection_state_for_signature(
    config: AppConfig,
    spec: TaskSpec,
    task_state: dict[str, Any],
    *,
    fetch_mode: str | None = None,
) -> None:
    signature = _task_execution_signature(config, spec, fetch_mode=fetch_mode)
    if signature is None:
        return
    previous = task_state.get("execution_signature")
    if previous == signature:
        return
    task_state["execution_signature"] = signature
    # Adopt legacy state without discarding useful learned deltas. Only a known
    # signature change proves that the execution shape became incomparable.
    if previous is None:
        return
    for key in (
        "last_request_delta",
        "last_request_delta_at",
        "last_request_delta_history",
        "last_request_delta_by_mode",
        "last_request_delta_history_by_mode",
        "projected_request_count",
        "projected_request_total",
        "projected_ratio",
        "projected_request_source",
        "budget_backoff_level",
        "budget_backoff_until_epoch",
        "budget_backoff_until",
        "budget_backoff_remaining_seconds",
        "budget_backoff_floor_seconds",
        "budget_backoff_cooldown_source",
        "last_skip_reason",
        "last_skipped_at",
    ):
        task_state.pop(key, None)
    _append_log(config, f"task={spec.name} status=reset reason=execution_signature_changed old={previous!r} new={signature!r}")



def _planned_fetch_mode(config: AppConfig, spec: TaskSpec, task_state: dict[str, Any], *, now: float) -> tuple[str | None, list[str], dict[str, Any] | None]:
    if not spec.name.startswith("sync_fetch_"):
        return None, [], None
    provider = spec.name.removeprefix("sync_fetch_")
    full_refresh_reasons: list[str] = []
    health_request = _provider_fetch_health_request(config, provider, task_state)
    # Crunchyroll cold work is page-bounded by the daemon command. HIDIVE does
    # not expose chunk controls, so its full refresh remains an explicit manual
    # operation rather than an accidental unattended crawl.
    unattended_full_refresh_supported = provider == "crunchyroll"
    if unattended_full_refresh_supported and _provider_fetch_requires_full_refresh(config, task_state, now=now):
        full_refresh_reasons.append("periodic_cadence")
    if unattended_full_refresh_supported and isinstance(health_request, dict) and health_request.get("mode") == "full_refresh":
        full_refresh_reasons.append("health_recommended")
    if not unattended_full_refresh_supported and isinstance(health_request, dict) and health_request.get("mode") == "full_refresh":
        return "hot", [], health_request
    if isinstance(health_request, dict) and health_request.get("mode") in {"hot", "incremental"}:
        return "hot", ["health_recommended_hot"], health_request
    return ("full_refresh" if full_refresh_reasons else "hot"), full_refresh_reasons, health_request



def _maybe_downgrade_fetch_mode_for_budget(
    config: AppConfig,
    spec: TaskSpec,
    task_state: dict[str, Any],
    *,
    planned_fetch_mode: str | None,
    planned_full_refresh_reasons: list[str],
    allowed: bool,
    reason: str | None,
    usage: dict[str, Any] | None,
) -> tuple[bool, str | None, dict[str, Any] | None, str | None, list[str], str | None, dict[str, Any] | None]:
    if allowed or not spec.name.startswith("sync_fetch_") or planned_fetch_mode != "full_refresh":
        return allowed, reason, usage, planned_fetch_mode, planned_full_refresh_reasons, None, None
    hot_allowed, hot_reason, hot_usage = _budget_gate(config, spec, task_state, fetch_mode="hot")
    if not hot_allowed:
        return allowed, reason, usage, planned_fetch_mode, planned_full_refresh_reasons, None, hot_usage
    deferred_reason = "+".join(planned_full_refresh_reasons) if planned_full_refresh_reasons else "budget_deferred"
    return True, None, hot_usage, "hot", [], deferred_reason, hot_usage



def _projected_request_policy_details(
    config: AppConfig,
    spec: TaskSpec,
    task_state: dict[str, Any],
    *,
    fetch_mode: str | None = None,
) -> dict[str, Any]:
    history_window = config.service.projected_request_history_window_for(spec.name, provider=spec.budget_provider)
    details: dict[str, Any] = {
        "projected_request_history_window": history_window,
    }
    history = _normalized_request_delta_history(task_state.get("last_request_delta_history"))
    if fetch_mode:
        details["projected_request_history_mode"] = fetch_mode
        history_by_mode = task_state.get("last_request_delta_history_by_mode")
        if isinstance(history_by_mode, dict):
            history = _normalized_request_delta_history(history_by_mode.get(fetch_mode))
        else:
            history = []
    details["projected_request_history_sample_count"] = len(history)
    percentile = config.service.projected_request_percentile_for(spec.name, provider=spec.budget_provider)
    if percentile is not None:
        details["projected_request_percentile"] = round(float(percentile), 6)
        details["projected_request_percentile_source"] = "configured"
        return details
    auto_percentile = _auto_projected_request_percentile(history)
    if auto_percentile is not None:
        details["projected_request_percentile"] = round(float(auto_percentile), 6)
        details["projected_request_percentile_source"] = "auto"
    return details



def _projected_request_count(
    config: AppConfig,
    spec: TaskSpec,
    task_state: dict[str, Any],
    *,
    fetch_mode: str | None = None,
) -> tuple[int, str | None]:
    configured, configured_source = config.service.projected_request_count_for(spec.name, fetch_mode=fetch_mode)
    built_in_mode_default = None
    if fetch_mode:
        built_in_mode_default = DEFAULT_SERVICE_TASK_PROJECTED_REQUEST_COUNTS_BY_MODE.get(spec.name, {}).get(fetch_mode)
        if built_in_mode_default is None and fetch_mode == "hot":
            built_in_mode_default = DEFAULT_SERVICE_TASK_PROJECTED_REQUEST_COUNTS_BY_MODE.get(spec.name, {}).get("incremental")
    built_in_task_default = DEFAULT_SERVICE_TASK_PROJECTED_REQUEST_COUNTS.get(spec.name)
    task_wide_configured = config.service.task_projected_request_counts.get(spec.name)

    use_mode_default_as_cold_start_seed = (
        fetch_mode is not None
        and configured_source == f"configured_{fetch_mode}"
        and isinstance(configured, int)
        and built_in_mode_default == configured
    )
    use_task_default_as_cold_start_seed = (
        configured_source == "configured"
        and isinstance(configured, int)
        and built_in_task_default == configured
    )

    if use_mode_default_as_cold_start_seed and isinstance(task_wide_configured, int):
        return max(0, int(task_wide_configured)), "configured"
    if configured is not None and not use_mode_default_as_cold_start_seed and not use_task_default_as_cold_start_seed:
        return configured, configured_source
    percentile = config.service.projected_request_percentile_for(spec.name, provider=spec.budget_provider)
    if fetch_mode:
        history_by_mode = task_state.get("last_request_delta_history_by_mode")
        if isinstance(history_by_mode, dict):
            projected_mode, projected_mode_label, _projected_mode_percentile, _projected_mode_percentile_source = _projected_request_delta_from_history(
                _normalized_request_delta_history(history_by_mode.get(fetch_mode)),
                percentile=percentile,
            )
            if projected_mode is not None and projected_mode_label is not None:
                return projected_mode, f"observed_{fetch_mode}_{projected_mode_label}"
        if isinstance(task_state.get("last_request_delta_by_mode"), dict):
            mode_value = task_state["last_request_delta_by_mode"].get(fetch_mode)
            if isinstance(mode_value, int):
                return max(0, int(mode_value)), f"observed_{fetch_mode}"
    projected, projected_label, _projected_percentile, _projected_percentile_source = _projected_request_delta_from_history(
        _normalized_request_delta_history(task_state.get("last_request_delta_history")),
        percentile=percentile,
    )
    if projected is not None and projected_label is not None:
        return projected, f"observed_{projected_label}"
    value = task_state.get("last_request_delta")
    if isinstance(value, int):
        return max(0, int(value)), "observed_last_run"
    if configured is not None:
        return configured, configured_source
    return 0, None



def _refresh_projected_request_state(
    config: AppConfig,
    spec: TaskSpec,
    task_state: dict[str, Any],
    *,
    fetch_mode: str | None = None,
) -> tuple[int, str | None]:
    projected_request_count, projected_request_source = _projected_request_count(config, spec, task_state, fetch_mode=fetch_mode)
    task_state["projected_request_count"] = projected_request_count
    if projected_request_source is not None:
        task_state["projected_request_source"] = projected_request_source
    else:
        task_state.pop("projected_request_source", None)
    for field, value in _projected_request_policy_details(config, spec, task_state, fetch_mode=fetch_mode).items():
        task_state[field] = value
    return projected_request_count, projected_request_source


def _budget_gate(
    config: AppConfig,
    spec: TaskSpec,
    task_state: dict[str, Any],
    *,
    fetch_mode: str | None = None,
) -> tuple[bool, str | None, dict[str, Any] | None]:
    provider = spec.budget_provider
    if provider is None:
        return True, None, None
    global_usage = summarize_recent_api_usage(provider=provider, window_seconds=_BUDGET_GATE_WINDOW_SECONDS, config=config).as_dict()
    task_usage = summarize_recent_api_usage(
        provider=provider, task=spec.name, include_legacy_in_task=True,
        window_seconds=_BUDGET_GATE_WINDOW_SECONDS, config=config,
    ).as_dict()
    global_limit = config.service.hourly_limit_for(provider)
    task_limit = config.service.hourly_limit_for(provider, task_name=spec.name)
    task_override = spec.name in config.service.task_hourly_limits
    usage = task_usage if task_override else global_usage
    limit = task_limit if task_override else global_limit
    ratio = 0.0 if limit <= 0 else float(usage.get("request_count", 0)) / float(limit)
    projected_request_count, projected_request_source = _projected_request_count(config, spec, task_state, fetch_mode=fetch_mode)
    projected_request_total = int(usage.get("request_count", 0)) + projected_request_count
    projected_ratio = 0.0 if limit <= 0 else float(projected_request_total) / float(limit)
    recovery_task = spec.name if task_override else None
    warn_recovery_seconds = estimate_budget_recovery_seconds_for_ratio(
        provider=provider,
        limit=limit,
        target_ratio=config.service.warn_ratio,
        projected_requests=0,
        window_seconds=_BUDGET_GATE_WINDOW_SECONDS,
        task=recovery_task,
        include_legacy_in_task=task_override,
        config=config,
    )
    projected_warn_recovery_seconds = estimate_budget_recovery_seconds_for_ratio(
        provider=provider,
        limit=limit,
        target_ratio=config.service.warn_ratio,
        projected_requests=projected_request_count,
        window_seconds=_BUDGET_GATE_WINDOW_SECONDS,
        task=recovery_task,
        include_legacy_in_task=task_override,
        config=config,
    )
    recovery_seconds = estimate_budget_recovery_seconds(
        provider=provider,
        limit=limit,
        critical_ratio=config.service.critical_ratio,
        projected_requests=0,
        window_seconds=_BUDGET_GATE_WINDOW_SECONDS,
        task=recovery_task,
        include_legacy_in_task=task_override,
        config=config,
    )
    projected_recovery_seconds = estimate_budget_recovery_seconds(
        provider=provider,
        limit=limit,
        critical_ratio=config.service.critical_ratio,
        projected_requests=projected_request_count,
        window_seconds=_BUDGET_GATE_WINDOW_SECONDS,
        task=recovery_task,
        include_legacy_in_task=task_override,
        config=config,
    )
    budget_scope = config.service.budget_scope_for(provider, task_name=spec.name)
    warn_floor_seconds = config.service.backoff_floor_seconds_for(provider, level="warn", task_name=spec.name)
    critical_floor_seconds = config.service.backoff_floor_seconds_for(provider, level="critical", task_name=spec.name)
    warn_cooldown_seconds = max(warn_recovery_seconds, warn_floor_seconds)
    critical_cooldown_seconds = max(recovery_seconds, critical_floor_seconds)
    projected_warn_cooldown_seconds = max(projected_warn_recovery_seconds, warn_floor_seconds)
    projected_critical_cooldown_seconds = max(projected_recovery_seconds, critical_floor_seconds)
    usage["limit"] = limit
    usage["global_request_count"] = int(global_usage.get("request_count", 0))
    usage["global_limit"] = global_limit
    usage["task_request_count"] = int(task_usage.get("request_count", 0))
    usage["task_limit"] = task_limit
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
    usage["projected_request_count"] = projected_request_count
    usage["projected_request_total"] = projected_request_total
    usage["projected_ratio"] = projected_ratio
    usage["projected_warn_recovery_seconds"] = projected_warn_recovery_seconds
    usage["projected_recovery_seconds"] = projected_recovery_seconds
    usage["projected_warn_cooldown_seconds"] = projected_warn_cooldown_seconds
    usage["projected_critical_cooldown_seconds"] = projected_critical_cooldown_seconds
    if projected_request_source is not None:
        usage["projected_request_source"] = projected_request_source
    usage.update(_projected_request_policy_details(config, spec, task_state, fetch_mode=fetch_mode))
    global_ratio = 0.0 if global_limit <= 0 else float(global_usage.get("request_count", 0)) / float(global_limit)
    global_projected_ratio = 0.0 if global_limit <= 0 else float(int(global_usage.get("request_count", 0)) + projected_request_count) / float(global_limit)
    global_warn_recovery_seconds = estimate_budget_recovery_seconds_for_ratio(
        provider=provider, limit=global_limit, target_ratio=config.service.warn_ratio,
        projected_requests=projected_request_count, window_seconds=_BUDGET_GATE_WINDOW_SECONDS, config=config,
    )
    global_critical_recovery_seconds = estimate_budget_recovery_seconds(
        provider=provider, limit=global_limit, critical_ratio=config.service.critical_ratio,
        projected_requests=projected_request_count, window_seconds=_BUDGET_GATE_WINDOW_SECONDS, config=config,
    )
    global_warn_cooldown_seconds = max(
        global_warn_recovery_seconds,
        config.service.backoff_floor_seconds_for(provider, level="warn"),
    )
    global_critical_cooldown_seconds = max(
        global_critical_recovery_seconds,
        config.service.backoff_floor_seconds_for(provider, level="critical"),
    )
    usage["global_ratio"] = global_ratio
    usage["global_projected_ratio"] = global_projected_ratio
    usage["global_warn_recovery_seconds"] = global_warn_recovery_seconds
    usage["global_recovery_seconds"] = global_critical_recovery_seconds
    # A task override narrows the lane but never exempts it from the provider-global cap.
    if task_override and (global_ratio >= config.service.critical_ratio or (projected_request_count > 0 and global_projected_ratio >= config.service.critical_ratio)):
        usage["backoff_level"] = "critical"
        usage["cooldown_seconds"] = global_critical_cooldown_seconds
        global_floor = config.service.backoff_floor_seconds_for(provider, level="critical")
        usage["critical_backoff_floor_seconds"] = global_floor
        if global_floor > global_critical_recovery_seconds:
            usage["cooldown_source"] = "provider_global_floor"
        return False, f"{provider}_global_budget_critical ratio={global_ratio:.3f} projected_ratio={global_projected_ratio:.3f} cooldown={global_critical_cooldown_seconds}s", usage
    if task_override and (global_ratio >= config.service.warn_ratio or (projected_request_count > 0 and global_projected_ratio >= config.service.warn_ratio)) and global_warn_cooldown_seconds > 0:
        usage["backoff_level"] = "warn"
        usage["cooldown_seconds"] = global_warn_cooldown_seconds
        global_floor = config.service.backoff_floor_seconds_for(provider, level="warn")
        usage["warn_backoff_floor_seconds"] = global_floor
        if global_floor > global_warn_recovery_seconds:
            usage["cooldown_source"] = "provider_global_floor"
        return False, f"{provider}_global_budget_warn ratio={global_ratio:.3f} projected_ratio={global_projected_ratio:.3f} cooldown={global_warn_cooldown_seconds}s", usage
    if ratio >= config.service.critical_ratio:
        usage["backoff_level"] = "critical"
        usage["cooldown_seconds"] = critical_cooldown_seconds
        if critical_floor_seconds > recovery_seconds:
            usage["cooldown_source"] = f"{budget_scope}_floor"
        return False, f"{provider}_budget_critical ratio={ratio:.3f} cooldown={critical_cooldown_seconds}s", usage
    if projected_request_count > 0 and projected_ratio >= config.service.critical_ratio:
        usage["backoff_level"] = "critical"
        usage["cooldown_seconds"] = projected_critical_cooldown_seconds
        if critical_floor_seconds > projected_recovery_seconds:
            usage["cooldown_source"] = f"{budget_scope}_floor"
        return False, f"{provider}_budget_projected_critical ratio={ratio:.3f} projected_ratio={projected_ratio:.3f} projected_requests={projected_request_count} cooldown={projected_critical_cooldown_seconds}s", usage
    if ratio >= config.service.warn_ratio and warn_cooldown_seconds > 0:
        usage["backoff_level"] = "warn"
        usage["cooldown_seconds"] = warn_cooldown_seconds
        if warn_floor_seconds > warn_recovery_seconds:
            usage["cooldown_source"] = f"{budget_scope}_floor"
        return False, f"{provider}_budget_warn ratio={ratio:.3f} cooldown={warn_cooldown_seconds}s", usage
    if projected_request_count > 0 and projected_ratio >= config.service.warn_ratio and projected_warn_cooldown_seconds > 0:
        usage["backoff_level"] = "warn"
        usage["cooldown_seconds"] = projected_warn_cooldown_seconds
        if warn_floor_seconds > projected_warn_recovery_seconds:
            usage["cooldown_source"] = f"{budget_scope}_floor"
        return False, f"{provider}_budget_projected_warn ratio={ratio:.3f} projected_ratio={projected_ratio:.3f} projected_requests={projected_request_count} cooldown={projected_warn_cooldown_seconds}s", usage
    return True, None, usage


def _failure_backoff_profile(config: AppConfig, spec: TaskSpec, reason: str) -> tuple[str, int]:
    provider = spec.budget_provider
    critical_floor = 0
    auth_floor = 0
    if provider:
        critical_floor = config.service.backoff_floor_seconds_for(provider, level="critical", task_name=spec.name)
        auth_floor = config.service.auth_failure_backoff_floor_seconds_for(provider, task_name=spec.name)
    classification = "auth" if provider and looks_auth_style_failure(reason) else "generic"
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
    reason = sanitize_text(reason, max_length=500)
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
        return sanitize_text(reason.strip(), max_length=500)
    stderr = result.get("stderr")
    if isinstance(stderr, str) and stderr.strip():
        return sanitize_text(stderr.strip().splitlines()[0], max_length=500)
    stdout = result.get("stdout")
    if isinstance(stdout, str) and stdout.strip():
        return sanitize_text(stdout.strip().splitlines()[0], max_length=500)
    return None


def _run_pending_tasks_unlocked(config: AppConfig) -> dict[str, Any]:
    config = config or load_config()
    ensure_directories(config)
    try:
        state = _load_state(config)
    except ServiceStateLoadError as exc:
        _append_log(config, f"scheduler status=error reason=service_state_unavailable detail={exc.safe_message}")
        return {
            "status": "error",
            "reason": "service_state_unavailable",
            "results": [],
            "service_state_parse_error": exc.safe_message,
            "state_file": str(config.service_state_path),
        }
    now = time.time()
    results: list[dict[str, Any]] = []
    try:
        prune_report = prune_api_request_events_with_diagnostics(retention_days=14, config=config)
    except Exception as exc:
        error_type = sanitize_text(type(exc).__name__, max_length=100)
        _append_log(config, f"scheduler status=error reason=api_request_events_unavailable status=blocked_error error_type={error_type}")
        return {
            "status": "error",
            "reason": "api_request_events_unavailable",
            "results": [],
            "api_request_events_prune": {"status": "blocked_error", "blocked": True, "error_type": error_type},
            "api_request_events_file": str(config.api_request_events_path),
            "state_file": str(config.service_state_path),
        }
    if prune_report.blocked:
        _append_log(
            config,
            "scheduler status=error reason=api_request_events_unavailable "
            f"status={prune_report.status} corrupt_records={prune_report.corrupt_records} "
            f"expired_candidates={prune_report.expired_candidates} kept_records={prune_report.kept_records} "
            f"scanned_records={prune_report.scanned_records}",
        )
        return {
            "status": "error",
            "reason": "api_request_events_unavailable",
            "results": [],
            "api_request_events_prune": prune_report.as_dict(),
            "api_request_events_file": str(config.api_request_events_path),
            "state_file": str(config.service_state_path),
        }
    if prune_report.actual_removed:
        _append_log(
            config,
            "api_request_events_pruned="
            f"{prune_report.actual_removed} expired_removed={prune_report.expired_removed} "
            f"kept_records={prune_report.kept_records} scanned_records={prune_report.scanned_records}",
        )
    try:
        snapshot_prune = prune_recommendation_score_snapshots(
            config.db_path,
            retention_days=config.service.recommendation_snapshot_retention_days,
            min_runs_per_kind=config.service.recommendation_snapshot_min_runs_per_kind,
            batch_size=config.service.recommendation_snapshot_prune_batch_size,
        ).as_dict()
    except Exception as exc:
        snapshot_prune = {
            "status": "blocked_error",
            "error_type": sanitize_text(type(exc).__name__, max_length=100),
            "deleted_rows": 0,
        }
    state["recommendation_snapshot_retention"] = snapshot_prune
    if snapshot_prune.get("deleted_rows"):
        _append_log(
            config,
            "recommendation_score_snapshots_pruned="
            f"{snapshot_prune['deleted_rows']} remaining_eligible={snapshot_prune.get('remaining_eligible_rows', 0)} "
            f"rows_after={snapshot_prune.get('rows_after', 0)}",
        )
    tasks_state = state.setdefault("tasks", {})

    for spec in _task_specs(config):
        task_state = tasks_state.setdefault(spec.name, {})
        task_state["budget_provider"] = spec.budget_provider
        task_state["budget_scope"] = config.service.budget_scope_for(spec.budget_provider, task_name=spec.name)
        task_state["every_seconds"] = int(spec.every_seconds)
        task_state["initial_delay_seconds"] = int(spec.initial_delay_seconds)
        if spec.initial_delay_seconds > 0 and "last_run_epoch" not in task_state and not task_state.get("last_status"):
            task_state["last_status"] = "scheduled"
            task_state["execution_state"] = "idle"
            _set_task_next_due(task_state, base_epoch=now, every_seconds=spec.initial_delay_seconds)
            continue
        if (
            spec.initial_delay_seconds > 0
            and "last_run_epoch" not in task_state
            and task_state.get("last_status") == "scheduled"
            and float(task_state.get("next_due_epoch", 0)) > now
        ):
            continue
        if spec.name == "recommend_maintain" and "last_run_epoch" not in task_state:
            if task_state.get("last_status") == "scheduled" and float(task_state.get("next_due_epoch", 0)) > now:
                continue
            if not task_state.get("last_status"):
                task_state["last_status"] = "scheduled"
                task_state["execution_state"] = "idle"
                _set_task_next_due(task_state, base_epoch=now, every_seconds=spec.every_seconds)
                continue
        if task_state.get("last_status") in {"skipped", "disabled"} and task_state.get("last_skip_reason") in {
            "lease_busy",
            "execute_limit_zero",
        } and float(task_state.get("next_due_epoch", 0)) > now:
            # A busy child keeps its due intent but must not spin every daemon
            # loop; disabled lanes are likewise quiet until their next config
            # observation cadence.
            continue
        last_run = float(task_state.get("last_run_epoch", 0))
        planned_fetch_mode, planned_full_refresh_reasons, health_request = _planned_fetch_mode(config, spec, task_state, now=now)
        health_requested_run = isinstance(health_request, dict)
        if spec.name == "sync_apply" and (config.service.execute_limit_for("sync_apply") or 0) <= 0:
            # Manual `apply-sync --limit 0` retains its explicit full-scan
            # meaning. In unattended service config, zero is a hard disable so
            # it can never normalize into unbounded writes.
            task_state.update(
                {
                    "last_status": "disabled",
                    "execution_state": "idle",
                    "last_skip_reason": "execute_limit_zero",
                }
            )
            _set_task_next_due(task_state, base_epoch=now, every_seconds=spec.every_seconds)
            results.append({"task": spec.name, "status": "skipped", "reason": "execute_limit_zero"})
            continue
        if spec.name == "recommend_full_harvest" and (config.service.execute_limit_for("recommend_full_harvest") or 0) <= 0:
            # Manual CLI --limit 0 means "all due". In the daemon's cold public
            # MAL userrecs lane, zero is a hard disable so a config typo cannot
            # turn into one giant unattended crawl.
            task_state.update(
                {
                    "last_status": "disabled",
                    "execution_state": "idle",
                    "last_skip_reason": "execute_limit_zero",
                }
            )
            _set_task_next_due(task_state, base_epoch=now, every_seconds=spec.every_seconds)
            results.append({"task": spec.name, "status": "skipped", "reason": "execute_limit_zero"})
            continue
        if spec.name.startswith("recommend_provider_eligibility_") and (
            config.service.execute_limit_for("recommend_provider_eligibility_candidates") or 0
        ) <= 0:
            # Manual provider enrichment defaults remain bounded. In the daemon's
            # credentialed provider lanes, zero is a hard disable so a config typo
            # cannot turn into an unintended provider search batch.
            task_state.update(
                {
                    "last_status": "disabled",
                    "execution_state": "idle",
                    "last_skip_reason": "execute_limit_zero",
                }
            )
            _set_task_next_due(task_state, base_epoch=now, every_seconds=spec.every_seconds)
            results.append({"task": spec.name, "status": "skipped", "reason": "execute_limit_zero"})
            continue
        if now - last_run < spec.every_seconds and not health_requested_run:
            _set_task_next_due(task_state, base_epoch=last_run, every_seconds=spec.every_seconds)
            continue
        _maybe_reset_task_projection_state_for_signature(config, spec, task_state, fetch_mode=planned_fetch_mode)
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
        if spec.budget_provider is None:
            allowed, reason, usage = True, None, None
        else:
            allowed, reason, usage = _budget_gate(config, spec, task_state, fetch_mode=planned_fetch_mode)
        downgrade_reason = None
        downgrade_usage = None
        allowed, reason, usage, planned_fetch_mode, planned_full_refresh_reasons, downgrade_reason, downgrade_usage = _maybe_downgrade_fetch_mode_for_budget(
            config,
            spec,
            task_state,
            planned_fetch_mode=planned_fetch_mode,
            planned_full_refresh_reasons=planned_full_refresh_reasons,
            allowed=allowed,
            reason=reason,
            usage=usage,
        )
        if isinstance(usage, dict):
            for field in (
                "projected_request_count",
                "projected_request_total",
                "projected_ratio",
                "projected_request_source",
                "projected_request_history_window",
                "projected_request_history_mode",
                "projected_request_history_sample_count",
                "projected_request_percentile",
                "projected_request_percentile_source",
            ):
                value = usage.get(field)
                if value is not None:
                    task_state[field] = value
                else:
                    task_state.pop(field, None)
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
        request_run_id = str(uuid.uuid4())
        request_boundary = capture_api_event_boundary(config=config)
        request_context_token = begin_api_request_context(task=spec.name, run_id=request_run_id)
        try:
            full_refresh_requested = planned_fetch_mode == "full_refresh"
            if spec.name == "mal_refresh":
                result = _refresh_mal_tokens(config)
            elif spec.name == "mal_list_refresh":
                command_args = _mal_list_refresh_command(config)
                started_epoch, started_at = _mark_task_running(config, state, spec.name, command_args)
                result = _run_subprocess(config, command_args, label=spec.name)
                result["max_pages"] = int(command_args[-1])
            elif spec.name.startswith("sync_fetch_"):
                provider = spec.name.removeprefix("sync_fetch_")
                full_refresh_reasons = list(planned_full_refresh_reasons)
                command_args = _provider_fetch_command(config, provider, full_refresh=full_refresh_requested)
                started_epoch, started_at = _mark_task_running(config, state, spec.name, command_args)
                result = _run_subprocess(config, command_args, label=spec.name)
                result["fetch_mode"] = planned_fetch_mode or "hot"
                if full_refresh_reasons:
                    result["full_refresh_reason"] = "+".join(full_refresh_reasons)
                if isinstance(health_request, dict) and isinstance(health_request.get("reason_code"), str):
                    result["health_request_reason_code"] = str(health_request["reason_code"])
                if downgrade_reason:
                    result["deferred_full_refresh_reason"] = downgrade_reason
            elif spec.name == "sync_apply":
                command_args = _apply_sync_command(config)
                started_epoch, started_at = _mark_task_running(config, state, spec.name, command_args)
                result = _run_subprocess(config, command_args, label="sync_apply")
                apply_limit = config.service.execute_limit_for("sync_apply")
                if apply_limit is None:
                    apply_limit = DEFAULT_SERVICE_TASK_EXECUTE_LIMITS.get("sync_apply", 0)
                result["apply_limit"] = max(0, int(apply_limit))
            elif spec.name == "recommend_metadata_refresh":
                command_args = _recommendation_metadata_refresh_command(config)
                started_epoch, started_at = _mark_task_running(config, state, spec.name, command_args)
                result = _run_subprocess(config, command_args, label="recommend_metadata_refresh")
                seed_limit = config.service.execute_limit_for("recommend_metadata_refresh")
                if seed_limit is None:
                    seed_limit = DEFAULT_SERVICE_TASK_EXECUTE_LIMITS.get("recommend_metadata_refresh", 0)
                discovery_target_limit = config.service.execute_limit_for("recommend_metadata_discovery_targets")
                if discovery_target_limit is None:
                    discovery_target_limit = DEFAULT_SERVICE_TASK_EXECUTE_LIMITS.get("recommend_metadata_discovery_targets", 0)
                result["refresh_limit"] = max(0, int(seed_limit))
                result["discovery_target_limit"] = max(0, int(discovery_target_limit))
                parsed_stdout = _parse_json_stdout(result)
                if parsed_stdout is not None:
                    for key in ("considered", "refreshed", "discovery_considered", "discovery_refreshed"):
                        value = parsed_stdout.get(key)
                        if isinstance(value, int):
                            result[key] = max(0, int(value))
            elif spec.name == "recommend_full_harvest":
                command_args = _recommendation_full_harvest_command(config)
                started_epoch, started_at = _mark_task_running(config, state, spec.name, command_args)
                result = _run_subprocess(config, command_args, label="recommend_full_harvest")
                source_limit = config.service.execute_limit_for("recommend_full_harvest")
                if source_limit is None:
                    source_limit = DEFAULT_SERVICE_TASK_EXECUTE_LIMITS.get("recommend_full_harvest", 0)
                max_pages = config.service.execute_limit_for("recommend_full_harvest_pages")
                if max_pages is None:
                    max_pages = DEFAULT_SERVICE_TASK_EXECUTE_LIMITS.get("recommend_full_harvest_pages", 1)
                result["refresh_limit"] = max(0, int(source_limit))
                result["max_pages"] = max(1, int(max_pages))
                parsed_stdout = _parse_json_stdout(result)
                if parsed_stdout is not None:
                    for key in ("seed_count", "considered", "harvested", "failed", "skipped_fresh", "total_edges"):
                        value = parsed_stdout.get(key)
                        if isinstance(value, int):
                            result[key] = max(0, int(value))
            elif spec.name.startswith("recommend_provider_eligibility_"):
                provider = spec.name.removeprefix("recommend_provider_eligibility_")
                command_args = _provider_eligibility_command(config, provider)
                started_epoch, started_at = _mark_task_running(config, state, spec.name, command_args)
                result = _run_subprocess(config, command_args, label=spec.name)
                result["provider"] = provider
                parsed_stdout = _parse_json_stdout(result)
                if parsed_stdout is not None:
                    for key in (
                        "candidates_considered",
                        "queries_selected",
                        "cache_hits",
                        "cache_misses",
                        "provider_searches",
                        "provider_detail_probes",
                        "eligibility_fresh_skips",
                        "eligibility_expired_retries",
                    ):
                        value = parsed_stdout.get(key)
                        if isinstance(value, int):
                            result[key] = max(0, int(value))
            elif spec.name == "recommend_maintain":
                command_args = _recommend_maintain_command(config)
                started_epoch, started_at = _mark_task_running(config, state, spec.name, command_args)
                result = _run_subprocess(config, command_args, label="recommend_maintain")
            elif spec.name == "push_recommendations_webhook":
                try:
                    result = _push_recommendations_webhook_task(config, task_state)
                except OpenClawDeliveryError as exc:
                    result = {
                        "status": "error",
                        "label": "push_recommendations_webhook",
                        "reason": sanitize_text(exc, max_length=500),
                    }
                result["delivery_limit"] = _recommendations_webhook_push_limit(config)
            elif spec.name == "health":
                command_args = [sys.executable, "-m", "mal_updater.cli", "--project-root", str(config.project_root), "health-check-cycle"]
                started_epoch, started_at = _mark_task_running(config, state, spec.name, command_args)
                result = _run_subprocess(config, command_args, label="health")
            else:
                result = {"status": "skipped", "reason": "unknown_task"}
            finished_epoch = time.time()
            finished_at = datetime.fromtimestamp(finished_epoch, tz=timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
            task_status = result.get("status", "ok")
            task_succeeded = task_status == "ok"
            if task_succeeded and spec.budget_provider is not None and isinstance(usage, dict):
                result.update(
                    _finalize_run_request_delta(
                        config, spec, task_state, boundary=request_boundary, run_id=request_run_id,
                        fetch_mode=planned_fetch_mode, finished_at=finished_at,
                    )
                )
            persisted_result = _persistable_task_result(result)
            task_state.update({"last_status": task_status, "last_result": persisted_result})
            if task_succeeded or task_status == "error":
                task_state.update({"last_run_epoch": now, "last_run_at": finished_at})
            task_state["execution_state"] = "idle"
            _clear_task_running(task_state)
            _record_task_timing(task_state, started_epoch=started_epoch, finished_epoch=finished_epoch, started_at=started_at, finished_at=finished_at)
            if task_succeeded:
                _set_task_next_due(task_state, base_epoch=now, every_seconds=spec.every_seconds)
            elif task_status != "error":
                retry_seconds = min(60, max(5, int(spec.every_seconds)))
                _set_task_next_due(task_state, base_epoch=now, every_seconds=retry_seconds)
                task_state["last_skipped_at"] = finished_at
                task_state["last_skip_reason"] = str(result.get("reason") or task_status)
            fetch_succeeded = task_succeeded
            if spec.name.startswith("sync_fetch_") and fetch_succeeded:
                task_state["last_fetch_mode"] = "full_refresh" if full_refresh_requested else "hot"
                task_state["last_fetch_mode_at"] = finished_at
                if isinstance(health_request, dict) and isinstance(health_request.get("health_mtime"), (int, float)):
                    task_state["last_health_request_handled_mtime"] = float(health_request["health_mtime"])
                    if isinstance(health_request.get("reason_code"), str):
                        task_state["last_health_request_reason_code"] = str(health_request["reason_code"])
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
            if task_succeeded or task_status == "error":
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
                _set_task_next_due(
                    task_state,
                    base_epoch=now,
                    every_seconds=int(failure_backoff["failure_backoff_remaining_seconds"]),
                )
                results.append({"task": spec.name, **result, **failure_backoff})
                _append_log(
                    config,
                    f"task={spec.name} status=error failure_backoff={failure_backoff['failure_backoff_remaining_seconds']}s reason={failure_reason}",
                )
                continue
            if task_succeeded:
                task_state.pop("last_error", None)
                _clear_failure_backoff(task_state)
            results.append({"task": spec.name, **result})
        except (MalApiError, OSError, subprocess.SubprocessError) as exc:
            finished_epoch = time.time()
            finished_at = datetime.fromtimestamp(finished_epoch, tz=timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
            # Failed/ambiguous attempts are still present in telemetry, but do
            # not teach the scheduler a new success projection.
            failed_request_delta: dict[str, Any] = {}
            exception_reason = sanitize_text(f"{type(exc).__name__}: {exc}", max_length=500)
            task_state.update({"last_run_epoch": now, "last_run_at": finished_at, "last_status": "error", "last_error": exception_reason})
            _clear_task_running(task_state)
            _record_task_timing(task_state, started_epoch=started_epoch, finished_epoch=finished_epoch, started_at=started_at, finished_at=finished_at)
            task_state.pop("last_skip_reason", None)
            task_state.pop("last_skipped_at", None)
            task_state.pop("budget_backoff_level", None)
            task_state.pop("budget_backoff_until_epoch", None)
            task_state.pop("budget_backoff_until", None)
            task_state.pop("budget_backoff_remaining_seconds", None)
            task_state.pop("budget_backoff_floor_seconds", None)
            task_state.pop("budget_backoff_cooldown_source", None)
            failure_backoff = _set_failure_backoff(config, spec, task_state, now=now, reason=exception_reason)
            _set_task_next_due(
                task_state,
                base_epoch=now,
                every_seconds=int(failure_backoff["failure_backoff_remaining_seconds"]),
            )
            results.append({"task": spec.name, "status": "error", "error": exception_reason, **failed_request_delta, **failure_backoff})
            _append_log(
                config,
                f"task={spec.name} status=error error={exception_reason} failure_backoff={failure_backoff['failure_backoff_remaining_seconds']}s",
            )
        finally:
            end_api_request_context(request_context_token)

    state["last_loop_at"] = _now_iso()
    tracked_providers = {"mal", "crunchyroll", *config.service.provider_hourly_limits.keys(), *_available_source_providers(config)}
    state["api_usage"] = {
        provider: summarize_recent_api_usage(provider=provider, window_seconds=_BUDGET_GATE_WINDOW_SECONDS, config=config).as_dict()
        for provider in sorted(tracked_providers)
    }
    _save_state(config, state)
    payload = {
        "status": "ok",
        "results": results,
        "state_file": str(config.service_state_path),
        "api_usage": state["api_usage"],
        "recommendation_snapshot_retention": snapshot_prune,
    }
    safe_payload = sanitize_value(payload, max_depth=10, max_items=500, max_string=_SUBPROCESS_STREAM_LIMIT)
    return safe_payload if isinstance(safe_payload, dict) else {"status": "error", "reason": "result_sanitization_failed"}


def run_pending_tasks(config: AppConfig | None = None) -> dict[str, Any]:
    config = config or load_config()
    ensure_directories(config)
    lease = _ProcessLease(config, "scheduler")
    if not lease.try_acquire(phase="task-pass"):
        _append_log(config, "scheduler status=skipped reason=lease_busy")
        return {"status": "skipped", "reason": "lease_busy", "results": [], "lease": lease.status}
    try:
        result = _run_pending_tasks_unlocked(config)
        result["lease"] = {"run_id": lease.run_id, "status_file": str(lease.status_path)}
        return result
    finally:
        lease.release()

def run_service_loop(config: AppConfig | None = None) -> int:
    config = config or load_config()
    ensure_directories(config)
    lease = _ProcessLease(config, "daemon")
    if not lease.try_acquire(phase="startup"):
        _append_log(config, "service loop not started reason=lease_busy")
        return 0
    try:
        grace_seconds = max(0, int(config.service.startup_grace_seconds))
        _append_log(config, f"service loop starting run_id={lease.run_id} startup_grace_seconds={grace_seconds}")
        if grace_seconds:
            lease.update_phase("startup_grace")
            time.sleep(grace_seconds)
        lease.update_phase("running")
        while True:
            run_pending_tasks(config)
            time.sleep(max(5, int(config.service.loop_sleep_seconds)))
    finally:
        lease.release()
