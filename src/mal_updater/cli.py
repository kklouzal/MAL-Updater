from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from pathlib import Path
import re

from .auth import OAuthCallbackError, format_auth_flow_prompt, persist_token_response, wait_for_oauth_callback
from .config import ensure_directories, load_config, load_mal_secrets
from .crunchyroll_auth import (
    CrunchyrollAuthError,
    crunchyroll_login_with_credentials,
    load_crunchyroll_credentials,
    resolve_crunchyroll_state_paths,
)
from .crunchyroll_snapshot import (
    CrunchyrollSnapshotError,
    fetch_snapshot,
    snapshot_to_dict,
    write_snapshot_file,
)
from .db import (
    bootstrap_database,
    get_provider_series_title_map,
    list_review_queue_entries,
    list_series_mappings,
    upsert_series_mapping,
)
from .ingestion import ingest_snapshot_file, ingest_snapshot_payload
from .mal_client import MalApiError, MalClient
from .mapping import SeriesMappingInput, map_series, normalize_title
from .recommendation_metadata import refresh_recommendation_metadata
from .recommendations import build_recommendations, group_recommendations
from .sync_planner import (
    build_dry_run_sync_plan,
    build_mapping_review,
    execute_approved_sync,
    load_provider_series_states,
    persist_mapping_review_queue,
    persist_sync_review_queue,
)
from .validation import SnapshotValidationError, validate_snapshot_payload


def _cmd_init(project_root: Path | None) -> int:
    config = load_config(project_root)
    ensure_directories(config)
    bootstrap_database(config.db_path)
    print(f"Initialized MAL-Updater scaffold at {config.project_root}")
    print(f"SQLite database: {config.db_path}")
    return 0


def _cmd_status(project_root: Path | None) -> int:
    config = load_config(project_root)
    secrets = load_mal_secrets(config)
    crunchyroll_credentials = load_crunchyroll_credentials(config)
    crunchyroll_state = resolve_crunchyroll_state_paths(config)
    print(f"project_root={config.project_root}")
    print(f"settings_path={config.settings_path}")
    print(f"config_dir={config.config_dir}")
    print(f"secrets_dir={config.secrets_dir}")
    print(f"data_dir={config.data_dir}")
    print(f"state_dir={config.state_dir}")
    print(f"cache_dir={config.cache_dir}")
    print(f"db_path={config.db_path}")
    print(f"contract_version={config.contract_version}")
    print(f"request_timeout_seconds={config.request_timeout_seconds}")
    print(f"completion_threshold={config.completion_threshold}")
    print(f"credits_skip_window_seconds={config.credits_skip_window_seconds}")
    print(f"mal.base_url={config.mal.base_url}")
    print(f"mal.auth_url={config.mal.auth_url}")
    print(f"mal.token_url={config.mal.token_url}")
    print(f"mal.bind_host={config.mal.bind_host}")
    print(f"mal.redirect_uri={config.mal.redirect_uri}")
    print(f"mal.request_spacing_seconds={config.mal.request_spacing_seconds}")
    print(f"mal.request_spacing_jitter_seconds={config.mal.request_spacing_jitter_seconds}")
    print(f"crunchyroll.locale={config.crunchyroll.locale}")
    print(f"crunchyroll.request_spacing_seconds={config.crunchyroll.request_spacing_seconds}")
    print(f"crunchyroll.request_spacing_jitter_seconds={config.crunchyroll.request_spacing_jitter_seconds}")
    print(f"crunchyroll.username_present={bool(crunchyroll_credentials.username)}")
    print(f"crunchyroll.password_present={bool(crunchyroll_credentials.password)}")
    print(f"crunchyroll.username_path={crunchyroll_credentials.username_path}")
    print(f"crunchyroll.password_path={crunchyroll_credentials.password_path}")
    print(f"crunchyroll.state_root={crunchyroll_state.root}")
    print(f"crunchyroll.refresh_token_path={crunchyroll_state.refresh_token_path}")
    print(f"crunchyroll.device_id_path={crunchyroll_state.device_id_path}")
    print(f"crunchyroll.session_state_path={crunchyroll_state.session_state_path}")
    print(f"crunchyroll.sync_boundary_path={crunchyroll_state.sync_boundary_path}")
    print(f"crunchyroll.refresh_token_present={crunchyroll_state.refresh_token_path.exists()}")
    print(f"crunchyroll.device_id_present={crunchyroll_state.device_id_path.exists()}")
    print(f"crunchyroll.sync_boundary_present={crunchyroll_state.sync_boundary_path.exists()}")
    print(f"mal.client_id_present={bool(secrets.client_id)}")
    print(f"mal.client_secret_present={bool(secrets.client_secret)}")
    print(f"mal.access_token_present={bool(secrets.access_token)}")
    print(f"mal.refresh_token_present={bool(secrets.refresh_token)}")
    print(f"mal.client_id_path={secrets.client_id_path}")
    print(f"mal.client_secret_path={secrets.client_secret_path}")
    print(f"mal.access_token_path={secrets.access_token_path}")
    print(f"mal.refresh_token_path={secrets.refresh_token_path}")
    return 0


def _cmd_mal_auth_url(project_root: Path | None, emit_json: bool) -> int:
    config = load_config(project_root)
    ensure_directories(config)
    client = MalClient(config, load_mal_secrets(config))
    pkce = client.generate_pkce_pair()
    try:
        auth_url = client.build_authorization_url(code_challenge=pkce.code_challenge)
    except MalApiError as exc:
        print(str(exc), file=sys.stderr)
        return 1
    if emit_json:
        print(
            json.dumps(
                {
                    "authorization_url": auth_url,
                    "redirect_uri": config.mal.redirect_uri,
                    "code_verifier": pkce.code_verifier,
                    "code_challenge": pkce.code_challenge,
                },
                indent=2,
            )
        )
        return 0
    print("Open this URL in a browser after writing down the code verifier:")
    print(auth_url)
    print()
    print("code_verifier=")
    print(pkce.code_verifier)
    print()
    print(f"redirect_uri={config.mal.redirect_uri}")
    return 0


def _cmd_mal_auth_login(project_root: Path | None, timeout_seconds: float, verify_whoami: bool) -> int:
    config = load_config(project_root)
    ensure_directories(config)
    secrets = load_mal_secrets(config)
    client = MalClient(config, secrets)
    pkce = client.generate_pkce_pair()
    state = client.generate_state()
    try:
        auth_url = client.build_authorization_url(code_challenge=pkce.code_challenge, state=state)
    except MalApiError as exc:
        print(str(exc), file=sys.stderr)
        return 1

    print(format_auth_flow_prompt(config, auth_url, timeout_seconds))
    try:
        callback = wait_for_oauth_callback(
            config.mal.bind_host,
            config.mal.redirect_port,
            expected_state=state,
            timeout_seconds=timeout_seconds,
        )
        token = client.exchange_code(callback.code, pkce.code_verifier)
        persisted = persist_token_response(token, secrets)
    except OSError as exc:
        print(f"Unable to start MAL callback listener on {config.mal.redirect_uri}: {exc}", file=sys.stderr)
        return 1
    except TimeoutError as exc:
        print(str(exc), file=sys.stderr)
        return 1
    except (OAuthCallbackError, MalApiError) as exc:
        print(str(exc), file=sys.stderr)
        return 1

    print()
    print(f"Persisted access token to {persisted.access_token_path}")
    if token.refresh_token:
        print(f"Persisted refresh token to {persisted.refresh_token_path}")
    else:
        print("No refresh token returned by MAL; existing refresh token file left untouched")

    if not verify_whoami:
        return 0

    try:
        whoami = client.get_my_user(access_token=token.access_token)
    except MalApiError as exc:
        print(f"Token exchange succeeded, but /users/@me verification failed: {exc}", file=sys.stderr)
        return 1

    print(f"Authenticated MAL user: {json.dumps(whoami, indent=2)}")
    return 0


def _cmd_mal_refresh(project_root: Path | None, verify_whoami: bool) -> int:
    config = load_config(project_root)
    ensure_directories(config)
    secrets = load_mal_secrets(config)
    client = MalClient(config, secrets)
    try:
        token = client.refresh_access_token()
        persisted = persist_token_response(token, secrets)
    except MalApiError as exc:
        print(str(exc), file=sys.stderr)
        return 1

    print(f"Persisted access token to {persisted.access_token_path}")
    if token.refresh_token:
        print(f"Persisted refresh token to {persisted.refresh_token_path}")

    if not verify_whoami:
        return 0

    try:
        whoami = client.get_my_user(access_token=token.access_token)
    except MalApiError as exc:
        print(f"Refresh succeeded, but /users/@me verification failed: {exc}", file=sys.stderr)
        return 1

    print(f"Authenticated MAL user: {json.dumps(whoami, indent=2)}")
    return 0


def _cmd_mal_whoami(project_root: Path | None) -> int:
    config = load_config(project_root)
    secrets = load_mal_secrets(config)
    client = MalClient(config, secrets)
    try:
        whoami = client.get_my_user()
    except MalApiError as exc:
        print(str(exc), file=sys.stderr)
        return 1
    print(json.dumps(whoami, indent=2))
    return 0


def _cmd_crunchyroll_auth_login(project_root: Path | None, profile: str, no_verify: bool) -> int:
    config = load_config(project_root)
    ensure_directories(config)
    try:
        result = crunchyroll_login_with_credentials(
            config,
            profile=profile,
            verify_account=not no_verify,
        )
    except CrunchyrollAuthError as exc:
        print(str(exc), file=sys.stderr)
        return 1

    print(f"Staged Crunchyroll refresh token to {result.refresh_token_path}")
    print(f"Staged Crunchyroll device id to {result.device_id_path}")
    print(f"Updated session state at {result.session_state_path}")
    if result.account_id:
        print(f"Crunchyroll account_id={result.account_id}")
    if result.account_email:
        print(f"Crunchyroll account_email={result.account_email}")
    print(f"profile={result.profile}")
    print(f"locale={result.locale}")
    print(f"device_type={result.device_type}")
    return 0


def _cmd_validate_snapshot(project_root: Path | None, snapshot_path: Path | None) -> int:
    load_config(project_root)
    if snapshot_path is None:
        payload = json.load(sys.stdin)
        source = "stdin"
    else:
        payload = json.loads(snapshot_path.read_text(encoding="utf-8"))
        source = str(snapshot_path)
    try:
        validate_snapshot_payload(payload)
    except SnapshotValidationError as exc:
        print(f"INVALID: {exc}", file=sys.stderr)
        return 1
    print(f"VALID: {source}")
    return 0


def _cmd_crunchyroll_fetch_snapshot(
    project_root: Path | None,
    profile: str,
    out_path: Path | None,
    ingest: bool,
    full_refresh: bool,
) -> int:
    config = load_config(project_root)
    ensure_directories(config)
    try:
        result = fetch_snapshot(config, profile=profile, use_incremental_boundary=not full_refresh)
    except (CrunchyrollAuthError, CrunchyrollSnapshotError) as exc:
        print(str(exc), file=sys.stderr)
        return 1

    payload = snapshot_to_dict(result.snapshot)
    target_path = out_path
    if target_path is not None:
        write_snapshot_file(target_path, result.snapshot)
        print(f"Wrote Crunchyroll snapshot to {target_path}")

    if ingest:
        summary = ingest_snapshot_payload(payload, config)
        print(json.dumps(summary.as_dict(), indent=2))
        return 0

    print(json.dumps(payload, indent=2))
    return 0


def _cmd_ingest_snapshot(project_root: Path | None, snapshot_path: Path | None) -> int:
    config = load_config(project_root)
    ensure_directories(config)
    if snapshot_path is None:
        payload = json.load(sys.stdin)
        summary = ingest_snapshot_payload(payload, config)
    else:
        summary = ingest_snapshot_file(snapshot_path, config)
    print(json.dumps(summary.as_dict(), indent=2))
    return 0


def _cmd_map_series(project_root: Path | None, limit: int, mapping_limit: int) -> int:
    config = load_config(project_root)
    states = load_provider_series_states(config, limit=limit)
    client = MalClient(config, load_mal_secrets(config))
    results = []
    for state in states:
        try:
            mapping = map_series(
                client,
                SeriesMappingInput(
                    provider=state.provider,
                    provider_series_id=state.provider_series_id,
                    title=state.title,
                    season_title=state.season_title,
                    season_number=state.season_number,
                    max_episode_number=state.max_episode_number,
                    completed_episode_count=state.completed_episode_count,
                    max_completed_episode_number=state.max_completed_episode_number,
                ),
                limit=mapping_limit,
            )
        except MalApiError as exc:
            print(str(exc), file=sys.stderr)
            return 1
        results.append(
            {
                "provider_series_id": state.provider_series_id,
                "title": state.title,
                "season_title": state.season_title,
                "mapping_status": mapping.status,
                "confidence": mapping.confidence,
                "rationale": mapping.rationale,
                "chosen_candidate": None
                if not mapping.chosen_candidate
                else {
                    "mal_anime_id": mapping.chosen_candidate.mal_anime_id,
                    "title": mapping.chosen_candidate.title,
                    "score": mapping.chosen_candidate.score,
                    "matched_query": mapping.chosen_candidate.matched_query,
                    "match_reasons": mapping.chosen_candidate.match_reasons,
                },
                "candidates": [
                    {
                        "mal_anime_id": candidate.mal_anime_id,
                        "title": candidate.title,
                        "score": candidate.score,
                        "matched_query": candidate.matched_query,
                        "media_type": candidate.media_type,
                    }
                    for candidate in mapping.candidates
                ],
            }
        )
    print(json.dumps(results, indent=2))
    return 0


def _normalize_limit(limit: int) -> int | None:
    return None if limit <= 0 else limit


def _cmd_review_mappings(project_root: Path | None, limit: int, mapping_limit: int, persist_queue: bool) -> int:
    config = load_config(project_root)
    ensure_directories(config)
    bootstrap_database(config.db_path)
    normalized_limit = _normalize_limit(limit)
    if persist_queue and normalized_limit is not None:
        print("--persist-review-queue requires a full scan; rerun with --limit 0", file=sys.stderr)
        return 2
    try:
        items = build_mapping_review(config, limit=normalized_limit, mapping_limit=mapping_limit)
    except MalApiError as exc:
        print(str(exc), file=sys.stderr)
        return 1
    payload: dict[str, object] = {"items": [item.as_dict() for item in items]}
    if persist_queue:
        payload["review_queue"] = persist_mapping_review_queue(config, items)
    print(json.dumps(payload, indent=2))
    return 0


def _cmd_list_mappings(project_root: Path | None, approved_only: bool) -> int:
    config = load_config(project_root)
    ensure_directories(config)
    bootstrap_database(config.db_path)
    items = list_series_mappings(config.db_path, provider="crunchyroll", approved_only=approved_only)
    print(
        json.dumps(
            [
                {
                    "provider": item.provider,
                    "provider_series_id": item.provider_series_id,
                    "mal_anime_id": item.mal_anime_id,
                    "confidence": item.confidence,
                    "mapping_source": item.mapping_source,
                    "approved_by_user": item.approved_by_user,
                    "notes": item.notes,
                    "created_at": item.created_at,
                    "updated_at": item.updated_at,
                }
                for item in items
            ],
            indent=2,
        )
    )
    return 0


def _cmd_approve_mapping(
    project_root: Path | None,
    provider_series_id: str,
    mal_anime_id: int,
    confidence: float | None,
    notes: str | None,
    exact: bool,
) -> int:
    config = load_config(project_root)
    ensure_directories(config)
    bootstrap_database(config.db_path)
    mapping = upsert_series_mapping(
        config.db_path,
        provider="crunchyroll",
        provider_series_id=provider_series_id,
        mal_anime_id=mal_anime_id,
        confidence=confidence,
        mapping_source="user_exact" if exact else "user_approved",
        approved_by_user=True,
        notes=notes,
    )
    print(
        json.dumps(
            {
                "provider": mapping.provider,
                "provider_series_id": mapping.provider_series_id,
                "mal_anime_id": mapping.mal_anime_id,
                "confidence": mapping.confidence,
                "mapping_source": mapping.mapping_source,
                "approved_by_user": mapping.approved_by_user,
                "notes": mapping.notes,
                "created_at": mapping.created_at,
                "updated_at": mapping.updated_at,
            },
            indent=2,
        )
    )
    return 0


def _cmd_dry_run_sync(
    project_root: Path | None,
    limit: int,
    mapping_limit: int,
    approved_mappings_only: bool,
    exact_approved_only: bool,
    persist_queue: bool,
) -> int:
    config = load_config(project_root)
    ensure_directories(config)
    bootstrap_database(config.db_path)
    normalized_limit = _normalize_limit(limit)
    if persist_queue and normalized_limit is not None:
        print("--persist-review-queue requires a full scan; rerun with --limit 0", file=sys.stderr)
        return 2
    try:
        proposals = build_dry_run_sync_plan(
            config,
            limit=normalized_limit,
            mapping_limit=mapping_limit,
            approved_mappings_only=approved_mappings_only,
            exact_approved_only=exact_approved_only,
        )
    except MalApiError as exc:
        print(str(exc), file=sys.stderr)
        return 1
    payload: dict[str, object] = {"proposals": [proposal.as_dict() for proposal in proposals]}
    if persist_queue:
        payload["review_queue"] = persist_sync_review_queue(config, proposals)
    print(json.dumps(payload, indent=2))
    return 0


def _review_queue_item_label(
    item: object,
    *,
    provider_series_titles: dict[str, dict[str, str | None]] | None = None,
) -> dict[str, object]:
    payload = getattr(item, "payload", None)
    title = None
    if isinstance(payload, dict):
        for key in ("title", "crunchyroll_title", "season_title", "mal_title", "suggested_mal_title"):
            value = payload.get(key)
            if isinstance(value, str) and value.strip():
                title = value.strip()
                break
    provider_series_id = getattr(item, "provider_series_id", None)
    series_row = provider_series_titles.get(provider_series_id) if provider_series_titles and isinstance(provider_series_id, str) else None
    if title is None and isinstance(series_row, dict):
        for key in ("season_title", "title"):
            value = series_row.get(key)
            if isinstance(value, str) and value.strip():
                title = value.strip()
                break
    return {
        "provider_series_id": provider_series_id,
        "issue_type": getattr(item, "issue_type", None),
        "severity": getattr(item, "severity", None),
        "title": title,
        "created_at": getattr(item, "created_at", None),
    }



_TRAILING_INSTALLMENT_CLUSTER_RE = re.compile(
    r"(?:\b(?:season|part|cour)\s*\d+\b|\b\d+(?:st|nd|rd|th)\s+season\b|\b(?:final|last)\s+season\b|\b(?:part|cour)\s+[ivx]+\b|\b[ivx]+\b)$",
    re.IGNORECASE,
)


def _review_queue_title_cluster_key(title: str | None) -> str | None:
    if not isinstance(title, str) or not title.strip():
        return None
    value = title.strip()
    previous = None
    while previous != value:
        previous = value
        value = _TRAILING_INSTALLMENT_CLUSTER_RE.sub("", value).strip(" -:()[]")
    normalized = normalize_title(value)
    return normalized or None


def _review_queue_fix_strategy_key(payload: dict[str, object]) -> str | None:
    decision = payload.get("decision")
    if not isinstance(decision, str) or not decision.strip():
        return None
    parts = [decision.strip()]
    reasons = payload.get("reasons")
    normalized_reasons: list[str] = []
    if isinstance(reasons, list):
        normalized_reasons = sorted({reason.strip() for reason in reasons if isinstance(reason, str) and reason.strip()})
    parts.extend(normalized_reasons)
    return " | ".join(parts)


def _filter_review_queue_items(
    items: list[object],
    *,
    provider_series_titles: dict[str, dict[str, str | None]] | None = None,
    title_cluster: str | None = None,
    fix_strategy: str | None = None,
) -> list[object]:
    normalized_title_cluster = _review_queue_title_cluster_key(title_cluster) if title_cluster else None
    normalized_fix_strategy = fix_strategy.strip() if isinstance(fix_strategy, str) and fix_strategy.strip() else None
    if normalized_title_cluster is None and normalized_fix_strategy is None:
        return items
    filtered: list[object] = []
    for item in items:
        payload = getattr(item, "payload", None)
        if not isinstance(payload, dict):
            continue
        if normalized_title_cluster is not None:
            label = _review_queue_item_label(item, provider_series_titles=provider_series_titles)
            if _review_queue_title_cluster_key(label.get("title")) != normalized_title_cluster:
                continue
        if normalized_fix_strategy is not None and _review_queue_fix_strategy_key(payload) != normalized_fix_strategy:
            continue
        filtered.append(item)
    return filtered


def _summarize_review_queue(
    items: list[object],
    *,
    status: str,
    issue_type: str | None,
    provider_series_titles: dict[str, dict[str, str | None]] | None = None,
    title_cluster_filter: str | None = None,
    fix_strategy_filter: str | None = None,
) -> dict[str, object]:
    by_issue_type = Counter(getattr(item, "issue_type", None) for item in items)
    by_severity = Counter(getattr(item, "severity", None) for item in items)
    by_decision: Counter[str] = Counter()
    by_reason: Counter[str] = Counter()
    by_title_cluster: Counter[str] = Counter()
    by_fix_strategy: Counter[str] = Counter()
    decision_examples: dict[str, list[dict[str, object]]] = {}
    reason_examples: dict[str, list[dict[str, object]]] = {}
    title_cluster_examples: dict[str, list[dict[str, object]]] = {}
    title_cluster_labels: dict[str, str] = {}
    fix_strategy_examples: dict[str, list[dict[str, object]]] = {}

    for item in items:
        payload = getattr(item, "payload", None)
        if not isinstance(payload, dict):
            continue
        label = _review_queue_item_label(item, provider_series_titles=provider_series_titles)
        decision = payload.get("decision")
        if isinstance(decision, str) and decision.strip():
            normalized_decision = decision.strip()
            by_decision[normalized_decision] += 1
            examples = decision_examples.setdefault(normalized_decision, [])
            if len(examples) < 3:
                examples.append(label)
        title_cluster_key = _review_queue_title_cluster_key(label.get("title"))
        if title_cluster_key is not None:
            by_title_cluster[title_cluster_key] += 1
            title_cluster_labels.setdefault(title_cluster_key, str(label.get("title") or title_cluster_key))
            examples = title_cluster_examples.setdefault(title_cluster_key, [])
            if len(examples) < 3:
                examples.append(label)
        fix_strategy_key = _review_queue_fix_strategy_key(payload)
        if fix_strategy_key is not None:
            by_fix_strategy[fix_strategy_key] += 1
            examples = fix_strategy_examples.setdefault(fix_strategy_key, [])
            if len(examples) < 3:
                examples.append(label)
        reasons = payload.get("reasons")
        if isinstance(reasons, list):
            for reason in reasons:
                if not isinstance(reason, str) or not reason.strip():
                    continue
                normalized_reason = reason.strip()
                by_reason[normalized_reason] += 1
                examples = reason_examples.setdefault(normalized_reason, [])
                if len(examples) < 3:
                    examples.append(label)

    return {
        "status": status,
        "issue_type_filter": issue_type,
        "title_cluster_filter": _review_queue_title_cluster_key(title_cluster_filter) if title_cluster_filter else None,
        "fix_strategy_filter": fix_strategy_filter.strip() if isinstance(fix_strategy_filter, str) and fix_strategy_filter.strip() else None,
        "count": len(items),
        "by_issue_type": dict(sorted((key, value) for key, value in by_issue_type.items() if key)),
        "by_severity": dict(sorted((key, value) for key, value in by_severity.items() if key)),
        "by_decision": dict(sorted(by_decision.items())),
        "decision_examples": {key: value for key, value in sorted(decision_examples.items())},
        "top_reasons": [
            {"reason": reason, "count": count, "examples": reason_examples.get(reason, [])}
            for reason, count in by_reason.most_common(10)
        ],
        "top_title_clusters": [
            {
                "cluster": cluster,
                "label": title_cluster_labels.get(cluster, cluster),
                "count": count,
                "examples": title_cluster_examples.get(cluster, []),
            }
            for cluster, count in by_title_cluster.most_common(10)
        ],
        "top_fix_strategies": [
            {"strategy": strategy, "count": count, "examples": fix_strategy_examples.get(strategy, [])}
            for strategy, count in by_fix_strategy.most_common(10)
        ],
    }


def _cmd_list_review_queue(
    project_root: Path | None,
    status: str,
    issue_type: str | None,
    summary: bool,
    title_cluster: str | None,
    fix_strategy: str | None,
) -> int:
    config = load_config(project_root)
    ensure_directories(config)
    bootstrap_database(config.db_path)
    items = list_review_queue_entries(config.db_path, status=status, issue_type=issue_type)
    provider_series_titles = get_provider_series_title_map(
        config.db_path,
        provider="crunchyroll",
        provider_series_ids=[item.provider_series_id for item in items if item.provider_series_id],
    )
    items = _filter_review_queue_items(
        items,
        provider_series_titles=provider_series_titles,
        title_cluster=title_cluster,
        fix_strategy=fix_strategy,
    )
    if summary:
        print(
            json.dumps(
                _summarize_review_queue(
                    items,
                    status=status,
                    issue_type=issue_type,
                    provider_series_titles=provider_series_titles,
                    title_cluster_filter=title_cluster,
                    fix_strategy_filter=fix_strategy,
                ),
                indent=2,
            )
        )
        return 0
    print(
        json.dumps(
            [
                {
                    "id": item.id,
                    "provider": item.provider,
                    "provider_series_id": item.provider_series_id,
                    "provider_episode_id": item.provider_episode_id,
                    "issue_type": item.issue_type,
                    "severity": item.severity,
                    "status": item.status,
                    "created_at": item.created_at,
                    "resolved_at": item.resolved_at,
                    "payload": item.payload,
                }
                for item in items
            ],
            indent=2,
        )
    )
    return 0


def _cmd_apply_sync(project_root: Path | None, limit: int, mapping_limit: int, exact_approved_only: bool, execute: bool) -> int:
    config = load_config(project_root)
    ensure_directories(config)
    bootstrap_database(config.db_path)
    try:
        results = execute_approved_sync(
            config,
            limit=_normalize_limit(limit),
            mapping_limit=mapping_limit,
            exact_approved_only=exact_approved_only,
            dry_run=not execute,
        )
    except MalApiError as exc:
        print(str(exc), file=sys.stderr)
        return 1
    print(json.dumps([item.as_dict() for item in results], indent=2))
    return 0


def _cmd_recommend(project_root: Path | None, limit: int, flat: bool) -> int:
    config = load_config(project_root)
    ensure_directories(config)
    bootstrap_database(config.db_path)
    results = build_recommendations(config, limit=_normalize_limit(limit))
    payload: object
    if flat:
        payload = [item.as_dict() for item in results]
    else:
        payload = group_recommendations(results)
    print(json.dumps(payload, indent=2))
    return 0


def _cmd_recommend_refresh_metadata(
    project_root: Path | None,
    limit: int,
    include_discovery_targets: bool,
    discovery_target_limit: int,
) -> int:
    config = load_config(project_root)
    ensure_directories(config)
    bootstrap_database(config.db_path)
    summary = refresh_recommendation_metadata(
        config,
        limit=_normalize_limit(limit),
        include_discovery_targets=include_discovery_targets,
        discovery_target_limit=_normalize_limit(discovery_target_limit),
    )
    print(json.dumps(summary.as_dict(), indent=2))
    return 0


def _cmd_sync(_: Path | None) -> int:
    raise SystemExit(
        "Sync pipeline not implemented yet. Use 'dry-run-sync' for guarded read-only proposals or 'apply-sync' for the approved-mapping-only executor."
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="mal-updater")
    parser.add_argument("--project-root", type=Path, default=None, help="Override project root")
    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser("init", help="Create local dirs and initialize SQLite schema")
    subparsers.add_parser("status", help="Print resolved config, paths, and secret presence")
    mal_auth = subparsers.add_parser("mal-auth-url", help="Generate a MAL OAuth authorization URL + PKCE verifier")
    mal_auth.add_argument("--json", action="store_true", help="Emit machine-readable JSON")
    mal_auth_login = subparsers.add_parser("mal-auth-login", help="Run a local loopback MAL OAuth flow and persist returned tokens")
    mal_auth_login.add_argument("--timeout-seconds", type=float, default=300.0, help="How long to wait for the local callback before failing")
    mal_auth_login.add_argument("--no-verify", action="store_true", help="Skip the follow-up GET /users/@me token check")
    mal_refresh = subparsers.add_parser("mal-refresh", help="Refresh the persisted MAL access token using the local refresh token")
    mal_refresh.add_argument("--no-verify", action="store_true", help="Skip the follow-up GET /users/@me token check")
    subparsers.add_parser("mal-whoami", help="Call MAL GET /users/@me with the currently configured access token")
    crunchyroll_auth_login = subparsers.add_parser(
        "crunchyroll-auth-login",
        help="Use local Crunchyroll username/password secrets to stage Crunchyroll refresh-token auth material",
    )
    crunchyroll_auth_login.add_argument("--profile", default="default", help="Crunchyroll state profile name")
    crunchyroll_auth_login.add_argument("--no-verify", action="store_true", help="Skip the follow-up GET /accounts/v1/me token check")
    crunchyroll_fetch_snapshot = subparsers.add_parser(
        "crunchyroll-fetch-snapshot",
        help="Use the Python Crunchyroll transport to fetch a live normalized snapshot",
    )
    crunchyroll_fetch_snapshot.add_argument("--profile", default="default", help="Crunchyroll state profile name")
    crunchyroll_fetch_snapshot.add_argument("--out", type=Path, default=None, help="Optional JSON file path to write the fetched snapshot")
    crunchyroll_fetch_snapshot.add_argument("--ingest", action="store_true", help="Immediately validate and ingest the fetched snapshot into SQLite")
    crunchyroll_fetch_snapshot.add_argument(
        "--full-refresh",
        action="store_true",
        help="Ignore the local incremental sync boundary and fetch the full currently reachable Crunchyroll history/watchlist pages",
    )
    validate_snapshot = subparsers.add_parser("validate-snapshot", help="Validate a Crunchyroll snapshot JSON payload")
    validate_snapshot.add_argument("snapshot", nargs="?", type=Path, help="Snapshot JSON file path (defaults to stdin)")
    ingest_snapshot = subparsers.add_parser("ingest-snapshot", help="Validate and ingest a Crunchyroll snapshot into SQLite")
    ingest_snapshot.add_argument("snapshot", nargs="?", type=Path, help="Snapshot JSON file path (defaults to stdin)")
    map_series_cmd = subparsers.add_parser("map-series", help="Search MAL for conservative mapping candidates for ingested Crunchyroll series")
    map_series_cmd.add_argument("--limit", type=int, default=20, help="How many ingested series to inspect")
    map_series_cmd.add_argument("--mapping-limit", type=int, default=5, help="How many MAL candidates to keep per series")
    review_mappings = subparsers.add_parser(
        "review-mappings",
        help="Build a mapping review list that preserves existing approved mappings and flags the rest for approval or manual review",
    )
    review_mappings.add_argument("--limit", type=int, default=20, help="How many ingested series to inspect (use 0 for all; required when persisting review_queue)")
    review_mappings.add_argument("--mapping-limit", type=int, default=5, help="How many MAL candidates to keep per series")
    review_mappings.add_argument("--persist-review-queue", action="store_true", help="Replace the open mapping_review queue rows with this run's unresolved items")
    list_mappings = subparsers.add_parser("list-mappings", help="List persisted Crunchyroll -> MAL mappings from SQLite")
    list_mappings.add_argument("--approved-only", action="store_true", help="Only include mappings explicitly approved by the user")
    approve_mapping = subparsers.add_parser("approve-mapping", help="Persist a user-approved Crunchyroll -> MAL series mapping")
    approve_mapping.add_argument("provider_series_id", help="Crunchyroll provider_series_id to approve")
    approve_mapping.add_argument("mal_anime_id", type=int, help="Chosen MAL anime id")
    approve_mapping.add_argument("--confidence", type=float, default=None, help="Optional confidence score to store alongside the approval")
    approve_mapping.add_argument("--notes", default=None, help="Optional operator note explaining the approval")
    approve_mapping.add_argument(
        "--exact",
        action="store_true",
        help="Mark this manual approval as exact-safe so the unattended exact-approved executor may use it",
    )
    dry_run_sync = subparsers.add_parser("dry-run-sync", help="Generate guarded read-only MAL sync proposals from ingested Crunchyroll data")
    dry_run_sync.add_argument("--limit", type=int, default=20, help="How many ingested series to inspect (use 0 for all; required when persisting review_queue)")
    dry_run_sync.add_argument("--mapping-limit", type=int, default=5, help="How many MAL candidates to keep per series")
    dry_run_sync.add_argument(
        "--approved-mappings-only",
        action="store_true",
        help="Only produce proposals for series with explicit user-approved persisted mappings",
    )
    dry_run_sync.add_argument("--persist-review-queue", action="store_true", help="Replace the open sync_review queue rows with this run's non-actionable items")
    dry_run_sync.add_argument(
        "--exact-approved-only",
        action="store_true",
        help="When using approved mappings, restrict planning to exact approved mappings only (currently auto_exact/user_exact)",
    )
    list_review_queue = subparsers.add_parser("list-review-queue", help="List persisted review_queue rows from SQLite")
    list_review_queue.add_argument("--status", default="open", choices=["open", "resolved"], help="Review row status to show")
    list_review_queue.add_argument("--issue-type", default=None, choices=["mapping_review", "sync_review"], help="Optional issue type filter")
    list_review_queue.add_argument("--summary", action="store_true", help="Emit a compact summary of queue counts/decisions/reasons instead of every row")
    list_review_queue.add_argument("--title-cluster", default=None, help="Only show review rows whose normalized title cluster matches this value (for example: 'example show' or 'Example Show Season 2')")
    list_review_queue.add_argument("--fix-strategy", default=None, help="Only show review rows whose decision+reasons strategy exactly matches this value from --summary")
    apply_sync = subparsers.add_parser("apply-sync", help="Guarded MAL executor that only operates on approved mappings and forward-safe proposals")
    apply_sync.add_argument("--limit", type=int, default=20, help="How many ingested series to inspect")
    apply_sync.add_argument("--mapping-limit", type=int, default=5, help="Reserved for parity with dry-run planning")
    apply_sync.add_argument(
        "--exact-approved-only",
        action="store_true",
        help="Only operate on exact approved mappings (currently auto_exact/user_exact)",
    )
    apply_sync.add_argument("--execute", action="store_true", help="Actually write MAL updates; otherwise revalidate and print what would be applied")
    recommend = subparsers.add_parser(
        "recommend",
        help="Generate local recommendations from the ingested Crunchyroll dataset (grouped by category by default)",
    )
    recommend.add_argument("--limit", type=int, default=20, help="How many recommendations to emit (use 0 for all)")
    recommend.add_argument("--flat", action="store_true", help="Emit the legacy single flat JSON list instead of grouped sections")
    recommend_refresh = subparsers.add_parser(
        "recommend-refresh-metadata",
        help="Refresh MAL metadata/relation cache for mapped anime so recommendations can use richer continuation evidence",
    )
    recommend_refresh.add_argument("--limit", type=int, default=0, help="How many mapped MAL anime to refresh (use 0 for all)")
    recommend_refresh.add_argument(
        "--include-discovery-targets",
        action="store_true",
        help="Also hydrate minimal metadata for top recommended target anime so discovery suppression/ranking can use MAL list state and metadata",
    )
    recommend_refresh.add_argument(
        "--discovery-target-limit",
        type=int,
        default=0,
        help="How many discovered target anime to hydrate when --include-discovery-targets is used (use 0 for all discovered targets)",
    )
    subparsers.add_parser("sync", help="Reserved for future sync orchestration")
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    if args.command == "init":
        return _cmd_init(args.project_root)
    if args.command == "status":
        return _cmd_status(args.project_root)
    if args.command == "mal-auth-url":
        return _cmd_mal_auth_url(args.project_root, args.json)
    if args.command == "mal-auth-login":
        return _cmd_mal_auth_login(args.project_root, args.timeout_seconds, verify_whoami=not args.no_verify)
    if args.command == "mal-refresh":
        return _cmd_mal_refresh(args.project_root, verify_whoami=not args.no_verify)
    if args.command == "mal-whoami":
        return _cmd_mal_whoami(args.project_root)
    if args.command == "crunchyroll-auth-login":
        return _cmd_crunchyroll_auth_login(args.project_root, args.profile, args.no_verify)
    if args.command == "crunchyroll-fetch-snapshot":
        return _cmd_crunchyroll_fetch_snapshot(args.project_root, args.profile, args.out, args.ingest, args.full_refresh)
    if args.command == "validate-snapshot":
        return _cmd_validate_snapshot(args.project_root, args.snapshot)
    if args.command == "ingest-snapshot":
        return _cmd_ingest_snapshot(args.project_root, args.snapshot)
    if args.command == "map-series":
        return _cmd_map_series(args.project_root, args.limit, args.mapping_limit)
    if args.command == "review-mappings":
        return _cmd_review_mappings(args.project_root, args.limit, args.mapping_limit, args.persist_review_queue)
    if args.command == "list-mappings":
        return _cmd_list_mappings(args.project_root, args.approved_only)
    if args.command == "approve-mapping":
        return _cmd_approve_mapping(
            args.project_root,
            args.provider_series_id,
            args.mal_anime_id,
            args.confidence,
            args.notes,
            args.exact,
        )
    if args.command == "dry-run-sync":
        return _cmd_dry_run_sync(
            args.project_root,
            args.limit,
            args.mapping_limit,
            args.approved_mappings_only,
            args.exact_approved_only,
            args.persist_review_queue,
        )
    if args.command == "list-review-queue":
        return _cmd_list_review_queue(
            args.project_root,
            args.status,
            args.issue_type,
            args.summary,
            args.title_cluster,
            args.fix_strategy,
        )
    if args.command == "apply-sync":
        return _cmd_apply_sync(args.project_root, args.limit, args.mapping_limit, args.exact_approved_only, args.execute)
    if args.command == "recommend":
        return _cmd_recommend(args.project_root, args.limit, args.flat)
    if args.command == "recommend-refresh-metadata":
        return _cmd_recommend_refresh_metadata(
            args.project_root,
            args.limit,
            args.include_discovery_targets,
            args.discovery_target_limit,
        )
    if args.command == "sync":
        return _cmd_sync(args.project_root)
    parser.error(f"Unknown command: {args.command}")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
