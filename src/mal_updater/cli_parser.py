from __future__ import annotations

import argparse
from pathlib import Path

from .provider_registry import list_provider_slugs


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="mal-updater")
    parser.add_argument("--project-root", type=Path, default=None, help="Override project root")
    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser("init", help="Create the externalized runtime dirs and initialize SQLite schema")
    subparsers.add_parser("status", help="Print resolved config, runtime paths, and secret presence")
    install_service_parser = subparsers.add_parser("install-service", help="Install/update the repo-owned user systemd service for the long-lived MAL-Updater daemon")
    install_service_parser.add_argument("--no-start", action="store_true", help="Write/enable the service but do not restart it immediately")
    uninstall_service_parser = subparsers.add_parser("uninstall-service", help="Disable and remove the repo-owned user systemd service")
    uninstall_service_parser.add_argument("--no-stop", action="store_true", help="Remove/disable the service without attempting a stop first")
    subparsers.add_parser("start-service", help="Start the MAL-Updater user service")
    subparsers.add_parser("stop-service", help="Stop the MAL-Updater user service")
    subparsers.add_parser("restart-service", help="Restart the MAL-Updater user service")
    service_status = subparsers.add_parser("service-status", help="Print MAL-Updater user service health/runtime status")
    service_status.add_argument("--format", default="json", choices=["json", "summary"], help="Output format: machine-readable JSON (default) or terse operator summary")
    subparsers.add_parser("service-run", help="Run the MAL-Updater daemon loop in the foreground")
    subparsers.add_parser("service-run-once", help="Run one MAL-Updater daemon loop pass and exit")
    exact_approved_sync_cycle = subparsers.add_parser(
        "exact-approved-sync-cycle",
        help="Initialize runtime, refresh all staged provider snapshots, then execute an exact-approved MAL apply cycle",
    )
    exact_approved_sync_cycle.add_argument(
        "--full-refresh",
        action="store_true",
        help="Request full-refresh provider fetches before the exact-approved MAL apply pass",
    )
    bootstrap_audit = subparsers.add_parser(
        "bootstrap-audit",
        help="Audit bootstrap/onboarding readiness: dependencies, runtime dirs, credentials, redirect settings, and service install prerequisites",
    )
    bootstrap_audit.add_argument("--summary", action="store_true", help="Emit terse line-oriented output instead of JSON")
    health_check = subparsers.add_parser("health-check", help="Emit a local operational health summary for auth material, snapshot freshness, mappings, and review backlog")
    health_check.add_argument("--stale-hours", type=float, default=72.0, help="Warn when the latest completed ingest snapshot is older than this many hours")
    health_check.add_argument("--strict", action="store_true", help="Return exit code 2 when warnings are present, while still printing the JSON payload")
    health_check.add_argument("--review-issue-type", default=None, choices=["mapping_review", "sync_review"], help="Optional review_queue issue type to use when building recommended_next/recommended_worklist")
    health_check.add_argument("--review-worklist-limit", type=int, default=3, help="How many ranked review backlog drilldowns to include in recommended_worklist (use 0 to suppress it)")
    health_check.add_argument("--format", default="json", choices=["json", "summary"], help="Output format: machine-readable JSON (default) or terse operator summary")
    health_check.add_argument("--mapping-coverage-threshold", type=float, default=0.8, help="Warn when approved provider->MAL mapping coverage falls below this ratio (default: 0.8)")
    health_check.add_argument("--maintenance-review-limit", type=int, default=25, help="Deprecated compatibility option retained for older callers; ignored for persisted low-coverage mapping-review backlog recommendations, which always use a full scan (--limit 0) because partial review-queue replacement is unsafe")
    health_check_cycle = subparsers.add_parser("health-check-cycle", help="Run the repo-native health-check cycle with optional safe auto-remediation and summary output")
    health_check_cycle.add_argument("--stale-hours", type=float, default=72.0, help="Warn when the latest completed ingest snapshot is older than this many hours")
    health_check_cycle.add_argument("--strict", action="store_true", help="Return exit code 2 when warnings are present in the final summary")
    health_check_cycle.add_argument("--auto-run-recommended", action="store_true", help="Automatically run one allowlisted automation-safe maintenance command when recommended")
    health_check_cycle.add_argument("--auto-run-reason-codes", default="refresh_ingested_snapshot", help="Comma-separated allowlist of maintenance reason codes eligible for auto-remediation; CLI remediations run against --project-root; include refresh_full_snapshot only for explicit recovery")
    health_check_cycle.add_argument("--review-issue-type", default=None, choices=["mapping_review", "sync_review"], help="Optional review_queue issue type to use when building recommended_next/recommended_worklist")
    health_check_cycle.add_argument("--review-worklist-limit", type=int, default=3, help="How many ranked review backlog drilldowns to include in recommended_worklist (use 0 to suppress it)")
    health_check_cycle.add_argument("--mapping-coverage-threshold", type=float, default=0.8, help="Warn when approved provider->MAL mapping coverage falls below this ratio (default: 0.8)")
    health_check_cycle.add_argument("--maintenance-review-limit", type=int, default=25, help="Deprecated compatibility option retained for older callers; ignored for persisted low-coverage mapping-review backlog recommendations, which always use a full scan (--limit 0) because partial review-queue replacement is unsafe")
    mal_auth = subparsers.add_parser("mal-auth-url", help="Generate a MAL OAuth authorization URL + PKCE verifier")
    mal_auth.add_argument("--json", action="store_true", help="Emit machine-readable JSON")
    mal_auth_login = subparsers.add_parser("mal-auth-login", help="Run a local loopback MAL OAuth flow and persist returned tokens")
    mal_auth_login.add_argument("--timeout-seconds", type=float, default=300.0, help="How long to wait for the local callback before failing")
    mal_auth_login.add_argument("--no-verify", action="store_true", help="Skip the follow-up GET /users/@me token check")
    mal_refresh = subparsers.add_parser("mal-refresh", help="Refresh the persisted MAL access token using the local refresh token")
    mal_refresh.add_argument("--no-verify", action="store_true", help="Skip the follow-up GET /users/@me token check")
    subparsers.add_parser("mal-whoami", help="Call MAL GET /users/@me with the currently configured access token")
    provider_auth_login = subparsers.add_parser(
        "provider-auth-login",
        help="Run provider-specific auth bootstrap for a named content provider",
    )
    provider_auth_login.add_argument("--provider", required=True, choices=list_provider_slugs(), help="Provider slug")
    provider_auth_login.add_argument("--profile", default="default", help="Provider state profile name")
    provider_auth_login.add_argument("--no-verify", action="store_true", help="Skip any provider-specific follow-up account verification step")
    provider_fetch_snapshot = subparsers.add_parser(
        "provider-fetch-snapshot",
        help="Fetch account-scoped history/watchlist details from a named content provider; not a whole-library crawler",
    )
    provider_fetch_snapshot.add_argument("--provider", required=True, choices=list_provider_slugs(), help="Provider slug")
    provider_fetch_snapshot.add_argument("--profile", default="default", help="Provider state profile name")
    provider_fetch_snapshot.add_argument("--out", type=Path, default=None, help="Optional JSON file path to write the fetched snapshot")
    provider_fetch_snapshot.add_argument("--ingest", action="store_true", help="Immediately validate and ingest the fetched snapshot into SQLite")
    provider_fetch_snapshot.add_argument(
        "--full-refresh",
        action="store_true",
        help="Ignore provider-local incremental boundaries and re-fetch account-scoped history/watchlist surfaces only; never crawl whole Crunchyroll/HIDIVE libraries",
    )
    provider_fetch_snapshot.add_argument("--max-history-pages", type=int, default=None, help="Crunchyroll only: stop after this many watch-history pages and mark snapshot raw.partial=true")
    provider_fetch_snapshot.add_argument("--max-watchlist-pages", type=int, default=None, help="Crunchyroll only: stop after this many watchlist pages and mark snapshot raw.partial=true")
    provider_fetch_snapshot.add_argument("--history-start-page", type=int, default=1, help="Crunchyroll only: first watch-history page to fetch for manual chunk resume")
    provider_fetch_snapshot.add_argument("--watchlist-start", type=int, default=0, help="Crunchyroll only: first watchlist offset to fetch for manual chunk resume")
    crunchyroll_auth_login = subparsers.add_parser(
        "crunchyroll-auth-login",
        help="Use local Crunchyroll username/password secrets to stage Crunchyroll refresh-token auth material",
    )
    crunchyroll_auth_login.add_argument("--profile", default="default", help="Crunchyroll state profile name")
    crunchyroll_auth_login.add_argument("--no-verify", action="store_true", help="Skip the follow-up GET /accounts/v1/me token check")
    crunchyroll_fetch_snapshot = subparsers.add_parser(
        "crunchyroll-fetch-snapshot",
        help="Use the Python Crunchyroll transport to fetch account-scoped history/watchlist details; not a whole-library crawler",
    )
    crunchyroll_fetch_snapshot.add_argument("--profile", default="default", help="Crunchyroll state profile name")
    crunchyroll_fetch_snapshot.add_argument("--out", type=Path, default=None, help="Optional JSON file path to write the fetched snapshot")
    crunchyroll_fetch_snapshot.add_argument("--ingest", action="store_true", help="Immediately validate and ingest the fetched snapshot into SQLite")
    crunchyroll_fetch_snapshot.add_argument(
        "--full-refresh",
        action="store_true",
        help="Ignore the local incremental sync boundary and re-fetch account-scoped Crunchyroll history/watchlist pages only; never crawl whole libraries",
    )
    crunchyroll_fetch_snapshot.add_argument("--max-history-pages", type=int, default=None, help="Stop after this many watch-history pages and mark snapshot raw.partial=true")
    crunchyroll_fetch_snapshot.add_argument("--max-watchlist-pages", type=int, default=None, help="Stop after this many watchlist pages and mark snapshot raw.partial=true")
    crunchyroll_fetch_snapshot.add_argument("--history-start-page", type=int, default=1, help="First watch-history page to fetch for manual chunk resume")
    crunchyroll_fetch_snapshot.add_argument("--watchlist-start", type=int, default=0, help="First watchlist offset to fetch for manual chunk resume")
    validate_snapshot = subparsers.add_parser("validate-snapshot", help="Validate a normalized provider snapshot JSON payload")
    validate_snapshot.add_argument("snapshot", nargs="?", type=Path, help="Snapshot JSON file path (defaults to stdin)")
    ingest_snapshot = subparsers.add_parser("ingest-snapshot", help="Validate and ingest a normalized provider snapshot into SQLite")
    ingest_snapshot.add_argument("snapshot", nargs="?", type=Path, help="Snapshot JSON file path (defaults to stdin)")
    provider_stale_rows = subparsers.add_parser(
        "provider-stale-rows",
        help="Inspect read-only provider cache rows older than a cutoff, defaulting to the provider's latest completed full refresh",
    )
    provider_stale_rows.add_argument("--provider", required=True, choices=["all", *list_provider_slugs()], help="Provider slug to inspect, or 'all' for every known provider")
    provider_stale_rows.add_argument("--cutoff", default=None, help="SQLite timestamp cutoff; rows with last_seen_at older than this are reported")
    provider_stale_rows.add_argument("--older-than-days", type=float, default=None, help="Further restrict stale diagnostics to rows last seen at least this many days ago (read-only)")
    provider_stale_rows.add_argument("--limit", type=int, default=5, help="Maximum samples per row family (1-25)")
    provider_stale_rows.add_argument("--format", choices=["json", "summary"], default="json", help="Output format (default: json)")
    map_series_cmd = subparsers.add_parser("map-series", help="Search MAL for conservative mapping candidates for ingested provider series")
    map_series_cmd.add_argument("--limit", type=int, default=20, help="How many ingested series to inspect")
    map_series_cmd.add_argument("--mapping-limit", type=int, default=5, help="How many MAL candidates to keep per series")
    review_mappings = subparsers.add_parser(
        "review-mappings",
        help="Build a mapping review list that preserves existing approved mappings and flags the rest for approval or manual review",
    )
    review_mappings.add_argument("--limit", type=int, default=20, help="How many ingested series to inspect (use 0 for all; required when persisting review_queue)")
    review_mappings.add_argument("--mapping-limit", type=int, default=5, help="How many MAL candidates to keep per series")
    review_mappings.add_argument("--persist-review-queue", action="store_true", help="Replace the open mapping_review queue rows with this run's unresolved items")
    refresh_mapping_review_queue = subparsers.add_parser(
        "refresh-mapping-review-queue",
        help="Recompute mapping-review results for specific provider_series_id values and refresh only those persisted queue rows",
    )
    refresh_mapping_review_queue.add_argument(
        "--provider-series-id",
        action="append",
        default=[],
        help="Provider provider_series_id to refresh in the persisted mapping_review queue (repeatable)",
    )
    refresh_mapping_review_queue.add_argument(
        "--all-open",
        action="store_true",
        help="Refresh every currently open persisted mapping_review row (may be combined with explicit --provider-series-id values)",
    )
    refresh_mapping_review_queue.add_argument("--title-cluster", default=None, help="Only refresh open mapping_review rows whose normalized title cluster matches this value")
    refresh_mapping_review_queue.add_argument("--decision", default=None, help="Only refresh open mapping_review rows whose payload decision exactly matches this value")
    refresh_mapping_review_queue.add_argument("--reason", default=None, help="Only refresh open mapping_review rows whose payload reasons include this exact value")
    refresh_mapping_review_queue.add_argument("--reason-family", default=None, help="Only refresh open mapping_review rows whose payload reasons include this normalized reason family")
    refresh_mapping_review_queue.add_argument("--fix-strategy", default=None, help="Only refresh open mapping_review rows whose normalized fix strategy exactly matches this value")
    refresh_mapping_review_queue.add_argument("--fix-strategy-family", default=None, help="Only refresh open mapping_review rows whose normalized fix strategy family exactly matches this value")
    refresh_mapping_review_queue.add_argument("--cluster-strategy", default=None, help="Only refresh open mapping_review rows whose combined normalized title-cluster/fix-strategy exactly matches this value")
    refresh_mapping_review_queue.add_argument("--cluster-strategy-family", default=None, help="Only refresh open mapping_review rows whose combined normalized title-cluster/fix-strategy family exactly matches this value")
    refresh_mapping_review_queue.add_argument("--mapping-limit", type=int, default=5, help="How many MAL candidates to keep per series")
    list_mappings = subparsers.add_parser("list-mappings", help="List persisted provider -> MAL mappings from SQLite")
    list_mappings.add_argument("--provider", default="all", choices=["all", "crunchyroll", "hidive"], help="Optional provider filter (default: all)")
    list_mappings.add_argument("--approved-only", action="store_true", help="Only include mappings explicitly approved by the user")
    approve_mapping = subparsers.add_parser("approve-mapping", help="Persist a user-approved provider -> MAL series mapping")
    approve_mapping.add_argument("provider_series_id", help="Provider provider_series_id to approve")
    approve_mapping.add_argument("mal_anime_id", type=int, help="Chosen MAL anime id")
    approve_mapping.add_argument("--provider", default="crunchyroll", choices=list(list_provider_slugs()), help="Provider slug for this approval (default: crunchyroll)")
    approve_mapping.add_argument("--confidence", type=float, default=None, help="Optional confidence score to store alongside the approval")
    approve_mapping.add_argument("--notes", default=None, help="Optional operator note explaining the approval")
    approve_mapping.add_argument(
        "--exact",
        action="store_true",
        help="Mark this manual approval as exact-safe so the unattended exact-approved executor may use it",
    )
    dry_run_sync = subparsers.add_parser("dry-run-sync", help="Generate guarded read-only MAL sync proposals from ingested provider data")
    dry_run_sync.add_argument("--provider", default="all", choices=["all", *list_provider_slugs()], help="Provider slug to plan against, or 'all' to aggregate across providers")
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
    list_review_queue.add_argument("--format", dest="output_format", default="json", choices=["json", "summary"], help="Output format: json rows (default) or summary JSON")
    list_review_queue.add_argument("--limit", type=int, default=0, help="Maximum number of filtered rows to emit in non-summary mode (use 0 for all)")
    list_review_queue.add_argument("--provider-series-id", default=None, help="Only show review rows for one exact provider_series_id")
    list_review_queue.add_argument("--title-cluster", default=None, help="Only show review rows whose normalized title cluster matches this value (for example: 'example show' or 'Example Show Season 2')")
    list_review_queue.add_argument("--decision", default=None, help="Only show review rows whose payload decision exactly matches this value from --summary")
    list_review_queue.add_argument("--reason", default=None, help="Only show review rows whose payload reasons include this exact value from --summary")
    list_review_queue.add_argument("--reason-family", default=None, help="Only show review rows whose payload reasons include this normalized reason family from --summary")
    list_review_queue.add_argument("--fix-strategy", default=None, help="Only show review rows whose decision+reasons strategy exactly matches this value from --summary")
    list_review_queue.add_argument("--fix-strategy-family", default=None, help="Only show review rows whose normalized decision+reason-family strategy exactly matches this value from --summary")
    list_review_queue.add_argument("--cluster-strategy", default=None, help="Only show review rows whose combined franchise/fix-strategy bucket exactly matches this value from --summary (format: '<cluster> || <strategy>')")
    list_review_queue.add_argument("--cluster-strategy-family", default=None, help="Only show review rows whose combined franchise/fix-strategy-family bucket exactly matches this value from --summary (format: '<cluster> || <strategy-family>')")
    review_queue_next = subparsers.add_parser(
        "review-queue-next",
        help="Pick the next highest-signal review backlog bucket and emit the exact drilldown command to run",
    )
    review_queue_next.add_argument("--status", default="open", choices=["open", "resolved"], help="Review row status to inspect")
    review_queue_next.add_argument("--issue-type", default=None, choices=["mapping_review", "sync_review"], help="Optional issue type filter")
    review_queue_next.add_argument(
        "--bucket",
        default="auto",
        choices=["auto", "cluster-strategy", "cluster-strategy-family", "fix-strategy", "fix-strategy-family", "title-cluster", "reason", "reason-family", "decision"],
        help="Which bucket family to prefer when choosing the next drilldown",
    )
    review_queue_next.add_argument("--title-cluster", default=None, help="Optional existing title-cluster scope to preserve while choosing the next drilldown")
    review_queue_next.add_argument("--decision", default=None, help="Optional existing decision scope to preserve while choosing the next drilldown")
    review_queue_next.add_argument("--reason", default=None, help="Optional existing reason scope to preserve while choosing the next drilldown")
    review_queue_next.add_argument("--fix-strategy", default=None, help="Optional existing fix-strategy scope to preserve while choosing the next drilldown")
    review_queue_next.add_argument("--cluster-strategy", default=None, help="Optional existing combined cluster/fix-strategy scope to preserve while choosing the next drilldown")
    review_queue_next.add_argument("--reason-family", default=None, help="Optional existing reason-family scope to preserve while choosing the next drilldown")
    review_queue_next.add_argument("--fix-strategy-family", default=None, help="Optional existing fix-strategy-family scope to preserve while choosing the next drilldown")
    review_queue_next.add_argument("--cluster-strategy-family", default=None, help="Optional existing combined cluster/fix-strategy-family scope to preserve while choosing the next drilldown")
    review_queue_worklist = subparsers.add_parser(
        "review-queue-worklist",
        help="Emit the next several highest-signal review backlog drilldowns as a ranked worklist",
    )
    review_queue_worklist.add_argument("--status", default="open", choices=["open", "resolved"], help="Review row status to inspect")
    review_queue_worklist.add_argument("--issue-type", default=None, choices=["mapping_review", "sync_review"], help="Optional issue type filter")
    review_queue_worklist.add_argument("--limit", type=int, default=5, help="Maximum number of ranked drilldowns to emit")
    review_queue_worklist.add_argument("--title-cluster", default=None, help="Optional existing title-cluster scope to preserve while building the worklist")
    review_queue_worklist.add_argument("--decision", default=None, help="Optional existing decision scope to preserve while building the worklist")
    review_queue_worklist.add_argument("--reason", default=None, help="Optional existing reason scope to preserve while building the worklist")
    review_queue_worklist.add_argument("--fix-strategy", default=None, help="Optional existing fix-strategy scope to preserve while building the worklist")
    review_queue_worklist.add_argument("--cluster-strategy", default=None, help="Optional existing combined cluster/fix-strategy scope to preserve while building the worklist")
    review_queue_worklist.add_argument("--reason-family", default=None, help="Optional existing reason-family scope to preserve while building the worklist")
    review_queue_worklist.add_argument("--fix-strategy-family", default=None, help="Optional existing fix-strategy-family scope to preserve while building the worklist")
    review_queue_worklist.add_argument("--cluster-strategy-family", default=None, help="Optional existing combined cluster/fix-strategy-family scope to preserve while building the worklist")
    review_queue_apply_worklist = subparsers.add_parser(
        "review-queue-apply-worklist",
        help="Apply the ranked review-queue worklist in one shot by resolving or reopening the selected buckets",
    )
    review_queue_apply_worklist.add_argument("--status", default="open", choices=["open", "resolved"], help="Review row status to mutate (open -> resolve, resolved -> reopen)")
    review_queue_apply_worklist.add_argument("--issue-type", default=None, choices=["mapping_review", "sync_review"], help="Optional issue type filter")
    review_queue_apply_worklist.add_argument("--limit", type=int, default=3, help="How many ranked worklist buckets to apply")
    review_queue_apply_worklist.add_argument("--per-bucket-limit", type=int, default=20, help="Maximum number of matching rows to update per selected bucket (use 0 for all)")
    review_queue_apply_worklist.add_argument("--title-cluster", default=None, help="Optional existing title-cluster scope to preserve while applying the worklist")
    review_queue_apply_worklist.add_argument("--decision", default=None, help="Optional existing decision scope to preserve while applying the worklist")
    review_queue_apply_worklist.add_argument("--reason", default=None, help="Optional existing reason scope to preserve while applying the worklist")
    review_queue_apply_worklist.add_argument("--fix-strategy", default=None, help="Optional existing fix-strategy scope to preserve while applying the worklist")
    review_queue_apply_worklist.add_argument("--cluster-strategy", default=None, help="Optional existing combined cluster/fix-strategy scope to preserve while applying the worklist")
    review_queue_apply_worklist.add_argument("--reason-family", default=None, help="Optional existing reason-family scope to preserve while applying the worklist")
    review_queue_apply_worklist.add_argument("--fix-strategy-family", default=None, help="Optional existing fix-strategy-family scope to preserve while applying the worklist")
    review_queue_apply_worklist.add_argument("--cluster-strategy-family", default=None, help="Optional existing combined cluster/fix-strategy-family scope to preserve while applying the worklist")
    review_queue_refresh_worklist = subparsers.add_parser(
        "review-queue-refresh-worklist",
        help="Apply the ranked review-queue worklist in one shot by recomputing mapping_review buckets and refreshing the persisted rows",
    )
    review_queue_refresh_worklist.add_argument("--status", default="open", choices=["open", "resolved"], help="Review row status to inspect while selecting refresh buckets")
    review_queue_refresh_worklist.add_argument("--format", dest="output_format", default="json", choices=["json", "summary"], help="Output format: machine-readable JSON (default) or terse operator summary")
    review_queue_refresh_worklist.add_argument("--issue-type", default=None, choices=["mapping_review", "sync_review"], help="Optional issue type filter (currently only mapping_review is supported)")
    review_queue_refresh_worklist.add_argument("--limit", type=int, default=3, help="How many ranked worklist buckets to refresh")
    review_queue_refresh_worklist.add_argument("--per-bucket-limit", type=int, default=20, help="Maximum number of matching rows to refresh per selected bucket (use 0 for all)")
    review_queue_refresh_worklist.add_argument("--mapping-limit", type=int, default=5, help="How many MAL candidates to keep per refreshed series")
    review_queue_refresh_worklist.add_argument("--title-cluster", default=None, help="Optional existing title-cluster scope to preserve while refreshing the worklist")
    review_queue_refresh_worklist.add_argument("--decision", default=None, help="Optional existing decision scope to preserve while refreshing the worklist")
    review_queue_refresh_worklist.add_argument("--reason", default=None, help="Optional existing reason scope to preserve while refreshing the worklist")
    review_queue_refresh_worklist.add_argument("--fix-strategy", default=None, help="Optional existing fix-strategy scope to preserve while refreshing the worklist")
    review_queue_refresh_worklist.add_argument("--cluster-strategy", default=None, help="Optional existing combined cluster/fix-strategy scope to preserve while refreshing the worklist")
    review_queue_refresh_worklist.add_argument("--reason-family", default=None, help="Optional existing reason-family scope to preserve while refreshing the worklist")
    review_queue_refresh_worklist.add_argument("--fix-strategy-family", default=None, help="Optional existing fix-strategy-family scope to preserve while refreshing the worklist")
    review_queue_refresh_worklist.add_argument("--cluster-strategy-family", default=None, help="Optional existing combined cluster/fix-strategy-family scope to preserve while refreshing the worklist")
    resolve_review_queue = subparsers.add_parser(
        "resolve-review-queue",
        help="Mark matching open review_queue rows as resolved after triage",
    )
    resolve_review_queue.add_argument("--issue-type", default=None, choices=["mapping_review", "sync_review"], help="Optional issue type filter")
    resolve_review_queue.add_argument("--limit", type=int, default=20, help="Maximum number of matching open rows to resolve (use 0 for all)")
    resolve_review_queue.add_argument("--title-cluster", default=None, help="Only resolve rows whose normalized title cluster matches this value")
    resolve_review_queue.add_argument("--decision", default=None, help="Only resolve rows whose payload decision exactly matches this value")
    resolve_review_queue.add_argument("--reason", default=None, help="Only resolve rows whose payload reasons include this value")
    resolve_review_queue.add_argument("--fix-strategy", default=None, help="Only resolve rows whose decision+reasons strategy exactly matches this value")
    resolve_review_queue.add_argument("--cluster-strategy", default=None, help="Only resolve rows whose combined franchise/fix-strategy bucket exactly matches this value")
    resolve_review_queue.add_argument("--reason-family", default=None, help="Only resolve rows whose payload reasons include this normalized family")
    resolve_review_queue.add_argument("--fix-strategy-family", default=None, help="Only resolve rows whose normalized decision+reason-family strategy exactly matches this value")
    resolve_review_queue.add_argument("--cluster-strategy-family", default=None, help="Only resolve rows whose combined franchise/fix-strategy-family bucket exactly matches this value")
    reopen_review_queue = subparsers.add_parser(
        "reopen-review-queue",
        help="Move matching resolved review_queue rows back to open when residue was cleared too aggressively or needs another pass",
    )
    reopen_review_queue.add_argument("--issue-type", default=None, choices=["mapping_review", "sync_review"], help="Optional issue type filter")
    reopen_review_queue.add_argument("--limit", type=int, default=20, help="Maximum number of matching resolved rows to reopen (use 0 for all)")
    reopen_review_queue.add_argument("--title-cluster", default=None, help="Only reopen rows whose normalized title cluster matches this value")
    reopen_review_queue.add_argument("--decision", default=None, help="Only reopen rows whose payload decision exactly matches this value")
    reopen_review_queue.add_argument("--reason", default=None, help="Only reopen rows whose payload reasons include this value")
    reopen_review_queue.add_argument("--fix-strategy", default=None, help="Only reopen rows whose decision+reasons strategy exactly matches this value")
    reopen_review_queue.add_argument("--cluster-strategy", default=None, help="Only reopen rows whose combined franchise/fix-strategy bucket exactly matches this value")
    reopen_review_queue.add_argument("--reason-family", default=None, help="Only reopen rows whose payload reasons include this normalized family")
    reopen_review_queue.add_argument("--fix-strategy-family", default=None, help="Only reopen rows whose normalized decision+reason-family strategy exactly matches this value")
    reopen_review_queue.add_argument("--cluster-strategy-family", default=None, help="Only reopen rows whose combined franchise/fix-strategy-family bucket exactly matches this value")
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
        help="Generate local recommendations from the ingested provider dataset (grouped by category by default)",
    )
    recommend.add_argument("--limit", type=int, default=20, help="How many recommendations to emit (use 0 for all)")
    recommend.add_argument("--flat", action="store_true", help="Emit the legacy single flat JSON list instead of grouped sections")
    recommend.add_argument(
        "--include-dormant",
        action="store_true",
        help="Operator diagnostic: include dormant/internal discovery candidates without actionable verified provider+dub eligibility",
    )
    recommend.add_argument(
        "--persist-snapshot",
        action="store_true",
        help="Persist the emitted recommendation rows as a scored snapshot for later comparison/export",
    )
    recommend_snapshots = subparsers.add_parser(
        "recommend-snapshots",
        help="List rows from the latest persisted recommendation snapshot",
    )
    recommend_snapshots.add_argument("--limit", type=int, default=100, help="Maximum rows to emit from the latest run")
    recommend_snapshots.add_argument("--format", dest="output_format", default="json", choices=["json", "summary"], help="Output format: machine-readable JSON (default) or terse operator summary")
    recommend_maintain = subparsers.add_parser(
        "recommend-maintain",
        help="Run one unattended, write-conservative recommendation maintenance cycle",
    )
    recommend_maintain.add_argument("--dry-run", action="store_true", help="Print the planned command sequence without invoking provider/MAL calls")
    recommend_maintain.add_argument("--metadata-limit", type=int, default=25, help="Mapped MAL anime metadata rows to refresh this cycle")
    recommend_maintain.add_argument("--discovery-target-limit", type=int, default=25, help="Discovery target anime metadata rows to hydrate this cycle")
    recommend_maintain.add_argument("--recommendation-limit", type=int, default=100, help="Recommendation rows to persist in the snapshot")
    recommend_maintain.add_argument("--mapping-limit", type=int, default=25, help="Provider rows to inspect in exact-approved-only sync dry-run review")
    recommend_maintain.add_argument("--mal-list-max-pages", type=int, default=3, help="Official MAL @me anime-list pages to refresh before retaining older rows as a bounded partial cycle")
    recommend_maintain.add_argument("--provider-max-history-pages", type=int, default=None, help="Crunchyroll chunk budget for history pages; partial chunks stay incremental")
    recommend_maintain.add_argument("--provider-max-watchlist-pages", type=int, default=None, help="Crunchyroll chunk budget for watchlist pages; partial chunks stay incremental")
    recommend_maintain.add_argument("--skip-provider-refresh", action="store_true", help="Skip provider snapshot refresh for this cycle")
    recommend_maintain.add_argument("--local-only", action="store_true", help="Run only DB/local snapshot and health work (daemon-safe; no MAL/provider calls)")
    recommend_dashboard = subparsers.add_parser(
        "recommend-dashboard",
        help="Write a dependency-free sortable local HTML recommendation dashboard",
    )
    recommend_dashboard.add_argument("--output", required=True, type=Path, help="HTML file to write")
    recommend_dashboard.add_argument(
        "--limit",
        type=int,
        default=120,
        help=f"How many recommendations to include (default: {120}; use 0 for all)",
    )
    recommend_dashboard.add_argument(
        "--include-dormant",
        action="store_true",
        help="Operator diagnostic: include dormant/internal discovery candidates without actionable verified provider+dub eligibility",
    )
    dashboard_serve = subparsers.add_parser(
        "dashboard-serve",
        help="Serve a live local HTTP dashboard from current SQLite/runtime state",
    )
    dashboard_serve.add_argument("--host", default="127.0.0.1", help="Bind host (default: 127.0.0.1)")
    dashboard_serve.add_argument("--port", type=int, default=8766, help="Bind port (default: 8766)")
    dashboard_serve.add_argument(
        "--limit",
        type=int,
        default=120,
        help=(
            f"Maximum latest snapshot recommendation rows per section to expose "
            f"(default: {120}; safe range "
            f"{1}-{500}; "
            "use ?limit= on /api/dashboard to override per request; invalid query values use the default)"
        ),
    )
    mal_list_refresh = subparsers.add_parser(
        "mal-list-refresh",
        help="Refresh the official read-only MAL @me anime list cache for recommendation seeds",
    )
    mal_list_refresh.add_argument(
        "--status",
        action="append",
        choices=["all", "completed", "watching", "on_hold", "dropped", "plan_to_watch"],
        default=None,
        help="MAL list status to refresh (repeatable); default/all fetches every status and prunes absent rows only after a complete run",
    )
    mal_list_refresh.add_argument("--page-size", type=int, default=100, help="MAL page size, clamped by the client to 1-100")
    mal_list_refresh.add_argument("--max-pages", type=int, default=3, help="Positive maximum pages to fetch before preserving existing cache as partial (default: 3)")
    mal_list_refresh.add_argument("--complete", action="store_true", help="Opt in to pruning absent rows, but only if MAL pagination reaches a terminal page within --max-pages")
    mal_list_refresh.add_argument("--format", choices=["json", "summary"], default="json", help="Output format (default: json)")
    recommend_refresh = subparsers.add_parser(
        "recommend-refresh-metadata",
        help="Refresh paced MAL metadata/relation cache for mapped anime; provider lookups remain title-specific only",
    )
    recommend_refresh.add_argument("--limit", type=int, default=0, help="How many mapped MAL anime to refresh (use 0 for all; MAL refreshes are paced by client throttling and should be spread over time)")
    recommend_refresh.add_argument(
        "--include-discovery-targets",
        action="store_true",
        help="Also hydrate minimal metadata for top recommended target anime so discovery suppression/ranking can use MAL list state and metadata",
    )
    recommend_refresh.add_argument("--force-refresh", action="store_true", help="Bypass fresh MAL metadata/detail caches for an operator-forced refresh")
    recommend_refresh.add_argument(
        "--discovery-target-limit",
        type=int,
        default=0,
        help="How many discovered target anime to hydrate when --include-discovery-targets is used (use 0 for all discovered targets)",
    )
    recommend_full_userrecs = subparsers.add_parser(
        "recommend-refresh-full-userrecs",
        help="Bounded cold-path harvest of complete public MAL /userrecs aggregates for cached completed/watching/on_hold MAL list titles",
    )
    recommend_full_userrecs.add_argument("--limit", type=int, default=0, help="How many positive MAL list source titles to harvest this run (use 0 for all due sources)")
    recommend_full_userrecs.add_argument("--force-refresh", action="store_true", help="Refresh due and fresh complete public userrecs rows without letting official-detail top-10 data overwrite them")
    recommend_full_userrecs.add_argument("--stale-after-days", type=int, default=45, help="Refresh complete public userrecs rows older than this many days (default: 45)")
    recommend_full_userrecs.add_argument("--max-pages", type=int, default=3, help="Maximum advertised same-origin userrecs pages to follow per title before preserving existing edges as partial/failed")
    recommend_full_userrecs.add_argument("--max-body-mb", type=float, default=4.0, help="Maximum HTML body size per public MAL userrecs page before preserving existing edges as failed")
    recommend_full_userrecs.add_argument("--format", choices=["json", "summary"], default="json", help="Output format (default: json)")
    recommend_enrich = subparsers.add_parser(
        "recommend-enrich-provider-availability",
        help="Use bounded provider title search to enrich recommendation availability cache",
    )
    recommend_enrich.add_argument("--limit", type=int, default=25, help="Maximum recommendation candidates to inspect")
    recommend_enrich.add_argument("--provider", choices=list(list_provider_slugs()), help="Provider slug to query; defaults to all registered providers")
    recommend_enrich.add_argument("--search-limit", type=int, default=10, help="Maximum provider search results per query")
    recommend_enrich.add_argument(
        "--queries-per-candidate",
        type=int,
        default=0,
        help="Maximum title aliases queried per recommendation candidate (0 keeps the manual all-alias behavior)",
    )
    recommend_enrich.add_argument("--dry-run", action="store_true", help="Report/cache provider search candidates without replacing the review queue")
    recommend_coverage = subparsers.add_parser(
        "recommend-coverage",
        help="Report read-only MAL recommendation harvest coverage for mapped/watched MAL anime IDs",
    )
    recommend_coverage.add_argument(
        "--stale-after-days",
        type=int,
        default=14,
        help="Mark recommendation source caches older than this many days as stale (use 0 to disable stale marking)",
    )
    push_recommendations_webhook = subparsers.add_parser(
        "push-recommendations-webhook",
        help="Send the current recommendation digest to OpenClaw via the configured webhook ingress",
    )
    push_recommendations_webhook.add_argument("--limit", type=int, default=20, help="How many recommendations to consider (use 0 for all)")
    push_recommendations_webhook.add_argument(
        "--include-dormant",
        action="store_true",
        help="Include discovery candidates that are not currently matched to a registered provider catalog",
    )
    push_recommendations_webhook.add_argument(
        "--delivery-mode",
        choices=["fresh", "digest", "all"],
        default=None,
        help="Override the OpenClaw delivery policy (defaults to config; the daemon should usually stay on 'fresh')",
    )
    push_recommendations_webhook.add_argument(
        "--dry-run",
        action="store_true",
        help="Build the outbound payload without performing the webhook POST",
    )
    return parser
