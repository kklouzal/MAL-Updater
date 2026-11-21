# TODO

Open decisions only. Shipped work is recorded in `CHANGELOG.md`; bugs and feature requests go to <https://github.com/kklouzal/MAL-Updater/issues>.

- [ ] Choose a retention policy for provider rows that a successful full refresh no longer returns. Today they are classified (`latest_sync_run_stale_provider_rows`) and inspected with the read-only `provider-stale-rows`; nothing prunes or archives them.
- [ ] Decide whether HIDIVE needs a chunk-resumable full refresh. HIDIVE has no periodic unattended full refresh; only a hot run that cannot reach its stored boundary within 20 history pages escalates to one.
- [ ] Decide whether to retire `scripts/run_exact_approved_sync_cycle.sh` and `scripts/run_health_check_cycle.sh`, which only wrap the `exact-approved-sync-cycle` and `health-check-cycle` commands.
