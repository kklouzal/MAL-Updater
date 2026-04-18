# Decisions

## 2026-03-14 - Core integration direction

### Decision
Build MAL-Updater as a local Orin-hosted integration using:
- Python application/worker
- SQLite state database
- official MAL OAuth + REST API
- Python-side Crunchyroll auth + live fetches

### Why
- the working Crunchyroll path on this host is Python-side
- Python is better for orchestration, mapping logic, sync policy, and future recommendation work
- SQLite is sufficient and simple for local state
- keeping the implementation in one language leaves the repo smaller and easier to maintain

## 2026-03-14 - Sync direction

### Decision
One-way sync first: Crunchyroll -> MyAnimeList.

### Why
- Crunchyroll is the behavioral source of truth for watched progress
- MAL should be updated conservatively as the public-facing tracking layer
- two-way reconciliation adds unnecessary risk early

## 2026-03-14 - Sync policy

### Decision
This is a **missing-data-first** system.

### Rules
- Do not decrease MAL progress automatically.
- Do not overwrite meaningful existing MAL data automatically.
- Treat MAL `status` as missing only when absent/null.
- Treat MAL watched-progress as missing only when list status is absent; `plan_to_watch` + `0` is meaningful and should be preserved.
- Exception: if Crunchyroll proves completed episode progress (`> 0` watched episodes), a MAL `plan_to_watch` entry may be upgraded forward to `watching` or `completed`.
- Suppress `watching` proposals with `0` watched episodes entirely; partial playback without at least one completed episode is not honest enough to auto-write.
- Treat MAL `score` as missing only when null/absent/`0`.
- Treat MAL `start_date` / `finish_date` as missing only when null/empty.
- Only fill dates when the source evidence is trustworthy enough; currently that means `finish_date` may be filled from Crunchyroll `last_watched_at` only when Crunchyroll-derived status is `completed`.
- Do not auto-resolve ambiguous mappings.
- Only auto-approve mappings when the top MAL candidate is an exact normalized-title match, clearly ahead of the runner-up, and there is no contradictory season/episode/installment evidence.
- Expand generic Crunchyroll season labels like `Season 2` / `Part 2` / `2nd Cour` / `Final Season` into `Title ...` search queries before giving up.
- Treat explicit installment cues (season numbers, ordinal seasons, roman numerals, parts, cours, split indexes, `Final Season`) as explainable matching evidence; matching cues can promote a result, conflicting cues must block auto-approval.
- Use stricter exact-title normalization than the similarity scorer so installment-bearing titles like `Part 1` and `Part 2` do not collapse into the same "exact" match.
- When Crunchyroll `season_number` metadata conflicts with an explicit season number inside `season_title`, prefer the human-readable title cue and surface the conflict in rationale instead of silently trusting the integer.
- Keep a default penalty on MAL movie candidates, but waive it when the provider season title itself is an exact movie title; this handles provider collection shells conservatively without making movies broadly preferred.
- Penalize single-episode `special`/`OVA` residue more strongly when Crunchyroll clearly looks like a normal multi-episode series and the provider title did not explicitly ask for auxiliary content.
- If Crunchyroll episode numbering looks aggregated across seasons, do not treat that raw max episode number as a hard contradiction when explicit installment hints line up and the completed-episode count still fits inside the candidate; surface that as explainable `aggregated_episode_numbering_suspected` evidence instead.
- Do not claim episode-title matching exists unless the metadata source is trustworthy enough to explain and maintain. The current official MAL API surface does not expose episode titles directly, so any future episode-title path must be an explicit, justified choice rather than confidence theater.
- Queue conflicts for review.
- Dry-run before live writes.

## 2026-03-14 - Completion semantics

### Decision
Treat Crunchyroll episodes as watched only when the evidence is strong enough to explainable justify it:
- `completion_ratio >= 0.95`, or
- known remaining playback time is `<= 120` seconds, or
- the episode is at least `0.85` complete and a later episode in the same Crunchyroll series was watched afterwards.

### Why
The real local dataset showed that a blind `0.90` ratio threshold was too hand-wavy:
- many credit-skip cases cluster around **80-120 seconds remaining** rather than a single stable ratio
- **725 / 775** episodes in the `0.85-0.95` band were followed by a later watched episode in the same series
- **555 / 775** episodes in that band had `<= 120s` remaining
- only **20 / 775** episodes in that band had neither follow-on evidence nor the short remaining-time signature, so those should stay incomplete by default

### Working defaults
- strict completion ratio: `0.95`
- credits-skip remaining-time window: `120` seconds
- follow-on completion floor: `0.85`

## 2026-03-14 - Recommendation priorities

### Highest priority alerts
1. New season released for an anime the user has completed.
2. New dubbed episode released for an in-progress anime the user is currently following.

### Hard recommendation filter
- Do not recommend anime or new episodes that lack English dubs.

## 2026-03-14 / 2026-03-20 - Repo posture

### Decision
Treat `MAL-Updater` as a public repository and keep all tracked artifacts anonymized.

### Rules
- Do not commit real credentials, tokens, API keys, or account identifiers.
- Do not commit host-specific absolute paths, private workspace paths, or machine-local identifiers.
- Use obviously fake placeholders in examples and tests.
- Runtime-generated state may exist under external runtime directories, but it must not be tracked.
- If identifying residue is accidentally committed, history rewrite is an acceptable remediation.

### Why
Public-repo hygiene needs to be part of the project contract, not an afterthought. Future development should assume anything tracked in git may be published and mirrored.

### Related maintainer channel
Operational bugs, usability issues, and feature requests discovered by third-party users should be reported through the authoritative upstream issue tracker:
- <https://github.com/kklouzal/MAL-Updater/issues>

## 2026-03-14 - Project memory habit

### Decision
Use `references/` as project-specific durable memory.

### Why
OpenClaw memory is useful, but project-specific knowledge should live with the project repo.

## 2026-03-14 - Crunchyroll implementation choice

### Decision
Use the Python-side impersonated transport as the primary Crunchyroll auth and live fetch path.

### Why
- it is the path that produced real live account/history/watchlist data on this host
- it reuses the already-proven `curl_cffi` browser-TLS impersonation workaround when needed
- it gets real data into the local pipeline now instead of blocking on alternative transport ideas
- it keeps the repo architecture coherent and smaller

## 2026-03-14 - Crunchyroll incremental boundary posture

### Decision
Treat repeated Crunchyroll fetches as incremental by default.

### Rules
- Persist a local `sync_boundary.json` checkpoint under the resolved runtime state tree (`.MAL-Updater/state/crunchyroll/<profile>/` by default) only after a successful snapshot fetch completes.
- Store only lightweight leading-page markers for watch-history and watchlist, not a second shadow database.
- On the next fetch, stop paging once a previously seen marker appears in the current page; keep the current page, but do not keep walking older pages.
- If the stored boundary belongs to a different Crunchyroll account, ignore it.
- Provide an explicit operator escape hatch (`crunchyroll-fetch-snapshot --full-refresh`) for full pagination when needed.
- Prefer explainable overlap-based stopping over more aggressive heuristics about dates/count deltas/order stability.

### Why
- the recurring durability problem is still fresh full-cycle Crunchyroll paging, especially `watch-history` returning `401` before the run finishes
- an overlap checkpoint directly reduces request count on repeated runs without pretending deleted/reordered remote history is solved
- keeping the boundary file small and local makes the behavior auditable and easy to reset

## 2026-03-20 - Daemon budget backoff posture

### Decision
When the daemon hits a provider budget critical threshold, persist a per-task cooldown window and stop re-checking that task every loop until enough request history ages out of the hourly window.

### Why
- repeated every-loop budget skips create noisy logs without making progress
- recovery should be based on the observed request window, not a hand-wavy fixed sleep
- surfacing `budget_backoff_until` in service state/status makes unattended behavior easier to reason about during debugging

## 2026-03-22 - Adaptive provider failure backoff posture

### Decision
When a daemon task tied to a provider fails, persist an adaptive failure-backoff window with the failure reason and consecutive-failure streak so the lane cools down before retrying instead of immediately thrashing.

### Why
- auth-fragile provider fetches can fail repeatedly for a while after a bad login/session state transition
- every-loop retries create noisy logs and extra pressure without improving recovery odds
- surfacing `failure_backoff_until`, `failure_backoff_reason`, and consecutive failures in service state/status makes unattended debugging clearer

## 2026-03-22 / 2026-03-23 / 2026-03-28 - Provider full-refresh escalation posture

### Decision
Keep provider fetch lanes incremental by default, but let the unattended daemon both:
- persist a provider-specific full-refresh anchor and force a conservative `--full-refresh` sweep whenever `service.full_refresh_every_seconds` elapses (default 24 hours), and
- honor the latest health-check `refresh_full_snapshot` recommendation for a provider so partial-coverage warnings can trigger the next unattended fetch to self-upgrade into a full refresh immediately.

Only advance `last_fetch_mode`, `last_successful_full_refresh_*`, and the full-refresh anchor after a **successful** provider fetch. A failed attempted full refresh must not be recorded as if the canonical resweep already happened.

### Why
- incremental fetches are the right steady-state posture for fragile providers, but unattended sync quality still needs occasional canonical resweeps
- a persisted cadence anchor gives the daemon an explainable, low-complexity way to recover from stale provider state without requiring operators to remember ad-hoc maintenance commands
- wiring health-check maintenance recommendations back into daemon execution closes a real gap between diagnosis and remediation for partial-coverage residue
- surfacing fetch mode plus last successful full refresh in task state keeps the behavior auditable instead of hiding refresh policy in operator folklore
- treating a failed full refresh as successful would silently suppress the very resweep the daemon was trying to obtain

## 2026-03-24 - Generic source-provider budget default posture

### Decision
Keep MAL as its own explicit daemon budget lane, but let non-MAL source providers inherit shared source-provider hourly/backoff/auth-failure defaults unless a per-provider override is configured.

### Why
- the provider architecture should not silently treat every new source provider as if it were Crunchyroll
- shared source-provider defaults make new providers safer to onboard before their final tuned per-provider numbers are known
- explicit per-provider overrides still win, so operators can keep Crunchyroll/HIDIVE-specific tuning where it matters

## 2026-04-10 - Bootstrap remediation metadata posture

### Decision
Keep `bootstrap-audit` onboarding output machine-readable enough for operators and automation to distinguish **why** a bootstrap command is being recommended, not just which command to run.

`bootstrap-audit` onboarding steps / `recommended_commands` should preserve a stable `reason_code`, `automation_safe`, and `requires_auth_interaction` posture for actionable commands. When the recommendation is driven by auth degradation, it should also carry the classified `auth_failure_kind` and `auth_remediation_kind`, and `bootstrap-audit --summary` should surface those auth-classification fields plus the top recommended command posture too.

### Why
- bootstrap/onboarding had already become the first operator surface, but its actionable command list still lagged behind `health-check` in machine-readable remediation specificity
- preserving the auth classification in both JSON and terse summary output makes partial/bootstrap-degraded state easier to debug without forcing operators to jump to a different command
- stable reason codes keep future automation/reporting aligned with the conservative shared reauth/rebootstrap posture instead of re-parsing freeform detail strings
- the same metadata should also follow MAL/provider `operation_guidance` next-command hints so lane-specific bootstrap consumers do not have to rediscover command posture from a separate top-level list

## 2026-04-09 - Discovery mixed-signal recommendation posture

### Decision
Keep discovery recommendations conservative when support is mixed: candidates backed by both positive and explicitly negative seed titles may still be shown, but negative supporting seeds should not contribute full multi-seed confidence.

When a discovery candidate has mixed support, discount dropped/disliked supporting seeds from the support-count boost and apply a small additional `mixed_signal_penalty` based on the negative-support share. Still suppress candidates whose support is entirely dropped or disliked-only.

### Why
- mixed-signal candidates can still be useful when one strong positive seed is present, so outright suppression would be too blunt
- counting negative supporting seeds as full consensus overstates confidence and can let noisy candidates outrank cleaner support
- exposing `negative_support_ratio`, `effective_supporting_seed_count`, and `mixed_signal_penalty` keeps the tradeoff inspectable for operators instead of burying it in opaque ranking folklore

## 2026-04-05 - MAL auth-failure remediation posture

### Decision
Extend the same shared auth-style failure detection used for provider daemon lanes into the `mal_refresh` lane's operator surface, so health-check treats repeated unattended MAL token-refresh auth failures as explicit MAL re-auth work rather than generic daemon residue.

When the daemon records repeated auth-style `mal_refresh` failures (for example `invalid_grant`, expired/revoked refresh material, or other refresh-token/auth residue), `health-check` should emit a dedicated warning plus a `mal-auth-login` maintenance recommendation.

That shared command surface should still preserve machine-readable residue specificity (`reason_code`, `auth_remediation_kind`) so automation can distinguish revoked-token, missing-refresh-material, and malformed-token-payload cases even when the conservative next command remains the same.

### Why
- unattended MAL token-refresh failures are operationally similar to provider auth degradation: the daemon should cool down, and the operator should get a clear re-auth instruction
- without an explicit MAL lane recommendation, the health surface can look noisier and less actionable than the provider-side guidance it already ships
- reusing the same auth-style detector keeps MAL/provider remediation posture aligned instead of creating a separate folklore path for one auth lane

## 2026-03-23 / 2026-03-27 - Auth-style provider failure cooldown posture

### Decision
Keep adaptive provider failure backoff for all daemon task errors, but classify auth-style provider failures separately and allow a stronger provider-specific cooldown floor for that class. Persist the failure class and effective floor in service state/status.

Auth-style detection should be shared between the daemon/runtime path and the health-check/operator path, and should conservatively treat these residue classes as auth-related:
- repeated `401` / `403` / unauthorized-style failures
- refresh/login failures (`invalid_grant`, token refresh/login failure text)
- missing refresh-token / missing refresh-material residue
- malformed token-payload residue (`did not return ...token...`, non-JSON login/token responses)
- provider session-state auth phases like `auth_failed`

### Why
- repeated auth/login/refresh failures are a different recovery shape than generic subprocess or network residue
- brittle provider auth should cool down longer before retrying so unattended loops stop re-poking broken sessions
- the same auth residue should drive both unattended cooldown behavior and later health-check rebootstrap guidance instead of letting those heuristics drift apart
- surfacing the failure class and floor keeps the daemon's retry posture explainable during debugging instead of hiding it in timing folklore

## 2026-03-22 - Same-title split-bundle suffix posture

### Decision
Allow exact-title split-bundle auto-resolution when the base candidate is an exact TV match and the bundle companion is a same-title TV suffix variant (for example a year-tagged entry like `Title (2009)`), **but only** when provider episode evidence fits the combined bundle length and there is no stronger non-bundle rival nearby.

### Why
- some real MAL franchises split one provider shell across multiple TV entries without advertising a plain `Season 2` / `Part 2` hint
- this residue is still explainable when the provider title is exact, the companion stays in the same normalized title family, and the episode count only makes sense as the combined bundle
- keeping the rule tied to same-title TV suffix companions preserves conservative behavior while removing a class of manual-review busywork

## 2026-04-12 - Alias-backed bundle review posture

### Decision
Allow exact-title overflow handling to treat MAL **alternative-title family alignment** as enough evidence to surface a later-season bundle companion for review, but keep exact bundle auto-resolution on the stricter primary-title-family rule.

### Why
- some franchises only line up cleanly through MAL English/synonym aliases even when the primary titles diverge more than conservative auto-resolution should trust
- surfacing the companion still reduces manual review detective work for bundled provider shells
- keeping auto-resolution tied to the stricter primary-title-family check preserves the existing conservative write/approval boundary

## 2026-04-13 - Contiguous numbered bundle-companion posture

### Decision
When exact-overflow bundle review finds multiple plausible numbered follow-up installments with the same episode-fit envelope, prefer the nearest contiguous follow-up sequence (for example season 2 -> season 3) over a skip-ahead later season.

Fall back to the existing smallest-total-episodes heuristic when the candidates do not expose reliable explicit installment numbers.

### Why
- numbered franchise bundles are easier to review when the surfaced companion sequence matches the expected next installment instead of an arbitrary later season
- preferring contiguous follow-ups reduces manual detective work without widening auto-approval scope
- keeping the fallback for unnumbered/suffix-only bundles preserves the conservative same-title bundle behavior already shipped

## 2026-04-13 - Standalone installment-only provider season-label posture

### Decision
Treat standalone installment-only provider season labels (for example bare `III` or `2`) as real season cues when they appear in the provider `season_title` without a separate reliable `season_number` field.

Prefix those standalone labels with the base series title during MAL search generation, and let the resulting inferred installment number participate in season-match scoring / conservative exact later-installment auto-approval the same way an explicit `Season N` cue already does.

### Why
- some provider catalogs expose a later season as only a bare installment token instead of a full `Title Season N` string or trustworthy numeric metadata
- dropping the base title in those cases leaves later-season lookup and disambiguation weaker than the repo's existing conservative installment posture warrants
- prefixing only installment-only labels keeps the behavior explainable and bounded instead of inventing broader alias expansion

## 2026-04-13 - Franchise-specific sequel-alias query posture

### Decision
Allow explicit later-season search generation to try a **small explicit franchise-specific sequel-alias table** when MAL files the sequel under a non-generic follow-up label instead of `Season N` / roman / part wording.

When one of those alias queries lands an exact title hit for the same provider season context, treat that exact alias hit as explainable season evidence (`season_alias_query_match`) rather than penalizing it like a base-title candidate missing installment metadata.

### Why
- some MAL franchises expose later TV installments through suffix labels like `Railgun S` rather than generic season numbering, so ordinary conservative season-query expansion can miss them even when the provider season context is explicit
- keeping the behavior table-driven and season-scoped is narrower and more explainable than teaching the scorer a broad new class of speculative suffix heuristics
- promoting only exact alias hits preserves the conservative mapping boundary while reducing manual review for a real class of franchise-specific sequel-label residue

## 2026-04-14 - Explicit later-season relation-recovery posture

### Decision
When the provider clearly targets a later installment, allow related-anime expansion to run even if the current top MAL hit still looks like a **base-series false positive**, so long as that top hit carries the conservative signs already exposed by scoring (`candidate_missing_installment_hint` plus `base_installment_penalty_for_explicit_later_season`).

If relation expansion then finds a sequel node whose alternative titles or other installment evidence cleanly match the provider's later-season context, let that related candidate compete normally and auto-approve only under the existing exact later-installment safeguards.

### Why
- some MAL sequels are not discoverable from ordinary season queries or the explicit alias table, but are easy to reach once the mapper lands on the base franchise entry and follows MAL relations
- the previous relation-expansion trigger could stay too quiet when the misleading base-series hit looked strong on title similarity alone despite obvious later-season context
- keeping the trigger tied to both explicit later-season context and the existing missing-installment/base-penalty reasons preserves conservative scope instead of turning every exact base-title hit into broad relation crawling

## 2026-04-14 - Inferred provider-season query generation posture

### Decision
When search-query generation needs an explicit later-season number, reuse the **effective provider season number** inferred from provider season-title text instead of relying only on raw `season_number` metadata.

This applies to both:
- generic later-season query variants (`Title Season N`, ordinal, roman, numeric), and
- the small franchise-specific sequel-alias table used for suffix-labeled MAL sequels such as `Railgun S`.

### Why
- the mapper already learned how to infer later-season intent from provider season titles even when sparse provider metadata omits `season_number`
- leaving query generation and sequel-alias lookup on the raw metadata field created an inconsistent gap where scoring could understand the later-season context but search generation could still miss the best sequel lookup path
- reusing the inferred provider season number keeps the behavior conservative and explainable while reducing later-season false negatives caused only by missing provider metadata

## 2026-04-14 - Inferred provider-season exact-classification posture

### Decision
When deciding whether an exact mapping is specific enough to classify as a clean later-season resolution, use the same **effective provider season number** inferred from provider season-title text instead of looking only at raw `season_number` metadata.

This keeps exact later-season classification aligned with the mapper's existing inferred-season query generation and scoring posture.

### Why
- sparse provider metadata can omit `season_number` even when the human-readable provider season title clearly says `Season 2`, `III`, or a similar later-installment cue
- leaving exact classification on raw metadata alone created an inconsistent gap where the mapper could find and score the right sequel but still hesitate at the final classification step
- reusing the same inferred provider season number preserves conservative behavior while avoiding review-only residue that existed only because one classification path lagged behind the rest of the later-season logic

## 2026-04-15 - Alias-labeled provider-season inference posture

### Decision
Let the small franchise-specific sequel-alias table contribute **season-number inference and installment hints** even when the provider season title itself is already the alias label (for example `Demon Lord, Retry! R`) rather than a generic `Season N` string.

Use that same alias-derived season context when scoring candidate season matches and exact later-installment classification, not only when generating explicit alias search queries from a numeric provider season.

### Why
- some providers expose a later season directly through the franchise-specific sequel label instead of a numeric season title, while also omitting reliable `season_number` metadata
- query-time alias expansion alone left an inconsistency where the mapper could find an exact alias-title hit but still lack the explicit later-season context needed to separate it cleanly from an exact base-series rival
- reusing the same conservative alias table for provider/candidate season hints keeps the behavior explainable and bounded instead of inventing broader speculative suffix heuristics

## 2026-04-16 - Whole-season vs split-part tie-break posture

### Decision
When a provider title clearly signals a **whole later season** (`Season 2`, `II`, etc.) but does **not** itself mention `Part`, `Cour`, or another segmented split label, treat MAL split-part/cour candidates for that same season as slightly weaker **when the provider's observed episode evidence already fits an unsegmented season entry**.

Keep split-part/cour candidates viable when the provider episode numbering still looks aggregated for a half-season shell; in that case the existing season-to-split alignment and aggregated-episode evidence should continue to win.

### Why
- some MAL franchises publish a later season both as a whole-season entry and as a narrower part/cour continuation, and season-number alignment alone can otherwise let the segmented entry tie with the broader season
- real provider titles like plain `Season 2` / `II` should not auto-resolve onto `Part 2` unless the provider data itself actually looks like a segmented/aggregated shell
- tying the penalty to episode-fit evidence preserves the earlier conservative split-cour recovery work for genuine aggregated provider shells while reducing false ties on ordinary later-season labels

## 2026-03-20 / 2026-04-12 - Supplemental mapping candidate posture

### Decision
Treat hard-coded supplemental MAL candidate IDs as conservative rescue inputs, not as enough evidence by themselves to auto-resolve exact-title overflow cases.

Supplemental candidates may still contribute **bundle-review evidence** when they reveal an exact base title plus a plausible follow-up TV installment whose combined episode counts explain the provider shell, but that evidence should remain review-only unless stronger non-supplemental mapping signals exist.

### Why
- supplemental IDs help recover titles that MAL search fails to surface at all
- overflow on top of a supplemental-only hit can still mean a multi-entry bundle or broader franchise residue
- surfacing the companion for review reduces manual detective work without pretending the rescue path is as trustworthy as ordinary search evidence
- keeping auto-resolution disabled on supplemental-only bundle rescue preserves explainability and avoids overconfident approval when the search surface was already weak

## 2026-03-27 / 2026-03-28 - Built-in projected-request seed posture

### Decision
Ship explicit built-in projected-request defaults for conservative unattended lanes, but treat those built-ins as **cold-start seeds** rather than permanent hard overrides once the daemon has real observed request history.

This applies to:
- `mal_refresh`
- `sync_apply`
- fetch-mode-specific provider defaults such as Crunchyroll/HIDIVE incremental and full-refresh fetches

If an operator supplies an explicit task-wide override, that override should still beat the shipped seed when no more specific explicit mode override exists.

### Why
- fresh installs should not treat routine daemon lanes like zero-cost mysteries before the first real run lands
- once a lane has observed request history, synthetic shipped defaults should stop pinning budget behavior to folklore instead of evidence
- keeping built-ins seed-like preserves the repo's current direction: conservative, explainable cold starts first, then learned host-specific behavior as soon as real data exists

## 2026-04-09 - Discovery low-confidence seed penalty posture

### Decision
Keep discovery-candidate ranking explainable and mostly vote-driven, but add a small **symmetric penalty** when the supporting seed titles themselves carry clear low-confidence taste signals from cached MAL scores.

### Rules
- Treat cached MAL seed scores of `5` as a mild penalty, `4` as a moderate penalty, and `<=3` as a stronger penalty.
- Apply the penalty as a modest tie-break / near-tie tempering signal, not as a hard filter and not as something strong enough to outweigh aggregate cross-seed consensus by itself.
- Record the penalty transparently in recommendation context (`seed_quality_penalty`, `penalized_seed_scores`, `lowest_supporting_seed_score`) and emit an operator-readable reason when it materially affects a candidate.
- Keep positive high-score / deep-engagement seed bonuses alongside this penalty so mixed-signal discovery candidates can still rank well when the broader support remains convincing.

### Why
- positive-only taste calibration can overstate recommendations that are backed mainly by titles the user actually rated poorly
- a small symmetric penalty better reflects the user's demonstrated taste without pretending the recommendation graph is precise enough for aggressive filtering
- surfacing the penalty explicitly keeps recommendation ordering explainable instead of burying negative taste signals inside opaque score math

## 2026-03-25 - Bursty learned-request projection posture

### Decision
When a daemon lane has no explicit projected-request percentile configured, keep the default smoothed-history projection for ordinary lanes but automatically switch to a conservative learned `p90` baseline once the recent observed request history is clearly bursty.

### Why
- unattended budget gating should react before a spiky lane surprises the provider limit
- requiring manual percentile tuning for every lane is unnecessary operator busywork
- auto-switching only after several observed runs keeps the default path simple for stable lanes while making burstier ones safer by default

## 2026-03-24 - Task-level daemon budget override posture

### Decision
Keep provider/shared budget defaults as the baseline, but allow optional per-task daemon budget overrides for hourly limit, warn/critical cooldown floors, and auth-failure cooldown floors. Persist the effective budget scope (`task` vs `provider`) in service state/status.

### Why
- some daemon lanes share a provider but do not deserve the same throttling posture (for example MAL token refresh vs aggregate apply)
- per-task overrides let operators tune the risky/expensive lane without inventing fake provider identities
- surfacing the effective budget scope keeps cooldown decisions explainable during unattended debugging

## 2026-03-24 - First-pass projected request budgeting posture

### Decision
Teach daemon budget gates to look at projected per-run request cost, not only the current hourly count. Use explicit `service.task_projected_request_counts` overrides when present; otherwise reuse the lane's last observed request delta (including fetch-mode-specific deltas for incremental vs full-refresh provider fetches). Persist the projection source/count in service state/status so unattended skips remain explainable.

### Why
- raw current-hour counts alone react too late for chunky lanes whose next run is what would actually push the provider over warn/critical budget
- last-observed request deltas are a cheap, local, provider-agnostic signal that improves pacing without inventing fake precision or requiring a large planning subsystem
- explicit per-task overrides still matter for operators who know a lane's expected cost better than one recent observed run
- persisting projection source/count keeps the gate auditable instead of feeling like invisible daemon magic

## 2026-03-24 - Smoothed observed request projection posture

### Decision
When a lane does not have an explicit configured projected request count, keep a short rolling observed request-delta history and derive projections from a smoothed average instead of blindly trusting only the most recent run. Prefer fetch-mode-specific history (`incremental` vs `full_refresh`) when available, then fall back to overall lane history, then finally to the legacy last-run delta.

### Why
- one unusually expensive unattended run should not immediately become the whole budget policy for the next run
- fetch-mode-specific smoothing preserves the useful distinction between incremental and full-refresh provider cost without needing a larger forecasting subsystem
- preserving the legacy last-run fallback keeps cold-start behavior simple while letting repeated runs become more stable
- persisting the projection source still keeps daemon pacing explainable during debugging

## 2026-03-25 - Tunable learned request projection posture

### Decision
Keep the learned observed-request projection path configurable per task: allow each lane to override its observed-history window and optionally use a percentile baseline instead of the default smoothed mean. Prefer the same fetch-mode-specific history split (`incremental` vs `full_refresh`) when present.

### Why
- different daemon lanes have different burst shapes, so one fixed five-sample mean is too blunt as the project grows
- conservative percentile baselines let fragile/high-cost fetch lanes pace themselves without forcing every quieter lane into worst-case budgeting
- keeping the tuning task-scoped preserves explainability and avoids inventing hidden provider-wide heuristics too early
- bounded per-task history windows keep the state small and auditable while still letting operators smooth or tighten learning where it matters

## 2026-03-25 / 2026-03-29 - Shipped provider pacing defaults posture

### Decision
Ship opinionated daemon pacing defaults for the currently supported source providers instead of leaving those numbers only in example config. Keep shared source-provider defaults as the fallback for unknown/new providers, but seed built-in provider tables for:
- Crunchyroll: deeper learned request-cost history, conservative percentile projection, and stronger warn/critical/auth-failure cooldown floors
- HIDIVE: quieter hourly budget, provider-specific backoff/auth-failure floors, and a conservative learned `p90` percentile baseline so observed history can take over from cold-start request-count seeds without falling back to smoothed mean behavior first

### Why
- the repo's own likely-next-step had already converged on provider defaults being justified for the current providers
- HIDIVE already had built-in hourly/backoff posture and mode-specific request-count seeds, but not a shipped learned-percentile stance once real history accumulated
- giving HIDIVE the same conservative percentile family as the rest of the fragile-provider budget lane makes unattended behavior more coherent and less dependent on operators discovering the setting manually
- keeping unknown providers on the generic source-provider fallback preserves extensibility while making today's unattended installs safer out of the box

## 2026-03-26 / 2026-03-29 - Shipped `sync_apply` task-default posture

### Decision
Ship built-in daemon task defaults for the aggregate MAL `sync_apply` lane instead of leaving that lane's conservative posture only in example config. Seed built-in task tables for:
- hourly limit: `48`
- projected request count: `8`
- learned request-history window: `3`
- learned projected-request percentile: `0.9`
- warn cooldown floor: `900s`
- critical cooldown floor: `1800s`
- auth-failure cooldown floor: `2400s`

### Why
- `sync_apply` is the first MAL lane that clearly behaves like a higher-cost/riskier aggregate task than the provider-level MAL defaults imply
- once the lane has real observed request history, a conservative built-in learned percentile keeps MAL write budgeting from drifting back to mean-smoothed optimism on fresh unattended installs
- fresh unattended installs should inherit a conservative aggregate-apply posture even before an operator copies optional example settings into their runtime config
- keeping the defaults task-scoped preserves the clean provider/task split while making the most meaningful shipped lane safer out of the box

## 2026-03-26 - Fetch-mode task projection defaults posture

### Decision
Keep the existing task-level projected-request override table, but add an optional fetch-mode-specific task table for lanes whose incremental and full-refresh cost differ materially. Ship the first built-in default for `sync_fetch_hidive` full refresh at `71` projected requests.

### Why
- some fetch lanes are cheap incrementally but materially more expensive when forced into `--full-refresh`, so a single task-wide configured projection is too blunt
- cold-start unattended budgeting should not treat a known heavy full-refresh lane as zero-cost just because local history has not been learned yet
- keeping the override task-scoped and fetch-mode-scoped preserves explainability without inventing a larger forecasting subsystem
- HIDIVE already has enough observed local shape to justify one concrete shipped full-refresh default while leaving other lanes on learned history until they have equally solid evidence

## 2026-03-26 - Crunchyroll full-refresh projection default posture

### Decision
Extend the built-in fetch-mode task projection defaults to ship a conservative `sync_fetch_crunchyroll` full-refresh projected-request cost of `55`.

### Why
- Crunchyroll full refreshes are materially heavier than the lane's ordinary incremental fetches, so cold-start budgeting should not have to pretend the first overdue resweep costs zero requests
- the daemon already has mode-specific budget behavior and downgrade-to-incremental logic, so shipping a concrete Crunchyroll full-refresh default lets those controls act earlier on fresh unattended installs instead of waiting for local history to be learned
- keeping the default task-scoped and mode-scoped preserves explainability and leaves ordinary incremental Crunchyroll pacing on the existing task/provider learned-history path
- this mirrors the earlier HIDIVE full-refresh default and closes the most obvious remaining gap in the repo's own shipped provider-fetch posture

## 2026-03-27 - Incremental fetch projection default posture

### Decision
Extend the built-in fetch-mode task projection defaults so the ordinary unattended provider fetch path also starts with a conservative non-zero request-cost estimate. Ship built-in incremental projected-request defaults of `4` for both:
- `sync_fetch_crunchyroll`
- `sync_fetch_hidive`

### Why
- the daemon already ships built-in full-refresh fetch costs, but fresh installs were still treating the ordinary incremental fetch path as effectively zero-cost until enough local history accumulated
- live runtime on this host has now repeatedly settled both Crunchyroll and HIDIVE incremental fetch lanes around `4` requests, which is stable enough to justify a small shipped seed value
- a low non-zero incremental default makes cold-start budget gating more honest without forcing operators to pre-tune config or waiting for the first few unattended runs to teach the daemon something it already broadly knows
- keeping the defaults task-scoped and mode-scoped preserves explainability and leaves room for learned history or host-specific overrides to take over once local evidence exists

## 2026-03-31 - Bounded unattended `sync_apply` posture

### Decision
Keep unattended `sync_apply` runs bounded by an explicit execution-limit surface (`service.task_execute_limits`) instead of assuming a full aggregate apply pass.

Also treat a material execution-posture change (for example historical full-pass apply vs bounded unattended batches) as a reason to clear stale projected-request/backoff state for that lane so old pathological history does not keep poisoning future daemon decisions.

### Why
- an unattended exact-approved apply lane should make steady forward progress without behaving like an all-or-nothing catch-up pass every cycle
- a single oversized historical apply run should not be able to trap the daemon in near-permanent projected-budget skips once the intended unattended posture becomes smaller and more conservative
- keeping the bounded batch size explicit and configurable preserves explainability while still allowing hosts with stronger evidence to tune the lane differently later

## 2026-04-10 - Discovery neutral-seed weighting posture

### Decision
When discovery candidates are supported by seed titles with an explicit neutral MAL score (`6`), count that support more conservatively than clearly positive seed support.

Specifically:
- expose neutral support metadata as `neutral_supporting_seed_ids`, `neutral_support_ratio`, and `neutral_support_penalty`
- discount neutral supporting seeds from the candidate's effective multi-seed support-count boost
- apply only a small additional penalty for neutral-heavy support, keeping the posture explainable and conservative rather than aggressively suppressive

### Why
- an explicit neutral score is meaningfully different from both positive taste evidence and missing score data
- treating neutral seeds like full positive consensus overstates confidence for otherwise tied discovery candidates
- surfacing the neutral-support metadata keeps the ranking auditable instead of hiding the decay inside opaque scoring math

## 2026-04-16 - Discovery catalog-age decay posture

### Decision
Keep discovery ranking mildly freshness-aware in both directions: newer MAL catalog entries may still receive a small freshness bonus, but sufficiently old catalog entries should also take a **modest explainable age decay** instead of merely losing the bonus race.

Specifically:
- preserve the existing freshness buckets/bonuses for current and still-modern catalog entries
- add `freshness_penalty` plus `catalog_age_in_seasons` metadata for aging/older/legacy catalog buckets
- keep the decay small enough to act as a tie-break / near-tie tempering signal rather than a hard filter against older titles

### Why
- a one-way freshness bonus still let very old catalog entries look stronger than intended whenever votes and metadata otherwise tied
- recommendation ranking should be able to say *why* an older title lost ground instead of hiding that under raw priority totals
- exposing both the bucket and the age/penalty keeps discovery ordering inspectable while preserving the conservative, non-aggressive recommendation posture

## 2026-04-17 - Discovery stale-support decay posture

### Decision
When discovery candidates are supported mainly by materially old seed-watch activity, count that support a bit more conservatively instead of only withholding the existing recent-activity bonus.

Specifically:
- add `stale_supporting_seed_ids`, `stale_support_ratio`, and `stale_support_penalty` metadata for discovery candidates whose supporting seeds are older than the recent-activity window
- modestly downweight stale seeds in the underlying recommendation-vote contribution before final ranking
- keep the stale-support penalty small and capped so it behaves as explainable near-tie / consensus tempering rather than a hard suppression rule

### Why
- a previously loved seed title can still matter, but very old watch activity should not look as current as fresh taste evidence when everything else ties
- the earlier `recent_seed_activity_bonus` only rewarded freshness; it did not actually temper stale support enough when older seeds still had strong votes or scores
- exposing the stale-support metadata keeps recommendation ordering auditable instead of hiding the decay in opaque raw-score math

## 2026-04-17 - Discovery stale-heavy consensus-count posture

### Decision
When stale support dominates a discovery candidate's backing seeds, reduce the **multi-seed consensus-count boost itself**, not only the late stale-support penalty.

Specifically:
- preserve the existing `stale_support_penalty` and staleness-aware raw-score decay
- add `base_effective_supporting_seed_count` plus a bounded `stale_consensus_discount` so stale-heavy support can lower `effective_supporting_seed_count`
- keep the discount conservative and capped so stale-heavy support still counts, just not as strongly as equally broad fresh consensus

### Why
- stale-heavy candidates were still receiving the same multi-seed confidence boost as fresh multi-seed support, which overstated how current that consensus really was
- reducing the count boost is more honest than trying to solve the whole problem with one larger late-stage penalty
- surfacing both the base count and the discount keeps the ranking inspectable instead of hiding this calibration inside one opaque priority number

## 2026-04-17 - Discovery effective support-spread posture

### Decision
Keep the discovery support-balance bonus, but compute it from **effective cross-seed spread** rather than raw vote spread alone.

Specifically:
- preserve raw `best_single_source_votes` / `cross_seed_support_votes` for transparency
- add `effective_best_single_source_votes` and `effective_cross_seed_support_votes` after removing neutral/disliked-only support from the spread bonus and applying stale-seed weighting
- derive `support_balance_bonus` from that effective spread, and emit an operator-readable reason when the effective spread is lower than the raw spread

### Why
- neutral-heavy or stale-heavy candidates could still look too consensus-backed if raw cross-seed vote spread kept granting the same support-balance bonus as cleaner fresh support
- the recommendation surface already had conservative support-count logic; letting the spread bonus ignore that posture left a real inconsistency
- exposing both raw and effective spread keeps the ranking auditable instead of replacing one opaque bonus with another

## 2026-04-08 - Discovery seed-quality posture

### Decision
When discovery-candidate support is otherwise close, let stronger supporting seed titles matter a bit more without replacing the existing aggregate-vote / cross-seed-consensus posture.

Use small explainable bonuses from:
- cached MAL seed scores when present (`my_list_status.score`)
- deeper provider-side completion evidence even when MAL scores are absent

Expose the resulting discovery metadata as:
- `seed_quality_bonus`
- `supporting_seed_scores`
- `best_supporting_seed_score`

### Why
- not every watched/mapped seed is equally representative of the operator's taste
- a conservative recommendation model should distinguish between a candidate backed by a highly rated or deeply completed seed and one backed only by weak seed evidence when the rest of the ranking is nearly tied
- keeping the bonus small and explicit preserves explainability and avoids turning one enthusiastic seed into an opaque hard override

## 2026-04-09 - Discovery negative-signal suppression posture

### Decision
Keep the existing small discovery penalty for mixed-signal candidates, but fully suppress discovery candidates when *all* supporting seed titles are already explicit negative taste signals.

Treat the following as suppression-worthy when they are the candidate's only support:
- supporting seeds whose cached MAL status is `dropped`
- supporting seeds whose cached MAL score falls into the strongly disliked bucket already used by `seed_quality_penalty`

Do **not** suppress mixed-signal candidates when there is still genuinely stronger positive seed-quality evidence from other supporting seeds; keep those explainable via the existing bonus/penalty metadata instead.

### Why
- a conservative recommendation surface should not keep suggesting discovery titles that are only connected to anime the operator explicitly dropped or strongly disliked
- mixed-signal taste evidence is still useful, but all-negative support is different from merely weak support and should be filtered rather than cosmetically demoted
- retaining the smaller penalty for mixed cases preserves explainability without letting one negative seed erase a broader positive consensus

## 2026-04-07 - Discovery support-balance posture

### Decision
When discovery-candidate MAL recommendation edges have similar aggregate support, prefer candidates whose support is spread across multiple watched/mapped seed titles over candidates whose votes are concentrated in one bursty seed.

Expose that evidence in recommendation context as:
- `best_single_source_votes`
- `cross_seed_support_votes`
- `support_balance_bonus`

Use the same posture when choosing optional discovery-target metadata refreshes so limited refresh budget follows steadier consensus rather than the loudest single seed.

### Why
- aggregate vote count alone can overstate one noisy recommendation edge
- cross-seed consensus is a better conservative signal for discovery suggestions than a single strong burst when totals are otherwise close
- surfacing the balance metadata keeps the recommendation order explainable instead of hiding this as an opaque tie-break

## 2026-03-31 - Bootstrap partial-install operation-mode posture

### Decision
Teach `bootstrap-audit` to distinguish between:
- untouched bootstrap state where no provider intent is staged yet, and
- partial installs where MAL auth and/or one or more providers are already partly staged but unattended daemon sync is not fully ready.

Expose provider-intent and partial-bootstrap counts in the audit payload/summary and use a dedicated `bootstrap-provider-staged` operation mode for the latter case.

### Why
- partial installs are operationally different from fresh untouched ones: the operator has already started enabling real runtime surfaces, so the audit should say that plainly instead of collapsing everything into one generic bootstrap mode
- automation-friendly consumers need a small machine-readable signal to tell “nothing chosen yet” from “finish the staged provider bootstrap before expecting unattended sync”
- this keeps the daemon-first posture conservative without pretending that every runtime-initialized install is already ready for unattended background operation

## 2026-03-31 - Provider-specific bootstrap guidance for staged installs

### Decision
Extend `bootstrap-audit` with per-provider operation guidance so each provider reports a small machine-readable operation mode plus an optional next-command hint.

Current provider guidance distinguishes between at least:
- `not-configured`
- `credentials-staged-awaiting-bootstrap`
- `session-staged-missing-credentials`
- `ready-for-unattended`

### Why
- once a provider is partially staged, operators and automation need more than a repo-wide `bootstrap-provider-staged` label to know what is actually missing
- provider-specific next-command hints let the audit stay actionable without guessing at wider live-write behavior
- this keeps bootstrap/onboarding conservative: the surface can tell the truth about staged state without pretending every provider needs the same remediation path

## 2026-03-26 - Budget-blocked full-refresh downgrade posture

### Decision
When an unattended provider fetch is due for `full_refresh` because of cadence or health recommendations but that heavier run is budget-blocked, immediately retry the budget check for `incremental` mode and run the cheaper fetch when it fits. Keep the full-refresh anchor/reason overdue so the heavier resweep still happens later once budget allows.

### Why
- a heavy overdue full refresh should not starve the entire fetch lane when a cheaper incremental pass would still keep fresh progress/watchlist data moving
- fetch-mode-specific projected-cost defaults are only truly useful if the daemon can act differently on them instead of turning every overdue full refresh into repeated no-op skips
- preserving the overdue full-refresh anchor/reason keeps the behavior conservative and explainable instead of silently forgetting that a broader resweep is still needed
