-- Recommendations no longer prefer a dub language: provider eligibility needs
-- a verified identity and current catalog presence, and audio_locales_json is
-- kept as information for the dashboard's audio-language filter.

-- Evidence that failed only the old English-audio rule was a successful
-- verification at fetched_at under the new rule (strict identity, verified
-- review, catalog present, not explicitly invalidated by a no-match).
-- refresh_due_at is rescheduled from the new verification time in Python.
UPDATE recommendation_provider_eligibility_evidence
SET verification_outcome = 'positive',
    last_verified_at = fetched_at,
    last_successful_positive_at = fetched_at,
    invalidated_at = NULL,
    invalidation_reason = NULL,
    refresh_due_at = NULL
WHERE review_status = 'verified'
  AND catalog_status = 'present'
  AND english_dub_status <> 'present'
  AND verification_outcome <> 'positive'
  AND invalidation_reason IS NULL
  AND identity_match_kind IN (
      'approved_mapping',
      'manual_verified',
      'user_exact',
      'auto_exact',
      'provider_title_search_exact',
      'provider_franchise_shell_child_match'
  );

-- The dub columns are removed by rebuilding the tables (SQLite's documented
-- generalized ALTER TABLE procedure) rather than ALTER TABLE ... DROP COLUMN,
-- which needs SQLite 3.35; Python's sqlite3 may link an older library.
CREATE TABLE recommendation_provider_eligibility_evidence_new (
    mal_anime_id INTEGER NOT NULL,
    provider TEXT NOT NULL,
    provider_series_id TEXT NOT NULL,
    provider_title TEXT,
    provider_url TEXT,
    identity_match_kind TEXT NOT NULL DEFAULT 'unknown',
    match_confidence REAL CHECK (match_confidence IS NULL OR (match_confidence >= 0.0 AND match_confidence <= 1.0)),
    review_status TEXT NOT NULL DEFAULT 'unknown'
        CHECK (review_status IN ('unknown', 'present', 'absent', 'stale', 'review-needed', 'verified')),
    catalog_status TEXT NOT NULL DEFAULT 'unknown'
        CHECK (catalog_status IN ('unknown', 'present', 'absent', 'stale', 'review-needed')),
    audio_locales_json TEXT NOT NULL DEFAULT '[]',
    source_evidence_json TEXT NOT NULL DEFAULT '{}',
    fetched_at TEXT NOT NULL,
    expires_at TEXT NOT NULL,
    last_verified_at TEXT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    refresh_status TEXT NOT NULL DEFAULT 'ok',
    failure_count INTEGER NOT NULL DEFAULT 0,
    next_retry_at TEXT,
    logic_version TEXT NOT NULL DEFAULT 'legacy-v1',
    verification_outcome TEXT NOT NULL DEFAULT 'unknown'
        CHECK (verification_outcome IN ('unknown', 'positive', 'negative')),
    refresh_due_at TEXT,
    refresh_schedule_version TEXT NOT NULL DEFAULT 'provider-eligibility-120d-v1',
    refresh_schedule_key TEXT,
    last_successful_positive_at TEXT,
    invalidated_at TEXT,
    invalidation_reason TEXT,
    PRIMARY KEY (mal_anime_id, provider, provider_series_id)
);

INSERT INTO recommendation_provider_eligibility_evidence_new (
    mal_anime_id, provider, provider_series_id, provider_title, provider_url,
    identity_match_kind, match_confidence, review_status, catalog_status,
    audio_locales_json, source_evidence_json, fetched_at, expires_at, last_verified_at,
    created_at, updated_at, refresh_status, failure_count, next_retry_at, logic_version,
    verification_outcome, refresh_due_at, refresh_schedule_version, refresh_schedule_key,
    last_successful_positive_at, invalidated_at, invalidation_reason
)
SELECT
    mal_anime_id, provider, provider_series_id, provider_title, provider_url,
    identity_match_kind, match_confidence, review_status, catalog_status,
    audio_locales_json, source_evidence_json, fetched_at, expires_at, last_verified_at,
    created_at, updated_at, refresh_status, failure_count, next_retry_at, logic_version,
    verification_outcome, refresh_due_at, refresh_schedule_version, refresh_schedule_key,
    last_successful_positive_at, invalidated_at, invalidation_reason
FROM recommendation_provider_eligibility_evidence;

DROP TABLE recommendation_provider_eligibility_evidence;

ALTER TABLE recommendation_provider_eligibility_evidence_new RENAME TO recommendation_provider_eligibility_evidence;

CREATE INDEX idx_recommendation_provider_eligibility_actionable
    ON recommendation_provider_eligibility_evidence(mal_anime_id, provider, expires_at)
    WHERE review_status = 'verified'
      AND catalog_status = 'present';

CREATE INDEX idx_recommendation_provider_eligibility_expiry
    ON recommendation_provider_eligibility_evidence(expires_at, review_status);

CREATE INDEX idx_recommendation_provider_eligibility_provider_lookup
    ON recommendation_provider_eligibility_evidence(provider, provider_series_id, mal_anime_id);

CREATE INDEX idx_recommendation_eligibility_retry
    ON recommendation_provider_eligibility_evidence(refresh_status, next_retry_at, expires_at);

CREATE INDEX idx_recommendation_eligibility_refresh_due
    ON recommendation_provider_eligibility_evidence(provider, refresh_due_at, mal_anime_id, provider_series_id)
    WHERE refresh_due_at IS NOT NULL;

CREATE INDEX idx_recommendation_eligibility_last_known_positive
    ON recommendation_provider_eligibility_evidence(mal_anime_id, provider, provider_series_id)
    WHERE last_successful_positive_at IS NOT NULL AND invalidated_at IS NULL;

CREATE TABLE recommendation_score_snapshots_new (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT NOT NULL,
    generated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    kind TEXT NOT NULL,
    provider TEXT,
    title TEXT NOT NULL,
    provider_series_id TEXT,
    mal_anime_id INTEGER,
    score REAL,
    priority INTEGER,
    reasons_json TEXT,
    scorecard_json TEXT,
    context_json TEXT,
    availability_providers_json TEXT,
    availability_confidence REAL,
    availability_confidence_label TEXT
);

INSERT INTO recommendation_score_snapshots_new (
    id, run_id, generated_at, kind, provider, title, provider_series_id, mal_anime_id,
    score, priority, reasons_json, scorecard_json, context_json, availability_providers_json,
    availability_confidence, availability_confidence_label
)
SELECT
    id, run_id, generated_at,
    CASE WHEN kind = 'new_dubbed_episode' THEN 'new_episode' ELSE kind END,
    provider, title, provider_series_id, mal_anime_id,
    score, priority, reasons_json, scorecard_json, context_json, availability_providers_json,
    availability_confidence, availability_confidence_label
FROM recommendation_score_snapshots;

-- Keep the AUTOINCREMENT high-water mark: retention may have deleted the
-- highest ids, and a snapshot id must never be reused.
INSERT INTO sqlite_sequence(name, seq)
SELECT 'recommendation_score_snapshots_new', seq
FROM sqlite_sequence
WHERE name = 'recommendation_score_snapshots'
  AND NOT EXISTS (SELECT 1 FROM sqlite_sequence WHERE name = 'recommendation_score_snapshots_new');

UPDATE sqlite_sequence
SET seq = MAX(seq, COALESCE((SELECT seq FROM sqlite_sequence WHERE name = 'recommendation_score_snapshots'), 0))
WHERE name = 'recommendation_score_snapshots_new';

DROP TABLE recommendation_score_snapshots;

ALTER TABLE recommendation_score_snapshots_new RENAME TO recommendation_score_snapshots;

CREATE INDEX idx_recommendation_score_snapshots_run
    ON recommendation_score_snapshots(run_id, id);

CREATE INDEX idx_recommendation_score_snapshots_latest
    ON recommendation_score_snapshots(generated_at DESC, id DESC);

CREATE INDEX idx_recommendation_score_snapshots_identity
    ON recommendation_score_snapshots(kind, provider, provider_series_id, mal_anime_id);
