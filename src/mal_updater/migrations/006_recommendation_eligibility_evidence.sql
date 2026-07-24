ALTER TABLE recommendation_score_snapshots
    ADD COLUMN availability_confidence_label TEXT;

CREATE TABLE IF NOT EXISTS recommendation_provider_eligibility_evidence (
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
    english_dub_status TEXT NOT NULL DEFAULT 'unknown'
        CHECK (english_dub_status IN ('unknown', 'present', 'absent', 'stale', 'review-needed')),
    explicit_dub_evidence_source TEXT,
    audio_locales_json TEXT NOT NULL DEFAULT '[]',
    source_evidence_json TEXT NOT NULL DEFAULT '{}',
    fetched_at TEXT NOT NULL,
    expires_at TEXT NOT NULL,
    last_verified_at TEXT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (mal_anime_id, provider, provider_series_id)
);

CREATE INDEX IF NOT EXISTS idx_recommendation_provider_eligibility_actionable
    ON recommendation_provider_eligibility_evidence(
        mal_anime_id,
        provider,
        expires_at
    )
    WHERE review_status = 'verified'
      AND catalog_status = 'present'
      AND english_dub_status = 'present';

CREATE INDEX IF NOT EXISTS idx_recommendation_provider_eligibility_expiry
    ON recommendation_provider_eligibility_evidence(expires_at, review_status);

CREATE INDEX IF NOT EXISTS idx_recommendation_provider_eligibility_provider_lookup
    ON recommendation_provider_eligibility_evidence(provider, provider_series_id, mal_anime_id);
