ALTER TABLE recommendation_provider_eligibility_evidence
    ADD COLUMN verification_outcome TEXT NOT NULL DEFAULT 'unknown'
        CHECK (verification_outcome IN ('unknown', 'positive', 'negative'));
ALTER TABLE recommendation_provider_eligibility_evidence ADD COLUMN refresh_due_at TEXT;
ALTER TABLE recommendation_provider_eligibility_evidence
    ADD COLUMN refresh_schedule_version TEXT NOT NULL DEFAULT 'provider-eligibility-120d-v1';
ALTER TABLE recommendation_provider_eligibility_evidence ADD COLUMN refresh_schedule_key TEXT;
ALTER TABLE recommendation_provider_eligibility_evidence ADD COLUMN last_successful_positive_at TEXT;
ALTER TABLE recommendation_provider_eligibility_evidence ADD COLUMN invalidated_at TEXT;
ALTER TABLE recommendation_provider_eligibility_evidence ADD COLUMN invalidation_reason TEXT;

UPDATE recommendation_provider_eligibility_evidence
SET verification_outcome = CASE
        WHEN review_status = 'verified'
         AND catalog_status = 'present'
         AND english_dub_status = 'present'
         AND last_verified_at IS NOT NULL THEN 'positive'
        WHEN review_status = 'verified'
         AND (catalog_status = 'absent' OR english_dub_status = 'absent') THEN 'negative'
        ELSE 'unknown'
    END,
    last_successful_positive_at = CASE
        WHEN review_status = 'verified'
         AND catalog_status = 'present'
         AND english_dub_status = 'present'
         AND last_verified_at IS NOT NULL THEN last_verified_at
        ELSE NULL
    END;

CREATE INDEX IF NOT EXISTS idx_recommendation_eligibility_refresh_due
    ON recommendation_provider_eligibility_evidence(provider, refresh_due_at, mal_anime_id, provider_series_id)
    WHERE refresh_due_at IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_recommendation_eligibility_last_known_positive
    ON recommendation_provider_eligibility_evidence(mal_anime_id, provider, provider_series_id)
    WHERE last_successful_positive_at IS NOT NULL AND invalidated_at IS NULL;
