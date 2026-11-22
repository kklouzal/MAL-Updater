-- Recommendations no longer prefer a dub language: provider eligibility needs
-- a verified identity and current catalog presence, and audio_locales_json is
-- kept as information for the dashboard's audio-language filter.

DROP INDEX IF EXISTS idx_recommendation_provider_eligibility_actionable;

CREATE INDEX IF NOT EXISTS idx_recommendation_provider_eligibility_actionable
    ON recommendation_provider_eligibility_evidence(
        mal_anime_id,
        provider,
        expires_at
    )
    WHERE review_status = 'verified'
      AND catalog_status = 'present';

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

ALTER TABLE recommendation_provider_eligibility_evidence DROP COLUMN english_dub_status;

ALTER TABLE recommendation_provider_eligibility_evidence DROP COLUMN explicit_dub_evidence_source;

ALTER TABLE recommendation_score_snapshots DROP COLUMN dub_signal;

UPDATE recommendation_score_snapshots
SET kind = 'new_episode'
WHERE kind = 'new_dubbed_episode';
