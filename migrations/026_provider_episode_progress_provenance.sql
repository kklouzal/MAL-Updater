-- Additive progress provenance. Existing rows remain byte-for-byte unchanged in
-- their original columns and intentionally receive NULL (legacy/unproven) provenance.
ALTER TABLE provider_episode_progress ADD COLUMN progress_source_surface TEXT;
ALTER TABLE provider_episode_progress ADD COLUMN progress_observation_kind TEXT
    CHECK (progress_observation_kind IS NULL OR progress_observation_kind IN (
        'position', 'ratio', 'history_membership', 'explicit_completed', 'inferred_later_episode'
    ));
ALTER TABLE provider_episode_progress ADD COLUMN completion_assertion TEXT
    CHECK (completion_assertion IS NULL OR completion_assertion IN ('confirmed', 'inferred', 'unknown'));
ALTER TABLE provider_episode_progress ADD COLUMN normalization_logic_version TEXT;

CREATE INDEX IF NOT EXISTS idx_progress_provider_provenance
    ON provider_episode_progress(provider, progress_observation_kind, completion_assertion);
