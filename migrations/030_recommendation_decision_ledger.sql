CREATE TABLE IF NOT EXISTS recommendation_decision_ledger_runs (
    run_id TEXT PRIMARY KEY,
    cutoff_at TEXT NOT NULL
        CHECK (
            datetime(cutoff_at) IS NOT NULL
            AND (length(cutoff_at) = 20 OR (
                length(cutoff_at) BETWEEN 22 AND 27
                AND substr(cutoff_at, 20, 1) = '.'
                AND substr(cutoff_at, 21, length(cutoff_at) - 21) NOT GLOB '*[^0-9]*'
            ))
            AND substr(cutoff_at, -1) = 'Z'
            AND strftime('%Y-%m-%dT%H:%M:%SZ', cutoff_at) = substr(cutoff_at, 1, 19) || 'Z'
        ),
    surface TEXT NOT NULL,
    objective TEXT NOT NULL,
    policy_id TEXT NOT NULL,
    policy_version TEXT NOT NULL,
    policy_artifact_sha256 TEXT NOT NULL
        CHECK (length(policy_artifact_sha256) = 64 AND policy_artifact_sha256 NOT GLOB '*[^0-9a-f]*'),
    maximum_evidence_at TEXT
        CHECK (
            maximum_evidence_at IS NULL OR (
                datetime(maximum_evidence_at) IS NOT NULL
                AND (length(maximum_evidence_at) = 20 OR (
                    length(maximum_evidence_at) BETWEEN 22 AND 27
                    AND substr(maximum_evidence_at, 20, 1) = '.'
                    AND substr(maximum_evidence_at, 21, length(maximum_evidence_at) - 21) NOT GLOB '*[^0-9]*'
                ))
                AND substr(maximum_evidence_at, -1) = 'Z'
                AND strftime('%Y-%m-%dT%H:%M:%SZ', maximum_evidence_at) = substr(maximum_evidence_at, 1, 19) || 'Z'
            )
        ),
    output_limit INTEGER
        CHECK (output_limit IS NULL OR output_limit >= 0),
    candidate_count INTEGER NOT NULL
        CHECK (candidate_count >= 0),
    selected_count INTEGER NOT NULL
        CHECK (selected_count >= 0 AND selected_count <= candidate_count),
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CHECK (maximum_evidence_at IS NULL OR datetime(maximum_evidence_at) <= datetime(cutoff_at))
);

CREATE TABLE IF NOT EXISTS recommendation_decision_ledger_items (
    run_id TEXT NOT NULL,
    item_identity TEXT NOT NULL,
    candidate_ordinal INTEGER NOT NULL CHECK (candidate_ordinal >= 1),
    exposure_rank INTEGER CHECK (exposure_rank IS NULL OR exposure_rank >= 1),
    selected INTEGER NOT NULL CHECK (selected IN (0, 1)),
    eligibility_state TEXT NOT NULL,
    exposure_state TEXT NOT NULL,
    kind TEXT NOT NULL,
    provider TEXT,
    provider_series_id TEXT,
    mal_anime_id INTEGER,
    title TEXT NOT NULL,
    priority INTEGER,
    score REAL,
    scorecard_json TEXT,
    reasons_json TEXT NOT NULL,
    context_json TEXT,
    feature_evidence_payload_hash TEXT NOT NULL
        CHECK (length(feature_evidence_payload_hash) = 64 AND feature_evidence_payload_hash NOT GLOB '*[^0-9a-f]*'),
    maximum_evidence_at TEXT
        CHECK (
            maximum_evidence_at IS NULL OR (
                datetime(maximum_evidence_at) IS NOT NULL
                AND (length(maximum_evidence_at) = 20 OR (
                    length(maximum_evidence_at) BETWEEN 22 AND 27
                    AND substr(maximum_evidence_at, 20, 1) = '.'
                    AND substr(maximum_evidence_at, 21, length(maximum_evidence_at) - 21) NOT GLOB '*[^0-9]*'
                ))
                AND substr(maximum_evidence_at, -1) = 'Z'
                AND strftime('%Y-%m-%dT%H:%M:%SZ', maximum_evidence_at) = substr(maximum_evidence_at, 1, 19) || 'Z'
            )
        ),
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (run_id, item_identity),
    UNIQUE (run_id, candidate_ordinal),
    UNIQUE (run_id, exposure_rank),
    CHECK ((selected = 1 AND exposure_rank IS NOT NULL) OR (selected = 0 AND exposure_rank IS NULL)),
    FOREIGN KEY (run_id) REFERENCES recommendation_decision_ledger_runs(run_id) ON DELETE RESTRICT
);

CREATE INDEX IF NOT EXISTS idx_recommendation_decision_ledger_runs_cutoff
    ON recommendation_decision_ledger_runs(cutoff_at DESC, run_id);
CREATE INDEX IF NOT EXISTS idx_recommendation_decision_ledger_runs_policy
    ON recommendation_decision_ledger_runs(surface, objective, policy_id, policy_version, cutoff_at DESC);
CREATE INDEX IF NOT EXISTS idx_recommendation_decision_ledger_items_exposure_rank
    ON recommendation_decision_ledger_items(run_id, exposure_rank);

CREATE TRIGGER IF NOT EXISTS recommendation_decision_ledger_items_cutoff_guard
BEFORE INSERT ON recommendation_decision_ledger_items
WHEN NEW.maximum_evidence_at IS NOT NULL
 AND (
     datetime(NEW.maximum_evidence_at) IS NULL
     OR datetime(NEW.maximum_evidence_at) > datetime((
         SELECT cutoff_at FROM recommendation_decision_ledger_runs WHERE run_id = NEW.run_id
     ))
 )
BEGIN
    SELECT RAISE(ABORT, 'recommendation decision evidence is newer than cutoff');
END;

CREATE TRIGGER IF NOT EXISTS recommendation_decision_ledger_runs_no_update
BEFORE UPDATE ON recommendation_decision_ledger_runs
BEGIN
    SELECT RAISE(ABORT, 'recommendation decision ledger is immutable');
END;

CREATE TRIGGER IF NOT EXISTS recommendation_decision_ledger_runs_no_delete
BEFORE DELETE ON recommendation_decision_ledger_runs
BEGIN
    SELECT RAISE(ABORT, 'recommendation decision ledger is immutable');
END;

CREATE TRIGGER IF NOT EXISTS recommendation_decision_ledger_items_no_update
BEFORE UPDATE ON recommendation_decision_ledger_items
BEGIN
    SELECT RAISE(ABORT, 'recommendation decision ledger is immutable');
END;

CREATE TRIGGER IF NOT EXISTS recommendation_decision_ledger_items_no_delete
BEFORE DELETE ON recommendation_decision_ledger_items
BEGIN
    SELECT RAISE(ABORT, 'recommendation decision ledger is immutable');
END;
