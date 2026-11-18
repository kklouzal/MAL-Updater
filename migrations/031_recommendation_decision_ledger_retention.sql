-- The decision ledger stays immutable while it is fresh, but it is a bounded
-- audit log rather than a permanent record: the service prunes whole runs once
-- they age past the configured retention. Deletes of rows younger than 7 days
-- are still rejected so a misconfigured or accidental delete cannot erase
-- recent decisions.
DROP TRIGGER IF EXISTS recommendation_decision_ledger_runs_no_delete;
DROP TRIGGER IF EXISTS recommendation_decision_ledger_items_no_delete;

CREATE TRIGGER IF NOT EXISTS recommendation_decision_ledger_runs_no_recent_delete
BEFORE DELETE ON recommendation_decision_ledger_runs
WHEN datetime(OLD.created_at) > datetime('now', '-7 days')
BEGIN
    SELECT RAISE(ABORT, 'recommendation decision ledger rows younger than 7 days are immutable');
END;

CREATE TRIGGER IF NOT EXISTS recommendation_decision_ledger_items_no_recent_delete
BEFORE DELETE ON recommendation_decision_ledger_items
WHEN datetime(OLD.created_at) > datetime('now', '-7 days')
BEGIN
    SELECT RAISE(ABORT, 'recommendation decision ledger rows younger than 7 days are immutable');
END;

CREATE INDEX IF NOT EXISTS idx_recommendation_decision_ledger_runs_created
    ON recommendation_decision_ledger_runs(created_at, run_id);
