CREATE TABLE IF NOT EXISTS provider_fetch_provenance (
    provider TEXT NOT NULL,
    surface TEXT NOT NULL,
    account_id_hint TEXT NOT NULL DEFAULT '',
    completeness TEXT NOT NULL CHECK (completeness IN ('complete', 'partial', 'unknown')),
    expected_total INTEGER,
    collected_count INTEGER,
    pages_fetched INTEGER,
    observed_at TEXT,
    route TEXT,
    profile TEXT,
    region TEXT,
    sync_run_id TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (provider, surface, account_id_hint),
    FOREIGN KEY (sync_run_id) REFERENCES sync_runs(id)
);

CREATE INDEX IF NOT EXISTS idx_provider_fetch_provenance_run
ON provider_fetch_provenance(sync_run_id);
