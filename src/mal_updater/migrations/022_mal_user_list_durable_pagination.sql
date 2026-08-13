-- Durable, fail-closed MAL @me anime-list pagination state.  Published cache
-- rows remain in mal_user_anime_list_cache; incomplete generations stage here.
ALTER TABLE mal_user_anime_list_refresh_generations ADD COLUMN account_key TEXT NOT NULL DEFAULT 'legacy';
ALTER TABLE mal_user_anime_list_refresh_generations ADD COLUMN account_id INTEGER;
ALTER TABLE mal_user_anime_list_refresh_generations ADD COLUMN account_name TEXT;
ALTER TABLE mal_user_anime_list_refresh_generations ADD COLUMN query_identity TEXT NOT NULL DEFAULT 'legacy';
ALTER TABLE mal_user_anime_list_refresh_generations ADD COLUMN query_json TEXT NOT NULL DEFAULT '{}';
ALTER TABLE mal_user_anime_list_refresh_generations ADD COLUMN logic_version TEXT NOT NULL DEFAULT 'mal-user-list-pagination-v2';
ALTER TABLE mal_user_anime_list_refresh_generations ADD COLUMN claim_token TEXT;
ALTER TABLE mal_user_anime_list_refresh_generations ADD COLUMN claim_expires_at TEXT;
ALTER TABLE mal_user_anime_list_refresh_generations ADD COLUMN revision INTEGER NOT NULL DEFAULT 0 CHECK (revision >= 0);
ALTER TABLE mal_user_anime_list_refresh_generations ADD COLUMN fairness_sequence INTEGER NOT NULL DEFAULT 0 CHECK (fairness_sequence >= 0);
ALTER TABLE mal_user_anime_list_refresh_generations ADD COLUMN requests_attempted INTEGER NOT NULL DEFAULT 0 CHECK (requests_attempted >= 0);
ALTER TABLE mal_user_anime_list_refresh_generations ADD COLUMN requests_succeeded INTEGER NOT NULL DEFAULT 0 CHECK (requests_succeeded >= 0);
ALTER TABLE mal_user_anime_list_refresh_generations ADD COLUMN requests_failed INTEGER NOT NULL DEFAULT 0 CHECK (requests_failed >= 0);
ALTER TABLE mal_user_anime_list_refresh_generations ADD COLUMN restart_count INTEGER NOT NULL DEFAULT 0 CHECK (restart_count >= 0);
ALTER TABLE mal_user_anime_list_refresh_generations ADD COLUMN drift_count INTEGER NOT NULL DEFAULT 0 CHECK (drift_count >= 0);
ALTER TABLE mal_user_anime_list_refresh_generations ADD COLUMN quarantined_at TEXT;
ALTER TABLE mal_user_anime_list_refresh_generations ADD COLUMN quarantine_reason TEXT;
ALTER TABLE mal_user_anime_list_refresh_generations ADD COLUMN validated_at TEXT;
ALTER TABLE mal_user_anime_list_refresh_generations ADD COLUMN validation_fingerprint TEXT;
ALTER TABLE mal_user_anime_list_refresh_generations ADD COLUMN terminal_empty_proof_json TEXT NOT NULL DEFAULT '{}';
ALTER TABLE mal_user_anime_list_refresh_generations ADD COLUMN staged_revision INTEGER NOT NULL DEFAULT 0 CHECK (staged_revision >= 0);
ALTER TABLE mal_user_anime_list_refresh_generations ADD COLUMN validated_staged_revision INTEGER;
ALTER TABLE mal_user_anime_list_refresh_generations ADD COLUMN publication_epoch INTEGER NOT NULL DEFAULT 0 CHECK (publication_epoch >= 0);
ALTER TABLE mal_user_anime_list_refresh_generations ADD COLUMN identity_assertion_nonce TEXT;
ALTER TABLE mal_user_anime_list_refresh_generations ADD COLUMN identity_asserted_at TEXT;
ALTER TABLE mal_user_anime_list_refresh_generations ADD COLUMN identity_asserted_revision INTEGER;
ALTER TABLE mal_user_anime_list_refresh_generations ADD COLUMN identity_assertion_consumed_at TEXT;

DROP INDEX IF EXISTS uq_mal_user_anime_list_refresh_generations_active;
CREATE UNIQUE INDEX IF NOT EXISTS uq_mal_user_anime_list_refresh_active_identity
    ON mal_user_anime_list_refresh_generations(account_key, query_identity)
    WHERE status = 'active';
-- Retain the historical contract name as a non-unique diagnostic index; active
-- ownership is now partitioned by account/query identity above.
CREATE INDEX IF NOT EXISTS uq_mal_user_anime_list_refresh_generations_active
    ON mal_user_anime_list_refresh_generations(status)
    WHERE status = 'active';
CREATE INDEX IF NOT EXISTS idx_mal_user_anime_list_refresh_claim
    ON mal_user_anime_list_refresh_generations(claim_expires_at, claim_token, status);

CREATE TABLE IF NOT EXISTS mal_user_anime_list_refresh_partitions (
    generation INTEGER NOT NULL,
    partition_key TEXT NOT NULL,
    requested_status TEXT,
    ordinal INTEGER NOT NULL CHECK (ordinal >= 0),
    initial_url TEXT NOT NULL,
    next_url TEXT,
    page_sequence INTEGER NOT NULL DEFAULT 0 CHECK (page_sequence >= 0),
    item_count INTEGER NOT NULL DEFAULT 0 CHECK (item_count >= 0),
    terminal INTEGER NOT NULL DEFAULT 0 CHECK (terminal IN (0, 1)),
    terminal_explicit INTEGER NOT NULL DEFAULT 0 CHECK (terminal_explicit IN (0, 1)),
    empty_proven INTEGER NOT NULL DEFAULT 0 CHECK (empty_proven IN (0, 1)),
    first_page_fingerprint TEXT,
    first_page_anchor_json TEXT NOT NULL DEFAULT '{}',
    final_page_url TEXT,
    final_page_fingerprint TEXT,
    final_page_anchor_json TEXT NOT NULL DEFAULT '{}',
    page1_validated_at TEXT,
    boundary_validated_at TEXT,
    attempt_count INTEGER NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    retry_count INTEGER NOT NULL DEFAULT 0 CHECK (retry_count >= 0),
    requests_succeeded INTEGER NOT NULL DEFAULT 0 CHECK (requests_succeeded >= 0),
    requests_failed INTEGER NOT NULL DEFAULT 0 CHECK (requests_failed >= 0),
    next_retry_at TEXT,
    retry_class TEXT,
    last_error TEXT CHECK (last_error IS NULL OR length(last_error) <= 2000),
    fairness_sequence INTEGER NOT NULL DEFAULT 0 CHECK (fairness_sequence >= 0),
    first_started_at TEXT,
    terminal_at TEXT,
    queue_class TEXT NOT NULL DEFAULT 'never_started'
        CHECK (queue_class IN ('never_started', 'resumable', 'retry_due', 'refresh_due', 'terminal', 'quarantined')),
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (generation, partition_key),
    FOREIGN KEY (generation) REFERENCES mal_user_anime_list_refresh_generations(generation) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_mal_user_list_partition_queue
    ON mal_user_anime_list_refresh_partitions(generation, terminal, next_retry_at, fairness_sequence, ordinal);

CREATE TABLE IF NOT EXISTS mal_user_anime_list_staged_pages (
    generation INTEGER NOT NULL,
    partition_key TEXT NOT NULL,
    page_sequence INTEGER NOT NULL CHECK (page_sequence >= 1),
    page_url TEXT NOT NULL,
    page_offset INTEGER NOT NULL DEFAULT 0 CHECK (page_offset >= 0),
    expected_page_size INTEGER NOT NULL DEFAULT 100 CHECK (expected_page_size >= 1),
    next_url TEXT,
    item_count INTEGER NOT NULL CHECK (item_count >= 0),
    page_fingerprint TEXT NOT NULL,
    anchor_json TEXT NOT NULL DEFAULT '{}',
    terminal_explicit INTEGER NOT NULL DEFAULT 0 CHECK (terminal_explicit IN (0, 1)),
    fetched_at TEXT NOT NULL,
    validated_at TEXT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (generation, partition_key, page_sequence),
    UNIQUE (generation, partition_key, page_url),
    FOREIGN KEY (generation, partition_key)
        REFERENCES mal_user_anime_list_refresh_partitions(generation, partition_key) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS mal_user_anime_list_staged_rows (
    generation INTEGER NOT NULL,
    partition_key TEXT NOT NULL,
    page_sequence INTEGER NOT NULL,
    item_order INTEGER NOT NULL CHECK (item_order >= 0),
    mal_anime_id INTEGER NOT NULL CHECK (mal_anime_id > 0),
    mal_status TEXT NOT NULL,
    item_json TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (generation, mal_anime_id),
    UNIQUE (generation, partition_key, page_sequence, item_order),
    FOREIGN KEY (generation, partition_key, page_sequence)
        REFERENCES mal_user_anime_list_staged_pages(generation, partition_key, page_sequence) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_mal_user_list_staged_rows_page
    ON mal_user_anime_list_staged_rows(generation, partition_key, page_sequence, item_order);

CREATE TABLE IF NOT EXISTS mal_user_anime_list_publication_fence (
    account_key TEXT PRIMARY KEY,
    generation INTEGER NOT NULL,
    query_identity TEXT NOT NULL,
    published_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (generation) REFERENCES mal_user_anime_list_refresh_generations(generation)
);

CREATE TABLE IF NOT EXISTS mal_user_anime_list_account_authority (
    account_key TEXT PRIMARY KEY,
    account_id INTEGER NOT NULL CHECK (account_id > 0),
    account_name TEXT NOT NULL,
    publication_epoch INTEGER NOT NULL CHECK (publication_epoch >= 1),
    current_generation INTEGER NOT NULL,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (current_generation) REFERENCES mal_user_anime_list_refresh_generations(generation)
);

-- Existing complete generation history and published rows are intentionally
-- retained.  Legacy rows gain deterministic identity/backfill values above.
UPDATE mal_user_anime_list_refresh_generations
SET account_key = COALESCE(NULLIF(account_key, ''), 'legacy'),
    query_identity = COALESCE(NULLIF(query_identity, ''), 'legacy'),
    query_json = COALESCE(NULLIF(query_json, ''), '{}'),
    logic_version = COALESCE(NULLIF(logic_version, ''), 'mal-user-list-pagination-v2');
