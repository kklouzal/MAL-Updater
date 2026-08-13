CREATE TABLE IF NOT EXISTS mal_public_userrecs_source_queue (
    source_mal_anime_id INTEGER PRIMARY KEY,
    queue_class TEXT NOT NULL DEFAULT 'never_started'
        CHECK (queue_class IN ('never_started', 'resumable', 'retry_due', 'refresh_due', 'fresh', 'quarantined')),
    eligible INTEGER NOT NULL DEFAULT 1 CHECK (eligible IN (0, 1)),
    enqueued_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    class_entered_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_selected_at TEXT,
    selection_sequence INTEGER NOT NULL DEFAULT 0 CHECK (selection_sequence >= 0),
    selection_count INTEGER NOT NULL DEFAULT 0 CHECK (selection_count >= 0),
    next_retry_at TEXT,
    claim_token TEXT,
    claim_expires_at TEXT,
    last_generation_id INTEGER,
    last_outcome TEXT,
    last_error_code TEXT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_public_userrecs_source_queue_select
    ON mal_public_userrecs_source_queue(queue_class, eligible, next_retry_at, last_selected_at, class_entered_at, source_mal_anime_id);
CREATE INDEX IF NOT EXISTS idx_public_userrecs_source_queue_claim
    ON mal_public_userrecs_source_queue(claim_expires_at, claim_token);

CREATE TABLE IF NOT EXISTS mal_public_userrecs_claim_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    generation_id INTEGER NOT NULL,
    source_mal_anime_id INTEGER NOT NULL,
    event_type TEXT NOT NULL CHECK (event_type IN ('bind', 'rebind')),
    previous_claim_token TEXT,
    claim_token TEXT NOT NULL,
    generation_revision INTEGER NOT NULL CHECK (generation_revision >= 0),
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (generation_id) REFERENCES mal_public_userrecs_crawl_generations(generation_id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_public_userrecs_claim_events_source
    ON mal_public_userrecs_claim_events(source_mal_anime_id, id DESC);

ALTER TABLE mal_public_userrecs_crawl_generations
    ADD COLUMN logic_version TEXT NOT NULL DEFAULT 'public-userrecs-snapshot-v2';
ALTER TABLE mal_public_userrecs_crawl_generations ADD COLUMN generation_key TEXT;
ALTER TABLE mal_public_userrecs_crawl_generations
    ADD COLUMN attempt_count INTEGER NOT NULL DEFAULT 0 CHECK (attempt_count >= 0);
ALTER TABLE mal_public_userrecs_crawl_generations
    ADD COLUMN restart_count INTEGER NOT NULL DEFAULT 0 CHECK (restart_count >= 0);
ALTER TABLE mal_public_userrecs_crawl_generations
    ADD COLUMN drift_count INTEGER NOT NULL DEFAULT 0 CHECK (drift_count >= 0);
ALTER TABLE mal_public_userrecs_crawl_generations ADD COLUMN next_retry_at TEXT;
ALTER TABLE mal_public_userrecs_crawl_generations ADD COLUMN retry_class TEXT;
ALTER TABLE mal_public_userrecs_crawl_generations ADD COLUMN first_page_revalidated_at TEXT;
ALTER TABLE mal_public_userrecs_crawl_generations ADD COLUMN boundary_revalidated_at TEXT;
ALTER TABLE mal_public_userrecs_crawl_generations ADD COLUMN terminal_evidence_json TEXT NOT NULL DEFAULT '{}';
ALTER TABLE mal_public_userrecs_crawl_generations ADD COLUMN quarantined_at TEXT;
ALTER TABLE mal_public_userrecs_crawl_generations ADD COLUMN quarantine_reason TEXT;
ALTER TABLE mal_public_userrecs_crawl_generations ADD COLUMN claim_token TEXT;
ALTER TABLE mal_public_userrecs_crawl_generations ADD COLUMN claim_expires_at TEXT;
ALTER TABLE mal_public_userrecs_crawl_generations
    ADD COLUMN generation_revision INTEGER NOT NULL DEFAULT 0 CHECK (generation_revision >= 0);
ALTER TABLE mal_public_userrecs_crawl_generations
    ADD COLUMN staged_revision INTEGER NOT NULL DEFAULT 0 CHECK (staged_revision >= 0);
ALTER TABLE mal_public_userrecs_crawl_generations ADD COLUMN validated_staged_revision INTEGER;
ALTER TABLE mal_public_userrecs_crawl_generations ADD COLUMN validation_fingerprint TEXT;

UPDATE mal_public_userrecs_crawl_generations
SET generation_key = 'legacy-' || generation_id
WHERE generation_key IS NULL OR TRIM(generation_key) = '';

CREATE UNIQUE INDEX IF NOT EXISTS idx_public_userrecs_generation_key
    ON mal_public_userrecs_crawl_generations(generation_key);
CREATE INDEX IF NOT EXISTS idx_public_userrecs_generation_retry
    ON mal_public_userrecs_crawl_generations(next_retry_at, status)
    WHERE next_retry_at IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_public_userrecs_generation_quarantine
    ON mal_public_userrecs_crawl_generations(quarantined_at, source_mal_anime_id)
    WHERE quarantined_at IS NOT NULL;

INSERT OR IGNORE INTO mal_public_userrecs_source_queue (
    source_mal_anime_id,
    queue_class,
    enqueued_at,
    class_entered_at,
    last_selected_at,
    selection_count,
    selection_sequence,
    next_retry_at,
    last_generation_id,
    last_outcome
)
SELECT
    authoritative.source_mal_anime_id,
    CASE
        WHEN authoritative.quarantined_at IS NOT NULL THEN 'quarantined'
        WHEN authoritative.status IN ('active', 'paused', 'ready') AND authoritative.retry_class IS NOT NULL THEN 'retry_due'
        WHEN authoritative.status IN ('active', 'paused', 'ready') THEN 'resumable'
        WHEN authoritative.status = 'published' THEN 'refresh_due'
        WHEN authoritative.status = 'failed' THEN 'retry_due'
        ELSE 'never_started'
    END,
    authoritative.created_at,
    authoritative.created_at,
    authoritative.updated_at,
    0,
    0,
    authoritative.next_retry_at,
    authoritative.generation_id,
    authoritative.status
FROM mal_public_userrecs_crawl_generations AS authoritative
JOIN (
    SELECT source_mal_anime_id, MAX(generation_id) AS generation_id
    FROM mal_public_userrecs_crawl_generations
    GROUP BY source_mal_anime_id
) AS newest ON newest.generation_id = authoritative.generation_id;
