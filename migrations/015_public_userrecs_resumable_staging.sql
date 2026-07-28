CREATE TABLE IF NOT EXISTS mal_public_userrecs_crawl_generations (
    generation_id INTEGER PRIMARY KEY AUTOINCREMENT,
    source_mal_anime_id INTEGER NOT NULL,
    source_title TEXT,
    source_url TEXT,
    status TEXT NOT NULL DEFAULT 'active'
        CHECK (status IN ('active', 'paused', 'ready', 'published', 'discarded', 'failed')),
    cursor_url TEXT,
    pages_fetched INTEGER NOT NULL DEFAULT 0 CHECK (pages_fetched >= 0),
    staged_edge_count INTEGER NOT NULL DEFAULT 0 CHECK (staged_edge_count >= 0),
    last_page_url TEXT,
    last_page_fingerprint TEXT,
    last_error TEXT,
    started_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    completed_at TEXT,
    published_at TEXT,
    discarded_at TEXT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_mal_public_userrecs_one_open_generation
    ON mal_public_userrecs_crawl_generations(source_mal_anime_id)
    WHERE status IN ('active', 'paused', 'ready');

CREATE INDEX IF NOT EXISTS idx_mal_public_userrecs_generation_source_history
    ON mal_public_userrecs_crawl_generations(source_mal_anime_id, generation_id DESC);

CREATE INDEX IF NOT EXISTS idx_mal_public_userrecs_generation_status
    ON mal_public_userrecs_crawl_generations(status, updated_at);

CREATE TABLE IF NOT EXISTS mal_public_userrecs_staged_pages (
    generation_id INTEGER NOT NULL,
    source_mal_anime_id INTEGER NOT NULL,
    page_number INTEGER NOT NULL CHECK (page_number >= 1),
    page_url TEXT NOT NULL,
    page_fingerprint TEXT NOT NULL,
    anchor_json TEXT NOT NULL DEFAULT '{}',
    next_url TEXT,
    edge_count INTEGER NOT NULL DEFAULT 0 CHECK (edge_count >= 0),
    fetched_at TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (generation_id, page_number),
    FOREIGN KEY (generation_id)
        REFERENCES mal_public_userrecs_crawl_generations(generation_id)
        ON DELETE CASCADE
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_mal_public_userrecs_staged_pages_url
    ON mal_public_userrecs_staged_pages(generation_id, page_url);

CREATE INDEX IF NOT EXISTS idx_mal_public_userrecs_staged_pages_source
    ON mal_public_userrecs_staged_pages(source_mal_anime_id, page_number);

CREATE TABLE IF NOT EXISTS mal_public_userrecs_staged_edges (
    generation_id INTEGER NOT NULL,
    source_mal_anime_id INTEGER NOT NULL,
    page_number INTEGER NOT NULL CHECK (page_number >= 1),
    target_mal_anime_id INTEGER NOT NULL,
    target_title TEXT,
    num_recommendations INTEGER CHECK (num_recommendations IS NULL OR num_recommendations >= 0),
    raw_json TEXT NOT NULL DEFAULT '{}',
    provenance_json TEXT NOT NULL DEFAULT '{}',
    fetched_at TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (generation_id, page_number, target_mal_anime_id),
    FOREIGN KEY (generation_id, page_number)
        REFERENCES mal_public_userrecs_staged_pages(generation_id, page_number)
        ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_mal_public_userrecs_staged_edges_source
    ON mal_public_userrecs_staged_edges(source_mal_anime_id, target_mal_anime_id);

CREATE INDEX IF NOT EXISTS idx_mal_public_userrecs_staged_edges_generation_target
    ON mal_public_userrecs_staged_edges(generation_id, target_mal_anime_id, num_recommendations DESC);

CREATE TABLE IF NOT EXISTS mal_public_userrecs_crawl_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    generation_id INTEGER,
    source_mal_anime_id INTEGER NOT NULL,
    event_type TEXT NOT NULL
        CHECK (event_type IN ('begin', 'page_upsert', 'pause', 'resume', 'ready', 'publish', 'discard', 'fail')),
    page_number INTEGER CHECK (page_number IS NULL OR page_number >= 1),
    page_url TEXT,
    error TEXT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (generation_id)
        REFERENCES mal_public_userrecs_crawl_generations(generation_id)
        ON DELETE SET NULL
);

CREATE INDEX IF NOT EXISTS idx_mal_public_userrecs_crawl_events_generation
    ON mal_public_userrecs_crawl_events(generation_id, id);

CREATE INDEX IF NOT EXISTS idx_mal_public_userrecs_crawl_events_source
    ON mal_public_userrecs_crawl_events(source_mal_anime_id, id);
