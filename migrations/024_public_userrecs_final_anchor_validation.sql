ALTER TABLE mal_public_userrecs_crawl_generations
    ADD COLUMN final_anchor_step INTEGER NOT NULL DEFAULT 0 CHECK (final_anchor_step BETWEEN 0 AND 2);
ALTER TABLE mal_public_userrecs_crawl_generations
    ADD COLUMN final_anchor_revision INTEGER;
