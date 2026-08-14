ALTER TABLE mal_public_userrecs_crawl_generations
    ADD COLUMN validation_page_number INTEGER NOT NULL DEFAULT 0 CHECK (validation_page_number >= 0);
ALTER TABLE mal_public_userrecs_crawl_generations
    ADD COLUMN validation_revision INTEGER;
