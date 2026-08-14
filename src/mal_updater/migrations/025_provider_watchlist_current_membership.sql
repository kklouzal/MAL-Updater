ALTER TABLE provider_watchlist ADD COLUMN is_active INTEGER NOT NULL DEFAULT 1 CHECK (is_active IN (0, 1));
ALTER TABLE provider_watchlist ADD COLUMN membership_generation INTEGER;
ALTER TABLE provider_watchlist ADD COLUMN account_id_hint TEXT;
ALTER TABLE provider_watchlist ADD COLUMN deactivated_at TEXT;

CREATE INDEX IF NOT EXISTS idx_provider_watchlist_active_series
    ON provider_watchlist(provider, is_active, provider_series_id);
CREATE INDEX IF NOT EXISTS idx_provider_watchlist_account_generation
    ON provider_watchlist(provider, account_id_hint, membership_generation);
