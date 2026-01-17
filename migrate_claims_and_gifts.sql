-- Optional manual migration (server.py init_db() also applies these on startup)
-- Safe to run multiple times.

ALTER TABLE prizes ADD COLUMN IF NOT EXISTS gift_id TEXT;
ALTER TABLE prizes ADD COLUMN IF NOT EXISTS is_unique BOOLEAN NOT NULL DEFAULT FALSE;

ALTER TABLE inventory ADD COLUMN IF NOT EXISTS is_locked BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE inventory ADD COLUMN IF NOT EXISTS locked_reason TEXT;

CREATE TABLE IF NOT EXISTS claims (
  id BIGSERIAL PRIMARY KEY,
  tg_user_id TEXT NOT NULL REFERENCES users(tg_user_id) ON DELETE CASCADE,
  inventory_id BIGINT NOT NULL REFERENCES inventory(id) ON DELETE CASCADE,
  prize_id BIGINT NOT NULL,
  prize_name TEXT NOT NULL,
  status TEXT NOT NULL,
  created_at BIGINT NOT NULL,
  processed_at BIGINT
);

CREATE INDEX IF NOT EXISTS idx_claims_status_time ON claims(status, created_at);
