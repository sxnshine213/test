-- Optional manual migration (server.py init_db also applies these on startup)

ALTER TABLE prizes ADD COLUMN IF NOT EXISTS gift_id TEXT;
ALTER TABLE prizes ADD COLUMN IF NOT EXISTS is_unique BOOLEAN DEFAULT FALSE;

ALTER TABLE inventory ADD COLUMN IF NOT EXISTS is_locked BOOLEAN DEFAULT FALSE;
ALTER TABLE inventory ADD COLUMN IF NOT EXISTS locked_reason TEXT;

CREATE TABLE IF NOT EXISTS claims (
  id BIGSERIAL PRIMARY KEY,
  tg_user_id TEXT NOT NULL,
  inventory_id BIGINT NOT NULL,
  prize_id BIGINT NOT NULL,
  prize_name TEXT NOT NULL,
  prize_cost INTEGER NOT NULL,
  status TEXT NOT NULL,
  note TEXT,
  created_at BIGINT NOT NULL,
  updated_at BIGINT
);
CREATE INDEX IF NOT EXISTS idx_claims_status_time ON claims(status, created_at);
