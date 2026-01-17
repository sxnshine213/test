-- Manual migration for multi-case roulette support (cases + case_prizes).
-- NOTE: server.py init_db() already applies these changes on startup.
-- Safe to run multiple times.

-- Cases table
CREATE TABLE IF NOT EXISTS cases (
  id BIGSERIAL PRIMARY KEY,
  name TEXT NOT NULL,
  description TEXT,
  price INTEGER NOT NULL,
  is_active BOOLEAN NOT NULL DEFAULT TRUE,
  sort_order INTEGER NOT NULL DEFAULT 0,
  created_at BIGINT NOT NULL,
  cover_url TEXT
);

-- Make existing DBs compatible (defaults are used only when adding missing columns)
ALTER TABLE cases ADD COLUMN IF NOT EXISTS description TEXT;
ALTER TABLE cases ADD COLUMN IF NOT EXISTS price INTEGER NOT NULL DEFAULT 25;
ALTER TABLE cases ADD COLUMN IF NOT EXISTS is_active BOOLEAN NOT NULL DEFAULT TRUE;
ALTER TABLE cases ADD COLUMN IF NOT EXISTS sort_order INTEGER NOT NULL DEFAULT 0;
ALTER TABLE cases ADD COLUMN IF NOT EXISTS created_at BIGINT NOT NULL DEFAULT 0;
ALTER TABLE cases ADD COLUMN IF NOT EXISTS cover_url TEXT;

CREATE INDEX IF NOT EXISTS idx_cases_active_sort ON cases(is_active, sort_order, id);

-- Case prizes mapping
CREATE TABLE IF NOT EXISTS case_prizes (
  case_id BIGINT NOT NULL REFERENCES cases(id) ON DELETE CASCADE,
  prize_id BIGINT NOT NULL REFERENCES prizes(id) ON DELETE CASCADE,
  weight INTEGER NOT NULL,
  is_active BOOLEAN NOT NULL DEFAULT TRUE,
  created_at BIGINT NOT NULL,
  PRIMARY KEY (case_id, prize_id)
);

ALTER TABLE case_prizes ADD COLUMN IF NOT EXISTS weight INTEGER NOT NULL DEFAULT 1;
ALTER TABLE case_prizes ADD COLUMN IF NOT EXISTS is_active BOOLEAN NOT NULL DEFAULT TRUE;
ALTER TABLE case_prizes ADD COLUMN IF NOT EXISTS created_at BIGINT NOT NULL DEFAULT 0;

CREATE INDEX IF NOT EXISTS idx_case_prizes_case ON case_prizes(case_id, is_active);
CREATE INDEX IF NOT EXISTS idx_case_prizes_prize ON case_prizes(prize_id);

-- Spins: store which case was opened
ALTER TABLE spins ADD COLUMN IF NOT EXISTS case_id BIGINT;
ALTER TABLE spins ADD COLUMN IF NOT EXISTS case_name TEXT;
ALTER TABLE spins ADD COLUMN IF NOT EXISTS case_price INTEGER;
