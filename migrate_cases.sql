-- Adds multi-case roulette support (cases + case_prizes) and fixes missing columns/indexes.
-- Safe to run multiple times.

CREATE TABLE IF NOT EXISTS cases (
  id BIGINT PRIMARY KEY,
  name TEXT NOT NULL,
  description TEXT,
  image_url TEXT,
  price INTEGER NOT NULL DEFAULT 25,
  is_active BOOLEAN NOT NULL DEFAULT TRUE,
  sort_order INTEGER NOT NULL DEFAULT 0,
  created_at BIGINT NOT NULL
);

ALTER TABLE cases ADD COLUMN IF NOT EXISTS description TEXT;
ALTER TABLE cases ADD COLUMN IF NOT EXISTS image_url TEXT;
ALTER TABLE cases ADD COLUMN IF NOT EXISTS price INTEGER NOT NULL DEFAULT 25;
ALTER TABLE cases ADD COLUMN IF NOT EXISTS is_active BOOLEAN NOT NULL DEFAULT TRUE;
ALTER TABLE cases ADD COLUMN IF NOT EXISTS sort_order INTEGER NOT NULL DEFAULT 0;
ALTER TABLE cases ADD COLUMN IF NOT EXISTS created_at BIGINT NOT NULL DEFAULT 0;

CREATE INDEX IF NOT EXISTS idx_cases_active_sort ON cases(is_active, sort_order, id);

CREATE TABLE IF NOT EXISTS case_prizes (
  case_id BIGINT NOT NULL REFERENCES cases(id) ON DELETE CASCADE,
  prize_id BIGINT NOT NULL REFERENCES prizes(id) ON DELETE CASCADE,
  weight INTEGER NOT NULL DEFAULT 1,
  is_active BOOLEAN NOT NULL DEFAULT TRUE,
  sort_order INTEGER NOT NULL DEFAULT 0,
  created_at BIGINT NOT NULL,
  PRIMARY KEY (case_id, prize_id)
);

ALTER TABLE case_prizes ADD COLUMN IF NOT EXISTS weight INTEGER NOT NULL DEFAULT 1;
ALTER TABLE case_prizes ADD COLUMN IF NOT EXISTS is_active BOOLEAN NOT NULL DEFAULT TRUE;
ALTER TABLE case_prizes ADD COLUMN IF NOT EXISTS sort_order INTEGER NOT NULL DEFAULT 0;
ALTER TABLE case_prizes ADD COLUMN IF NOT EXISTS created_at BIGINT NOT NULL DEFAULT 0;

CREATE INDEX IF NOT EXISTS idx_case_prizes_case_active_sort
ON case_prizes(case_id, is_active, sort_order, prize_id);

ALTER TABLE spins ADD COLUMN IF NOT EXISTS case_id BIGINT;
ALTER TABLE spins ADD COLUMN IF NOT EXISTS case_name TEXT;
