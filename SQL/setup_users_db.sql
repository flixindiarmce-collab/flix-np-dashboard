-- Run these in the NP-team Railway PostgreSQL console (Query tab).
-- This DB is separate from flix-price-alerts' user DB.

-- Step 1: enable bcrypt
CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- Step 2: create the users table
CREATE TABLE IF NOT EXISTS dashboard_users (
  id            SERIAL PRIMARY KEY,
  user_id       VARCHAR(50)  UNIQUE NOT NULL,
  name          VARCHAR(100) NOT NULL,
  password_hash VARCHAR(300) NOT NULL,
  role          VARCHAR(20)  NOT NULL DEFAULT 'user',  -- 'admin' | 'np' | 'user'
  tabs          TEXT[],        -- NULL = all non-admin tabs; restricts per user when set
  active        BOOLEAN      NOT NULL DEFAULT TRUE,
  created_at    TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
  created_by    VARCHAR(50),
  last_login    TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS dashboard_users_user_id_idx ON dashboard_users (user_id);

-- Step 3: create your first admin — change the values in single quotes
INSERT INTO dashboard_users (user_id, name, password_hash, role)
VALUES (
  'admin',
  'NP Master Admin',
  crypt('CHANGE_ME_BEFORE_RUNNING', gen_salt('bf', 12)),
  'admin'
);

-- Verify it worked
SELECT user_id, name, role, active, created_at FROM dashboard_users;
