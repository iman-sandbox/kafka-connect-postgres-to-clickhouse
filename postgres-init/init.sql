-- Replace any references to schema 'public' or 'postgres'
CREATE SCHEMA iman;
SET search_path TO iman;

CREATE TABLE IF NOT EXISTS iman.users (
  user_id SERIAL PRIMARY KEY,
  username VARCHAR(255),
  account_type VARCHAR(50),
  updated_at TIMESTAMP,
  created_at TIMESTAMP
);

-- -- Optional: preload rows
-- INSERT INTO users (username, account_type, updated_at, created_at)
-- VALUES 
--   ('user1', 'Bronze', now(), now()),
--   ('user2', 'Silver', now(), now()),
--   ('user3', 'Gold', now(), now());
