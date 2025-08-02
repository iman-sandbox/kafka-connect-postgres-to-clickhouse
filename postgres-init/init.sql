-- USERS TABLE for CDC testing
CREATE TABLE IF NOT EXISTS users (
    user_id SERIAL PRIMARY KEY,
    username VARCHAR(50) NOT NULL,
    account_type VARCHAR(20) NOT NULL,
    updated_at TIMESTAMP DEFAULT timezone('UTC', CURRENT_TIMESTAMP),
    created_at TIMESTAMP DEFAULT timezone('UTC', CURRENT_TIMESTAMP)
);

-- TABLE TO LOG DDL COMMANDS
CREATE TABLE IF NOT EXISTS ddl_log (
    id BIGSERIAL PRIMARY KEY,
    executed_at TIMESTAMPTZ DEFAULT now(),
    username TEXT,
    command_tag TEXT,
    ddl_command TEXT
);

-- FUNCTION TO LOG DDL COMMANDS (OPTIONALLY FILTER BY SCHEMA)
CREATE OR REPLACE FUNCTION log_ddl_commands()
RETURNS event_trigger AS $$
DECLARE
  obj RECORD;
BEGIN
  FOR obj IN SELECT * FROM pg_event_trigger_ddl_commands()
  LOOP
    -- Optional: only capture commands in 'public' schema
    IF obj.object_identity LIKE 'public.%' THEN
      INSERT INTO ddl_log(username, command_tag, ddl_command)
      VALUES (current_user, obj.command_tag, current_query());
    END IF;
  END LOOP;
END;
$$ LANGUAGE plpgsql;

-- EVENT TRIGGER TO CAPTURE DDL
CREATE EVENT TRIGGER capture_ddl
ON ddl_command_end
EXECUTE FUNCTION log_ddl_commands();
