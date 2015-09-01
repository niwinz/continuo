CREATE TYPE partition AS ENUM ('system', 'schema', 'user');


-- A table that will store the whole transaction log
-- of the database.
CREATE TABLE IF NOT EXISTS txlog (
  id bigint PRIMARY KEY,
  part partition,
  facts bytea,
  created_at timestamptz default now()
) WITH (OIDS=FALSE);

-- A table that will store the key value properties
-- and will be used usually for storing settings.
CREATE TABLE IF NOT EXISTS properties (
  key text PRIMARY KEY;
  value bytea
) WITH (OIDS=FALSE);

-- A table that will stores all the entities
CREATE TABLE IF NOT EXISTS entity (
  id uuid PRIMARY KEY,
  part partition,
  attributes bytea,
  created_at timestamptz default now(),
  modified_at timestamptz
) WITH (OIDS=FALSE);

-- Schema related attributes tables

CREATE TABLE IF NOT EXISTS schema_db_ident (
  eid uuid PRIMARY KEY,
  txid bigint,
  created_at timestamptz default now(),
  modified_at timestamptz,
  content bytea
) WITH (OIDS=FALSE);

CREATE TABLE IF NOT EXISTS schema_db_unique (
  eid uuid PRIMARY KEY,
  txid bigint,
  created_at timestamptz default now(),
  modified_at timestamptz,
  content boolean
) WITH (OIDS=FALSE);
