CREATE TYPE partition AS ENUM ('system', 'schema', 'user');

-- A table that will stre the whole transaction log
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

-- A table that represents the materialized view of
-- the current applied schema.
CREATE TABLE IF NOT EXISTS schemaview (
  name text PRIMARY KEY,
  opts bytea,
  created_at timestamptz default now(),
  modified_at timestamptz default now(),
  txid bigint references txlog(id)
) WITH (OIDS=FALSE);

-- A table that will stores all the entities
CREATE TABLE IF NOT EXISTS entity (
  id uuid PRIMARY KEY,
  attributes text[],
  created_at timestamptz default now(),
  modified_at timestamptz
) WITH (OIDS=FALSE);
