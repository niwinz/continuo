CREATE TYPE partition AS ENUM ('system', 'schema', 'user');

-- A table that will store the whole transaction log
-- of the database.
CREATE TABLE IF NOT EXISTS txlog (
  id uuid PRIMARY KEY,
  part partition,
  facts bytea,
  created_at timestamptz
) WITH (OIDS=FALSE);

-- A table that will store the current
-- view of the schema.
CREATE TABLE IF NOT EXISTS dbschema {
  ident text PRIMARY KEY,
  opts bytea,
  created_at timestamptz,
  modified_at timestamptz
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
  created_at timestamptz,
  modified_at timestamptz
) WITH (OIDS=FALSE);

-- A table that tracks the attributes associated
-- with an entity.
CREATE TABLE IF NOT EXISTS entity_attrs (
   eid uuid NOT NULL,
   attr text NOT NULL,
   PRIMARY KEY(eid,attr)
) WITH (OIDS=FALSE);
