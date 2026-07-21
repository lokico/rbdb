-- SQLite Database Schema for RBDB

 PRAGMA foreign_keys = ON;

-- The following are internal tables (names starting with underscore "_") ...

-- Maps shorter int entity IDs that are internal to this DB to UUIDs
--  that can be used externally. Currently, this is not super useful,
--  but in the future I imagine we'll use it for a couple things:
--    1. When attaching multiple database files and merging them.
--    2. When we discover that different entities are actually the same
--        thing and we want to merge them. When we do this, we'll keep
--        the UUID with the oldest timestamp.
CREATE TABLE IF NOT EXISTS _entity (
    internal_entity_id INTEGER PRIMARY KEY,
    entity_id BLOB NOT NULL DEFAULT (uuidv7())
) STRICT;

-- FIXME: Table of entity links to external data sources (e.g. reminder linked to Reminders app ID)

CREATE TABLE IF NOT EXISTS _predicate (
    internal_entity_id INTEGER PRIMARY KEY REFERENCES _entity,
    name TEXT UNIQUE NOT NULL,
    descr TEXT,
    column_names BLOB -- JSONB
) STRICT;

CREATE TABLE IF NOT EXISTS _rule (
    internal_entity_id INTEGER PRIMARY KEY REFERENCES _entity,
    formula BLOB UNIQUE NOT NULL, -- JSONB
    output_type TEXT GENERATED ALWAYS AS (formula->>0) VIRTUAL COLLATE NOCASE,
    arg1_constant ANY GENERATED ALWAYS AS (json_extract(formula, '$[1][0].""')) VIRTUAL, -- NULL if arg is not a constant
    arg2_constant ANY GENERATED ALWAYS AS (json_extract(formula, '$[1][1].""')) VIRTUAL,  -- NULL if arg is not a constant
    negative_literal_count INT GENERATED ALWAYS AS (case when output_type LIKE '@%' then json_array_length(formula) - 2 else null end) VIRTUAL -- NULL if not a horn clause
) STRICT;

-- Using "COLLATE NOCASE" ensures that the LIKE optimization can be applied for output_type
-- https://www.sqlite.org/optoverview.html#the_like_optimization
CREATE INDEX IF NOT EXISTS idx_rule_ot_nlc_arg1_arg2 ON _rule(output_type COLLATE NOCASE, negative_literal_count, arg1_constant, arg2_constant);
CREATE INDEX IF NOT EXISTS idx_rule_ot_nlc_arg2_arg1 ON _rule(output_type COLLATE NOCASE, negative_literal_count, arg2_constant, arg1_constant);

-- Trigger to drop temporary views when new rules (not facts) are added
CREATE TRIGGER IF NOT EXISTS _drop_temp_view_on_rule_insert
AFTER INSERT ON _rule
WHEN NEW.negative_literal_count > 0
BEGIN
  -- Drop the temporary view if it exists (using substr to remove @ prefix)
  SELECT sql_exec('DROP VIEW IF EXISTS ' || substr(NEW.output_type, 2));
END;

-- FIXME: Expose a "rule" view that has entity uuid and formula as a string?
