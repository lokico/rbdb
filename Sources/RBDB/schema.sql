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

-- The record is immutable: nothing here is ever deleted. A row leaves the believed set by being
--  *superseded* (see `superceded_by`), which keeps the history queryable.
CREATE TABLE IF NOT EXISTS _rule (
    internal_entity_id INTEGER PRIMARY KEY REFERENCES _entity,
    formula BLOB NOT NULL, -- JSONB; uniqueness is over *live* rows only, see idx_rule_live_formula
    -- NULL ⟹ live: part of the believed set. Otherwise the entity whose assertion superseded this
    --  row — either another `_rule` row (a more general rule that subsumes it) or a bare `_entity`
    --  standing for an explicit retraction act. `_entity.entity_id` is a uuidv7, so both the assert
    --  time (this row's own entity) and the supersede time (the target's) are recoverable without
    --  storing a timestamp.
    superceded_by INTEGER NULL REFERENCES _entity,
    output_type TEXT GENERATED ALWAYS AS (formula->>0) VIRTUAL COLLATE NOCASE,
    arg1_constant ANY GENERATED ALWAYS AS (json_extract(formula, '$[1][0].""')) VIRTUAL, -- NULL if arg is not a constant
    arg2_constant ANY GENERATED ALWAYS AS (json_extract(formula, '$[1][1].""')) VIRTUAL,  -- NULL if arg is not a constant
    negative_literal_count INT GENERATED ALWAYS AS (case when output_type LIKE '@%' then json_array_length(formula) - 2 else null end) VIRTUAL -- NULL if not a horn clause
) STRICT;

-- Re-asserting a *live* fact or rule stays a no-op (the insert sites carry the matching
--  `ON CONFLICT (formula) WHERE superceded_by IS NULL DO NOTHING`). A *superseded* row no longer
--  blocks re-assertion, so assert → retract → re-assert produces two rows and a real history.
CREATE UNIQUE INDEX IF NOT EXISTS idx_rule_live_formula ON _rule(formula) WHERE superceded_by IS NULL;

-- Using "COLLATE NOCASE" ensures that the LIKE optimization can be applied for output_type
-- https://www.sqlite.org/optoverview.html#the_like_optimization
-- Both carry the `superceded_by IS NULL` predicate so they stay usable under the filter that every
--  read site now applies.
CREATE INDEX IF NOT EXISTS idx_rule_ot_nlc_arg1_arg2 ON _rule(output_type COLLATE NOCASE, negative_literal_count, arg1_constant, arg2_constant) WHERE superceded_by IS NULL;
CREATE INDEX IF NOT EXISTS idx_rule_ot_nlc_arg2_arg1 ON _rule(output_type COLLATE NOCASE, negative_literal_count, arg2_constant, arg1_constant) WHERE superceded_by IS NULL;

-- Bookkeeping for materialized recursive closures (see IterativeEvaluator). Both are temp (per
--  connection), like the temp tables that hold the closures themselves.
--   _materialized_dep: for each materialized top predicate, one row per member of its dependency cone
--     (itself, its derived members, and the base predicates it reads). Lets the trigger below map a
--     changed predicate back to the closures that must be refreshed.
--   _dirty: materialized top predicates whose closure is now stale and must be brought up to date
--     before the next query reads it. `rebuild = 0` means an *additive* change (re-iterating suffices);
--     `rebuild = 1` means a *destructive* one (a supersession — the closure must be dropped and rebuilt
--     from scratch, since re-iterating can only ever add rows). `rebuild` is sticky: the additive
--     trigger uses `INSERT OR IGNORE` and the destructive one `INSERT OR REPLACE … 1`, so an additive
--     change arriving after a destructive one cannot downgrade the pending rebuild.
CREATE TEMP TABLE IF NOT EXISTS _materialized_dep (
    materialized_top TEXT NOT NULL,
    depends_on TEXT NOT NULL,
    PRIMARY KEY (materialized_top, depends_on)
);
CREATE TEMP TABLE IF NOT EXISTS _dirty (name TEXT PRIMARY KEY, rebuild INT NOT NULL DEFAULT 0);

-- Migration: an earlier schema shipped a *persistent* `_drop_temp_view_on_rule_insert` trigger that
--  unconditionally ran `DROP VIEW IF EXISTS <predicate>` on every `_rule` insert. Now that predicates
--  can be materialized as temp *tables* (see IterativeEvaluator), that DROP VIEW aborts with "use DROP
--  TABLE" whenever the predicate a new rule targets is currently materialized. The temp
--  `_invalidate_on_rule_insert` below supersedes it (and guards on `type = 'view'`), so drop this one.
DROP TRIGGER IF EXISTS _drop_temp_view_on_rule_insert;

-- Trigger is temp so it can access temp.sqlite_schema (bypassing same-DB restriction for non-temp triggers)
-- FIXME: Take a close look at using `main.` below whenever we support attaching other DBs
-- (used as recommended by https://www.sqlite.org/lang_createtrigger.html#temp_triggers_on_non_temp_tables)
-- Note the scope: `main._rule` is shared, but a trigger program is compiled into the statement that
--  fires it, so this runs only on the connection performing the write — every connection creating its
--  own copy does not change that. A rule asserted on another connection would therefore leave this
--  one's views and closures standing, answering from the rule set they were built against. That is
--  precisely why a database serves one connection at a time (`RBDB.claimExclusively`): this
--  invalidation is only sound when there is nothing else to invalidate.
CREATE TEMP TRIGGER IF NOT EXISTS _invalidate_on_rule_insert
AFTER INSERT ON main._rule
BEGIN
  -- Every materialized closure that (transitively) depends on the changed predicate is now stale. We
  --  don't drop it here — dropping a temp table while this INSERT statement runs deadlocks on the
  --  on-disk table lock, and a fixpoint loop can't run in a trigger anyway. We just flag it;
  --  `refreshDirtyMaterializations` re-iterates it at the next query (a safe point). An *insert* is
  --  monotonic — positive Datalog only ever derives more from more — so re-running the fixpoint over
  --  the existing rows suffices and `rebuild` stays 0. (Retraction is the non-monotonic half; see
  --  `_invalidate_on_rule_supersede` below.) `OR IGNORE` leaves an already-pending `rebuild = 1` alone.
  INSERT OR IGNORE INTO _dirty (name)
  SELECT materialized_top FROM _materialized_dep
  WHERE depends_on = substr(NEW.output_type, 2);

  -- A new rule for a predicate backed by a plain (non-materialized) view: the view must be rebuilt to
  --  include the rule — or re-routed to the iterative evaluator if the rule makes it recursive — so
  --  drop it and let the next query rebuild it via `rescue`. Dropping a *view* doesn't take the table
  --  lock that dropping a materialized table would, so it is safe to do here. This must be the trigger's
  --  LAST statement: it changes the schema, and any statement running after that in the same trigger
  --  aborts with SQLITE_ABORT_ROLLBACK.
  SELECT sql_exec('DROP VIEW temp.[' || s.name || ']')
  FROM temp.sqlite_schema s
  WHERE s.type = 'view'
    AND s.name = substr(NEW.output_type, 2)
    AND NEW.negative_literal_count > 0;
END;

-- The `UPDATE` twin of the trigger above: a row leaving the believed set. Where an insert is additive
--  (and so can be absorbed by re-iterating a closure), a supersession is *destructive*: re-running the
--  fixpoint over a materialized closure can never remove rows that are no longer derivable. So this
--  flags `rebuild = 1`, which makes `refreshDirtyMaterializations` drop the closure instead of
--  re-iterating it. As above, the drop itself can't happen here (the on-disk table lock).
CREATE TEMP TRIGGER IF NOT EXISTS _invalidate_on_rule_supersede
AFTER UPDATE OF superceded_by ON main._rule
WHEN OLD.superceded_by IS NULL AND NEW.superceded_by IS NOT NULL
BEGIN
  INSERT OR REPLACE INTO _dirty (name, rebuild)
  SELECT materialized_top, 1 FROM _materialized_dep
  WHERE depends_on = substr(NEW.output_type, 2);

  -- Same view-drop as the insert trigger, and same guard: base facts are read *live* through
  --  `baseFactsSelect` inside the view body, so retracting a fact needs no view drop; only a retracted
  --  *rule* changes a view's definition. Must be the trigger's LAST statement — see above.
  SELECT sql_exec('DROP VIEW temp.[' || s.name || ']')
  FROM temp.sqlite_schema s
  WHERE s.type = 'view'
    AND s.name = substr(NEW.output_type, 2)
    AND NEW.negative_literal_count > 0;
END;

-- FIXME: Expose a "rule" view that has entity uuid and formula as a string?
