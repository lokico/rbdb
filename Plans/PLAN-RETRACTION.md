# Plan: Retraction, supersession, and strong negation

## Context

RBDB stores every fact and rule as a row in `_rule`. There is currently no way to take one
back. This plan adds that — as an **immutable record**: nothing is ever deleted, rows are
*superseded*, and the history stays queryable.

### Retraction is not the same as asserting the negation

The two readings — "I know this to be false" and "I no longer know this to be true" — are
distinct operations, and the difference is load-bearing here. In AGM terms:

- **Asserting ¬p is expansion (or revision).** The theory says *more*: it entails ¬p.
- **Retracting p is contraction.** The theory says *less*: it entails neither p nor ¬p.

The Levi identity makes the relationship exact: `T * ¬p = (T ÷ p) + ¬p`. Revising by ¬p
*is* contraction by p followed by something strictly stronger. So the second operation is a
proper part of the first.

This plan implements both, and **keeps them separate**: the engine does not perform the Levi
composition on your behalf. Asserting `-p` over a live `p` is refused (§4.2), not silently
repaired — to revise, retract and then assert, two recorded acts rather than one that hides
half of itself.

### Why this matters now: the move to open world

Under a closed-world/NAF reading these two collapse for a ground atom with no rule deriving
it: retract `p(1)` and `not p(1)` succeeds, indistinguishable from having stored ¬p(1). RBDB
is **not currently committed to CWA** — queries return what is derivable, which is neutral
between the readings, and the closed-world commitment only enters when you can ask a
*negative* question, which needs negation-as-failure (not yet built). So there is nothing to
undo; the open-world decision is a constraint on what gets added next, and it lands first.

Under OWA the distinction becomes observable in query answers:

| | `?- p(1)` | `?- -p(1)` |
|---|---|---|
| `p(1)` asserted | yes | no |
| `p(1)` retracted | no | no — *unknown* |
| `-p(1)` asserted | no | yes — *known false* |

That is what makes strong negation (Step 4) worth building rather than deferring, and what
makes `superceded_by` mean contraction specifically rather than "gone."

### Relationship to the other plans

- **PLAN-EVENTS.md** — negation-as-failure (`not` in rule bodies) is a *different* negation
  and belongs to Phase 2 there. Under OWA it means "not derivable", not "false"; where
  genuine closure is wanted it is written as an ordinary rule
  (`-q(X,Y) :- dom(X), dom(Y), not q(X,Y)`) rather than declared out of band, which is what
  lets the event calculus's minimal-model semantics live inside a globally open world. See
  that plan's §2.0. Its Phase 4.1 (retraction) is superseded by this document and pulled
  forward.
- **Entities/existentials** (PLAN-EVENTS Phase 1) is the intended next work after this. OWA
  is its precondition: `∃x.p(x)` is exactly what a closed world cannot represent.

### No migration

Deliberate decision: **existing database files are not migrated and backward compatibility is
not a goal.** `schema.sql` is edited in place; stale `.db` files fail to open with an ordinary
SQLite error and should be recreated. No `PRAGMA user_version`, no table-rebuild dance. This
supersedes the "add a `user_version` bump" line in PLAN-EVENTS' cross-cutting notes.

---

## Step 1 — Make writes to materialized tables behave like writes to views

*Independent of everything else; smallest first.*

`IterativeEvaluator` materializes a recursive predicate's closure as a `TEMP TABLE`
(`IterativeEvaluator.swift:82-86`). Unlike the `TEMP VIEW` form, that is an ordinary writable
table, so writes that the view form handles or refuses are instead silently accepted and lost
at the next refresh. The goal of this step is that **a write to a predicate behaves the same
way regardless of how it currently happens to be backed**, and that both surfaces mean the same
thing as the Swift API: `INSERT` asserts a base fact (1a), `DELETE` retracts one (1b), `UPDATE`
is refused (1c).

> **Ordering note.** 1b depends on Step 2 (`superceded_by` must exist before a `DELETE` can
> supersede). Either land Step 2 first, or ship 1b with the `RAISE(ABORT)` placeholder from 1c
> and convert it when Step 2 lands.

**1a. `INSERT` — divert to `_rule`, don't raise.** `INSERT INTO parent(…)` against the *view*
routes through the `INSTEAD OF INSERT` trigger and `predicate_formula`, asserting a base fact
into `_rule` (`RBDB.swift:266-280`). Against a *materialized* table the same statement inserts
a temp row that vanishes at the next rebuild. So whether an insert persists depends on whether
that predicate happens to be materialized at that moment — invisible to the caller. Raising
would only make that visible; the fix is to make the two paths *identical*.

SQLite has no `INSTEAD OF` trigger for tables, but `RAISE(IGNORE)` in a `BEFORE INSERT`
trigger is the equivalent: it abandons the remainder of the trigger program and skips the row
without aborting the statement. So the materialized table gets the *same trigger body as the
view*, plus a guard and a trailing `RAISE(IGNORE)`:

```sql
CREATE TEMP TRIGGER IF NOT EXISTS [<name>_insert_trigger]
BEFORE INSERT ON [<name>]
WHEN (SELECT COUNT(*) FROM _materializing) = 0
BEGIN
  -- byte-identical to the view trigger's body: assert a base fact
  INSERT INTO _entity (internal_entity_id) VALUES (NULL);
  INSERT INTO _rule (internal_entity_id, formula)
  VALUES (last_insert_rowid(), jsonb(predicate_formula('<name>', NEW.[c1], …)))
  ON CONFLICT (formula) WHERE superceded_by IS NULL DO NOTHING;
  SELECT RAISE(IGNORE);        -- must be last: it abandons the rest of the program
END;
```

The row is not written to the temp table, but that is correct rather than lossy: the `_rule`
insert fires `_invalidate_on_rule_insert`, which flags the closure dirty, and the next query's
`refreshDirtyMaterializations` re-seeds from `baseFactsSelect` and re-runs the fixpoint —
picking up the new base fact *and* anything newly derivable from it. Same trigger name as the
view's, which is safe because `dropIfView` (`IterativeEvaluator.swift:80`) has already removed
the view and its trigger before the temp table takes the name.

**Factor the shared body out.** The `INSERT INTO _entity` / `INSERT INTO _rule` pair is
exactly `sqlForInsert` (`RBDB.swift:163-170`) as already inlined by `createViewAndTrigger`
(`RBDB.swift:271-280`). Both call sites should build it from one helper taking
`(tableName, columns)` and differing only in the wrapper — `INSTEAD OF INSERT ON <view>` vs
`BEFORE INSERT ON <table> WHEN … / RAISE(IGNORE)`. The whole point is that the two paths behave
identically, so they should share the code that makes them.

**The `_materializing` guard is not optional.** The fixpoint's own
`INSERT OR IGNORE INTO [<name>]` (`IterativeEvaluator.swift:93`, `:112`) must not be diverted —
that would turn derived rows into asserted base facts and prevent the loop from settling. This
is not hypothetical: it is precisely the disaster already documented at
`IterativeEvaluator.swift:71-79`, where a *shadowing view* causes the fixpoint's inserts to hit
a trigger and "the loop spins forever." We are deliberately installing a trigger on that path,
so the guard is what keeps it correct.

**No table is needed** — register a SQL function instead, exactly as `predicate_formula` is
registered (`RBDB.swift:24-33`), backed by a depth counter on the `RBDB` instance:

```sql
… BEFORE INSERT ON [<name>] WHEN NOT is_materializing() …
```

```swift
private var materializingDepth = 0   // guarded by `defer`, incremented around each build
```

The counter is per-`RBDB`, i.e. per-connection, which is exactly the scope a temp table would
have given. It must be a **counter, not a boolean**, and managed with `defer`: `materialize`
must clear it on the throwing path (`IterativeEvaluator.swift:125-131`), and it calls
`query(sql:)` internally (`:94`, `:122`), which reaches `refreshDirtyMaterializations` — a
nested build is possible, and a boolean would be cleared by the inner one while the outer is
still running.

Register it **without** `SQLITE_DETERMINISTIC` (unlike `predicate_formula`), or SQLite may
cache the value across the fixpoint's statements.

If a registered function is unwanted, the table equivalent needs only one column and no
seeding: `CREATE TEMP TABLE _materializing (id INTEGER PRIMARY KEY)`, one row inserted per
build (deleted by captured rowid on exit), with row *count* serving as the depth — the guard
is then `WHEN (SELECT COUNT(*) FROM _materializing) = 0`. Either way there is no second
column: depth is the only state, and it lives better in Swift where `defer` already manages
it.

**Two hazards worth recording**, neither currently triggerable but both cheap to get wrong
later:

- `RAISE(IGNORE)` must be the trigger's *last* statement, since it abandons the remainder.
- The inner `_invalidate_on_rule_insert` ends with a schema-changing `DROP VIEW`, which
  `schema.sql:83-85` notes must be *its* last statement. Nesting a schema change inside an
  outer trigger that still has statements to run is the kind of thing that aborts with
  `SQLITE_ABORT_ROLLBACK`. Safe today only because that `DROP` is guarded by
  `negative_literal_count > 0` and this path can only produce base facts (count 0).

**1b. `DELETE` → retract the matching base fact.** The view form currently refuses this — it
carries only an `INSTEAD OF INSERT` trigger, so SQLite rejects a `DELETE` with "cannot modify a
view" — and the materialized form silently accepts it and loses the change. Neither is what a
caller means. Give *both* an `INSTEAD OF DELETE` / `BEFORE DELETE` trigger that supersedes the
matching row in `_rule`, i.e. does exactly what `retract(formula:)` does (§2.4):

```sql
CREATE TEMP TRIGGER IF NOT EXISTS [p_delete_trigger]
INSTEAD OF DELETE ON [p] FOR EACH ROW
BEGIN
  -- Retraction operates on the base, not the closure (§2.4). A row with no live base fact
  --  behind it came from a rule arm of the view's UNION and cannot be retracted — say so
  --  rather than no-op'ing, which is what made this look unworkable in an earlier draft.
  SELECT RAISE(ABORT, 'p(' || OLD.[c1] || … || ') is derived, not asserted — retract what it follows from')
  WHERE NOT EXISTS (
    SELECT 1 FROM _rule WHERE superceded_by IS NULL
      AND formula = jsonb(predicate_formula('p', OLD.[c1], …)));

  INSERT INTO _entity (internal_entity_id) VALUES (NULL);   -- the retraction act
  UPDATE _rule SET superceded_by = last_insert_rowid()
  WHERE superceded_by IS NULL
    AND formula = jsonb(predicate_formula('p', OLD.[c1], …));
END;
```

The materialized-table form is the same body wrapped in `BEFORE DELETE … / SELECT RAISE(IGNORE)`
— the mirror of 1a, and likewise built from the shared helper.

Notes:

- **Check-then-act, not `changes()`.** Testing `changes() = 0` after the `UPDATE` would be
  terser, but `changes()` semantics inside a trigger body are worth not relying on. The
  `NOT EXISTS` probe is a unique-index hit on `idx_rule_live_formula`, so the double evaluation
  is cheap.
- **No new equality assumption.** `formula = jsonb(predicate_formula(…))` is the same BLOB
  comparison that `ON CONFLICT (formula)` already relies on for insert dedup (§2.1). If one
  works the other does.
- **Multi-row `DELETE` is all-or-nothing.** The trigger is `FOR EACH ROW`; if any matched row is
  derived, the `ABORT` rolls back the whole statement including supersessions already applied.
  That is the standard SQL expectation.
- **The one genuine surprise:** a row that is *both* asserted and derivable stays visible after
  a successful `DELETE` — the assertion was retracted, but the conclusion still follows. This is
  not a wart specific to `DELETE`; it is §2.4's "retraction operates on the base, not the
  closure" showing through the SQL surface, and it needs documenting in exactly those terms.
- **No DDL under the statement.** Superseding fires `_invalidate_on_rule_supersede` (§2.3),
  whose `DROP VIEW` arm is guarded by `negative_literal_count > 0`; base facts are 0, so the
  trigger only writes `_dirty`. The rebuild happens at the next `query()`.
- `retract(formula:)` and this trigger should share the supersession SQL — they are the same
  operation reached from two surfaces.

**1c. `UPDATE` → `RAISE(ABORT)`.** No retract-and-assert pair has an obvious atomic reading, and
views reject `UPDATE` natively for want of an `INSTEAD OF UPDATE` trigger, so the materialized
form matches with a better message:

```sql
CREATE TEMP TRIGGER IF NOT EXISTS [<name>_no_update] BEFORE UPDATE ON [<name>]
BEGIN SELECT RAISE(ABORT, 'predicate <name> is materialized; delete and re-insert instead'); END;
```

Free internally: the fixpoint only ever runs `INSERT OR IGNORE`
(`IterativeEvaluator.swift:112`, `:93`), and `invalidateMaterialization`
(`IterativeEvaluator.swift:187-193`) uses `DROP TABLE`, which does not fire row triggers.

**Tests.** The spine of this step is one **alignment table**: for each of `INSERT`, `DELETE`,
`UPDATE`, run the statement against a view-backed predicate and against a materialized one and
assert the *same* observable outcome — same `_rule` state, same query results, same error.
Parameterize it rather than writing six tests; the whole point is that the two columns agree.

Then, per operation:

- **`INSERT`** — the fact reaches `_rule`, and after the next query it is visible in the closure
  along with anything derivable from it (proving the divert-then-rebuild round trip, not just
  the write).
- **`DELETE`** of an asserted fact supersedes it and it disappears from queries; of a *derived*
  row raises, naming the row; of a row that is *both* asserted and derived succeeds and the row
  **remains visible** — the §2.4 semantics surfacing, and the case most likely to be "fixed" by
  someone who misreads it, so assert it deliberately; a multi-row `DELETE` where one row is
  derived leaves *nothing* superseded (all-or-nothing).
- **`UPDATE`** raises on both paths.
- **Guard** (1a): a fixpoint build completes with the trigger installed and writes nothing to
  `_rule`; the guard survives a nested `materialize` and is cleared when a build throws.
- **Equivalence with the Swift API:** `DELETE FROM p WHERE …` and `retract(datalog: "p(…).")`
  leave byte-identical `_rule` state, including which entity `superceded_by` points at being a
  bare retraction act in both cases.

---

## Step 2 — `superceded_by` and `retract()`

### 2.1 Schema (`schema.sql`)

`_rule` gains one column, and the table-level `UNIQUE ON CONFLICT IGNORE` on `formula` moves
to a **partial** unique index over live rows:

```sql
CREATE TABLE IF NOT EXISTS _rule (
    internal_entity_id INTEGER PRIMARY KEY REFERENCES _entity,
    formula BLOB NOT NULL,                          -- JSONB; no longer UNIQUE (see index below)
    -- NULL ⟹ live: part of the believed set. Otherwise the entity whose assertion superseded
    --  this row — either another `_rule` row (a more general rule that subsumes it, Step 3) or
    --  a bare `_entity` standing for an explicit retraction act. `_entity.entity_id` is a
    --  uuidv7, so both the assert time (this row's own entity) and the supersede time (the
    --  target's) are recoverable without storing a timestamp.
    superceded_by INTEGER NULL REFERENCES _entity,
    output_type TEXT GENERATED ALWAYS AS (formula->>0) VIRTUAL COLLATE NOCASE,
    arg1_constant ANY GENERATED ALWAYS AS (json_extract(formula, '$[1][0].""')) VIRTUAL,
    arg2_constant ANY GENERATED ALWAYS AS (json_extract(formula, '$[1][1].""')) VIRTUAL,
    negative_literal_count INT GENERATED ALWAYS AS (…unchanged…) VIRTUAL
) STRICT;

-- Re-asserting a live fact or rule stays a no-op; a *superseded* row no longer blocks
--  re-assertion, so assert → retract → re-assert produces two rows and a real history.
CREATE UNIQUE INDEX IF NOT EXISTS idx_rule_live_formula
    ON _rule(formula) WHERE superceded_by IS NULL;

-- The two existing lookup indexes gain the same predicate so they stay usable under the
--  `superceded_by IS NULL` filter every read site now carries.
CREATE INDEX IF NOT EXISTS idx_rule_ot_nlc_arg1_arg2
    ON _rule(output_type COLLATE NOCASE, negative_literal_count, arg1_constant, arg2_constant)
    WHERE superceded_by IS NULL;
CREATE INDEX IF NOT EXISTS idx_rule_ot_nlc_arg2_arg1
    ON _rule(output_type COLLATE NOCASE, negative_literal_count, arg2_constant, arg1_constant)
    WHERE superceded_by IS NULL;
```

A `CREATE UNIQUE INDEX` cannot carry an `ON CONFLICT` clause, so `sqlForInsert`
(`RBDB.swift:163-170`) takes the conflict target explicitly — a partial index is a legal
upsert target as long as its `WHERE` is repeated:

```sql
INSERT INTO _rule (internal_entity_id, formula)
VALUES (last_insert_rowid(), jsonb(?))
ON CONFLICT (formula) WHERE superceded_by IS NULL DO NOTHING
```

*Pre-existing wart, unchanged:* the `_entity` row is inserted first (to supply
`last_insert_rowid()`), so a no-op insert leaves an orphan `_entity`. True today with
`ON CONFLICT IGNORE`; not worth fixing here.

### 2.2 Read sites

Every read of `_rule` gains `AND superceded_by IS NULL`. The surface is small — verify with
`grep -n 'FROM _rule' -r Sources`:

- `baseFactsSelect` (`RBDB.swift:223-235`) — the `WHERE` at `:232-234`
- `fetchRules` (`RBDB.swift:364-384`) — the `WHERE` at `:369-370`
- the subsumption scan in `canonicalizeRuleForAssert` reaches `_rule` only through
  `fetchRules`, so it inherits the filter (Step 3 changes what it *writes*)

### 2.3 Invalidation — the correctness risk

The existing temp trigger `_invalidate_on_rule_insert` (`schema.sql:67-90`) fires `AFTER
INSERT` and its whole design leans on monotonicity, stated explicitly at `schema.sql:71-74`
and again at `IterativeEvaluator.swift:145-147`: *re-running the fixpoint over the existing
rows only adds newly-derivable facts, so no drop-and-rebuild is needed.*

**Retraction breaks that.** Re-iterating a materialized closure will never *remove* rows that
are no longer derivable. A supersession must therefore drop and rebuild the closure, not
refresh it — and it cannot do the drop inside the trigger (the on-disk table lock described
at `schema.sql:71-73`). So:

**`_dirty` gains a stickiness bit** (`schema.sql:55`):

```sql
CREATE TEMP TABLE IF NOT EXISTS _dirty (name TEXT PRIMARY KEY, rebuild INT NOT NULL DEFAULT 0);
```

The existing insert trigger keeps `INSERT OR IGNORE INTO _dirty (name)` — `rebuild` defaults
to 0 and an existing `rebuild = 1` is left alone. The new trigger uses `INSERT OR REPLACE …
1`. The two conflict clauses give the right lattice for free: **`rebuild` is sticky**, so an
additive change arriving after a destructive one cannot downgrade the pending rebuild.

**New trigger**, the `UPDATE` twin of the insert one:

```sql
CREATE TEMP TRIGGER IF NOT EXISTS _invalidate_on_rule_supersede
AFTER UPDATE OF superceded_by ON main._rule
WHEN OLD.superceded_by IS NULL AND NEW.superceded_by IS NOT NULL
BEGIN
  INSERT OR REPLACE INTO _dirty (name, rebuild)
  SELECT materialized_top, 1 FROM _materialized_dep
  WHERE depends_on = substr(NEW.output_type, 2);

  -- Same view-drop as the insert trigger, and same guard: base facts are read *live* through
  --  `baseFactsSelect` inside the view body, so retracting a fact needs no view drop; only a
  --  retracted *rule* changes a view's definition. Must be the trigger's last statement (it
  --  changes the schema) — see the insert trigger's comment.
  SELECT sql_exec('DROP VIEW temp.[' || s.name || ']')
  FROM temp.sqlite_schema s
  WHERE s.type = 'view' AND s.name = substr(NEW.output_type, 2)
    AND NEW.negative_literal_count > 0;
END;
```

**`refreshDirtyMaterializations`** (`IterativeEvaluator.swift:148-168`) reads `rebuild`
alongside `name`. For `rebuild = 1` it calls `invalidateMaterialization(name)` (drop the temp
table, clear `_materialized_dep`) and does **not** re-materialize: the next query fails on the
missing table and `rescue` rebuilds it from current data. That is both the cheapest correct
option and the same recovery path the build-failure handler already uses
(`IterativeEvaluator.swift:125-131`).

The monotonicity comments at `schema.sql:71-74`, `IterativeEvaluator.swift:40-44` and
`:143-147` all need amending — they are now the *additive* half of a two-mode invalidation.

### 2.4 API

```swift
public func retract(formula: Formula) throws
public func retract(datalog: String) throws

public enum RetractionError: Error {
    case notFound(Formula)      // no live row with this canonical form
}
```

Canonicalize (so the JSONB is the identity key, exactly as `assert` does), then in one
transaction: insert a bare `_entity` for the retraction act, `UPDATE _rule SET superceded_by =
<that id> WHERE formula = jsonb(?) AND superceded_by IS NULL`, and throw `.notFound` +
rollback if `changes() = 0`.

**Retraction operates on the base, not the closure.** Retracting a formula that is *derivable
but not stored* is `.notFound`, not a silent success — a derived conclusion is retracted by
removing what derives it. Documented, tested, and deliberate.

### 2.5 Tests

Retract a fact → gone from queries, row still present with `superceded_by` set; retract a rule
→ its conclusions gone, view rebuilt; retract → re-assert → **two** rows, one superseded one
live, and the fact is derivable again (this is the case the old `UNIQUE` forbade); retract a
non-existent and a derivable-but-not-stored formula → `.notFound`; retract through the
canonical form (assert `q :- p, X < Y` and retract the commuted-guard spelling of the same
rule) → matches; retract a fact feeding a *materialized* recursive closure → the closure is
dropped and rebuilt, and the derived rows are gone (the monotonicity regression — this is the
test that fails loudly without §2.3); `rebuild` stickiness (retract then assert, in the same
refresh window → still a full rebuild).

---

## Step 3 — Canonicalization deletes become supersessions

`canonicalizeRuleForAssert` (`RuleCanonicalization.swift:50-53`) currently hard-`DELETE`s
stored rules that the incoming rule subsumes. Under an immutable record that becomes
`UPDATE _rule SET superceded_by = <incoming rule's entity>` — "made redundant by", the only
case where `superceded_by` points at a `_rule` row rather than a bare retraction `_entity`, so
the two reasons a row left the believed set stay distinguishable by a join.

**Ordering problem to resolve.** Canonicalization runs *before* the insert
(`RBDB.swift:101-107`), so the incoming rule's entity id does not exist yet. Restructure so
`canonicalizeRuleForAssert` returns the rules to supersede rather than acting on them, and
`assert` performs the supersession after the insert, using the new row's id. Two edge cases:
if canonicalization returns `nil` (tautology, or incoming already subsumed) there is nothing
to supersede; and if the insert was a no-op via `ON CONFLICT DO NOTHING`, there is no new
entity to point at and nothing should be superseded.

**Tests:** subsumption still leaves exactly one live rule, with the subsumed one present and
pointing at the subsumer; order-independence (assert in both orders → same *live* set);
asserting a duplicate does not supersede anything.

---

## Step 4 — Strong negation

Storing `-p(…)` — "known false" — as distinct from retraction's "no longer known true".

**This gives three-valued answers directly**, with no separate mechanism: `p(a)` derivable ⟹
*true*, `-p(a)` derivable ⟹ *false*, neither ⟹ *unknown*. Under the open world that is the
honest reading, and it is why strong negation earns its place rather than being a decoration
on retraction. What remains deferred is only the *result representation* — surfacing "unknown"
as a distinct answer value at the query API instead of leaving the caller to observe absence
from both relations.

`-p` is permitted in **rule heads**, not just as stored ground facts — the §4.1 encoding
supports it unchanged (an `output_type` of `@-p` with `negative_literal_count > 0` is an
ordinary rule that `fetchRules(for: "-p")` finds and `createViewAndTrigger` backs with a
view). This is what lets the closed-world assumption be *stated in the logic* rather than
declared out of band:

```prolog
-q(X, Y) :- dom(X), dom(Y), not q(X, Y).     % CWA for q, over the domain `dom`
```

— the standard CWA axiom of extended logic programs, and the reason PLAN-EVENTS needs no
`closed` declaration on `_predicate` (see its §2.0 for why that is preferable, and for the
range-restriction requirement that makes the `dom` literals mandatory).

**Restriction (§4.2.1): a negative head requires the matching negated subgoal in the body.**
`-foo(t̄)` may appear as a head only if the body contains `not foo(t̄)` with the *same*
argument terms. This is what keeps rules from introducing contradictions the ground-fact check
in §4.2 cannot see, and — combined with the stratification check — it is a real guarantee
rather than a heuristic: when `-foo(ā)` is derived, `not foo(ā)` held, so `foo(ā)` was not
derivable; and `foo` cannot depend back on `-foo`, because that is a cycle through negation
and the stratification check (PLAN-EVENTS §2.2) rejects it. So `foo(ā)` and `-foo(ā)` cannot
both be derivable.

Two consequences worth being explicit about:

- **It needs `not`.** Until PLAN-EVENTS Phase 2 lands there is no way to satisfy the
  restriction, so this step ships negative **facts** only, plus the head encoding and the
  validation rule. The first legal negative-headed rule is the CWA axiom above.
- **It rules out `-alive(X) :- dead(X).`** — you must write
  `-alive(X) :- dead(X), not alive(X).` Arguably better (it will not derive a contradiction
  from someone having asserted both `alive(x)` and `dead(x)`), but it *suppresses* that
  conflict rather than reporting it. The restriction is a "for now": it can be relaxed once
  there is genuine contradiction detection over derived atoms, which is what PLAN-EVENTS
  Phase 5's integrity constraints provide.

### 4.1 Encoding: polarity in the formula, not a column

`p(1)` and `-p(1)` must be storable *simultaneously* — that is the contradiction we want to
**detect**, not something the unique index should prevent. So polarity belongs in the head
symbol, where `output_type` (`formula->>0`) picks it up for free and the existing indexes keep
working.

`SymbolType.hornClause(positiveName:)` (`SymbolType.swift:15,52,56`) gains a polarity payload;
`stringValue` renders `"@-\(name)"` when negated. **`@-name`, not `-@name`**, because that
keeps three things working untouched:

- `negative_literal_count`'s `output_type LIKE '@%'` guard (`schema.sql:35`);
- `SymbolType.init?(stringValue:)`'s `first == "@"` dispatch (`SymbolType.swift:56`);
- `baseFactsSelect`'s `output_type = '@name'` (`RBDB.swift:232`) and `fetchRules`'
  (`RBDB.swift:370`) — both naturally exclude negative facts with no change.

The cost is that `substr(NEW.output_type, 2)` in the invalidation triggers (`schema.sql:77`,
`:88`, and Step 2's new trigger) yields `-name`, matching no view or materialization named
`p`. That is correct, not merely harmless: `p` and `-p` are separate relations, so asserting
into one does not change what the other derives. `-p`'s *own* view is invalidated normally,
since it is named `-p`.

`getColumns` (`RBDB.swift:284-322`) strips a leading `-` so `-p` resolves to `p`'s declared
columns — a negative predicate is not separately declared.

**And must not be.** `interceptCreateTable` (`RBDB.swift:172-217`) rejects a table name
beginning with `-`, with a message pointing at the positive form:

```
cannot declare '-p': negative predicates are implicit — declare 'p' and its
negation follows, sharing its columns
```

Without this, `CREATE TABLE [-p](…)` would write a second `_predicate` row that `getColumns`
never consults (it strips the `-` first), so the declared columns would be silently ignored and
`-p` would take `p`'s — or, if `p` were never declared, `-p` would resolve to nothing despite
appearing to exist. Rejecting at declaration time is much cheaper than either failure. This
belongs with the other reserved-name rejections in PLAN-EVENTS' cross-cutting notes.

### 4.2 Coherence: refuse the contradiction, don't repair it

**Asserting a base fact whose inverse is queryable throws.** Asserting `-p(c₁,…,cₙ)` runs the
ordinary query `?- p(c₁,…,cₙ)`; if it returns a row, the assert fails with
`.contradiction`. Symmetrically for asserting `p(…)` against `-p(…)`.

An earlier draft had this *auto-retract* the inverse instead — the Levi identity performed by
the engine. Rejected, for two reasons:

- **It only works for stored facts.** A derivable-but-not-stored inverse has nothing to
  supersede, so auto-retraction would have to throw in that case anyway — exposing the
  base/derived distinction at the assert site, which is not a distinction the caller should
  have to reason about. Querying the inverse is uniform: it sees stored and derived alike, and
  behaves the same either way.
- **Silent destruction.** Asserting `-p(1)` quietly retiring `p(1)` makes an assert
  destructive. For an immutable record, revision should be two recorded acts, not one that
  hides half of itself.

The Levi identity is still the right description of what revision *is* — it just isn't
automated. To revise, the caller does the contraction explicitly:

```swift
try db.retract(datalog: "p(1).")     // contraction  — now unknown
try db.assert(datalog: "-p(1).")     // expansion    — now known false
```

**Cost and its short-circuit.** This puts a query on the fact-assert path. Guard it with an
indexed existence probe first: if no live row has the complementary `output_type` at all
(`@-p` vs `@p`), there is nothing that could derive the inverse, so skip the query. That is one
index hit on `idx_rule_ot_nlc_*` in the overwhelmingly common case.

**Citing the culprit.** Where the inverse is a *stored* fact, name it: the query gives the
bindings and a `_rule` lookup by canonical JSON gives the row, so the error can carry the exact
formula to retract. Where it is *derived*, report the predicate and bindings but not the
derivation — a proof trace is provenance the SQL engine discards, and building one is out of
scope here. `storedAs: nil` is therefore a placeholder for "we know it follows, but not from
what", and **PLAN-EVENTS Phase 5's abduction engine is what fills it in**: its top-down SLD
search produces exactly the derivation this error is missing, so when that lands, the derived
case should cite the base facts and rules the inverse rests on — the set you would have to
retract to make the assert succeed. See that plan's §5.2.

```swift
public enum CoherenceError: Error {
    case contradiction(asserted: Formula, inverse: Formula, storedAs: Formula?)  // nil ⟹ derived
}
```

#### 4.2.2 Covering the SQL surface: query the inverse *relation*

`INSERT INTO p(…)` reaches `_rule` through the view/table trigger (Step 1a), not through Swift
`assert`, so the §4.2 check does not run. The fix is for that trigger to make the *same* test —
query the inverse relation — so both paths ask one question, differing only in which language
they ask it in.

**Placement: per-predicate, not on `_rule`.** A single trigger on `_rule` would be the true
chokepoint, but it cannot do this: it is generic over every predicate and sees `NEW.output_type`
as a *value*, and SQL has no dynamic table names — there is no way to get from that to
`FROM [-p]`. So the check goes in `createViewAndTrigger` (`RBDB.swift:237-281`), which emits one
trigger per predicate and therefore knows the name statically:

```sql
CREATE TEMP TRIGGER IF NOT EXISTS [p_insert_trigger]
INSTEAD OF INSERT ON [p] FOR EACH ROW
-- Short-circuit: if nothing negative about `p` exists at all, skip the check (and skip
--  forcing `[-p]` into existence). Indexed on output_type.
WHEN EXISTS (SELECT 1 FROM _rule WHERE output_type = '@-p' AND superceded_by IS NULL)
BEGIN
  SELECT RAISE(ABORT, 'contradiction: -p of these arguments is already derivable')
  WHERE EXISTS (SELECT 1 FROM [-p] WHERE [c1] IS NEW.[c1] AND …);
  … the existing assert-a-base-fact body …
END;
```

and symmetrically, `-p`'s own view gets the mirror check against `[p]`. Since the body is
generated by the shared helper from Step 1a, the materialized-table form gets it too.

**`[-p]` not existing is the mechanism, not a problem.** If the view is absent the statement
fails to prepare with `no such table: -p`, which `rescue` (`RBDB.swift:324-362`) matches,
resolves via `getColumns` (stripping the `-`, per §4.1), builds the view for, and retries. The
view creation happens *outside* the failed statement — nothing is in flight — so this is
categorically unlike the UDF case below: no DDL under an active statement, no re-entrancy.
Recursive inverses route to `materialize` the same way any other predicate does.

Crucially this covers **derived** inverses, not just stored ones, because a view is
`baseFactsSelect UNION rules`. One check, both cases, every path.

**Three things to verify before building it**, each of which decides whether this lands:

1. **`assert` bypasses `rescue`.** It uses `super.query` (`RBDB.swift:102-107`), i.e.
   `SQLiteDatabase.query`, which has no rescue logic — only the `RBDB.query` override
   (`:74-91`) does. If `assert` is ever routed through the view trigger this must change, and
   the change has knock-on effects (`self.query` calls `refreshDirtyMaterializations` first,
   which would run a fixpoint inside `assert`'s transaction).
2. **Does `CREATE TRIGGER` resolve table names in the body at creation time?** If SQLite
   validates eagerly rather than when the trigger fires, `[-p]` must exist before `p`'s trigger
   can be created, and the rescue-on-demand flow does not work as described.
3. **Reading `_rule`-derived views from a `BEFORE`/`INSTEAD OF` trigger on a relation over
   `_rule`.** Read-only, so it should be well-defined, but confirm — SQLite only documents
   *modifying* the triggering table as undefined.

**Cost.** An insert can now force view creation or a full materialization of the inverse. The
`WHEN` short-circuit keeps the common case (nothing negative about `p` exists) to one indexed
probe. Also note a materialized inverse is refreshed at `query()` entry, not inside triggers, so
within a multi-statement batch the check can read a stale closure.

**Why not `assert_formula` — replacing `predicate_formula` with a UDF that calls
`assert(formula:)`.** Tempting for the same "one code path" reason, but a UDF running the full
assert re-enters the connection *mid-statement* and hits three walls: `assert` opens a
transaction (`RBDB.swift:97`) and `BEGIN` inside an active statement fails; its coherence check
reaches `refreshDirtyMaterializations` and `rescue` and therefore **DDL** — the exact
`SQLITE_ABORT_ROLLBACK` hazard that forces `sql_exec`'s DROP to be its trigger's last statement
(`schema.sql:83-85`); and that path can run an unbounded fixpoint inside a UDF callback.
`sql_exec` (`SQLiteDatabase.swift:141-179`) gets away with re-entrancy because it runs one DDL
statement under a documented ordering constraint, not an arbitrary code path. The trigger above
achieves the same unification without any of that, because the work happens *between*
statements rather than inside one.

**The remaining divergence is small.** For a base fact arriving via SQL, everything else
`assert` does is already satisfied or vacuous: the predicate must exist (you inserted into its
view), unsafe variables are impossible (`predicate_formula` accepts only constants, rejecting
NULL and BLOB — `SQLiteFunctions.swift:60-85`), and canonicalization returns facts untouched
(`RuleCanonicalization.swift:17-21`). Coherence was the only gap.

### 4.3 Datalog surface

```prolog
-p(1).                % assert "p(1) is known false"
?- -p(X).             % query what is known false
```

Parser: a leading `-` before a predication in assertion, rule-head, and goal position.
Disambiguation from negative number literals is by lookahead (an identifier followed by `(`
vs a digit). Printer round-trips the form. This is ASP's `-p` spelling, and deliberately *not*
`not p`, which is reserved for negation-as-failure (PLAN-EVENTS Phase 2).

### 4.4 Tests

JSON snapshot pinning the `@-name` head encoding; parser round-trip for assertion, rule-head,
and goal position, and that `-5` still parses as a number; `p(1)` and `-q(1)` both storable and
each queryable in its own relation; `-p` inherits `p`'s declared columns, and a negative fact
never leaks into `p`'s view or `fetchRules`; `CREATE TABLE [-p](…)` is rejected with the message
naming the positive form, and rejected *before* writing a `_predicate` row (assert against
`-p` afterward to prove nothing was left half-declared).

**Coherence (§4.2):** asserting `-p(1)` over a live `p(1)` throws `.contradiction` and stores
*nothing* — `p(1)` is still live afterward (the no-silent-repair test); the symmetric
direction; the inverse reached *through a rule* (`p(X) :- q(X).` and `q(1).`, then assert
`-p(1)`) throws the same way, with `storedAs == nil` where the direct case carries the stored
formula; retract-then-assert is the supported revision path and succeeds; a fact whose
complementary `output_type` has no live rows at all takes the short-circuit and issues no
query (assert on a hot path stays cheap).

**SQL surface (§4.2.2):** `INSERT INTO p(…)` raises where the inverse is a live stored fact
*and* where it is only derivable through a rule — the point of querying the relation rather
than probing `_rule`; both via the view and via a materialized table (Step 1a); the trigger
fires on matching arguments only (`p(1)` with `-p(2)` stored inserts fine); a *superseded*
inverse does not block, so retract-then-assert works through this path too; the check survives
`[-p]` not yet existing (the rescue round-trip — assert into a fresh connection where no view
has been built); and the `WHEN` short-circuit means a predicate with nothing negative asserted
about it never forces `[-p]` into existence.

**Restriction (§4.2.1):** `-p(X) :- q(X).` is rejected; `-p(X) :- q(X), not p(X).` is accepted.
Since `not` does not exist until PLAN-EVENTS Phase 2, the acceptance half is a pending test
there — as is the CWA axiom end-to-end. What is testable now is the rejection, and that the
encoding round-trips a negative-headed rule through storage and `fetchRules`.

---

## Limitations after this work

- **No resurrection.** Retracting a general rule does not bring back the specific rules it
  superseded. Correct under a foundational reading of belief revision, surprising under a
  coherence one; the `_rule`-target vs `_entity`-target distinction in `superceded_by` keeps a
  future resurrection policy possible.
- **Retraction is on the base, not the closure** (§2.4), and this is most visible through the
  SQL surface: `DELETE` raises on a purely derived row, and on a row that is *both* asserted and
  derived it succeeds while the row stays visible. Correct — the assertion was retracted, the
  conclusion still follows — but it violates the reflex that `DELETE` makes rows go away, so it
  needs saying in the docs, not just the tests (§1b).
- **Contradiction detection is at assert time, on ground facts** — by querying the inverse
  relation, in Swift on the `assert` path (§4.2) and in the per-predicate trigger on the SQL
  path (§4.2.2), so both catch stored *and* derived inverses. Neither can detect a
  contradiction that only becomes derivable *later*, when a subsequent rule or fact makes the
  inverse reachable. Negative rule heads are kept safe by the §4.2.1 restriction instead of by
  checking; lifting that restriction requires real integrity constraints (PLAN-EVENTS Phase 5).
- **Contradictions arising from a rule are suppressed, not reported** — the §4.2.1 restriction
  means a negative-headed rule simply does not fire where its positive counterpart holds.
- **No group retraction** — retracting a whole scenario at once is PLAN-EVENTS §4.2's
  `group_name`, unchanged by this plan and still pending.
- **"Unknown" is not surfaced as an answer value.** The three-valued *semantics* is here
  (§4); what is deferred is a query API that reports it, rather than the caller inferring it
  from absence in both `p` and `-p`.
- **Stale `.db` files do not open.** Recreate them.
