# Plan: Hybrid recursion evaluation + assert-time canonicalization

Status: ✅ Implemented in 799b62d4ae791d130e7173577b16f3c007dfff3d

## Motivation

The query-time recursive-CTE builder (`RecursiveClosure.swift`) is elegant for the case it
was designed for — **linear, possibly-infinite, value-generating recursion** (`nat(N+1) :-
nat(N)`, `square(X, X*X) :- nat(X)`) — where it *streams* rows lazily out of a `WITH
RECURSIVE` CTE and uses constraint propagation to bound otherwise-infinite recursion.

But SQLite's recursive CTE is **linear only** (one self-reference per arm, never inside a
subquery). Supporting *mutual* recursion on top of it required a combined tag-column CTE
(`ensureGroupCTE`, SCC detection) and SQL string surgery (`replaceTableReference`), and it
still can't express *non-linear* recursion (a rule joining two members of the same cycle, e.g.
`son(A,B) :- child(A,B), male(A)`) — that's a hard SQLite limit, not a shape we can rework.

Meanwhile, the rules that hit this wall are **relational and finite** (they only shuffle
existing constants; the active domain is finite). Finite recursion has a standard, simple
evaluation strategy that handles linear, mutual, and non-linear recursion uniformly:
**iterative fixpoint (semi-naive) materialization**.

## Core idea: route by shape, don't replace wholesale

Two strategies, each used for what it's good at:

| Recursion shape | Strategy | Why |
|---|---|---|
| Value-generating / infinite (`nat`, `square`, `inc`) | **Recursive CTE** (existing) | Only this can *stream* an infinite relation; a fixpoint over an infinite set never terminates. Always linear anyway. |
| Finite / relational (family predicates) — incl. mutual & non-linear | **Iterative materialization** (new) | Terminates (finite domain); handles mutual/non-linear natively because each step is a plain non-recursive join. |

**This is deliberately a hybrid, not a replacement.** Iterative evaluation *cannot* stream and
*cannot* handle infinite/value-generating recursion (that capability — `ruleOverRecursivePredicate`
paging `square(X,Y)`, `recursiveDoubling` reaching 1e9 in ~30 lazy steps — is unique to the
lazy CTE). Trying to fully replace the CTE would lose it. So the CTE path stays for exactly the
predicates that need it; the messy *mutual* machinery is what gets deleted.

## Detection / routing

At query time (in `rescue`, when a predicate view is missing and `involvesRecursion` is true),
compute the **dependency cone** of the queried predicate `P` (P plus everything transitively
referenced through rule bodies). Then:

- `doesArithmetic(R)` ≔ rule `R` contains any `.expression` (add/multiply/exponent over a
  variable) **anywhere — head *or* body**. Value generation is not visible from the head alone:
  `nat(X) :- nat(X - 1)` has a bare-variable head and generates values by *inverting* the body
  expression (`X = [nat].n + 1`). So we must scan the whole clause, not just head arguments.
- `valueGenerating(Q)` ≔ `involvesRecursion(Q)` **and** some rule `R` of `Q` with
  `doesArithmetic(R)`. Arithmetic on a recursion-carrying rule is the marker of a relation that
  can escape the finite active domain (`nat`, `square`, `inc`).
- If **any** predicate in `cone(P)` is `valueGenerating` → route the whole thing to the
  **CTE path** (`buildRecursiveClosureCTE`, now mutual-free). Matches today's `square`/`inc2`
  behavior (P inlined/streamed over the infinite dependency). If that value-generating recursion
  is also mutual/non-linear (arithmetic feeding a cycle) it stays unsupported → clear error.
- Otherwise (purely relational recursion — mutual, non-linear, or plain) → **iterative
  evaluator**. Finite, so it terminates.

Family predicates do no arithmetic → finite → iterative (this is where mutual/non-linear runs).
`nat` / `square` / `inc` / `inc2` do arithmetic under recursion → CTE. No behavior change for
the value-generating tests. Routing to the CTE is safe even for a *finite* arithmetic-under-
recursion predicate: the CTE's `UNION` dedup terminates on finite relations too — the only thing
the CTE genuinely can't do is *mutual* recursion, which is never value-generating in practice.

## The iterative evaluator (new)

New file: `Sources/RBDB/Logic/IterativeEvaluator.swift`.

Given `cone(P)` (all finite):

1. **Materialize** each cone predicate as a `TEMP TABLE` with its declared columns and a
   `UNIQUE` constraint over all columns (for dedup via `INSERT OR IGNORE`).
2. **Seed** each with base facts (`baseFactsSelect`).
3. **Fixpoint loop** (naive first, correctness-first):
   - For each rule of each cone predicate, run
     `INSERT OR IGNORE INTO <head> SELECT … FROM <bodies>` — the body SQL is exactly what the
     existing `ruleIntoSQL` already produces (plain joins; referencing a relation twice is now
     legal because these are ordinary tables, not a recursive CTE).
   - Track `total_changes()` (or per-table row counts) across a full pass; stop when a pass
     inserts zero new rows.
4. **Answer** the user's query: the queried predicate is now a real temp table, so retry the
   user's *original, unmodified* SQL — `SELECT * FROM daughter` just resolves. **No string
   rewriting.**

Non-recursive members of the cone (e.g. `grandparent`, finite, depends on `parent`) fold into
the same loop harmlessly, or are computed once after the recursive core settles.

**Optimization (later): semi-naive.** Track per-predicate deltas and join only against newly
derived tuples each round, instead of recomputing every rule over the full relations. Start
naive; it's obviously correct and family-sized data is tiny.

### Invalidation / caching

Materialized temp tables are stale if facts/rules change; invalidate by dropping them on any
  `_rule` insert (facts *and* rules land in `_rule`). Extend the existing
  `_drop_temp_view_on_rule_insert` trigger to also drop materialized tables.

## What gets deleted / kept / added

**Delete** (the mutual-CTE machinery):
- `RecursiveClosure.swift`: `stronglyConnectedComponents`, `mutualGroup`, `ensureGroupCTE`, and
  the group branch of `reference`. The mutual-top branch of `buildRecursiveClosureCTE`.
- `IntoSQL.swift`: the `tableCondition` / `leadingColumns` / `trailingColumns` additions to
  `RuleIntoSQLReducer` (they existed only to emit the tag-column arms) — revert to pre-mutual
  `RuleIntoSQLReducer`. The iterative evaluator uses plain `ruleIntoSQL`.
- The `nonLinearMutualRecursionError` test (its scenario now *evaluates* → replace with a
  positive-result test).

**Keep** (the value-generating streaming path):
- `ensureCTE`, `predicateBody`, `reference` (minus the group branch), and the bounds machinery
  — `extractEqualityConstraints`, `propagateConstraints`, `boundsForRecursiveStep`, `solve`,
  `stepDirection`. These make value-generating recursion terminate; they belong to the CTE path
  and don't move.
- `replaceTableReference` — still used to inline a value-generating *dependent* predicate
  (`square` over `nat`) for streaming, and its bracket/unbracket robustness is still wanted for
  raw CLI SQL. Reverts to serving only that path.

**Add:**
- `IterativeEvaluator.swift` — cone computation, `valueGenerating` detection, naive fixpoint.
- Routing in `rescue`: finite cone → iterative; else → CTE.
- Canonicalization hook in `assert(formula:)` (below).

Net: we trade the tag-column/SCC complexity for a small, conceptually simple evaluator. Not
strictly fewer lines, but a much cleaner split and — crucially — the finite non-linear sets
actually run.

## Assert-time canonicalization (new)

New file: `Sources/RBDB/Logic/RuleCanonicalization.swift`, hooked into `assert(formula:)`
**before** the formula is stored, and it may **rewrite/remove already-stored rules** in the same
transaction — so the canonical form of the set is **order-independent** (asserting the rules in
any order lands on the same stored set).

**Why `assert(formula:)` is the only hook needed.** Rules (Horn clauses with a body) can *only*
enter through `assert(formula:)`. The SQL surface — `INSERT INTO daughter …` — goes through the
predicate view's `INSTEAD OF INSERT` trigger, which calls `predicate_formula(...)` and can only
produce **base facts** (bodyless), never rules. Canonicalization is purely a rule-level concern,
so facts inserted via SQL need none of it. (Cache invalidation is separate and *does* cover the
SQL path — it triggers on any `_rule` insert; see above.)

**No migration on DB open.** "Don't fix up existing DBs" means we never run a one-time pass over
a database just because it was opened. Canonicalization runs only when an `assert` happens; at
that point it's free to tidy the live set.

Transforms — restricted to ones that are sound **independent of what facts may be asserted
later** (a base fact for any predicate can appear at any future time):

1. **Tautology drop** — head is identical to a body literal (`p(X,Y) :- p(X,Y), …`) → derives
   nothing → don't store.
2. **Subsumption removal** — if one rule subsumes another (same head, body a
   superset-generalization), keep only the more general one. Applies across the incoming rule
   *and* stored rules, in both directions.
3. **Intra-rule literal dedup** — collapse duplicate body literals (`p(X) :- q(X), q(X)` →
   `p(X) :- q(X)`).

**Explicitly *not* doing unfolding/inlining** (the earlier `child` case). Inlining a predicate
`Q` that currently has a single rule and no base facts is **unsound over time**: a base fact for
`Q` can be asserted later, and the already-inlined rule would silently ignore it. Since we can't
constrain future facts, we don't do it.

**Framing:** with the iterative evaluator in place, canonicalization is **not needed for
correctness** — the messy set already evaluates. It's purely cosmetic: keep the stored set free
of tautologies and redundant/duplicate rules. Given that, the safe minimal transform set above
is the right scope; anything requiring assumptions about future facts is out.

## Test plan (per repo workflow: stash-verify each)

- **Family mutual recursion** (`parent`/`daughter`) now via iterative → same results the CTE
  gave. Existing `mutualRecursion` test should still pass (different engine, same answers).
- **Non-linear** (`son`/`child`/`male`, or minimal `p`/`q`/`r`) now **evaluates** → replace the
  error test with a positive-result test.
- **Value-generating** (`nat`, `square`, `inc2`, countdown, doubling, exponent) → unchanged;
  must still pass and still **stream** (`square(X,Y)` paging test). Note `nat(X) :- nat(X - 1)`
  (body-side arithmetic) must still route to the CTE — a routing regression test.
- **Routing**: a finite predicate that references an infinite one (`square`) still routes to the
  CTE (cone contains value-generating) and streams.
- **Invalidation**: assert a fact (via `assert` *and* via raw SQL `INSERT`) after a query that
  materialized the cone; re-query reflects the new fact (stale temp tables were dropped).
- **Canonicalization**: tautology dropped; a subsumed stored rule removed when a more general
  rule is asserted (order-independence — assert in both orders, same stored set); duplicate body
  literals collapsed.

## Limitations after this work

- **Mutual/non-linear *and* value-generating** together (arithmetic feeding a cycle) stays
  unsupported — CTE can't (non-linear) and iterative can't (infinite). Keep a clear error.
- Canonicalization is intentionally minimal (tautology / subsumption / literal-dedup); no
  unfolding, because future base facts could invalidate it.
- Iterative path materializes (no streaming) — fine, those relations are finite by construction.

## Phasing

1. Iterative evaluator + routing + cache-invalidation trigger (invalidation is required for
   correctness now that materialization is cached, not an optimization); migrate the
   mutual/non-linear cases onto it; delete the mutual-CTE machinery and revert the `IntoSQL`
   reducer additions. (Unblocks `male`.)
2. Assert-time canonicalization pass. (Tidies the stored set.)
3. Optimization: semi-naive deltas (join only against newly derived tuples per round).
