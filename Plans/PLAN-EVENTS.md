# Event Calculus support for RBDB — implementation plan

## Context

Van Lambalgen & Hamm, *The Proper Treatment of Events*, builds a computational semantics
of tense/aspect on the **event calculus (EC)** formulated in constraint logic programming:
reified *fluents* and *event types*, five inertia axioms, per-expression *scenarios*
(micro-theories), **minimal models** via Clark completion / negation-as-failure, and
**integrity constraints satisfied by abduction** (the computational meaning of tense — a
concept the book explicitly borrows from database theory).

This plan turns the gap analysis into six phases. Each phase is independently shippable
and tested; later phases depend on earlier ones.

**Progress:** Phase 1 not started. Phase 2's *comparison guards* are done and shipped (see
§2 — its sub-sections are marked DONE/outstanding individually, including two deviations
from what was planned: the guard JSON encoding in §2.1 and guards in queries in §2.3);
negation-as-failure is the remaining half. Phases 3–6 not started. Phase 2 was taken
before Phase 1 because guards need nothing from term flattening.

**Two things have moved out of this plan.** Phase 4.1's *retraction* was pulled forward and
now lives in **PLAN-RETRACTION.md**, together with supersession and strong negation (§4.1
below is a pointer). And that work settles a decision this plan had left implicit: **RBDB is
open-world**, so `not p` means "not derivable" rather than "false", and closure — where it is
genuinely wanted — is stated *in the logic* as an ordinary rule rather than declared out of
band (design decision 2 and §2.0). Immediate order of work is PLAN-RETRACTION.md, then
**Phase 1** (entities/existentials, which OWA is the precondition for), then the rest of this
plan.

Three design decisions shape everything:

1. **Intervals, not points.** Time is ℝ, so `HoldsAt(f,t)` is not enumerable. But the
   book's own representation theorems (Ch. 5, Theorem 4 / Corollary 1) guarantee that in
   the minimal model every fluent denotes a **finite union of half-open intervals**
   `(start, end]`, and parametrized fluents denote semialgebraic sets. So we represent a
   fluent's extent as *rows of intervals* and `holds_at(F,T)` as a range lookup. Dense
   time costs us nothing SQL can't do.
2. **Stratified negation-as-failure = the book's minimal model, computed inside an
   open world.** For the program class we accept (the book's own syntactic restrictions,
   Ch. 4 Def. 9), the perfect model of a stratified program coincides with the
   completion-based minimal model the book computes. We do not need constructive negation
   or a CLP solver; we need `NOT EXISTS`. **RBDB as a whole is open-world**, so `not p`
   means "not derivable", which is *not* the same as "false" — but the EC axioms need
   nothing more, because a scenario is complete by construction and that is the sense in
   which the book's minimal model is already a local one. Where genuine closure is wanted
   it is stated *in the logic* as an ordinary rule rather than declared out of band
   (§2.0), which keeps it visible, retractable, and per-predicate. Everything outside a
   scenario stays open for Phase 1's existentials, which a closed world could not
   represent.
3. **Flatten, don't nest.** A nested term is shorthand for an existentially quantified
   conjunction (`owns(john, book(title('W&P'); copy(1)))` ≡ `∃b (owns(john,b) ∧ book(b) ∧
   title('W&P', b) ∧ copy(1, b))`, witness entity in the trailing argument; `;` separates
   the predicates that describe that entity — §1.2). We store the flattened conjunction
   over entity witnesses (Phase 1), so every component relation stays independently
   queryable and ordinary joins replace structural pattern-matching. The witness-identity
   function is a pluggable policy (resolution deferred; §1.1).

Reference points in the current code:

- `Sources/RBDB/Logic/Term.swift` — `Term` enum (variable/boolean/number/string/expression)
- `Sources/RBDB/Logic/SymbolType.swift` — JSON key scheme (`""`, `v`, `x`, `@name`)
- `Sources/RBDB/Logic/Formula.swift` — `hornClause(positive:negative:guards:)`; `negative`
  = body atoms, `guards` = comparisons (§2.1)
- `Sources/RBDB/Logic/Expression.swift`, `BooleanExpression.swift` — the `Expression`
  protocol pair and comparison guards, canonical in the same sense as `ArithmeticExpression`
- `Sources/RBDB/Logic/IntoSQL.swift` — `RuleIntoSQLReducer` (rules → SELECT/JOIN/WHERE),
  `QueryIntoSQLReducer`, `lower(term:leaf:)`, `invert(_:equalTo:for:_:)`
- `Sources/RBDB/Logic/RuleCanonicalization.swift` — assert-time dedup, tautology drop,
  subsumption over body literals *and* guards
- `Sources/RBDB/Logic/RecursiveClosure.swift` — `solve(_:for:equals:)`, `monotonicity`,
  bound injection for recursive CTEs
- `Sources/RBDB/RBDB.swift` — `assert(formula:)`, `interceptCreateTable`, `baseFactsSelect`,
  `createViewAndTrigger`, `rescue`, `fetchRules`
- `Sources/RBDB/schema.sql` — `_rule` with generated columns `output_type`,
  `arg1_constant`, `arg2_constant`, `negative_literal_count`
- `Sources/Datalog/DatalogParser.swift` — swift-parsing ParserPrinter grammar

Throughout, follow the CLAUDE.md workflow: implement → manually test → `git stash` →
write test, watch it fail → `git stash pop` → watch it pass.

---

## Phase 1 — Entities and term flattening

**Book capability:** reification — fluents, events, and objects fill argument slots
(`Initiates(start, crossing, t)`, `HoldsAt(distance(x), t)`), under unique-names
equality (CET).

**Design (revised from an earlier compound-terms-as-stored-JSON approach):** a nested
term is *shorthand for an existentially quantified conjunction*, and we store the
flattened form:

```prolog
owns(john, book(title('War and Peace'); copy(1))).
%  ≡ ∃b ( owns(john,b) ∧ book(b) ∧ title('War and Peace',b) ∧ copy(1,b) )
%  stored as ordinary facts, b the witness entity, entity in the *trailing* argument:
%    owns('john', b).  book(b).  title('War and Peace', b).  copy(1, b).
```

A nested term introduces a witness entity `b` for the slot it fills. Inside its parens,
the separator chooses how `b` is described — and it is chosen **explicitly**, never
inferred from whether an argument happens to be nested:

- **`,` (comma) — positional arguments.** `f(a, b)` → `f(a′, b′, e)`: the functor becomes
  a relation whose columns are the arguments, `e` last; each nested argument recurses to
  *its own* entity. The classic "record" shape.
- **`;` (semicolon) — co-predications on the functor's entity.** `f(p1; p2)` →
  `f(e), p1@e, p2@e`, where each `pi@e` appends the shared witness `e` as `pi`'s trailing
  argument (recursing). The `;` reads "all of these predicates describe the entity in this
  slot."

So `book(title('Ulysses'); copy(1))` is *one* entity that is a book, titled Ulysses, copy 1
(`book(e), title('Ulysses', e), copy(1, e)` — `book` arity 1); `book(title('Ulysses'),
copy(1))` with commas is instead a single arity-3 `book` record with two field-entities —
a different, rarely-wanted shape. The witness is always the trailing argument; the
functor's declared arity enforces which form is legal (`book` arity 1 for `;`, arity 3 for
the comma form). A lone element defaults to positional: `distance(10)` → `distance(10, e)`
(the common functional case — a fluent parameter). Mixing is allowed: `f(a; p)` →
`f(a′, e), p@e`.

Why flatten rather than store structured values:

- every component relation is independently queryable — `?- title(T)` returns all titles
  (entity slot ignored, §1.3), `?- title(T, B)` returns titles with their entities;
- plain columns, ordinary joins, existing `arg1/arg2` indexes — no JSON
  pattern-matching layer, and **no changes to `_rule`'s generated columns**;
- it matches the book's own analysis: "'the street' in 'cross the street' is not at all
  a direct object … the (lexical) semantics of accomplishments cannot be given by
  copying the surface predicate–argument structure" (Ch. 4 §3.1);
- `_entity` (schema.sql) was designed for this: stable entity IDs, with merging
  anticipated.

**Everything else in this plan depends on this phase.**

### 1.1 Witness identity is a *pluggable policy* (resolution deferred)

Mapping an existential onto a specific existing entity — recognizing that *this* `john`
is *that* entity — is deferred. What we commit to now is the **indirection** that keeps
it open: flattened facts reference an opaque entity ID, and the function
`term → entity ID` is a single swappable policy, never baked into storage or the query
compiler. Entity resolution/merging later becomes a new policy (and `_entity`-based
row rewriting), touching nothing else.

The term ontology this exposes (four kinds, by how identity is assigned):

- **Literals** — quoted strings `'War and Peace'`, numbers, booleans. Global by *value*;
  join by equality, exactly as today. Unchanged.
- **Nested terms** — `distance(5)`, `book(title('Ulysses'); copy(1))`: a functor with a
  positional (`,`) and/or co-predication (`;`) body. Default policy = **content-addressed**
  (witness = hash of the canonical desugared group), so the same nested term denotes *the
  same* entity at every mention. This is not optional for the event calculus: a
  parametrized fluent `distance(5)` must be a function of its parameter for a scenario's
  statements to co-refer (Phase 3). Collision trade-off — two genuinely different things
  with identical descriptions collapse — is resolved by adding a distinguishing
  co-predication (`copy(2)`); incremental description is a known limitation (a later fact
  adding an attribute makes a *different* group ⇒ a different entity unless the full
  descriptor is repeated), which the deferred resolution policy is what ultimately fixes.
  True fresh-per-mention existentials (labeled nulls) remain a future policy, not a syntax
  we need now.
- **Names** — bare lowercase atoms `john`, `mary`. Today these parse to `.string` (i.e.
  they are literals — verified at `DatalogParser.swift:166`). Your proposed reading makes
  them *formula-local existential witnesses* instead. That is a real semantic change and
  **is exactly the deferred identity question** — see §1.1.1. Until it's resolved we keep
  the current behavior (atoms = string literals) so nothing regresses, and treat the
  name→entity policy as the first thing the resolution work will replace.
- **Reserved globals** — `me`, `today`, `now`: names with a fixed, well-known entity
  identity, supplied by the identity policy. A clean special case of "names," available
  independently of how ordinary-name resolution lands.

Implementation of the default functional policy: SQLite function
`skolem(functor, arg, …) → id` (canonical-form hash), registered like `predicate_formula`
(`RBDB.swift:20`), plus a byte-identical Swift mirror for grounding facts at assert time.
It is *one* implementation behind the policy seam, not the definition of identity.

#### 1.1.1 The open question, stated concretely

Formula-local atoms and "map an existential onto a known entity" pull in opposite
directions, and the tension is worth pinning down now even while deferring the answer:

- If `john` is **formula-local**, then two *separate* assertions `parent(john, mary)` and
  `parent(john, bob)` do **not** share a `john` — you cannot state two standalone facts
  about one individual without a shared name. Co-reference only survives *within* a single
  formula (which is genuinely useful for rule-local existentials, and matches "lowercase =
  ∃-var, uppercase = ∀-var").
- If `john` is **global-by-name** (content-address the name), cross-formula co-reference
  works but you've re-introduced global identity by the back door — the very thing you
  said an atom should *not* carry.

The reconciliation the four-kind ontology suggests: **within-formula co-reference is
structural** (same name in one clause → same witness variable — always safe, no identity
policy needed), while **cross-formula co-reference is a resolution question** answered by
the policy (default: none, i.e. formula-local; later: learned/declared entity links, with
`me`/`today` as the built-in cases). Building the policy seam now lets both the
formula-local default and future resolution coexist without a storage change. **Open for
your call:** is that the model you want, and do bare-atom *facts* (as opposed to
rule-local atoms) need any co-reference before resolution exists?

One input this question has since acquired: the engine is **open-world** (design decision
2). That is what makes the formula-local reading coherent in the first place — an
existential witness that no other formula can name is only meaningful if failing to
identify it leaves the matter *unknown* rather than false. Under CWA the default policy
would have been quietly asserting that no such entity exists.

### 1.2 The uniform desugaring and the `.skolem` term

New `Term` case `.skolem(String, [Term])` (SymbolType key `"k"`, inserted between
`expression` and `hornClause`): lowers via `lower(term:leaf:)` to a `skolem('f', …)` SQL
call; folded to its constant id by `canonicalize()` when ground. No structural pattern
matching, no inversion (`invert` returns `nil`), `monotonicity(in:)` = 0. Mechanical
switch sites: `freeVariables`, `substituting`, `rewrite(term:)`, `SymbolReducer.reduce`,
`CanonicalizeRewriter.rewrite(term:)`, `debugDescription`.

Desugaring lives in the Datalog layer (surface syntax → flat `Formula`s); the witness is
appended as the **trailing** argument of every emitted literal (§1.3 explains why last).
A nested term expands to its functor literal plus one literal per co-predication (`;`) and
per nested positional argument (`,`), all sharing the slot's witness:

- **Facts**: a nested assertion expands to a *set* of ground facts asserted in one
  transaction; each literal gets a ground `skolem` id computed in Swift.
- **Rule bodies / queries**: each nested term becomes a fresh variable in place plus its
  expanded literals as added body atoms (a pure pattern — matches any entity, invents
  none). Queries therefore need **conjunctive-query support**: lift
  `QueryIntoSQLReducer`'s single-literal restriction by reusing `RuleIntoSQLReducer`'s
  join construction with a synthetic head. (Generally useful, independent of flattening —
  `?- owns(john, B), title(T, B)` should work regardless.)
- **Rule heads**: a nested head term keeps a `.skolem` over body-bound variables, and the
  statement expands into **companion rules** (one per emitted literal) sharing that skolem
  expression:

```prolog
next(book(title(T); copy(N))) :- shelved(book(title(T); copy(N))).
%  b := sk(book, title(T), copy(N))   (same skolem in every companion rule)
%  ⇒  next(b)          :- shelved(B), book(B), title(T, B), copy(N, B).
%     book(b)          :- shelved(B), book(B), title(T, B), copy(N, B).
%     title(T, b)      :- …same body…
%     copy(N, b)       :- …same body…
```

- **Validation** (`Validate.swift`): skolem head arguments must be body-bound (existing
  `freeVariables` machinery covers it once it descends into `.skolem`). A recursive rule
  whose recursion flows through a head skolem is value invention the bounds machinery
  can't see into (`sk` is opaque to `monotonicity`/`solve`) — reject a recursive
  predicate whose recursive argument position is a skolem containing the recursion
  variable, with a clear error, rather than hanging at query time.

### 1.3 Entity-last, and require `CREATE TABLE` (no auto-declaration)

The witness goes in the **trailing** argument, and reified predicates must be declared
with `CREATE TABLE` before use — the two decisions reinforce each other:

- **Entity-last enables the "one assertion, two readings" trick.** RBDB queries tolerate
  *lower* arity, ignoring omitted trailing arguments (verified: `QueryIntoSQLReducer`
  indexes `columnNames[i]` positionally over the supplied args). So a single stored fact
  `title('War and Peace', b)` answers both `?- title('War and Peace')` ("it is a title")
  and `?- title('War and Peace', B)` ("…of entity b"). Entity-first would forfeit this —
  you could not drop the leading entity to ask the intrinsic question.
- **Declared arity tells the desugarer where the witness goes, and enforces the form.**
  The witness is always the trailing column. A **positional** use `f(a1, …, ak)` requires
  `f` declared arity `k+1` (args + entity); a **co-predication** functor `f(p1; …; pn)`
  requires `f` declared arity 1 (it's a type/tag — the entity is its only column, and the
  `pi` are their own predicates). A given predicate has one declared arity, so its role is
  fixed and checkable: `book` arity 1 is always a tag written with `;`; `title` arity 2 is
  always an attribute written `title(value)`. No "which column is the entity?" marker
  needed. So **no auto-declaration**: predicates used in nested position are declared like
  any other, controlling column names and the arity that fixes their role. (The `ec_*`
  scenario tables in Phase 3 are explicitly created for exactly this reason.)

### 1.4 Parser

`primaryTermParser` gains, before `atomParser`, the nested-term form
`identifier "(" body ")"` → `.skolem`. **A predication always has parens; a bare
identifier stays an atom** (`.string`, as today) — so nothing becomes ambiguous. `body`
is either a `,`-separated positional argument list *or* a `;`-separated co-predication
list (each co-predication reuses `predicateParser`, so it also has parens). The parser
records which separator was used, so desugaring can tell positional args from
co-predications. Consequences of "predications need parens":

- an arity-1 type tag is written with **empty parens** where it can't be the outer
  functor — `book(heavy(); title('W&P'))` → `book(e), heavy(e), title('W&P', e)` (the
  current `predicateParser` already accepts zero-arg `heavy()`); bare `heavy` is still the
  atom `'heavy'`;
- the outer functor's role (arity-1 tag vs positional record) is fixed by its body
  separator and checked against declared arity (§1.3) — no reliance on bare identifiers.

Printer renders `.skolem` back to the surface form (`;` vs `,` recovered from whether a
companion literal shares the parent witness); the flat companion form always round-trips.

### 1.5 Examples / tests (`Tests/RBDBTests/FlatteningTests.swift`, `Tests/DatalogTests/…`)

```prolog
% CREATE TABLE owns(owner, entity);  CREATE TABLE book(entity);
% CREATE TABLE title(title, entity);  CREATE TABLE copy(copy, entity);

% ';' co-predications: one entity that is a book, titled W&P, copy 1
owns(john, book(title('War and Peace'); copy(1))).
% desugared: owns('john', B). book(B). title('War and Peace', B). copy(1, B).

?- title('War and Peace').                    % true  — lower-arity: "it is a title"
?- title(T).                                  % T = 'War and Peace'  — all titles
?- title(T, B).                               % T = 'War and Peace', B = entity id
?- owns(john, book(title(T); copy(1))).       % pattern form: T = 'War and Peace' via joins
?- owns(john, B), book(B), title(T, B).       % fully explicit conjunctive-query equivalent

owns(john, book(title('War and Peace'); copy(1))).  % re-assert → UNIQUE violation (same witness)

% cross-fact co-reference: identical descriptor ⇒ same witness (default content-addressing)
likes(mary, book(title('War and Peace'); copy(1))).
?- owns(john, B), likes(mary, B).             % yes — same entity

% distinguishing individuals: a differing co-predication ⇒ a different entity
owns(john, book(title('Ulysses'); copy(1))).
owns(john, book(title('Ulysses'); copy(2))).
?- title('Ulysses', B).                       % two rows — two entities

% ',' positional (record) contrast: book here is arity 3, title/copy are field-entities
% CREATE TABLE bookrec(title_of, copy_of, entity);
owns(sue, bookrec(title('Ulysses'), copy(1))).
% desugared: owns('sue', B). bookrec(T, C, B). title('Ulysses', T). copy(1, C).

% parametrized fluent: lone element ⇒ positional/functional, parameter is an ordinary column
holds(distance(5), 0).                        % desugared: distance(5, F). holds(F, 0).
?- holds(F, T), distance(X, F).               % X = 5 — fluent parameter read by join

% dedup through canonical arithmetic: skolem is over the canonical form
holds(distance(4 + 1), 1).                    % same witness entity as distance(5)

% skolem in a rule head (deterministic value invention), preserving a ';' descriptor
initiates(start, book(title('W&P'); copy(1))).
reshelved(book(title(T); copy(N))) :- initiates(E, book(title(T); copy(N))).
?- reshelved(B), title(T, B), copy(N, B).     % T = 'W&P', N = 1

% nested co-predication whose value is itself an entity (arity-2 attribute → own entity)
wrote(book(title('War and Peace'); author(name('Leo Tolstoy')))).
% desugared: wrote(B). book(B). title('War and Peace', B). author(P, B). name('Leo Tolstoy', P).
?- name(N).                                   % N = 'Leo Tolstoy'
?- wrote(B), title(T, B), author(P, B), name(N, P).  % T = 'War and Peace', N = 'Leo Tolstoy'
```

Tests: parser round-trip for both `;` and `,` forms (and empty-parens `heavy()`); JSON
snapshot pinning the `"k"` skolem encoding; an agreement test that SQL `skolem()` and
Swift-side grounding produce identical ids (assert a fact via datalog, derive the "same"
entity via a rule head, assert the join succeeds); a `;`-vs-`,` test proving the two forms
produce different arities/entities; a lower-arity test proving one stored fact answers both
the arity-k and arity-(k+1) queries; and an identity-policy seam test (swap in a stub
policy, confirm
storage/query code is unchanged).

---

## Phase 2 — Negation as failure + comparison guards

**Status: comparison guards are DONE** (`fb2a4b9` "Add comparison guards to rule bodies");
negation-as-failure is not started. Done/outstanding is marked per sub-section below.
Two things landed differently from what this plan first specified — the guard JSON
encoding (§2.1) and guards in queries (§2.3); both are described in place.

**Book capability:** the EC axioms are non-Horn: `¬Clipped(t1,f,t2)`,
`¬∃s(s < r ∧ HoldsAt(f,s))` appear in bodies, plus real-order constraints `t < t'`.
Minimality = Clark completion, which for our accepted program class equals the perfect
(stratified) model.

### 2.0 What `not` means in an open world

RBDB is open-world (design decision 2), and nothing about it *today* is closed-world:
queries return what is derivable, which is neutral between the readings, and the
commitment only enters once you can ask a negative question. So this section is a
documentation obligation for the operator this phase adds, not a mechanism.

- **`not p` = "not derivable"**, compiled to `NOT EXISTS` (§2.3). Well-defined and correct
  regardless of what is known about `p` — it is a statement about the database.
- **It is not "false".** Under OWA, concluding falsity from underivability is a modeling
  judgment the engine cannot make for you. It happens to be *sound* for the EC axioms
  because a scenario is complete by construction: `not clipped(…)` ranges over all the
  clipping there is, since the scenario's clauses are all the clauses there are.
- **`-p` is a different operator.** Strong negation ("known false") is stored rather than
  inferred, and is PLAN-RETRACTION.md Step 4. Under OWA `not p` and `-p` are not
  interchangeable — that difference is the whole point.

**The closed-world assumption is expressible as an ordinary rule**, which is how to state
it when it is genuinely wanted:

```prolog
-q(X, Y) :- dom(X), dom(Y), not q(X, Y).     % CWA for q, over the domain `dom`
```

This is the standard CWA axiom of extended logic programs, and it is preferable to an
out-of-band `closed` declaration on `_predicate` (which an earlier draft of this plan
proposed) for four reasons: it is object-level and therefore readable and printable; it is
retractable by the ordinary machinery, so closure is revisable like any other belief; it is
per-predicate for free; and it makes three-valued answers compositional — `q(a,b)`
derivable ⟹ true, `-q(a,b)` derivable ⟹ false, neither ⟹ unknown, with the closure rule
being exactly what moves an atom from *unknown* to *false*.

The **domain literals are mandatory, not incidental**: `-q(X,Y) :- not q(X,Y)` is rejected
by the range restriction (§2.2), since `X` and `Y` occur only inside a negated subgoal.
That is a feature — an open world has no ambient active domain to quantify over, so the
syntax forces you to say what you are closing over, which is the content of the
assumption.

The `not q(X, Y)` literal is **required** by the range restriction, and that is now the only
thing requiring it. An earlier version of this paragraph gave a second reason: PLAN-RETRACTION
§4.2.1 permitted a negative head `-q(t̄)` only when the body carried the matching `not q(t̄)`,
which — with this phase's **stratification check** (§2.2) — made "`q(ā)` and `-q(ā)` are never
both derivable" true by construction, leaving the assert-time coherence check nothing to catch
on the rule side.

**That restriction has been withdrawn** (see PLAN-RETRACTION §4.2.1). The coherence check now
runs after every write and compares the two *relations*, so it catches what a rule derives, and
the guarantee is maintained inductively by checking rather than by syntax. Consequences here:

- Negative-headed rules are writable **now**, without `not`. The CWA axiom is still the
  *motivating* case for them, but no longer the only expressible one.
- The axiom still needs both halves — `not` from this phase, `-p` from PLAN-RETRACTION Step 4 —
  so **testing it end-to-end still belongs to this phase** (§2.5).
- Stratification is unaffected: `-q` is a distinct predicate in the dependency graph, so
  `-q(X) :- dom(X), not q(X)` closes no cycle through negation.
- §2.2 picks up an obligation from the withdrawal: once `not` exists, retraction stops being
  monotonic (retracting `q(1)` makes `not q(1)` hold, which can derive `-p(1)` over a live
  `p(1)`), so **`retract` must run the coherence check too**.

### 2.1 Formula shape — guards DONE, negated subgoals outstanding

As built (`Formula.swift`, `BooleanExpression.swift`, `Expression.swift`):

```swift
case hornClause(positive: Predicate, negative: [Predicate], guards: [BooleanExpression])

// plus a static overload keeping the many `positive:negative:` construction sites working:
static func hornClause(positive: Predicate, negative: [Predicate]) -> Formula
```

The negated-subgoal list is still to come, and slots in the same way:

```swift
case hornClause(positive: Predicate, negative: [Predicate],
                negated: [NegatedSubgoal], guards: [BooleanExpression])

public struct NegatedSubgoal {      // ¬∃x̄ (B1 ∧ … ∧ Bn ∧ guards) — book's "complex subgoal"
    public let predicates: [Predicate]
    public let guards: [BooleanExpression]
}
```

A guard is a `BooleanExpression`, the boolean-valued complement to `ArithmeticExpression`
and canonical in the same sense: only `<`, `<=`, `=`, `!=` are representable, `>`/`>=`
fold to their mirror by swapping operands, and the symmetric operators sort their
operands — so logically-equal comparisons are structurally equal and dedup by `==`.
`Expression`/`ExpressionInternal` factor out what the two expression types share
(`Codable`, ordering, debug description). `ArithmeticExpression` keeps its own `<`
(operand-case order, not operator symbol) because that ordering is baked into every
stored canonical form.

Notes:
- A `NegatedSubgoal` wraps a *conjunction* — the book's `¬∃s(s<r ∧ HoldsAt(f,s))` needs
  negation scoping over atoms + guards together, not a single literal.
- **Encoding (revised).** Formula JSON stays `[type, headArgs, body…]`. A guard is stored
  as `{op: [lhs, rhs]}` — e.g. `{"<": [{"v":0}, {"":5}]}` — *not* the originally planned
  `{"?": [lhs, "<", rhs]}`: the operator is already the key, exactly as
  `ArithmeticExpression` encodes, so there is no redundant op string and no second
  encoding convention to maintain. Body entries remain unambiguous by container shape (a
  predicate is an unkeyed array, a guard a keyed object), and `{"~": …}` is still free for
  negated subgoals. `negative_literal_count` needed no schema change: it is
  `json_array_length(formula) - 2` and so counts guards as body entries, which preserves
  its only real meaning ("rule, not fact").
- Canonicalization (done for guards): guards are rebuilt through `mappingOperands` so the
  symmetric operators re-sort under renamed variables, then sorted as a group after the
  positive literals. Variable renaming order = positives, then guards, then (later)
  negated subgoals, whose local existentials rename within their subgoal.
- Rule-level canonicalization (`RuleCanonicalization.swift`) dedups guards and counts a
  guard as a body constraint for subsumption, so `q :- p` subsumes `q :- p, X < Y`.

### 2.2 Safety + stratification (extend `Validate.swift`) — range restriction DONE

- **Range restriction (done for guards):** every variable in a guard, in the head, or free
  in a negated subgoal must occur in a *positive* body literal. Variables occurring only
  inside a negated subgoal are its existentials. Consequence worth documenting for users:
  `=` is a pure filter, not a binder, so `q(X) :- p(Y), X = Y + 1` is rejected as unsafe —
  arithmetic that *binds* belongs in a body literal argument (`q(X) :- p(X - 1)`), which
  the existing `invert` machinery already solves.
- **Stratification check at assert time (outstanding):** maintain the predicate dependency graph
  (readable straight from `_rule`: `output_type` → body predicate names, with a
  negative-edge flag). Reject an `assert` that closes a cycle containing a negative
  edge: `ValidationError.unstratifiableNegation(cycle: [String])`. This is also the
  guard that keeps us out of SQLite's own limitation — a recursive CTE may not be
  referenced from a subquery in its own definition, so `NOT EXISTS` through recursion
  wouldn't compile anyway. Fail early with a good message.

### 2.3 SQL lowering (`IntoSQL.swift`) — guards DONE

- Guards (done): lower both terms via the existing `lower(term:leaf:)` against the bound
  variable map and emit `lhs op rhs` as a `WHERE` condition. Sound because every join the
  rule reducer builds is an inner join, so a `WHERE` predicate and a `JOIN…ON` predicate
  are interchangeable. `SQLSelect` holds these conditions itself rather than hanging them
  on a table, so a body of nothing but guards (`p(1) :- 2 < 1`) still filters — a
  `FROM`-less `SELECT` carries its `WHERE` in SQLite. Inside a recursive CTE the guard
  shares the `WHERE` clause with `RecursiveClosure`'s injected bounds.
- Negated subgoal: a correlated antijoin —

```sql
NOT EXISTS (SELECT 1 FROM [p1] JOIN [p2] ON … WHERE <inner guards AND correlation>)
```

  built by recursively invoking the same reducer logic on the subgoal's predicates with
  the outer `cols` map seeded (correlated variables resolve to outer columns; subgoal
  existentials bind inside).
- `QueryIntoSQLReducer` — **deferred, not done.** The plan was to allow guards on queries
  (`?- holds(F,T), T < 10`); as shipped they are rejected with "Queries with comparison
  guards are not supported", because a guard on a query is only useful together with the
  conjunctive-query support the single-literal reducer still lacks (§1.2 plans the same
  lift for flattening). Phase 3's derived relations are queried this way, so this has to
  land before or with Phase 3 — the natural fix is to route queries through
  `RuleIntoSQLReducer`'s join construction with a synthetic head, which buys multi-literal
  queries and query guards in one move.

### 2.4 Datalog syntax — guard syntax DONE, `not` outstanding

```prolog
p(X) :- q(X), not (r(X, Y), Y < X).    % negated conjunction, Y existential
p(X) :- q(X), not r(X).                % sugar: single-literal subgoal
p(X) :- q(X, Y), X < Y.                % guard
```

Parser: body items become a `oneOf` over predicate / `not` subgoal / comparison;
comparisons recognized by lookahead for an operator after a term.

As built: a body item is a `OneOf` over predicate / comparison (the `not` subgoal arm is
still to come). Predicate is tried first and fails fast on a guard, since a guard has no
`(` after its leading term. Surface `>`/`>=` parse into the same canonical
`BooleanExpression` as their mirrors, so printing emits only `<`/`<=`/`=`/`!=` and the
flat form round-trips.

### 2.5 Examples / tests — the light-switch world (book Ch. 5, Exercise 1)

**Outstanding** — the light-switch fixture needs `not`. What exists today
(`Tests/DatalogTests/ComparisonGuardTests.swift`,
`Tests/RBDBTests/BooleanExpressionCanonicalizationTests.swift`, and the guard cases in
`DatalogParserTests.swift`) is the guard half: operator folding/symmetry and JSON
encoding, parser round-trip, the motivating `B != S` sibling rule, numeric and
arithmetic-operand guards, a guard inside a recursive rule, a guard-only body, range-
restriction rejection, storage round-trip through canonical JSON, commuted-guard dedup,
and guard subsumption.

This is deliberately the book's own exercise; it exercises negation + guards *without*
needing Phase 3's materializer, because all times are known constants:

```prolog
% CREATE TABLE happens(event, t);  CREATE TABLE initially(fluent);
% CREATE TABLE initiates(event, fluent);  CREATE TABLE terminates(event, fluent);
happens(switch1_on, 5).   happens(switch1_off, 10).
initially(light2_on).
initiates(switch1_on, light1_on).   terminates(switch1_off, light1_on).

% clipped(T1, F, T2): some terminating event strictly inside (T1, T2)
clipped(T1, F, T2) :- happens(E, S), terminates(E, F), T1 < S, S < T2.

% Point-sampled HoldsAt over a finite probe relation (dense time comes in Phase 3):
% CREATE TABLE probe(t);  probe(0). probe(1). … probe(15).
holds_at(F, T) :- initially(F), probe(T), not clipped(0, F, T).             % axioms 1+2
holds_at(F, T) :- happens(E, S), initiates(E, F), probe(T), S < T,
                  not clipped(S, F, T).                                     % axiom 3
```

Expected results (tests assert exactly these):

```prolog
?- holds_at(light1_on, 7).      % yes  (initiated at 5, not clipped in (5,7))
?- holds_at(light1_on, 12).     % no   (clipped at 10)
?- holds_at(light1_on, 5).      % no   (fluent does not hold at initiation instant)
?- holds_at(light1_on, 10).     % yes  (holds at the instant it is terminated — book §4 Ch.5)
?- holds_at(light2_on, T).      % T = 0..15, all probes (inertia from Initially; nothing clips it)
```

Also: unit tests for stratification rejection (`p(X) :- q(X), not p(X)` → error), range
restriction (`p(X) :- not q(X)` → unsafe), and a negation-under-recursion assert that must
be rejected rather than generating illegal SQL.

**The CWA axiom, end-to-end** (§2.0 — the negative-head restriction it satisfies is
PLAN-RETRACTION §4.2.1, whose acceptance half is only testable once `not` exists):

```prolog
% CREATE TABLE dom(x);  CREATE TABLE q(x);
dom(1). dom(2). q(1).
-q(X) :- dom(X), not q(X).
```

Assertions: `?- q(1)` yes, `?- -q(1)` no, `?- q(2)` no, `?- -q(2)` yes — the three-valued
reading, with `q(3)` *unknown* (absent from both, since `3` is outside `dom`). Then the
guarantee: `q` and `-q` are never both derivable — assert `q(2)` afterward and `?- -q(2)`
goes empty, no contradiction and no error. Contrast cases: the same rule *without* the
`dom(X)` literal is rejected as unsafe; and `-p(X) :- q(X), not p(X)` is *accepted* where
`p(X) :- q(X), not p(X)` is a stratification error — the negative head is a distinct
predicate. (The `not q(X)` literal is no longer required in its own right; §4.2.1's
restriction was withdrawn, and dropping it now yields a rule that is accepted unless it
actually derives a contradiction.) (Guarded recursion staying bounded is
already covered — `a guard applies inside a recursive rule` in `ComparisonGuardTests`.
Related: `doesArithmetic` deliberately ignores guards, so arithmetic in a guard does *not*
route a recursive predicate through the iterative evaluator — a guard filters rows and
cannot invent values.)

---

## Phase 3 — Interval time, trajectories, and the EC materializer

**Book capability:** dense real time; continuous change (`Trajectory`, `Releases`);
computing the unique minimal model of EC + scenario (Ch. 4–5). The evaluation feedback
loop — events initiate fluents, fluent states trigger events
(`HoldsAt(build,t) ∧ HoldsAt(house(c),t) → Happens(finish,t)`) — is recursion through
negation *in time order*, which is exactly what a chronological fixpoint resolves and a
single `WITH RECURSIVE` cannot.

New module: `Sources/EventCalculus/` (imports RBDB + Datalog), so the core engine stays
agnostic.

### 3.1 Reserved scenario predicates

`db.loadEventCalculus()` creates (via the normal CREATE TABLE interception):

```sql
CREATE TABLE ec_initially(fluent);
CREATE TABLE ec_happens(event, t);                  -- asserted event tokens
CREATE TABLE ec_initiates(event, fluent);           -- unconditional scenario statements
CREATE TABLE ec_terminates(event, fluent);
CREATE TABLE ec_releases(event, fluent);
CREATE TABLE ec_trigger(event);                     -- registry of defined/triggered events (see below)
```

Conditional scenario statements (`S(t) → Initiates(e,f,t)`) are ordinary rules with these
heads, where `S(t)` may reference the *output* relations below — the loader validates
they conform to the book's state restriction (Def. 7: only `holds_at` literals, guards,
and equalities; no `happens` — Def. 9 event-definitions are registered separately via
`ec_trigger` rules so the loader can order them).

Dynamics (Trajectory) get a dedicated statement form because their head variables are
not range-restricted in datalog terms:

```prolog
% "while F1 holds, parametrized fluent P evolves as EXPR of elapsed time D from start value X"
ec_trajectory(build, house(X), house(X + D)).
```

Stored in a reserved rule group (Phase 4 machinery; until then, a reserved table
`ec_trajectory(activity, from_pattern, to_pattern)` holding the canonical JSON of the
patterns). The loader checks: `to_pattern` = same functor, argument an `Expression` in
`X` and the reserved elapsed-time variable `D`, monotone in `D` (reuse
`Term.monotonicity`).

### 3.2 Output relations (materialized by the fixpoint)

```sql
CREATE TABLE ec_holds(fluent, start, end);          -- (start, end] ; end = NULL means unclipped
CREATE TABLE ec_happens_all(event, t);              -- asserted ∪ triggered tokens
CREATE TABLE ec_traj_active(fluent_pattern, activity, t0, x0, expr);  -- released param fluents
```

Derived, defined as ordinary rules by the loader:

```prolog
holds_at(F, T)  :- ec_holds(F, S, E), S < T, T <= E.
holds_at(F, T)  :- ec_holds_open(F, S), S < T.                  -- NULL-end split view
clipped(T1, F, T2) :- ec_happens_all(E, S), ec_terminates(E, F), T1 < S, S < T2.
% parametrized fluents under an active trajectory: which house(x) holds at t is computed
%   as x = expr(x0, t - t0) — exposed as ec_value_at(F, T, X), a rule joining
%   ec_traj_active with the trajectory expression; F is the fluent entity, X its
%   parameter (both ordinary columns thanks to Phase 1 flattening).
```

`end = NULL` (open interval) avoids JSON-illegal `Inf`; the split `ec_holds_open` view
keeps rule bodies clean.

### 3.3 The chronological fixpoint (`ECMaterializer.swift`)

Swift driver, semi-naive in event time:

1. **Seed.** `ec_holds` ← `(f, 0, NULL)` for each `ec_initially` fact. Agenda ←
   `ec_happens` tokens ordered by `t`.
2. **Pop** the earliest unprocessed event token `(e, t)`:
   - For each `ec_terminates(e, f)` (incl. conditional rules, evaluated *at* `t` via
     `holds_at`): close the open `ec_holds` row for `f` (`end = t`).
   - For each `ec_releases(e, f0)` where `f0` matches a trajectory's `from_pattern`:
     close `f0`'s interval and insert into `ec_traj_active` with `t0 = t`, `x0` from the
     matched pattern, `expr` from the trajectory statement.
   - For each `ec_initiates(e, f)`: open `ec_holds` row `(f, t, NULL)`.
3. **Trigger scan.** For each registered trigger rule
   `S(T) → happens(e, T)` whose state `S` references an active trajectory value
   reaching a constant (`holds_at(house(c), T)`): solve `expr(x0, T - t0) = c` for `T`
   using `RecursiveClosure.solve(_:for:equals:)` (already written — this is why it was
   factored as a general numeric inverter). If the solution `T* > t` and `S(T*)` fully
   holds, push `(e, T*)` onto the agenda and record it in `ec_happens_all`.
   Non-trajectory trigger conditions are evaluated by querying `S(T)` with `T` bound to
   candidate interval endpoints (state changes only at endpoints — inertia).
4. Repeat until the agenda is empty. Guard: `maxEvents` (default 10 000) →
   `ECError.nonTerminating` — the book's finite-evaluability assumption made explicit.

API:

```swift
try db.loadEventCalculus()
try db.assert(datalog: "ec_happens(start, 2).")           // etc.
try db.ecRun()                                            // (re)materialize from scratch
let cursor = try db.query(datalog: "holds_at(other_side, T)")
```

`ecRun()` is idempotent (truncate outputs, recompute) — nonmonotonic updates (book Ch. 4
§4) are handled by *recomputation*, which is the DB-native answer to belief revision:
assert one more `ec_happens` fact, run again, conclusions change.

### 3.4 Worked example A — crossing the street (book Ch. 4 §3.1 + Ch. 5 §3, §6)

The book's central example, verbatim as a test fixture (`m = 10`, start at `t0 = 2`):

```prolog
ec_initially(one_side).
ec_initially(distance(0)).
ec_initiates(start, crossing).
ec_releases(start, distance(0)).
ec_initiates(reach, other_side).
ec_terminates(reach, crossing).
ec_trajectory(crossing, distance(X), distance(X + D)).      % unit velocity
% trigger (book statement 3): reach fires when distance hits the street width
happens(reach, T) :- holds_at(distance(10), T), holds_at(crossing, T).   % registered trigger
ec_happens(start, 2).
```

Expected minimal model (each line is a test assertion):

```prolog
?- ec_happens_all(reach, T).            % T = 12          (solved: 0 + (T-2) = 10)
?- holds_at(crossing, 7).               % yes
?- holds_at(crossing, 12).              % yes  (holds AT termination instant)
?- holds_at(crossing, 12.5).            % no
?- holds_at(other_side, 13).            % yes
?- holds_at(one_side, 1).               % yes; holds_at(one_side, 5) also yes (nothing terminates it —
                                        %  faithful to the book's scenario as given)
?- ec_value_at(F, 7, X), distance(F, X).% X = 5           (trajectory value mid-crossing)
?- holds_at(distance(0), 1).            % yes; at 3: no (released at 2)
```

The "imperfective paradox" test: retract/omit the `ec_happens(start, 2)` fact → after
`ecRun()`, `?- holds_at(other_side, T)` has no rows, while the scenario (sense) is
unchanged. Add the fact back → goal reached. This is the book's nonmonotonic progression
in two `ecRun()` calls.

### 3.5 Worked example B — the bucket (book Ch. 5, Exercises 2–6)

```prolog
ec_initially(height(0)).
ec_happens(tap_on, 5).
ec_initiates(tap_on, filling).
ec_terminates(overflow, filling).
ec_releases(tap_on, height(0)).
ec_trajectory(filling, height(X), height(X + D)).
happens(overflow, T) :- holds_at(height(10), T), holds_at(filling, T).
```

Assertions: `overflow` at `T = 15`; `holds_at(height(10), 20)` — after the trajectory
ends, `height(10)` is re-initiated as an inert fluent (Exercise 3's point: axiom-2
persistence takes over; the materializer must convert the final trajectory value into an
open `ec_holds` row when the driving activity ends). Exercise 4 variant: add
`ec_happens(tap_off, 10)` + `ec_terminates(tap_off, filling)` → height freezes at 5,
no overflow. Exercise 5 variant: drop the `ec_releases` line → `height(0)` persists by
inertia and `height(10)` never holds (regression test that `Releases` is what exempts a
fluent from inertia).

### 3.6 Aktionsart validation (small, rides along)

A scenario-level check in the EC loader, per book Ch. 7 §2: classify each fluent's role
(activity if it appears as a trajectory's 1st argument; parametrized state if in
`ec_releases`/trajectory 3rd argument) and reject role conflicts (same fluent in both).
Expose `db.ecAktionsart(ofScenario:)` returning
`state / activity / accomplishment / achievement / point` from the quadruple-slot
occupancy — trivially derivable, useful for tests documenting the taxonomy:

```swift
// build-a-house scenario (Ch. 7 §2.1) classifies as .accomplishment
// know/love (bare fluent, no dynamics, no culmination) → .state
// reach-the-top (event + consequent fluent only) → .achievement
```

---

## Phase 4 — Retraction, named rule groups, parametrized scenarios

**Book capability:** scenarios are swappable micro-theories; **coercion** (Ch. 11) is
meta-programming over them — additive (extend a scenario), subtractive (delete
clauses), cross-coercion (unify *parameters* in a scenario template with terms from
semantic memory).

### 4.1 Retraction — moved to PLAN-RETRACTION.md, and pulled forward

**This sub-section is now a pointer.** Retraction turned out to be a prerequisite rather
than a Phase 4 convenience (§3.4's imperfective-paradox test and all of Phase 5 need it),
and it grew a design of its own, so it lives in **PLAN-RETRACTION.md** and is scheduled
before Phase 1. What landed there differs from the sketch this section used to carry:

- **Not `DELETE` — supersession.** The database is an immutable record: `_rule` gains a
  `superceded_by` column pointing at the entity whose assertion retired the row, and every
  read filters on `superceded_by IS NULL`. History stays queryable, and `_entity.entity_id`
  being a uuidv7 means assert and retract times come for free.
- **Retraction ≠ asserting the negation.** Contraction ("no longer known true") and strong
  negation ("known false") are distinct operations related by the Levi identity, and are
  kept separate: asserting `-p` over a live `p` is *refused*, not silently repaired, so
  revision is two recorded acts. §2.0 above covers how both relate to `not`.
- The invalidation twin is an `AFTER UPDATE OF superceded_by` trigger, and it must
  **drop and rebuild** materialized closures rather than re-iterate them — retraction is
  nonmonotonic, so the assumption the insert-side trigger rests on does not hold.

Group retraction (§4.2's `group_name`) is *not* covered there and remains part of this
phase.

### 4.2 Rule groups

```sql
ALTER: _rule gains  group_name TEXT NULL;         -- plus index (group_name)
```

```swift
try db.assert(datalog: "…", group: "scenario:build_a_house")
try db.retract(group: "scenario:build_a_house")                  // drops all + their views
let rules = try db.rules(inGroup: "scenario:build_a_house")      // [Formula]
```

`fetchRules(for:)` is unchanged (groups don't affect evaluation, only lifecycle).

### 4.3 Scenario templates (parameters)

A parameter is a reserved compound `param(name)` in any term position of a stored
template. Templates live in groups but are **excluded from evaluation** (loader marks
the group inactive: simplest is `group_name LIKE 'template:%'` filtered out of
`fetchRules` — one WHERE clause).

```swift
// Instantiate = SymbolRewriter substituting params, then assert into a live group
try db.instantiate(template: "template:progressive_dynamics",
                   bindings: ["f1": .string("resemble"),
                              "f2": .compound("resemblance", [.variable(x)])],
                   into: "scenario:resembling")
```

### 4.4 Coercion examples / tests (book Ch. 11)

```prolog
-- Additive: activity → accomplishment ("build" → "build a house")
-- group scenario:build starts as bare activity:
ec_initiates(start, build).
-- coercion adds (same group), turning it into the Ch.7 §2.1 accomplishment:
ec_releases(start, house(A)).
ec_trajectory(build, house(X), house(X + D)).
ec_initiates(finish, house(100)).
ec_terminates(finish, build).
happens(finish, T) :- holds_at(build, T), holds_at(house(100), T).
```

Test: before coercion, `ecRun()` after `ec_happens(start,0)` yields `build` holding
open-endedly and no `finish`; after adding the clauses and re-running, `finish` fires at
`T=100`. (`ecAktionsart` flips activity → accomplishment.)

```prolog
-- Subtractive: accomplishment → activity ("drink a glass of wine" → "drink wine")
-- retract exactly the culmination clauses (Ch. 11 §2.1's sentences (2) and (3)):
retract: ec_terminates(finish_glass, drink).
retract: happens(finish_glass, T) :- holds_at(wine_drunk(250), T), holds_at(drink, T).
```

Test: with the clauses, drinking terminates at the 250ml trigger; after retraction,
`drink` holds unboundedly (and `ecAktionsart` reports activity).

Cross-coercion test: instantiate the progressive-dynamics template against the state
`resemble` (Ch. 11 §3.1) and assert `?- holds_at(resemble, now)` succeeds only in the
instantiated scenario.

---

## Phase 5 — Integrity constraints + abduction (tense)

**Book capability:** a tensed sentence is an integrity constraint
`IF ?φ succeeds THEN ?ψ(R,…) must succeed/fail` (Ch. 8 Def. 23), satisfied by a
**minimal update**: abducing `Happens`/`Initially` facts from declared abducible
predicates, and/or accumulating `(in)equalities` on symbolic constants (Reichenbach's
reference time `R`, the utterance time `now`). Definition 24: the sentence is *true* iff
the constraint can be satisfied.

### 5.1 Declarations & storage

```sql
CREATE TABLE IF NOT EXISTS _abducible (predicate_name TEXT PRIMARY KEY);
CREATE TABLE IF NOT EXISTS _constraint (
    internal_entity_id INTEGER PRIMARY KEY REFERENCES _entity,
    goal BLOB NOT NULL,              -- JSONB conjunctive goal (predicates + guards)
    condition BLOB NULL,             -- optional antecedent goal (the IF part)
    polarity INT NOT NULL            -- 1 = must succeed, 0 = must fail
);
```

```swift
try db.declareAbducible("ec_happens")
try db.assertConstraint(goal: "holds_at(has_flu, R), R = now", mustSucceed: true)
```

`R`-style symbols: uppercase datalog variables in a constraint goal are *skolem
constants to solve for*, not universals. `now` is a reserved 0-ary term bound at
satisfaction time (`db.satisfyConstraints(now: 17.0)`).

### 5.2 The abduction engine (`Abduction.swift`)

Top-down SLD over the stored rules (the SQL layer answers EDB leaves), because
minimal-update search needs the *failed derivation frontier* — which bottom-up SQL
evaluation discards:

1. Resolve the goal against rules (`fetchRules`) depth-first with a depth bound;
   ground/EDB subgoals are answered by compiling the atom to SQL (existing
   `queryIntoSQL`) and probing.
2. Guards involving skolems accumulate into a constraint store: closed intervals per
   skolem (`R ∈ (a, b]`), maintained with interval intersection. Unsatisfiable store →
   branch fails.
3. When a subgoal on an **abducible** predicate cannot be answered from the DB, record
   it as a *hypothesis* (with its accumulated constraints) instead of failing.
4. Negated subgoals: evaluate against DB ∪ hypotheses; a hypothesis that would violate
   a `not clipped(…)` in the same derivation is rejected (consistency check — this is
   what makes the update *minimal and sound*, book Ch. 8 §1's "update with ¬Clipped
   formulas" means "choose hypotheses so the Clipped goals still fail").
5. Rank solutions: fewest hypotheses, then prefer widest constraint intervals; commit
   the best solution — insert hypothesis facts (choosing a witness value inside each
   skolem's interval, e.g. midpoint; record the interval in the returned report), then
   `ecRun()` to refresh, then verify the goal now succeeds through the normal engine.

**Ride-along: finish `CoherenceError`.** The derivation this search produces is exactly what
PLAN-RETRACTION §4.2's contradiction error cannot currently supply. That check refuses an
assert whose inverse is *derivable*, but where the inverse is derived rather than stored it can
only name the predicate and bindings — `storedAs: nil`, "we know it follows, but not from
what", because bottom-up SQL evaluation discards the proof. Step 1 of this engine (resolve the
goal against `fetchRules` depth-first, answering EDB leaves by compiling atoms to SQL) is a
proof search over the same rules, so running it on the *inverse* goal yields the base facts and
rules the contradiction rests on — the set the caller would have to retract for the assert to
succeed. Wire that into `CoherenceError` when this lands; it is a strictly better error, and
the machinery is already being built for other reasons.

```swift
public struct AbductionResult {
    public let added: [Formula]                       // e.g. ec_happens(catch_flu, 14.0)
    public let skolems: [String: ClosedRange<Double>] // e.g. "R": 14.0...17.0
}
func satisfy(constraint: …, now: Double) throws -> AbductionResult   // throws .unsatisfiable
```

Prohibition constraints (`polarity = 0`) and conditioned constraints don't need search:
run the condition, then require the goal to fail / succeed post-update — checked in
`satisfyConstraints` and on demand.

### 5.3 Examples / tests (book Ch. 8 §1)

**Present perfect — "I have caught a flu":**

```prolog
ec_initiates(catch_flu, has_flu).          % the entire scenario
% abducible: ec_happens
% constraint: ?- holds_at(has_flu, R), R = now.        (must succeed; now = 17)
```

Expected: `added = [ec_happens(catch_flu, T)]` for some `T < 17` (engine picks a
witness; test asserts `T < now` and that `?- holds_at(has_flu, 17)` succeeds afterward).
This is the book's own derivation: the failed frontier is exactly
`?Happens(e, t0), t0 < R` — the computational meaning of the perfect.

**Umbrella (Kowalski's reactive rule):**

```prolog
ec_initiates(take_umbrella, carry_umbrella).
% IF ?- holds_at(rain, now)  THEN ?- holds_at(carry_umbrella, now + 1) must succeed
```

Test: with `holds_at(rain, now)` false, no update; assert rain (initially + run), then
`satisfyConstraints` abduces `ec_happens(take_umbrella, now)`.

**Past tense — "John was crossing the street" ⊨ "John will have crossed" (Ch. 8 §2,
sentences (8a)/(8b)):** using the Phase-3 crossing scenario *without* the
`ec_happens(start, 2)` fact:

- Constraint: `?- holds_at(crossing, now)` must succeed (progressive) →
  abduces `ec_happens(start, T0)`, `T0 < now`.
- Then the *ordinary query* `?- ec_happens_all(reach, T), T > now` succeeds in the
  updated minimal model — the entailment the book celebrates as its solution to the
  imperfective paradox, and our end-to-end acceptance test for Phases 1–5 together.

**Unsatisfiable:** constraint `?- holds_at(has_flu, R), R < now` with *no* abducibles
declared → `.unsatisfiable` (Definition 24: the sentence is false).

---

## Phase 6 — Formulas as terms (nominalization) — optional / research

**Book capability:** Ch. 6 codes formulas as objects (Gödel-style) so nominals can fill
argument slots; `HoldsAt` is the truth predicate `T1`; iterated nominalization uses
paired `T1/T̄1` predicates for partiality (Stärk's trick) — pure modeling once the
engine has the rest.

Minimal engine surface:

- `Term.quote(Formula)` — the canonical JSONB *is* the Gödel code; encoding key `"q"`.
  Equality = code equality (intensional, not extensional — exactly the book's point that
  fluents with the same temporal profile may differ).
- A rule library implementing the `T1` clauses (Axiom 11) over quoted formulas, and the
  bridging convention `holds_at(quote(φ[t̂]), s) ↔ φ(s)` for fluents derived from atomic
  predicates (Lemma 2's safe case only — the consistency condition is on the *user's*
  scenario, documented, not enforced).
- Partiality: pairs `t1(F, X)` / `t1_bar(F, X)` as a documented modeling pattern + tests;
  no three-valued logic in the engine.

Example (deferred, sketch only):

```prolog
% "the crossing of the river occurred on April 1" — perfect nominal as event object
occurred(quote(crossing_river), 'April 1').
% imperfect nominal as fluent usable in holds_at:
?- holds_at(quote(run(john)), T).
```

Gate this phase on a real use case; Phases 1–5 deliver the book's tense/aspect engine
without it.

---

## Dependency graph & suggested order

```
PLAN-RETRACTION.md (supersession / retract / strong negation)    [next up, then Phase 1]
  └─→ Phase 1 (term flattening / entities)                       [not started]
        └─→ Phase 2 (negation + guards)                          [guards DONE, negation to do]
              └─→ Phase 3 (intervals + EC materializer + Aktionsart)  [not started]
                    ├─→ Phase 4 (groups / templates / coercion)  [retraction moved out — §4.1]
                    └─→ Phase 5 (integrity constraints + abduction)
                          └─→ Phase 6 (quoted formulas)          [optional]
```

PLAN-RETRACTION.md leads because it is independent of everything here (it touches storage
and `assert`, not the logic layer), because Phase 3 and Phase 5 both need retraction, and
because it settles the open-world question that Phase 1's existentials depend on.

The Phase 1 → Phase 2 arrow turned out to be soft: guards touch `Formula`, the parser,
`IntoSQL` and `Validate` but nothing in term flattening, so they shipped first. Negation
is likewise independent of Phase 1; Phase 3 is what genuinely needs both.

Rough sizing: P1 ≈ P2 ≈ medium (each touches Term/Formula/parser/IntoSQL/Validate +
tests); P3 large (new module + fixpoint driver); P4 small; P5 large (search engine);
P6 small-if-ever.

Carried into later phases from the guard work:

- **Conjunctive queries** (§2.3, §1.2): query lowering still handles a single positive
  literal and no guards. Both Phase 1's flattening patterns and Phase 3's derived
  relations need the lift, and one change buys both. Since Phase 1 is the next work after
  PLAN-RETRACTION, this is on the near-term critical path rather than shelved with the
  rest of the EC work: `?- owns(john, book(title(T); copy(1)))` compiles to a join, so
  flattening cannot be demonstrated without it. Route queries through
  `RuleIntoSQLReducer`'s join construction with a synthetic head; that buys multi-literal
  queries, query guards, and (later) `not` in queries in one move.
- **Reserved names:** the comparison operators are now real surface syntax, so the
  reserved-name check below has something concrete to reject against.

## Cross-cutting notes

- **Schema migration: none, by decision.** P1 and P4 alter `_rule`/indexes, as does
  PLAN-RETRACTION. Backward compatibility and migrating existing database files are
  explicitly *not* goals: `schema.sql` is edited in place and stale `.db` files are
  recreated. This supersedes an earlier note here proposing a `PRAGMA user_version` bump —
  don't add that complexity.
- **Reserved names:** `ec_*` tables, `param`, `quote`, `now`, comparison operators.
  Reject user CREATE TABLE / functors colliding with them (extend
  `interceptCreateTable` + parser validation). Same check, same place: a table name
  beginning with `-` is rejected, since negative predicates are implicit and share the
  positive form's columns (PLAN-RETRACTION §4.1).
- **Performance guardrails:** flattening (P1) stores entity IDs as ordinary constants,
  so the `arg1/arg2` indexes keep working; deep nesting costs joins, not JSON scans.
  The materializer (P3) is O(events × rules)
  with SQL probes — fine at linguistic scale (scenarios have tens of clauses).
- **Book fidelity checks worth encoding as tests:** fluent false at initiation instant,
  true at termination instant (Ch. 5 §4); `Releases` exempting from inertia (Ex. 5);
  no event at t = 0 and no event both initiating and terminating the same fluent
  (Ex. 7's integrity conditions — enforce in the EC loader).
