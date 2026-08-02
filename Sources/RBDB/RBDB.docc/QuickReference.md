# Quick Reference

Create a database, assert some facts, and let RBDB derive the rest.

## Opening a database

A ``RBDB`` instance wraps a SQLite database. Creating a table defines a predicate:

```swift
import RBDB

let db = try RBDB(path: "family.db")

try db.query(sql: "CREATE TABLE parent(parent, child)")
try db.query(sql: "CREATE TABLE grandparent(grandparent, grandchild)")
try db.query(sql: "CREATE TABLE sibling(s1, s2)")
```

### One connection at a time

Currently, a database serves **one connection at a time**, holding an exclusive lock on the file for as long
as that connection lives. A second ``RBDB`` on the same path throws ``RBDBError/databaseInUse(path:)``.
Release the first one and the next can open:

```swift
var db: RBDB? = try RBDB(path: "family.db")
// …
db = nil                                  // the database is free again
let other = try RBDB(path: "family.db")   // now this opens
```

We hope to lift this restriction in the future.

## Asserting facts

Facts are ``Formula`` values. You can build them directly, but usually, you'll just use SQL or Datalog syntax:

```swift
// SQL
try db.query(sql: "INSERT INTO parent VALUES ('John', 'Mary'), ('John', 'Sally')")

// Datalog
import Datalog
try db.assert(datalog: "parent('Mary', 'Tom')")
```

Both are equivalent ways of asserting facts. Whichever form you use, formulas are canonicalized before they are stored, and asserting an equivalent fact again, using either syntax, is a no-op.

## Adding rules

RBDB supports logical rules restricted to safe Horn clauses with guard conditions. In a safe Horn clause, all variables in the head must appear in at least one positive literal in the body.

Rules can only be asserted in Datalog syntax:

```swift
try db.assert(datalog: "grandparent(X, Z) :- parent(X, Y), parent(Y, Z)")
```

In Datalog syntax, `:-` means "**if**" and `,` means "**and**." So the above rule states, "X is the grandparent of Z **if** X is the parent of Y, **and** Y is the parent of Z."

Rules can also have boolean guard conditions:

```swift
try db.assert(datalog: "sibling(X, Y) :- parent(P, X), parent(P, Y), X != Y")
```

## Querying

Querying a predicate returns asserted facts along with everything the rules derive:

```swift
let result = try db.query(sql: "SELECT * FROM grandparent")
// grandchild | grandparent
// -----------+------------
// Tom        | John
```

You can also query using Datalog syntax:

```swift
let result = try db.query(datalog: "sibling(X, Y)")
// X     | Y
// ------+------
// Mary  | Sally
// Sally | Mary
```

## Taking things back

``RBDB/RBDB/retract(formula:)`` — or `retract(datalog:)` — takes back something you asserted.

Retracting does not actually delete any data — it is simply marked as superceded, so it is possible to query what was known to be true at an earlier time.

```swift
try db.retract(datalog: "parent('Mary', 'Tom')")
```

SQL `DELETE` means the same thing:

```swift
try db.query(sql: "DELETE FROM parent WHERE parent = 'Mary' AND child = 'Tom'")
```

**Retraction operates on the base, not the closure.** You can only take back what you asserted; a conclusion is retracted by removing what derives it. Two consequences worth knowing before they surprise you:

- Retracting something derivable but not stored throws `RetractionError.notFound`. For example, `grandparent('John', 'Tom')` follows from two `parent` facts, so retract one of those instead.
- A row that is *both* asserted and derivable **stays visible** after a successful `DELETE`. The assertion was retracted; the conclusion still follows.

Retracting a rule works the same way, and its conclusions go with it:

```swift
try db.retract(datalog: "grandparent(X, Z) :- parent(X, Y), parent(Y, Z)")
```

## Saying something is false

Retracting `p(1)` says you no longer know it to be true. That is a different claim from knowing it to be *false*, which is strong negation, `-p(1)`. RBDB is open-world, so the difference shows up in query answers:

| | `?- p(1)` | `?- -p(1)` |
|---|---|---|
| `p(1)` asserted | yes | no |
| `p(1)` retracted | no | no — *unknown* |
| `-p(1)` asserted | no | yes — *known false* |

`p` and `-p` are separate relations, but they both carry the same columns. You don't create `-p` yourself; it is automatically created for you when you create `p`.

An assert that would leave both `p(t̄)` and `-p(t̄)` derivable throws `CoherenceError.contradiction`. Retract the contradicting fact and then you can assert its inverse:

```swift
try db.retract(datalog: "parent('John', 'Mary').")   // now unknown
try db.assert(datalog: "-parent('John', 'Mary').")   // now known false
```

Negative predicates can also be the head of rules:

```swift
try db.assert(datalog: "-parent(X, Y) :- disowned(X, Y).")
```

One shape can't be checked: where *both* polarities generate values (arithmetic under recursion, so
each can run past any finite set of arguments), whether they ever agree on one is not a question the
engine can settle. Rather than accept such writes unchecked, the write that would leave a relation
that way throws `CoherenceError.undecidable` — retract either side's value-generating rule and it
goes through.

## Next steps

The `Datalog` module documents the datalog surface — the parser, and the `datalog()` SQL
function for querying from SQL directly.
