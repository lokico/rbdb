# Getting Started

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

## Asserting facts

Facts are ``Formula`` values. You can build them directly, but usually, you'll just use SQL or Datalog syntax:

```swift
try db.query(sql: "INSERT INTO parent VALUES ('John', 'Mary'), ('John', 'Sally')")

import Datalog
try db.assert(datalog: "parent('Mary', 'Tom')")
```

Both are equivalent ways of asserting facts. Whichever form you use, formulas are canonicalized before they are stored, and asserting an equivalent fact again, using either syntax, is a no-op.

## Adding rules

RBDB supports logical rules restricted to safe Horn clauses with guard conditions. A safe Horn clause has at most one positive literal in the head, and all variables in the head must appear in at least one positive literal in the body.

Rules can only be asserted in Datalog syntax:

```swift
try db.assert(datalog: "grandparent(X, Z) :- parent(X, Y), parent(Y, Z)")
```

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

## Next steps

The `Datalog` module documents the datalog surface — the parser, and the `datalog()` SQL
function for querying from SQL directly.
