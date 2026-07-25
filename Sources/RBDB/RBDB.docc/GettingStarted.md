# Getting Started

Create a database, assert some facts, and let RBDB derive the rest.

## Opening a database

A ``RBDB`` instance wraps a SQLite database. Creating a table defines a predicate:

```swift
import RBDB

let db = try RBDB(path: "family.db")

try db.query(sql: "CREATE TABLE parent(parent, child)")
try db.query(sql: "CREATE TABLE grandparent(grandparent, grandchild)")
```

## Asserting facts

Facts are ``Formula`` values. You can build one directly, or — with the `Datalog` module —
parse it from datalog syntax:

```swift
import Datalog

try db.assert(datalog: "parent('John', 'Mary')")
try db.assert(datalog: "parent('Mary', 'Tom')")
```

A plain SQL `INSERT` asserts the same fact. Whichever form you use, a given fact can only be
asserted once: asserting an equivalent fact again raises a unique constraint failure, because
formulas are canonicalized before they are stored.

## Adding rules

Rules are asserted the same way facts are, and are restricted to safe Horn clauses — at most one
positive literal in the head, with every head variable appearing in a positive literal of the body:

```swift
try db.assert(datalog: "grandparent(X, Z) :- parent(X, Y), parent(Y, Z)")
```

Querying a predicate returns asserted facts along with everything the rules derive:

```swift
let result = try db.query(sql: "SELECT * FROM grandparent")
// grandchild | grandparent
// -----------+------------
// Tom        | John
```

## Next steps

The `Datalog` module documents the datalog surface — the parser, and the `datalog()` SQL
function for querying from SQL directly.
