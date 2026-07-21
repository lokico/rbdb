# RBDB

A relational database built on top of SQLite with integrated Datalog capabilities.

## Installation

### Swift Package Manager

**Note:** RBDB is under active development and breaking changes may occur. We recommend pinning to the latest commit hash until we start making versioned releases.

Add to your `Package.swift`:

```swift
dependencies: [
    .package(url: "https://github.com/chkn/rbdb.git", revision: "COMMIT_HASH_HERE")
]
```
### SQLite Dependency

RBDB requires SQLite 3.45.0 or newer built with `SQLITE_ENABLE_MATH_FUNCTIONS` (the default in most builds). The system SQLite on macOS is known to work. Otherwise, you'll need to build SQLite and supply a module map (see Dockerfile for an example).

## Usage

### Create database and define a predicate

```swift
import RBDB

let db = try RBDB(path: "database.db")

// Defines a `user` predicate
try db.query(sql: "CREATE TABLE user(name)")
```

### Assert facts

You can assert simple facts in these equivalent ways:

```swift
// 1. Create Formula directly
let formula1 = Formula.predicate(Predicate(name: "user", arguments: [.string("Alice")]))
try db.assert(formula: formula1)

// 2. Parse datalog into Formula manually
import Datalog
let formula2 = try DatalogParser().parse("user('Alice')")
assert(formula1 == formula2)  // true
try db.assert(formula: formula2) // fails if formula1 was already asserted

// 3. Datalog convenience extension (requires `import Datalog`)
try db.assert(datalog: "user('Alice')") // fails if already asserted above

// 4. SQL INSERT
try db.query(sql: "INSERT INTO user(name) VALUES ('Alice')") // fails if either formula above was already asserted
```

All approaches above are equivalent ways of asserting the same fact. As noted in the code comments, you can only assert a fact once. Subsequent attempts to assert an equivalent fact will trigger a unique constraint failure in the database.

### Rules

RBDB supports logical rules restricted to safe Horn clauses. A safe Horn clause has at most one positive literal (the head) and all variables in the head must appear in at least one positive literal in the body.

Here's a simple example showing how to define rules and query them:

```swift
import RBDB
import Datalog

let db = try RBDB(path: "family.db")

// Create tables for our predicates
try db.query(sql: "CREATE TABLE parent(parent, child)")
try db.query(sql: "CREATE TABLE grandparent(grandparent, grandchild)")

// Assert some facts using datalog syntax
try db.assert(datalog: "parent('John', 'Mary')")
try db.assert(datalog: "parent('Mary', 'Tom')")
try db.assert(datalog: "parent('Bob', 'Alice')")

// Define a rule: X is the grandparent of Z if X is the parent of Y and Y is the parent of Z
try db.assert(datalog: "grandparent(X, Z) :- parent(X, Y), parent(Y, Z)")

// Query back using SQL
let result = try db.query(sql: "SELECT * FROM grandparent")
// grandchild | grandparent
// -----------+------------
// Tom        | John
```

### Canonicalize logically equivalent formulas

```swift
let x = Var()
let y = Var()
let f1 = Formula.predicate(Predicate(name: "User", arguments: [.variable(x)]))
let f2 = Formula.predicate(Predicate(name: "User", arguments: [.variable(y)]))
assert(f1.canonicalize() == f2.canonicalize())  // true
```

### Interactive CLI Tool

The included `rbdb` command provides an interactive console that supports both SQL and datalog modes. Use Shift+Tab to switch between modes:

```bash
# Interactive mode
swift run rbdb database.db

# Execute file
swift run rbdb -f script.sql database.db

# In-memory database
swift run rbdb
```

Example session:
```sql
sql> CREATE TABLE product (id, name, price);
sql> INSERT INTO product VALUES (1, 'Widget', 9.99);
sql> SELECT * FROM product;
┌────┬────────┬───────┐
│ id │ name   │ price │
├────┼────────┼───────┤
│ 1  │ Widget │ 9.99  │
└────┴────────┴───────┘

# Switch to datalog mode with Shift+Tab
datalog> ?- product(ID, Name, Price).
┌────┬────────┬───────┐
│ ID │ Name   │ Price │
├────┼────────┼───────┤
│ 1  │ Widget │ 9.99  │
└────┴────────┴───────┘
```

Note that datalog variables must start with an uppercase letter, but the results are equivalent between SQL and datalog queries.

## Docker & Containerization

The provided Dockerfile creates a complete Swift build environment with RBDB dependencies, including a custom SQLite build. This can be used as a builder stage for containerized services.

### Building RBDB in Docker

```bash
# Build the RBDB development/build environment
docker build -t rbdb-builder .

# Run tests
docker run --rm rbdb-builder swift test

# Build release binaries
docker run --rm rbdb-builder swift build -c release
```

### Multi-stage Build for Services

To containerize a service that depends on RBDB, use a multi-stage build pattern:

```dockerfile
# Use RBDB builder as base
FROM rbdb-builder as builder

# Copy your service code
COPY your-service/ /service/
WORKDIR /service

# Build your service with RBDB dependency
RUN swift build -c release

# Production stage
FROM ubuntu:latest
RUN apt-get update && apt-get install -y \
    libsqlite3-0 \
    && rm -rf /var/lib/apt/lists/*

# Copy custom SQLite library and your service binary
COPY --from=builder /usr/local/lib/libsqlite3.so* /usr/local/lib/
COPY --from=builder /service/.build/release/your-service /usr/local/bin/
RUN ldconfig

CMD ["your-service"]
```

This approach:
- Leverages the RBDB build environment with proper SQLite configuration
- Produces lightweight production containers with only runtime dependencies
- Maintains a custom SQLite build that could be easily customized further

## Development

### Prerequisites

- Swift 6.0 or later
- SQLite 3.45.0 or newer (e.g. system SQLite on macOS)

### Building from Source

```bash
swift build
```

### Running Tests

```bash
swift test
```

### Code Formatting

The project uses `swift-format` for consistent code style. Run this to format all source files:

```bash
swift format -i -r .
```


## Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

### Code Style

- Follow Swift naming conventions
- Use tabs for indentation
- Maintain test coverage for new features
- Try to add documentation for public APIs

## License

[MIT](LICENSE)

