# RBDB

A hybrid SQL/Datalog relational database built on top of SQLite.

## Installation

### Swift Package Manager

**Note:** RBDB is under active development and breaking changes may occur. We recommend pinning to the latest commit hash until we start making versioned releases.

Add to your `Package.swift`:

```swift
dependencies: [
    .package(url: "https://github.com/lokico/rbdb", revision: "COMMIT_HASH_HERE")
]
```
### SQLite Dependency

RBDB requires SQLite 3.45.0 or newer built with `SQLITE_ENABLE_MATH_FUNCTIONS` (the default in most builds). The system SQLite on macOS is known to work.

## Usage

See the [Getting Started](https://lokico.github.io/rbdb/documentation/rbdb/gettingstarted) guide for a quick overview of the Swift API.

### Interactive CLI Tool

The included `rbdb1` command provides an interactive console that supports both SQL and datalog modes. Use Shift+Tab to switch between modes:

```bash
# Interactive mode
swift run rbdb1 database.db

# Execute file
swift run rbdb1 -f script.sql database.db

# In-memory database
swift run rbdb1
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

### Documentation

Documentation lives in DocC catalogs alongside the sources (`Sources/RBDB/RBDB.docc`), and is published to GitHub Pages on every push to `main`. The published site is a combined archive covering both the `RBDB` and `Datalog` modules, with a landing page linking each.

To build and preview the same thing locally:

```bash
./Scripts/preview-docs.sh
```

That serves at <http://localhost:8080/documentation/>. Since DocC can only preview one target at a time, use `--live` for a server that rebuilds as you edit a single module:

```bash
./Scripts/preview-docs.sh --live --target RBDB
```

Pass `--port` to serve elsewhere; `--help` lists all options.

To add a guide or conceptual article, drop a Markdown file in `Sources/RBDB/RBDB.docc` and curate it under a `## Topics` section in `RBDB.md`.

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
