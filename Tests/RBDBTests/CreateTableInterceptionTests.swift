import Foundation
import Testing

@testable import RBDB

@Suite("CREATE TABLE Interception Tests")
struct CreateTableInterceptionTests {

	@Test(
		"Simple CREATE TABLE is intercepted and recorded in predicate table",
		arguments: [nil, "This is a comment!"])
	func simpleCreateTableInterception(comment: String?) async throws {
		let rbdb = try RBDB(path: ":memory:")

		var sql: String
		if let comment = comment {
			sql =
				"CREATE TABLE users (\n --! \(comment)\n id INTEGER, /* c-style\n multi-line */ name TEXT, email TEXT)"
		} else {
			sql = "CREATE TABLE users (id INTEGER, -- ignored\n name TEXT, email TEXT)"
		}

		// Execute a CREATE TABLE statement
		try rbdb.query(sql: SQL(sql))

		// Check that the predicate was recorded
		let results = Array(
			try rbdb.query(
				sql:
					"SELECT name, descr, json(column_names) as column_names_json FROM _predicate WHERE name = 'users'"
			))

		#expect(results.count == 1, "Should have one predicate record")
		#expect(
			results[0]["name"] as? String == "users",
			"Table name should be recorded"
		)
		#expect(
			results[0]["descr"] as? String == comment,
			"Table comment should be recorded"
		)

		if let columnNamesJson = results[0]["column_names_json"] as? String,
			let columnNamesData = columnNamesJson.data(using: .utf8)
		{
			let columnNames =
				try JSONSerialization.jsonObject(with: columnNamesData)
				as? [String]
			#expect(
				columnNames == ["id", "name", "email"],
				"Column names should be parsed correctly"
			)
		} else {
			#expect(Bool(false), "column_names should be accessible as JSON")
		}
	}

	@Test("CREATE TABLE IF NOT EXISTS is intercepted")
	func createTableIfNotExistsInterception() async throws {
		let rbdb = try RBDB(path: ":memory:")

		// Execute a CREATE TABLE IF NOT EXISTS statement
		try rbdb.query(
			sql:
				"CREATE TABLE IF NOT EXISTS products (id INTEGER PRIMARY KEY, name TEXT NOT NULL, price REAL)"
		)

		// Check that the predicate was recorded
		let results = Array(
			try rbdb.query(
				sql:
					"SELECT name, json(column_names) as column_names_json FROM _predicate WHERE name = 'products'"
			))

		#expect(results.count == 1, "Should have one predicate record")
		#expect(
			results[0]["name"] as? String == "products",
			"Table name should be recorded"
		)

		if let columnNamesJson = results[0]["column_names_json"] as? String,
			let columnNamesData = columnNamesJson.data(using: .utf8)
		{
			let columnNames =
				try JSONSerialization.jsonObject(with: columnNamesData)
				as? [String]
			#expect(
				columnNames == ["id", "name", "price"],
				"Column names should be parsed correctly"
			)
		} else {
			#expect(Bool(false), "column_names should be accessible as JSON")
		}
	}

	@Test("CREATE TABLE with complex column definitions")
	func createTableWithComplexColumns() async throws {
		let rbdb = try RBDB(path: ":memory:")

		// Execute a CREATE TABLE with various column types and constraints
		try rbdb.query(
			sql: """
				    CREATE TABLE complex_table (
				        id INTEGER PRIMARY KEY AUTOINCREMENT,
				        name VARCHAR(255) NOT NULL,
				        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
				        price DECIMAL(10,2),
				        data BLOB
				    )
				"""
		)

		// Check that the predicate was recorded
		let results = Array(
			try rbdb.query(
				sql:
					"SELECT name, json(column_names) as column_names_json FROM _predicate WHERE name = 'complex_table'"
			))

		#expect(results.count == 1, "Should have one predicate record")
		#expect(
			results[0]["name"] as? String == "complex_table",
			"Table name should be recorded"
		)

		if let columnNamesJson = results[0]["column_names_json"] as? String,
			let columnNamesData = columnNamesJson.data(using: .utf8)
		{
			let columnNames =
				try JSONSerialization.jsonObject(with: columnNamesData)
				as? [String]
			#expect(
				columnNames == ["id", "name", "created_at", "price", "data"],
				"Column names should be parsed correctly"
			)
		} else {
			#expect(Bool(false), "column_names should be accessible as JSON")
		}
	}

	@Test("CREATE TABLE with quoted column names throws error")
	func createTableWithQuotedColumnsThrows() async throws {
		let rbdb = try RBDB(path: ":memory:")

		// Execute a CREATE TABLE with quoted column names should throw an error
		#expect(throws: SQLiteError.self) {
			try rbdb.query(
				sql: """
					    CREATE TABLE quoted_table (
					        "user id" INTEGER,
					        name TEXT
					    )
					"""
			)
		}
	}

	@Test("Multiple CREATE TABLE statements are all intercepted")
	func multipleCreateTableStatements() async throws {
		let rbdb = try RBDB(path: ":memory:")

		// Execute multiple CREATE TABLE statements
		try rbdb.query(
			sql: """
				    CREATE TABLE table1 (id INTEGER, name TEXT);
				    CREATE TABLE table2 (id INTEGER, value REAL);
				    CREATE TABLE table3 (id INTEGER, data BLOB);
				"""
		)

		// Check that all predicates were recorded
		let results = Array(
			try rbdb.query(
				sql: "SELECT name FROM _predicate ORDER BY name"
			))

		#expect(results.count == 3, "Should have three predicate records")

		let tableNames = results.compactMap { $0["name"] as? String }
		#expect(
			tableNames == ["table1", "table2", "table3"],
			"All table names should be recorded"
		)
	}

	@Test("CREATE TABLE with table name containing special characters")
	func createTableWithSpecialTableName() async throws {
		let rbdb = try RBDB(path: ":memory:")

		// Execute a CREATE TABLE with a table name that might need quoting
		try rbdb.query(sql: "CREATE TABLE \"user-data\" (id INTEGER, info TEXT)")

		// Check that the predicate was recorded
		let results = Array(
			try rbdb.query(
				sql:
					"SELECT name, json(column_names) as column_names_json FROM _predicate WHERE name = 'user-data'"
			))

		#expect(results.count == 1, "Should have one predicate record")
		#expect(
			results[0]["name"] as? String == "user-data",
			"Table name should be recorded without quotes"
		)

		if let columnNamesJson = results[0]["column_names_json"] as? String,
			let columnNamesData = columnNamesJson.data(using: .utf8)
		{
			let columnNames =
				try JSONSerialization.jsonObject(with: columnNamesData)
				as? [String]
			#expect(
				columnNames == ["id", "info"],
				"Column names should be parsed correctly"
			)
		}
	}

	@Test("Unparseable CREATE TABLE statement throws error")
	func unparseableCreateTableThrows() async throws {
		let rbdb = try RBDB(path: ":memory:")

		// Execute a malformed CREATE TABLE statement should throw an error
		#expect(throws: SQLiteError.self) {
			try rbdb.query(sql: "CREATE TABLE malformed_table")
		}
	}

	@Test("CREATE TABLE with column names only (no types)")
	func createTableWithColumnNamesOnly() async throws {
		let rbdb = try RBDB(path: ":memory:")

		// Execute a CREATE TABLE with column names but no explicit types
		try rbdb.query(sql: "CREATE TABLE simple_table (id, name, value)")

		// Check that the predicate was recorded
		let results = Array(
			try rbdb.query(
				sql:
					"SELECT name, json(column_names) as column_names_json FROM _predicate WHERE name = 'simple_table'"
			))

		#expect(results.count == 1, "Should have one predicate record")
		#expect(
			results[0]["name"] as? String == "simple_table",
			"Table name should be recorded"
		)

		if let columnNamesJson = results[0]["column_names_json"] as? String,
			let columnNamesData = columnNamesJson.data(using: .utf8)
		{
			let columnNames =
				try JSONSerialization.jsonObject(with: columnNamesData)
				as? [String]
			#expect(
				columnNames == ["id", "name", "value"],
				"Column names should be parsed correctly"
			)
		} else {
			#expect(Bool(false), "column_names should be accessible as JSON")
		}
	}

	@Test("CREATE TABLE IF NOT EXISTS silently succeeds when table exists")
	func createTableIfNotExistsWhenTableExists() async throws {
		let rbdb = try RBDB(path: ":memory:")

		// First create the table
		try rbdb.query(sql: "CREATE TABLE test_table (id INTEGER, name TEXT)")

		// Verify it was created
		let initialResults = Array(
			try rbdb.query(
				sql: "SELECT name FROM _predicate WHERE name = 'test_table'"
			))
		#expect(
			initialResults.count == 1,
			"Should have one predicate record initially"
		)

		// Try to create the same table again with IF NOT EXISTS - should not throw
		try rbdb.query(
			sql: "CREATE TABLE IF NOT EXISTS test_table (id INTEGER, name TEXT, extra_col TEXT)"
		)

		// Verify we still have only one record (the original one)
		let finalResults = Array(
			try rbdb.query(
				sql: "SELECT name FROM _predicate WHERE name = 'test_table'"
			))
		#expect(
			finalResults.count == 1,
			"Should still have only one predicate record"
		)
	}

	@Test("CREATE TABLE without IF NOT EXISTS fails when table exists")
	func createTableWithoutIfNotExistsWhenTableExists() async throws {
		let rbdb = try RBDB(path: ":memory:")

		// First create the table
		try rbdb.query(sql: "CREATE TABLE test_table (id INTEGER, name TEXT)")

		// Try to create the same table again without IF NOT EXISTS - should throw
		#expect(throws: SQLiteError.self) {
			try rbdb.query(sql: "CREATE TABLE test_table (id INTEGER, name TEXT)")
		}
	}

	@Test("CREATE TABLE with WITHOUT ROWID / STRICT table options")
	func createTableWithTableOptions() async throws {
		let rbdb = try RBDB(path: ":memory:")

		try rbdb.query(
			sql: "CREATE TABLE options_table (id INTEGER PRIMARY KEY, name TEXT) STRICT"
		)

		let results = Array(
			try rbdb.query(
				sql:
					"SELECT name, json(column_names) as column_names_json FROM _predicate WHERE name = 'options_table'"
			))

		#expect(results.count == 1, "Should have one predicate record")

		if let columnNamesJson = results[0]["column_names_json"] as? String,
			let columnNamesData = columnNamesJson.data(using: .utf8)
		{
			let columnNames =
				try JSONSerialization.jsonObject(with: columnNamesData)
				as? [String]
			#expect(
				columnNames == ["id", "name"],
				"Column names should be parsed correctly, ignoring trailing table options"
			)
		} else {
			#expect(Bool(false), "column_names should be accessible as JSON")
		}
	}

	@Test(
		"CREATE TABLE with a table-level constraint throws since constraints aren't enforced",
		arguments: [
			"UNIQUE(name)",
			"UNIQUE (name)",
			"FOREIGN KEY (customer_id) REFERENCES customers(id)",
			"CHECK(quantity > 0)",
			"CHECK (quantity > 0)",
			"PRIMARY KEY (id, customer_id)",
			"CONSTRAINT pk PRIMARY KEY (id)",
		])
	func createTableWithTableConstraintThrows(constraint: String) async throws {
		let rbdb = try RBDB(path: ":memory:")

		#expect(throws: SQLiteError.self) {
			try rbdb.query(
				sql: """
					CREATE TABLE orders (
					    id INTEGER,
					    customer_id INTEGER,
					    quantity INTEGER,
					    name TEXT,
					    \(constraint)
					)
					"""
			)
		}
	}

	@Test("CREATE TABLE with multi-line table comment")
	func createTableWithMultiLineComment() async throws {
		let rbdb = try RBDB(path: ":memory:")

		try rbdb.query(
			sql: """
				CREATE TABLE commented_table (
				 --! First line of comment
				 --! Second line of comment
				 id INTEGER, name TEXT)
				"""
		)

		let results = Array(
			try rbdb.query(
				sql: "SELECT descr FROM _predicate WHERE name = 'commented_table'"
			))

		#expect(results.count == 1, "Should have one predicate record")
		#expect(
			results[0]["descr"] as? String == "First line of comment\nSecond line of comment",
			"Multi-line comment should be joined with newlines"
		)
	}

	@Test("CREATE TEMP TABLE is not intercepted")
	func createTempTableIsNotIntercepted() async throws {
		let rbdb = try RBDB(path: ":memory:")

		// CREATE TEMP TABLE doesn't match the "CREATE TABLE" prefix check, so it should
		//  fall through to normal SQLite execution rather than being recorded as a predicate.
		try rbdb.query(sql: "CREATE TEMP TABLE scratch (id INTEGER, name TEXT)")

		let results = Array(
			try rbdb.query(
				sql: "SELECT name FROM _predicate WHERE name = 'scratch'"
			))
		#expect(
			results.isEmpty,
			"CREATE TEMP TABLE should not be recorded in the predicate table"
		)

		// The table should still be usable directly via SQLite though.
		try rbdb.query(sql: "INSERT INTO scratch (id, name) VALUES (1, 'a')")
		let scratchResults = Array(try rbdb.query(sql: "SELECT id, name FROM scratch"))
		#expect(scratchResults.count == 1)
	}

	@Test("lowercase \"create table\" is intercepted")
	func lowercaseCreateTableIsIntercepted() async throws {
		let rbdb = try RBDB(path: ":memory:")

		try rbdb.query(sql: "create table scratch (id INTEGER, name TEXT)")

		let results = Array(
			try rbdb.query(
				sql:
					"SELECT name, json(column_names) as column_names_json FROM _predicate WHERE name = 'scratch'"
			))
		#expect(
			results.count == 1,
			"lowercase create table should be recorded in the predicate table"
		)

		if let columnNamesJson = results[0]["column_names_json"] as? String,
			let columnNamesData = columnNamesJson.data(using: .utf8)
		{
			let columnNames =
				try JSONSerialization.jsonObject(with: columnNamesData)
				as? [String]
			#expect(
				columnNames == ["id", "name"],
				"Column names should be parsed correctly"
			)
		} else {
			#expect(Bool(false), "column_names should be accessible as JSON")
		}
	}

	@Test("mixed-case \"Create Table\" is intercepted")
	func mixedCaseCreateTableIsIntercepted() async throws {
		let rbdb = try RBDB(path: ":memory:")

		try rbdb.query(sql: "Create Table scratch (id INTEGER, name TEXT)")

		let results = Array(
			try rbdb.query(
				sql: "SELECT name FROM _predicate WHERE name = 'scratch'"
			))
		#expect(
			results.count == 1,
			"mixed-case create table should be recorded in the predicate table"
		)
	}

	@Test("CREATE TABLE AS SELECT throws since it cannot be parsed as a column list")
	func createTableAsSelectThrows() async throws {
		let rbdb = try RBDB(path: ":memory:")

		try rbdb.query(sql: "CREATE TABLE source_table (id INTEGER, name TEXT)")

		#expect(throws: SQLiteError.self) {
			try rbdb.query(
				sql: "CREATE TABLE derived_table AS SELECT * FROM source_table"
			)
		}
	}

	@Test("CREATE TABLE with schema-qualified name")
	func createTableWithSchemaQualifiedName() async throws {
		let rbdb = try RBDB(path: ":memory:")

		try rbdb.query(sql: "CREATE TABLE main.qualified_table (id INTEGER, name TEXT)")

		let results = Array(
			try rbdb.query(
				sql: "SELECT name FROM _predicate WHERE name = 'main.qualified_table'"
			))

		#expect(
			results.count == 1,
			"Schema-qualified table name should be recorded as-is"
		)
	}

	@Test("CREATE TABLE with default values and generated columns")
	func createTableWithDefaultsAndGeneratedColumns() async throws {
		let rbdb = try RBDB(path: ":memory:")

		try rbdb.query(
			sql: """
				CREATE TABLE items (
				    id INTEGER PRIMARY KEY,
				    quantity INTEGER DEFAULT 0,
				    price REAL DEFAULT (1.0 + 2.0),
				    total REAL GENERATED ALWAYS AS (quantity * price) STORED
				)
				"""
		)

		let results = Array(
			try rbdb.query(
				sql:
					"SELECT json(column_names) as column_names_json FROM _predicate WHERE name = 'items'"
			))

		#expect(results.count == 1, "Should have one predicate record")

		if let columnNamesJson = results[0]["column_names_json"] as? String,
			let columnNamesData = columnNamesJson.data(using: .utf8)
		{
			let columnNames =
				try JSONSerialization.jsonObject(with: columnNamesData)
				as? [String]
			#expect(
				columnNames == ["id", "quantity", "price", "total"],
				"Column names should be parsed correctly despite parenthesized defaults"
			)
		} else {
			#expect(Bool(false), "column_names should be accessible as JSON")
		}
	}

	@Test("Failed CREATE TABLE doesn't leave orphaned entity records")
	func failedCreateTableDoesntLeaveOrphanedEntities() async throws {
		let rbdb = try RBDB(path: ":memory:")

		// First create the table
		try rbdb.query(sql: "CREATE TABLE test_table (id INTEGER, name TEXT)")

		// Count entities before failed attempt
		let entitiesBeforeResults = Array(
			try rbdb.query(
				sql: "SELECT COUNT(*) as count FROM _entity"
			))
		let entitiesBefore = entitiesBeforeResults[0]["count"] as! Int64

		// Try to create the same table again without IF NOT EXISTS - should throw
		#expect(throws: SQLiteError.self) {
			try rbdb.query(sql: "CREATE TABLE test_table (id INTEGER, name TEXT)")
		}

		// Count entities after failed attempt - should be the same
		let entitiesAfterResults = Array(
			try rbdb.query(
				sql: "SELECT COUNT(*) as count FROM _entity"
			))
		let entitiesAfter = entitiesAfterResults[0]["count"] as! Int64

		#expect(
			entitiesBefore == entitiesAfter,
			"Failed CREATE TABLE should not leave orphaned entity records"
		)
	}
}
