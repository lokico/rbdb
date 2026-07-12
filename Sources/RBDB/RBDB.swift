import Foundation
import SQLite3

public enum RBDBError: Error {
	case corruptData(message: String)
}

public class RBDB: SQLiteDatabase {
	private var isInitializing = false

	// FIXME: Can we validate that it's actually an RBDB?
	public override init(path: String) throws {
		try super.init(path: path)

		// Set flag to allow schema tables to be created during initialization
		isInitializing = true
		defer { isInitializing = false }

		// Register the custom predicate_formula() function
		let result = sqlite3_create_function(
			db,  // Database connection
			"predicate_formula",  // Function name
			-1,  // Number of arguments (-1 for var args)
			SQLITE_UTF8 | SQLITE_DETERMINISTIC,
			nil,  // User data pointer (not needed)
			predicateFormulaSQLiteFunction,  // Function implementation
			nil,  // Step function (for aggregates)
			nil  // Final function (for aggregates)
		)
		if result != SQLITE_OK {
			sqlite3_close(db)
			throw SQLiteError.couldNotRegisterFunction(
				name: "predicate_formula"
			)
		}

		// Migrate the schema
		try super.query(
			sql: SQL(String(decoding: PackageResources.schema_sql, as: UTF8.self))
		)
	}

	private class RBDBCursor: SQLiteCursor {
		private let rbdb: RBDB
		init(_ rbdb: RBDB, sql: SQL) throws {
			self.rbdb = rbdb
			try super.init(rbdb, sql: sql)
		}
		override func step(statement: SQLiteCursor.PreparedStatement) throws -> Bool {
			if !rbdb.isInitializing {
				if let normalizedSQL = sqlite3_normalized_sql(statement.ptr) {
					let sqlString = String(cString: normalizedSQL)
					if sqlString.hasPrefix("CREATE TABLE") {
						try rbdb.interceptCreateTable(sqlString)

						// Return empty result set instead of letting SQLite execute
						//  the CREATE TABLE
						return false
					}
				}
			}
			return try super.step(statement: statement)
		}
	}

	@discardableResult
	public override func query(sql: SQL) throws -> SQLiteCursor {
		do {
			return try RBDBCursor(self, sql: sql)
		} catch let error as SQLiteError {
			// Only attempt to rescue if we have an index to resume from, so
			//  we don't risk re-executing any potentially non-idempotent commands.
			if case .queryError(_, let index) = error, let index = index,
				let cursor = try rescue(error: error, in: sql, at: index)
			{
				return cursor
			}
			throw error
		}
	}

	public func assert(formula: Formula) throws {
		// Validate the formula (predicates exist, no unsafe variables, etc.)
		try validate(formula: formula)

		let jsonStr = try formulaToJSON(formula)

		try super.query(sql: "BEGIN TRANSACTION")
		do {
			try super.query(
				sql: sqlForInsert(
					ofFormula: jsonStr,
					usingParameters: true
				))
			try super.query(sql: "COMMIT")
		} catch {
			try super.query(sql: "ROLLBACK")
			throw error
		}
	}

	public func query(formula: Formula) throws -> SQLiteCursor {
		// Not running `validate` here because we want to allow variables in the head
		// that aren't in the body in this case. Also not canonicalizing because we
		// want to preserve the variable names for the columns of the result set.

		var columnsQuery: SQLiteCursor? = nil
		let sql = try formula.queryIntoSQL({ predicateName in
			guard
				let columns = try self.getColumns(for: predicateName, query: &columnsQuery)
			else {
				throw SQLiteError.queryError("no such table: \(predicateName)")
			}
			return columns
		})

		// Not calling `super.query` here because we want the view generation
		return try query(sql: SQL(sql))
	}

	private func validatePredicatesExist(in formula: Formula) throws {
		var predicateNames = try formula.getPredicateNames()
		guard !predicateNames.isEmpty else { return }

		let placeholders = Array(repeating: "?", count: predicateNames.count).joined(
			separator: ", ")
		let results = try super.query(
			sql: SQL(
				"SELECT name FROM _predicate WHERE name IN (\(placeholders))",
				arguments: Array(predicateNames))
		)

		for row in results {
			if let name = row["name"] as? String {
				predicateNames.remove(name)
			}
		}

		if let missing = predicateNames.first {
			throw SQLiteError.queryError("no such table: \(missing)")
		}
	}

	// FIXME: Make this public?
	private func validate(formula: Formula) throws {
		try formula.validate()
		try validatePredicatesExist(in: formula)
	}

	private func sqlForInsert(ofFormula expr: String, usingParameters: Bool) -> SQL {
		SQL(
			"""
			INSERT INTO _entity (internal_entity_id) VALUES (NULL);
			INSERT INTO _rule (internal_entity_id, formula)
			VALUES (last_insert_rowid(), jsonb(\(usingParameters ? expr : SQL(expr))))
			""")
	}

	func interceptCreateTable(_ sql: String) throws {
		guard
			let createTable = try ParsedCreateTable(
				sql: sql
			)
		else {
			throw SQLiteError.queryError(
				"Cannot parse CREATE TABLE statement: \(sql)"
			)
		}
		let columnNamesJson = try String(
			data: JSONSerialization.data(
				withJSONObject: createTable.columnNames
			),
			encoding: .utf8
		)!

		try super.query(sql: "BEGIN TRANSACTION")
		do {
			try super.query(sql: "INSERT INTO _entity DEFAULT VALUES")

			// Insert into predicate table using the last inserted entity ID and jsonb function
			// Use INSERT OR IGNORE if IF NOT EXISTS was specified
			let orIgnore = SQL(createTable.ifNotExists ? "OR IGNORE " : "")
			let insertSQL: SQL = """
					INSERT \(orIgnore)INTO _predicate (internal_entity_id, name, column_names)
					VALUES (last_insert_rowid(), \(createTable.tableName), jsonb(\(columnNamesJson)))
				"""
			try super.query(sql: insertSQL)

			try super.query(sql: "COMMIT")
		} catch {
			try super.query(sql: "ROLLBACK")
			throw error
		}

		// Optimization: Since we already have the column names parsed out, let's just
		// create the view and trigger too, so the table is usable right away.
		// If this fails, ignore the error - rescue will handle it later when needed
		try? createViewAndTrigger(
			for: createTable.tableName,
			columns: createTable.columnNames,
			rules: []  // We know there can't be any rules yet since the table didn't exist before
		)
	}

	/// The `SELECT` over `_rule` that yields a predicate's base facts — rows asserted with no body.
	/// Each column is aliased to its predicate column name. `arg1`/`arg2` are indexed columns; any
	/// further columns are read out of the JSON `formula`. Since facts have no body variables, the
	/// arguments are always constants and are selected directly.
	func baseFactsSelect(for name: String, columns: [String]) -> String {
		let selectList = columns.enumerated().map { idx, column -> String in
			let i = idx + 1
			let value = i <= 2 ? "arg\(i)_constant" : "json_extract(formula, '$[1][\(i - 1)].\"\"')"
			return "\(value) AS [\(column)]"
		}
		return """
			SELECT \(selectList.joined(separator: ", "))
			FROM _rule
			WHERE output_type = '@\(name)'
			  AND negative_literal_count = 0
			"""
	}

	private func createViewAndTrigger(for tableName: String, columns: [String], rules: [Formula])
		throws
	{
		let columnList = columns.map { "[\($0)]" }.joined(separator: ", ")

		var selects = [baseFactsSelect(for: tableName, columns: columns)]

		for rule in rules {
			var columnsQuery: SQLiteCursor? = nil
			let ruleSQL = try rule.ruleIntoSQL({ predicateName in
				guard
					let columns = try self.getColumns(
						for: predicateName, query: &columnsQuery)
				else {
					throw RBDBError.corruptData(
						message:
							"table '\(tableName)' references unknown predicate '\(predicateName)'"
					)
				}
				return columns
			})
			selects.append(ruleSQL)
		}

		let unionedSelects = selects.joined(separator: "\nUNION\n")
		let createViewSQL =
			"CREATE TEMP VIEW IF NOT EXISTS \(tableName) (\(columnList)) AS \(unionedSelects)"

		try super.query(sql: SQL(createViewSQL))

		// Create INSTEAD OF INSERT trigger
		let predicateFormulaCall =
			"predicate_formula('\(tableName)', "
			+ columns.map { "NEW.[\($0)]" }.joined(separator: ", ")
			+ ")"

		let createTrigger =
			"""
			CREATE TEMP TRIGGER IF NOT EXISTS \(tableName)_insert_trigger
			INSTEAD OF INSERT ON \(tableName)
			FOR EACH ROW
			BEGIN
			\(sqlForInsert(ofFormula: predicateFormulaCall, usingParameters: false).queryText);
			END
			"""
		try super.query(sql: SQL(createTrigger))
	}

	// Returns nil on unknown predicate
	func getColumns<T: StringProtocol>(for predicate: T) throws -> [String]? {
		var cursor: SQLiteCursor? = nil
		return try getColumns(for: predicate, query: &cursor)
	}

	func getColumns<T: StringProtocol>(for predicate: T, query: inout SQLiteCursor?) throws
		-> [String]?
	{
		var cursor: SQLiteCursor
		if let q = query {
			cursor = try q.rerun(withArguments: [predicate])
		} else {
			cursor = try super.query(
				sql:
					"SELECT json(column_names) as json_array FROM _predicate WHERE name = \(predicate)"
			)
			query = cursor
		}
		let iter = cursor.makeIterator()
		guard let predicate = iter.next() else { return nil }

		// Duplicate predicates in DB: shouldn't happen; throw corruptData error
		guard iter.next() == nil else {
			throw RBDBError.corruptData(message: "duplicate predicate '\(predicate)'")
		}

		guard
			let columnNamesJson = predicate["json_array"] as? String,
			let columnNamesData = columnNamesJson.data(using: .utf8),
			let columnNames = try? JSONDecoder().decode([String].self, from: columnNamesData),
			!columnNames.isEmpty
		else {
			throw RBDBError.corruptData(
				message: "expected JSON array in _predicate.column_names"
			)
		}

		return columnNames
	}

	private func rescue(error: SQLiteError, in sql: SQL, at startIndex: SQL.Index) throws
		-> SQLiteCursor?
	{
		guard case .queryError(let msg, _) = error,
			let match = msg.firstMatch(of: /no such table: ([^\s]+)/),
			let columnNames = try getColumns(for: match.1)
		else {
			return nil
		}
		let predicateName = String(match.1)

		let retrySQL: SQL
		if try involvesRecursion(predicateName) {
			// A predicate that (transitively) involves recursion can't be a plain view: it only ever
			//  exists as a query-time CTE, and a view body can't reference a CTE. So we materialize the
			//  whole recursive closure — the predicate plus every recursive predicate it depends on —
			//  as CTEs in one `WITH RECURSIVE` statement prepended to the failing query.
			//
			// We can't push the bounds via a plain view either: SQLite won't push the query's WHERE
			//  down into the recursive arm of a CTE, so the recursion wouldn't terminate. Instead we
			//  derive the bounds ourselves and inject them into each recursive step.
			// See https://github.com/sqlite/sqlite/blob/6176034151d10b29a38b0e67f27818a719c68139/src/select.c#L5054
			retrySQL = try buildRecursiveClosureCTE(
				topPredicate: predicateName,
				topColumns: columnNames,
				sql: sql,
				startIndex: startIndex
			)
		} else {
			let rules = try fetchRules(for: predicateName)
			try createViewAndTrigger(for: predicateName, columns: columnNames, rules: rules)
			retrySQL = sql.at(startIndex: startIndex)
		}
		return try query(sql: retrySQL)
	}

	func fetchRules(for predicateName: String) throws -> [Formula] {
		let cursor = try super.query(
			sql: """
				SELECT json(formula) as json
				FROM _rule
				WHERE negative_literal_count > 0
				  AND output_type = \("@\(predicateName)")
				""")
		let decoder = JSONDecoder()
		var rules: [Formula] = []
		for row in cursor {
			guard let json = row["json"] as? String,
				let data = json.data(using: .utf8)
			else {
				throw RBDBError.corruptData(
					message: "expected json stored as UTF-8 in _rule.formula")
			}
			rules.append(try decoder.decode(Formula.self, from: data))
		}
		return rules
	}
}
