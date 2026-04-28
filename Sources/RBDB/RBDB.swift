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
			rules: [] // We know there can't be any rules yet since the table didn't exist before
		)
	}

	private func createViewAndTrigger<T: StringProtocol>(for tableName: T, columns: [String], rules: [Formula])
		throws
	{
		let columnList = columns.map { "[\($0)]" }.joined(separator: ", ")

		// If there are no negative literals in a horn clause, it's a fact, and since we don't
		//  allow variables that only appear in the head and not the body, we shouldn't hit
		//  any variables in this case.. just assume the values are constants and select them.
		var selectList: [String] = []
		selectList.reserveCapacity(columns.count)
		for i in 1...columns.count {
			switch i {
			case 1...2: selectList.append("arg\(i)_constant")  // these are indexed
			default: selectList.append("json_extract(formula, '$[1][\(i-1)].\"\"')")  // not indexed
			}
		}

		var selects = [
			"""
			SELECT \(selectList.joined(separator: ", "))
			FROM _rule
			WHERE output_type = '@\(tableName)'
			  AND negative_literal_count = 0
			"""
		]

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
		let createViewSQL = "CREATE TEMP VIEW IF NOT EXISTS \(tableName) (\(columnList)) AS \(unionedSelects)"

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
	private func getColumns<T: StringProtocol>(for predicate: T) throws -> [String]? {
		var cursor: SQLiteCursor? = nil
		return try getColumns(for: predicate, query: &cursor)
	}

	private func getColumns<T: StringProtocol>(for predicate: T, query: inout SQLiteCursor?) throws
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
		let predicateName = match.1
		let rules = try fetchRules(for: predicateName)

		let retrySQL: SQL
		if rules.contains(where: { $0.isRecursive(for: predicateName) }) {
			// Recursion + arithmetic in head expressions can be unbounded under naive
			// fixed-point evaluation. Rather than create a view (which SQLite would
			// fully materialize and never terminate), inline a `WITH RECURSIVE` CTE
			// that shadows the missing name and pushes equality constraints from the
			// failing statement's WHERE clause into the recursive step as bounds.
			retrySQL = try inlineBoundedCTE(
				predicateName: String(predicateName),
				columnNames: columnNames,
				rules: rules,
				sql: sql,
				startIndex: startIndex
			)
		} else {
			try createViewAndTrigger(for: predicateName, columns: columnNames, rules: rules)
			retrySQL = sql.at(startIndex: startIndex)
		}
		return try query(sql: retrySQL)
	}

	private func fetchRules<S: StringProtocol>(for predicateName: S) throws -> [Formula] {
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

	/// Builds a SQL with a `WITH RECURSIVE [predicateName](cols) AS (...)` clause prepended to
	/// the failing statement (starting at `startIndex` in the original SQL). The CTE shadows the
	/// view that would otherwise be created. For each recursive rule whose head expression is a
	/// known monotonic function of a body variable bound to a constrained column in the user's
	/// WHERE clause, we wrap the rule SQL in a subquery and add the corresponding upper/lower
	/// bound — that is what makes the recursion terminate.
	private func inlineBoundedCTE(
		predicateName: String,
		columnNames: [String],
		rules: [Formula],
		sql: SQL,
		startIndex: SQL.Index
	) throws -> SQL {
		let stmt = String(sql.queryText.utf8.dropFirst(startIndex.queryOffset))!
		let constraints = extractEqualityConstraints(
			from: stmt, predicateName: predicateName, columnNames: columnNames)

		let columnList = columnNames.map { "[\($0)]" }.joined(separator: ", ")

		// Base case: facts (formulas with no negative literals for this predicate).
		var factSelectList: [String] = []
		factSelectList.reserveCapacity(columnNames.count)
		for i in 1...columnNames.count {
			switch i {
			case 1...2: factSelectList.append("arg\(i)_constant")
			default: factSelectList.append("json_extract(formula, '$[1][\(i-1)].\"\"')")
			}
		}
		var selects = [
			"""
			SELECT \(factSelectList.joined(separator: ", "))
			FROM _rule
			WHERE output_type = '@\(predicateName)'
			  AND negative_literal_count = 0
			"""
		]

		var columnsQuery: SQLiteCursor? = nil
		let getCols: (String) throws -> [String] = { name in
			guard let cols = try self.getColumns(for: name, query: &columnsQuery) else {
				throw RBDBError.corruptData(
					message: "rule for '\(predicateName)' references unknown predicate '\(name)'")
			}
			return cols
		}

		for rule in rules {
			let ruleSQL = try rule.ruleIntoSQL(getCols)
			var finalSQL = ruleSQL
			if rule.isRecursive(for: predicateName),
				let bounds = boundsForRecursiveStep(
					rule: rule,
					predicateName: predicateName,
					columnNames: columnNames,
					constraints: constraints)
			{
				// SQLite forbids referencing a recursive CTE from inside a subquery, so we can't
				//  wrap the rule SQL — inject the bound conditions directly. The rule SQL never
				//  contains a WHERE today (RuleIntoSQLReducer only puts conditions on JOIN ON
				//  clauses), but be defensive in case that changes.
				let connector =
					ruleSQL.range(of: " WHERE ", options: .caseInsensitive) != nil
					? " AND " : " WHERE "
				finalSQL = ruleSQL + connector + bounds.joined(separator: " AND ")
			}
			selects.append(finalSQL)
		}

		let cte = """
			WITH RECURSIVE [\(predicateName)] (\(columnList)) AS (
			\(selects.joined(separator: "\nUNION\n"))
			)
			"""
		return SQL(
			"\(cte)\n\(stmt)", arguments: Array(sql.arguments.dropFirst(startIndex.argumentIndex)))
	}

	/// Returns SQL bounds (e.g. `[n] <= 100`) to add to a recursive rule's step, derived from the
	/// query's equality constraints and the head expression's monotonicity in the body variables.
	/// Returns nil if no usable bound was found — the caller will leave the rule unconstrained.
	private func boundsForRecursiveStep(
		rule: Formula,
		predicateName: String,
		columnNames: [String],
		constraints: [String: String]
	) -> [String]? {
		guard !constraints.isEmpty,
			case .hornClause(let head, let bodies) = rule
		else { return nil }

		let recursiveVars: Set<Var> =
			bodies
			.filter { $0.name == predicateName }
			.flatMap(\.arguments)
			.compactMap { if case .variable(let v) = $0 { v } else { nil } }
			.reduce(into: []) { $0.insert($1) }

		var bounds: [String] = []
		for (colIdx, headTerm) in head.arguments.enumerated()
		where colIdx < columnNames.count {
			let colName = columnNames[colIdx]
			guard let bound = constraints[colName] else { continue }
			let usedVars = headTerm.freeVariables
			guard !usedVars.isEmpty, usedVars.allSatisfy({ recursiveVars.contains($0) }) else {
				continue
			}
			var common = 0
			var ok = true
			for v in usedVars {
				let m = headTerm.monotonicity(in: v)
				if m == 0 {
					ok = false
					break
				}
				if common == 0 {
					common = m
				} else if common != m {
					ok = false
					break
				}
			}
			if ok {
				bounds.append("[\(colName)] \(common > 0 ? "<=" : ">=") \(bound)")
			}
		}
		return bounds.isEmpty ? nil : bounds
	}

	/// Best-effort extraction of `[predicate].column = literal` style equality constraints from
	/// the failing statement text. Handles the form generated by `queryIntoSQL` and common
	/// hand-written variants. Returns map of column name → literal SQL expression.
	private func extractEqualityConstraints(
		from stmt: String, predicateName: String, columnNames: [String]
	) -> [String: String] {
		var result: [String: String] = [:]
		let escName = NSRegularExpression.escapedPattern(for: predicateName)
		for col in columnNames {
			let escCol = NSRegularExpression.escapedPattern(for: col)
			let pattern =
				"\\[?\\s*\(escName)\\s*\\]?\\s*\\.\\s*\\[?\\s*\(escCol)\\s*\\]?\\s*=\\s*(-?\\d+(?:\\.\\d+)?|'[^']*'|true|false)"
			guard let regex = try? NSRegularExpression(pattern: pattern, options: .caseInsensitive),
				let match = regex.firstMatch(
					in: stmt, range: NSRange(stmt.startIndex..., in: stmt)),
				let valueRange = Range(match.range(at: 1), in: stmt)
			else { continue }
			result[col] = String(stmt[valueRange])
		}
		return result
	}
}

func formulaToJSON(_ formula: Formula) throws -> String {
	let encoder = JSONEncoder()
	let canonicalFormula = formula.canonicalize()
	guard
		let jsonStr = String(
			data: try encoder.encode(canonicalFormula),
			encoding: .utf8
		)
	else {
		throw RBDBError.corruptData(
			message: "Failed to encode formula as UTF-8 JSON"
		)
	}
	return jsonStr
}

// SQLite function implementation for predicate_formula()
func predicateFormulaSQLiteFunction(
	context: OpaquePointer?,
	argc: Int32,
	argv: UnsafeMutablePointer<OpaquePointer?>?
) {
	guard argc >= 1, let argv = argv else {
		sqlite3_result_error(
			context,
			"predicate_formula() requires at least one argument",
			-1
		)
		return
	}

	// Get the predicate name (first argument)
	guard let predicateNamePtr = sqlite3_value_text(argv[0]) else {
		sqlite3_result_error(
			context,
			"predicate_formula() first argument must be a string",
			-1
		)
		return
	}
	let predicateName = String(cString: predicateNamePtr)

	// Convert remaining arguments to Terms
	var terms: [Term] = []
	for i in 1..<argc {
		let value = argv[Int(i)]
		let sqliteType = sqlite3_value_type(value)

		let term: Term
		switch sqliteType {
		case SQLITE_TEXT:
			let textPtr = sqlite3_value_text(value)
			let text = String(cString: textPtr!)
			term = .string(text)
		case SQLITE_INTEGER:
			let intValue = sqlite3_value_int64(value)
			term = .number(Float(intValue))
		case SQLITE_FLOAT:
			let floatValue = sqlite3_value_double(value)
			term = .number(Float(floatValue))
		case SQLITE_NULL:
			sqlite3_result_error(
				context,
				"predicate_formula() does not support NULL arguments",
				-1
			)
			return
		case SQLITE_BLOB:
			sqlite3_result_error(
				context,
				"predicate_formula() does not support BLOB arguments",
				-1
			)
			return
		default:
			sqlite3_result_error(
				context,
				"predicate_formula() unsupported argument type",
				-1
			)
			return
		}

		terms.append(term)
	}

	// Create the Formula
	let formula = Formula.predicate(Predicate(name: predicateName, arguments: terms))

	// Convert to JSON using the utility function
	do {
		let jsonStr = try formulaToJSON(formula)

		// Return the JSON string
		jsonStr.withCString { cString in
			sqlite3_result_text(
				context,
				cString,
				-1,
				unsafeBitCast(-1, to: sqlite3_destructor_type.self)
			)
		}
	} catch {
		sqlite3_result_error(context, "Failed to encode formula: \(error)", -1)
	}
}
