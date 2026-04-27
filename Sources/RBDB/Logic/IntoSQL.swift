fileprivate typealias SQLExpression = String

fileprivate struct SQLTable {
	var name: String
	var alias: String?
	var conditions: [SQLExpression] = []

	var effectiveName: String {
		alias ?? name
	}
}

fileprivate struct SQLSelect {
	var select: [SQLExpression]
	var fromTables: [SQLTable]
	static var empty: SQLSelect { SQLSelect(select: [], fromTables: []) }

	var sql: String {
		var result = "SELECT \(select.joined(separator: ", "))"

		if let t1 = fromTables.first {
			if let alias = t1.alias {
				result += " FROM [\(t1.name)] AS [\(alias)]"
			} else {
				result += " FROM [\(t1.name)]"
			}

			for t2 in fromTables.dropFirst() {
				if let alias = t2.alias {
					result +=
						" JOIN [\(t2.name)] AS [\(alias)] ON \(t2.conditions.joined(separator: " AND "))"
				} else {
					result += " JOIN [\(t2.name)] ON \(t2.conditions.joined(separator: " AND "))"
				}
			}

			if !t1.conditions.isEmpty {
				result += " WHERE \(t1.conditions.joined(separator: " AND "))"
			}
		}

		return result
	}
}

fileprivate struct RuleIntoSQLReducer: SymbolReducer {
	let getColumnNames: (_ predicateName: String) throws -> [String]

	struct SQLVarRef {
		var srcTableName: String
		var srcColumnName: String
	}

	func termToSQL(_ term: Term, _ cols: [Var: SQLVarRef]) -> SQLExpression {
		switch term {
		case .boolean(let b): return b ? "true" : "false"
		case .number(let n): return String(n)
		case .string(let s): return "'\(s)'"
		case .variable(let v):
			guard let col = cols[v] else {
				preconditionFailure("Variable \(v) not found in column mapping")
			}
			return "[\(col.srcTableName)].\(col.srcColumnName)"
		case .expression(let expr):
			switch expr {
			case .add(let lhs, let rhs):
				return "(\(termToSQL(lhs, cols)) + \(termToSQL(rhs, cols)))"
			case .subtract(let lhs, let rhs):
				return "(\(termToSQL(lhs, cols)) - \(termToSQL(rhs, cols)))"
			case .multiply(let lhs, let rhs):
				return "(\(termToSQL(lhs, cols)) * \(termToSQL(rhs, cols)))"
			case .divide(let lhs, let rhs):
				return "(\(termToSQL(lhs, cols)) / \(termToSQL(rhs, cols)))"
			}
		}
	}

	// Must be a valid, canonical formula (e.g. passes `validate` and has had `canonicalize` called)
	func reduce(_ prev: SQLSelect, _ formula: Formula) throws -> SQLSelect {
		var sql = prev
		switch formula {
		case .hornClause(positive: let positive, negative: let negatives):
			var cols: [Var: SQLVarRef] = [:]
			var tableNameCounts: [String: Int] = [:]

			// Process each predicate in the body
			for (index, predicate) in negatives.enumerated() {
				// Create unique table alias for duplicate table names
				let count = tableNameCounts[predicate.name, default: 0]
				tableNameCounts[predicate.name] = count + 1

				let alias = count > 0 ? "\(predicate.name)\(count)" : nil
				let table = SQLTable(name: predicate.name, alias: alias)
				sql.fromTables.append(table)

				let columnNames = try getColumnNames(predicate.name)

				for (i, term) in predicate.arguments.enumerated() {
					switch term {
					case .variable(let v):
						// If this variable was seen before, create a join condition
						if let existingRef = cols[v] {
							// This variable appears in multiple tables - create join condition
							let condition =
								"[\(existingRef.srcTableName)].\(existingRef.srcColumnName) = [\(table.effectiveName)].\(columnNames[i])"
							sql.fromTables[index].conditions.append(condition)
						} else {
							// First occurrence of this variable
							cols[v] = SQLVarRef(
								srcTableName: table.effectiveName,
								srcColumnName: columnNames[i]
							)
						}
					case .boolean, .number, .string, .expression:
						// Add to the WHERE clause
						let sqlExpr = termToSQL(term, cols)
						let condition = "[\(table.effectiveName)].\(columnNames[i]) = \(sqlExpr)"
						sql.fromTables[index].conditions.append(condition)
					}
				}
			}

			// Generate SELECT clause based on the head predicate
			let columnNames = try getColumnNames(positive.name)
			for (i, term) in positive.arguments.enumerated() {
				let value = termToSQL(term, cols)
				sql.select.append("\(value) AS \(columnNames[i])")
			}
		}
		return sql
	}
}

fileprivate struct QueryIntoSQLReducer: SymbolReducer {
	let getColumnNames: (_ predicateName: String) throws -> [String]

	func termToSQL(_ term: Term, _ table: SQLTable, _ columnName: String)
		-> SQLExpression
	{
		switch term {
		case .boolean(let b): return b ? "true" : "false"
		case .number(let n): return String(n)
		case .string(let s): return "'\(s)'"
		case .variable:
			return "[\(table.effectiveName)].\(columnName)"
		case .expression(let expr):
			switch expr {
			case .add(let lhs, let rhs):
				return
					"(\(termToSQL(lhs, table, columnName)) + \(termToSQL(rhs, table, columnName)))"
			case .subtract(let lhs, let rhs):
				return
					"(\(termToSQL(lhs, table, columnName)) - \(termToSQL(rhs, table, columnName)))"
			case .multiply(let lhs, let rhs):
				return
					"(\(termToSQL(lhs, table, columnName)) * \(termToSQL(rhs, table, columnName)))"
			case .divide(let lhs, let rhs):
				return
					"(\(termToSQL(lhs, table, columnName)) / \(termToSQL(rhs, table, columnName)))"
			}
		}
	}

	func reduce(_ prev: SQLSelect, _ formula: Formula) throws -> SQLSelect {
		var sql = prev
		switch formula {
		case .hornClause(positive: let predicate, negative: let negatives):
			// For queries, we don't allow negative literals for now
			guard negatives.isEmpty else {
				throw SQLiteError.queryError("Queries with negative literals are not supported")
			}

			var table = SQLTable(name: predicate.name, alias: nil)
			let columnNames = try getColumnNames(predicate.name)

			// Process arguments to build variable mappings and WHERE conditions for constants
			for (i, term) in predicate.arguments.enumerated() {
				let columnName = columnNames[i]
				switch term {
				case .variable(let v):
					// Variables become part of the result set
					// FIXME: Prevent SQL injection via variable name
					sql.select.append("[\(table.effectiveName)].\(columnName) AS [\(v)]")
				case .boolean, .number, .string, .expression:
					// Expressions in queries go into WHERE clause
					let sqlExpr = termToSQL(term, table, columnName)
					table.conditions.append(
						"[\(table.effectiveName)].\(columnName) = \(sqlExpr)")
				}
			}

			if sql.select.isEmpty {
				sql.select.append("true as sat")
			}
			sql.fromTables.append(table)
		}
		return sql
	}
}

extension Symbol {
	func ruleIntoSQL(_ getColumnNames: @escaping (_ predicateName: String) throws -> [String])
		throws
		-> String
	{
		try reduce(.empty, RuleIntoSQLReducer(getColumnNames: getColumnNames)).sql
	}

	func queryIntoSQL(_ getColumnNames: @escaping (_ predicateName: String) throws -> [String])
		throws
		-> String
	{
		try reduce(.empty, QueryIntoSQLReducer(getColumnNames: getColumnNames)).sql
	}
}
