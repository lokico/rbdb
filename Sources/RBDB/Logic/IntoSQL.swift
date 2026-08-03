fileprivate typealias SQLExpression = String

fileprivate struct SQLTable {
	var name: String
	var alias: String?
	var conditions: [SQLExpression] = []
	/// A raw FROM-clause source (e.g. a `(SELECT …)` subquery) to use instead of the bare table
	/// `[name]`. When set, the table is always given an alias (its `effectiveName`).
	var source: SQLExpression?

	var effectiveName: String {
		alias ?? name
	}

	/// How this table appears in a FROM/JOIN clause.
	var fromClause: SQLExpression {
		if let source {
			return "\(source) AS [\(effectiveName)]"
		} else if let alias {
			return "[\(name)] AS [\(alias)]"
		} else {
			return "[\(name)]"
		}
	}
}

fileprivate struct SQLSelect {
	var select: [SQLExpression]
	var fromTables: [SQLTable]
	/// Conditions that hold of the row as a whole rather than of a particular table — they land in
	/// the `WHERE` clause even when there is no `FROM` at all (SQLite allows a `FROM`-less `SELECT`
	/// to carry a `WHERE`), which is what makes a body of nothing but guards filter correctly.
	var conditions: [SQLExpression] = []
	static var empty: SQLSelect { SQLSelect(select: [], fromTables: []) }

	var sql: String {
		var result = "SELECT DISTINCT \(select.joined(separator: ", "))"
		var whereConditions = conditions

		if let t1 = fromTables.first {
			result += " FROM \(t1.fromClause)"

			for t2 in fromTables.dropFirst() {
				result += " JOIN \(t2.fromClause)"
				// A body literal that shares no variable with any literal before it constrains
				//  nothing, so there is no `ON` to write — the literals are joined on nothing, which
				//  is exactly a cross product. (`JOIN … ON` with an empty condition isn't valid SQL,
				//  and a bare `JOIN` is what SQLite spells that as, still free to be reordered.)
				if !t2.conditions.isEmpty {
					result += " ON \(t2.conditions.joined(separator: " AND "))"
				}
			}

			whereConditions = t1.conditions + whereConditions
		}

		if !whereConditions.isEmpty {
			result += " WHERE \(whereConditions.joined(separator: " AND "))"
		}

		return result
	}
}

/// Lowers a term to a SQL expression. Constants render directly; each `variable` is resolved by the
/// caller's `leaf` closure (to a bound column, an inverted expression, etc.).
private func lower(term: Term, leaf: (Var) throws -> SQLExpression) rethrows -> SQLExpression {
	switch term {
	case .boolean(let b): return b ? "true" : "false"
	case .number(let n): return String(n)
	case .string(let s): return "'\(s)'"
	case .variable(let v): return try leaf(v)
	case .arithmetic(let expr):
		switch expr.raw {
		case .add(let lhs, let rhs):
			return "(\(try lower(term: lhs, leaf: leaf)) + \(try lower(term: rhs, leaf: leaf)))"
		case .multiply(let lhs, let rhs):
			return "(\(try lower(term: lhs, leaf: leaf)) * \(try lower(term: rhs, leaf: leaf)))"
		case .exponent(let lhs, let rhs):
			return "pow(\(try lower(term: lhs, leaf: leaf)), \(try lower(term: rhs, leaf: leaf)))"
		}
	}
}

fileprivate struct RuleIntoSQLReducer: SymbolReducer {
	let getColumnNames: (_ predicateName: String) throws -> [String]
	/// Optionally supplies a FROM-clause source for a body predicate (e.g. an inlined `(SELECT …)`
	/// subquery). Returning `nil` references the predicate by name.
	let tableSource: (_ predicateName: String) throws -> SQLExpression?

	/// Renders `term` to SQL, resolving each variable through `cols` (variable → SQL expression).
	/// Throws if a variable has no binding — that means the rule couldn't be lowered.
	func termToSQL(_ term: Term, _ cols: [Var: SQLExpression]) throws -> SQLExpression {
		try lower(term: term) { v in
			guard let sql = cols[v] else {
				throw SQLiteError.queryError("variable \(v) is not bound by any body literal")
			}
			return sql
		}
	}

	/// Solves `expr = target` for the single unbound variable `v`, returning the SQL that computes
	/// `v` from the (already-bound) column. Only linear, single-occurrence uses of `v` are
	/// invertible — e.g. `v - 1` → `target + 1`, `v * 2` → `target / 2.0`. Any other shape (or `v`
	/// appearing on both sides) returns `nil`, leaving the caller to report the rule as unbindable.
	func invert(
		_ expr: Term, equalTo target: SQLExpression, for v: Var, _ cols: [Var: SQLExpression]
	)
		throws -> SQLExpression?
	{
		switch expr {
		case .variable(let u):
			return u == v ? target : nil
		case .arithmetic(let e):
			// Exactly one operand may contain `v`; the other is treated as a known and moved across.
			func across(
				_ withV: Term, _ known: Term, _ solved: (SQLExpression) -> SQLExpression
			)
				throws -> SQLExpression?
			{
				guard withV.freeVariables.contains(v), !known.freeVariables.contains(v) else {
					return nil
				}
				return try invert(withV, equalTo: try solved(termToSQL(known, cols)), for: v, cols)
			}
			var solved: (SQLExpression) -> SQLExpression
			switch e.raw {
			case .add(let l, let r):
				solved = { "(\(target) - \($0))" }
				return try across(l, r, solved) ?? across(r, l, solved)
			case .multiply(let l, let r):
				solved = { "(\(target) / \($0))" }
				return try across(l, r, solved) ?? across(r, l, solved)
			case .exponent:
				// Roots are lossy/multivalued over the reals, so we don't invert exponentiation.
				return nil
			}
		default:
			return nil
		}
	}

	// Must be a valid, canonical formula (e.g. passes `validate` and has had `canonicalize` called)
	func reduce(_ prev: SQLSelect, _ formula: Formula) throws -> SQLSelect {
		var sql = prev
		switch formula {
		case .hornClause(positive: let positive, negative: let negatives, guards: let guards):
			var cols: [Var: SQLExpression] = [:]
			var tableNameCounts: [String: Int] = [:]

			// Process each predicate in the body
			for (index, predicate) in negatives.enumerated() {
				// Create unique table alias for duplicate table names
				let count = tableNameCounts[predicate.name, default: 0]
				tableNameCounts[predicate.name] = count + 1

				let alias = count > 0 ? "\(predicate.name)\(count)" : nil
				var table = SQLTable(name: predicate.name, alias: alias)
				table.source = try tableSource(predicate.name)
				sql.fromTables.append(table)

				let columnNames = try getColumnNames(predicate.name)

				for (i, term) in predicate.arguments.enumerated() {
					let column = "[\(table.effectiveName)].\(columnNames[i])"
					switch term {
					case .variable(let v):
						// If this variable was seen before, create a join condition; otherwise bind it.
						if let existing = cols[v] {
							sql.fromTables[index].conditions.append("\(existing) = \(column)")
						} else {
							cols[v] = column
						}
					case .boolean, .number, .string:
						sql.fromTables[index].conditions.append(
							"\(column) = \(try termToSQL(term, cols))")
					case .arithmetic:
						// If every variable is already bound, this is a constraint. If exactly one is
						//  unbound, try to bind it by inverting the expression against the column (this
						//  is what lets the recursion variable live in the body, e.g. `nat(N) :- nat(N - 1)`).
						let unbound = term.freeVariables.filter { cols[$0] == nil }
						if unbound.isEmpty {
							sql.fromTables[index].conditions.append(
								"\(column) = \(try termToSQL(term, cols))")
						} else if unbound.count == 1, let v = unbound.first,
							let bound = try invert(term, equalTo: column, for: v, cols)
						{
							cols[v] = bound
						} else {
							throw SQLiteError.queryError(
								"cannot solve body expression \(term) for its unbound variable(s)")
						}
					}
				}
			}

			// Guards are pure filters: both operands are already bound by a positive literal (enforced by
			//  range-restriction validation), so each lowers to a boolean condition in the WHERE clause —
			//  sound because every join here is an inner join, so a WHERE predicate and a JOIN…ON
			//  predicate are interchangeable. A guard-only body has no table at all, and still filters.
			for g in guards {
				let lhs = try termToSQL(g.lhs, cols)
				let rhs = try termToSQL(g.rhs, cols)
				sql.conditions.append("\(lhs) \(g.operation.rawValue) \(rhs)")
			}

			// Generate SELECT clause based on the head predicate
			let columnNames = try getColumnNames(positive.name)
			for (i, term) in positive.arguments.enumerated() {
				let value = try termToSQL(term, cols)
				sql.select.append("\(value) AS \(columnNames[i])")
			}
		}
		return sql
	}
}

fileprivate struct QueryIntoSQLReducer: SymbolReducer {
	let getColumnNames: (_ predicateName: String) throws -> [String]

	func termToSQL(_ term: Term, _ table: SQLTable, _ columnName: String) -> SQLExpression {
		// In a query, every variable in an argument maps to that argument's single column.
		lower(term: term) { _ in "[\(table.effectiveName)].\(columnName)" }
	}

	func reduce(_ prev: SQLSelect, _ formula: Formula) throws -> SQLSelect {
		var sql = prev
		switch formula {
		case .hornClause(positive: let predicate, negative: let negatives, guards: let guards):
			// For queries, we don't allow negative literals for now
			guard negatives.isEmpty else {
				throw SQLiteError.queryError("Queries with negative literals are not supported")
			}
			// Guards need multi-literal (conjunctive) query support, which the single-literal query
			//  reducer doesn't yet have; they are only supported in rule bodies for now.
			guard guards.isEmpty else {
				throw SQLiteError.queryError("Queries with comparison guards are not supported")
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
				case .boolean, .number, .string, .arithmetic:
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
	func ruleIntoSQL(
		_ getColumnNames: @escaping (_ predicateName: String) throws -> [String],
		tableSource: @escaping (_ predicateName: String) throws -> String? = { _ in nil }
	)
		throws
		-> String
	{
		try reduce(
			.empty,
			RuleIntoSQLReducer(getColumnNames: getColumnNames, tableSource: tableSource)
		).sql
	}

	func queryIntoSQL(_ getColumnNames: @escaping (_ predicateName: String) throws -> [String])
		throws
		-> String
	{
		try reduce(.empty, QueryIntoSQLReducer(getColumnNames: getColumnNames)).sql
	}
}
