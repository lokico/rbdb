import Foundation

/// What a query pins about the predicate it asks: column name → the ground term that column must
/// equal. This is the *demand*, and it is what makes an otherwise-infinite recursion terminate —
/// `nat(99)` is answerable where `SELECT … FROM [nat]` is not, because 99 bounds the recursive step.
///
/// Carried as structure from the `Formula` the query was written as, rather than recovered from the
/// SQL that formula generated. `extractEqualityConstraints` is the recovery path, and exists only for
/// SQL that never was a formula — raw statements typed at the CLI.
struct Demand {
	/// The predicate the constraints are columns of.
	let predicate: String
	let constraints: [String: Term]

	/// The demand a single-literal query places on its predicate: every ground argument pins its
	/// column. An argument holding a variable pins nothing — it is what the query asks *for*.
	init?(of formula: Formula, columns: [String]) {
		guard case .hornClause(let head, let negatives, _) = formula, negatives.isEmpty else {
			return nil
		}
		var constraints: [String: Term] = [:]
		for (i, argument) in head.arguments.enumerated()
		where i < columns.count && argument.freeVariables.isEmpty {
			constraints[columns[i]] = argument
		}
		guard !constraints.isEmpty else { return nil }
		self.predicate = head.name
		self.constraints = constraints
	}
}

// Recursive-predicate detection and the query-time `WITH RECURSIVE` closure builder that lets
// recursive and recursion-dependent predicates be queried. See `buildRecursiveClosureCTE`.
extension RBDB {
	/// Whether `name` is recursive, or depends (transitively, through its rule bodies) on a recursive
	/// predicate. Such predicates must be materialized as CTEs rather than plain views.
	func involvesRecursion(_ name: String) throws -> Bool {
		var visiting: Set<String> = []
		return try involvesRecursion(name, &visiting)
	}

	private func involvesRecursion(_ name: String, _ visiting: inout Set<String>) throws -> Bool {
		// A cycle back to a predicate we're already examining is itself (mutual) recursion.
		guard visiting.insert(name).inserted else { return true }
		defer { visiting.remove(name) }
		for rule in try fetchRules(for: name) {
			if rule.isRecursive(for: name) { return true }
			if case .hornClause(_, let negatives, _) = rule {
				for body in negatives where try involvesRecursion(body.name, &visiting) {
					return true
				}
			}
		}
		return false
	}

	/// Rewrites the failing statement so every recursive predicate it needs is available.
	///
	/// Only *self-recursive* predicates become CTEs, emitted in a `WITH RECURSIVE …` prefix. Every
	/// other predicate that (transitively) depends on recursion is inlined as a `FROM`-subquery —
	/// at any depth. This is about *streaming*: a recursive CTE streams its (possibly infinite) rows
	/// only to the outermost query. Giving an intermediate predicate its own CTE would make another
	/// query read from it, forcing SQLite to fully materialize the recursive dependency (a hang on an
	/// infinite relation). Inlining lets SQLite flatten everything into the outer query so the
	/// recursion streams. Inlined bodies use `UNION ALL` (not `UNION`): de-duplicating an infinite
	/// relation would itself require materializing it.
	///
	/// The query's equality constraints are propagated down the dependency chain (`square.n = 5` ⟹
	/// `nat.n = 5`) so each recursive step can be bounded and terminate for value-specific queries.
	func buildRecursiveClosureCTE(
		topPredicate: String,
		topColumns: [String],
		sql: SQL,
		demand: Demand? = nil
	) throws -> SQL {
		var stmt = String(sql.queryText.utf8.dropFirst(sql.startIndex.queryOffset))!

		// Thread the query's constraints down the dependency graph so each self-recursive predicate
		//  learns the bounds that make its recursion terminate (first path to reach it wins).
		var recursiveConstraints: [String: [String: Term]] = [:]
		var threaded: Set<String> = []
		func thread(_ name: String, _ columns: [String], _ constraints: [String: Term]) throws {
			let rules = try fetchRules(for: name)
			if rules.contains(where: { $0.isRecursive(for: name) }),
				recursiveConstraints[name] == nil
			{
				recursiveConstraints[name] = constraints
			}
			guard threaded.insert(name).inserted else { return }
			for rule in rules {
				guard case .hornClause(let head, let negatives, _) = rule else { continue }
				for body in negatives where try involvesRecursion(body.name) {
					guard let bodyColumns = try getColumns(for: body.name) else { continue }
					let propagated = propagateConstraints(
						constraints, headColumns: columns, head: head,
						toBody: body, bodyColumns: bodyColumns)
					try thread(body.name, bodyColumns, propagated)
				}
			}
		}
		// The demand as the query stated it, where we have it; otherwise recovered from the statement
		//  text, which is all a raw SQL statement leaves to go on.
		let topConstraints =
			demand?.predicate == topPredicate
			? demand!.constraints
			: extractEqualityConstraints(
				from: stmt, predicateName: topPredicate, columnNames: topColumns)
		try thread(topPredicate, topColumns, topConstraints)

		// Build the SQL. `cteDefinitions`/`cteOrder` collect the self-recursive CTEs (emitted
		//  dependency-first); `inlining` guards against unbounded expansion of unsupported recursion.
		var cteDefinitions: [String: String] = [:]
		var cteOrder: [String] = []
		var inlining: Set<String> = []

		// How predicate `name` should appear in a FROM clause: `nil` (reference by name) for
		//  self-recursive predicates (a CTE) and plain views, or an inlined `(subquery)` for a
		//  predicate that merely depends on recursion. (Nested funcs may call each other freely.)
		func reference(_ name: String) throws -> String? {
			let rules = try fetchRules(for: name)
			if rules.contains(where: { $0.isRecursive(for: name) }) {
				try ensureCTE(name, rules)
				return nil
			}
			guard try involvesRecursion(name), let columns = try getColumns(for: name) else {
				return nil  // plain view or base table
			}
			// A non-self-recursive predicate that (transitively) depends on itself is mutual recursion.
			//  The finite case is handled by the iterative evaluator; reaching here means a
			//  value-generating predicate feeds a cycle, which neither engine can express. Reject it
			//  with a clear error rather than looping forever inlining the cycle.
			guard inlining.insert(name).inserted else {
				throw SQLiteError.queryError(
					"value-generating mutual recursion involving '\(name)' is not supported")
			}
			defer { inlining.remove(name) }
			return "(\n\(try predicateBody(name, columns, rules, separator: "\nUNION ALL\n"))\n)"
		}

		// Emit `name`'s recursive CTE once, dependency-first (its body is built before it's appended).
		func ensureCTE(_ name: String, _ rules: [Formula]) throws {
			guard cteDefinitions[name] == nil, let columns = try getColumns(for: name) else {
				return
			}
			cteDefinitions[name] = ""  // reserve so the recursive self-reference resolves to a name
			let body = try predicateBody(name, columns, rules, separator: "\nUNION\n")
			let columnList = columns.map { "[\($0)]" }.joined(separator: ", ")
			cteDefinitions[name] = "[\(name)] (\(columnList)) AS (\n\(body)\n)"
			cteOrder.append(name)
		}

		// One predicate's body: base facts UNION[ ALL] each rule, referencing dependencies via
		//  `reference` (CTE name or inlined subquery). Self-recursive rules get their bounds injected.
		func predicateBody(
			_ name: String, _ columns: [String], _ rules: [Formula], separator: String
		) throws -> String {
			var selects = [baseFactsSelect(for: name, columns: columns)]

			var columnsQuery: SQLiteCursor? = nil
			let getCols: (String) throws -> [String] = { referenced in
				guard let cols = try self.getColumns(for: referenced, query: &columnsQuery) else {
					throw RBDBError.corruptData(
						message: "rule for '\(name)' references unknown predicate '\(referenced)'")
				}
				return cols
			}

			for rule in rules {
				// SQLite forbids referencing a recursive CTE from inside a subquery, so a bounded step
				//  can't be expressed by wrapping the rule's SQL. Bound the *rule* instead: a derived
				//  bound is a guard like any the caller wrote, so it conjoins onto the body and the
				//  existing lowering puts it in the same WHERE clause as the rule's own constraints.
				let bounded =
					rule.isRecursive(for: name)
					? rule.adding(
						guards: boundsForRecursiveStep(
							rule: rule, predicateName: name, columnNames: columns,
							constraints: recursiveConstraints[name] ?? [:]))
					: rule
				selects.append(try bounded.ruleIntoSQL(getCols, tableSource: reference))
			}
			return selects.joined(separator: separator)
		}

		let topRules = try fetchRules(for: topPredicate)
		if topRules.contains(where: { $0.isRecursive(for: topPredicate) }) {
			// Recursive top: the user's statement reads its CTE directly and streams.
			try ensureCTE(topPredicate, topRules)
		} else {
			// Dependent top: inline it as a streaming `FROM`-subquery over its recursive closure.
			let body = try predicateBody(
				topPredicate, topColumns, topRules, separator: "\nUNION ALL\n")
			stmt = replaceTableReference(
				in: stmt, table: topPredicate, with: "(\n\(body)\n) AS [\(topPredicate)]")
		}

		let cte = "WITH RECURSIVE " + cteOrder.map { cteDefinitions[$0]! }.joined(separator: ",\n")
		return SQL(
			"\(cte)\n\(stmt)",
			arguments: Array(sql.arguments.dropFirst(sql.startIndex.argumentIndex)))
	}

	/// Replaces every `FROM`/`JOIN` reference to `table` with `replacement`, matching the table name
	/// whether or not it is `[bracket]`-quoted (queries built by `queryIntoSQL` bracket it; raw SQL
	/// typed at the CLI does not). A trailing boundary keeps `daughter` from matching `daughters`.
	private func replaceTableReference(in stmt: String, table: String, with replacement: String)
		-> String
	{
		let escaped = NSRegularExpression.escapedPattern(for: table)
		let pattern = "\\b(FROM|JOIN)\\s+\\[?\(escaped)\\]?(?![\\w\\[])"
		guard
			let regex = try? NSRegularExpression(pattern: pattern, options: .caseInsensitive)
		else { return stmt }

		var result = stmt
		let matches = regex.matches(in: result, range: NSRange(result.startIndex..., in: result))
		for match in matches.reversed() {
			guard let range = Range(match.range, in: result),
				let keywordRange = Range(match.range(at: 1), in: result)
			else { continue }
			result.replaceSubrange(range, with: "\(result[keywordRange]) \(replacement)")
		}
		return result
	}

	/// Maps equality constraints on a dependent predicate onto the columns of a recursive body
	/// predicate, following the rule's shared variables. A constrained head column that is a single
	/// variable `v` (bare, or wrapped in an invertible expression like `v * v`) pins that variable;
	/// if `v` also appears bare in the body literal, that body column inherits the pinned value. The
	/// expression case is what lets a query constrain a *computed* column (e.g. `square(X, 4)`, whose
	/// `4 = X²` bounds `X ≤ 2` and hence `nat`'s recursion).
	private func propagateConstraints(
		_ constraints: [String: Term],
		headColumns: [String],
		head: Predicate,
		toBody body: Predicate,
		bodyColumns: [String]
	) -> [String: Term] {
		var result: [String: Term] = [:]
		for (headIdx, headArg) in head.arguments.enumerated() where headIdx < headColumns.count {
			guard let literal = constraints[headColumns[headIdx]],
				headArg.freeVariables.count == 1, let v = headArg.freeVariables.first
			else { continue }

			// The value `v` must take for this column to equal `literal`.
			let value: Term
			if case .variable = headArg {
				value = literal  // bare variable: pass the literal through unchanged
			} else if case .number(let target) = literal,
				let solved = solve(headArg, for: v, equals: target),
				solved.isFinite
			{
				value = .number(solved)
			} else {
				continue  // non-numeric literal or non-invertible expression
			}

			for (bodyIdx, bodyArg) in body.arguments.enumerated() where bodyIdx < bodyColumns.count
			{
				if case .variable(let u) = bodyArg, u == v {
					result[bodyColumns[bodyIdx]] = value
				}
			}
		}
		return result
	}

	/// Solves `expr = target` for its single variable `v`, numerically. Mirrors the symbolic
	/// inversion in `RuleIntoSQLReducer`: `v` must occur once, on one side of each operator, with the
	/// other side constant. Returns `nil` if `expr` isn't invertible this way. Roots (`vᶜ = t →
	/// v = t^(1/c)`) take the principal value, which is exact for the monotonic-over-ℕ uses we bound.
	private func solve(_ expr: Term, for v: Var, equals target: Double) -> Double? {
		switch expr {
		case .variable(let u): return u == v ? target : nil
		case .number(let n): return n == target ? target : nil
		case .boolean, .string: return nil
		case .arithmetic(let e):
			// Move the `v`-free operand to the other side, then recurse into the one holding `v`.
			func across(_ withV: Term, _ known: Term, _ undo: (Double, Double) -> Double?)
				-> Double?
			{
				guard withV.freeVariables.contains(v), !known.freeVariables.contains(v),
					case .number(let c) = known, let inner = undo(target, c)
				else { return nil }
				return solve(withV, for: v, equals: inner)
			}
			var undo: (Double, Double) -> Double?
			switch e.raw {
			case .add(let l, let r):
				undo = { $0 - $1 }
				return across(l, r, undo) ?? across(r, l, undo)
			case .multiply(let l, let r):
				undo = { $1 == 0 ? nil : $0 / $1 }
				return across(l, r, undo) ?? across(r, l, undo)
			case .exponent(let base, let exp):
				return across(base, exp) { $1 == 0 ? nil : Foundation.pow($0, 1 / $1) }
			}
		}
	}

	/// Returns guards (e.g. `N + 1 <= 100`) to conjoin onto a recursive rule's step, derived from the
	/// query's equality constraints and the head expression's monotonicity in the body variables.
	/// Returns an empty array if no usable bound was found — the rule is then left unconstrained.
	private func boundsForRecursiveStep(
		rule: Formula,
		predicateName: String,
		columnNames: [String],
		constraints: [String: Term]
	) -> [BooleanExpression] {
		guard !constraints.isEmpty,
			case .hornClause(let head, let bodies, _) = rule,
			let recursiveBody = bodies.first(where: { $0.name == predicateName })
		else { return [] }

		var bounds: [BooleanExpression] = []
		for (colIdx, headTerm) in head.arguments.enumerated()
		where colIdx < columnNames.count && colIdx < recursiveBody.arguments.count {
			guard let bound = constraints[columnNames[colIdx]] else { continue }

			// Each recursion step maps this column from `bodyTerm` (the existing tuple) to `headTerm`
			//  (the tuple being derived). For a one-sided bound to be sound both must be a monotonic
			//  function of a single shared variable moving the *same* direction, which makes the step
			//  map monotone. Whether it drives the column up or down then decides `<=` vs `>=`.
			let bodyTerm = recursiveBody.arguments[colIdx]
			let vars = headTerm.freeVariables.union(bodyTerm.freeVariables)
			guard vars.count == 1, let v = vars.first,
				case let mHead = headTerm.monotonicity(in: v), mHead != 0,
				mHead == bodyTerm.monotonicity(in: v),
				let direction = stepDirection(head: headTerm, body: bodyTerm, in: v)
			else { continue }

			guard case .number = bound else {
				// `stepDirection` succeeds only for numeric steps, so this recursion produces numbers
				//  and can never equal a non-numeric target — prune the step. (A naive `N + 1 <= 'hi
				//  mom'` never terminates, because SQLite orders every number before every string.)
				bounds.append(.notEqual(.number(0), .number(0)))
				continue
			}
			// The bound is on the value this step *derives*, which is the head term — the same thing
			//  the column holds, said without depending on the column alias being in scope.
			bounds.append(
				direction > 0
					? .lessThanOrEqual(headTerm, bound)
					: .greaterThanOrEqual(headTerm, bound))
		}
		return bounds
	}

	/// Direction the column drifts each recursion step: `+1` if the derived value (`head`) exceeds
	/// the value it was derived from (`body`), `-1` if below, `nil` if that sign isn't constant.
	/// Determined by plugging sample values through `Term.substituting`, which re-canonicalizes to a
	/// number — the canonical form deliberately doesn't distribute, so `head - body` won't fold.
	private func stepDirection(head: Term, body: Term, in v: Var) -> Int? {
		var sign = 0
		for x: Double in [1, 2] {
			guard case .number(let h) = head.substituting(v, with: .number(x)),
				case .number(let b) = body.substituting(v, with: .number(x))
			else { return nil }
			let delta = h - b
			if delta == 0 { return nil }
			let s = delta > 0 ? 1 : -1
			if sign == 0 { sign = s } else if sign != s { return nil }
		}
		return sign == 0 ? nil : sign
	}

	/// Best-effort extraction of `[predicate].column = literal` style equality constraints from
	/// the failing statement text. Handles the form generated by `queryIntoSQL` and common
	/// hand-written variants. Returns map of column name → the ground term the column is pinned to.
	///
	/// The fallback, not the main path: a query written as a `Formula` states its demand outright (see
	/// `Demand`), and only a statement that never was a formula — raw SQL typed at the CLI — has to
	/// have it read back out of the text. Note that this can only see *literals*: a bound parameter or
	/// a correlated column reference carries no demand here, so such a query stays unbounded.
	private func extractEqualityConstraints(
		from stmt: String, predicateName: String, columnNames: [String]
	) -> [String: Term] {
		var result: [String: Term] = [:]
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
			result[col] = term(fromSQLLiteral: String(stmt[valueRange]))
		}
		return result
	}

	/// The term a SQL literal denotes, for the forms the pattern above admits. Nil for anything else,
	/// which pins nothing rather than pinning a value we couldn't read.
	private func term(fromSQLLiteral literal: String) -> Term? {
		if literal.hasPrefix("'") {
			return .string(String(literal.dropFirst().dropLast()))
		}
		switch literal.lowercased() {
		case "true": return .boolean(true)
		case "false": return .boolean(false)
		default: return Double(literal).map(Term.number)
		}
	}
}
