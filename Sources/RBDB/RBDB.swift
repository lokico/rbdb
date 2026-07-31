import Foundation
import SQLite3

public enum RBDBError: Error {
	case corruptData(message: String)
}

public enum RetractionError: Error {
	/// No *live* `_rule` row has this canonical form. Note that a formula which is derivable but not
	/// stored is `.notFound` too: retraction operates on the base, not the closure.
	case notFound(Formula)
}

public class RBDB: SQLiteDatabase {
	var isInitializing = false

	// Guards `refreshDirtyMaterializations` against re-entering itself: the refresh runs queries of its
	//  own (re-iterating closures), and those must not kick off another refresh pass.
	var isRefreshing = false

	// The same, for `checkCoherence`: its `INTERSECT`s go through `query(sql:)` and so can rescue views
	//  and materialize closures, none of which is a write to be checked in its own right.
	var isCheckingCoherence = false

	/// How many fixpoint builds are in flight on this connection, exposed to SQL as `is_materializing()`
	/// and read by the materialized predicates' `BEFORE INSERT` triggers (see `createWriteTriggers`).
	/// Per-connection, which is exactly the scope a temp table would have given.
	///
	/// A *counter*, not a boolean: `materialize` calls `query(sql:)` internally, which reaches
	/// `refreshDirtyMaterializations` and so can start a nested build — a boolean would be cleared by
	/// the inner one while the outer is still running.
	var materializingDepth = 0

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

		// Register is_materializing(). Deliberately *not* SQLITE_DETERMINISTIC — see the function's docs.
		let materializingResult = sqlite3_create_function(
			db,
			"is_materializing",
			0,
			SQLITE_UTF8,
			Unmanaged.passUnretained(self).toOpaque(),
			isMaterializingSQLiteFunction,
			nil,
			nil
		)
		if materializingResult != SQLITE_OK {
			sqlite3_close(db)
			throw SQLiteError.couldNotRegisterFunction(name: "is_materializing")
		}

		// Migrate the schema
		try super.query(
			sql: SQL(String(decoding: PackageResources.schema_sql, as: UTF8.self))
		)
	}

	@discardableResult
	public override func query(sql: SQL) throws -> SQLiteCursor {
		// Bring any materialized closures the `_rule` trigger flagged stale up to date before reading.
		//  Done here rather than in the trigger so it runs at a safe point — re-iterating, never
		//  dropping — which sidesteps the on-disk table lock. See `refreshDirtyMaterializations`.
		try refreshDirtyMaterializations()
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
		// This should also be moved into the below transaction if we ever support dropping predicates.
		try validate(formula: formula)

		// `IMMEDIATE`, not the default `DEFERRED`, because this transaction decides what to *keep* by
		//  reading (the coherence check below, and the subsumption scan). A deferred transaction is still
		//  correct here — its read lock is held for the rest of the transaction, so a competing writer
		//  can't commit underneath it — but it reaches that outcome by *upgrading* a read lock to a write
		//  lock, which SQLite reports as `SQLITE_BUSY` at the first write or at `COMMIT`, with no safe
		//  point to retry from. Taking the write lock at `BEGIN` moves that refusal to a better spot.
		try super.query(sql: "BEGIN IMMEDIATE TRANSACTION")
		do {
			// Canonicalize the rule set (tautology drop, subsumption, literal dedup) in the same
			//  transaction. Returns nil to store nothing, otherwise the formula to store together with
			//  any already-stored rules it makes redundant.
			if let canonical = try canonicalizeRuleForAssert(formula) {
				try super.query(
					sql: sqlForInsert(
						ofFormula: try formulaToJSON(canonical.store),
						usingParameters: true
					))

				// A rule made redundant by this one is superseded *by it* — the only case where
				//  `superceded_by` names another `_rule` row rather than a bare retraction act, which is
				//  what keeps the two reasons a row left the believed set distinguishable by a join.
				//  The target only exists if the insert actually happened: an equivalent live row makes it
				//  a no-op via `ON CONFLICT DO NOTHING`, and then nothing was subsumed anyway.
				if !canonical.supersedes.isEmpty, sqlite3_changes(db) > 0 {
					let ruleId = sqlite3_last_insert_rowid(db)
					for redundant in canonical.supersedes {
						try super.query(
							sql: """
								UPDATE _rule SET superceded_by = \(ruleId)
								WHERE superceded_by IS NULL
								  AND formula = jsonb(\(try formulaToJSON(redundant)))
								""")
					}
				}
			}

			// Under an open world, `p(1)` and `-p(1)` are different claims that can both be stored, which
			//  is a contradiction that we don't allow.
			//
			// Deliberately *after* the insert, and inside the transaction. The row is already in place, so
			//  the check sees what this assert actually made derivable rather than only what the formula
			//  says on its face — which is what lets one question cover facts and rules alike. Being in
			//  the transaction is what makes that safe: the throw below rolls the insert back, and no
			//  other connection can commit between the check and the decision it justifies.
			try checkCoherence(of: formula)

			try super.query(sql: "COMMIT")
		} catch {
			try super.query(sql: "ROLLBACK")
			throw error
		}
	}

	/// Retracts a previously asserted fact or rule.
	///
	/// The row is not deleted — it is superseded by a bare `_entity` standing for this retraction act,
	/// so the history stays queryable and `assert → retract → assert` records three acts.
	///
	/// Retraction operates on the **base, not the closure**: retracting a formula that is derivable but
	/// not stored throws `.notFound`. A derived conclusion is retracted by removing what derives it.
	public func retract(formula: Formula) throws {
		let json = try formulaToJSON(formula)

		try super.query(sql: "BEGIN TRANSACTION")
		do {
			try super.query(sql: sqlForSupersede(ofFormula: json, usingParameters: true))
			// The supersession is the last statement in that batch and carries no `RETURNING`, so it has
			//  run to completion by now; `sqlite3_changes` excludes rows touched by its triggers.
			guard sqlite3_changes(db) > 0 else {
				throw RetractionError.notFound(formula)
			}
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

	/// The two statements that assert a formula: a fresh `_entity` for the assertion act, then the
	/// `_rule` row that hangs off it. `formula` is unique only over *live* rows, so the upsert repeats
	/// the partial index's `WHERE` — a superseded row must not block re-assertion.
	private func sqlForInsert(ofFormula expr: String, usingParameters: Bool) -> SQL {
		SQL(
			"""
			INSERT INTO _entity (internal_entity_id) VALUES (NULL);
			INSERT INTO _rule (internal_entity_id, formula)
			VALUES (last_insert_rowid(), jsonb(\(usingParameters ? expr : SQL(expr))))
			ON CONFLICT (formula) WHERE superceded_by IS NULL DO NOTHING
			""")
	}

	/// The two statements that retract a formula: a bare `_entity` for the retraction act, then the
	/// supersession of the live `_rule` row with that canonical form. Shared by `retract(formula:)` and
	/// the per-predicate `DELETE` triggers — the same operation reached from two surfaces.
	func sqlForSupersede(ofFormula expr: String, usingParameters: Bool) -> SQL {
		SQL(
			"""
			INSERT INTO _entity (internal_entity_id) VALUES (NULL);
			UPDATE _rule SET superceded_by = last_insert_rowid()
			WHERE superceded_by IS NULL
			  AND formula = jsonb(\(usingParameters ? expr : SQL(expr)))
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
		// Reject a negative name at declaration time. `getColumns` strips the `-`, so a `_predicate` row
		//  for `-p` would never be consulted: the declared columns would be silently ignored and `-p`
		//  would take `p`'s — or, if `p` were never declared, resolve to nothing despite appearing to
		//  exist. Rejecting here is much cheaper than either failure.
		guard !createTable.tableName.hasPrefix("-") else {
			let positive = String(createTable.tableName.dropFirst())
			throw SQLiteError.queryError(
				"cannot declare '\(createTable.tableName)': negative predicates are implicit — "
					+ "declare '\(positive)' and its negation follows, sharing its columns"
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
			let descr: SQL = createTable.comment.flatMap { "\($0)" } ?? SQL("null")
			let insertSQL: SQL = """
					INSERT \(orIgnore)INTO _predicate (internal_entity_id, name, column_names, descr)
					VALUES (last_insert_rowid(), \(createTable.tableName), jsonb(\(columnNamesJson)), \(descr))
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
			WHERE superceded_by IS NULL
			  AND output_type = '@\(name)'
			  AND negative_literal_count = 0
			"""
	}

	func createViewAndTrigger(for tableName: String, columns: [String], rules: [Formula]) throws {
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
			"CREATE TEMP VIEW IF NOT EXISTS [\(tableName)] (\(columnList)) AS \(unionedSelects)"

		try super.query(sql: SQL(createViewSQL))

		try createWriteTriggers(for: tableName, columns: columns, backing: .view)
	}

	/// How a predicate happens to be backed right now, and therefore which trigger *wrapper* its write
	/// surface takes. The trigger bodies are identical between the two — that is the whole point.
	enum PredicateBacking {
		/// A `TEMP VIEW`, which SQLite lets `INSTEAD OF` triggers stand in for entirely.
		case view

		/// A materialized `TEMP TABLE` (see `IterativeEvaluator`). SQLite has no `INSTEAD OF` trigger for
		/// tables, but `RAISE(IGNORE)` in a `BEFORE` trigger is the equivalent: it abandons the remainder
		/// of the trigger program and skips the row without aborting the statement. Without it, a write
		/// would land in the temp table and vanish at the next rebuild — so whether an insert persisted
		/// would depend on whether the predicate happened to be materialized at that moment.
		case materializedTable
	}

	/// Emits a predicate's write surface: `INSERT` asserts a base fact, `DELETE` retracts one, `UPDATE`
	/// is refused. Both backings get the same bodies, so a write means the same thing regardless of how
	/// the predicate is backed — and the same thing `assert(formula:)` / `retract(formula:)` mean.
	func createWriteTriggers(for tableName: String, columns: [String], backing: PredicateBacking)
		throws
	{
		func formulaCall(_ prefix: String) -> String {
			"predicate_formula('\(tableName)', "
				+ columns.map { "\(prefix).[\($0)]" }.joined(separator: ", ")
				+ ")"
		}

		let insertWrapper =
			backing == .view
			? "INSTEAD OF INSERT ON [\(tableName)] FOR EACH ROW"
			: "BEFORE INSERT ON [\(tableName)] FOR EACH ROW WHEN NOT is_materializing()"

		// `RAISE(IGNORE)` must come *last*: it abandons the rest of the trigger program.
		let ignore = backing == .view ? "" : "\n  SELECT RAISE(IGNORE);"

		// Build each trigger's text first, then wrap: `SQL(String)` takes it verbatim, whereas an `SQL`
		//  string *literal* would bind the interpolations as `?` parameters.
		let insertTrigger = """
			CREATE TEMP TRIGGER IF NOT EXISTS [\(tableName)_insert_trigger]
			\(insertWrapper)
			BEGIN
			\(sqlForInsert(ofFormula: formulaCall("NEW"), usingParameters: false).queryText);\(ignore)
			END
			"""
		try super.query(sql: SQL(insertTrigger))

		// Retraction operates on the base, not the closure: a row with no live base fact behind it came
		//  from a rule arm and cannot be retracted — say so rather than silently no-op'ing. The
		//  check-then-act probe is a unique-index hit on `idx_rule_live_formula`, and avoids relying on
		//  `changes()` semantics inside a trigger body.
		let rowText =
			"'\(tableName)(' || "
			+ columns.map { "OLD.[\($0)]" }.joined(separator: " || ', ' || ")
			+ " || ')'"
		let deleteWrapper =
			backing == .view
			? "INSTEAD OF DELETE ON [\(tableName)] FOR EACH ROW"
			: "BEFORE DELETE ON [\(tableName)] FOR EACH ROW"
		let deleteTrigger = """
			CREATE TEMP TRIGGER IF NOT EXISTS [\(tableName)_delete_trigger]
			\(deleteWrapper)
			BEGIN
			  SELECT RAISE(ABORT, \(rowText) || ' is derived, not asserted — retract what it follows from')
			  WHERE NOT EXISTS (
			    SELECT 1 FROM _rule
			    WHERE superceded_by IS NULL AND formula = jsonb(\(formulaCall("OLD"))));
			\(sqlForSupersede(ofFormula: formulaCall("OLD"), usingParameters: false).queryText);\(ignore)
			END
			"""
		try super.query(sql: SQL(deleteTrigger))

		// No retract-and-assert pair has an obvious atomic reading. A view rejects `UPDATE` natively for
		//  want of an `INSTEAD OF UPDATE` trigger; the table form matches it with a better message.
		guard backing == .materializedTable else { return }
		let noUpdateTrigger = """
			CREATE TEMP TRIGGER IF NOT EXISTS [\(tableName)_no_update]
			BEFORE UPDATE ON [\(tableName)]
			BEGIN
			  SELECT RAISE(ABORT, 'cannot UPDATE predicate \(tableName): DELETE the fact and INSERT the replacement');
			END
			"""
		try super.query(sql: SQL(noUpdateTrigger))
	}

	// Returns nil on unknown predicate
	func getColumns<T: StringProtocol>(for predicate: T) throws -> [String]? {
		var cursor: SQLiteCursor? = nil
		return try getColumns(for: predicate, query: &cursor)
	}

	func getColumns<T: StringProtocol>(for predicate: T, query: inout SQLiteCursor?) throws
		-> [String]?
	{
		// A negative predicate is not separately declared (and cannot be — see `interceptCreateTable`):
		//  `-p` resolves to `p`'s declared columns.
		let declaredName =
			predicate.hasPrefix("-") ? String(predicate.dropFirst()) : String(predicate)

		var cursor: SQLiteCursor
		if let q = query {
			cursor = try q.rerun(withArguments: [declaredName])
		} else {
			cursor = try super.query(
				sql:
					"SELECT json(column_names) as json_array FROM _predicate WHERE name = \(declaredName)"
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
		var retrySQL = sql.at(startIndex: startIndex)

		if try involvesRecursion(predicateName) {
			// A predicate that (transitively) involves recursion can't be a plain view. Route by shape:

			if try coneIsFinite(predicateName) {
				// A *finite* (purely relational) cone — mutual, non-linear, or plain — is evaluated by
				//  iterative fixpoint materialization, which terminates and handles every shape uniformly.
				try materialize(topPredicate: predicateName)
			} else {
				// A cone containing a *value-generating* predicate (arithmetic under recursion: `nat`,
				//  `square`, `inc`) is materialized as CTEs in one `WITH RECURSIVE` statement prepended to
				//  the failing query — only the CTE can *stream* an otherwise-infinite relation.
				//  We can't wrap that in a view because SQLite wouldn't push the query's WHERE clause
				//  down into the recursive arm of the CTE, so the recursion wouldn't terminate. Instead we
				//  derive the bounds ourselves and inject them into each recursive step.
				// See https://github.com/sqlite/sqlite/blob/6176034151d10b29a38b0e67f27818a719c68139/src/select.c#L5054
				retrySQL = try buildRecursiveClosureCTE(
					topPredicate: predicateName,
					topColumns: columnNames,
					sql: retrySQL,
				)
			}
		} else {
			let rules = try fetchRules(for: predicateName)
			try createViewAndTrigger(for: predicateName, columns: columnNames, rules: rules)
		}
		return try query(sql: retrySQL)
	}

	func fetchRules(for predicateName: String) throws -> [Formula] {
		let cursor = try super.query(
			sql: """
				SELECT json(formula) as json
				FROM _rule
				WHERE superceded_by IS NULL
				  AND negative_literal_count > 0
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
