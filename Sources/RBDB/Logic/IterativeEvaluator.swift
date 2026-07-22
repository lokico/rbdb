import Foundation
import SQLite3

// Finite (relational) recursion evaluator. Where `buildRecursiveClosureCTE` streams value-generating
// recursion lazily out of a `WITH RECURSIVE` CTE, this evaluates *finite* recursion by iterative
// semi-naive... (naive, for now) fixpoint materialization into TEMP tables. Because each step is a
// plain non-recursive join, it handles linear, mutual, and non-linear recursion uniformly — the
// shapes SQLite's linear recursive CTE can't express. See PLAN-HYBRID-EVAL.md and `rescue`'s routing.
extension RBDB {
	/// The dependency cone of `start`: `start` together with every predicate transitively referenced
	/// through rule bodies. This is the set of predicates whose facts contribute to `start`.
	func dependencyCone(of start: String) throws -> Set<String> {
		var cone: Set<String> = []
		var stack = [start]
		while let name = stack.popLast() {
			guard cone.insert(name).inserted else { continue }
			for rule in try fetchRules(for: name) {
				guard case .hornClause(_, let bodies) = rule else { continue }
				for body in bodies { stack.append(body.name) }
			}
		}
		return cone
	}

	/// Whether rule `R` contains any arithmetic (`.expression`) anywhere — head *or* body. Value
	/// generation isn't visible from the head alone: `nat(X) :- nat(X - 1)` has a bare-variable head
	/// and generates values by inverting the body expression, so the whole clause must be scanned.
	private func doesArithmetic(_ rule: Formula) -> Bool {
		guard case .hornClause(let head, let bodies) = rule else { return false }
		func any(_ predicate: Predicate) -> Bool {
			predicate.arguments.contains {
				if case .expression = $0 { return true } else { return false }
			}
		}
		return any(head) || bodies.contains(where: any)
	}

	/// Whether `name` is value-generating: it involves recursion *and* some rule of it does arithmetic.
	/// Arithmetic on a recursion-carrying predicate is the marker of a relation that can escape the
	/// finite active domain (`nat`, `square`, `inc`) — such predicates must stream via the CTE.
	private func valueGenerating(_ name: String) throws -> Bool {
		try involvesRecursion(name) && fetchRules(for: name).contains(where: doesArithmetic)
	}

	/// Whether the dependency cone of `predicate` is purely finite (relational) recursion — i.e. no
	/// predicate in it is value-generating. Finite cones route to the iterative evaluator; a cone that
	/// contains any value-generating predicate routes to the streaming CTE.
	func coneIsFinite(_ predicate: String) throws -> Bool {
		try !dependencyCone(of: predicate).contains(where: valueGenerating)
	}

	/// Builds — or, if the tables already exist, incrementally *extends* — the materialized closure of
	/// `topPredicate`'s dependency cone. Every step is `CREATE … IF NOT EXISTS` + `INSERT OR IGNORE`, so
	/// calling this again on an already-materialized cone just re-runs the fixpoint from the current
	/// rows. In a positive (monotonic) program that only adds newly-derivable facts, which is exactly
	/// what `refreshDirtyMaterializations` relies on to reflect newly asserted facts and rules without
	/// dropping (and thus without taking the on-disk table lock).
	func materialize(topPredicate: String) throws {
		let cone = try dependencyCone(of: topPredicate)

		// Cone members with rules (derived predicates) are materialized as temp tables. Pure base-fact
		//  predicates resolve through their own views, referenced by name — create those now, since on a
		//  freshly opened DB the per-connection view may not exist yet.
		var derived: [(name: String, columns: [String], rules: [Formula])] = []
		for name in cone {
			guard let columns = try getColumns(for: name) else { continue }
			let rules = try fetchRules(for: name)
			if rules.isEmpty {
				try createViewAndTrigger(for: name, columns: columns, rules: [])
				continue
			}
			derived.append((name, columns, rules))
		}

		// Temp tables this call touches. Building the closure isn't atomic: a failure partway (a bad
		//  seed, `ruleIntoSQL`, or fixpoint insert) would leave a table partially populated, and a later
		//  query would read it as if complete and silently return wrong rows. On failure we drop them so
		//  the next query re-fails cleanly on the missing table and rematerializes from current data.
		var touched: [String] = []
		func discardPartialMaterialization() {
			for name in touched {
				// `SQL(String)` takes the text verbatim; interpolating into an `SQL` literal would bind
				//  the name as a parameter (as the parameterized statements further below do).
				let drop = "DROP TABLE IF EXISTS [\(name)]"
				_ = try? super.query(sql: SQL(drop))
			}
		}

		do {
			// 1. Create a temp table per derived predicate (UNIQUE over all columns for `INSERT OR IGNORE`
			//    dedup) and seed it with the predicate's base facts.
			for entry in derived {
				// A predicate that was queried while it wasn't (yet) recursive has a plain TEMP VIEW of
				//  the same name (with an `INSTEAD OF INSERT` trigger). A later rule can pull it into a
				//  finite recursive cone — making it a *derived* member here — but nothing dropped that
				//  view: the `_rule` trigger only drops the view of the predicate a new rule is *for*, not
				//  ones dragged in transitively. Left in place, `CREATE TABLE IF NOT EXISTS` silently
				//  no-ops against it and our `INSERT OR IGNORE` fires the view's trigger — asserting base
				//  facts into `_rule` (never dedup'd) instead of filling a table, so the fixpoint's
				//  `total_changes` never settles and the loop spins forever. Drop the shadowing view (which
				//  also drops its trigger) so the temp table owns the name.
				try dropIfView(entry.name)

				let columnList = entry.columns.map { "[\($0)]" }.joined(separator: ", ")
				let create =
					"CREATE TEMP TABLE IF NOT EXISTS [\(entry.name)] (\(columnList), UNIQUE (\(columnList)))"
				try super.query(sql: SQL(create))
				touched.append(entry.name)

				// Build the text first, then wrap: `SQL(String)` takes it verbatim, whereas an `SQL`
				//  string *literal* would bind the interpolations as `?` parameters. Use `query` (not
				//  `super.query`) so a failed statement is torn down through the same path as the initial
				//  build — `super.query` was leaving the statement open, blocking the cleanup `DROP`.
				let seed =
					"INSERT OR IGNORE INTO [\(entry.name)]\n\(baseFactsSelect(for: entry.name, columns: entry.columns))"
				try query(sql: SQL(seed))
			}

			// 2. Precompute each rule's insert. Body predicates resolve by name — materialized ones are
			//    the temp tables above; base-fact ones their views. Referencing a relation twice is legal
			//    now (ordinary tables, not a recursive CTE), so non-linear rules just work.
			var columnsQuery: SQLiteCursor? = nil
			let getCols: (String) throws -> [String] = { name in
				guard let cols = try self.getColumns(for: name, query: &columnsQuery) else {
					throw RBDBError.corruptData(
						message: "rule references unknown predicate '\(name)'")
				}
				return cols
			}
			var inserts: [SQL] = []
			for entry in derived {
				for rule in entry.rules {
					let ruleSQL = try rule.ruleIntoSQL(getCols)
					let insert = "INSERT OR IGNORE INTO [\(entry.name)]\n\(ruleSQL)"
					inserts.append(SQL(insert))
				}
			}

			// 3. Naive fixpoint: run every rule over the full relations until a pass derives nothing new.
			//    `total_changes` counts only rows actually inserted (ignored duplicates don't count), so a
			//    pass with no change means the fixpoint is reached. Finite domain ⟹ it terminates.
			while true {
				let before = sqlite3_total_changes(db)
				for insert in inserts { try query(sql: insert) }
				if sqlite3_total_changes(db) == before { break }
			}
		} catch {
			discardPartialMaterialization()
			throw error
		}

		// Record the dependency cone so the `_rule` trigger can flag this closure dirty when any member
		//  changes. Recorded only after a successful build, keyed by the top predicate (the refresh unit).
		for member in cone {
			try super.query(
				sql:
					"INSERT OR IGNORE INTO _materialized_dep (materialized_top, depends_on) VALUES (\(topPredicate), \(member))"
			)
		}
	}

	/// Re-iterates every materialized closure the `_rule` trigger flagged stale (in `_dirty`), then
	/// clears the flags. Called at the top of every `query` — a safe point, outside any trigger and with
	/// no statement mid-flight — so the on-disk table lock that blocks an in-trigger `DROP` never
	/// applies. Positive Datalog is monotonic, so re-running the fixpoint over the existing rows only
	/// adds the newly-derivable facts; no drop-and-rebuild is needed.
	func refreshDirtyMaterializations() throws {
		// `materialize` runs queries of its own; guard against re-entering this refresh from them.
		guard !isRefreshing else { return }
		isRefreshing = true
		defer { isRefreshing = false }

		let dirty = try super.query(sql: "SELECT name FROM _dirty")
			.compactMap { $0["name"] as? String }
		guard !dirty.isEmpty else { return }

		for name in dirty {
			if try coneIsFinite(name) {
				try materialize(topPredicate: name)
			} else {
				// A newly-asserted rule made the cone value-generating (arithmetic under recursion): the
				//  iterative fixpoint would no longer terminate. Drop the closure so the next query routes
				//  it to the streaming CTE instead. Safe to drop here — no statement is in flight.
				try invalidateMaterialization(name)
			}
		}
		try super.query(sql: "DELETE FROM _dirty")
	}

	/// If `name` exists and is a view, drops it. Otherwise does nothing.
	private func dropIfView(_ name: String) throws {
		let isView =
			try super.query(
				sql: "SELECT 1 FROM temp.sqlite_schema WHERE type = 'view' AND name = \(name)"
			).makeIterator().next() != nil
		guard isView else { return }
		// Build the text first so `name` interpolates as a verbatim identifier: a `let` string is plain
		//  Swift interpolation, whereas `SQL("… [\(name)]")` inline would parse as an `SQL` literal and
		//  turn `[\(name)]` into the bracketed identifier `[?]` plus a stray bound argument. The lookup
		//  above, by contrast, wants `name` bound as a value parameter — which its interpolation does.
		let drop = "DROP VIEW IF EXISTS [\(name)]"
		try super.query(sql: SQL(drop))
	}

	/// Drops a materialized closure and forgets its dependency rows, so the next query rebuilds it from
	/// scratch (or, if it is no longer finite, routes it to the CTE).
	private func invalidateMaterialization(_ name: String) throws {
		// The name is an identifier ⟹ inline it verbatim via `SQL(String)`; the `DELETE` below binds it
		//  as a value parameter, which is correct there.
		let drop = "DROP TABLE IF EXISTS [\(name)]"
		try super.query(sql: SQL(drop))
		try super.query(sql: "DELETE FROM _materialized_dep WHERE materialized_top = \(name)")
	}
}
