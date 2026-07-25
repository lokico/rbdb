import Foundation
import Testing

@testable import Datalog
@testable import RBDB

@Suite("Comparison guards")
struct ComparisonGuardTests {

	/// Builds the family DB from the user's motivating example.
	private func familyDB() throws -> RBDB {
		let db = try RBDB(path: ":memory:")
		try db.query(sql: "CREATE TABLE male(name)")
		try db.query(sql: "CREATE TABLE parent(parent, child)")
		try db.query(sql: "CREATE TABLE brother(brother, sibling)")

		try db.assert(datalog: "male('bob')")
		try db.assert(datalog: "male('tom')")
		try db.assert(datalog: "parent('sr', 'bob')")
		try db.assert(datalog: "parent('sr', 'tom')")
		try db.assert(datalog: "parent('sr', 'ann')")  // a sister
		return db
	}

	@Test("the motivating example: brother(B, S) :- male(B), parent(X, B), parent(X, S), B != S")
	func brotherRuleFiltersSelf() throws {
		let db = try familyDB()
		try db.assert(
			datalog: "brother(B, S) :- male(B), parent(X, B), parent(X, S), B != S")

		let pairs = Set(
			try db.query(datalog: "brother(B, S)").map {
				[$0["B"] as! String, $0["S"] as! String]
			})

		// bob and tom are each other's brothers; each is also a brother of their sister ann. The
		//  `B != S` guard is what removes the spurious self-pairs (bob,bob) and (tom,tom).
		#expect(pairs == [["bob", "tom"], ["bob", "ann"], ["tom", "bob"], ["tom", "ann"]])
		#expect(!pairs.contains(["bob", "bob"]), "the B != S guard removes self-siblings")
		#expect(!pairs.contains(["tom", "tom"]), "the B != S guard removes self-siblings")
	}

	@Test("without the guard, self-siblings are (wrongly) derived")
	func withoutGuardSelfSiblingsAppear() throws {
		// Contrast case, pinning down exactly what the guard is responsible for filtering.
		let db = try familyDB()
		try db.assert(datalog: "brother(B, S) :- male(B), parent(X, B), parent(X, S)")

		let pairs = Set(
			try db.query(datalog: "brother(B, S)").map {
				[$0["B"] as! String, $0["S"] as! String]
			})
		#expect(pairs.contains(["bob", "bob"]), "no guard ⟹ self-pair present")
	}

	@Test("numeric guard filters on a bound column")
	func numericGuard() throws {
		let db = try RBDB(path: ":memory:")
		try db.query(sql: "CREATE TABLE reading(sensor, value)")
		try db.query(sql: "CREATE TABLE high(sensor)")
		try db.assert(datalog: "reading('a', 3)")
		try db.assert(datalog: "reading('b', 10)")
		try db.assert(datalog: "reading('c', 20)")

		// high(S) :- reading(S, V), V > 9
		try db.assert(datalog: "high(S) :- reading(S, V), V > 9")
		let sensors = Set(try db.query(datalog: "high(S)").compactMap { $0["S"] as? String })
		#expect(sensors == ["b", "c"], "only readings above 9: \(sensors)")
	}

	@Test("guard over an arithmetic expression")
	func arithmeticGuard() throws {
		let db = try RBDB(path: ":memory:")
		try db.query(sql: "CREATE TABLE box(id, w, h)")
		try db.query(sql: "CREATE TABLE tall(id)")
		try db.assert(datalog: "box('a', 2, 3)")  // h > w
		try db.assert(datalog: "box('b', 5, 4)")  // h < w
		try db.assert(datalog: "box('c', 1, 10)")  // h > w

		// tall(Id) :- box(Id, W, H), W < H
		try db.assert(datalog: "tall(Id) :- box(Id, W, H), W < H")
		let ids = Set(try db.query(datalog: "tall(Id)").compactMap { $0["Id"] as? String })
		#expect(ids == ["a", "c"])
	}

	@Test("guard over an arithmetic operand filters on the computed value")
	func arithmeticOperandGuard() throws {
		let db = try RBDB(path: ":memory:")
		try db.query(sql: "CREATE TABLE span(lo, hi)")
		try db.query(sql: "CREATE TABLE wide(lo, hi)")
		try db.assert(datalog: "span(1, 3)")  // hi is more than one above lo
		try db.assert(datalog: "span(1, 2)")  // adjacent — excluded
		try db.assert(datalog: "span(5, 4)")  // inverted — excluded

		// The guard's *operand* is an arithmetic expression, so it lowers to a computed SQL condition.
		try db.assert(datalog: "wide(L, H) :- span(L, H), L + 1 < H")
		let rows = Set(
			try db.query(datalog: "wide(L, H)").map { [$0["L"] as! Int64, $0["H"] as! Int64] })
		#expect(rows == [[1, 3]])
	}

	@Test("a guard applies inside a recursive rule")
	func guardInRecursiveRule() throws {
		let db = try RBDB(path: ":memory:")
		try db.query(sql: "CREATE TABLE edge(a, b)")
		try db.query(sql: "CREATE TABLE path(a, b)")
		try db.assert(datalog: "edge(1, 2)")
		try db.assert(datalog: "edge(2, 3)")
		try db.assert(datalog: "edge(3, 1)")  // closes a cycle, so the guard must do real work

		try db.assert(datalog: "path(X, Y) :- edge(X, Y)")
		// The recursive step is lowered inside a WITH RECURSIVE CTE, where the guard lands in the
		//  recursive SELECT's WHERE clause (and has to coexist with bound injection).
		try db.assert(datalog: "path(X, Z) :- path(X, Y), edge(Y, Z), X < Z")

		let pairs = Set(
			try db.query(datalog: "path(A, B)").map { [$0["A"] as! Int64, $0["B"] as! Int64] })
		// The three base edges, plus the one transitive extension with increasing endpoints (1→3).
		//  (2,1) and (3,2) are blocked by the guard, and so is (1,1) closing the cycle.
		#expect(pairs == [[1, 2], [2, 3], [3, 1], [1, 3]])
	}

	@Test("a false guard with no body literals derives nothing")
	func guardOnlyBodyIsEnforced() throws {
		let db = try RBDB(path: ":memory:")
		try db.query(sql: "CREATE TABLE p(a)")
		try db.query(sql: "CREATE TABLE q(a)")

		// A ground head with a guard-only body: there is no table to hang the condition on, but the
		//  guard still decides whether the head is derived.
		try db.assert(datalog: "p(1) :- 2 < 1")
		#expect(Array(try db.query(datalog: "p(X)")).isEmpty, "a false guard derives nothing")

		try db.assert(datalog: "q(1) :- 1 < 2")
		#expect(
			Array(try db.query(datalog: "q(X)")).count == 1, "a true guard derives the head")
	}

	// MARK: - Validation

	@Test("a guard variable not bound by a positive literal is rejected as unsafe")
	func unsafeGuardVariableRejected() throws {
		let db = try RBDB(path: ":memory:")
		try db.query(sql: "CREATE TABLE p(a)")
		try db.query(sql: "CREATE TABLE q(a)")

		// Y appears only in the guard, never in a positive body literal ⟹ unsafe (not range-restricted).
		#expect(throws: ValidationError.self) {
			try db.assert(datalog: "q(X) :- p(X), X < Y")
		}
	}

	@Test("a guard whose variables are all body-bound is accepted")
	func safeGuardAccepted() throws {
		let db = try RBDB(path: ":memory:")
		try db.query(sql: "CREATE TABLE p(a, b)")
		try db.query(sql: "CREATE TABLE q(a, b)")
		// Both X and Y are bound by p, so the guard is safe.
		try db.assert(datalog: "q(X, Y) :- p(X, Y), X < Y")
		#expect(try db.fetchRules(for: "q").count == 1)
	}

	// MARK: - Storage / canonical form

	@Test("a guard makes the clause a rule (non-zero negative_literal_count)")
	func guardCountsTowardBody() throws {
		let db = try RBDB(path: ":memory:")
		try db.query(sql: "CREATE TABLE p(a, b)")
		try db.query(sql: "CREATE TABLE q(a, b)")
		try db.assert(datalog: "q(X, Y) :- p(X, Y), X != Y")

		// One positive literal + one guard ⟹ body length 2 ⟹ negative_literal_count 2.
		let rows = Array(
			try db.query(
				sql:
					"SELECT negative_literal_count AS n FROM _rule WHERE output_type = '@q'"))
		#expect(rows.first?["n"] as? Int64 == 2)
	}

	@Test("a guard survives a store/fetch round-trip through canonical JSON")
	func guardRoundTripsThroughStorage() throws {
		let db = try RBDB(path: ":memory:")
		try db.query(sql: "CREATE TABLE p(a, b)")
		try db.query(sql: "CREATE TABLE q(a, b)")
		try db.assert(datalog: "q(X, Y) :- p(X, Y), X != Y")

		let rules = try db.fetchRules(for: "q")
		#expect(rules.count == 1)
		guard case .hornClause(_, let body, let guards) = rules.first else {
			Issue.record("expected a Horn clause")
			return
		}
		#expect(body.count == 1, "one positive literal")
		#expect(guards.count == 1, "one guard survived the round-trip")
		#expect(guards.first?.operation == .ne)
	}

	@Test("commuted `>`/`<` guards canonicalize to the same stored rule")
	func commutedGuardsDedup() throws {
		let db = try RBDB(path: ":memory:")
		try db.query(sql: "CREATE TABLE p(a, b)")
		try db.query(sql: "CREATE TABLE q(a, b)")

		// `X < Y` and `Y > X` are the same comparison; the second assertion collides with the first
		//  on the `_rule.formula` UNIQUE constraint (both fold to the identical canonical guard), which
		//  is silently ignored rather than thrown.
		try db.assert(datalog: "q(X, Y) :- p(X, Y), X < Y")
		try db.assert(datalog: "q(X, Y) :- p(X, Y), Y > X")
		let rows = Array(
			try db.query(sql: "SELECT COUNT(*) AS c FROM _rule WHERE output_type='@q'"))
		#expect(rows.first?["c"] as? Int64 == 1, "commuted guards store as one rule")
	}

	@Test("a rule without a guard subsumes the same rule with one, order-independently")
	func guardSubsumption() throws {
		func storedRules(_ order: (RBDB) throws -> Void) throws -> [Formula] {
			let db = try RBDB(path: ":memory:")
			try db.query(sql: "CREATE TABLE p(a, b)")
			try db.query(sql: "CREATE TABLE q(a, b)")
			try order(db)
			return try db.fetchRules(for: "q")
		}

		// A guard is a body constraint like any other, so `q :- p` derives a superset of
		//  `q :- p, X < Y` and subsumes it whichever order they arrive in.
		for rules in [
			try storedRules { db in
				try db.assert(datalog: "q(X, Y) :- p(X, Y), X < Y")
				try db.assert(datalog: "q(X, Y) :- p(X, Y)")
			},
			try storedRules { db in
				try db.assert(datalog: "q(X, Y) :- p(X, Y)")
				try db.assert(datalog: "q(X, Y) :- p(X, Y), X < Y")
			},
		] {
			#expect(rules.count == 1, "only the unguarded rule survives")
			guard case .hornClause(_, _, let guards) = rules.first else {
				Issue.record("expected a Horn clause")
				return
			}
			#expect(guards.isEmpty, "the surviving rule is the general (unguarded) one")
		}
	}

	@Test("a malformed guard reports the guard's own decoding error")
	func malformedGuardDecodingError() throws {
		// `{"<": [...]}` is unambiguously a guard (a predicate is an unkeyed array), so a guard with
		//  the wrong operand count must surface as such — not as "expected an array, found an object"
		//  from the predicate decoder it would otherwise fall through to.
		let json = #"["@p",[{"":1}],{"<":[{"v":0}]}]"#
		var message = ""
		#expect(throws: (any Error).self) {
			do {
				_ = try JSONDecoder().decode(Formula.self, from: Data(json.utf8))
			} catch let error as DecodingError {
				if case .dataCorrupted(let context) = error { message = context.debugDescription }
				throw error
			}
		}
		#expect(message == "Invalid expression", "got: \(message)")
	}
}
