import Foundation
import Testing

@testable import RBDB
@testable import Datalog

@Suite("Datalog Extension Tests")
struct DatalogExtensionTests {

	@Test("assert(datalog:) and query(datalog:) with README sample code")
	func readmeSampleCode() async throws {
		// IMPORTANT: This test uses the exact sample code from the README.
		// If this test fails due to an intentional breaking API change,
		// you MUST update the corresponding sample code in README.md as well.

		let db = try RBDB(path: ":memory:")

		// Create tables for our predicates
		try db.query(sql: "CREATE TABLE parent(parent, child)")
		try db.query(sql: "CREATE TABLE grandparent(grandparent, grandchild)")

		// Assert some facts using datalog syntax
		try db.assert(datalog: "parent('John', 'Mary')")
		try db.assert(datalog: "parent('Mary', 'Tom')")
		try db.assert(datalog: "parent('Bob', 'Alice')")

		// Define a rule: grandparent(X, Z) :- parent(X, Y), parent(Y, Z)
		try db.assert(datalog: "grandparent(X, Z) :- parent(X, Y), parent(Y, Z)")

		// Query back using SQL to verify the rule works
		let result = try db.query(sql: "SELECT * FROM grandparent")
		let rows = Array(result)

		#expect(rows.count == 1, "Should have exactly one grandparent relationship")
		#expect(rows[0]["grandparent"] as? String == "John", "Grandparent should be John")
		#expect(rows[0]["grandchild"] as? String == "Tom", "Grandchild should be Tom")
	}

	@Test("querying a projected subset of columns returns distinct rows")
	func projectedQueryIsDistinct() async throws {
		let db = try RBDB(path: ":memory:")
		try db.query(sql: "CREATE TABLE parent(parent, child)")
		try db.query(sql: "CREATE TABLE grandparent(grandparent, grandchild)")
		for (p, c) in [
			("Amy", "Maeve"), ("Amy", "Henry"), ("Laura", "Mia"), ("Sophie", "Enda"),
			("Sophie", "Cara"), ("Antonette", "Amy"), ("Antonette", "Laura"),
			("Antonette", "Sophie"),
		] {
			try db.assert(datalog: "parent('\(p)', '\(c)')")
		}
		try db.assert(datalog: "grandparent(X, Z) :- parent(X, Y), parent(Y, Z)")

		// `grandparent(X)` projects away the grandchild column. Antonette is the grandparent of
		//  five people, but as a *set* of grandparents the answer is just {Antonette}.
		let grandparents = Array(try db.query(datalog: "grandparent(X)"))
			.compactMap { $0["X"] as? String }
		#expect(grandparents == ["Antonette"])

		// The full-arity query still returns every distinct pair.
		#expect(Array(try db.query(datalog: "grandparent(X, Z)")).count == 5)
	}

	@Test("query(datalog:) basic functionality")
	func queryDatalogBasic() async throws {
		let db = try RBDB(path: ":memory:")

		try db.query(sql: "CREATE TABLE user(name)")
		try db.assert(datalog: "user('Alice')")
		try db.assert(datalog: "user('Bob')")

		// Query with variable
		let results = try db.query(datalog: "user(Name)")
		let rows = Array(results)

		#expect(rows.count == 2, "Should return two users")
		let names = rows.compactMap { $0["Name"] as? String }.sorted()
		#expect(names == ["Alice", "Bob"], "Should return Alice and Bob")
	}

	@Test("assert(datalog:) basic functionality")
	func assertDatalogBasic() async throws {
		let db = try RBDB(path: ":memory:")

		try db.query(sql: "CREATE TABLE user(name)")

		// Assert using datalog syntax
		try db.assert(datalog: "user('Charlie')")

		// Verify using SQL query
		let result = try db.query(sql: "SELECT * FROM user")
		let rows = Array(result)

		#expect(rows.count == 1, "Should have one user")
		#expect(rows[0]["name"] as? String == "Charlie", "Name should be Charlie")
	}

	@Test("assert(datalog:) with rule")
	func assertDatalogRule() async throws {
		let db = try RBDB(path: ":memory:")

		try db.query(sql: "CREATE TABLE human(name)")
		try db.query(sql: "CREATE TABLE mortal(name)")

		// Assert a fact
		try db.assert(datalog: "human('Socrates')")

		// Assert a rule: mortal(X) :- human(X)
		try db.assert(datalog: "mortal(X) :- human(X)")

		// Verify the rule works
		let result = try db.query(sql: "SELECT * FROM mortal")
		let rows = Array(result)

		#expect(rows.count == 1, "Should have one mortal")
		#expect(rows[0]["name"] as? String == "Socrates", "Mortal should be Socrates")
	}

	@Test("query(datalog:) with ground formula")
	func queryDatalogGround() async throws {
		let db = try RBDB(path: ":memory:")

		try db.query(sql: "CREATE TABLE user(name)")
		try db.assert(datalog: "user('Alice')")

		// Query for specific user (ground formula)
		let results = try db.query(datalog: "user('Alice')")
		let rows = Array(results)

		#expect(rows.count == 1, "Should return one row for existing user")
		#expect(rows[0]["sat"] as? Int64 == 1, "Should return sat=1 for ground query")

		// Query for non-existent user
		let noResults = try db.query(datalog: "user('Bob')")
		let noRows = Array(noResults)

		#expect(noRows.count == 0, "Should return no rows for non-existent user")
	}

	@Test("recursive natural number with arithmetic")
	func recursiveNaturalNumber() async throws {
		let db = try RBDB(path: ":memory:")

		try db.query(sql: "CREATE TABLE nat(n)")

		// Assert that 0 is a natural number
		try db.assert(datalog: "nat(0)")

		// Assert recursive rule: nat(X + 1) :- nat(X)
		// This defines that if X is a natural number, then X+1 is also a natural number
		try db.assert(datalog: "nat(X + 1) :- nat(X)")

		// Query for a large natural number (e.g., 100)
		// This should work through recursive inference: 0 -> 1 -> 2 -> ... -> 100
		let results = try db.query(datalog: "nat(100)")
		let rows = Array(results)

		#expect(rows.count == 1, "Should infer that 100 is a natural number")
		#expect(rows[0]["sat"] as? Int64 == 1, "Query should be satisfied")
	}

	@Test("recursive arithmetic terminates without spurious matches")
	func recursiveNaturalNumberMiss() async throws {
		let db = try RBDB(path: ":memory:")
		try db.query(sql: "CREATE TABLE evens(n)")
		try db.assert(datalog: "evens(0)")
		try db.assert(datalog: "evens(X + 2) :- evens(X)")

		let hit = Array(try db.query(datalog: "evens(50)"))
		#expect(hit.count == 1, "50 is even")

		let miss = Array(try db.query(datalog: "evens(51)"))
		#expect(miss.count == 0, "51 is not even")
	}

	@Test("arithmetic expressions in rule heads")
	func arithmeticInRuleHeads() async throws {
		let db = try RBDB(path: ":memory:")

		try db.query(sql: "CREATE TABLE base(n)")
		try db.query(sql: "CREATE TABLE doubled(n)")

		try db.assert(datalog: "base(5)")
		try db.assert(datalog: "base(10)")

		// Rule using an arithmetic expression in the head
		try db.assert(datalog: "doubled(X * 2) :- base(X)")

		let hit = Array(try db.query(datalog: "doubled(20)"))
		#expect(hit.count == 1, "doubled(20) should follow from base(10)")
		#expect(hit[0]["sat"] as? Int64 == 1)

		let miss = Array(try db.query(datalog: "doubled(7)"))
		#expect(miss.count == 0, "doubled(7) should not be derivable")
	}

	@Test("commuted rule operands canonicalize to a single stored rule")
	func commutedRulesDedup() async throws {
		let db = try RBDB(path: ":memory:")
		try db.query(sql: "CREATE TABLE b(x)")
		try db.query(sql: "CREATE TABLE d(x)")

		// The same rule written with commuted `+` operands canonicalizes identically, so the
		//  second assertion collides with the first on the `_rule.formula` UNIQUE constraint.
		try db.assert(datalog: "d(X + 1) :- b(X)")
		#expect(throws: (any Error).self) {
			try db.assert(datalog: "d(1 + X) :- b(X)")
		}

		let rows = Array(try db.query(sql: "SELECT COUNT(*) AS c FROM _rule"))
		#expect(rows.first?["c"] as? Int64 == 1, "commuted rules should store as one row")
	}

	@Test("recursive countdown via subtraction")
	func recursiveCountdownWithSubtraction() async throws {
		let db = try RBDB(path: ":memory:")
		try db.query(sql: "CREATE TABLE count(n)")
		try db.assert(datalog: "count(100)")
		try db.assert(datalog: "count(X - 1) :- count(X)")

		#expect(Array(try db.query(datalog: "count(0)")).count == 1, "0 reachable from 100")
		#expect(Array(try db.query(datalog: "count(50)")).count == 1, "50 reachable from 100")
		#expect(Array(try db.query(datalog: "count(200)")).count == 0, "200 is above the start")

		// The lowering must not leave a subtract node in the stored (JSONB) formula.
		let rules = Array(
			try db.query(
				sql: "SELECT json(formula) AS j FROM _rule WHERE negative_literal_count > 0"))
		let json = rules.compactMap { $0["j"] as? String }.joined()
		#expect(!json.isEmpty)
		#expect(!json.contains("subtract"), "stored formula should contain no subtract node")
	}

	@Test("recursive doubling via multiplication")
	func recursiveDoubling() async throws {
		let db = try RBDB(path: ":memory:")
		try db.query(sql: "CREATE TABLE powers(n)")
		try db.assert(datalog: "powers(1)")
		try db.assert(datalog: "powers(X * 2) :- powers(X)")

		#expect(Array(try db.query(datalog: "powers(64)")).count == 1, "64 = 1·2⁶")
		#expect(Array(try db.query(datalog: "powers(65)")).count == 0, "65 is not a power of two")
	}

	@Test("large integer constants keep full precision and don't render in scientific notation")
	func largeIntegerConstants() async throws {
		let db = try RBDB(path: ":memory:")
		try db.query(sql: "CREATE TABLE tens(n)")
		try db.assert(datalog: "tens(1)")
		try db.assert(datalog: "tens(X * 10) :- tens(X)")

		// 10⁹ is reached in nine steps. Numbers were stored as 32-bit `Float`, which both loses
		//  precision past 2²⁴ and renders large values in scientific notation ("1e+09"). The latter
		//  broke the recursion-bound extraction (it read "1e+09" as just "1"), so the recursion
		//  stopped at 1 and the query wrongly returned nothing. `Double` holds it exactly and renders
		//  it as a plain decimal.
		#expect(Array(try db.query(datalog: "tens(1000000000)")).count == 1, "10⁹ = 1·10⁹")
		#expect(
			Array(try db.query(datalog: "tens(1000000001)")).isEmpty, "10⁹+1 is not a power of ten")
	}

	@Test("recursive rule with arithmetic in the body binds the head variable by inversion")
	func recursiveBodyArithmetic() async throws {
		let db = try RBDB(path: ":memory:")
		try db.query(sql: "CREATE TABLE nat(n)")
		try db.assert(datalog: "nat(0)")

		// N is a nat if N-1 is a nat. The arithmetic is on the *body* side and the head is a bare
		//  variable, so N can only be bound by inverting the body expression (N = [nat].n + 1).
		try db.assert(datalog: "nat(N) :- nat(N - 1)")

		#expect(Array(try db.query(datalog: "nat(5)")).count == 1, "5 reachable upward from 0")
		#expect(Array(try db.query(datalog: "nat(100)")).count == 1, "100 reachable upward from 0")
		#expect(Array(try db.query(datalog: "nat(-1)")).count == 0, "-1 is below the base fact")
	}

	@Test("constraining a numeric recursion by a string terminates and is empty")
	func recursionConstrainedByString() async throws {
		let db = try RBDB(path: ":memory:")
		try db.query(sql: "CREATE TABLE nat(n)")
		try db.assert(datalog: "nat(0)")
		try db.assert(datalog: "nat(N + 1) :- nat(N)")

		// The recursion only ever produces numbers, so no natural equals 'hi mom'. The bound must
		//  recognise this and terminate — a naive `[n] <= 'hi mom'` never stops, because SQLite
		//  orders every number before every string.
		#expect(Array(try db.query(datalog: "nat('hi mom')")).isEmpty)
	}

	@Test("recursive countdown with arithmetic in the body")
	func recursiveBodyArithmeticDown() async throws {
		let db = try RBDB(path: ":memory:")
		try db.query(sql: "CREATE TABLE count(n)")
		try db.assert(datalog: "count(100)")

		// N counts if N+1 counts — the body drives the value *down* each step.
		try db.assert(datalog: "count(N) :- count(N + 1)")

		#expect(
			Array(try db.query(datalog: "count(50)")).count == 1, "50 reachable downward from 100")
		#expect(
			Array(try db.query(datalog: "count(0)")).count == 1, "0 reachable downward from 100")
		#expect(Array(try db.query(datalog: "count(200)")).count == 0, "200 is above the base fact")
	}

	@Test("querying a non-recursive rule that references a recursive predicate")
	func ruleOverRecursivePredicate() async throws {
		let db = try RBDB(path: ":memory:")
		try db.query(sql: "CREATE TABLE nat(n)")
		try db.query(sql: "CREATE TABLE square(n, squared)")
		try db.assert(datalog: "nat(0)")
		try db.assert(datalog: "nat(N + 1) :- nat(N)")
		// `square` isn't itself recursive, but its body references the recursive `nat`.
		try db.assert(datalog: "square(X, X * X) :- nat(X)")

		// Bounded query: the constraint propagates into `nat`'s recursion so it terminates.
		let rows = Array(try db.query(datalog: "square(5, Y)"))
		#expect(rows.count == 1, "5 is a nat, so square(5, 25) should hold")
		#expect(rows.first?["Y"] as? Double == 25)

		// Unbounded query: `square` ranges over an infinite `nat`, but must *stream* — you can page
		//  through results one at a time, just like querying `nat(X)` directly. (If `square` were
		//  materialized as an intermediate CTE, fetching even the first row would hang forever.)
		var streamed: [Double] = []
		for row in try db.query(datalog: "square(X, Y)") {
			switch row["X"] {
			case let x as Double: streamed.append(x)
			case let x as Int64: streamed.append(Double(x))
			default: break
			}
			if streamed.count >= 4 { break }
		}
		#expect(streamed == [0, 1, 2, 3], "should stream square rows lazily: \(streamed)")
	}

	@Test("constraining a computed column bounds the recursion by inverting the expression")
	func constraintOnComputedColumn() async throws {
		let db = try RBDB(path: ":memory:")
		try db.query(sql: "CREATE TABLE nat(n)")
		try db.query(sql: "CREATE TABLE square(n, squared)")
		try db.assert(datalog: "nat(0)")
		try db.assert(datalog: "nat(N + 1) :- nat(N)")
		try db.assert(datalog: "square(X, X * X) :- nat(X)")

		// The constraint is on the *computed* column: 4 = X² bounds X ≤ 2, so `nat` terminates.
		let rows = Array(try db.query(datalog: "square(X, 4)"))
		#expect(rows.count == 1, "only 2² = 4")
		let x = (rows.first?["X"] as? Double) ?? (rows.first?["X"] as? Int64).map(Double.init)
		#expect(x == 2)

		// A value that isn't a perfect square terminates with no rows.
		#expect(Array(try db.query(datalog: "square(X, 5)")).isEmpty, "5 is not a perfect square")
	}

	@Test("querying through a multi-level chain that bottoms out in recursion")
	func chainOverRecursivePredicate() async throws {
		let db = try RBDB(path: ":memory:")
		try db.query(sql: "CREATE TABLE nat(n)")
		try db.query(sql: "CREATE TABLE inc(n, m)")
		try db.query(sql: "CREATE TABLE inc2(n, m)")
		try db.assert(datalog: "nat(0)")
		try db.assert(datalog: "nat(N + 1) :- nat(N)")
		try db.assert(datalog: "inc(X, X + 1) :- nat(X)")  // depth 1: inc → nat
		try db.assert(datalog: "inc2(X, Y) :- inc(X, Y)")  // depth 2: inc2 → inc → nat

		func asDouble(_ v: Any??) -> Double? {
			switch v {
			case let x as Double: return x
			case let x as Int64: return Double(x)
			default: return nil
			}
		}

		// Bounded query: the constraint propagates through *both* levels down to `nat`.
		let rows = Array(try db.query(datalog: "inc2(3, Y)"))
		#expect(rows.count == 1, "inc2(3, 4) should hold")
		#expect(asDouble(rows.first?["Y"]) == 4)

		// Unbounded query streams through both inlined levels rather than materializing.
		var streamed: [Double] = []
		for row in try db.query(datalog: "inc2(X, Y)") {
			if let x = asDouble(row["X"]) { streamed.append(x) }
			if streamed.count >= 4 { break }
		}
		#expect(streamed == [0, 1, 2, 3], "should stream through the chain: \(streamed)")
	}

	@Test("exponent in a rule head evaluates via pow")
	func exponentInRuleHead() async throws {
		let db = try RBDB(path: ":memory:")
		try db.query(sql: "CREATE TABLE base(n)")
		try db.query(sql: "CREATE TABLE squared(n)")
		try db.assert(datalog: "base(5)")
		try db.assert(datalog: "base(9)")
		try db.assert(datalog: "squared(X ^ 2) :- base(X)")

		#expect(Array(try db.query(datalog: "squared(25)")).count == 1, "5² = 25")
		#expect(Array(try db.query(datalog: "squared(81)")).count == 1, "9² = 81")
		#expect(Array(try db.query(datalog: "squared(30)")).count == 0)
	}
}
