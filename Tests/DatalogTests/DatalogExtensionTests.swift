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
		//  second assertion collides with the first on the `_rule.formula` UNIQUE constraint, which
		//  is silently ignored rather than thrown.
		try db.assert(datalog: "d(X + 1) :- b(X)")
		try db.assert(datalog: "d(1 + X) :- b(X)")

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

	@Test("mutual recursion between two predicates resolves via a combined tagged CTE")
	func mutualRecursion() async throws {
		let db = try RBDB(path: ":memory:")
		try db.query(sql: "CREATE TABLE female(a)")
		try db.query(sql: "CREATE TABLE son(a, b)")
		try db.query(sql: "CREATE TABLE daughter(a, b)")
		try db.query(sql: "CREATE TABLE parent(a, b)")
		try db.query(sql: "CREATE TABLE grandparent(a, b)")

		try db.assert(datalog: "female('Sophie')")
		try db.assert(datalog: "son('Alex', 'Joe')")
		try db.assert(datalog: "son('Henry', 'Alex')")
		try db.assert(datalog: "son('Enda', 'Sophie')")
		try db.assert(datalog: "daughter('Maeve', 'Alex')")
		try db.assert(datalog: "daughter('Amy', 'Geordie')")
		try db.assert(datalog: "daughter('Cara', 'Sophie')")
		try db.assert(datalog: "parent('Geordie', 'Sophie')")

		// `parent` and `daughter` are mutually recursive: `parent` is derived from `daughter` (and
		//  `son`), while `daughter` is derived from `parent`. Neither predicate is self-recursive, so
		//  the cycle can only be expressed as a single combined `WITH RECURSIVE` CTE.
		try db.assert(datalog: "parent(A, B) :- son(B, A)")
		try db.assert(datalog: "parent(A, B) :- daughter(B, A)")
		try db.assert(datalog: "daughter(A, B) :- female(A), parent(B, A)")
		try db.assert(datalog: "grandparent(A, B) :- parent(A, C), parent(C, B)")

		func pairs(_ datalog: String) throws -> Set<[String]> {
			Set(
				try db.query(datalog: datalog).map { row in
					[row["A"] as! String, row["B"] as! String]
				})
		}

		// `daughter` closes over `parent`, which closes back over `daughter`. Sophie is female and
		//  Geordie is her parent (asserted fact), so daughter(Sophie, Geordie) must be derived — that
		//  derivation is only reachable *through* the mutual cycle.
		let daughters = try pairs("daughter(A, B)")
		#expect(daughters.contains(["Maeve", "Alex"]), "asserted daughter fact")
		#expect(
			daughters.contains(["Sophie", "Geordie"]),
			"Sophie is female and Geordie is her parent ⟹ daughter(Sophie, Geordie)")

		// A predicate that merely depends on the cycle (`grandparent → parent`) must also resolve.
		// `son('Alex','Joe')` ⟹ `parent('Joe','Alex')` and `son('Henry','Alex')` ⟹
		//  `parent('Alex','Henry')`, so Joe is Henry's grandparent.
		let grandparents = try pairs("grandparent(A, B)")
		#expect(grandparents.contains(["Joe", "Henry"]), "Joe → Alex → Henry")

		// A bound query against a cycle member returns just the matching rows (only `A` is selected,
		//  since `B` is pinned to the constant). Geordie is Sophie's only parent.
		let sophiesParents = Set(
			try db.query(datalog: "parent(A, 'Sophie')").map { $0["A"] as! String })
		#expect(sophiesParents == ["Geordie"], "Geordie is Sophie's only parent: \(sophiesParents)")

		// Raw SQL against the predicate views must resolve too. Unlike the datalog path, `SELECT * FROM
		//  daughter` references the table *unbracketed*, so the closure must expose the predicate under
		//  its own name (rather than rewriting a bracketed `FROM [daughter]`), for both a cycle member
		//  (`daughter`) and a predicate that merely depends on the cycle (`grandparent`).
		#expect(
			try Array(db.query(sql: "SELECT * FROM daughter")).count
				== daughters.count, "raw SQL over a cycle member matches the datalog query")
		#expect(
			try Array(db.query(sql: "SELECT * FROM grandparent")).count
				== grandparents.count, "raw SQL over a cycle-dependent predicate matches too")
	}

	@Test("non-linear recursion evaluates via iterative fixpoint materialization")
	func nonLinearRecursion() async throws {
		let db = try RBDB(path: ":memory:")
		try db.query(sql: "CREATE TABLE edge(a, b)")
		try db.query(sql: "CREATE TABLE path(a, b)")

		try db.assert(datalog: "edge(1, 2)")
		try db.assert(datalog: "edge(2, 3)")
		try db.assert(datalog: "edge(3, 4)")

		// The transitive-closure rule joins two occurrences of the *same* recursive predicate
		//  (`path`, `path`), which is genuinely non-linear — a shape SQLite's linear recursive CTE
		//  cannot express. The finite iterative evaluator handles it because each step is a plain join.
		try db.assert(datalog: "path(X, Y) :- edge(X, Y)")
		try db.assert(datalog: "path(X, Z) :- path(X, Y), path(Y, Z)")

		func pairs(_ datalog: String) throws -> Set<[Int]> {
			Set(
				try db.query(datalog: datalog).map { row in
					[Int(row["A"] as! Int64), Int(row["B"] as! Int64)]
				})
		}

		// Every reachable pair, both direct edges and multi-hop transitive paths.
		let paths = try pairs("path(A, B)")
		#expect(
			paths == [[1, 2], [2, 3], [3, 4], [1, 3], [2, 4], [1, 4]],
			"transitive closure of the 1→2→3→4 chain: \(paths)")

		// A bound query against the non-linear predicate returns just the matching rows.
		let fromOne = Set(try db.query(datalog: "path(1, B)").map { Int($0["B"] as! Int64) })
		#expect(fromOne == [2, 3, 4], "everything reachable from 1: \(fromOne)")
	}

	@Test("a freshly opened DB materializes a base-fact-only predicate referenced within the cone")
	func freshConnectionBaseFactInCone() async throws {
		// Predicate "tables" exist only as per-connection TEMP views, created lazily. When a DB is
		//  opened fresh, none exist yet. The iterative evaluator materializes *derived* cone predicates
		//  as temp tables but references *base-fact-only* ones (here `tagged`) by name — so their view
		//  must be created on demand, else the seeding join fails with "no such table: tagged". This
		//  regression only reproduces across a fresh connection (same-connection tests create the views
		//  eagerly at CREATE TABLE time, masking it).
		let path = NSTemporaryDirectory() + "rbdb-fresh-\(UUID().uuidString).db"
		defer { try? FileManager.default.removeItem(atPath: path) }

		do {
			let setup = try RBDB(path: path)
			try setup.query(sql: "CREATE TABLE edge(a, b)")
			try setup.query(sql: "CREATE TABLE tagged(a)")
			try setup.query(sql: "CREATE TABLE reach(a, b)")

			try setup.assert(datalog: "edge(1, 2)")
			try setup.assert(datalog: "edge(2, 3)")
			try setup.assert(datalog: "tagged(1)")

			// `reach` is recursive (finite ⟹ iterative evaluator); its cone includes the base-fact-only
			//  `tagged`, referenced by a rule body — the predicate whose view must be created on demand.
			try setup.assert(datalog: "reach(X, Y) :- edge(X, Y), tagged(X)")
			try setup.assert(datalog: "reach(X, Z) :- reach(X, Y), edge(Y, Z)")
		}

		// Separate connection: no predicate TEMP views exist. Querying the recursive predicate must
		//  materialize `tagged`'s view rather than fail, and must return the *complete* closure.
		let db = try RBDB(path: path)
		let pairs = Set(
			try db.query(datalog: "reach(A, B)").map { row in
				[Int(row["A"] as! Int64), Int(row["B"] as! Int64)]
			})
		#expect(pairs == [[1, 2], [1, 3]], "closure seeded from tagged node 1: \(pairs)")
	}

	@Test("a mid-build failure leaves no partial materialization to silently return wrong rows")
	func failedMaterializationDoesNotPersist() async throws {
		let db = try RBDB(path: ":memory:")
		try db.query(sql: "CREATE TABLE p(a, b)")

		// A base fact so materialization *seeds* `p`'s temp table with a real row before the rule
		//  that fails runs — the partial state that must not survive.
		try db.assert(datalog: "p(1, 2)")

		// A recursive rule whose head arity (1) doesn't match `p`'s two columns. It passes `validate`
		//  (which only checks unsafe variables), routes `p` to the iterative evaluator (self-referential,
		//  no arithmetic ⟹ finite), and then fails at the fixpoint INSERT — a mid-build error after the
		//  seed already populated the temp table.
		try db.assert(datalog: "p(X) :- p(X, Y)")

		// First query fails as expected (the evaluator runs the fixpoint eagerly, so `query` throws).
		#expect(throws: (any Error).self) {
			_ = try db.query(datalog: "p(A, B)")
		}

		// It must fail *again*, not silently resolve against a leftover, partially seeded `[p]` temp
		//  table (which would return the seeded `(1, 2)` as if it were the complete relation).
		#expect(throws: (any Error).self) {
			_ = try db.query(datalog: "p(A, B)")
		}
	}

	@Test("materialized cone is invalidated when a new fact is asserted or inserted")
	func materializationInvalidation() async throws {
		let db = try RBDB(path: ":memory:")
		try db.query(sql: "CREATE TABLE edge(a, b)")
		try db.query(sql: "CREATE TABLE path(a, b)")

		try db.assert(datalog: "edge(1, 2)")
		try db.assert(datalog: "edge(2, 3)")
		try db.assert(datalog: "path(X, Y) :- edge(X, Y)")
		try db.assert(datalog: "path(X, Z) :- path(X, Y), path(Y, Z)")

		func reachableFromOne() throws -> Set<Int> {
			Set(try db.query(datalog: "path(1, B)").map { Int($0["B"] as! Int64) })
		}

		// First query materializes the cone into temp tables.
		#expect(try reachableFromOne() == [2, 3], "1 reaches 2 and 3")

		// Asserting a new fact must drop the stale materialization so the re-query reflects it.
		try db.assert(datalog: "edge(3, 4)")
		#expect(try reachableFromOne() == [2, 3, 4], "extending the chain must be reflected")

		// A fact inserted through raw SQL (via the base predicate's view → `_rule`) invalidates too.
		try db.query(sql: "INSERT INTO edge (a, b) VALUES (4, 5)")
		#expect(try reachableFromOne() == [2, 3, 4, 5], "raw-SQL fact insert must be reflected")
	}

	/// Sets up `tc` as the finite transitive closure of `edge` and materializes it, returning the DB.
	private func materializedTransitiveClosure() throws -> RBDB {
		let db = try RBDB(path: ":memory:")
		try db.query(sql: "CREATE TABLE edge(a, b)")
		try db.query(sql: "CREATE TABLE tc(a, b)")
		try db.assert(datalog: "edge(1, 2)")
		try db.assert(datalog: "edge(2, 3)")
		try db.assert(datalog: "tc(X, Y) :- edge(X, Y)")
		try db.assert(datalog: "tc(X, Z) :- tc(X, Y), edge(Y, Z)")
		// First query materializes `tc` into a temp table (finite ⟹ iterative evaluator).
		#expect(try Set(db.query(datalog: "tc(1, B)").map { Int($0["B"] as! Int64) }) == [2, 3])
		return db
	}

	@Test(
		"a rule that turns a materialized closure value-generating invalidates it, never loops",
		.timeLimit(.minutes(1)))
	func valueGeneratingFlipInvalidatesInsteadOfLooping() async throws {
		let db = try materializedTransitiveClosure()

		// Asserting a rule with arithmetic under recursion makes `tc` value-generating: its fixpoint no
		//  longer terminates (each pass derives a fresh `Y+1`). The refresh must NOT try to re-iterate
		//  it — it must invalidate instead, or this test hangs until the time limit trips.
		try db.assert(datalog: "tc(X, Y + 1) :- tc(X, Y)")

		// Any query triggers the refresh of the now-dirty `tc`. Query an unrelated predicate so the
		//  refresh — not this statement — is what would loop if the guard were missing.
		#expect(try db.query(datalog: "edge(A, B)").map { _ in 1 }.count == 2)

		// The guard dropped `tc`'s materialization rather than re-iterating it.
		let stillMaterialized = try Array(
			db.query(
				sql: "SELECT name FROM sqlite_temp_master WHERE type = 'table' AND name = 'tc'"))
		#expect(
			stillMaterialized.isEmpty, "the value-generating closure must be invalidated (dropped)")
	}

	@Test(
		"after the value-generating flip, the predicate re-routes to the streaming CTE",
		.timeLimit(.minutes(1)))
	func valueGeneratingFlipReroutesToCTE() async throws {
		let db = try materializedTransitiveClosure()
		try db.assert(datalog: "tc(X, Y + 1) :- tc(X, Y)")

		// Post-flip `tc` is value-generating, so a *ground* query bounds and streams via the CTE.
		//  `tc(1, 5)` is reachable: 1→2→3 (edges) then +1 increments up to 5.
		#expect(Array(try db.query(datalog: "tc(1, 3)")).first?["sat"] as? Int64 == 1)
		#expect(Array(try db.query(datalog: "tc(1, 5)")).first?["sat"] as? Int64 == 1)
		// `4` has no incoming edge and nothing reaches it, so it is not derivable.
		#expect(Array(try db.query(datalog: "tc(4, 5)")).isEmpty, "tc(4, 5) is not derivable")
	}

	@Test("materialized cone is invalidated when a base fact is asserted — file-backed DB")
	func materializationInvalidationOnDiskFact() async throws {
		// Same as `materializationInvalidation`, but on a *file* database, where SQLite enforces table
		//  locks that an in-memory DB does not — exercising the invalidation trigger's `DROP` under
		//  those locks.
		let path = NSTemporaryDirectory() + "rbdb-inval-fact-\(UUID().uuidString).db"
		defer { try? FileManager.default.removeItem(atPath: path) }
		let db = try RBDB(path: path)
		try db.query(sql: "CREATE TABLE edge(a, b)")
		try db.query(sql: "CREATE TABLE path(a, b)")
		try db.assert(datalog: "edge(1, 2)")
		try db.assert(datalog: "edge(2, 3)")
		try db.assert(datalog: "path(X, Y) :- edge(X, Y)")
		try db.assert(datalog: "path(X, Z) :- path(X, Y), path(Y, Z)")

		func reachableFromOne() throws -> Set<Int> {
			Set(try db.query(datalog: "path(1, B)").map { Int($0["B"] as! Int64) })
		}

		// First query materializes the cone into temp tables.
		#expect(try reachableFromOne() == [2, 3], "1 reaches 2 and 3")

		// Asserting a new base fact must invalidate the stale materialization so the re-query sees it.
		try db.assert(datalog: "edge(3, 4)")
		#expect(try reachableFromOne() == [2, 3, 4], "extending the chain must be reflected")
	}

	@Test("materialized table is invalidated when a new rule is asserted — file-backed DB")
	func materializationInvalidationOnDiskRule() async throws {
		// A new *rule* for a predicate that already has a materialized temp table must drop that table
		//  (via the invalidation trigger) so the next query rematerializes with the rule. On a file DB
		//  the table's lock is enforced, so this exercises the trigger's `DROP` against a live lock.
		let path = NSTemporaryDirectory() + "rbdb-inval-rule-\(UUID().uuidString).db"
		defer { try? FileManager.default.removeItem(atPath: path) }
		let db = try RBDB(path: path)
		try db.query(sql: "CREATE TABLE edge(a, b)")
		try db.query(sql: "CREATE TABLE path(a, b)")
		try db.assert(datalog: "edge(1, 2)")
		try db.assert(datalog: "edge(2, 3)")
		try db.assert(datalog: "path(X, Y) :- edge(X, Y)")
		try db.assert(datalog: "path(X, Z) :- path(X, Y), path(Y, Z)")

		func reachableFromOne() throws -> Set<Int> {
			Set(try db.query(datalog: "path(1, B)").map { Int($0["B"] as! Int64) })
		}

		// Materialize `path` into a temp table.
		#expect(try reachableFromOne() == [2, 3], "1 reaches 2 and 3")

		// Assert a new rule for the now-materialized `path`: `path(X, X) :- edge(X, Y)` adds a self-pair
		//  for every node with an outgoing edge (so `path(1, 1)`). The materialized table must be dropped
		//  and rebuilt for this to show up.
		try db.assert(datalog: "path(X, X) :- edge(X, Y)")
		#expect(
			try reachableFromOne() == [1, 2, 3], "the new rule must be reflected (adds path(1, 1))")
	}

	/// Number of stored *rules* (Horn clauses with a body) for a predicate.
	private func ruleCount(_ db: RBDB, _ predicate: String) throws -> Int {
		try db.fetchRules(for: predicate).count
	}

	@Test("a tautological rule (head identical to a body literal) is not stored")
	func tautologyDropped() async throws {
		let db = try RBDB(path: ":memory:")
		try db.query(sql: "CREATE TABLE p(a, b)")
		try db.query(sql: "CREATE TABLE q(a, b)")

		// `p(X, Y) :- p(X, Y), q(X, Y)` only ever re-derives rows `p` already has, so it derives
		//  nothing new — it must be dropped rather than stored.
		try db.assert(datalog: "p(X, Y) :- p(X, Y), q(X, Y)")
		#expect(try ruleCount(db, "p") == 0, "tautology should not be stored")
	}

	@Test("duplicate body literals are collapsed before storing")
	func duplicateBodyLiteralsCollapsed() async throws {
		let db = try RBDB(path: ":memory:")
		try db.query(sql: "CREATE TABLE p(a)")
		try db.query(sql: "CREATE TABLE q(a)")

		try db.assert(datalog: "p(X) :- q(X), q(X)")

		let rules = try db.fetchRules(for: "p")
		#expect(rules.count == 1, "one rule stored")
		guard case .hornClause(_, let body, _) = rules.first else {
			Issue.record("expected a Horn clause")
			return
		}
		#expect(body.count == 1, "the duplicate `q(X)` literal is collapsed: \(body)")
	}

	@Test("a more general rule subsumes a more specific one, order-independently")
	func subsumptionRemovesRedundantRule() async throws {
		func storedRule(_ order: (RBDB) throws -> Void) throws -> Formula {
			let db = try RBDB(path: ":memory:")
			try db.query(sql: "CREATE TABLE anc(a, b)")
			try db.query(sql: "CREATE TABLE parent(a, b)")
			try order(db)
			let rules = try db.fetchRules(for: "anc")
			#expect(rules.count == 1, "exactly the general rule remains")
			return rules[0]
		}

		// `anc(X, Y) :- parent(X, Y)` is strictly more general than
		//  `anc(X, Y) :- parent(X, Y), parent(Y, Z)` (a subset of its body constraints), so whichever
		//  order they're asserted in, only the general rule is left standing — and it's the same rule.
		let generalFirst = try storedRule { db in
			try db.assert(datalog: "anc(X, Y) :- parent(X, Y)")
			try db.assert(datalog: "anc(X, Y) :- parent(X, Y), parent(Y, Z)")
		}
		let specificFirst = try storedRule { db in
			try db.assert(datalog: "anc(X, Y) :- parent(X, Y), parent(Y, Z)")
			try db.assert(datalog: "anc(X, Y) :- parent(X, Y)")
		}
		#expect(
			generalFirst.canonicalize() == specificFirst.canonicalize(),
			"the stored set is order-independent")
		guard case .hornClause(_, let body, _) = generalFirst.canonicalize() else { return }
		#expect(body.count == 1, "the surviving rule is the general one: \(body)")
	}
}
