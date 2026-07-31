import Foundation
import Testing

@testable import Datalog
@testable import RBDB

@Suite("Retraction")
struct RetractionTests {

	/// The `_rule` rows for a predicate, live ones first, as `(superseded: Bool)` flags.
	private func ruleRows(_ db: RBDB, _ predicate: String) throws -> [Bool] {
		try db.query(
			sql: SQL(
				"SELECT superceded_by IS NOT NULL AS gone FROM _rule WHERE output_type = ? ORDER BY internal_entity_id",
				arguments: ["@\(predicate)"])
		).map { ($0["gone"] as! Int64) != 0 }
	}

	@Test("retracting a fact removes it from queries but leaves the row, superseded")
	func retractFact() async throws {
		let db = try RBDB(path: ":memory:")
		try db.query(sql: "CREATE TABLE user(name)")
		try db.assert(datalog: "user('alice')")
		try db.assert(datalog: "user('bob')")

		try db.retract(datalog: "user('alice')")

		let names = Set(try db.query(sql: "SELECT name FROM user").map { $0["name"] as! String })
		#expect(names == ["bob"], "the retracted fact is no longer derivable")

		// The record is immutable: the row is still there, pointing at the retraction act.
		let rows = Array(
			try db.query(
				sql: """
					SELECT superceded_by FROM _rule
					WHERE output_type = '@user' AND arg1_constant = 'alice'
					"""))
		#expect(rows.count == 1, "the retracted row is not deleted")
		#expect(rows[0]["superceded_by"] is Int64, "superceded_by names the retraction act")
	}

	@Test("the retraction act is a bare entity, not another rule")
	func retractionActIsBareEntity() async throws {
		let db = try RBDB(path: ":memory:")
		try db.query(sql: "CREATE TABLE user(name)")
		try db.assert(datalog: "user('alice')")
		try db.retract(datalog: "user('alice')")

		let rows = Array(
			try db.query(
				sql: """
					SELECT e.internal_entity_id AS id,
					       (SELECT COUNT(*) FROM _rule r2 WHERE r2.internal_entity_id = e.internal_entity_id) AS is_rule
					FROM _rule r JOIN _entity e ON e.internal_entity_id = r.superceded_by
					WHERE r.output_type = '@user'
					"""))
		#expect(rows.count == 1)
		#expect(
			rows[0]["is_rule"] as? Int64 == 0, "an explicit retraction points at a bare _entity")
	}

	@Test("retracting a rule stops its conclusions being derivable")
	func retractRule() async throws {
		let db = try RBDB(path: ":memory:")
		try db.query(sql: "CREATE TABLE human(name)")
		try db.query(sql: "CREATE TABLE mortal(name)")
		try db.assert(datalog: "human('socrates')")
		try db.assert(datalog: "mortal(X) :- human(X)")

		#expect(try db.query(datalog: "mortal(N)").map { _ in 1 }.count == 1)

		try db.retract(datalog: "mortal(X) :- human(X)")

		#expect(
			try db.query(datalog: "mortal(N)").map { _ in 1 }.isEmpty,
			"the view must be rebuilt without the retracted rule")
		#expect(try db.fetchRules(for: "mortal").isEmpty, "no live rule remains")
	}

	@Test("retract then re-assert leaves two rows — one superseded, one live — and re-derives")
	func retractThenReassert() async throws {
		let db = try RBDB(path: ":memory:")
		try db.query(sql: "CREATE TABLE user(name)")
		try db.assert(datalog: "user('alice')")
		try db.retract(datalog: "user('alice')")
		try db.assert(datalog: "user('alice')")

		#expect(
			try ruleRows(db, "user") == [true, false],
			"the old row stays superseded; the re-assertion is a new live row")

		let names = try db.query(sql: "SELECT name FROM user").map { $0["name"] as! String }
		#expect(names == ["alice"], "re-assertion makes the fact derivable again, exactly once")
	}

	@Test("retracting something not stored throws .notFound")
	func retractNotFound() async throws {
		let db = try RBDB(path: ":memory:")
		try db.query(sql: "CREATE TABLE user(name)")
		try db.assert(datalog: "user('alice')")

		#expect(throws: RetractionError.self) {
			try db.retract(datalog: "user('nobody')")
		}
	}

	@Test("retraction operates on the base, not the closure: derivable-but-unstored is .notFound")
	func retractDerivedIsNotFound() async throws {
		let db = try RBDB(path: ":memory:")
		try db.query(sql: "CREATE TABLE human(name)")
		try db.query(sql: "CREATE TABLE mortal(name)")
		try db.assert(datalog: "human('socrates')")
		try db.assert(datalog: "mortal(X) :- human(X)")

		// `mortal('socrates')` is derivable but not stored — retract what it follows from instead.
		#expect(throws: RetractionError.self) {
			try db.retract(datalog: "mortal('socrates')")
		}
		#expect(try db.query(datalog: "mortal(N)").map { _ in 1 }.count == 1, "still derivable")
	}

	@Test("retraction matches through the canonical form")
	func retractCanonicalForm() async throws {
		let db = try RBDB(path: ":memory:")
		try db.query(sql: "CREATE TABLE p(a, b)")
		try db.query(sql: "CREATE TABLE q(a, b)")
		try db.assert(datalog: "q(X, Y) :- p(X, Y), X < Y")

		// `Y > X` is the same guard commuted; canonicalization folds it to `X < Y`.
		try db.retract(datalog: "q(X, Y) :- p(X, Y), Y > X")

		#expect(
			try db.fetchRules(for: "q").isEmpty, "the commuted spelling matched the stored rule")
	}

	/// `path` as the transitive closure of `edge`, materialized by a first query.
	private func materializedPath() throws -> RBDB {
		let db = try RBDB(path: ":memory:")
		try db.query(sql: "CREATE TABLE edge(a, b)")
		try db.query(sql: "CREATE TABLE path(a, b)")
		try db.assert(datalog: "edge(1, 2)")
		try db.assert(datalog: "edge(2, 3)")
		try db.assert(datalog: "path(X, Y) :- edge(X, Y)")
		try db.assert(datalog: "path(X, Z) :- path(X, Y), path(Y, Z)")
		#expect(try reachableFromOne(db) == [2, 3])
		return db
	}

	private func reachableFromOne(_ db: RBDB) throws -> Set<Int> {
		Set(try db.query(datalog: "path(1, B)").map { Int($0["B"] as! Int64) })
	}

	@Test("retracting a fact under a materialized closure drops and rebuilds it")
	func retractUnderMaterialization() async throws {
		let db = try materializedPath()

		// Re-iterating the existing closure could never *remove* `path(1, 3)`: this is the
		//  monotonicity assumption breaking, and it must force a drop-and-rebuild.
		try db.retract(datalog: "edge(2, 3)")

		#expect(try reachableFromOne(db) == [2], "rows no longer derivable must disappear")
	}

	@Test("retracting a rule under a materialized closure drops and rebuilds it")
	func retractRuleUnderMaterialization() async throws {
		let db = try materializedPath()

		// The closure is a temp *table*, so there is no view for the trigger's `DROP VIEW` arm to find —
		//  the `_dirty` rebuild flag is what has to carry this.
		try db.retract(datalog: "path(X, Z) :- path(X, Y), path(Y, Z)")

		#expect(try db.fetchRules(for: "path").count == 1, "only the base rule is live")
		#expect(try reachableFromOne(db) == [2], "the transitive step is gone")
	}

	@Test("a pending rebuild is sticky: a later assert cannot downgrade it to a refresh")
	func rebuildIsSticky() async throws {
		let db = try materializedPath()

		// Both land in the same refresh window (neither `retract` nor `assert` runs a refresh), so the
		//  additive invalidation arrives *after* the destructive one and must not overwrite it.
		try db.retract(datalog: "edge(2, 3)")
		try db.assert(datalog: "edge(1, 5)")

		#expect(try reachableFromOne(db) == [2, 5], "still a full rebuild, not a re-iteration")
	}

	@Test("retracting invalidates every table of a closure, not just the top predicate's")
	func retractInvalidatesWholeCone() async throws {
		let db = try RBDB(path: ":memory:")
		try db.query(sql: "CREATE TABLE edge(a, b)")
		try db.query(sql: "CREATE TABLE path(a, b)")
		try db.query(sql: "CREATE TABLE reach(b)")
		try db.assert(datalog: "edge(1, 2)")
		try db.assert(datalog: "edge(2, 3)")
		try db.assert(datalog: "path(X, Y) :- edge(X, Y)")
		try db.assert(datalog: "path(X, Z) :- path(X, Y), edge(Y, Z)")
		try db.assert(datalog: "reach(B) :- path(1, B)")

		// Materializing `reach` builds a temp table for *every* derived member of its cone — `reach`
		//  and `path` both — but registers only `reach` as a materialized top. `path` is therefore
		//  reachable by name while being invalidated by nothing of its own.
		#expect(try Set(db.query(datalog: "reach(B)").map { Int($0["B"] as! Int64) }) == [2, 3])

		try db.retract(datalog: "edge(2, 3)")

		#expect(
			try reachableFromOne(db) == [2],
			"`path`'s closure was built as part of `reach`'s and must be dropped with it")
	}

	@Test("retracting the fact behind a derived negative literal clears the contradiction")
	func retractClearsContradiction() async throws {
		let db = try RBDB(path: ":memory:")
		try db.query(sql: "CREATE TABLE male(x)")
		try db.query(sql: "CREATE TABLE female(x)")
		// Mutually recursive across both polarities: every one of `male`, `-male`, `female`, `-female`
		//  is materialized, and each build creates the temp tables of its whole cone.
		try db.assert(datalog: "male(X) :- -female(X)")
		try db.assert(datalog: "female(X) :- -male(X)")
		try db.assert(datalog: "-male(X) :- female(X)")
		try db.assert(datalog: "-female(X) :- male(X)")

		try db.assert(datalog: "male('foo')")
		#expect(throws: CoherenceError.self) {
			// `-female('foo')` follows from `male('foo')`, so this is a contradiction.
			try db.assert(datalog: "female('foo')")
		}

		try db.retract(datalog: "male('foo')")

		// Nothing derives `-female('foo')` any more, so the same assert is now coherent.
		try db.assert(datalog: "female('foo')")
		#expect(try db.query(datalog: "female(X)").map { $0["X"] as! String } == ["foo"])
	}

	@Test("retracting a fact under a materialized closure — file-backed DB")
	func retractUnderMaterializationOnDisk() async throws {
		// A file DB enforces the table locks an in-memory one does not, so this exercises the
		//  drop-at-a-safe-point contract of `_invalidate_on_rule_supersede`.
		let path = NSTemporaryDirectory() + "rbdb-retract-\(UUID().uuidString).db"
		defer { try? FileManager.default.removeItem(atPath: path) }
		let db = try RBDB(path: path)
		try db.query(sql: "CREATE TABLE edge(a, b)")
		try db.query(sql: "CREATE TABLE path(a, b)")
		try db.assert(datalog: "edge(1, 2)")
		try db.assert(datalog: "edge(2, 3)")
		try db.assert(datalog: "path(X, Y) :- edge(X, Y)")
		try db.assert(datalog: "path(X, Z) :- path(X, Y), path(Y, Z)")
		#expect(try reachableFromOne(db) == [2, 3])

		try db.retract(datalog: "edge(2, 3)")
		#expect(try reachableFromOne(db) == [2])
	}
}
