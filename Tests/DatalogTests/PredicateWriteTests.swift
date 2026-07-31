import Foundation
import Testing

@testable import Datalog
@testable import RBDB

/// A write to a predicate must mean the same thing regardless of how that predicate currently happens
/// to be backed — a plain `TEMP VIEW` or a materialized `TEMP TABLE` — and the same thing the Swift API
/// means: `INSERT` asserts a base fact, `DELETE` retracts one, `UPDATE` is refused.
@Suite("Predicate write surface")
struct PredicateWriteTests {

	/// How a predicate happens to be backed right now. Invisible to the caller, and that is the point.
	enum Backing: String, CaseIterable, CustomStringConvertible {
		case view, materialized
		var description: String { rawValue }
	}

	/// A database where `p(a, b)` holds the base facts `p(1, 2)` and `p(3, 4)` and is backed as
	/// requested. The materialized variant gets a recursive rule set that derives nothing extra from
	/// those facts, so the two are observationally identical before any write.
	private func db(backedBy backing: Backing) throws -> RBDB {
		let db = try RBDB(path: ":memory:")
		try db.query(sql: "CREATE TABLE e(a, b)")
		try db.query(sql: "CREATE TABLE p(a, b)")
		if backing == .materialized {
			try db.assert(datalog: "p(X, Y) :- e(X, Y)")
			try db.assert(datalog: "p(X, Z) :- p(X, Y), p(Y, Z)")
		}
		try db.assert(datalog: "p(1, 2)")
		try db.assert(datalog: "p(3, 4)")

		// Force the backing into existence, and confirm we actually got the one under test.
		#expect(try rows(db) == [[1, 2], [3, 4]])
		let isTable =
			Array(
				try db.query(
					sql: "SELECT type FROM temp.sqlite_schema WHERE name = 'p'")
			).first?["type"] as? String
		#expect(isTable == (backing == .materialized ? "table" : "view"))
		return db
	}

	private func rows(_ db: RBDB) throws -> [[Int]] {
		try db.query(sql: "SELECT a, b FROM p ORDER BY a, b")
			.map { [Int($0["a"] as! Int64), Int($0["b"] as! Int64)] }
	}

	/// Live base facts of `p` recorded in `_rule`, as argument pairs.
	private func liveFacts(_ db: RBDB) throws -> [[Int]] {
		try db.query(
			sql: """
				SELECT arg1_constant AS a, arg2_constant AS b FROM _rule
				WHERE superceded_by IS NULL AND output_type = '@p' AND negative_literal_count = 0
				ORDER BY a, b
				"""
		).map { [Int($0["a"] as! Int64), Int($0["b"] as! Int64)] }
	}

	// MARK: - The alignment table: same statement, same outcome, either backing

	@Test("INSERT asserts a base fact", arguments: Backing.allCases)
	func insertAsserts(_ backing: Backing) async throws {
		let db = try db(backedBy: backing)

		try db.query(sql: "INSERT INTO p(a, b) VALUES (5, 6)")

		#expect(try liveFacts(db) == [[1, 2], [3, 4], [5, 6]], "the fact reaches `_rule`")
		#expect(try rows(db) == [[1, 2], [3, 4], [5, 6]], "and is visible afterwards")
	}

	@Test("DELETE retracts the matching base fact", arguments: Backing.allCases)
	func deleteRetracts(_ backing: Backing) async throws {
		let db = try db(backedBy: backing)

		try db.query(sql: "DELETE FROM p WHERE a = 1")

		#expect(try liveFacts(db) == [[3, 4]], "the matching row is superseded, not deleted")
		#expect(try rows(db) == [[3, 4]])

		let recorded = Array(
			try db.query(
				sql: """
					SELECT COUNT(*) AS n FROM _rule
					WHERE output_type = '@p' AND arg1_constant = 1 AND superceded_by IS NOT NULL
					"""))
		#expect(recorded.first?["n"] as? Int64 == 1, "the retracted row stays on the record")
	}

	@Test("UPDATE is refused", arguments: Backing.allCases)
	func updateRefused(_ backing: Backing) async throws {
		let db = try db(backedBy: backing)

		#expect(throws: SQLiteError.self) {
			try db.query(sql: "UPDATE p SET b = 9 WHERE a = 1")
		}
		#expect(try rows(db) == [[1, 2], [3, 4]], "nothing changed")
	}

	// MARK: - INSERT: the divert-then-rebuild round trip

	@Test("an inserted fact is picked up by the closure it feeds")
	func insertFeedsClosure() async throws {
		let db = try RBDB(path: ":memory:")
		try db.query(sql: "CREATE TABLE edge(a, b)")
		try db.query(sql: "CREATE TABLE path(a, b)")
		try db.assert(datalog: "path(X, Y) :- edge(X, Y)")
		try db.assert(datalog: "path(X, Z) :- path(X, Y), path(Y, Z)")
		try db.assert(datalog: "edge(1, 2)")

		func reachable() throws -> Set<Int> {
			Set(try db.query(datalog: "path(1, B)").map { Int($0["B"] as! Int64) })
		}
		#expect(try reachable() == [2])

		// Inserted into `path` itself — the materialized table — so this is the diverted path, and what
		//  comes back must include what is newly *derivable* from the new base fact, not just the row.
		try db.query(sql: "INSERT INTO path(a, b) VALUES (2, 3)")
		#expect(try reachable() == [2, 3], "the diverted insert re-seeds and re-runs the fixpoint")
	}

	// MARK: - DELETE: retraction operates on the base, not the closure

	/// `p` with one derived row (`p(1, 2)` via `e`) and one asserted row (`p(3, 4)`).
	private func mixedDB() throws -> RBDB {
		let db = try RBDB(path: ":memory:")
		try db.query(sql: "CREATE TABLE e(a, b)")
		try db.query(sql: "CREATE TABLE p(a, b)")
		try db.assert(datalog: "p(X, Y) :- e(X, Y)")
		try db.assert(datalog: "e(1, 2)")
		try db.assert(datalog: "p(3, 4)")
		#expect(try rows(db) == [[1, 2], [3, 4]])
		return db
	}

	@Test("deleting a purely derived row raises, naming the row")
	func deleteDerivedRaises() async throws {
		let db = try mixedDB()

		var message = ""
		#expect(throws: SQLiteError.self) {
			do {
				try db.query(sql: "DELETE FROM p WHERE a = 1")
			} catch let error as SQLiteError {
				if case .queryError(let msg, _) = error { message = msg }
				throw error
			}
		}
		#expect(message.contains("p(1, 2)"), "the message names the row: \(message)")
		#expect(message.contains("derived"), "…and says why: \(message)")
		#expect(try rows(db) == [[1, 2], [3, 4]], "nothing was retracted")
	}

	@Test("a row that is both asserted and derived stays visible after a successful DELETE")
	func deleteAssertedAndDerived() async throws {
		let db = try mixedDB()
		try db.assert(datalog: "p(1, 2)")  // now *also* a base fact

		try db.query(sql: "DELETE FROM p WHERE a = 1")

		// This is §2.4 showing through the SQL surface, not a wart of `DELETE`: the assertion was
		//  retracted, but the conclusion still follows from `e(1, 2)` and the rule.
		#expect(try liveFacts(db) == [[3, 4]], "the assertion is gone from the base")
		#expect(try rows(db) == [[1, 2], [3, 4]], "the conclusion still follows")
	}

	@Test("a multi-row DELETE that touches a derived row retracts nothing at all")
	func deleteMultiRowIsAllOrNothing() async throws {
		let db = try mixedDB()

		#expect(throws: SQLiteError.self) {
			try db.query(sql: "DELETE FROM p")
		}
		#expect(
			try liveFacts(db) == [[3, 4]],
			"the ABORT rolls back the supersession already applied to p(3, 4)")
		#expect(try rows(db) == [[1, 2], [3, 4]])
	}

	@Test("DELETE and retract(datalog:) leave identical `_rule` state")
	func deleteMatchesRetract() async throws {
		/// The whole `_rule` table, plus whether each supersession target is a bare entity.
		func state(_ db: RBDB) throws -> [String] {
			try db.query(
				sql: """
					SELECT r.internal_entity_id AS id, hex(r.formula) AS f, r.superceded_by AS by,
					       (SELECT COUNT(*) FROM _rule t WHERE t.internal_entity_id = r.superceded_by) AS target_is_rule
					FROM _rule r ORDER BY r.internal_entity_id
					"""
			).map {
				"\($0["id"] as! Int64)|\($0["f"] as! String)|\(String(describing: $0["by"]))"
					+ "|\($0["target_is_rule"] as! Int64)"
			}
		}

		let viaSQL = try db(backedBy: .view)
		try viaSQL.query(sql: "DELETE FROM p WHERE a = 1")

		let viaSwift = try db(backedBy: .view)
		try viaSwift.retract(datalog: "p(1, 2)")

		let sqlState = try state(viaSQL)
		let swiftState = try state(viaSwift)
		#expect(sqlState == swiftState)
	}

	// MARK: - The `_materializing` guard

	@Test("a fixpoint build writes nothing to `_rule`")
	func fixpointDoesNotAssert() async throws {
		let db = try RBDB(path: ":memory:")
		try db.query(sql: "CREATE TABLE edge(a, b)")
		try db.query(sql: "CREATE TABLE path(a, b)")
		try db.assert(datalog: "path(X, Y) :- edge(X, Y)")
		try db.assert(datalog: "path(X, Z) :- path(X, Y), path(Y, Z)")
		try db.assert(datalog: "edge(1, 2)")
		try db.assert(datalog: "edge(2, 3)")

		func ruleRowCount() throws -> Int64 {
			Array(try db.query(sql: "SELECT COUNT(*) AS n FROM _rule")).first?["n"] as! Int64
		}
		let before = try ruleRowCount()

		// The fixpoint's own `INSERT OR IGNORE INTO [path]` must not be diverted into `_rule`: that
		//  would turn derived rows into asserted base facts, and the loop would never settle.
		#expect(try db.query(datalog: "path(1, B)").map { _ in 1 }.count == 2)

		#expect(try ruleRowCount() == before, "derived rows are not asserted")
	}

	@Test("the guard is a counter, not a boolean: a nested build can't clear an outer one")
	func guardNests() async throws {
		let db = try db(backedBy: .materialized)

		db.materializingDepth += 1  // stand in for an outer build in progress
		defer { db.materializingDepth = 0 }
		try db.materialize(topPredicate: "p")

		#expect(
			db.materializingDepth == 1, "the inner build must not clear the outer build's guard")
	}

	@Test("the guard is cleared when a build throws")
	func guardClearedOnThrow() async throws {
		let db = try RBDB(path: ":memory:")
		for name in ["ea", "pa", "eb", "pb"] {
			try db.query(sql: SQL("CREATE TABLE \(SQL(name))(a, b)"))
		}
		for prefix in ["a", "b"] {
			try db.assert(datalog: "p\(prefix)(X, Y) :- e\(prefix)(X, Y)")
			try db.assert(datalog: "p\(prefix)(X, Z) :- p\(prefix)(X, Y), p\(prefix)(Y, Z)")
			try db.assert(datalog: "e\(prefix)(1, 2)")
		}

		// Sabotage `pa`'s cone behind the engine's back so building it throws partway.
		try db.query(sql: "DELETE FROM _predicate WHERE name = 'ea'")
		#expect(throws: (any Error).self) {
			try db.query(datalog: "pa(A, B)")
		}
		#expect(db.materializingDepth == 0, "a failed build must not leave the guard set")

		// A leaked guard would silently drop this insert into the temp table instead of asserting it.
		#expect(try db.query(datalog: "pb(A, B)").map { _ in 1 }.count == 1)
		try db.query(sql: "INSERT INTO pb(a, b) VALUES (7, 8)")
		let asserted = Array(
			try db.query(
				sql: """
					SELECT COUNT(*) AS n FROM _rule
					WHERE superceded_by IS NULL AND output_type = '@pb' AND arg1_constant = 7
					"""))
		#expect(asserted.first?["n"] as? Int64 == 1, "the insert still reaches `_rule`")
	}
}
