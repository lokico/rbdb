import Foundation
import Testing

@testable import Datalog
@testable import RBDB

/// Assert-time subsumption under the immutable record: a rule made redundant by a more general one is
/// *superseded by that rule*, not deleted — the one case where `superceded_by` points at another
/// `_rule` row rather than at a bare retraction act, so the two reasons a row left the believed set
/// stay distinguishable by a join.
@Suite("Supersession by subsumption")
struct SupersessionTests {

	private func newDB() throws -> RBDB {
		let db = try RBDB(path: ":memory:")
		try db.query(sql: "CREATE TABLE anc(a, b)")
		try db.query(sql: "CREATE TABLE parent(a, b)")
		return db
	}

	/// Total `_rule` rows recorded for a predicate, live or not.
	private func rowCount(_ db: RBDB, _ predicate: String) throws -> Int {
		Array(
			try db.query(
				sql: SQL(
					"SELECT COUNT(*) AS n FROM _rule WHERE output_type = ?",
					arguments: ["@\(predicate)"]))
		).first.flatMap { $0["n"] as? Int64 }.map(Int.init) ?? 0
	}

	@Test("a subsumed rule is superseded by the subsuming rule, not deleted")
	func subsumedRulePointsAtSubsumer() async throws {
		let db = try newDB()
		try db.assert(datalog: "anc(X, Y) :- parent(X, Y), parent(Y, Z)")
		try db.assert(datalog: "anc(X, Y) :- parent(X, Y)")

		#expect(try db.fetchRules(for: "anc").count == 1, "exactly the general rule is live")
		#expect(try rowCount(db, "anc") == 2, "the subsumed rule is still on the record")

		// The supersession target is itself a `_rule` row — "made redundant by" — and specifically the
		//  surviving general rule.
		let rows = Array(
			try db.query(
				sql: """
					SELECT (SELECT COUNT(*) FROM _rule live
					        WHERE live.internal_entity_id = gone.superceded_by
					          AND live.superceded_by IS NULL) AS by_live_rule
					FROM _rule gone
					WHERE gone.output_type = '@anc' AND gone.superceded_by IS NOT NULL
					"""))
		#expect(rows.count == 1)
		#expect(
			rows[0]["by_live_rule"] as? Int64 == 1,
			"a subsumed rule points at the live rule that subsumed it, not a bare entity")
	}

	@Test("the general rule arriving first leaves the specific one unstored, superseding nothing")
	func generalFirstStoresNothingNew() async throws {
		let db = try newDB()
		try db.assert(datalog: "anc(X, Y) :- parent(X, Y)")
		try db.assert(datalog: "anc(X, Y) :- parent(X, Y), parent(Y, Z)")

		#expect(try db.fetchRules(for: "anc").count == 1, "exactly the general rule is live")
		#expect(
			try rowCount(db, "anc") == 1,
			"an already-subsumed incoming rule is dropped, so there is nothing to record")
	}

	@Test("asserting a duplicate rule supersedes nothing")
	func duplicateSupersedesNothing() async throws {
		let db = try newDB()
		try db.assert(datalog: "anc(X, Y) :- parent(X, Y)")
		try db.assert(datalog: "anc(X, Y) :- parent(X, Y)")

		#expect(try db.fetchRules(for: "anc").count == 1)
		#expect(
			try rowCount(db, "anc") == 1,
			"a no-op insert has no new entity to point at, and supersedes nothing")
	}

	@Test("a retracted rule does not resurrect the rules it superseded")
	func noResurrection() async throws {
		let db = try newDB()
		try db.assert(datalog: "anc(X, Y) :- parent(X, Y), parent(Y, Z)")
		try db.assert(datalog: "anc(X, Y) :- parent(X, Y)")
		try db.retract(datalog: "anc(X, Y) :- parent(X, Y)")

		#expect(
			try db.fetchRules(for: "anc").isEmpty,
			"belief revision here is foundational: the subsumed rule stays superseded")
	}
}
