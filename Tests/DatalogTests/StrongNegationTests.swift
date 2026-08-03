import Foundation
import SQLite3
import Testing

@testable import Datalog
@testable import RBDB

/// Aborts any statement on a connection that runs longer than a fixed budget of VM instructions.
///
/// This is how a *non-terminating* query is pinned by a test without stalling the suite: the runaway
/// below otherwise churns for five minutes before dying of its own accord, which is far too slow to
/// live in a test run. The budget is generous enough that no bounded statement in these tests comes
/// near it, so tripping it means the work really was unbounded.
private final class StepBudget {
	private(set) var exhausted = false

	func install(on db: RBDB, opcodes: Int32) {
		sqlite3_progress_handler(
			db.db, opcodes,
			{ context in
				guard let context else { return 0 }
				let box = Unmanaged<StepBudget>.fromOpaque(context).takeUnretainedValue()
				box.exhausted = true
				return 1  // non-zero interrupts the running statement
			}, Unmanaged.passUnretained(self).toOpaque())
	}

	func uninstall(on db: RBDB) {
		sqlite3_progress_handler(db.db, 0, nil, nil)
	}
}

/// Strong negation: `-p(…)` means "p of these arguments is known **false**", as distinct from
/// retraction's "no longer known true". Under the open world the two are observably different — a
/// retracted `p(1)` answers *no* to both `p(1)` and `-p(1)`, while an asserted `-p(1)` answers *yes*
/// to the latter.
@Suite("Strong negation")
struct StrongNegationTests {

	// MARK: - §4.1 Encoding

	@Test("a negative head encodes as `@-name`")
	func encoding() async throws {
		let formula = try DatalogParser().parse(formula: "-p(1)")
		#expect(formula.type.stringValue == "@-p")

		let json = try formulaToJSON(formula)
		#expect(json == #"["@-p",[{"":1}]]"#, "got \(json)")

		let decoded = try JSONDecoder().decode(Formula.self, from: json.data(using: .utf8)!)
		#expect(decoded == formula, "the encoding round-trips")
	}

	@Test("a positive head is unaffected")
	func positiveEncoding() async throws {
		let formula = try DatalogParser().parse(formula: "p(1)")
		#expect(formula.type.stringValue == "@p")
	}

	// MARK: - §4.3 Datalog surface

	@Test(
		"the leading `-` round-trips in assertion, rule-head and goal position",
		arguments: [
			"-p(1.0)",
			"-p(X) :- q(X), r(X)",
			"q(X) :- -p(X)",
		])
	func parserRoundTrip(_ source: String) async throws {
		let parser = DatalogParser()
		let formula = try parser.parse(formula: source)
		#expect(String(try parser.print(formula: formula)) == source)
	}

	@Test("a negative number literal still parses as a number")
	func negativeNumbersStillParse() async throws {
		let formula = try DatalogParser().parse(formula: "p(-5)")
		guard case .hornClause(let head, _, _) = formula else {
			Issue.record("expected a Horn clause")
			return
		}
		#expect(head.arguments == [.number(-5)])
		#expect(head.name == "p", "`-5` is an argument, not a polarity marker")
	}

	// MARK: - `p` and `-p` are separate relations

	@Test("`p` and `-p` are storable together and each queryable in its own relation")
	func separateRelations() async throws {
		let db = try RBDB(path: ":memory:")
		try db.query(sql: "CREATE TABLE p(a)")
		try db.query(sql: "CREATE TABLE q(a)")
		try db.assert(datalog: "p(1)")
		try db.assert(datalog: "-q(1)")

		#expect(try db.query(datalog: "p(A)").map { Int($0["A"] as! Int64) } == [1])
		#expect(try db.query(datalog: "-q(A)").map { Int($0["A"] as! Int64) } == [1])
		#expect(
			try db.query(datalog: "-p(A)").map { _ in 1 }.isEmpty, "nothing is known false of p")
		#expect(try db.query(datalog: "q(A)").map { _ in 1 }.isEmpty, "nothing is known true of q")
	}

	@Test("`-p` inherits `p`'s declared columns and never leaks into `p`")
	func inheritsColumns() async throws {
		let db = try RBDB(path: ":memory:")
		try db.query(sql: "CREATE TABLE p(first, second)")
		try db.assert(datalog: "-p(1, 2)")

		let rows = Array(try db.query(sql: "SELECT first, second FROM [-p]"))
		#expect(rows.count == 1, "`-p` resolves to `p`'s declared columns")
		#expect(rows[0]["first"] as? Int64 == 1)

		#expect(Array(try db.query(sql: "SELECT * FROM p")).isEmpty, "not in `p`'s view")
		#expect(try db.fetchRules(for: "p").isEmpty, "and not among `p`'s rules")
	}

	@Test("a negative predicate cannot be declared")
	func cannotDeclareNegative() async throws {
		let db = try RBDB(path: ":memory:")
		try db.query(sql: "CREATE TABLE p(a)")

		var message = ""
		#expect(throws: (any Error).self) {
			do {
				try db.query(sql: "CREATE TABLE [-p](x, y)")
			} catch let error as SQLiteError {
				if case .queryError(let msg, _) = error { message = msg }
				throw error
			}
		}
		#expect(message.contains("'p'"), "the message points at the positive form: \(message)")

		// Nothing may be left half-declared: `-p` must still take `p`'s columns.
		let declared = Array(
			try db.query(sql: "SELECT COUNT(*) AS n FROM _predicate WHERE name = '-p'"))
		#expect(declared.first?["n"] as? Int64 == 0, "no `_predicate` row was written")
		try db.assert(datalog: "-p(1)")
		let rows = Array(try db.query(sql: "SELECT * FROM [-p]"))
		#expect(rows.count == 1)
		#expect(rows[0]["a"] as? Int64 == 1, "still `p`'s single column `a`, not `x`/`y`")
	}

	// MARK: - §4.2 Coherence

	@Test("asserting `-p` over a live `p` throws and stores nothing")
	func contradictionOnAssert() async throws {
		let db = try RBDB(path: ":memory:")
		try db.query(sql: "CREATE TABLE p(a)")
		try db.assert(datalog: "p(1)")

		#expect(throws: CoherenceError.self) {
			try db.assert(datalog: "-p(1)")
		}
		// No silent repair: revision is two recorded acts, so `p(1)` must still be live.
		#expect(try db.query(datalog: "p(A)").map { Int($0["A"] as! Int64) } == [1])
		#expect(try db.query(datalog: "-p(A)").map { _ in 1 }.isEmpty, "nothing was stored")
	}

	@Test("asserting `p` over a live `-p` throws too")
	func contradictionSymmetric() async throws {
		let db = try RBDB(path: ":memory:")
		try db.query(sql: "CREATE TABLE p(a)")
		try db.assert(datalog: "-p(1)")

		#expect(throws: CoherenceError.self) {
			try db.assert(datalog: "p(1)")
		}
		#expect(try db.query(datalog: "-p(A)").map { Int($0["A"] as! Int64) } == [1])
	}

	/// Whether `p(1)` and `-p(1)` are *both* derivable — the state the coherence check exists to prevent.
	private func believesBoth(_ db: RBDB) throws -> Bool {
		try !db.query(datalog: "p(A)").map { _ in 1 }.isEmpty
			&& !db.query(datalog: "-p(A)").map { _ in 1 }.isEmpty
	}

	@Test(
		"a rule deriving a relation from its own inverse is refused, in either arrival order",
		arguments: [true, false])
	func contradictionThroughSelfInverseRule(_ ruleFirst: Bool) async throws {
		let db = try RBDB(path: ":memory:")
		try db.query(sql: "CREATE TABLE p(a)")

		// `p(X) :- -p(X)` turns every negative fact into its own positive counterpart. Neither half is
		//  a contradiction on its own, so whichever arrives second is the one that has to be refused.
		let steps = ruleFirst ? ["p(X) :- -p(X)", "-p(1)"] : ["-p(1)", "p(X) :- -p(X)"]
		try db.assert(datalog: steps[0])
		#expect(throws: CoherenceError.self) {
			try db.assert(datalog: steps[1])
		}
		#expect(try !believesBoth(db), "p(1) and -p(1) must not both be derivable")
	}

	@Test("a rule that newly makes the inverse of a stored fact derivable is refused")
	func contradictionFromNewRule() async throws {
		let db = try RBDB(path: ":memory:")
		try db.query(sql: "CREATE TABLE p(a)")
		try db.query(sql: "CREATE TABLE q(a)")
		try db.assert(datalog: "-p(1)")
		try db.assert(datalog: "q(1)")

		// Nothing here mentions both polarities in one formula: the contradiction only exists once the
		//  rule joins a stored `q(1)` to the already-stored `-p(1)`.
		#expect(throws: CoherenceError.self) {
			try db.assert(datalog: "p(X) :- q(X)")
		}
		#expect(try !believesBoth(db), "p(1) and -p(1) must not both be derivable")
		#expect(try db.fetchRules(for: "p").isEmpty, "the refused rule was rolled back")

		// The rollback must leave the connection usable — the refused assert did DDL of its own
		//  (building `[-p]` to run the check) inside the transaction it then rolled back.
		try db.assert(datalog: "q(2)")
		#expect(try db.query(datalog: "q(A)").map { _ in 1 }.count == 2)
	}

	/// The reason the check can't be keyed to the relation the asserted formula heads: a contradiction
	/// can be completed by an assert that mentions neither polarity of the relation it breaks.
	@Test("a contradiction reached only through a third relation is refused")
	func contradictionIndirect() async throws {
		let db = try RBDB(path: ":memory:")
		try db.query(sql: "CREATE TABLE p(a)")
		try db.query(sql: "CREATE TABLE r(a)")
		try db.assert(datalog: "-p(1)")

		// Legitimately accepted: `r` is still empty, so the rule derives no `p` yet and nothing collides.
		try db.assert(datalog: "p(X) :- r(X)")

		// This is the one that completes the contradiction, and it is invisible to a check keyed to the
		//  asserted formula's own relation: the head here is `r`, and nothing negative about `r` exists,
		//  so the short-circuit skips before `p` and `-p` are ever compared.
		#expect(throws: CoherenceError.self) {
			try db.assert(datalog: "r(1)")
		}
		#expect(try !believesBoth(db), "p(1) and -p(1) must not both be derivable")
	}

	/// The same hole as above, one link further out. Widening the check to *directly* dependent
	/// relations is not enough: what has to be reachable from the asserted head is the transitive
	/// dependents. Here `r` feeds `s` feeds `p`, and only `p` has a negative side — so a check that
	/// looked one level up from `r` would find `s`, see nothing negative about it, and stop short.
	@Test("a contradiction reached through a chain of relations is refused")
	func contradictionIndirectTransitive() async throws {
		let db = try RBDB(path: ":memory:")
		try db.query(sql: "CREATE TABLE p(a)")
		try db.query(sql: "CREATE TABLE s(a)")
		try db.query(sql: "CREATE TABLE r(a)")
		try db.assert(datalog: "-p(1)")

		// Both accepted on arrival: `r` is empty, so neither rule derives anything yet.
		try db.assert(datalog: "p(X) :- s(X)")
		try db.assert(datalog: "s(X) :- r(X)")

		#expect(throws: CoherenceError.self) {
			try db.assert(datalog: "r(1)")
		}
		#expect(try !believesBoth(db), "p(1) and -p(1) must not both be derivable")
	}

	/// The scan considers every relation with a live negative side, and one it passes over — whether
	/// because the write cannot reach it or because it is coherent — must not end it. Two relations are
	/// known-negative here and only one of them contradicts, so whichever the scan reaches first, it has
	/// to keep going — the parameter puts the clean candidate first in one case and second in the other,
	/// so neither enumeration order can pass by accident.
	@Test(
		"a coherent candidate does not abandon the scan",
		arguments: ["a", "z"])
	func contradictionBehindACoherentCandidate(_ contradicting: String) async throws {
		let db = try RBDB(path: ":memory:")
		try db.query(sql: "CREATE TABLE a(x)")
		try db.query(sql: "CREATE TABLE z(x)")
		try db.query(sql: "CREATE TABLE t(x)")

		// Both `a` and `z` are known-negative, so both are candidates; only one is reachable from `t`.
		try db.assert(datalog: "-a(1)")
		try db.assert(datalog: "-z(1)")
		try db.assert(datalog: "\(contradicting)(X) :- t(X)")

		#expect(throws: CoherenceError.self) {
			try db.assert(datalog: "t(1)")
		}
		#expect(
			try db.query(datalog: "\(contradicting)(A)").map { _ in 1 }.isEmpty,
			"\(contradicting)(1) and -\(contradicting)(1) must not both be derivable")
	}

	/// Which of the two contradicting literals gets named. It must never be the formula being asserted —
	/// that one is the newcomer, and telling the caller to retract what they just asked for is no help —
	/// and among the rest it should be one that is actually *stored*, since that is the row they can act
	/// on. Keyed to the asserted formula's own polarity, this picks wrong: the assert here is negative
	/// but the relation it breaks is `p`, whose retractable side is the stored `-p(1)`.
	@Test("the cited literal is the stored culprit, not the newly asserted formula")
	func contradictionCitesStoredCulpritInAThirdRelation() async throws {
		let db = try RBDB(path: ":memory:")
		try db.query(sql: "CREATE TABLE p(a)")
		try db.query(sql: "CREATE TABLE s(a)")
		try db.assert(datalog: "-p(1)")

		// This rule makes `p` derivable from what is known *false* of `s`.
		try db.assert(datalog: "p(X) :- -s(X)")

		let error = #expect(throws: CoherenceError.self) {
			try db.assert(datalog: "-s(1)")
		}
		guard case .contradiction(let contradicts, let derivedFrom) = error else { return }
		#expect(
			contradicts == (try DatalogParser().parse(formula: "-p(1)")),
			"the stored `-p(1)` is the culprit, not the derived `p(1)`")
		#expect(derivedFrom != nil, "and being stored, it can be named exactly")
	}

	/// The same preference, but *across* relations rather than within one. A write can break several at
	/// once, and they are not equally useful to report: one whose two sides are both derived names a
	/// literal the caller cannot retract — and, once the write is undone, cannot even query, which is
	/// what makes citing it read as nonsense. So a coherent-looking candidate is not the only reason to
	/// keep scanning; an *unactionable* contradiction is too.
	///
	/// `a` sorts before `z` in the candidate scan, so the parameter puts the unactionable relation first
	/// in one case and second in the other, and neither enumeration order can pass by accident.
	@Test(
		"the scan prefers a relation with a retractable culprit over one with none",
		arguments: ["z", "a"])
	func contradictionPrefersRetractableRelation(_ stored: String) async throws {
		let derived = stored == "a" ? "z" : "a"
		let db = try RBDB(path: ":memory:")
		try db.query(sql: "CREATE TABLE a(x)")
		try db.query(sql: "CREATE TABLE z(x)")

		// Both polarities of `derived` follow from the corresponding polarity of `stored`, so asserting
		//  the inverse of the stored fact contradicts *both* relations at once.
		try db.assert(datalog: "\(stored)(1)")
		try db.assert(datalog: "\(derived)(X) :- \(stored)(X)")
		try db.assert(datalog: "-\(derived)(X) :- -\(stored)(X)")

		let error = #expect(throws: CoherenceError.self) {
			try db.assert(datalog: "-\(stored)(1)")
		}
		guard case .contradiction(let contradicts, let derivedFrom) = error else { return }
		let culprit = try DatalogParser().parse(formula: "\(stored)(1)")
		#expect(
			contradicts == culprit,
			"the stored `\(stored)(1)` is what the caller can act on; `\(derived)` has no such side"
		)
		#expect(derivedFrom == [culprit], "and it can be named exactly")
	}

	/// The shape that surfaced this: two relations mutually defined *across* polarities, so asserting
	/// either polarity of either one makes all four literals derivable at once. Only the fact the caller
	/// stored is retractable; the other relation's two sides are both derived, and after the failed
	/// write neither is even queryable — citing one is what read as nonsense.
	///
	/// Both arguments matter. The name decides scan order (`a` before `z`), so one case has the
	/// unactionable relation first; the polarity decides which side of the collision the newcomer is,
	/// which is the within-relation preference this must not regress.
	@Test(
		"mutually defined polarities cite the stored fact, whichever relation and polarity it is",
		arguments: ["a", "z"], ["", "-"])
	func contradictionUnderMutualPolarityRules(_ name: String, _ polarity: String) async throws {
		let other = name == "a" ? "z" : "a"
		let inverse = polarity.isEmpty ? "-" : ""
		let db = try RBDB(path: ":memory:")
		try db.query(sql: "CREATE TABLE a(x)")
		try db.query(sql: "CREATE TABLE z(x)")
		try db.assert(datalog: "-\(name)(X) :- \(other)(X)")
		try db.assert(datalog: "\(name)(X) :- -\(other)(X)")
		try db.assert(datalog: "-\(other)(X) :- \(name)(X)")
		try db.assert(datalog: "\(other)(X) :- -\(name)(X)")

		try db.assert(datalog: "\(polarity)\(name)(1)")

		let error = #expect(throws: CoherenceError.self) {
			try db.assert(datalog: "\(inverse)\(name)(1)")
		}
		guard case .contradiction(let contradicts, let derivedFrom) = error else { return }
		let culprit = try DatalogParser().parse(formula: "\(polarity)\(name)(1)")
		#expect(contradicts == culprit, "the stored fact, not a derived literal of `\(other)`")
		#expect(derivedFrom == [culprit], "retracting it is what resolves the contradiction")
	}

	@Test("a stored inverse is cited; a merely derived one is not")
	func contradictionCitesCulprit() async throws {
		func inverse(_ setUp: (RBDB) throws -> Void) throws -> CoherenceError? {
			let db = try RBDB(path: ":memory:")
			try db.query(sql: "CREATE TABLE p(a)")
			try db.query(sql: "CREATE TABLE q(a)")
			try setUp(db)
			do {
				try db.assert(datalog: "-p(1)")
				return nil
			} catch let error as CoherenceError {
				return error
			}
		}

		let direct = try inverse { try $0.assert(datalog: "p(1)") }
		guard case .contradiction(_, let storedAs) = direct else {
			Issue.record("expected a contradiction, got \(String(describing: direct))")
			return
		}
		#expect(storedAs != nil, "a stored inverse can be named exactly")

		let viaRule = try inverse {
			try $0.assert(datalog: "q(1)")
			try $0.assert(datalog: "p(X) :- q(X)")
		}
		guard case .contradiction(_, let derivedStoredAs) = viaRule else {
			Issue.record("expected a contradiction, got \(String(describing: viaRule))")
			return
		}
		#expect(
			derivedStoredAs == nil,
			"a derived inverse has no row to cite — the derivation is provenance we don't keep")
	}

	@Test("retract-then-assert is the supported revision path")
	func revisionIsTwoActs() async throws {
		let db = try RBDB(path: ":memory:")
		try db.query(sql: "CREATE TABLE p(a)")
		try db.assert(datalog: "p(1)")

		try db.retract(datalog: "p(1)")  // contraction — now unknown
		try db.assert(datalog: "-p(1)")  // expansion   — now known false

		#expect(try db.query(datalog: "p(A)").map { _ in 1 }.isEmpty)
		#expect(try db.query(datalog: "-p(A)").map { Int($0["A"] as! Int64) } == [1])
	}

	/// A second connection cannot slip a fact past this one's coherence check, because there is no
	/// second connection: a database serves one at a time, and the second `init` is refused.
	///
	/// This replaces an interleaving test that drove two connections through each other's `assert` at
	/// three chosen points. What that test established — that the check inside the transaction closes
	/// the window an outside-the-transaction check would leave open — is no longer the property that
	/// carries the guarantee, and its scenario can no longer be built at all. The reason for the
	/// restriction is broader than the race it covered: what one connection believes is partly held in
	/// *its* temp views and closures, so a second one can reason from a rule set the checking
	/// connection cannot see.
	@Test("a second connection on the same database is refused")
	func secondConnectionRefused() async throws {
		let path = NSTemporaryDirectory() + "rbdb-single-\(UUID().uuidString).db"
		defer { try? FileManager.default.removeItem(atPath: path) }

		var first: RBDB? = try RBDB(path: path)
		try first!.query(sql: "CREATE TABLE p(a)")
		try first!.assert(datalog: "p(1)")

		#expect(throws: RBDBError.self) {
			_ = try RBDB(path: path)
		}

		// Releasing the first connection hands the database on: the restriction is one connection at a
		//  time, not one ever.
		first = nil
		let second = try RBDB(path: path)
		#expect(try second.query(datalog: "p(A)").map { Int($0["A"] as! Int64) } == [1])

		// And it holds for a database that already carries the schema, where opening writes nothing and
		//  a lock taken only on demand would be a shared one that a second reader could join.
		#expect(throws: RBDBError.self) {
			_ = try RBDB(path: path)
		}
	}

	@Test("a fact with no live rows of the complementary polarity skips the inverse query")
	func coherenceShortCircuits() async throws {
		let db = try RBDB(path: ":memory:")
		try db.query(sql: "CREATE TABLE p(a)")
		try db.assert(datalog: "p(1)")

		// Nothing negative about `p` exists, so the check is one indexed probe on `_rule` and must not
		//  force `[-p]` into existence.
		let negativeViews = Array(
			try db.query(sql: "SELECT COUNT(*) AS n FROM temp.sqlite_schema WHERE name = '-p'"))
		#expect(negativeViews.first?["n"] as? Int64 == 0)
	}

	/// A candidate is only worth comparing if this write could have changed what it derives, which is to
	/// say if the written relation is somewhere in one of its two dependency cones. `-a(1)` makes `a` a
	/// candidate for every later write; writing an unrelated `q` must not re-ask about it.
	///
	/// Whether the check looked is visible in whether it forced `a`'s relations into existence, which is
	/// why this starts from a fresh connection: the setup's own writes build those views, and a fresh
	/// connection has none. The parameter puts `q` inside the cone in one case and outside it in the
	/// other, so the skip has to be reached by reachability rather than by never looking at all.
	@Test(
		"a write that cannot reach a candidate does not compare it", arguments: [false, true])
	func coherenceSkipsUnreachableCandidate(_ reachable: Bool) async throws {
		let path = NSTemporaryDirectory() + "rbdb-cone-\(UUID().uuidString).db"
		defer { try? FileManager.default.removeItem(atPath: path) }

		do {
			let setup = try RBDB(path: path)
			try setup.query(sql: "CREATE TABLE a(x)")
			try setup.query(sql: "CREATE TABLE q(x)")
			try setup.assert(datalog: "-a(1)")
			if reachable { try setup.assert(datalog: "a(X) :- q(X)") }
		}

		let db = try RBDB(path: path)
		try db.assert(datalog: "q(2)")

		let touched = Set(
			try db.query(sql: "SELECT name FROM temp.sqlite_schema")
				.compactMap { $0["name"] as? String })
		#expect(
			touched.contains("a") == reachable,
			"`a` was \(reachable ? "not " : "")compared: \(touched.sorted())")
		#expect(touched.contains("-a") == reachable)

		// Either way the answer is the same, and still correct: `a(2)` is derivable in the reachable
		//  case and collides with nothing, since what is known false is `a(1)`.
		#expect(try db.query(datalog: "-a(A)").map { Int($0["A"] as! Int64) } == [1])
	}

	/// `[p] INTERSECT [-p]` reads both relations end to end, which is only an option where both of them
	/// end. A *value-generating* side — arithmetic under recursion, so it streams out of a `WITH
	/// RECURSIVE` CTE rather than being materialized — has no bound, and the scan used to churn for ~5
	/// minutes before dying with a misleading `cannot rollback - no transaction is active` (`assert`'s
	/// own `ROLLBACK`, firing after SQLite had already abandoned the transaction).
	///
	/// The question is now asked from the finite side instead: a contradiction is always some *ground*
	/// literal, so the finitely many literals the finite polarity holds are the complete candidate set,
	/// and each is put to the other polarity as a query with its arguments bound — which is exactly what
	/// `RecursiveClosure` can bound the recursion with. Both answers have to be reachable, so this
	/// checks a value that *is* derivable (the contradiction must be found, not merely survived) and one
	/// that is not (the clean answer must be reached by bounding the recursion, not by giving up on it).
	///
	/// Only the asserts below are bounded: they are the ones that make `nat` a coherence candidate, and
	/// so the only ones that go near it.
	@Test("the coherence check terminates against a value-generating relation")
	func coherenceAgainstValueGeneratingRelation() async throws {
		let db = try RBDB(path: ":memory:")
		try db.query(sql: "CREATE TABLE nat(n)")
		try db.assert(datalog: "nat(0)")
		try db.assert(datalog: "nat(X + 1) :- nat(X)")

		let budget = StepBudget()
		budget.install(on: db, opcodes: 5_000_000)
		defer { budget.uninstall(on: db) }

		#expect(throws: CoherenceError.self) {
			try db.assert(datalog: "-nat(99)")
		}
		#expect(!budget.exhausted, "the check did not terminate on the derivable value")

		// `99.5` is not reachable from `0` by `+1`, so nothing collides and the assert stands.
		try db.assert(datalog: "-nat(99.5)")
		#expect(!budget.exhausted, "the check did not terminate on the underivable value")
		#expect(try db.query(datalog: "-nat(A)").map { $0["A"] as! Double } == [99.5])
	}

	/// The same, entered from the other side: here the *negative* relation is the value-generating one
	/// and `nat` is a lone ground fact, so it is the positive side that has to be enumerated and the
	/// negative side probed. Nothing about the check may be keyed to polarity.
	@Test("the check terminates where it is the negative side that is value-generating")
	func coherenceAgainstValueGeneratingNegativeRelation() async throws {
		let db = try RBDB(path: ":memory:")
		try db.query(sql: "CREATE TABLE nat(n)")
		try db.assert(datalog: "-nat(0)")
		try db.assert(datalog: "-nat(X + 1) :- -nat(X)")

		let budget = StepBudget()
		budget.install(on: db, opcodes: 5_000_000)
		defer { budget.uninstall(on: db) }

		#expect(throws: CoherenceError.self) {
			try db.assert(datalog: "nat(99)")
		}
		#expect(!budget.exhausted, "the check did not terminate on the derivable value")

		try db.assert(datalog: "nat(99.5)")
		#expect(!budget.exhausted, "the check did not terminate on the underivable value")
	}

	/// **The remaining gap, pinned.** Where *both* polarities are value-generating there is no finite
	/// side to enumerate and so no ground literal to probe with, and deciding it in general means
	/// deciding whether two Datalog-with-arithmetic relations intersect. Rather than accept such writes
	/// unchecked — which would let `nat(50)` and `-nat(50)` both become derivable with nothing said —
	/// the engine declines to *enter* that state: the write that would make the second polarity
	/// value-generating is refused, and the refusal names the relation it could no longer decide.
	///
	/// Neither order can slip through, so the parameter asserts the two rules the other way round. The
	/// first of them is always fine on arrival — one value-generating side is still decidable.
	@Test(
		"a write that would leave both polarities value-generating is refused",
		arguments: [true, false])
	func coherenceBetweenTwoValueGeneratingRelations(_ positiveFirst: Bool) async throws {
		let db = try RBDB(path: ":memory:")
		try db.query(sql: "CREATE TABLE nat(n)")
		try db.assert(datalog: "nat(0)")

		let steps =
			positiveFirst
			? ["nat(X + 1) :- nat(X)", "-nat(X + 1) :- -nat(X)"]
			: ["-nat(X + 1) :- -nat(X)", "nat(X + 1) :- nat(X)"]
		try db.assert(datalog: steps[0])

		let budget = StepBudget()
		budget.install(on: db, opcodes: 5_000_000)
		defer { budget.uninstall(on: db) }

		let error = #expect(throws: CoherenceError.self) {
			try db.assert(datalog: steps[1])
		}
		#expect(!budget.exhausted, "the refusal must be reached without scanning either side")
		guard case .undecidable(let relation) = error else {
			Issue.record("expected `.undecidable`, got \(String(describing: error))")
			return
		}
		#expect(relation == "nat")

		// The refused rule was rolled back, and the connection is still usable — including for the
		//  check, which is decidable again now that only one side generates values.
		#expect(try db.fetchRules(for: steps[1].hasPrefix("-") ? "-nat" : "nat").isEmpty)
		try db.assert(datalog: "-nat(99.5)")
	}

	/// An undecidable pair is the *weakest* answer the scan can give: it says only that the engine
	/// cannot tell. A definite contradiction found in some other relation is a fact about the write, so
	/// it must be the one reported — reaching an undecidable candidate can no more end the scan than
	/// reaching a coherent one can.
	///
	/// `a` sorts before `p`, so the undecidable candidate is the one the scan meets first. The final
	/// rule is refused twice over: it leaves `a`/`-a` both value-generating, *and* it makes `-a(1)`
	/// derivable, which carries `p(1)` against the stored `-p(1)`.
	@Test("a definite contradiction outranks an undecidable candidate")
	func contradictionOutranksUndecidable() async throws {
		let db = try RBDB(path: ":memory:")
		try db.query(sql: "CREATE TABLE a(n)")
		try db.query(sql: "CREATE TABLE p(m)")
		try db.assert(datalog: "-a(0)")
		try db.assert(datalog: "a(X + 1) :- a(X)")
		try db.assert(datalog: "p(X) :- -a(X)")
		try db.assert(datalog: "-p(1)")

		let budget = StepBudget()
		budget.install(on: db, opcodes: 5_000_000)
		defer { budget.uninstall(on: db) }

		let error = #expect(throws: CoherenceError.self) {
			try db.assert(datalog: "-a(X + 1) :- -a(X)")
		}
		#expect(!budget.exhausted, "the check did not terminate")
		guard case .contradiction(let contradicts, _) = error else {
			Issue.record("expected `.contradiction`, got \(String(describing: error))")
			return
		}
		#expect(
			contradicts == (try DatalogParser().parse(formula: "-p(1)")),
			"the stored culprit, not `a`")
	}

	// MARK: - §4.2.2 The SQL surface asks the same question

	@Test("INSERT raises where the inverse is a live stored fact")
	func sqlInsertContradictionStored() async throws {
		let db = try RBDB(path: ":memory:")
		try db.query(sql: "CREATE TABLE p(a)")
		try db.assert(datalog: "-p(1)")

		#expect(throws: CoherenceError.self) {
			try db.query(sql: "INSERT INTO p(a) VALUES (1)")
		}
		#expect(try db.query(datalog: "p(A)").map { _ in 1 }.isEmpty, "nothing was asserted")
	}

	@Test("INSERT raises where the inverse is only derivable through a rule")
	func sqlInsertContradictionDerived() async throws {
		let db = try RBDB(path: ":memory:")
		try db.query(sql: "CREATE TABLE p(a)")
		try db.query(sql: "CREATE TABLE q(a)")
		try db.assert(datalog: "q(1)")
		try db.assert(datalog: "p(X) :- q(X)")
		#expect(throws: CoherenceError.self) {
			try db.query(sql: "INSERT INTO [-p] VALUES (1)")
		}
	}

	/// Which of the two literals a *raw SQL* write is told about. `assert` knows the formula it was
	/// handed and filters it out, but an `INSERT` arrives here already executed and unparsed, so the
	/// newcomer has to be recognized another way — by its `_rule` row being younger than the write.
	/// Missed, the check names the row the statement just wrote and offers it as the retraction that
	/// resolves things, which it cannot: the write was rolled back, so that row no longer exists.
	@Test("an INSERT is never cited as its own culprit", arguments: ["", "-"])
	func sqlInsertCitesTheOtherSide(_ polarity: String) async throws {
		let inverse = polarity == "-" ? "" : "-"
		let db = try RBDB(path: ":memory:")
		try db.query(sql: "CREATE TABLE p(a)")
		try db.assert(datalog: "\(inverse)p(1)")

		// The name is an identifier, so build the text first: an `SQL` literal would bind it as a `?`.
		let insert = "INSERT INTO [\(polarity)p] VALUES (1)"
		let error = #expect(throws: CoherenceError.self) {
			try db.query(sql: SQL(insert))
		}
		guard case .contradiction(let contradicts, let derivedFrom) = error else { return }
		let culprit = try DatalogParser().parse(formula: "\(inverse)p(1)")
		#expect(contradicts == culprit, "the standing row is the culprit, not the inserted one")
		#expect(derivedFrom == [culprit], "and retracting it is what resolves the contradiction")
	}

	/// The same, where the other side is *derived* and so nothing is retractable at all. Naming the
	/// inserted row here is worse than saying nothing: it reads as a resolution and is not one.
	@Test("an INSERT whose other side is derived offers no retraction rather than a false one")
	func sqlInsertDerivedOtherSideOffersNothing() async throws {
		let db = try RBDB(path: ":memory:")
		try db.query(sql: "CREATE TABLE p(a)")
		try db.query(sql: "CREATE TABLE q(a)")
		try db.assert(datalog: "q(1)")
		try db.assert(datalog: "p(X) :- q(X)")

		let error = #expect(throws: CoherenceError.self) {
			try db.query(sql: "INSERT INTO [-p] VALUES (1)")
		}
		guard case .contradiction(let contradicts, let derivedFrom) = error else { return }
		#expect(
			contradicts == (try DatalogParser().parse(formula: "p(1)")),
			"the derived `p(1)` is what the insert collides with")
		#expect(derivedFrom == nil, "and there is no stored row to hand back")
	}

	/// A rolled-back statement must leave the connection as it found it. `ROLLBACK TO` rewinds a
	/// savepoint without popping it, so without the `RELEASE` the coherence savepoint outlives the
	/// statement — and when it is the outermost one, so does the transaction it implicitly opened. The
	/// next `BEGIN` then fails with "cannot start a transaction within a transaction", and anything
	/// written in between is stranded in a transaction nothing will commit.
	@Test("a rolled-back write leaves no transaction open")
	func rolledBackWriteLeavesNoTransaction() async throws {
		let db = try RBDB(path: ":memory:")
		try db.query(sql: "CREATE TABLE p(a)")
		try db.assert(datalog: "-p(1)")

		#expect(throws: CoherenceError.self) {
			try db.query(sql: "INSERT INTO p(a) VALUES (1)")
		}
		#expect(sqlite3_get_autocommit(db.db) != 0, "the failed write opened nothing lasting")

		// The revision path is the first thing a caller reaches for after this error, and it opens a
		//  transaction of its own.
		try db.retract(datalog: "-p(1)")
		try db.query(sql: "INSERT INTO p(a) VALUES (1)")
		#expect(try db.query(datalog: "p(A)").map { Int($0["A"] as! Int64) } == [1])
	}

	@Test("the check fires on matching arguments only")
	func sqlInsertNonMatchingArguments() async throws {
		let db = try RBDB(path: ":memory:")
		try db.query(sql: "CREATE TABLE p(a)")
		try db.assert(datalog: "-p(2)")

		try db.query(sql: "INSERT INTO p(a) VALUES (1)")
		#expect(try db.query(datalog: "p(A)").map { Int($0["A"] as! Int64) } == [1])
	}

	@Test("a superseded inverse does not block, so retract-then-assert works through SQL too")
	func sqlInsertAfterRetraction() async throws {
		let db = try RBDB(path: ":memory:")
		try db.query(sql: "CREATE TABLE p(a)")
		try db.assert(datalog: "-p(1)")
		try db.retract(datalog: "-p(1)")

		try db.query(sql: "INSERT INTO p(a) VALUES (1)")
		#expect(try db.query(datalog: "p(A)").map { Int($0["A"] as! Int64) } == [1])
	}

	@Test("the check survives `[-p]` not yet existing")
	func sqlInsertRescuesInverseView() async throws {
		let path = NSTemporaryDirectory() + "rbdb-neg-\(UUID().uuidString).db"
		defer { try? FileManager.default.removeItem(atPath: path) }
		do {
			let setup = try RBDB(path: path)
			try setup.query(sql: "CREATE TABLE p(a)")
			try setup.assert(datalog: "-p(1)")
		}

		// A fresh connection: no temp views exist yet, so `[p]` and `[-p]` both have to be resolved by
		//  `rescue` — from inside the check, after the write has landed — before the `INTERSECT` can run.
		//  It has to be a *separate* connection rather than a second one, since a database serves one at
		//  a time — see `secondConnectionRefused`.
		let db = try RBDB(path: path)
		#expect(throws: CoherenceError.self) {
			try db.query(sql: "INSERT INTO p(a) VALUES (1)")
		}
	}

	@Test("`-p` has the same write surface as any other predicate")
	func negativeWriteSurface() async throws {
		let db = try RBDB(path: ":memory:")
		try db.query(sql: "CREATE TABLE p(a)")

		try db.query(sql: "INSERT INTO [-p](a) VALUES (1)")
		#expect(try db.query(datalog: "-p(A)").map { Int($0["A"] as! Int64) } == [1])

		// And the mirror check: the contradiction is found whichever polarity is written.
		try db.assert(datalog: "p(2)")
		#expect(throws: CoherenceError.self) {
			try db.query(sql: "INSERT INTO [-p](a) VALUES (2)")
		}

		try db.query(sql: "DELETE FROM [-p] WHERE a = 1")
		#expect(try db.query(datalog: "-p(A)").map { _ in 1 }.isEmpty)
	}

	@Test("an insert with nothing negative in the database touches no inverse relation")
	func sqlInsertSkipsInverseRelation() async throws {
		let db = try RBDB(path: ":memory:")
		try db.query(sql: "CREATE TABLE p(a)")

		try db.query(sql: "INSERT INTO p(a) VALUES (1)")
		#expect(try db.query(datalog: "p(A)").map { Int($0["A"] as! Int64) } == [1])

		// `coherenceShortCircuits`, reached through SQL. This used to be the opposite expectation: the
		//  trigger named `[-p]` in its own text, and SQLite compiles a trigger's body when the firing
		//  statement is prepared — regardless of any `WHEN` or short-circuit — so the reference forced the
		//  view into existence on the very first insert. With the check moved out of the trigger, an
		//  insert into a database that knows nothing negative names no inverse relation at all.
		let inverseExists = Array(
			try db.query(sql: "SELECT COUNT(*) AS n FROM temp.sqlite_schema WHERE name = '-p'"))
		#expect(inverseExists.first?["n"] as? Int64 == 0)
	}

	@Test("the SQL check applies to a materialized predicate too")
	func sqlInsertContradictionMaterialized() async throws {
		let db = try RBDB(path: ":memory:")
		try db.query(sql: "CREATE TABLE e(a, b)")
		try db.query(sql: "CREATE TABLE p(a, b)")
		try db.assert(datalog: "p(X, Y) :- e(X, Y)")
		try db.assert(datalog: "p(X, Z) :- p(X, Y), p(Y, Z)")
		try db.assert(datalog: "-p(9, 9)")
		#expect(try db.query(datalog: "p(A, B)").map { _ in 1 }.isEmpty)

		#expect(throws: CoherenceError.self) {
			try db.query(sql: "INSERT INTO p(a, b) VALUES (9, 9)")
		}
		try db.query(sql: "INSERT INTO p(a, b) VALUES (1, 2)")
		#expect(try db.query(datalog: "p(A, B)").map { _ in 1 }.count == 1)
	}

	/// `contradictionIndirect`'s twin on the SQL surface, and this section's premise in its sharpest
	/// form. A check keyed to the inserted relation cannot see this one — which is why it no longer lives
	/// in the per-predicate trigger, where the candidate set would have to be baked into static text.
	@Test("INSERT raises where the contradiction is reached only through a third relation")
	func sqlInsertContradictionIndirect() async throws {
		let db = try RBDB(path: ":memory:")
		try db.query(sql: "CREATE TABLE p(a)")
		try db.query(sql: "CREATE TABLE r(a)")
		try db.assert(datalog: "-p(1)")
		try db.assert(datalog: "p(X) :- r(X)")

		// The relation written here is `r`, and nothing negative about `r` exists — yet the row makes
		//  `p(1)` derivable against a stored `-p(1)`.
		#expect(throws: CoherenceError.self) {
			try db.query(sql: "INSERT INTO r(a) VALUES (1)")
		}
		#expect(try !believesBoth(db), "p(1) and -p(1) must not both be derivable")
	}

	/// The check runs in a savepoint wrapped around each writing statement, which has to nest inside a
	/// transaction the caller opened rather than fight it. `BEGIN IMMEDIATE` is the trap: it takes a
	/// write lock, so SQLite reports it as a *write*, and wrapping it in a savepoint would make it a
	/// transaction within a transaction.
	@Test("an explicit transaction survives, and its statements are still checked")
	func sqlExplicitTransaction() async throws {
		let db = try RBDB(path: ":memory:")
		try db.query(sql: "CREATE TABLE p(a)")
		try db.query(sql: "CREATE TABLE q(a)")
		try db.assert(datalog: "-p(1)")

		try db.query(sql: "BEGIN IMMEDIATE")
		try db.query(sql: "INSERT INTO q(a) VALUES (1)")
		#expect(throws: CoherenceError.self) {
			try db.query(sql: "INSERT INTO p(a) VALUES (1)")
		}
		try db.query(sql: "COMMIT")

		// Only the contradicting statement was undone: the savepoint rewinds to just before it, leaving
		//  the rest of the caller's transaction intact to commit.
		#expect(try db.query(datalog: "q(A)").map { Int($0["A"] as! Int64) } == [1])
		#expect(try db.query(datalog: "p(A)").map { _ in 1 }.isEmpty)
	}

	// MARK: - A negative head is an ordinary head

	@Test("a negative *fact* needs no rule at all")
	func negativeFactAllowed() async throws {
		let db = try RBDB(path: ":memory:")
		try db.query(sql: "CREATE TABLE p(a)")
		try db.assert(datalog: "-p(1)")
		#expect(try db.query(datalog: "-p(A)").map { Int($0["A"] as! Int64) } == [1])
	}

	@Test("a negative-headed rule round-trips through storage and `fetchRules`")
	func negativeHeadedRuleStorage() async throws {
		let db = try RBDB(path: ":memory:")
		try db.query(sql: "CREATE TABLE p(a)")
		try db.query(sql: "CREATE TABLE q(a)")
		try db.assert(datalog: "q(1)")

		let rule = try DatalogParser().parse(formula: "-p(X) :- q(X)")
		try db.assert(formula: rule)

		let fetched = try db.fetchRules(for: "-p")
		#expect(fetched.count == 1, "a negative head is an ordinary rule to `fetchRules`")
		#expect(fetched.first?.canonicalize() == rule.canonicalize())
		#expect(try db.fetchRules(for: "p").isEmpty, "`p` and `-p` are separate relations")

		// And it backs a view like any other rule.
		#expect(try db.query(datalog: "-p(A)").map { Int($0["A"] as! Int64) } == [1])
	}

	/// The same program written in both polarities must behave identically, because the only thing that
	/// distinguishes `-p` from `p` is its name. Derivation, chaining through an intermediate relation,
	/// and the SQL surface are all checked twice over — once with a positive head, once with a negative
	/// one — so any divergence shows up as a difference between the two runs rather than as a bare
	/// assertion about one of them.
	@Test(
		"a negative head derives, chains and reads back exactly like a positive one",
		arguments: ["", "-"])
	func negativeHeadMatchesPositive(_ polarity: String) async throws {
		let db = try RBDB(path: ":memory:")
		try db.query(sql: "CREATE TABLE p(a)")
		try db.query(sql: "CREATE TABLE mid(a)")
		try db.query(sql: "CREATE TABLE q(a)")

		try db.assert(datalog: "\(polarity)p(X) :- mid(X)")
		try db.assert(datalog: "mid(X) :- q(X)")
		try db.assert(datalog: "q(1)")
		try db.assert(datalog: "q(2)")

		// Derived through two rules, and read back through both surfaces.
		#expect(
			try db.query(datalog: "\(polarity)p(A)").map { Int($0["A"] as! Int64) }.sorted() == [
				1, 2,
			])
		// The relation name is an identifier, so build the text before handing it to `SQL` — an `SQL`
		//  literal would bind the interpolation as a `?` parameter.
		let selectSQL = "SELECT a FROM [\(polarity)p] ORDER BY a"
		#expect(try db.query(sql: SQL(selectSQL)).map { $0["a"] as! Int64 } == [1, 2])

		// A fact asserted straight into the same relation joins the derived rows.
		try db.assert(datalog: "\(polarity)p(3)")
		#expect(
			try db.query(datalog: "\(polarity)p(A)").map { _ in 1 }.count == 3,
			"base facts and rule arms union in either polarity")

		// Retracting the rule takes the derived rows with it and leaves the base fact.
		try db.retract(datalog: "\(polarity)p(X) :- mid(X)")
		#expect(try db.query(datalog: "\(polarity)p(A)").map { Int($0["A"] as! Int64) } == [3])
	}

	@Test("a negative head recurses like a positive one", arguments: ["", "-"])
	func negativeHeadRecursion(_ polarity: String) async throws {
		let db = try RBDB(path: ":memory:")
		try db.query(sql: "CREATE TABLE edge(x, y)")
		try db.query(sql: "CREATE TABLE path(x, y)")

		// Transitive closure, which routes through the iterative fixpoint evaluator. `-path` is a
		//  relation name like any other, so it must materialize the same way.
		try db.assert(datalog: "\(polarity)path(X, Y) :- edge(X, Y)")
		try db.assert(
			datalog: "\(polarity)path(X, Z) :- \(polarity)path(X, Y), \(polarity)path(Y, Z)")
		try db.assert(datalog: "edge(1, 2)")
		try db.assert(datalog: "edge(2, 3)")

		let pairs = try db.query(datalog: "\(polarity)path(A, B)")
			.map { [Int($0["A"] as! Int64), Int($0["B"] as! Int64)] }
			.sorted { $0.lexicographicallyPrecedes($1) }
		#expect(pairs == [[1, 2], [1, 3], [2, 3]], "1→3 is reached only by transitivity")
	}

	/// The case the restriction was written to rule out — `-alive(X) :- dead(X)`, with no `not alive(X)`
	/// to protect it. It is now assertable, and where it *would* derive the inverse of something live the
	/// contradiction is **reported** rather than silently suppressed by a rule that declines to fire.
	/// PLAN-RETRACTION called that out as the better behaviour when it settled for the restriction.
	@Test(
		"a rule that would derive the inverse of a live fact is refused, in either arrival order",
		arguments: [true, false])
	func negativeHeadedRuleContradiction(_ ruleFirst: Bool) async throws {
		let db = try RBDB(path: ":memory:")
		try db.query(sql: "CREATE TABLE alive(a)")
		try db.query(sql: "CREATE TABLE dead(a)")
		try db.assert(datalog: "alive(1)")

		let steps =
			ruleFirst
			? ["-alive(X) :- dead(X)", "dead(1)"]
			: ["dead(1)", "-alive(X) :- dead(X)"]
		try db.assert(datalog: steps[0])
		#expect(throws: CoherenceError.self) {
			try db.assert(datalog: steps[1])
		}
		#expect(
			try db.query(datalog: "-alive(A)").map { _ in 1 }.isEmpty,
			"alive(1) and -alive(1) must not both be derivable")
	}

	@Test("a negative-headed rule is coherent as long as nothing collides")
	func negativeHeadedRuleWithoutCollision() async throws {
		let db = try RBDB(path: ":memory:")
		try db.query(sql: "CREATE TABLE alive(a)")
		try db.query(sql: "CREATE TABLE dead(a)")
		try db.assert(datalog: "alive(1)")
		try db.assert(datalog: "dead(2)")

		// `2` is dead and only `1` is alive, so the rule derives `-alive(2)` and contradicts nothing.
		try db.assert(datalog: "-alive(X) :- dead(X)")
		#expect(try db.query(datalog: "-alive(A)").map { Int($0["A"] as! Int64) } == [2])
		#expect(try db.query(datalog: "alive(A)").map { Int($0["A"] as! Int64) } == [1])
	}
}
