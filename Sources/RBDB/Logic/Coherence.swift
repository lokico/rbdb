import Foundation
import SQLite3

public enum CoherenceError: Error {
	/// Some ground literal is derivable in **both** polarities, `p(t̄)` and `-p(t̄)` at once,
	/// which is not allowed.
	///
	/// - Parameter contradicts: The offending literal in whichever polarity the caller can act on
	/// - Parameter derivedFrom: One or more formulas that could be passed to ``RBDB/RBDB/retract(formula:)``
	///   to resolve the contradiction. Empty if the contradicting formula is not derived from anything retractable.
	///   `nil` if that information is not available.
	case contradiction(contradicts: Formula, derivedFrom: [Formula]?)
}

// Coherence for strong negation: `p(…)` and `-p(…)` are different claims about the same arguments, and
// under the open world both are storable — which is a contradiction we don't want to allow.
//
// The engine refuses the contradiction rather than repairing it. An earlier design had asserting
// `-p(1)` auto-retract `p(1)` — the Levi identity (`T * ¬p = (T ÷ p) + ¬p`) performed on the caller's
// behalf. That was rejected twice over: it only works for a *stored* inverse (a derivable-but-unstored
// one has nothing to supersede, so it would have to throw anyway, exposing a base/derived distinction
// the caller shouldn't have to reason about), and it makes an assert silently destructive. Revision is
// still exactly the Levi identity — it just isn't automated:
//
//     try db.retract(datalog: "p(1).")     // contraction — now unknown
//     try db.assert(datalog: "-p(1).")     // expansion   — now known false
extension RBDB {
	/// The savepoint a writing statement runs inside, so a contradiction it turns out to create can be
	/// undone. A savepoint rather than a transaction because it *nests*: the caller may already have one
	/// open, and on the `assert` path there always is.
	static let coherenceSavepoint = "_coherence"

	/// Throws `CoherenceError.contradiction` if any relation is now derivable in both polarities — i.e.
	/// some `p(t̄)` and `-p(t̄)` both hold.
	///
	/// **Runs after the write, inside the same transaction**, and that ordering is what gives it its
	/// reach. Asked *before* the write it could only answer "is this exact ground fact's inverse already
	/// derivable", which misses everything the new row itself enables — a rule like `p(X) :- -p(X)`
	/// contradicts nothing when stored and everything once a `-p` fact arrives, and vice versa. Asked
	/// after, the relations already reflect the new row, so facts and rules are checked by one question
	/// and neither arrival order can slip through. A throw undoes the write.
	///
	/// It checks *every* relation with a live negative side, not only the one `formula` heads, because a
	/// contradiction need not mention either polarity to complete one: asserting `r(1)` where
	/// `p(X) :- r(X)` and `-p(1)` are already stored makes `p`/`-p` contradict without naming `p` at all,
	/// at any depth of rule chaining. Driving the scan from the negative side is what makes that
	/// affordable — a contradiction needs *both* polarities, so the candidates are exactly the relations
	/// someone has said something negative about, which is usually none at all.
	///
	/// Only writes need this. Positive Datalog is monotonic, so a retraction can only ever make *fewer*
	/// literals derivable and cannot create a contradiction. That stops being true once
	/// negation-as-failure lands: retracting `q(1)` will make `not q(1)` hold, which can derive `-p(1)`
	/// over a live `p(1)`.
	func checkCoherence(of formula: Formula? = nil) throws {
		// The check writes nothing, but its queries do reach `rescue` and the materializer, so guard
		//  against re-entering it the way `refreshDirtyMaterializations` guards against a nested refresh.
		guard !isCheckingCoherence else { return }
		isCheckingCoherence = true
		defer { isCheckingCoherence = false }

		// FIXME: One `INTERSECT` per candidate is the unoptimized form. The lever, when it matters, is to
		//  skip a candidate whose `dependencyCone` doesn't contain what was just written — nothing else
		//  can have changed what that candidate derives.
		let asserted = formula?.canonicalize()
		// The best answer found so far in a relation where *nothing* is retractable. Held rather than
		//  thrown, because a later candidate may yet offer one that is — see below.
		var unactionable: Formula? = nil

		for positiveName in try possiblyContradictingNegativelyKnownRelations() {
			guard let columns = try getColumns(for: positiveName),
				let contradicting = try contradictingLiterals(of: positiveName, columns: columns)
			else {
				// This relation is coherent (or undeclared)
				continue
			}

			// Which of the two to name. Never the formula being asserted: that one is the newcomer, not
			//  the culprit, and citing it would tell the caller to retract what they just asked for.
			//  Among what's left, prefer a live stored row, since that is the one they *can* retract —
			//  where the contradiction is completed through some third relation neither side is the
			//  newcomer, and the stored side is exactly the useful answer.
			let eligible = contradicting.filter { $0.formula != asserted }
			let stored = try eligible.filter(isStored).map(\.formula)
			if let culprit = stored.first {
				throw CoherenceError.contradiction(contradicts: culprit, derivedFrom: stored)
			}

			// Both sides here are derived, so there is nothing to hand back to `retract`. That answer is
			//  honest but close to useless — and actively confusing, since undoing the write also makes
			//  the cited literal unqueryable, so the caller is told about a "known value" they can then
			//  find no trace of. One write can break several relations at once (these rules propagate a
			//  collision across every relation that mentions either polarity), so keep scanning: another
			//  candidate may contradict through a stored row, which is the one they can act on.
			unactionable = unactionable ?? eligible.first?.formula
		}

		if let unactionable {
			throw CoherenceError.contradiction(contradicts: unactionable, derivedFrom: nil)
		}
	}

	/// Gets all relations anyone has said anything live and negative about that could contradict something else.
	///
	/// A contradiction needs both polarities, so this is the complete candidate set — and when it comes
	/// back empty the whole check is one indexed probe, which is what keeps a write on a hot path cheap
	/// and keeps the overwhelmingly common case from forcing views or materialized tables into existence.
	///
	/// Breaking the query down:
	///  - `substr(output_type, 3)` - returns results as *positive* names (`@-p` → `p`).
	///  - `superceded_by IS NULL` - only care about live facts/rules (also required to use the index)
	///  - `LIKE '@-%'` - a range scan on `idx_rule_ot_nlc_*`, thanks to SQLite's `LIKE` optimization (see schema.sql).
	private func possiblyContradictingNegativelyKnownRelations() throws -> [String] {
		try super.query(
			sql: """
				SELECT DISTINCT substr(output_type, 3) AS relation
				FROM _rule
				WHERE superceded_by IS NULL
				  AND output_type LIKE '@-%'
				"""
		).map { row in
			guard let name = row["relation"] as? String else {
				throw RBDBError.corruptData(message: "expected a text output_type in _rule")
			}
			return name
		}
	}

	/// A ground literal, kept alongside the canonical JSON it decoded from so that probing `_rule` for it
	/// doesn't have to re-encode it.
	private typealias Literal = (formula: Formula, json: String)

	/// A ground literal of `name` that holds in both polarities, given in each polarity — or nil where
	/// the relation is coherent.
	///
	/// Asks the two *relations*, not `_rule`: a view is `baseFactsSelect UNION rules`, so intersecting
	/// them covers stored and derived rows alike. `INTERSECT` compares every column, which is exactly
	/// "some ground literal holds in both polarities".
	private func contradictingLiterals(of name: String, columns: [String]) throws -> [Literal]? {
		let columnList = columns.map { "[\($0)]" }.joined(separator: ", ")
		let intersect = SQL(
			"""
			SELECT predicate_formula(?, \(columnList)) AS positive,
			       predicate_formula(?, \(columnList)) AS negative
			FROM (
				SELECT \(columnList) FROM [\(name)]
				INTERSECT
				SELECT \(columnList) FROM [-\(name)]
			)
			LIMIT 1
			""", arguments: [name, "-\(name)"])
		guard let row = try query(sql: intersect).makeIterator().next() else { return nil }

		func literal(_ key: String) throws -> Literal {
			guard let json = row[key] as? String, let data = json.data(using: .utf8) else {
				throw RBDBError.corruptData(
					message: "predicate_formula did not return valid UTF-8 JSON")
			}
			return Literal(formula: try JSONDecoder().decode(Formula.self, from: data), json: json)
		}
		return [try literal("positive"), try literal("negative")]
	}

	/// Whether this literal is itself a live row of `_rule` — something someone asserted, as opposed to a
	/// conclusion that merely follows.
	private func isStored(_ literal: Literal) throws -> Bool {
		try super.query(
			sql: """
				SELECT 1 FROM _rule
				WHERE superceded_by IS NULL AND formula = jsonb(\(literal.json))
				LIMIT 1
				"""
		).hasMoreRows
	}

	/// Whether `statement` writes, and so has to be checked for coherence once it completes.
	///
	/// `sqlite3_stmt_readonly` is the authority on what counts as a write: it accounts for triggers, so
	/// an `INSERT` into a predicate *view* counts even though the view stores nothing itself, and it
	/// reports transaction control as read-only, so `BEGIN`/`COMMIT`/`SAVEPOINT` are never wrapped. The
	/// one case it doesn't cover is `BEGIN IMMEDIATE`/`BEGIN EXCLUSIVE`, which take a write lock and so
	/// report as writes — a savepoint around one would make it a transaction within a transaction, so
	/// they are excluded by name.
	func writes(_ statement: OpaquePointer, sql: String) -> Bool {
		guard !isCheckingCoherence, sqlite3_stmt_readonly(statement) == 0 else { return false }
		return sql.range(of: "BEGIN", options: [.caseInsensitive, .anchored]) == nil
	}

	func beginCoherenceSavepoint() throws {
		let sql = "SAVEPOINT " + RBDB.coherenceSavepoint
		try super.query(sql: SQL(sql))
	}

	/// Ends the savepoint, undoing everything inside it first where asked. `ROLLBACK TO` rewinds without
	/// popping, so the `RELEASE` is needed either way.
	func endCoherenceSavepoint(rollingBack: Bool) throws {
		let sql = (rollingBack ? "ROLLBACK TO " : "RELEASE ") + RBDB.coherenceSavepoint
		try super.query(sql: SQL(sql))
	}
}
