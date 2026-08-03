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

	/// Coherence could not be decided for `relation`, so the write was refused.
	///
	/// This can occur with a *value-generating* rule on both polarities of a predicate. With `p` and `-p` each
	/// able to escape the finite active domain, neither can be enumerated to bound the other. The write that would
	/// leave a relation in that state gets refused.
	///
	/// - Parameter relation: The relation, named positively (`p`, never `-p`), whose two polarities
	///   could not be compared.
	case undecidable(relation: String)
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
	///
	/// - Parameter formula: What the caller asserted, where it can say. Never cited as the culprit.
	/// - Parameter writtenAfter: The id of the youngest `_rule` row that predates this write, for a
	///   caller that *can't* name what it wrote — a raw SQL statement arrives here already executed and
	///   unparsed. Anything younger was stored by the write itself and is likewise not the culprit.
	///   `nil` where the caller took no such reading, and then nothing is treated as new.
	func checkCoherence(of formula: Formula? = nil, writtenAfter watermark: Int64? = nil) throws {
		// The check writes nothing, but its queries do reach `rescue` and the materializer, so guard
		//  against re-entering it the way `refreshDirtyMaterializations` guards against a nested refresh.
		guard !isCheckingCoherence else { return }
		isCheckingCoherence = true
		defer { isCheckingCoherence = false }

		let asserted = formula?.canonicalize()
		// The relation this write landed in, where the caller named it. What a candidate derives is a
		//  function of its dependency cone, so a write outside both of a candidate's cones cannot have
		//  changed either side of it — and it was coherent before, or this check would not have let the
		//  previous write stand. Such a candidate is skipped without either side being read.
		//
		//  Only the `assert` path can say. A raw SQL statement arrives here already executed and
		//  unparsed, so `formula` is nil and every candidate is compared, as before.
		let written: String? =
			switch asserted {
			case .hornClause(let head, _, _): head.name
			case nil: nil
			}
		// The best answer found so far in a relation where *nothing* is retractable. Held rather than
		//  thrown, because a later candidate may yet offer one that is — see below.
		var unactionable: Formula? = nil
		// The first relation the check couldn't decide. Held for the same reason and one more: it is the
		//  weakest answer there is — it says only that the engine can't tell — so any *definite*
		//  contradiction found later outranks it.
		var undecidable: String? = nil

		for positiveName in try possiblyContradictingNegativelyKnownRelations() {
			guard let columns = try getColumns(for: positiveName) else {
				continue  // undeclared
			}

			// Both cones are needed anyway — they are what says whether either side can be enumerated —
			//  so they are taken here, once, and answer the reachability question on the way past.
			let cones = (
				positive: try dependencyCone(of: positiveName),
				negative: try dependencyCone(of: "-\(positiveName)")
			)
			if let written, !cones.positive.contains(written), !cones.negative.contains(written) {
				continue
			}

			let contradicting: [Literal]
			switch try verdict(for: positiveName, columns: columns, cones: cones) {
			case .coherent: continue
			case .undecidable:
				undecidable = undecidable ?? positiveName
				continue
			case .contradicting(let literals): contradicting = literals
			}

			// Which of the two to name. Never the newcomer: that one is what the caller just wrote, not
			//  the culprit, and citing it would tell them to retract what they just asked for — a
			//  retraction that, the write having been rolled back by the time they read it, no longer
			//  exists to be performed. The `assert` path hands its formula in and it is recognized by
			//  name; a raw SQL write is recognized by its row being younger than the write itself.
			//  Among what's left, prefer a live stored row, since that is the one they *can* retract.
			var eligible: [Formula] = []
			var stored: [Formula] = []
			for literal in contradicting where literal.formula != asserted {
				let id = try storedID(of: literal)
				if let id, let watermark, id > watermark { continue }
				eligible.append(literal.formula)
				if id != nil { stored.append(literal.formula) }
			}
			if let culprit = stored.first {
				throw CoherenceError.contradiction(contradicts: culprit, derivedFrom: stored)
			}

			// Both sides here are derived, so there is nothing to hand back to `retract`.
			//  Hold on to it, but keep scanning in case we find a more useful contradiction.
			//  The last fallback is for a write that stored *both* sides itself: nothing is eligible,
			//  but the contradiction is real and still has to be reported.
			unactionable = unactionable ?? eligible.first ?? contradicting.first?.formula
		}

		if let unactionable {
			throw CoherenceError.contradiction(contradicts: unactionable, derivedFrom: nil)
		}
		if let undecidable {
			throw CoherenceError.undecidable(relation: undecidable)
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

	/// What the check could establish about one relation.
	private enum Verdict {
		/// No ground literal of this relation holds in both polarities.
		case coherent

		/// One that does, given in each polarity.
		case contradicting([Literal])

		/// Neither polarity could be compared against the other, so nothing is claimed either way.
		case undecidable
	}

	/// Whether some ground literal of `name` holds in both polarities.
	///
	/// Two ways to ask, chosen by whether each side is finite. `INTERSECT` reads both relations end to
	/// end, so it is only available where both of them end: a *value-generating* side (arithmetic under
	/// recursion) streams out of a `WITH RECURSIVE` CTE with nothing to stop it, and the scan never
	/// finishes. Where exactly one side is finite, the question is asked the other way round — see
	/// `probingLiterals`.
	///
	/// - Parameter cones: The two sides' dependency cones, which the caller holds already. Whether a
	///   side is finite is a property of its cone, so nothing here has to walk the rule graph again.
	private func verdict(
		for name: String, columns: [String], cones: (positive: Set<String>, negative: Set<String>)
	) throws -> Verdict {
		func verdict(_ literals: [Literal]?) -> Verdict {
			literals.map(Verdict.contradicting) ?? .coherent
		}
		switch (try isFinite(cone: cones.positive), try isFinite(cone: cones.negative)) {
		case (true, true):
			return verdict(try intersectingLiterals(of: name, columns: columns))
		case (true, false):
			return verdict(try probingLiterals(enumerating: name, columns: columns))
		case (false, true):
			return verdict(try probingLiterals(enumerating: "-\(name)", columns: columns))
		case (false, false):
			// Both sides value-generating: neither can be enumerated, so there is no ground literal to
			//  probe the other with, and deciding it in general is deciding whether two Datalog-with-
			//  arithmetic relations intersect. Closing this properly needs symbolic reasoning over the
			//  two sides' rule heads rather than a query against either; until then the caller is told,
			//  and the write that would leave the relation this way is refused.
			return .undecidable
		}
	}

	/// The both-finite form: ask the two *relations*, not `_rule`, since a view is
	/// `baseFactsSelect UNION rules` and so intersecting them covers stored and derived rows alike.
	/// `INTERSECT` compares every column, which is exactly "some ground literal holds in both
	/// polarities".
	private func intersectingLiterals(of name: String, columns: [String]) throws -> [Literal]? {
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

		return [try literal(row, "positive"), try literal(row, "negative")]
	}

	/// The one-side-infinite form: enumerate the finite side and ask about each of its literals
	/// individually, rather than scanning the infinite one.
	///
	/// The switch is what makes the question answerable. A contradiction is always some *ground*
	/// literal, so where one polarity is finite, every candidate contradiction is one of the finitely
	/// many literals it holds — and asking the other polarity about a ground literal is a query with its
	/// arguments bound, which `RecursiveClosure` turns into a bound on the recursive step. `nat(99)`
	/// terminates for the same reason a user's `nat(99)` does, where `SELECT … FROM [nat]` does not.
	///
	/// Asking it as a *correlated* `EXISTS` over the finite side (`WHERE EXISTS (SELECT 1 FROM [nat]
	/// WHERE [nat].n = f.n)`) does not work and is not merely unimplemented: the constraint is then a
	/// column reference rather than a literal, and a recursive CTE is evaluated once, outside the
	/// correlation, so there is no per-row bound to inject. Ground literals, one at a time, is the form
	/// that carries a bound.
	private func probingLiterals(enumerating name: String, columns: [String]) throws -> [Literal]? {
		let columnList = columns.map { "[\($0)]" }.joined(separator: ", ")
		let enumerated = SQL(
			"SELECT predicate_formula(?, \(columnList)) AS known FROM [\(name)]",
			arguments: [name])

		for row in try query(sql: enumerated) {
			let known = try literal(row, "known")
			guard case .hornClause(let head, _, _) = known.formula else { continue }

			// The same arguments in the other polarity. Built as a `Formula` and asked through
			//  `query(formula:)` so the arguments lower to SQL *literals* — a bound parameter would be
			//  invisible to the bound derivation, and the probe would be the unbounded scan again.
			let probe = Formula.predicate(
				Predicate(name: head.inverse.name, arguments: head.arguments))
			guard try query(formula: probe).hasMoreRows else { continue }

			let derived = Literal(formula: probe, json: try formulaToJSON(probe))
			// Callers expect the pair positive-first.
			return head.isNegated ? [derived, known] : [known, derived]
		}
		return nil
	}

	/// Decodes a `predicate_formula` column into a `Literal`.
	private func literal(_ row: Row, _ key: String) throws -> Literal {
		guard let json = row[key] as? String, let data = json.data(using: .utf8) else {
			throw RBDBError.corruptData(
				message: "predicate_formula did not return valid UTF-8 JSON")
		}
		return Literal(formula: try JSONDecoder().decode(Formula.self, from: data), json: json)
	}

	/// The live `_rule` row this literal is stored as — something someone asserted, as opposed to a
	/// conclusion that merely follows, which has no row and so comes back `nil`.
	///
	/// The id doubles as *when*: `internal_entity_id` is allocated from `_entity`, which only ever
	/// grows, so a row younger than a watermark taken before a write is one that write stored.
	private func storedID(of literal: Literal) throws -> Int64? {
		try super.query(
			sql: """
				SELECT internal_entity_id AS id FROM _rule
				WHERE superceded_by IS NULL AND formula = jsonb(\(literal.json))
				LIMIT 1
				"""
		).makeIterator().next()?["id"] as? Int64
	}

	/// The youngest `_rule` row there is, as a mark to tell a subsequent write's rows apart from
	/// everything that stood before it. Zero when there are none: every id is greater.
	func latestRuleID() throws -> Int64 {
		try super.query(sql: "SELECT max(internal_entity_id) AS id FROM _rule")
			.makeIterator().next()?["id"] as? Int64 ?? 0
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
	/// popping, so the `RELEASE` is needed either way — and it is the whole of the job, not tidying: an
	/// unpopped savepoint that was the *outermost* one holds open the transaction it implicitly began,
	/// so the caller's next `BEGIN` fails with "cannot start a transaction within a transaction" and
	/// whatever they write in the meantime sits in a transaction nobody will commit.
	func endCoherenceSavepoint(rollingBack: Bool) throws {
		let name = RBDB.coherenceSavepoint
		let sql = (rollingBack ? "ROLLBACK TO \(name); " : "") + "RELEASE \(name)"
		try super.query(sql: SQL(sql))
	}
}
