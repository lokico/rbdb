import Foundation

// Assert-time canonicalization of *rules* (Horn clauses with a body). Hooked into `assert(formula:)`
// before the formula is stored, it keeps the stored rule set free of tautologies and redundant or
// duplicate rules, and — because it may also remove already-stored rules — makes the canonical form
// order-independent (asserting the same rules in any order lands on the same stored set).
//
// The transforms are restricted to ones sound *independent of what facts may be asserted later*: a
// base fact for any predicate can appear at any future time, so we do no unfolding/inlining. See
// PLAN-HYBRID-EVAL.md. Facts (bodyless) need none of this and pass through untouched.
extension RBDB {
	/// Applies the rule-level canonicalization transforms to a formula about to be asserted, deleting
	/// any stored rules it subsumes (within the caller's transaction). Returns the formula to store, or
	/// `nil` if it should not be stored (a tautology, or already subsumed by a stored rule). Facts are
	/// returned unchanged.
	func canonicalizeRuleForAssert(_ formula: Formula) throws -> Formula? {
		guard case .hornClause(let head, let body) = formula.canonicalize(), !body.isEmpty else {
			return formula  // a fact — no rule-level canonicalization applies
		}

		// 1. Intra-rule literal dedup: `p(X) :- q(X), q(X)` ⟹ `p(X) :- q(X)`.
		var dedupedBody: [Predicate] = []
		for literal in body where !dedupedBody.contains(literal) { dedupedBody.append(literal) }

		// 2. Tautology drop: a head identical to one of its body literals derives nothing new.
		if dedupedBody.contains(head) { return nil }

		// 3. Subsumption. A rule with the same head and a *strictly smaller* body is more general (fewer
		//    constraints ⟹ derives a superset), so it subsumes the one with the larger body. Compare the
		//    incoming rule against every stored rule for this head, in both directions.
		var toDelete: [Formula] = []
		for stored in try fetchRules(for: head.name) {
			guard case .hornClause(let storedHead, let storedBody) = stored.canonicalize(),
				storedHead == head
			else { continue }
			// A stored rule strictly more general than the incoming one ⟹ incoming is redundant.
			if bodySubsumes(storedBody, dedupedBody) { return nil }
			// The incoming rule strictly more general than a stored one ⟹ that stored rule is redundant.
			if bodySubsumes(dedupedBody, storedBody) { toDelete.append(stored) }
		}
		for rule in toDelete {
			let json = try formulaToJSON(rule)
			try super.query(sql: "DELETE FROM _rule WHERE formula = jsonb(\(json))")
		}

		return .hornClause(positive: head, negative: dedupedBody)
	}

	/// Whether body `general` strictly subsumes body `specific`: every literal of `general` also
	/// appears in `specific`, and `specific` has at least one literal `general` lacks (proper subset,
	/// treating each as a set). Both share the same canonical variable names, so `==` on literals is a
	/// sound comparison. Fewer body literals ⟹ weaker guard ⟹ more general rule.
	private func bodySubsumes(_ general: [Predicate], _ specific: [Predicate]) -> Bool {
		general.allSatisfy { specific.contains($0) } && specific.contains { !general.contains($0) }
	}
}
