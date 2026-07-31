import Foundation

// Assert-time canonicalization of *rules* (Horn clauses with a body). Hooked into `assert(formula:)`
// before the formula is stored, it keeps the stored rule set free of tautologies and redundant or
// duplicate rules, and — because it may also remove already-stored rules — makes the canonical form
// order-independent (asserting the same rules in any order lands on the same stored set).
//
// The transforms are restricted to ones sound *independent of what facts may be asserted later*: a
// base fact for any predicate can appear at any future time, so we do no unfolding/inlining. See
// PLAN-HYBRID-EVAL.md. Facts (bodyless) need none of this and pass through untouched.
/// The outcome of assert-time rule canonicalization: the formula to store, plus the already-stored
/// rules it makes redundant. The redundant ones are *not* acted on here — they are superseded by
/// `assert` once the incoming rule has a row to point at. See `canonicalizeRuleForAssert`.
struct CanonicalizedRule {
	let store: Formula
	let supersedes: [Formula]

	init(store: Formula, supersedes: [Formula] = []) {
		self.store = store
		self.supersedes = supersedes
	}
}

extension RBDB {
	/// Applies the rule-level canonicalization transforms to a formula about to be asserted. Returns the
	/// formula to store together with the stored rules it subsumes, or `nil` if nothing should be stored
	/// (a tautology, or an incoming rule already subsumed by a stored one — in which case there is
	/// nothing to supersede either). Facts are returned unchanged.
	func canonicalizeRuleForAssert(_ formula: Formula) throws -> CanonicalizedRule? {
		guard case .hornClause(let head, let body, let guards) = formula.canonicalize(),
			!body.isEmpty || !guards.isEmpty
		else {
			// A fact — no rule-level canonicalization applies.
			return CanonicalizedRule(store: formula)
		}

		// 1. Intra-rule literal dedup: `p(X) :- q(X), q(X)` ⟹ `p(X) :- q(X)`. Canonicalization has
		//    sorted (but not deduped) both lists, so a repeated guard collapses the same way.
		var dedupedBody: [Predicate] = []
		for literal in body where !dedupedBody.contains(literal) { dedupedBody.append(literal) }
		var dedupedGuards: [BooleanExpression] = []
		for g in guards where !dedupedGuards.contains(g) { dedupedGuards.append(g) }

		// 2. Tautology drop: a head identical to one of its body literals derives nothing new.
		if dedupedBody.contains(head) { return nil }

		// 3. Subsumption. A rule with the same head and a *strictly smaller* body — fewer positive
		//    literals and/or fewer guards — is more general (weaker constraints ⟹ derives a superset),
		//    so it subsumes the one with the larger body. Compare in both directions.
		let incoming = (dedupedBody, dedupedGuards)
		var subsumed: [Formula] = []
		for stored in try fetchRules(for: head.name) {
			guard
				case .hornClause(let storedHead, let storedBody, let storedGuards) =
					stored.canonicalize(), storedHead == head
			else { continue }
			let existing = (storedBody, storedGuards)
			// A stored rule strictly more general than the incoming one ⟹ incoming is redundant.
			if bodySubsumes(existing, incoming) { return nil }
			// The incoming rule strictly more general than a stored one ⟹ that stored rule is redundant.
			if bodySubsumes(incoming, existing) { subsumed.append(stored) }
		}

		return CanonicalizedRule(
			store: .hornClause(positive: head, negative: dedupedBody, guards: dedupedGuards),
			supersedes: subsumed)
	}

	/// Whether body `general` strictly subsumes body `specific`: every literal *and* guard of `general`
	/// also appears in `specific`, and `specific` has at least one literal or guard `general` lacks
	/// (proper subset, treating each as a set). Both share the same canonical variable names, so `==`
	/// is a sound comparison. Fewer constraints ⟹ more general rule.
	private func bodySubsumes(
		_ general: ([Predicate], [BooleanExpression]),
		_ specific: ([Predicate], [BooleanExpression])
	) -> Bool {
		let literalsSubset = general.0.allSatisfy { specific.0.contains($0) }
		let guardsSubset = general.1.allSatisfy { specific.1.contains($0) }
		let proper =
			specific.0.contains { !general.0.contains($0) }
			|| specific.1.contains { !general.1.contains($0) }
		return literalsSubset && guardsSubset && proper
	}
}
