class CanonicalizeRewriter: VariableMappingRewriter, SymbolRewriter {

	override func map(variable: Var) -> Var {
		Var(id: UInt8(variableMapping.count))
	}

	func rewrite(formula: Formula) -> Formula {
		switch formula {
		case .hornClause(positive: let positive, negative: let negatives):
			.hornClause(
				positive: rewrite(predicate: positive),
				negative: negatives.map(rewrite(predicate:)).sorted())
		}
	}

	// Variable renaming happens first-occurrence (left-to-right), which can invalidate an
	//  expression's construction-time operand sort (e.g. `add(Y, Z)` may rename to `add(B, A)`).
	//  So we rewrite operands left-to-right, then rebuild through the normalizing factories to
	//  re-sort under the renamed variables. `.variable` still routes through `rewrite(variable:)`
	//  to preserve the memoized renaming (matching the default `rewrite(term:)` witness).
	func rewrite(term: Term) -> Term {
		switch term {
		case .variable(let v):
			.variable(rewrite(variable: v))
		case .expression(let expr):
			switch expr.raw {
			case .add(let l, let r):
				Term.sum(rewrite(term: l), rewrite(term: r))
			case .multiply(let l, let r):
				Term.product(rewrite(term: l), rewrite(term: r))
			case .exponent(let l, let r):
				Term.power(rewrite(term: l), rewrite(term: r))
			}
		default:
			term
		}
	}
}

extension Symbol {
	public func canonicalize() -> Self {
		rewrite(CanonicalizeRewriter())
	}
}
