import Foundation

/// A canonical arithmetic expression over `Term`s.
///
/// Expressions are always kept in a normal form ("sum of products of powers"): commutative
/// operands are sorted, associative chains are right-nested, constants are folded, and algebraic
/// identities (like-term collection, `aᵇ·aᶜ → aᵇ⁺ᶜ`, `x^0 → 1`, …) are applied. As a result,
/// two logically-equal expressions are also *structurally* equal, so `==` performs semantic dedup.
///
/// The raw union is `package`-visible so the parser, SQL lowering, and tests can inspect it, but
/// the only way to *build* an expression is through the normalizing factories on `Term`
/// (`Term.sum`, `Term.product`, `Term.power`, …). There is deliberately no subtract or divide
/// case — they lower to add/multiply/exponent.
public struct Expression: Comparable, CustomDebugStringConvertible {
	package enum Raw: Comparable {
		case add(Term, Term)
		case multiply(Term, Term)
		case exponent(Term, Term)
	}

	public enum Op: String, CodingKey {
		case add = "+"
		case multiply = "*"
		case exponent = "^"
	}

	package var raw: Raw

	public var operation: Op {
		switch raw {
		case .add: .add
		case .multiply: .multiply
		case .exponent: .exponent
		}
	}

	public var operands: [Term] {
		switch raw {
		// binary ops
		case .add(let t0, let t1),
			.multiply(let t0, let t1),
			.exponent(let t0, let t1):
			return [t0, t1]
		}
	}

	public var debugDescription: String {
		let operands = self.operands
		if operands.count == 2 {
			return "\(operands[0]) \(operation.rawValue) \(operands[1])"
		} else {
			return "\(operation)(\(operands.map(\.debugDescription).joined(separator: ", ")))"
		}
	}

	package init(_ raw: Raw) {
		self.raw = raw
	}

	// Not public because it's possible to create a non-canonical expression, e.g. Expression(.add, .number(1), .number(2))
	package init(_ op: Op, _ t1: Term, _ t2: Term) {
		switch op {
		case .add: self.raw = .add(t1, t2)
		case .multiply: self.raw = .multiply(t1, t2)
		case .exponent: self.raw = .exponent(t1, t2)
		}
	}

	// Comparable is never synthesized for a struct, so forward to the synthesized ordering on `raw`.
	public static func == (lhs: Expression, rhs: Expression) -> Bool { lhs.raw == rhs.raw }
	public static func < (lhs: Expression, rhs: Expression) -> Bool { lhs.raw < rhs.raw }
}

extension Expression: Codable {
	public init(from decoder: Decoder) throws {
		let c = try decoder.container(keyedBy: Op.self)
		guard let op = c.allKeys.last else {
			throw DecodingError.dataCorrupted(
				DecodingError.Context(
					codingPath: decoder.codingPath,
					debugDescription: "No valid expression operation key found"
				)
			)
		}

		let operands = try c.decode([Term].self, forKey: op)
		guard operands.count == 2 else {
			throw DecodingError.dataCorrupted(
				DecodingError.Context(
					codingPath: decoder.codingPath,
					debugDescription: "Invalid expression"
				)
			)
		}
		self = .init(op, operands[0], operands[1])
	}
	public func encode(to encoder: Encoder) throws {
		var c = encoder.container(keyedBy: Op.self)
		try c.encode(operands, forKey: operation)
	}
}

// MARK: - Normalizing factories (the only public way to build expressions)

extension Term {
	/// The canonical sum of the given terms. Collapses to a bare `Term` (e.g. `.number`) where the
	/// normal form has no remaining `+`.
	public static func sum(_ terms: [Term]) -> Term {
		var addends: [Term] = []
		for t in terms { flatten(t, .add, into: &addends) }

		// Split each addend into (coefficient, monomial) and collect like terms. A `nil` monomial
		//  means the addend is a pure numeric constant.
		var constants: [Double] = []
		var groups: [(monomial: Term, coefficients: [Double])] = []
		for addend in addends {
			let (coefficient, monomial) = splitCoefficient(addend)
			guard let monomial else {
				constants.append(coefficient)
				continue
			}
			if let i = groups.firstIndex(where: { $0.monomial == monomial }) {
				groups[i].coefficients.append(coefficient)
			} else {
				groups.append((monomial, [coefficient]))
			}
		}

		var result: [Term] = []
		for group in groups {
			for coefficient in foldFinite(group.coefficients, +) where coefficient != 0 {
				result.append(product(.number(coefficient), group.monomial))
			}
		}
		for constant in foldFinite(constants, +) where constant != 0 {
			result.append(.number(constant))
		}

		if result.isEmpty { return .number(0) }
		if result.count == 1 { return result[0] }
		return nest(result.sorted(), .add)
	}

	/// The canonical product of the given terms.
	public static func product(_ terms: [Term]) -> Term {
		var factors: [Term] = []
		for t in terms { flatten(t, .multiply, into: &factors) }
		if factors.contains(where: { numericValue($0) == 0 }) { return .number(0) }

		// Group factors by base, summing their exponents (`aᵇ·aᶜ → aᵇ⁺ᶜ`, `2·2ˣ → 2ˣ⁺¹`, …).
		var groups: [(base: Term, exponents: [Term])] = []
		for factor in factors {
			let (base, exponent) = splitPower(factor)
			if let i = groups.firstIndex(where: { $0.base == base }) {
				groups[i].exponents.append(exponent)
			} else {
				groups.append((base, [exponent]))
			}
		}

		// Fold numeric factors together; anything symbolic passes through. A literal `0` factor
		//  makes the whole product `0`.
		var constants: [Double] = []
		var result: [Term] = []
		for group in groups {
			let factor = power(group.base, sum(group.exponents))
			if let n = numericValue(factor) {
				if n == 0 { return .number(0) }
				if n != 1 { constants.append(n) }
			} else {
				result.append(factor)
			}
		}
		for constant in foldFinite(constants, *) where constant != 1 {
			if constant == 0 { return .number(0) }
			result.append(.number(constant))
		}

		if result.isEmpty { return .number(1) }
		if result.count == 1 { return result[0] }
		return nest(result.sorted(), .multiply)
	}

	/// The canonical power `base ^ exponent`.
	public static func power(_ base: Term, _ exponent: Term) -> Term {
		// (aᵇ)ᶜ → a^(b·c)
		if case .expression(let e) = base, case .exponent(let b, let bExp) = e.raw {
			return power(b, product(bExp, exponent))
		}
		if let e = numericValue(exponent) {
			if e == 0 { return .number(1) }  // x^0 → 1
			if e == 1 { return base }  // x^1 → x
			if let b = numericValue(base) {
				let r = Foundation.pow(b, e)
				if r.isFinite { return .number(r) }  // fold constants (skip non-finite, e.g. 0^-1)
			}
		}
		if let b = numericValue(base), b == 1 { return .number(1) }  // 1^x → 1
		return .expression(Expression(.exponent(base, exponent)))
	}

	// Variadic conveniences.
	public static func sum(_ terms: Term...) -> Term { sum(terms) }
	public static func product(_ terms: Term...) -> Term { product(terms) }

	// Lowerings — subtract, divide, and negation are all expressed via the above.
	public static func difference(_ a: Term, _ b: Term) -> Term { sum(a, negation(b)) }
	public static func quotient(_ a: Term, _ b: Term) -> Term { product(a, power(b, .number(-1))) }
	public static func negation(_ a: Term) -> Term { product(.number(-1), a) }
}

// MARK: - Normalization internals

/// Left-folds `values` with `combine`, but keeps operands separate whenever combining them would
/// produce a non-finite (`inf`/`nan`) result. This keeps folded constants out of stored rules and
/// SQL text while still simplifying the common finite case.
private func foldFinite(_ values: [Double], _ combine: (Double, Double) -> Double) -> [Double] {
	var residuals: [Double] = []
	var accumulator: Double? = nil
	for value in values {
		guard let acc = accumulator else {
			accumulator = value
			continue
		}
		let combined = combine(acc, value)
		if combined.isFinite {
			accumulator = combined
		} else {
			residuals.append(acc)
			accumulator = value
		}
	}
	if let acc = accumulator { residuals.append(acc) }
	return residuals
}

/// Flattens a same-operator chain (`add` or `multiply`) into its leaf operands.
private func flatten(_ term: Term, _ op: Expression.Op, into leaves: inout [Term]) {
	if case .expression(let e) = term, e.operation == op {
		for operand in e.operands {
			flatten(operand, op, into: &leaves)
		}
	} else {
		leaves.append(term)
	}
}

/// Splits an addend into `(coefficient, monomial)`; a `nil` monomial denotes a pure constant.
private func splitCoefficient(_ term: Term) -> (Double, Term?) {
	if let n = numericValue(term) { return (n, nil) }
	if case .expression(let e) = term, case .multiply = e.raw {
		var factors: [Term] = []
		flatten(term, .multiply, into: &factors)
		let numbers = factors.compactMap(numericValue)
		let others = factors.filter { numericValue($0) == nil }
		// Canonical products carry at most one numeric factor; anything else, keep the term whole.
		if numbers.count == 1, !others.isEmpty {
			return (numbers[0], Term.product(others))
		}
	}
	return (1, term)
}

/// Splits a factor into `(base, exponent)`; a bare term is treated as `term^1`.
private func splitPower(_ term: Term) -> (Term, Term) {
	if case .expression(let e) = term, case .exponent(let base, let exponent) = e.raw {
		return (base, exponent)
	}
	return (term, .number(1))
}

/// Right-nests a sorted, non-empty operand list into a canonical binary tree.
private func nest(_ sorted: [Term], _ op: Expression.Op) -> Term {
	var accumulator = sorted[sorted.count - 1]
	for term in sorted[..<(sorted.count - 1)].reversed() {
		accumulator = .expression(Expression(op, term, accumulator))
	}
	return accumulator
}

private func numericValue(_ term: Term) -> Double? {
	if case .number(let n) = term { return n }
	return nil
}
