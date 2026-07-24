import Foundation
import Testing

@testable import RBDB

// Ported from Brain's CExpr spec (`tests/FSharp/exprtests.fs`, arithmetic subset). Each asserts
//  structural equality of canonical `Term`s built through the normalizing factories — logically
//  equal expressions must become `==`.

@Suite("Expression canonicalization")
struct ExpressionCanonicalizationTests {

	private let a = Term.variable(Var(id: 0))
	private let b = Term.variable(Var(id: 1))
	private let c = Term.variable(Var(id: 2))

	// MARK: CExpr spec

	@Test("transitive addition / multiplication")
	func transitivity() {
		#expect(Term.sum(a, b) == Term.sum(b, a))
		#expect(Term.product(a, b) == Term.product(b, a))
	}

	@Test("constant folding of grouped addition / multiplication")
	func groupingConstants() {
		#expect(Term.sum(Term.sum(.number(1), .number(2)), .number(3)) == .number(6))
		#expect(Term.sum(.number(1), Term.sum(.number(2), .number(3))) == .number(6))
		#expect(Term.product(Term.product(.number(1), .number(2)), .number(3)) == .number(6))
		#expect(Term.product(.number(1), Term.product(.number(2), .number(3))) == .number(6))
	}

	@Test("symbolic grouped addition / multiplication")
	func groupingVars() {
		#expect(Term.sum(Term.sum(a, b), c) == Term.sum(a, Term.sum(b, c)))
		#expect(Term.product(Term.product(a, b), c) == Term.product(a, Term.product(b, c)))
	}

	@Test("addition or multiplication")
	func additionOrMultiplication() {
		#expect(Term.sum(a, a, a) == Term.product(a, .number(3)))
	}

	@Test("multiplication or exponent")
	func multiplicationOrExponent() {
		#expect(Term.product(a, a, a) == Term.power(a, .number(3)))
	}

	@Test("(a^2)*a collapses to a^3")
	func multiplyExponent() {
		#expect(Term.product(Term.power(a, .number(2)), a) == Term.power(a, .number(3)))
	}

	@Test("(2^a)*2 collapses to 2^(a+1)")
	func multiplyExponentConstantBase() {
		#expect(
			Term.product(Term.power(.number(2), a), .number(2))
				== Term.power(.number(2), Term.sum(a, .number(1))))
	}

	@Test("multiplication adds exponents")
	func multiplicationAddsExponents() {
		#expect(
			Term.product(Term.power(a, b), Term.power(a, c)) == Term.power(a, Term.sum(b, c)))
	}

	@Test("exponent of exponent")
	func exponentOfExponent() {
		#expect(Term.power(Term.power(a, b), c) == Term.power(a, Term.product(b, c)))
	}

	@Test("folding addition")
	func foldingAddition() {
		#expect(Term.sum(a, .number(0)) == a)
	}

	@Test("folding multiplication by 0 / 1")
	func foldingMultiplication() {
		#expect(Term.product(a, .number(0)) == .number(0))
		#expect(Term.product(a, .number(1)) == a)
	}

	@Test("folding exponent")
	func foldingExponent() {
		#expect(Term.power(a, .number(1)) == a)
		#expect(Term.power(a, .number(0)) == .number(1))
	}

	@Test("constant folding")
	func constantFolding() {
		#expect(Term.sum(.number(1), .number(2)) == .number(3))
		#expect(Term.power(.number(2), .number(10)) == .number(1024))
	}

	@Test("non-commutative operations are NOT collapsed")
	func nonCommutative() {
		#expect(Term.difference(a, b) != Term.difference(b, a))
		#expect(Term.quotient(a, b) != Term.quotient(b, a))
	}

	// MARK: Beyond CExpr — like-term collection and lowerings

	@Test("like-term collection")
	func likeTerms() {
		#expect(
			Term.sum(Term.product(.number(2), a), Term.product(.number(3), a))
				== Term.product(.number(5), a))
	}

	@Test("subtraction lowers to addition of a negation")
	func subtractionLowering() {
		#expect(Term.difference(a, .number(2)) == Term.sum(a, .number(-2)))
		#expect(Term.difference(a, b) == Term.sum(a, Term.negation(b)))
	}

	@Test("division lowers to multiplication by an inverse power")
	func divisionLowering() {
		#expect(Term.quotient(a, .number(2)) == Term.product(a, .number(0.5)))
		#expect(Term.quotient(a, a) == .number(1))
	}

	@Test("non-finite folds are skipped")
	func nonFiniteFoldsSkipped() {
		let big = Term.number(1e308)
		let doubled = Term.sum(big, big)
		// The sum would overflow Double, so it must stay symbolic rather than fold to `inf`.
		guard case .arithmetic(let e) = doubled, case .add(let l, let r) = e.raw else {
			Issue.record("expected an unfolded add, got \(doubled)")
			return
		}
		#expect(numericIsFinite(l))
		#expect(numericIsFinite(r))
	}

	@Test("normalization is idempotent")
	func idempotent() {
		// Re-normalizing an already-canonical term is a no-op.
		let expr = Term.sum(Term.product(.number(2), a), b, Term.power(c, .number(2)))
		#expect(Term.sum(expr) == expr)
		#expect(Term.product(expr) == expr)
		// `canonicalize()` (which also renames variables) is idempotent under repetition.
		#expect(expr.canonicalize().canonicalize() == expr.canonicalize())
	}
}

private func numericIsFinite(_ term: Term) -> Bool {
	if case .number(let n) = term { return n.isFinite }
	return false
}
