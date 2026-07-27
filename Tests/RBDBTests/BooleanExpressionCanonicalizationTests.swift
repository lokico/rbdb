import Foundation
import Testing

@testable import RBDB

// Ported from Brain's expression spec (`tests/FSharp/exprtests.fs`), the comparison subset of
//  ``Structural Equality`` / ``Numeric logic``. A `BooleanExpression` models a single comparison
//  (no boolean algebra, folding, or NaN reasoning — those F# cases don't apply), so what carries
//  over is operator canonicalization: order-reversed operators fold to their mirror, and the
//  symmetric operators are order-independent, so logically-equal comparisons become `==`.

@Suite("Boolean expression canonicalization")
struct BooleanExpressionCanonicalizationTests {

	private let a = Term.variable(Var(id: 0))
	private let b = Term.variable(Var(id: 1))

	// `greater or less than`: a > b ≡ b < a
	@Test("a > b canonicalizes to b < a")
	func greaterFoldsToLess() {
		#expect(BooleanExpression.greaterThan(a, b) == BooleanExpression.lessThan(b, a))
		let gt = BooleanExpression.greaterThan(a, b)
		#expect(gt.operation == .lt)  // `>` is never stored
		#expect(gt.lhs == b && gt.rhs == a)
	}

	@Test("a >= b canonicalizes to b <= a")
	func greaterOrEqualFoldsToLessOrEqual() {
		#expect(
			BooleanExpression.greaterThanOrEqual(a, b) == BooleanExpression.lessThanOrEqual(b, a))
		#expect(BooleanExpression.greaterThanOrEqual(a, b).operation == .le)
	}

	// `transitive string` / `transitive lambda`: the symmetric operators ignore operand order.
	@Test("= is symmetric (operands sorted)")
	func equalityIsSymmetric() {
		#expect(BooleanExpression.equal(a, b) == BooleanExpression.equal(b, a))
	}

	@Test("!= is symmetric (operands sorted)")
	func inequalityIsSymmetric() {
		#expect(BooleanExpression.notEqual(a, b) == BooleanExpression.notEqual(b, a))
	}

	// `non-transitive subtraction` analogue: the order operators are NOT symmetric.
	@Test("< and <= are order-sensitive")
	func orderOperatorsAreNotSymmetric() {
		#expect(BooleanExpression.lessThan(a, b) != BooleanExpression.lessThan(b, a))
		#expect(BooleanExpression.lessThanOrEqual(a, b) != BooleanExpression.lessThanOrEqual(b, a))
	}

	@Test("canonicalization applies through arithmetic operands")
	func foldsWithArithmeticOperands() {
		// (a + 1) > b  ≡  b < (a + 1)
		let lhs = Term.sum(a, .number(1))
		#expect(BooleanExpression.greaterThan(lhs, b) == BooleanExpression.lessThan(b, lhs))
	}

	@Test("construction is idempotent")
	func idempotent() {
		// Rebuilding an already-canonical comparison with identity operands is a no-op.
		let eq = BooleanExpression.equal(b, a)  // stored as (a = b)
		#expect(eq.mappingOperands { $0 } == eq)
		let gt = BooleanExpression.greaterThan(a, b)  // stored as (b < a)
		#expect(gt.mappingOperands { $0 } == gt)
	}

	@Test("JSON encoding mirrors ArithmeticExpression's {op: [operands]}")
	func jsonEncoding() throws {
		// `<` stores operands in place; a plain `{"<": [lhs, rhs]}` object.
		try assertJSON(
			BooleanExpression.lessThan(a, .number(5)), expect: #"{"<":[{"v":0},{"":5}]}"#)
		// `>` is folded to `<` with swapped operands before encoding.
		try assertJSON(BooleanExpression.greaterThan(a, b), expect: #"{"<":[{"v":1},{"v":0}]}"#)
		// `!=` sorts operands.
		try assertJSON(BooleanExpression.notEqual(b, a), expect: #"{"!=":[{"v":0},{"v":1}]}"#)
	}
}
