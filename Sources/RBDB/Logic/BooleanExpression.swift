import Foundation

/// A comparison over two `Term`s — a body constraint like `B != S` or `T < 10`. The boolean-valued
/// complement to `ArithmeticExpression`: where an arithmetic expression combines terms into a new
/// term, a boolean expression compares terms into a truth value.
///
/// Like `ArithmeticExpression`, a `BooleanExpression` is always kept in a canonical form, so two
/// logically-equal comparisons are also *structurally* equal (`==` performs semantic dedup):
///
/// - Only the four canonical operators (`<`, `<=`, `=`, `!=`) are representable. The order-reversed
///   operators are lowerings: `a > b` is built as `b < a`, `a >= b` as `b <= a` (compare
///   `Term.difference`/`quotient`, which lower to add/multiply). The only way to build a `>`/`>=`
///   comparison is through the `greaterThan`/`greaterThanOrEqual` factories.
/// - The symmetric operators (`=`, `!=`) sort their operands, so `a = b` and `b = a` are equal.
///
/// A boolean expression is used as a rule/query *guard*: it filters the tuples a body binds and never
/// binds new variables (every variable it mentions must already appear in a positive body literal —
/// see `Validate`), so it lowers to a SQL boolean condition rather than a joined table.
public struct BooleanExpression: ExpressionInternal {
	/// The canonical (representable) comparison operators. `>`/`>=` are deliberately absent — they are
	/// expressed by swapping operands (see the `greaterThan`/`greaterThanOrEqual` factories). Each
	/// raw value is also its SQL spelling, which SQLite accepts verbatim.
	public enum Op: String, CodingKey {
		case lt = "<"
		case le = "<="
		case eq = "="
		case ne = "!="

		/// Whether the comparison is unchanged by swapping its operands (`=`, `!=`).
		fileprivate var isSymmetric: Bool { self == .eq || self == .ne }
	}
	public let operation: Op

	package let lhs: Term
	package let rhs: Term
	public var operands: [Term] { [lhs, rhs] }

	// Not public: Don't restrict ourselves to binary operators in the public API
	package init(operation: Op, lhs: Term, rhs: Term) {
		self.operation = operation
		if operation.isSymmetric && rhs < lhs {
			self.lhs = rhs
			self.rhs = lhs
		} else {
			self.lhs = lhs
			self.rhs = rhs
		}
	}
}

// MARK: - Normalizing factories (the only public way to build a comparison)

extension BooleanExpression {
	public static func lessThan(_ lhs: Term, _ rhs: Term) -> BooleanExpression {
		BooleanExpression(operation: .lt, lhs: lhs, rhs: rhs)
	}
	public static func lessThanOrEqual(_ lhs: Term, _ rhs: Term) -> BooleanExpression {
		BooleanExpression(operation: .le, lhs: lhs, rhs: rhs)
	}
	public static func equal(_ lhs: Term, _ rhs: Term) -> BooleanExpression {
		BooleanExpression(operation: .eq, lhs: lhs, rhs: rhs)
	}
	public static func notEqual(_ lhs: Term, _ rhs: Term) -> BooleanExpression {
		BooleanExpression(operation: .ne, lhs: lhs, rhs: rhs)
	}
	// `a > b` ⟹ `b < a`, `a >= b` ⟹ `b <= a`: order-reversed comparisons are stored swapped.
	public static func greaterThan(_ lhs: Term, _ rhs: Term) -> BooleanExpression {
		BooleanExpression(operation: .lt, lhs: rhs, rhs: lhs)
	}
	public static func greaterThanOrEqual(_ lhs: Term, _ rhs: Term) -> BooleanExpression {
		BooleanExpression(operation: .le, lhs: rhs, rhs: lhs)
	}

	/// Rebuilds this comparison with each operand transformed, re-normalizing the result (an operand
	/// rename may change how the symmetric operators sort). Used by canonicalization / substitution.
	func mappingOperands(_ transform: (Term) -> Term) -> BooleanExpression {
		BooleanExpression(operation: operation, lhs: transform(lhs), rhs: transform(rhs))
	}
}
