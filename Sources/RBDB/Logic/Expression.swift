public protocol Expression: Codable, Comparable, CustomDebugStringConvertible, Sendable {
	associatedtype Op: CodingKey, RawRepresentable where Op.RawValue: Comparable
	var operation: Op { get }
	var operands: [Term] { get }
}

// allows us to have a common Codable implementation without making the expression initializer public
package protocol ExpressionInternal: Expression {
	/// Builds an expression from its (currently always binary) operands, applying whatever normalization
	/// the conforming type defines. Not public: for types whose normal form is established elsewhere —
	/// `ArithmeticExpression`, built through the `Term` factories — this can produce a non-canonical
	/// expression.
	init(operation: Op, lhs: Term, rhs: Term)
}

extension Expression {
	public var debugDescription: String {
		let operands = self.operands
		if operands.count == 2 {
			return "\(operands[0]) \(operation.rawValue) \(operands[1])"
		} else {
			return "\(operation)(\(operands.map(\.debugDescription).joined(separator: ", ")))"
		}
	}

	public static func < (lhs: Self, rhs: Self) -> Bool {
		if lhs.operation != rhs.operation { return lhs.operation.rawValue < rhs.operation.rawValue }
		return lhs.operands.lexicographicallyPrecedes(rhs.operands)
	}
}

extension ExpressionInternal {
	public func encode(to encoder: Encoder) throws {
		var c = encoder.container(keyedBy: Op.self)
		try c.encode(operands, forKey: operation)
	}

	public init(from decoder: Decoder) throws {
		let c = try decoder.container(keyedBy: Op.self)
		guard let op = c.allKeys.last else {
			throw DecodingError.dataCorrupted(
				DecodingError.Context(
					codingPath: decoder.codingPath,
					debugDescription: "No valid operation key found"
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
		self.init(operation: op, lhs: operands[0], rhs: operands[1])
	}
}
