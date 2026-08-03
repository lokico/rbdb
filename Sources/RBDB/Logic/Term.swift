public enum Term: Symbol, CustomStringConvertible {
	case variable(Var)

	// constants
	case boolean(Bool)
	case number(Double)
	case string(String)

	// expressions
	indirect case arithmetic(ArithmeticExpression)

	public var type: SymbolType {
		switch self {
		case .variable: .variable
		case .boolean, .number, .string: .constant
		case .arithmetic: .arithmetic
		}
	}

	/// Datalog-style surface syntax for this term.
	public var description: String {
		switch self {
		case .variable(let v): v.description
		case .boolean(let v): v ? "true" : "false"
		case .number(let v): String(v)
		case .string(let v): "\"\(v)\""
		case .arithmetic(let expr): "(\(expr))"
		}
	}

	public func rewrite<T: SymbolRewriter>(_ rewriter: T) -> Term {
		rewriter.rewrite(term: self)
	}

	public func reduce<T: SymbolReducer>(_ initialResult: T.Result, _ reducer: T) throws -> T.Result
	{
		try reducer.reduce(initialResult, self)
	}
}

extension SymbolRewriter {
	public func rewrite(term: Term) -> Term {
		switch term {
		case .variable(let v):
			.variable(rewrite(variable: v))
		case .arithmetic(let expr):
			.arithmetic(rewrite(expression: expr))
		default:
			term
		}
	}
	public func rewrite(variable: Var) -> Var { variable }
	public func rewrite(expression: ArithmeticExpression) -> ArithmeticExpression {
		switch expression.raw {
		case .add(let lhs, let rhs):
			ArithmeticExpression(.add(rewrite(term: lhs), rewrite(term: rhs)))
		case .multiply(let lhs, let rhs):
			ArithmeticExpression(.multiply(rewrite(term: lhs), rewrite(term: rhs)))
		case .exponent(let lhs, let rhs):
			ArithmeticExpression(.exponent(rewrite(term: lhs), rewrite(term: rhs)))
		}
	}
}

extension Term {
	/// Variables appearing free in this term.
	public var freeVariables: Set<Var> {
		switch self {
		case .variable(let v): [v]
		case .boolean, .number, .string: []
		case .arithmetic(let expr):
			switch expr.raw {
			case .add(let l, let r), .multiply(let l, let r), .exponent(let l, let r):
				l.freeVariables.union(r.freeVariables)
			}
		}
	}

	/// Returns +1 if this term is monotonically non-decreasing in `variable`,
	/// -1 if non-increasing, 0 if non-monotonic / unknown / independent.
	/// Handles addition and multiplication by a numeric constant (e.g. `X + 1`, `X * 2`, `-X`).
	public func monotonicity(in variable: Var) -> Int {
		switch self {
		case .variable(let v): v == variable ? 1 : 0
		case .boolean, .number, .string: 0
		case .arithmetic(let expr):
			switch expr.raw {
			case .add(let l, let r):
				combineMonotonicity(l.monotonicity(in: variable), r.monotonicity(in: variable))
			case .multiply(let l, let r):
				// Scaling by a numeric constant preserves (positive) or flips (negative) monotonicity.
				if case .number(let n) = r {
					l.monotonicity(in: variable) * (n > 0 ? 1 : (n < 0 ? -1 : 0))
				} else if case .number(let n) = l {
					r.monotonicity(in: variable) * (n > 0 ? 1 : (n < 0 ? -1 : 0))
				} else {
					0
				}
			case .exponent: 0
			}
		}
	}

	/// Returns this term with every occurrence of `variable` replaced by `replacement`, rebuilt
	/// through the normalizing factories so the result is re-canonicalized (constants fold, etc.).
	/// Substituting a number into a purely-arithmetic term therefore collapses it to a `.number`.
	public func substituting(_ variable: Var, with replacement: Term) -> Term {
		switch self {
		case .variable(let v): return v == variable ? replacement : self
		case .boolean, .number, .string: return self
		case .arithmetic(let e):
			let sub = { (t: Term) in t.substituting(variable, with: replacement) }
			switch e.raw {
			case .add(let lhs, let rhs): return .sum(sub(lhs), sub(rhs))
			case .multiply(let lhs, let rhs): return .product(sub(lhs), sub(rhs))
			case .exponent(let lhs, let rhs): return .power(sub(lhs), sub(rhs))
			}
		}
	}
}

private func combineMonotonicity(_ a: Int, _ b: Int) -> Int {
	if a == 0 { return b }
	if b == 0 { return a }
	return a == b ? a : 0
}

extension SymbolReducer {
	public func reduce(_ prev: Result, _ term: Term) throws -> Result {
		switch term {
		case .variable(let v):
			return try reduce(prev, v)
		case .arithmetic(let expr):
			return try reduce(prev, expr)
		default:
			return prev
		}
	}

	public func reduce(_ prev: Result, _ variable: Var) throws -> Result { prev }
	public func reduce(_ prev: Result, _ expression: ArithmeticExpression) throws -> Result {
		switch expression.raw {
		case .add(let lhs, let rhs),
			.multiply(let lhs, let rhs),
			.exponent(let lhs, let rhs):
			let result = try reduce(prev, lhs)
			return try reduce(result, rhs)
		}
	}
}

extension Term: Codable {
	public init(from decoder: Decoder) throws {
		let container = try decoder.container(keyedBy: SymbolType.self)

		// Take the last key here because we want to prefer types added later to the SymbolType enum.
		//  This will allow us to add a new type while not breaking older clients by including a fallback.
		if let key = container.allKeys.sorted().last(where: { $0.isTerm }) {
			switch key {
			case .variable:
				self = .variable(
					Var(id: try container.decode(UInt8.self, forKey: key))
				)

			// This is a bit naughty, but it keeps the json very concise. We could store the expected constant
			//  data type in SymbolType, but that opens the door to data anomalies where the actual type of the
			//  value doesn't jibe with the declared type.
			case .constant:
				// Bool last because it's probably least frequently used type
				if let stringValue = try? container.decode(
					String.self,
					forKey: key
				) {
					self = .string(stringValue)
				} else if let floatValue = try? container.decode(
					Double.self,
					forKey: key
				) {
					self = .number(floatValue)
				} else if let boolValue = try? container.decode(
					Bool.self,
					forKey: key
				) {
					self = .boolean(boolValue)
				} else {
					throw DecodingError.dataCorrupted(
						DecodingError.Context(
							codingPath: decoder.codingPath,
							debugDescription: "Invalid constant value"
						)
					)
				}
			case .arithmetic:
				self = .arithmetic(try container.decode(ArithmeticExpression.self, forKey: key))
			default:
				// should never get here
				fatalError()
			}
		} else {
			throw DecodingError.dataCorrupted(
				DecodingError.Context(
					codingPath: decoder.codingPath,
					debugDescription: "No valid term symbol type key found"
				)
			)
		}
	}

	public func encode(to encoder: Encoder) throws {
		var container = encoder.container(keyedBy: SymbolType.self)
		switch self {
		case .variable(let v):
			guard let id = v.id else {
				throw EncodingError.invalidValue(
					v,
					EncodingError.Context(
						codingPath: encoder.codingPath,
						debugDescription: "Term must be canonicalized before encoding"
					)
				)
			}
			try container.encode(id, forKey: .variable)
		case .boolean(let value): try container.encode(value, forKey: .constant)
		case .number(let value): try container.encode(value, forKey: .constant)
		case .string(let value): try container.encode(value, forKey: .constant)
		case .arithmetic(let expr): try container.encode(expr, forKey: .arithmetic)
		}
	}
}
