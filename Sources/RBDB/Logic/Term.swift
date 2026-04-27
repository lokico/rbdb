public enum Expression: Codable, Comparable {
	case add(Term, Term)
	case subtract(Term, Term)
	case multiply(Term, Term)
	case divide(Term, Term)

	public var op: String {
		switch self {
		case .add: "+"
		case .subtract: "-"
		case .multiply: "*"
		case .divide: "/"
		}
	}
}

public enum Term: Symbol {
	case variable(Var)

	// constants
	case boolean(Bool)
	case number(Float)
	case string(String)

	// expressions
	indirect case expression(Expression)

	public var type: SymbolType {
		switch self {
		case .variable: .variable
		case .boolean, .number, .string: .constant
		case .expression: .expression
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
		case .expression(let expr):
			.expression(rewrite(expression: expr))
		default:
			term
		}
	}
	public func rewrite(variable: Var) -> Var { variable }
	public func rewrite(expression: Expression) -> Expression {
		switch expression {
		case .add(let lhs, let rhs):
			.add(rewrite(term: lhs), rewrite(term: rhs))
		case .subtract(let lhs, let rhs):
			.subtract(rewrite(term: lhs), rewrite(term: rhs))
		case .multiply(let lhs, let rhs):
			.multiply(rewrite(term: lhs), rewrite(term: rhs))
		case .divide(let lhs, let rhs):
			.divide(rewrite(term: lhs), rewrite(term: rhs))
		}
	}
}

extension SymbolReducer {
	public func reduce(_ prev: Result, _ term: Term) throws -> Result {
		switch term {
		case .variable(let v):
			return try reduce(prev, v)
		case .expression(let expr):
			return try reduce(prev, expr)
		default:
			return prev
		}
	}

	public func reduce(_ prev: Result, _ variable: Var) throws -> Result { prev }
	public func reduce(_ prev: Result, _ expression: Expression) throws -> Result {
		switch expression {
		case .add(let lhs, let rhs),
			.subtract(let lhs, let rhs),
			.multiply(let lhs, let rhs),
			.divide(let lhs, let rhs):
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
					Float.self,
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
			case .expression:
				self = .expression(try container.decode(Expression.self, forKey: key))
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
		case .expression(let expr): try container.encode(expr, forKey: .expression)
		}
	}
}
