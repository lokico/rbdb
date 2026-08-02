public enum Formula: Symbol {
	case hornClause(positive: Predicate, negative: [Predicate], guards: [BooleanExpression])

	/// Convenience for the common guard-free horn clause.
	public static func hornClause(positive: Predicate, negative: [Predicate]) -> Formula {
		.hornClause(positive: positive, negative: negative, guards: [])
	}

	public static func predicate(_ predicate: Predicate) -> Formula {
		.hornClause(positive: predicate, negative: [], guards: [])
	}

	public var type: SymbolType {
		switch self {
		case .hornClause(positive: let positive, negative: _, guards: _):
			.hornClause(headName: positive.name)
		}
	}

	public func rewrite<T: SymbolRewriter>(_ rewriter: T) -> Formula {
		rewriter.rewrite(formula: self)
	}

	public func reduce<T: SymbolReducer>(_ initialResult: T.Result, _ reducer: T) throws -> T.Result
	{
		try reducer.reduce(initialResult, self)
	}

	public func isRecursive(for predicateName: String) -> Bool {
		switch self {
		case .hornClause(positive: _, negative: let negatives, guards: _):
			return negatives.contains { $0.name == predicateName }
		}
	}

	/// This clause with further guards conjoined to its body. Used to constrain a rule *before* it is
	/// lowered — a derived bound is a filter on the rule like any the caller wrote, so it belongs in the
	/// clause rather than being concatenated onto the SQL the clause produced.
	public func adding(guards additional: [BooleanExpression]) -> Formula {
		guard !additional.isEmpty else { return self }
		switch self {
		case .hornClause(positive: let positive, negative: let negatives, guards: let guards):
			return .hornClause(
				positive: positive, negative: negatives, guards: guards + additional)
		}
	}
}

extension SymbolRewriter {
	public func rewrite(formula: Formula) -> Formula {
		switch formula {
		case .hornClause(positive: let positive, negative: let negatives, guards: let guards):
			.hornClause(
				positive: rewrite(predicate: positive),
				negative: negatives.map(rewrite(predicate:)),
				guards: guards.map { $0.mappingOperands(rewrite(term:)) }
			)
		}
	}

	public func rewrite(predicate: Predicate) -> Predicate {
		Predicate(name: predicate.name, arguments: predicate.arguments.map(rewrite(term:)))
	}
}

extension SymbolReducer {
	public func reduce(_ prev: Result, _ formula: Formula) throws -> Result {
		switch formula {
		case .hornClause(positive: let positive, negative: let negatives, guards: let guards):
			var result = try negatives.reduce(reduce(prev, positive), reduce)
			for g in guards {
				result = try reduce(reduce(result, g.lhs), g.rhs)
			}
			return result
		}
	}

	public func reduce(_ prev: Result, _ predicate: Predicate) throws -> Result {
		try predicate.arguments.reduce(prev, reduce)
	}
}

extension Formula: Codable {
	public init(from decoder: Decoder) throws {
		var arr = try decoder.unkeyedContainer()
		let key = try arr.decode(SymbolType.self)

		switch key {
		case .hornClause(let name):
			let positive = Predicate(name: name, arguments: try arr.decode([Term].self))
			var negatives: [Predicate] = []
			var guards: [BooleanExpression] = []
			// Body entries are heterogeneous: a predicate is an unkeyed array, a guard the keyed
			//  object `{op: [lhs, rhs]}`. `BodyEntry` tells them apart by container shape.
			while !arr.isAtEnd {
				switch try arr.decode(BodyEntry.self) {
				case .predicate(let p): negatives.append(p)
				case .guard(let g): guards.append(g)
				}
			}
			self = .hornClause(positive: positive, negative: negatives, guards: guards)
		default:
			throw DecodingError.dataCorrupted(
				DecodingError.Context(
					codingPath: decoder.codingPath,
					debugDescription: "No valid formula symbol type key found"
				)
			)
		}
	}

	public func encode(to encoder: Encoder) throws {
		var arr = encoder.unkeyedContainer()
		try arr.encode(type)
		switch self {
		case .hornClause(positive: let positive, negative: let negatives, guards: let guards):
			try arr.encode(positive.arguments)
			for negative in negatives {
				try arr.encode(negative)
			}
			for g in guards {
				try arr.encode(g)
			}
		}
	}
}

/// A single body entry when decoding a formula: a positive literal (unkeyed array) or a comparison
/// guard (keyed object). The two use disjoint JSON container kinds, so the container itself decides
/// which this is — and once an entry is known to be a guard, its own decoding error is reported
/// rather than the confusing one from a fallthrough to the predicate decoder.
private enum BodyEntry: Decodable {
	case predicate(Predicate)
	case `guard`(BooleanExpression)

	init(from decoder: Decoder) throws {
		if let container = try? decoder.container(keyedBy: BooleanExpression.Op.self),
			!container.allKeys.isEmpty
		{
			self = .guard(try BooleanExpression(from: decoder))
		} else {
			self = .predicate(try Predicate(from: decoder))
		}
	}
}
