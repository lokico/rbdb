public enum Formula: Symbol {
	case hornClause(positive: Predicate, negative: [Predicate])

	public static func predicate(_ predicate: Predicate) -> Formula {
		.hornClause(positive: predicate, negative: [])
	}

	public var type: SymbolType {
		switch self {
		case .hornClause(positive: let positive, negative: _):
			.hornClause(positiveName: positive.name)
		}
	}

	public func rewrite<T: SymbolRewriter>(_ rewriter: T) -> Formula {
		rewriter.rewrite(formula: self)
	}

	public func reduce<T: SymbolReducer>(_ initialResult: T.Result, _ reducer: T) throws -> T.Result
	{
		try reducer.reduce(initialResult, self)
	}

	public func isRecursive<T: StringProtocol>(for predicateName: T) -> Bool {
		switch self {
		case .hornClause(positive: _, negative: let negatives):
			return negatives.contains { $0.name == predicateName }
		}
	}
}

extension SymbolRewriter {
	public func rewrite(formula: Formula) -> Formula {
		switch formula {
		case .hornClause(positive: let positive, negative: let negatives):
			.hornClause(
				positive: rewrite(predicate: positive),
				negative: negatives.map(rewrite(predicate:))
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
		case .hornClause(positive: let positive, negative: let negatives):
			try negatives.reduce(reduce(prev, positive), reduce)
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
		case .hornClause(let positiveName):
			let positive = Predicate(name: positiveName, arguments: try arr.decode([Term].self))
			var negatives: [Predicate] = []
			while !arr.isAtEnd {
				negatives.append(try arr.decode(Predicate.self))
			}
			self = .hornClause(positive: positive, negative: negatives)
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
		case .hornClause(positive: let positive, negative: let negatives):
			try arr.encode(positive.arguments)
			for negative in negatives {
				try arr.encode(negative)
			}
		}
	}
}
