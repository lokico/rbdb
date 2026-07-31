public enum SymbolType: Comparable {
	// terms
	case constant
	case variable
	case arithmetic

	public var isTerm: Bool {
		switch self {
		case .constant, .variable, .arithmetic: true
		default: false
		}
	}

	// formulas

	/// A Horn clause, identified by its head predicate's name — *including* any polarity marker, as
	/// `Predicate.name` carries it: `p` or `-p`, which are separate relations.
	case hornClause(headName: String)

	public var isFormula: Bool {
		switch self {
		case .hornClause: true
		default: false
		}
	}
}

extension SymbolType: Codable {
	public init(from decoder: Decoder) throws {
		let container = try decoder.singleValueContainer()
		let str = try container.decode(String.self)
		if let value = SymbolType(stringValue: str) {
			self = value
		} else {
			throw DecodingError.dataCorrupted(
				DecodingError.Context(
					codingPath: decoder.codingPath,
					debugDescription: "Invalid SymbolType string \(str)"))
		}
	}

	public func encode(to encoder: Encoder) throws {
		var container = encoder.singleValueContainer()
		try container.encode(stringValue)
	}
}

// Conforms to CodingKey because it's used as the key for Term
extension SymbolType: CodingKey {
	public var stringValue: String {
		switch self {
		case .constant: ""
		case .variable: "v"
		case .arithmetic: "x"
		case .hornClause(headName: let name): "@\(name)"
		}
	}
	public init?(stringValue: String) {
		if stringValue.first == "@" {
			self = .hornClause(headName: String(stringValue.dropFirst()))
		} else {
			switch stringValue {
			case "": self = .constant
			case "v": self = .variable
			case "x": self = .arithmetic
			default: return nil
			}
		}
	}

	public var intValue: Int? { nil }
	public init?(intValue: Int) { nil }
}
