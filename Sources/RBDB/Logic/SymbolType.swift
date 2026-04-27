public enum SymbolType: Comparable {
	// terms
	case constant
	case variable
	case expression

	public var isTerm: Bool {
		switch self {
		case .constant, .variable, .expression: true
		default: false
		}
	}

	// formulas
	case hornClause(positiveName: String)

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
		case .expression: "x"
		case .hornClause(positiveName: let name): "@\(name)"
		}
	}
	public init?(stringValue: String) {
		if stringValue.first == "@" {
			self = .hornClause(positiveName: String(stringValue.dropFirst()))
		} else {
			switch stringValue {
			case "": self = .constant
			case "v": self = .variable
			case "x": self = .expression
			default: return nil
			}
		}
	}

	public var intValue: Int? { nil }
	public init?(intValue: Int) { nil }
}
