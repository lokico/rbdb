public struct Predicate: Equatable, Comparable, Sendable {
	/// The relation this literal is about, *including* its polarity marker: `p` or `-p`. Polarity lives
	/// in the name because `p` and `-p` really are separate relations — every site that resolves a
	/// predicate to a table, a view, or an `output_type` should treat them as such, and does so with no
	/// special-casing. `getColumns` is the one exception: a negative predicate is not separately
	/// declared, so it borrows the positive form's columns.
	public let name: String
	public let arguments: [Term]

	/// Whether this is a *strongly negated* literal — `-p(…)`, "known false" (as opposed to the
	/// retraction of `p(…)`, which says only that `p` is no longer known true)
	public var isNegated: Bool { name.hasPrefix("-") }

	/// The name with any polarity marker removed.
	public var positiveName: String { isNegated ? String(name.dropFirst()) : name }

	/// This literal with its polarity flipped: the one whose derivability contradicts this one's.
	public var inverse: Predicate {
		Predicate(name: isNegated ? positiveName : "-\(name)", arguments: arguments)
	}

	public init(name: String, arguments: [Term]) {
		self.name = name.lowercased()
		self.arguments = arguments
	}

	public static func < (lhs: Predicate, rhs: Predicate) -> Bool {
		if lhs.name != rhs.name {
			return lhs.name < rhs.name
		}
		return lhs.arguments.lexicographicallyPrecedes(rhs.arguments)
	}
}

extension Predicate: CustomStringConvertible {
	/// Datalog-style surface syntax for this literal.
	public var description: String {
		"\(name)(\(arguments.map(\.description).joined(separator: ", ")))"
	}
}

extension Predicate: Codable {
	public func encode(to encoder: Encoder) throws {
		var container = encoder.unkeyedContainer()
		try container.encode(name)
		for arg in arguments {
			try container.encode(arg)
		}
	}

	public init(from decoder: Decoder) throws {
		var container = try decoder.unkeyedContainer()
		let name = try container.decode(String.self)
		var args: [Term] = []
		while !container.isAtEnd {
			let term = try container.decode(Term.self)
			args.append(term)
		}
		self.init(name: name, arguments: args)
	}
}
