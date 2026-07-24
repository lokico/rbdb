import Foundation

public enum ValidationError: Error, Equatable {
	case unsafeVariables([Var])

	public var localizedDescription: String {
		switch self {
		case .unsafeVariables(let variables):
			let varNames = variables.map { "var\($0.id ?? 255)" }.sorted().joined(separator: ", ")
			return
				"Unsafe variables in horn clause: \(varNames). Every variable in the head or in a comparison guard must also appear in a positive body literal."
		}
	}
}

struct UnsafeVariableCollector: SymbolReducer {
	func reduce(_ prev: [Var], _ formula: Formula) throws -> [Var] {
		var unsafeVariables = prev
		let collector = VariableCollector()

		switch formula {
		case .hornClause(positive: let positive, negative: let negatives, guards: let guards):
			let headVariables = try collector.reduce(Set(), positive)
			let bodyVariables = try negatives.reduce(
				Set(), { $0.union(try collector.reduce(Set(), $1)) })
			// A guard binds no variables — it only filters — so every variable it mentions must be
			//  range-restricted by a positive body literal, exactly like a head variable.
			let guardVariables = try guards.reduce(Set()) { variables, g in
				try g.operands.reduce(variables) { try collector.reduce($0, $1) }
			}

			// Check for unsafe variables in the head and in guards.
			for v in headVariables.union(guardVariables) {
				if !bodyVariables.contains(v) {
					unsafeVariables.append(v)
				}
			}
		}

		return unsafeVariables
	}
}

extension Symbol {
	/// Validates this `Symbol` and throws a `FormulaValidationError` if it fails validation.
	func validate() throws {
		let unsafeVariables = try reduce([], UnsafeVariableCollector())
		if !unsafeVariables.isEmpty {
			throw ValidationError.unsafeVariables(unsafeVariables)
		}
	}
}
