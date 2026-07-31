import Foundation
import SQLite3

/// Encodes a formula to canonical JSON, as stored in `_rule.formula`.
func formulaToJSON(_ formula: Formula) throws -> String {
	let encoder = JSONEncoder()
	let canonicalFormula = formula.canonicalize()
	guard
		let jsonStr = String(
			data: try encoder.encode(canonicalFormula),
			encoding: .utf8
		)
	else {
		throw RBDBError.corruptData(
			message: "Failed to encode formula as UTF-8 JSON"
		)
	}
	return jsonStr
}

/// SQLite scalar function implementing `is_materializing()`: whether a fixpoint build is currently
/// running on this connection. The materialized predicates' `BEFORE INSERT` triggers use it to leave
/// the fixpoint's own `INSERT OR IGNORE` alone — diverting those into `_rule` would turn derived rows
/// into asserted base facts and the loop would never settle.
///
/// Registered with the `RBDB` instance as user data, and *without* `SQLITE_DETERMINISTIC`: the value
/// changes between the statements of a single build, which SQLite would otherwise be free to cache.
func isMaterializingSQLiteFunction(
	context: OpaquePointer?,
	argc: Int32,
	argv: UnsafeMutablePointer<OpaquePointer?>?
) {
	guard let userData = sqlite3_user_data(context) else {
		sqlite3_result_error(context, "is_materializing: no database handle", -1)
		return
	}
	let rbdb = Unmanaged<RBDB>.fromOpaque(userData).takeUnretainedValue()
	sqlite3_result_int(context, rbdb.materializingDepth > 0 ? 1 : 0)
}

/// SQLite scalar function implementing `predicate_formula(name, args…)`, which builds a predicate
/// formula from its arguments and returns it as canonical JSON (used by the insert triggers).
func predicateFormulaSQLiteFunction(
	context: OpaquePointer?,
	argc: Int32,
	argv: UnsafeMutablePointer<OpaquePointer?>?
) {
	guard argc >= 1, let argv = argv else {
		sqlite3_result_error(
			context,
			"predicate_formula() requires at least one argument",
			-1
		)
		return
	}

	// Get the predicate name (first argument)
	guard let predicateNamePtr = sqlite3_value_text(argv[0]) else {
		sqlite3_result_error(
			context,
			"predicate_formula() first argument must be a string",
			-1
		)
		return
	}
	let predicateName = String(cString: predicateNamePtr)

	// Convert remaining arguments to Terms
	var terms: [Term] = []
	for i in 1..<argc {
		let value = argv[Int(i)]
		let sqliteType = sqlite3_value_type(value)

		let term: Term
		switch sqliteType {
		case SQLITE_TEXT:
			let textPtr = sqlite3_value_text(value)
			let text = String(cString: textPtr!)
			term = .string(text)
		case SQLITE_INTEGER:
			let intValue = sqlite3_value_int64(value)
			term = .number(Double(intValue))
		case SQLITE_FLOAT:
			let floatValue = sqlite3_value_double(value)
			term = .number(Double(floatValue))
		case SQLITE_NULL:
			sqlite3_result_error(
				context,
				"predicate_formula() does not support NULL arguments",
				-1
			)
			return
		case SQLITE_BLOB:
			sqlite3_result_error(
				context,
				"predicate_formula() does not support BLOB arguments",
				-1
			)
			return
		default:
			sqlite3_result_error(
				context,
				"predicate_formula() unsupported argument type",
				-1
			)
			return
		}

		terms.append(term)
	}

	// Create the Formula
	let formula = Formula.predicate(Predicate(name: predicateName, arguments: terms))

	// Convert to JSON using the utility function
	do {
		let jsonStr = try formulaToJSON(formula)

		// Return the JSON string
		jsonStr.withCString { cString in
			sqlite3_result_text(
				context,
				cString,
				-1,
				unsafeBitCast(-1, to: sqlite3_destructor_type.self)
			)
		}
	} catch {
		sqlite3_result_error(context, "Failed to encode formula: \(error)", -1)
	}
}
