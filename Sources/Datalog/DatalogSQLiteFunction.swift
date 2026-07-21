import Foundation
import RBDB
import SQLite3

extension RBDB {
	/// Registers the `datalog(formula)` SQL function, which takes a JSONB-encoded formula
	/// (as stored in `_rule.formula`) and returns it rendered back as datalog text.
	public func registerDatalogFunction() throws {
		try registerFunction(name: "datalog", argumentCount: 1, datalogSQLiteFunction)
	}
}

/// SQLite scalar function implementing `datalog(formula)`: decodes a JSONB-encoded
/// `Formula` blob (as stored in `_rule.formula`) and prints it back as datalog text
/// using `DatalogParser`.
func datalogSQLiteFunction(
	context: OpaquePointer?,
	argc: Int32,
	argv: UnsafeMutablePointer<OpaquePointer?>?
) {
	guard argc == 1, let argv = argv, let value = argv[0] else {
		sqlite3_result_error(context, "datalog() requires exactly one argument", -1)
		return
	}

	guard sqlite3_value_type(value) == SQLITE_BLOB else {
		sqlite3_result_error(context, "datalog() argument must be a JSONB blob", -1)
		return
	}

	guard let db = sqlite3_context_db_handle(context) else {
		sqlite3_result_error(context, "datalog() could not access database connection", -1)
		return
	}

	// The blob is SQLite's binary JSONB encoding, not text JSON, so convert it with
	//  SQLite's own json() function before decoding.
	var statement: OpaquePointer?
	guard sqlite3_prepare_v2(db, "SELECT json(?)", -1, &statement, nil) == SQLITE_OK,
		let statement = statement
	else {
		sqlite3_result_error(context, "datalog() failed to prepare JSON conversion", -1)
		return
	}
	defer { sqlite3_finalize(statement) }

	sqlite3_bind_value(statement, 1, value)

	guard sqlite3_step(statement) == SQLITE_ROW,
		let jsonPtr = sqlite3_column_text(statement, 0)
	else {
		sqlite3_result_error(context, "datalog() failed to decode JSONB blob", -1)
		return
	}
	let jsonData = Data(String(cString: jsonPtr).utf8)

	let formula: Formula
	do {
		formula = try JSONDecoder().decode(Formula.self, from: jsonData)
	} catch {
		sqlite3_result_error(context, "datalog() failed to decode formula: \(error)", -1)
		return
	}

	let text: String
	do {
		text = String(try DatalogParser().print(formula))
	} catch {
		sqlite3_result_error(context, "datalog() failed to print formula: \(error)", -1)
		return
	}

	text.withCString { cString in
		sqlite3_result_text(
			context,
			cString,
			-1,
			unsafeBitCast(-1, to: sqlite3_destructor_type.self)
		)
	}
}
