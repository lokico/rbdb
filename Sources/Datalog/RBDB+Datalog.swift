import RBDB

extension RBDB {
	/// Convenience method to query using datalog syntax
	/// - Parameter datalog: A datalog query string (e.g., "user(Name)")
	/// - Returns: A cursor with the query results
	/// - Throws: Parsing errors or query execution errors
	public func query(datalog: String) throws -> SQLiteCursor {
		let parser = DatalogParser()
		let formula = try parser.parse(datalog)
		return try query(formula: formula)
	}

	/// Convenience method to assert using datalog syntax
	/// - Parameter datalog: A datalog assertion string (e.g., "user('Alice')")
	/// - Throws: Parsing errors or assertion errors
	public func assert(datalog: String) throws {
		let parser = DatalogParser()
		let formula = try parser.parse(datalog)
		try assert(formula: formula)
	}

	/// Convenience method to retract using datalog syntax
	/// - Parameter datalog: A datalog assertion string (e.g., "user('Alice')")
	/// - Throws: Parsing errors, or `RetractionError.notFound` if no live row matches
	public func retract(datalog: String) throws {
		let parser = DatalogParser()
		let formula = try parser.parse(datalog)
		try retract(formula: formula)
	}
}
