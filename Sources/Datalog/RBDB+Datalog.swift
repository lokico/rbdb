import RBDB

/// What one step of a datalog program did, reported as soon as it has done it.
public enum DatalogOutcome {
	case asserted(Formula)
	case retracted(Formula)
	case answered(Formula, SQLiteCursor)
}

extension RBDB {
	/// Runs a sequence of datalog formulas.
	///
	/// What is done with each formula depends on its suffix: `?` to ask it, `.` to assert it, `~` to retract it.
	/// If no suffix is given, a default behavior is executed.
	/// - Parameters:
	///   - datalog: The program (e.g., "user('Alice'). user(Name)?")
	///   - defaultStep: What an unmarked formula means.
	///   - onStep: Called with each step's outcome once that step has run, so a caller reporting as
	///     it goes still has everything up to a step that throws.
	/// - Throws: Parsing errors, or whatever a step throws. The steps before it have already run.
	public func run(
		datalog: String,
		default defaultStep: DatalogStep.Kind = .assert,
		onStep: (DatalogOutcome) throws -> Void = { _ in }
	) throws {
		for step in try DatalogParser(defaultStepKind: defaultStep).parse(datalog) {
			switch step.kind {
			case .assert:
				try assert(formula: step.formula)
				try onStep(.asserted(step.formula))
			case .retract:
				try retract(formula: step.formula)
				try onStep(.retracted(step.formula))
			case .query:
				try onStep(.answered(step.formula, query(formula: step.formula)))
			}
		}
	}

	/// Convenience method to query using datalog syntax
	/// - Parameter datalog: A datalog query string (e.g., "user(Name)")
	/// - Returns: A cursor with the query results
	/// - Throws: Parsing errors or query execution errors
	public func query(datalog: String) throws -> SQLiteCursor {
		let parser = DatalogParser()
		let formula = try parser.parse(
			formula: datalog.droppingSuffix(DatalogStep.Kind.query.rawValue))
		return try query(formula: formula)
	}

	/// Convenience method to assert a single formula using datalog syntax
	/// - Parameter datalog: A datalog assertion string (e.g., "user('Alice')")
	/// - Throws: Parsing errors or assertion errors
	public func assert(datalog: String) throws {
		let parser = DatalogParser()
		let formula = try parser.parse(
			formula: datalog.droppingSuffix(DatalogStep.Kind.assert.rawValue))
		try assert(formula: formula)
	}

	/// Convenience method to retract a single formula using datalog syntax
	/// - Parameter datalog: A datalog retraction string (e.g., "user('Alice')")
	/// - Throws: Parsing errors, or `RetractionError.notFound` if no live row matches
	public func retract(datalog: String) throws {
		let parser = DatalogParser()
		let formula = try parser.parse(
			formula: datalog.droppingSuffix(DatalogStep.Kind.retract.rawValue))
		try retract(formula: formula)
	}
}

extension String {
	func droppingSuffix(_ suffix: Character) -> Substring {
		self.last == suffix ? self.dropLast() : self[...]
	}
}
