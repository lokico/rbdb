import Testing

@testable import Datalog
@testable import RBDB

/// A datalog source is a *program*: a sequence of formulas, each marked with what to do with it —
/// `?` to ask it, `.` to assert it, `~` to retract it. An unmarked formula means whatever the caller
/// said it means, which is how the same text reads as a query on one surface and an assertion on
/// another.
@Suite("Datalog programs")
struct DatalogProgramTests {

	/// A ground one-argument fact, the shape most of these tests need.
	func fact(_ name: String, _ argument: Double) -> Formula {
		.hornClause(positive: Predicate(name: name, arguments: [.number(argument)]), negative: [])
	}

	// MARK: - Parsing

	@Test("each marker selects its step")
	func markersSelectStep() throws {
		let steps = try DatalogParser().parse("p(1). q(2)? r(3)~")
		#expect(
			steps == [
				.assert(fact("p", 1)),
				.query(fact("q", 2)),
				.retract(fact("r", 3)),
			])
	}

	@Test(
		"an unmarked formula takes the caller's default",
		arguments: DatalogStep.Kind.allCases)
	func unmarkedFormulaTakesDefault(_ step: DatalogStep.Kind) throws {
		let steps = try DatalogParser(defaultStepKind: step).parse("p(1)")
		#expect(steps.count == 1)
		#expect(steps[0].kind == step)
		#expect(steps[0].formula == fact("p", 1))
	}

	@Test("a marked formula keeps its marker whatever the default")
	func markerBeatsDefault() throws {
		let steps = try DatalogParser(defaultStepKind: .query).parse("p(1)~")
		#expect(steps.count == 1)
		#expect(steps[0].kind == .retract)
		#expect(steps[0].formula == fact("p", 1))
	}

	@Test("steps may be laid out over several lines, and rules mix with facts")
	func multiLineProgram() throws {
		let steps = try DatalogParser().parse(
			"""
			  parent(alice, bob).
			  grandparent(X, Z) :- parent(X, Y), parent(Y, Z).

			  grandparent(A, B)?
			""")
		#expect(steps.count == 3)
		guard steps[2].kind == .query else {
			Issue.record("the trailing `?` is a query even after a blank line; got \(steps)")
			return
		}
	}

	@Test("an empty program is an empty sequence of steps")
	func emptyProgram() throws {
		#expect(try DatalogParser().parse("   \n  ") == [])
	}

	// MARK: - Printing

	@Test(
		"a program round-trips through printing, whatever the default it is read back under",
		arguments: DatalogStep.Kind.allCases)
	func programRoundTrips(_ step: DatalogStep.Kind) throws {
		let parser = DatalogParser(defaultStepKind: step)
		let program: [DatalogStep] = [
			.assert(fact("p", 1)), .query(fact("q", 2)), .retract(fact("r", 3)),
		]
		#expect(try parser.parse(parser.print(program)) == program)
	}

	/// The default only says how to read a formula that came in unmarked; it never suppresses the
	/// marker on the way out, so printed text says what it means on any surface.
	@Test(
		"every step prints its marker, including the default one",
		arguments: DatalogStep.Kind.allCases)
	func everyStepPrintsItsMarker(_ step: DatalogStep.Kind) throws {
		let program: [DatalogStep] = [
			.assert(fact("p", 1)), .query(fact("p", 1)), .retract(fact("p", 1)),
		]
		#expect(
			String(try DatalogParser(defaultStepKind: step).print(program))
				== "p(1.0).\np(1.0)?\np(1.0)~")
	}

	@Test("a lone formula prints without any marker at all")
	func formulaPrintsBare() throws {
		let parser = DatalogParser()
		#expect(String(try parser.print(formula: parser.parse(formula: "p(1)"))) == "p(1.0)")
	}

	// MARK: - Running

	@Test("a program runs its steps in order and answers with its last query")
	func runsStepsInOrder() throws {
		let db = try RBDB(path: ":memory:")
		try db.query(sql: "CREATE TABLE p(a)")

		var steps: [DatalogOutcome] = []
		try db.run(
			datalog: """
				                    p(1).
				                    p(2).
				                    p(1)~
				                    p(A)?
				""", onStep: { steps.append($0) })
		#expect(steps.count == 4)

		if case let .answered(_, results) = steps.last! {
			#expect(results.compactMap { $0["A"] as? Int64 } == [2], "`p(1)` was retracted")
		} else {
			Issue.record("Last step was not an `.answered`")
		}
	}

	/// A caller that reports as it goes — the CLI does — needs each step's outcome as it happens, so
	/// that a step which throws doesn't take the record of the ones before it with it.
	@Test("each step's outcome is reported as it happens, up to one that throws")
	func reportsEachOutcome() throws {
		let db = try RBDB(path: ":memory:")
		try db.query(sql: "CREATE TABLE p(a)")

		var outcomes: [String] = []
		func record(_ outcome: DatalogOutcome) {
			switch outcome {
			case .asserted(let formula): outcomes.append("asserted \(formula.type.stringValue)")
			case .retracted(let formula): outcomes.append("retracted \(formula.type.stringValue)")
			case .answered(_, let results): outcomes.append("answered \(Array(results).count)")
			}
		}

		try db.run(datalog: "p(1). p(2). p(A)? p(1)~", onStep: record)
		#expect(outcomes == ["asserted @p", "asserted @p", "answered 2", "retracted @p"])

		// `p(1)` is gone, so retracting it again throws — after the assertion before it has run.
		outcomes = []
		#expect(throws: (any Error).self) {
			try db.run(datalog: "p(3). p(1)~ p(4).", onStep: record)
		}
		#expect(
			outcomes == ["asserted @p"],
			"the step that threw, and everything after, reported nothing")
	}

	@Test("`query(datalog:)` asks an unmarked formula, and `assert(datalog:)` asserts it")
	func conveniencesSetTheDefault() throws {
		let db = try RBDB(path: ":memory:")
		try db.query(sql: "CREATE TABLE p(a)")

		try db.assert(datalog: "p(1)")
		#expect(try db.query(datalog: "p(A)").compactMap { $0["A"] as? Int64 } == [1])

		// …and either surface still honours an explicit marker.
		try db.retract(datalog: "p(1)~")
		#expect(Array(try db.query(datalog: "p(A)?")).isEmpty)
	}
}
