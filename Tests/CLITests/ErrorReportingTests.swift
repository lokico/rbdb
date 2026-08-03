import Foundation
import Testing

@testable import CLI
@testable import RBDB

/// How an error reaches the console. Everything the engine raises describes itself (see
/// `ErrorDescriptionTests`), so there is nothing here that knows about any particular error — only
/// what a description, a reason and a suggestion look like once printed.
@Suite("Error reporting")
struct ErrorReportingTests {

	/// Stands in for a parsing-library error: it describes itself, but through `description` rather
	/// than `LocalizedError`, and it leads with a severity of its own.
	private struct Foreign: Error, CustomStringConvertible {
		var description: String { "error: unexpected input\n --> input:1:1" }
	}

	@Test("a described error prints its description, reason and suggestion, in that order")
	func localizedError() {
		let formula = Formula.predicate(Predicate(name: "p", arguments: [.number(1)]))
		let lines = errorLines(RetractionError.notFound(formula))

		#expect(lines.count == 3)
		#expect(lines.first == "Error: Nothing to retract: p(1.0)")
		#expect(lines.last == "Retract what it follows from instead.")
	}

	@Test("an error with nothing to add prints one line")
	func descriptionOnly() {
		#expect(
			errorLines(SQLiteError.queryError("no such table: cousin"))
				== ["Error: no such table: cousin"])
	}

	@Test("an error that leads with a severity of its own is not prefixed twice")
	func foreignError() {
		#expect(errorLines(Foreign()) == ["Error: unexpected input\n --> input:1:1"])
	}
}
