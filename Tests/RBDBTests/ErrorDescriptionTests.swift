import Foundation
import Testing

@testable import RBDB

/// Everything thrown out of RBDB reaches a person eventually — through the console, or through
/// whatever a caller shows their own users. An `Error` with no description reaches them as the
/// compiler's dump of its associated values (`notFound(RBDB.Formula.hornClause(positive: …))`),
/// which is a stack trace pretending to be a sentence. So every error type carries `LocalizedError`
/// text, and the formulas inside them render in the surface syntax they were written in.
@Suite("Error descriptions")
struct ErrorDescriptionTests {
	private let mia = Term.string("mia")
	private let henry = Term.string("henry")
	private let x = Term.variable(Var(id: 23))
	private let y = Term.variable(Var(id: 24))

	private func description(of error: Error) -> String? {
		(error as? LocalizedError)?.errorDescription
	}

	// MARK: - The formulas inside them

	@Test("a ground literal renders as it was written")
	func groundLiteralDescription() {
		let fact = Formula.predicate(Predicate(name: "-cousin", arguments: [mia, henry]))
		#expect(fact.description == "-cousin(\"mia\", \"henry\")")
	}

	@Test("a string that isn't identifier-shaped keeps its quotes")
	func quotedStringDescription() {
		let fact = Formula.predicate(Predicate(name: "p", arguments: [.string("two words")]))
		#expect(fact.description == #"p("two words")"#)
	}

	@Test("a rule renders with its body and guards")
	func ruleDescription() {
		let rule = Formula.hornClause(
			positive: Predicate(name: "cousin", arguments: [x, y]),
			negative: [
				Predicate(name: "parent", arguments: [mia, x]),
				Predicate(name: "parent", arguments: [henry, y]),
			],
			guards: [.notEqual(x, y)])
		#expect(
			rule.description == "cousin(X, Y) :- parent(\"mia\", X), parent(\"henry\", Y), X != Y")
	}

	// MARK: - The errors themselves

	@Test("a retraction that matches nothing names the formula and why it isn't there")
	func retractionNotFound() throws {
		let formula = Formula.predicate(Predicate(name: "-cousin", arguments: [mia, henry]))
		let error = RetractionError.notFound(formula)
		#expect(description(of: error) == "Nothing to retract: -cousin(\"mia\", \"henry\")")
		#expect(
			error.failureReason?.contains("derivable") == true,
			"the interesting case is a formula that holds but was never asserted")
	}

	@Test("a contradiction names the culprit and how to resolve it")
	func contradiction() throws {
		let culprit = Formula.predicate(Predicate(name: "-p", arguments: [.number(1)]))
		let error = CoherenceError.contradiction(contradicts: culprit, derivedFrom: [culprit])
		#expect(description(of: error) == "Contradicts known value: -p(1.0)")
		#expect(error.recoverySuggestion == "Retract one or more of: -p(1.0)")

		// Nothing stored on either side: there is no retraction to offer, and inventing one is worse
		//  than saying nothing.
		let underivable = CoherenceError.contradiction(contradicts: culprit, derivedFrom: nil)
		#expect(underivable.recoverySuggestion == nil)
	}

	@Test("an undecidable relation says which one, and what made it undecidable")
	func undecidable() throws {
		let error = CoherenceError.undecidable(relation: "nat")
		#expect(description(of: error) == "Could not decide whether 'nat' contradicts itself")
	}

	@Test("a database held by someone else says so")
	func databaseInUse() throws {
		#expect(
			description(of: RBDBError.databaseInUse(path: "/tmp/x.db"))
				== "Database already in use: /tmp/x.db")
		#expect(
			description(of: RBDBError.corruptData(message: "expected a text output_type"))
				== "Corrupt data: expected a text output_type")
	}

	@Test("a SQLite error reads as the message SQLite gave, not as a wrapper around it")
	func sqliteErrors() throws {
		#expect(
			description(of: SQLiteError.queryError("no such table: cousin", index: nil))
				== "no such table: cousin")
		#expect(
			description(of: SQLiteError.couldNotOpenDatabase("unable to open database file"))
				== "Could not open database: unable to open database file")
		#expect(
			description(of: SQLiteError.couldNotRegisterFunction(name: "uuidv7"))
				== "Could not register SQL function 'uuidv7'")
		#expect(
			description(of: SQLiteError.queryParameterCount(expected: 2, got: 1))
				== "SQL has 2 parameter(s), but 1 argument(s) were given")
	}

	@Test("an unsafe-variable error is unchanged")
	func validationErrorStillReads() throws {
		#expect(
			description(of: ValidationError.unsafeVariables([Var(id: 23)]))
				== "Unsafe variables: X")
	}

	// MARK: - End to end

	@Test("the error a real failed retraction throws is legible")
	func retractionThroughTheDatabase() throws {
		let db = try RBDB(path: ":memory:")
		try db.query(sql: "CREATE TABLE p(a)")
		try db.query(sql: "CREATE TABLE q(a)")
		try db.assert(formula: Formula.predicate(Predicate(name: "q", arguments: [.number(1)])))
		// `p(1)` holds, but by derivation — there is no row of it to supersede.
		try db.assert(
			formula: .hornClause(
				positive: Predicate(name: "p", arguments: [x]),
				negative: [Predicate(name: "q", arguments: [x])]))

		let error = #expect(throws: RetractionError.self) {
			try db.retract(
				formula: Formula.predicate(Predicate(name: "p", arguments: [.number(1)])))
		}
		#expect(description(of: error!) == "Nothing to retract: p(1.0)")
	}
}
