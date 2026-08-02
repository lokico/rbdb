import Testing

@testable import CLI
@testable import RBDB

@Suite("Dot command handling")
struct DotCommandTests {

	@Test("parseLanguage recognizes sql spellings")
	func parseLanguageSQL() {
		#expect(parseLanguage("sql") == .sql)
		#expect(parseLanguage("s") == .sql)
		#expect(parseLanguage("SQL") == .sql)
	}

	@Test("parseLanguage recognizes datalog spellings")
	func parseLanguageDatalog() {
		#expect(parseLanguage("datalog") == .datalog(isQueryMode: false))
		#expect(parseLanguage("d") == .datalog(isQueryMode: false))
		#expect(parseLanguage("Datalog") == .datalog(isQueryMode: false))
	}

	@Test("parseLanguage rejects unknown languages")
	func parseLanguageUnknown() {
		#expect(parseLanguage("python") == nil)
	}

	@Test(
		".lang switches the current mode to the parsed language", arguments: [".lang", ".language"])
	func dotLangSwitchesMode(commandName: String) throws {
		let database = try RBDB(path: ":memory:")
		var mode: InputMode = .sql

		let outcome = handleDotCommand("\(commandName) datalog", database: database, mode: &mode)

		#expect(outcome == .handled)
		#expect(mode == .datalog(isQueryMode: false))
	}

	@Test(".lang accepts abbreviated language names")
	func dotLangAbbreviation() throws {
		let database = try RBDB(path: ":memory:")
		var mode: InputMode = .datalog(isQueryMode: false)

		let outcome = handleDotCommand(".lang s", database: database, mode: &mode)

		#expect(outcome == .handled)
		#expect(mode == .sql)
	}

	@Test(".lang with an unrecognized language leaves the mode unchanged")
	func dotLangUnknownLanguage() throws {
		let database = try RBDB(path: ":memory:")
		var mode: InputMode = .sql

		let outcome = handleDotCommand(".lang python", database: database, mode: &mode)

		#expect(outcome == .handled)
		#expect(mode == .sql)
	}

	@Test(".lang with no argument leaves the mode unchanged")
	func dotLangMissingArgument() throws {
		let database = try RBDB(path: ":memory:")
		var mode: InputMode = .sql

		let outcome = handleDotCommand(".lang", database: database, mode: &mode)

		#expect(outcome == .handled)
		#expect(mode == .sql)
	}

	@Test(".exit reports the exit outcome")
	func dotExit() throws {
		let database = try RBDB(path: ":memory:")
		var mode: InputMode = .sql

		let outcome = handleDotCommand(".exit", database: database, mode: &mode)

		#expect(outcome == .exit)
	}

	@Test("a non-dot command is left for the language executor")
	func nonDotCommand() throws {
		let database = try RBDB(path: ":memory:")
		var mode: InputMode = .sql

		let outcome = handleDotCommand("SELECT 1", database: database, mode: &mode)

		#expect(outcome == .notADotCommand)
		#expect(mode == .sql)
	}
}
