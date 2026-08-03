import Foundation
import Testing

@testable import CLI
@testable import RBDB

@Suite("executeCommandsFromFile dot command handling")
struct ExecuteCommandsFromFileTests {

	private func writeTempFile(_ contents: String) throws -> String {
		let path = FileManager.default.temporaryDirectory
			.appendingPathComponent("rbdb-cli-test-\(UUID().uuidString).sql")
			.path
		try contents.write(toFile: path, atomically: false, encoding: .utf8)
		return path
	}

	private func predicateNames(_ database: RBDB) throws -> [String] {
		try Array(database.query(sql: "SELECT name FROM _predicate ORDER BY name"))
			.compactMap { $0["name"] as? String }
	}

	@Test("a .schema line prints the schema without erroring")
	func schemaLineIsHandled() async throws {
		let database = try RBDB(path: ":memory:")
		let path = try writeTempFile(
			"""
			CREATE TABLE foo (id INTEGER);
			.schema
			"""
		)

		let shouldContinue = try await executeCommandsFromFile(
			filePath: path, database: database, mode: .sql)

		#expect(shouldContinue == true)
		#expect(try predicateNames(database) == ["foo"])
	}

	@Test(".lang switches mode partway through the file")
	func langLineSwitchesMode() async throws {
		let database = try RBDB(path: ":memory:")
		let path = try writeTempFile(
			"""
			CREATE TABLE foo (id INTEGER);
			.lang datalog
			.lang sql
			CREATE TABLE bar (id INTEGER);
			"""
		)

		let shouldContinue = try await executeCommandsFromFile(
			filePath: path, database: database, mode: .sql)

		#expect(shouldContinue == true)
		#expect(try predicateNames(database) == ["bar", "foo"])
	}

	/// A datalog block is flushed to `executeCommand` as one program, so every formula in it has to
	/// be run as what its own marker says — an unmarked one must not take its meaning from whatever
	/// the *last* line of the block happens to end with.
	@Test("every step of a datalog block runs as itself")
	func datalogBlockRunsEveryStep() async throws {
		let database = try RBDB(path: ":memory:")
		let path = try writeTempFile(
			"""
			CREATE TABLE p (a);
			CREATE TABLE q (a);
			.lang datalog
			q(1).
			p(1)
			q(1)~
			"""
		)

		let shouldContinue = try await executeCommandsFromFile(
			filePath: path, database: database, mode: .sql)

		#expect(shouldContinue == true)
		#expect(
			try Array(database.query(datalog: "p(A)?")).count == 1,
			"the unmarked `p(1)` is an assertion, the block's trailing `~` notwithstanding")
		#expect(try Array(database.query(datalog: "q(A)?")).isEmpty, "and `q(1)~` retracted `q(1)`")
	}

	@Test(".exit stops processing the rest of the file and skips interactive mode")
	func exitLineStopsProcessing() async throws {
		let database = try RBDB(path: ":memory:")
		let path = try writeTempFile(
			"""
			CREATE TABLE foo (id INTEGER);
			.exit
			CREATE TABLE bar (id INTEGER);
			"""
		)

		let shouldContinue = try await executeCommandsFromFile(
			filePath: path, database: database, mode: .sql)

		#expect(shouldContinue == false)
		#expect(try predicateNames(database) == ["foo"])
	}
}
