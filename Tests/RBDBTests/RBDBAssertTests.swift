import Foundation
import Testing

@testable import RBDB

struct RBDBAssertTests {

	@Test("assert(formula:) should store rule in database")
	func assertFormulaStoresRule() async throws {
		let rbdb = try RBDB(path: ":memory:")

		try rbdb.query(sql: "CREATE TABLE user(name)")

		// Create a simple predicate formula: user("Alice")
		let formula = Formula.predicate(
			Predicate(
				name: "user",
				arguments: [Term.string("Alice")]
			))

		try rbdb.assert(formula: formula)

		let ruleResults = Array(try rbdb.query(sql: "SELECT name FROM user"))
		#expect(
			ruleResults.count == 1,
			"Should have one record stored in user table"
		)
		#expect(ruleResults[0]["name"] as? String == "Alice")
	}

	@Test("INSERT should assert fact")
	func createInsertSelectFlow() async throws {
		let rbdb = try RBDB(path: ":memory:")

		// Create a table with one column
		try rbdb.query(sql: "CREATE TABLE user(name)")

		// Insert a row into the table
		try rbdb.query(
			sql: SQL("INSERT INTO user(name) VALUES (?)", arguments: ["Alice"])
		)

		let ruleResults = Array(try rbdb.query(sql: "SELECT name FROM user"))
		#expect(
			ruleResults.count == 1,
			"Should have one record stored in user table"
		)
		#expect(ruleResults[0]["name"] as? String == "Alice")
	}

	@Test("Asserting the same fact twice is a no-op")
	func duplicateFactAssertIsNoop() async throws {
		let rbdb = try RBDB(path: ":memory:")
		try rbdb.query(sql: "CREATE TABLE user(name)")

		let alice = Formula.predicate(Predicate(name: "user", arguments: [.string("Alice")]))
		try rbdb.assert(formula: alice)
		try rbdb.assert(formula: alice)  // duplicate: should be ignored, not throw

		// A non-duplicate asserted alongside the duplicate must still be stored.
		try rbdb.assert(
			formula: .predicate(Predicate(name: "user", arguments: [.string("Bob")])))

		let users = Array(try rbdb.query(sql: "SELECT name FROM user"))
		#expect(users.count == 2, "Duplicate fact should not create a second row")
		let names = Set(users.compactMap { $0["name"] as? String })
		#expect(names == ["Alice", "Bob"], "Non-duplicate fact should still be stored")
	}

	@Test("Asserting the same rule twice is a no-op")
	func duplicateRuleAssertIsNoop() async throws {
		let rbdb = try RBDB(path: ":memory:")
		try rbdb.query(sql: "CREATE TABLE human(name)")
		try rbdb.query(sql: "CREATE TABLE mortal(name)")
		try rbdb.query(sql: "CREATE TABLE finite(name)")

		let X = Var()
		let mortalRule = Formula.hornClause(
			positive: Predicate(name: "mortal", arguments: [.variable(X)]),
			negative: [Predicate(name: "human", arguments: [.variable(X)])])
		try rbdb.assert(formula: mortalRule)
		try rbdb.assert(formula: mortalRule)  // duplicate: should be ignored, not throw

		// A non-duplicate rule asserted alongside the duplicate must still be stored.
		let Y = Var()
		let finiteRule = Formula.hornClause(
			positive: Predicate(name: "finite", arguments: [.variable(Y)]),
			negative: [Predicate(name: "mortal", arguments: [.variable(Y)])])
		try rbdb.assert(formula: finiteRule)

		let mortalRules = Array(
			try rbdb.query(sql: "SELECT * FROM _rule WHERE output_type = '@mortal'"))
		#expect(mortalRules.count == 1, "Duplicate rule should not create a second row")

		let finiteRules = Array(
			try rbdb.query(sql: "SELECT * FROM _rule WHERE output_type = '@finite'"))
		#expect(finiteRules.count == 1, "Non-duplicate rule should still be stored")
	}

	@Test("Multi-row INSERT with duplicates still stores the non-duplicate rows")
	func multiRowInsertSkipsOnlyDuplicates() async throws {
		let rbdb = try RBDB(path: ":memory:")
		try rbdb.query(sql: "CREATE TABLE user(name)")

		try rbdb.query(sql: "INSERT INTO user(name) VALUES ('Alice')")

		// A single multi-row INSERT mixing a duplicate ('Alice') with new rows ('Bob', 'Carol').
		try rbdb.query(
			sql: "INSERT INTO user(name) VALUES ('Alice'), ('Bob'), ('Carol')"
		)

		let users = Array(try rbdb.query(sql: "SELECT name FROM user"))
		#expect(users.count == 3, "Duplicate row in the batch should not create a second entry")
		let names = Set(users.compactMap { $0["name"] as? String })
		#expect(
			names == ["Alice", "Bob", "Carol"],
			"Non-duplicate rows in the batch should still be stored")
	}
}
