import Foundation
import Testing

@testable import RBDB
@testable import Datalog

@Suite("Datalog Extension Tests")
struct DatalogExtensionTests {

	@Test("assert(datalog:) and query(datalog:) with README sample code")
	func readmeSampleCode() async throws {
		// IMPORTANT: This test uses the exact sample code from the README.
		// If this test fails due to an intentional breaking API change,
		// you MUST update the corresponding sample code in README.md as well.

		let db = try RBDB(path: ":memory:")

		// Create tables for our predicates
		try db.query(sql: "CREATE TABLE parent(parent, child)")
		try db.query(sql: "CREATE TABLE grandparent(grandparent, grandchild)")

		// Assert some facts using datalog syntax
		try db.assert(datalog: "parent('John', 'Mary')")
		try db.assert(datalog: "parent('Mary', 'Tom')")
		try db.assert(datalog: "parent('Bob', 'Alice')")

		// Define a rule: grandparent(X, Z) :- parent(X, Y), parent(Y, Z)
		try db.assert(datalog: "grandparent(X, Z) :- parent(X, Y), parent(Y, Z)")

		// Query back using SQL to verify the rule works
		let result = try db.query(sql: "SELECT * FROM grandparent")
		let rows = Array(result)

		#expect(rows.count == 1, "Should have exactly one grandparent relationship")
		#expect(rows[0]["grandparent"] as? String == "John", "Grandparent should be John")
		#expect(rows[0]["grandchild"] as? String == "Tom", "Grandchild should be Tom")
	}

	@Test("query(datalog:) basic functionality")
	func queryDatalogBasic() async throws {
		let db = try RBDB(path: ":memory:")

		try db.query(sql: "CREATE TABLE user(name)")
		try db.assert(datalog: "user('Alice')")
		try db.assert(datalog: "user('Bob')")

		// Query with variable
		let results = try db.query(datalog: "user(Name)")
		let rows = Array(results)

		#expect(rows.count == 2, "Should return two users")
		let names = rows.compactMap { $0["Name"] as? String }.sorted()
		#expect(names == ["Alice", "Bob"], "Should return Alice and Bob")
	}

	@Test("assert(datalog:) basic functionality")
	func assertDatalogBasic() async throws {
		let db = try RBDB(path: ":memory:")

		try db.query(sql: "CREATE TABLE user(name)")

		// Assert using datalog syntax
		try db.assert(datalog: "user('Charlie')")

		// Verify using SQL query
		let result = try db.query(sql: "SELECT * FROM user")
		let rows = Array(result)

		#expect(rows.count == 1, "Should have one user")
		#expect(rows[0]["name"] as? String == "Charlie", "Name should be Charlie")
	}

	@Test("assert(datalog:) with rule")
	func assertDatalogRule() async throws {
		let db = try RBDB(path: ":memory:")

		try db.query(sql: "CREATE TABLE human(name)")
		try db.query(sql: "CREATE TABLE mortal(name)")

		// Assert a fact
		try db.assert(datalog: "human('Socrates')")

		// Assert a rule: mortal(X) :- human(X)
		try db.assert(datalog: "mortal(X) :- human(X)")

		// Verify the rule works
		let result = try db.query(sql: "SELECT * FROM mortal")
		let rows = Array(result)

		#expect(rows.count == 1, "Should have one mortal")
		#expect(rows[0]["name"] as? String == "Socrates", "Mortal should be Socrates")
	}

	@Test("query(datalog:) with ground formula")
	func queryDatalogGround() async throws {
		let db = try RBDB(path: ":memory:")

		try db.query(sql: "CREATE TABLE user(name)")
		try db.assert(datalog: "user('Alice')")

		// Query for specific user (ground formula)
		let results = try db.query(datalog: "user('Alice')")
		let rows = Array(results)

		#expect(rows.count == 1, "Should return one row for existing user")
		#expect(rows[0]["sat"] as? Int64 == 1, "Should return sat=1 for ground query")

		// Query for non-existent user
		let noResults = try db.query(datalog: "user('Bob')")
		let noRows = Array(noResults)

		#expect(noRows.count == 0, "Should return no rows for non-existent user")
	}

	@Test("recursive natural number with arithmetic")
	func recursiveNaturalNumber() async throws {
		let db = try RBDB(path: ":memory:")

		try db.query(sql: "CREATE TABLE nat(n)")

		// Assert that 0 is a natural number
		try db.assert(datalog: "nat(0)")

		// Assert recursive rule: nat(X + 1) :- nat(X)
		// This defines that if X is a natural number, then X+1 is also a natural number
		try db.assert(datalog: "nat(X + 1) :- nat(X)")

		// Query for a large natural number (e.g., 100)
		// This should work through recursive inference: 0 -> 1 -> 2 -> ... -> 100
		let results = try db.query(datalog: "nat(100)")
		let rows = Array(results)

		#expect(rows.count == 1, "Should infer that 100 is a natural number")
		#expect(rows[0]["sat"] as? Int64 == 1, "Query should be satisfied")
	}

	@Test("recursive arithmetic terminates without spurious matches")
	func recursiveNaturalNumberMiss() async throws {
		let db = try RBDB(path: ":memory:")
		try db.query(sql: "CREATE TABLE evens(n)")
		try db.assert(datalog: "evens(0)")
		try db.assert(datalog: "evens(X + 2) :- evens(X)")

		let hit = Array(try db.query(datalog: "evens(50)"))
		#expect(hit.count == 1, "50 is even")

		let miss = Array(try db.query(datalog: "evens(51)"))
		#expect(miss.count == 0, "51 is not even")
	}

	@Test("arithmetic expressions in rule heads")
	func arithmeticInRuleHeads() async throws {
		let db = try RBDB(path: ":memory:")

		try db.query(sql: "CREATE TABLE base(n)")
		try db.query(sql: "CREATE TABLE doubled(n)")

		try db.assert(datalog: "base(5)")
		try db.assert(datalog: "base(10)")

		// Rule using an arithmetic expression in the head
		try db.assert(datalog: "doubled(X * 2) :- base(X)")

		let hit = Array(try db.query(datalog: "doubled(20)"))
		#expect(hit.count == 1, "doubled(20) should follow from base(10)")
		#expect(hit[0]["sat"] as? Int64 == 1)

		let miss = Array(try db.query(datalog: "doubled(7)"))
		#expect(miss.count == 0, "doubled(7) should not be derivable")
	}
}
