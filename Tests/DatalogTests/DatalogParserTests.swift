import Testing
@testable import Datalog
@testable import RBDB

@Test("Parse simple facts")
func parseSimpleFacts() throws {
	let parser = DatalogParser()

	// Test simple fact: parent(alice, bob).
	let result1 = try parser.parse(formula: "parent(alice, bob)")
	let expected1 = Formula.hornClause(
		positive: Predicate(name: "parent", arguments: [.string("alice"), .string("bob")]),
		negative: []
	)
	#expect(result1 == expected1)

	// Test fact with numbers: age(alice, 30)
	let result2 = try parser.parse(formula: "age(alice, 30)")
	let expected2 = Formula.hornClause(
		positive: Predicate(name: "age", arguments: [.string("alice"), .number(30.0)]),
		negative: []
	)
	#expect(result2 == expected2)

	// Test fact with quoted strings: name(1, "Alice Smith")
	let result3 = try parser.parse(formula: "name(1, \"Alice Smith\")")
	let expected3 = Formula.hornClause(
		positive: Predicate(name: "name", arguments: [.number(1.0), .string("Alice Smith")]),
		negative: []
	)
	#expect(result3 == expected3)
}

@Test("Parse facts with variables")
func parseFactsWithVariables() throws {
	let parser = DatalogParser()

	// Test fact with variable: person(X)
	let result = try parser.parse(formula: "person(X)")

	// Create expected formula with a variable and use canonicalize to compare
	let expectedVar = Var()
	let expected = Formula.hornClause(
		positive: Predicate(name: "person", arguments: [.variable(expectedVar)]),
		negative: []
	)

	// Use canonicalize to compare formulas with variables
	#expect(result.canonicalize() == expected.canonicalize())
}

@Test("Parse simple rules")
func parseSimpleRules() throws {
	let parser = DatalogParser()

	// Test rule: grandparent(X, Z) :- parent(X, Y), parent(Y, Z)
	let result = try parser.parse(formula: "grandparent(X, Z) :- parent(X, Y), parent(Y, Z)")

	// Create expected formula with variables and use canonicalize to compare
	let X = Var()
	let Y = Var()
	let Z = Var()
	let expected = Formula.hornClause(
		positive: Predicate(name: "grandparent", arguments: [.variable(X), .variable(Z)]),
		negative: [
			Predicate(name: "parent", arguments: [.variable(X), .variable(Y)]),
			Predicate(name: "parent", arguments: [.variable(Y), .variable(Z)]),
		]
	)

	// Use canonicalize to compare formulas with variables
	#expect(result.canonicalize() == expected.canonicalize())
}

@Test("Parse mixed rules with constants and variables")
func parseMixedRules() throws {
	let parser = DatalogParser()

	// Test rule: adult(X) :- age(X, 18)
	let result = try parser.parse(formula: "adult(X) :- age(X, 18)")

	// Create expected formula and use canonicalize to compare
	let X = Var()
	let expected = Formula.hornClause(
		positive: Predicate(name: "adult", arguments: [.variable(X)]),
		negative: [
			Predicate(name: "age", arguments: [.variable(X), .number(18.0)])
		]
	)

	// Use canonicalize to compare formulas with variables
	#expect(result.canonicalize() == expected.canonicalize())
}

@Test("Parse with periods and whitespace")
func parseWithPeriodsAndWhitespace() throws {
	let parser = DatalogParser()

	// Test with trailing period
	let result1 = try parser.parse("parent(alice, bob).")
	let expected = Formula.hornClause(
		positive: Predicate(name: "parent", arguments: [.string("alice"), .string("bob")]),
		negative: []
	)
	#expect(result1 == [.assert(expected)])

	// Test with extra whitespace
	let result2 = try parser.parse("  parent( alice , bob )  .  ")
	#expect(result2 == [.assert(expected)])

	// Test rule with whitespace and period
	let result3 = try parser.parse("  grandparent(X, Z) :- parent(X, Y) , parent(Y, Z)  .  ")

	// Create expected formula and use canonicalize to compare
	let X2 = Var()
	let Y2 = Var()
	let Z2 = Var()
	let expected3 = Formula.hornClause(
		positive: Predicate(name: "grandparent", arguments: [.variable(X2), .variable(Z2)]),
		negative: [
			Predicate(name: "parent", arguments: [.variable(X2), .variable(Y2)]),
			Predicate(name: "parent", arguments: [.variable(Y2), .variable(Z2)]),
		]
	)

	// Use canonicalize to compare formulas with variables
	guard result3.count == 1, result3[0].kind == .assert else {
		Issue.record("expected a single assertion, got \(result3)")
		return
	}
	#expect(result3[0].formula.canonicalize() == expected3.canonicalize())
}

@Test("Print simple facts")
func printSimpleFacts() throws {
	let parser = DatalogParser()

	// Test simple fact
	let fact1 = Formula.hornClause(
		positive: Predicate(name: "parent", arguments: [.string("alice"), .string("bob")]),
		negative: []
	)
	let printed1 = try parser.print(formula: fact1)
	#expect(printed1 == "parent(alice, bob)")

	// Test fact with numbers
	let fact2 = Formula.hornClause(
		positive: Predicate(name: "age", arguments: [.string("alice"), .number(30.0)]),
		negative: []
	)
	let printed2 = try parser.print(formula: fact2)
	#expect(printed2 == "age(alice, 30.0)")

	// Test fact with quoted strings
	let fact3 = Formula.hornClause(
		positive: Predicate(name: "name", arguments: [.number(1.0), .string("Alice Smith")]),
		negative: []
	)
	let printed3 = try parser.print(formula: fact3)
	#expect(printed3 == "name(1.0, \"Alice Smith\")")
}

@Test("Print rules")
func printRules() throws {
	let parser = DatalogParser()

	// Create variables for testing with specific IDs
	let X = Var(id: 23)  // X
	let Y = Var(id: 24)  // Y
	let Z = Var(id: 25)  // Z

	// Test simple rule
	let rule = Formula.hornClause(
		positive: Predicate(name: "grandparent", arguments: [.variable(X), .variable(Z)]),
		negative: [
			Predicate(name: "parent", arguments: [.variable(X), .variable(Y)]),
			Predicate(name: "parent", arguments: [.variable(Y), .variable(Z)]),
		]
	)

	let printed = try parser.print(formula: rule)
	// Variables with IDs 23, 24, 25 should print as X, Y, Z
	#expect(printed == "grandparent(X, Z) :- parent(X, Y), parent(Y, Z)")
}

@Test("Round-trip parsing and printing")
func roundTripParsingAndPrinting() throws {
	let parser = DatalogParser()

	// Test facts round-trip
	let factStrings = [
		"parent(alice, bob)",
		"age(alice, 30.0)",
		"name(1.0, \"Alice Smith\")",
	]

	for factString in factStrings {
		let parsed = try parser.parse(formula: factString)
		let printed = try parser.print(formula: parsed)
		let reparsed = try parser.parse(formula: printed)
		#expect(parsed == reparsed)
	}
}

@Test("Subtraction, division, and exponent lower/parse to canonical expressions")
func parseArithmeticLowerings() throws {
	let parser = DatalogParser()
	let v = Var()

	// `-` lowers to add with a negated operand: X - 1 → add(X, -1)
	let minus = try parser.parse(formula: "d(X - 1)")
	let minusExpected = Formula.hornClause(
		positive: Predicate(name: "d", arguments: [Term.difference(.variable(v), .number(1))]),
		negative: [])
	#expect(minus.canonicalize() == minusExpected.canonicalize())

	// `/` lowers to multiplication by an inverse: X / 2 → X * 0.5
	let divide = try parser.parse(formula: "d(X / 2)")
	let divideExpected = Formula.hornClause(
		positive: Predicate(name: "d", arguments: [Term.quotient(.variable(v), .number(2))]),
		negative: [])
	#expect(divide.canonicalize() == divideExpected.canonicalize())

	// `^` parses to a power (right-associative).
	let power = try parser.parse(formula: "d(X ^ 2)")
	let powerExpected = Formula.hornClause(
		positive: Predicate(name: "d", arguments: [Term.power(.variable(v), .number(2))]),
		negative: [])
	#expect(power.canonicalize() == powerExpected.canonicalize())

	// Mixed operators at one precedence level parse correctly: X - 1 + 3 → X + 2
	let mixed = try parser.parse(formula: "d(X - 1 + 3)")
	let mixedExpected = Formula.hornClause(
		positive: Predicate(name: "d", arguments: [Term.sum(.variable(v), .number(2))]),
		negative: [])
	#expect(mixed.canonicalize() == mixedExpected.canonicalize())
}

@Test("Arithmetic expressions round-trip through printing")
func arithmeticRoundTrip() throws {
	let parser = DatalogParser()
	for source in [
		"d(X - 1) :- b(X)",
		"d(X / 2) :- b(X)",
		"d(X ^ 2) :- b(X)",
		"d(2 ^ X ^ 3) :- b(X)",
		"d(X * Y + 1) :- b(X, Y)",
	] {
		let parsed = try parser.parse(formula: source)
		let printed = try parser.print(formula: parsed)
		let reparsed = try parser.parse(formula: printed)
		#expect(parsed == reparsed, "round-trip failed for \(source) (printed: \(printed))")
	}
}

@Test("Parse facts with single-quoted strings")
func parseFactsWithSingleQuotedStrings() throws {
	let parser = DatalogParser()

	// Test simple fact with single-quoted string: user('Alice')
	let result1 = try parser.parse(formula: "user('Alice')")
	let expected1 = Formula.hornClause(
		positive: Predicate(name: "user", arguments: [.string("Alice")]),
		negative: []
	)
	#expect(result1 == expected1)

	// Test fact with mixed quotes: name('Alice', "Smith")
	let result2 = try parser.parse(formula: "name('Alice', \"Smith\")")
	let expected2 = Formula.hornClause(
		positive: Predicate(name: "name", arguments: [.string("Alice"), .string("Smith")]),
		negative: []
	)
	#expect(result2 == expected2)

	// Test fact with only single quotes: person('John', 'Doe')
	let result3 = try parser.parse(formula: "person('John', 'Doe')")
	let expected3 = Formula.hornClause(
		positive: Predicate(name: "person", arguments: [.string("John"), .string("Doe")]),
		negative: []
	)
	#expect(result3 == expected3)
}

@Test("Parse a rule with a comparison guard")
func parseComparisonGuard() throws {
	let parser = DatalogParser()

	// brother(B, S) :- male(B), parent(X, B), parent(X, S), B != S
	let result = try parser.parse(
		formula:
			"brother(B, S) :- male(B), parent(X, B), parent(X, S), B != S")

	let B = Var()
	let S = Var()
	let X = Var()
	let expected = Formula.hornClause(
		positive: Predicate(name: "brother", arguments: [.variable(B), .variable(S)]),
		negative: [
			Predicate(name: "male", arguments: [.variable(B)]),
			Predicate(name: "parent", arguments: [.variable(X), .variable(B)]),
			Predicate(name: "parent", arguments: [.variable(X), .variable(S)]),
		],
		guards: [.notEqual(.variable(B), .variable(S))]
	)
	#expect(result.canonicalize() == expected.canonicalize())
}

@Test("Comparison operators parse to their canonical BooleanExpression")
func parseComparisonOperators() throws {
	let parser = DatalogParser()

	func guardOf(_ source: String) throws -> BooleanExpression? {
		guard case .hornClause(_, _, let guards) = try parser.parse(formula: source) else {
			return nil
		}
		return guards.first
	}

	// `>` / `>=` fold to `<` / `<=` with swapped operands; `=` / `!=` sort operands.
	#expect(try guardOf("q(X, Y) :- p(X, Y), X < Y")?.operation == .lt)
	#expect(try guardOf("q(X, Y) :- p(X, Y), X <= Y")?.operation == .le)
	#expect(try guardOf("q(X, Y) :- p(X, Y), X > Y")?.operation == .lt)  // folded
	#expect(try guardOf("q(X, Y) :- p(X, Y), X >= Y")?.operation == .le)  // folded
	#expect(try guardOf("q(X, Y) :- p(X, Y), X = Y")?.operation == .eq)
	#expect(try guardOf("q(X, Y) :- p(X, Y), X != Y")?.operation == .ne)

	// `X > Y` and `Y < X` produce the identical canonical guard.
	#expect(try guardOf("q(X, Y) :- p(X, Y), X > Y") == guardOf("q(X, Y) :- p(X, Y), Y < X"))
}

@Test("Comparison guards round-trip through printing")
func comparisonGuardRoundTrip() throws {
	let parser = DatalogParser()
	for source in [
		"q(X, Y) :- p(X, Y), X < Y",
		"q(X, Y) :- p(X, Y), X <= Y",
		"q(X, Y) :- p(X, Y), X = Y",
		"q(X, Y) :- p(X, Y), X != Y",
		"brother(B, S) :- male(B), parent(X, B), parent(X, S), B != S",
		"r(X, Y) :- p(X, Y), X + 1 < Y",
	] {
		let parsed = try parser.parse(formula: source)
		let printed = try parser.print(formula: parsed)
		let reparsed = try parser.parse(formula: printed)
		#expect(parsed == reparsed, "round-trip failed for \(source) (printed: \(printed))")
	}
}

/// `Formula.description` is what an error message shows, and errors are raised from RBDB, which
/// knows nothing of this parser. So there are two renderings of a formula in the package, and this is
/// what keeps them one language: whatever an error prints, the console can be handed straight back.
@Test("a formula's description is datalog, and parses back as itself")
func descriptionIsDatalog() throws {
	let parser = DatalogParser()
	for source in [
		"-cousin(mia, henry)",
		#"p("two words")"#,
		"p(1.0)",
		"cousin(X, Y) :- parent(Z, X), parent(W, Y), sibling(Z, W), X != Y",
		// Arithmetic is where the two renderings differ in *text*: a description parenthesizes an
		//  expression term (`(X + 1.0) < Y`) where the printer relies on precedence. Both read back the
		//  same, which is the property being pinned here.
		"r(X, Y) :- p(X, Y), X + 1 < Y",
	] {
		let parsed = try parser.parse(formula: source)
		#expect(
			try parser.parse(formula: parsed.description) == parsed,
			"\(parsed.description) did not parse back as itself")
	}

	// And where there is no expression to parenthesize, the two agree to the character.
	for source in [
		"-cousin(\"asdg b\", \"sakvwe c\")", #"p("two words")"#, "p(1.0)", "q(X) :- p(X), X != 5.0",
	] {
		let parsed = try parser.parse(formula: source)
		#expect(parsed.description == source)
		#expect(parsed.description == String(try parser.print(formula: parsed)))
	}
}
