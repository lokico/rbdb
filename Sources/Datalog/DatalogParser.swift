import RBDB
import Parsing

enum ParsingError: Error {
	case conversionError
	case expected(String)
	case cannotPrintUnparseable(String)
}

/// Parser for Datalog syntax into RBDB Formula objects.
public struct DatalogParser: ParserPrinter {

	private class Context {
		private var variables: [String: Var] = [:]

		func getVariable(name: String) -> Var {
			if let existingVar = variables[name] {
				return existingVar
			}
			let newVar = Var(name)
			variables[name] = newVar
			return newVar
		}
	}
	private let ctx = Context()

	public init() {}

	public var body: some ParserPrinter<Substring, Formula> {
		ParsePrint {
			Whitespace()
			hornClauseParser
			Whitespace()
		}
	}
}

// MARK: - Horn Clause Parser

extension DatalogParser {
	private var hornClauseParser: some ParserPrinter<Substring, Formula> {
		let emptyBody: [BodyItem] = []
		return ParsePrint(HornClauseConversion()) {
			// Head
			predicateParser

			Whitespace()

			Optionally {
				":-".printing(" :- ")
				Whitespace()
				Many {
					bodyItemParser
					Whitespace()
				} separator: {
					","
					Whitespace().printing(" ".utf8)
				}
			}.map(.orDefault(emptyBody))

			// Optional period at the end
			".".replaceError(with: ())
		}
	}

	// A body item is either a positive literal or a comparison guard. Predicate is tried first (the
	//  common case); it fails fast on a guard because a guard has no `(` after its leading term, so the
	//  parser backtracks to `comparisonParser`.
	private var bodyItemParser: some ParserPrinter<Substring, BodyItem> {
		OneOf {
			predicateParser.map(.case(BodyItem.predicate))
			comparisonParser.map(.case(BodyItem.comparison))
		}
	}
}

/// A parsed rule-body element, before it is split into the positive literals and guards a `Formula`
/// stores separately.
enum BodyItem: Equatable {
	case predicate(Predicate)
	case comparison(BooleanExpression)
}

/// Assembles a head predicate plus a flat list of body items into a `Formula`, and back. Printing
/// emits the positive literals first, then the guards — the canonical body order.
private struct HornClauseConversion: Conversion {
	func apply(_ input: (Predicate, [BodyItem])) throws -> Formula {
		var predicates: [Predicate] = []
		var guards: [BooleanExpression] = []
		for item in input.1 {
			switch item {
			case .predicate(let p): predicates.append(p)
			case .comparison(let g): guards.append(g)
			}
		}
		return .hornClause(positive: input.0, negative: predicates, guards: guards)
	}

	func unapply(_ output: Formula) throws -> (Predicate, [BodyItem]) {
		guard case .hornClause(let head, let negatives, let guards) = output else {
			throw ParsingError.conversionError
		}
		return (head, negatives.map(BodyItem.predicate) + guards.map(BodyItem.comparison))
	}
}

// MARK: - Predicate Parser

extension DatalogParser {
	private var predicateParser: some ParserPrinter<Substring, Predicate> {
		ParsePrint(.memberwise(Predicate.init(name:arguments:))) {
			// Parse predicate name (identifier)
			// (initial hyphen is for strong negation)
			IdentifierParser(allowInitialHyphen: true)

			// Parse arguments in parentheses
			"("
			Whitespace()

			// Parse comma-separated terms, or empty
			Many {
				termParser
				Whitespace()
			} separator: {
				","
				Whitespace().printing(" ".utf8)
			}

			")"
		}
	}
}

// MARK: - Comparison Guard Parser

extension DatalogParser {
	// A comparison guard: `term op term`, e.g. `B != S` or `T + 1 < 10`.
	private var comparisonParser: some ParserPrinter<Substring, BooleanExpression> {
		ParsePrint(ComparisonFold()) {
			termParser
			Whitespace().printing(" ".utf8)
			comparisonOpParser
			Whitespace().printing(" ".utf8)
			termParser
		}
	}

	// Longer operators first, so `<=`/`>=`/`!=` aren't mis-read as `<`/`>`/`=`.
	private var comparisonOpParser: some ParserPrinter<Substring, CompareOp> {
		OneOf {
			"<=".map { .le }
			">=".map { .ge }
			"!=".map { .ne }
			"<".map { .lt }
			">".map { .gt }
			"=".map { .eq }
		}
	}
}

// The surface comparison operators, including the order-reversed `>`/`>=` that `BooleanExpression`
//  itself doesn't store (it folds them to `<`/`<=`).
private enum CompareOp { case lt, le, gt, ge, eq, ne }

/// Builds a canonical `BooleanExpression` from a parsed `lhs op rhs`, routing each operator through
/// the normalizing factory; prints a canonical comparison back (its op is always one of `<`/`<=`/`=`/`!=`).
private struct ComparisonFold: Conversion {
	func apply(_ input: (Term, CompareOp, Term)) throws -> BooleanExpression {
		let (lhs, op, rhs) = input
		switch op {
		case .lt: return .lessThan(lhs, rhs)
		case .le: return .lessThanOrEqual(lhs, rhs)
		case .gt: return .greaterThan(lhs, rhs)
		case .ge: return .greaterThanOrEqual(lhs, rhs)
		case .eq: return .equal(lhs, rhs)
		case .ne: return .notEqual(lhs, rhs)
		}
	}

	func unapply(_ output: BooleanExpression) throws -> (Term, CompareOp, Term) {
		let op: CompareOp
		switch output.operation {
		case .lt: op = .lt
		case .le: op = .le
		case .eq: op = .eq
		case .ne: op = .ne
		}
		return (output.lhs, op, output.rhs)
	}
}

// MARK: - Term Parser

extension DatalogParser {
	// Must be type-erased to break the recursion in additiveExpressionParser -> multiplicativeExpressionParser -> primaryTermParser -> termParser
	private var termParser: AnyParserPrinter<Substring, Term> {
		additiveExpressionParser.eraseToAnyParserPrinter()
	}

	// Additive expressions: a chain of `+`/`-` over multiplicative terms (left-associative).
	private var additiveExpressionParser: some ParserPrinter<Substring, Term> {
		ParsePrint(AdditiveFold()) {
			multiplicativeExpressionParser
			Whitespace()
			Many {
				OneOf {
					"+".printing(" + ").map { AddOp.plus }
					"-".printing(" - ").map { AddOp.minus }
				}
				Whitespace()
				multiplicativeExpressionParser
				Whitespace()
			}
		}
	}

	// Multiplicative expressions: a chain of `*`/`/` over exponential terms (left-associative).
	private var multiplicativeExpressionParser: some ParserPrinter<Substring, Term> {
		ParsePrint(MultiplicativeFold()) {
			exponentialExpressionParser
			Whitespace()
			Many {
				OneOf {
					"*".map { MulOp.times }
					"/".map { MulOp.divide }
				}
				Whitespace()
				exponentialExpressionParser
				Whitespace()
			}
		}
	}

	// Exponential expressions: `a ^ b ^ c` over primary terms (right-associative).
	private var exponentialExpressionParser: some ParserPrinter<Substring, Term> {
		ParsePrint(ExponentFold()) {
			primaryTermParser
			Whitespace()
			Many {
				"^"
				Whitespace()
				primaryTermParser
				Whitespace()
			}
		}
	}

	// Primary terms: variables, numbers, strings, atoms, parenthesized expressions
	private var primaryTermParser: some ParserPrinter<Substring, Term> {
		OneOf {
			// Parenthesized expressions
			ParsePrint {
				"("
				Whitespace()
				Lazy { termParser }
				Whitespace()
				")"
			}

			// Variables (start with uppercase or _)
			variableParser.map(.case(Term.variable))

			// Quoted strings
			quotedStringParser.map(.case(Term.string))

			// Numbers
			numberParser.map(.case(Term.number))

			// Atoms (lowercase identifiers) - treated as strings
			atomParser.map(.case(Term.string))
		}
	}

	private var variableParser: some ParserPrinter<Substring, Var> {
		ParsePrint {
			// Variables start with uppercase letter or underscore
			Peek {
				Prefix(1) { char in
					char.isUppercase || char == "_"
				}
			}
			// Followed by alphanumeric characters or underscores
			Prefix { char in
				char.isLetter || char.isNumber || char == "_"
			}
		}.map(
			.convert(
				apply: { ctx.getVariable(name: String($0)) },
				unapply: { String(describing: $0)[...] }
			)
		)
	}

	private var quotedStringParser: some ParserPrinter<Substring, String> {
		OneOf {
			// Single-quoted strings (parse only, always print as double-quoted)
			Parse {
				"'"
				Prefix { $0 != "'" }.map(.string)
				"'"
			}
			// Double-quoted strings (default for printing)
			ParsePrint {
				"\""
				Prefix { $0 != "\"" }.map(.string)
				"\""
			}
		}
	}

	private var numberParser: some ParserPrinter<Substring.UTF8View, Double> {
		Double.parser()
	}

	private var atomParser: some ParserPrinter<Substring, String> {
		IdentifierParser()
	}

	private struct IdentifierParser: ParserPrinter {
		var allowInitialHyphen: Bool = false

		func parse(_ input: inout Substring) throws -> String {
			guard let first = input.popFirst(),
				first.isLetter || first == "_" || (allowInitialHyphen && first == "-")
			else {
				throw ParsingError.expected(
					allowInitialHyphen ? "letter, underscore, or hyphen" : "letter or underscore")
			}

			var prefix = input.prefix { $0.isLetter || $0.isNumber || $0 == "_" }
			input.trimPrefix(prefix)

			prefix.prepend(first)
			return String(prefix)
		}

		func print(_ output: String, into input: inout Substring) throws {
			var substr = output[...]
			guard try parse(&substr) == output else {
				throw ParsingError.cannotPrintUnparseable(output)
			}
			input.prepend(contentsOf: output)
		}
	}
}

// MARK: - Arithmetic operator folding

private enum AddOp { case plus, minus }
private enum MulOp { case times, divide }

/// Flattens a same-operator (`add` or `multiply`) chain of `Term`s into its leaves for printing.
private func flattenChain(_ term: Term, op: ArithmeticExpression.Op) -> [Term] {
	guard case .arithmetic(let e) = term, e.operation == op else { return [term] }
	return e.operands
		.map {
			flattenChain($0, op: op)
		}
		.reduce(into: []) {
			$0.append(contentsOf: $1)
		}
}

/// Parses `first (± term)*` into a canonical sum; prints a canonical sum back into that shape,
/// rendering negative-literal addends with `-`.
private struct AdditiveFold: Conversion {
	func apply(_ input: (Term, [(AddOp, Term)])) throws -> Term {
		var result = input.0
		for (op, rhs) in input.1 {
			switch op {
			case .plus: result = .sum(result, rhs)
			case .minus: result = .difference(result, rhs)
			}
		}
		return result
	}

	func unapply(_ output: Term) throws -> (Term, [(AddOp, Term)]) {
		let addends = flattenChain(output, op: .add)
		var rest: [(AddOp, Term)] = []
		for addend in addends.dropFirst() {
			if case .number(let n) = addend, n < 0 {
				rest.append((.minus, .number(-n)))
			} else {
				rest.append((.plus, addend))
			}
		}
		return (addends[0], rest)
	}
}

/// Parses `first (* or / term)*` into a canonical product; prints a canonical product back.
private struct MultiplicativeFold: Conversion {
	func apply(_ input: (Term, [(MulOp, Term)])) throws -> Term {
		var result = input.0
		for (op, rhs) in input.1 {
			switch op {
			case .times: result = .product(result, rhs)
			case .divide: result = .quotient(result, rhs)
			}
		}
		return result
	}

	func unapply(_ output: Term) throws -> (Term, [(MulOp, Term)]) {
		let factors = flattenChain(output, op: .multiply)
		return (factors[0], factors.dropFirst().map { (.times, $0) })
	}
}

/// Parses `a ^ b ^ c` right-associatively into a canonical power; prints a canonical power back.
private struct ExponentFold: Conversion {
	func apply(_ input: (Term, [Term])) throws -> Term {
		guard let last = input.1.last else { return input.0 }
		var result = last
		for base in input.1.dropLast().reversed() {
			result = .power(base, result)
		}
		return .power(input.0, result)
	}

	func unapply(_ output: Term) throws -> (Term, [Term]) {
		var chain: [Term] = []
		var current = output
		while case .arithmetic(let e) = current, case .exponent(let base, let exp) = e.raw {
			chain.append(base)
			current = exp
		}
		chain.append(current)
		return (chain[0], Array(chain.dropFirst()))
	}
}
