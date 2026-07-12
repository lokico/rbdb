import RBDB
import Parsing

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
		// FIXME: Swift really want a type annotation for this
		let emptyPredicates: [Predicate] = []
		return ParsePrint(.case(Formula.hornClause)) {
			// Head
			predicateParser

			Whitespace()

			Optionally {
				":-".printing(" :- ")
				Whitespace()
				Many {
					predicateParser
					Whitespace()
				} separator: {
					","
					Whitespace().printing(" ".utf8)
				}
			}.map(.orDefault(emptyPredicates))

			// Optional period at the end
			".".replaceError(with: ())
		}
	}
}

// MARK: - Predicate Parser

extension DatalogParser {
	private var predicateParser: some ParserPrinter<Substring, Predicate> {
		ParsePrint(.memberwise(Predicate.init(name:arguments:))) {
			// Parse predicate name (identifier)
			identifierParser

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
					"+".map { AddOp.plus }
					"-".map { AddOp.minus }
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
		identifierParser
	}

	private var identifierParser: some ParserPrinter<Substring, String> {
		ParsePrint(.string) {
			// Start with letter or underscore
			Peek {
				Prefix(1) { char in
					char.isLetter || char == "_"
				}
			}
			// Followed by alphanumeric characters or underscores
			Prefix { char in
				char.isLetter || char.isNumber || char == "_"
			}
		}
	}
}

extension Conversion {
	@inlinable
	public static func orDefault<T: Equatable>(_ defaultValue: T) -> Self
	where Self == Conversions.OrDefault<T> {
		return .init(defaultValue: defaultValue)
	}

	@inlinable
	public static func leftAssociate<T, C: Conversion<(T, T), T>>(_ combine: C) -> Self
	where Self == Conversions.LeftAssociate<T, C> {
		return .init(combine: combine)
	}
}

extension Conversions {
	public struct OrDefault<T: Equatable>: Conversion {
		public let defaultValue: T

		@inlinable
		public init(defaultValue: T) {
			self.defaultValue = defaultValue
		}

		@inlinable
		public func apply(_ input: T?) throws -> T {
			return input ?? defaultValue
		}

		@inlinable
		public func unapply(_ output: T) -> T? {
			return output == defaultValue ? nil : output
		}
	}

	public struct LeftAssociate<T, C: Conversion<(T, T), T>>: Conversion {
		public let combine: C

		@inlinable
		public init(combine: C) {
			self.combine = combine
		}

		@inlinable
		public func apply(_ input: (T, [T])) throws -> T {
			var lhs = input.0
			for rhs in input.1 {
				lhs = try combine.apply((lhs, rhs))
			}
			return lhs
		}

		@inlinable
		public func unapply(_ output: T) throws -> (T, [T]) {
			guard let initial = try? combine.unapply(output) else {
				return (output, [])
			}
			var fst = initial.0
			var arr = [initial.1]

			while let (newFst, snd) = try? combine.unapply(fst) {
				fst = newFst
				arr.prepend(snd)
			}

			return (fst, arr)
		}
	}
}

// MARK: - Arithmetic operator folding

private enum AddOp { case plus, minus }
private enum MulOp { case times, divide }

/// Flattens a same-operator (`add` or `multiply`) chain of `Term`s into its leaves for printing.
private func flattenChain(_ term: Term, op: Expression.Op) -> [Term] {
	guard case .expression(let e) = term, e.operation == op else { return [term] }
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
		while case .expression(let e) = current, case .exponent(let base, let exp) = e.raw {
			chain.append(base)
			current = exp
		}
		chain.append(current)
		return (chain[0], Array(chain.dropFirst()))
	}
}
