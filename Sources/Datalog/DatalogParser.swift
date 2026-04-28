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

	// Additive expressions: term + term, term - term
	private var additiveExpressionParser: some ParserPrinter<Substring, Term> {
		OneOf {
			binaryExpressionParser(multiplicativeExpressionParser, "+", Expression.add)
			binaryExpressionParser(multiplicativeExpressionParser, "-", Expression.subtract)
		}
	}

	// Multiplicative expressions: term * term, term / term
	private var multiplicativeExpressionParser: some ParserPrinter<Substring, Term> {
		OneOf {
			binaryExpressionParser(primaryTermParser, "*", Expression.multiply)
			binaryExpressionParser(primaryTermParser, "/", Expression.divide)
		}
	}

	// Multiplicative expressions: term * term, term / term
	private func binaryExpressionParser<
		A: ParserPrinter<Substring, Term>, O: ParserPrinter<Substring, ()>
	>(
		_ argParser: A,
		_ op: O,
		_ expr: @escaping (Term, Term) -> Expression
	) -> some ParserPrinter<Substring, Term> {
		ParsePrint(.leftAssociate(.case(expr).map(.case(Term.expression)))) {
			argParser
			Whitespace()
			Many {
				op
				Whitespace()
				argParser
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

	private var numberParser: some ParserPrinter<Substring.UTF8View, Float> {
		Float.parser()
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
