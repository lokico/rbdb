public protocol Symbol: Comparable, Codable, Sendable {
	var type: SymbolType { get }
	func rewrite<T: SymbolRewriter>(_ rewriter: T) -> Self
	func reduce<T: SymbolReducer>(_ initialResult: T.Result, _ reducer: T) throws -> T.Result
}

public protocol SymbolRewriter {
	func rewrite(formula: Formula) -> Formula
	func rewrite(predicate: Predicate) -> Predicate

	func rewrite(term: Term) -> Term
	func rewrite(variable: Var) -> Var
}

public protocol SymbolReducer {
	associatedtype Result
	func reduce(_ prev: Result, _ formula: Formula) throws -> Result
	func reduce(_ prev: Result, _ predicate: Predicate) throws -> Result

	func reduce(_ prev: Result, _ term: Term) throws -> Result
	func reduce(_ prev: Result, _ variable: Var) throws -> Result
}
