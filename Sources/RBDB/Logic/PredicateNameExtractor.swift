import Foundation

struct PredicateNameExtractor: SymbolReducer {
	func reduce(_ predicateNames: Set<String>, _ predicate: Predicate) throws -> Set<String> {
		var names = predicateNames
		// The *positive* name: a negative predicate is not separately declared — `-p` follows from `p`
		//  and shares its columns — so `-p` exists exactly when `p` does.
		names.insert(predicate.positiveName)
		return names
	}
}

extension Symbol {
	public func getPredicateNames() throws -> Set<String> {
		try reduce(Set<String>(), PredicateNameExtractor())
	}
}
