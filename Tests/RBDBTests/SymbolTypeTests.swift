import Foundation
import Testing
@testable import RBDB

@Test func serializeConstantType() async throws {
	try assertJSON(SymbolType.constant, expect: "\"\"")
}

@Test func serializeVariableType() async throws {
	try assertJSON(SymbolType.variable, expect: "\"v\"")
}

@Test func serializeRelationType() async throws {
	try assertJSON(SymbolType.hornClause(headName: "Foo"), expect: "\"@Foo\"")
}

@Test func serializeNegatedRelationType() async throws {
	// `@-Foo`, not `-@Foo`: the leading `@` is what `negative_literal_count`'s `LIKE '@%'` guard and
	//  `init?(stringValue:)`'s dispatch key off, and `output_type = '@Foo'` still excludes this row.
	try assertJSON(SymbolType.hornClause(headName: "-Foo"), expect: "\"@-Foo\"")
}
