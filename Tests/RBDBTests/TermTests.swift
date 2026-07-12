import Foundation
import Testing

@testable import RBDB

@Test func serializeVariable() async throws {
	try assertJSON(Term.variable(Var(id: 42)), expect: "{\"v\":42}")
}

@Test func serializeBooleanConstant() async throws {
	try assertJSON(Term.boolean(true), expect: "{\"\":true}")
	try assertJSON(Term.boolean(false), expect: "{\"\":false}")
}

@Test func serializeIntConstant() async throws {
	try assertJSON(Term.number(5), expect: "{\"\":5}")
	try assertJSON(Term.number(-5), expect: "{\"\":-5}")
	try assertJSON(Term.number(0), expect: "{\"\":0}")
}

@Test func serializeFloatConstant() async throws {
	try assertJSON(Term.number(5.7), expect: "{\"\":5.7}")
	try assertJSON(Term.number(-5.123), expect: "{\"\":-5.123}")
	#expect(throws: EncodingError.self) {
		try assertJSON(Term.number(Double.nan), expect: "")
	}
}

@Test func serializeStringConstant() async throws {
	try assertJSON(Term.string("hi mom"), expect: "{\"\":\"hi mom\"}")
	try assertJSON(Term.string("123"), expect: "{\"\":\"123\"}")
	try assertJSON(Term.string("true"), expect: "{\"\":\"true\"}")
	try assertJSON(Term.string(""), expect: "{\"\":\"\"}")
}

@Test func deserializeWithNoKeysThrows() async throws {
	let dec = JSONDecoder()
	let data = "{}".data(using: .utf8)!
	#expect(throws: DecodingError.self) {
		let _ = try dec.decode(Term.self, from: data)
	}
}

@Test func deserializeWithUnknownType() async throws {
	let dec = JSONDecoder()
	let data = "{\"----UNKNOWN KEY---\": \"foo\"}".data(using: .utf8)!
	#expect(throws: DecodingError.self) {
		let _ = try dec.decode(Term.self, from: data)
	}
}

@Test func deserializeWithExtraneousUnknownType() async throws {
	let dec = JSONDecoder()
	let data = "{\"----UNKNOWN KEY---\": \"foo\",\"\":\"bar\"}".data(
		using: .utf8
	)!
	let result = try dec.decode(Term.self, from: data)
	#expect(result == .string("bar"))
}

@Test func deserializePrefersLaterDefinedTypes1() async throws {
	let dec = JSONDecoder()
	let data = "{\"\":\"bar\",\"v\":5}".data(using: .utf8)!
	let result = try dec.decode(Term.self, from: data)
	#expect(result == .variable(Var(id: 5)))
}

@Test func deserializePrefersLaterDefinedTypes2() async throws {
	let dec = JSONDecoder()
	let data = "{\"v\":5,\"\":\"bar\"}".data(using: .utf8)!
	let result = try dec.decode(Term.self, from: data)
	#expect(result == .variable(Var(id: 5)))
}
