import Foundation
import Testing

@testable import CLI

/// `URL.lines` only exists in Foundation on Darwin, so the CLI ships its own on other platforms.
/// These tests pin the two implementations to the same behavior: they run against whichever one
/// the platform provides.
@Suite("URL.lines")
struct URLLinesTests {

	private func writeTempFile(_ contents: String) throws -> URL {
		let url = FileManager.default.temporaryDirectory
			.appendingPathComponent("rbdb-lines-test-\(UUID().uuidString).txt")
		try contents.write(to: url, atomically: false, encoding: .utf8)
		return url
	}

	private func lines(of contents: String) async throws -> [String] {
		let url = try writeTempFile(contents)
		defer { try? FileManager.default.removeItem(at: url) }

		var result: [String] = []
		for try await line in url.lines {
			result.append(line)
		}
		return result
	}

	@Test("splits on newlines")
	func splitsOnNewlines() async throws {
		#expect(try await lines(of: "one\ntwo\nthree\n") == ["one", "two", "three"])
	}

	@Test("yields a final line that has no terminator")
	func unterminatedFinalLine() async throws {
		#expect(try await lines(of: "one\ntwo") == ["one", "two"])
	}

	@Test("an empty file has no lines")
	func emptyFile() async throws {
		#expect(try await lines(of: "") == [])
	}

	/// Not an obvious behavior, but it is Foundation's: a run of terminators yields nothing between
	/// them, so a caller can't tell `"one\n\ntwo"` from `"one\ntwo"`.
	@Test("empty lines are dropped")
	func emptyLinesAreDropped() async throws {
		#expect(try await lines(of: "one\n\n\ntwo\n\n") == ["one", "two"])
	}

	@Test("vertical tab, form feed and the Unicode line breaks terminate a line too")
	func unicodeTerminators() async throws {
		#expect(
			try await lines(of: "a\u{0B}b\u{0C}c\u{85}d\u{2028}e\u{2029}f")
				== ["a", "b", "c", "d", "e", "f"])
	}

	@Test("CRLF and lone CR both terminate a line")
	func carriageReturns() async throws {
		#expect(try await lines(of: "one\r\ntwo\rthree\r\n") == ["one", "two", "three"])
	}

	@Test("a CRLF straddling a read boundary is one terminator, not two")
	func carriageReturnAcrossReadBoundary() async throws {
		// Long enough that the \r and the \n land in separate reads for any plausible buffer size.
		let filler = String(repeating: "a", count: 100_000)
		#expect(try await lines(of: "\(filler)\r\ntail") == [filler, "tail"])
	}

	@Test("multi-byte characters straddling a read boundary survive intact")
	func multiByteAcrossReadBoundary() async throws {
		// Every scalar here is 4 bytes, so some of them are guaranteed to straddle a read boundary.
		let line = String(repeating: "🐈", count: 50_000)
		#expect(try await lines(of: "\(line)\n\(line)") == [line, line])
	}

	@Test("reading a file that does not exist throws")
	func missingFileThrows() async throws {
		let url = FileManager.default.temporaryDirectory
			.appendingPathComponent("rbdb-lines-test-missing-\(UUID().uuidString).txt")

		await #expect(throws: (any Error).self) {
			for try await _ in url.lines {}
		}
	}
}
