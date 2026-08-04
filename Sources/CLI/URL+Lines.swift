// `URL.lines` is Darwin-only: swift-corelibs-foundation has no `URL.AsyncBytes`, so this fills in
// the one use we have of it — reading a local file a line at a time.
#if !canImport(Darwin)

	import Foundation

	extension URL {
		/// The lines of the file at this URL, read lazily.
		///
		/// Matches Foundation's `URL.lines` on Darwin, quirks included: LF, VT, FF, CR, CRLF, NEL,
		/// U+2028 and U+2029 all end a line, terminators are stripped, and empty lines are dropped.
		/// Unlike Darwin's, this only handles file URLs.
		var lines: Lines { Lines(url: self) }

		struct Lines: AsyncSequence {
			typealias Element = String

			let url: URL

			func makeAsyncIterator() -> Iterator { Iterator(url: url) }

			final class Iterator: AsyncIteratorProtocol {
				/// What the bytes at some offset in `buffer` turned out to be.
				private enum Scan {
					case notATerminator
					/// A terminator that may be truncated by the end of the buffer -- read more first.
					case needsMoreBytes
					case terminator(length: Int)
				}

				private static let chunkSize = 32 * 1024

				private let url: URL
				private var handle: FileHandle?
				private var started = false
				private var buffer: [UInt8] = []
				/// Index of the first byte not yet returned to the caller.
				private var start = 0
				private var atEOF = false

				init(url: URL) {
					self.url = url
				}

				deinit {
					try? handle?.close()
				}

				func next() async throws -> String? {
					while true {
						if let line = nextBufferedLine() { return line }

						guard atEOF else {
							try readMore()
							continue
						}

						// Whatever is left over is the last line, if it isn't empty.
						let remainder = buffer[start...]
						start = buffer.count
						guard remainder.isEmpty else {
							return String(decoding: remainder, as: UTF8.self)
						}
						return nil
					}
				}

				/// The next complete line already in `buffer`, or nil if we need to read more bytes
				/// (or, at EOF, if what remains is the unterminated last line).
				private func nextBufferedLine() -> String? {
					var i = start
					while i < buffer.count {
						switch scanTerminator(at: i) {
						case .notATerminator:
							i += 1
						case .needsMoreBytes:
							return nil
						case .terminator(let length):
							let line = buffer[start..<i]
							start = i + length
							guard line.isEmpty else {
								return String(decoding: line, as: UTF8.self)
							}
							i = start
						}
					}
					return nil
				}

				/// Whether `buffer[i]` begins a line terminator, and how long it is.
				///
				/// Bytes past the end of the buffer only mean "truncated" while more can still arrive;
				/// at EOF they resolve to whatever the bytes on hand say.
				private func scanTerminator(at i: Int) -> Scan {
					func byte(_ offset: Int) -> UInt8? {
						let index = i + offset
						return index < buffer.count ? buffer[index] : nil
					}

					switch buffer[i] {
					case 0x0A, 0x0B, 0x0C:  // LF, VT, FF
						return .terminator(length: 1)
					case 0x0D:  // CR, on its own or as the first half of CRLF
						guard let next = byte(1) else {
							return atEOF ? .terminator(length: 1) : .needsMoreBytes
						}
						return .terminator(length: next == 0x0A ? 2 : 1)
					case 0xC2:  // U+0085 NEXT LINE
						guard let next = byte(1) else {
							return atEOF ? .notATerminator : .needsMoreBytes
						}
						return next == 0x85 ? .terminator(length: 2) : .notATerminator
					case 0xE2:  // U+2028 LINE SEPARATOR, U+2029 PARAGRAPH SEPARATOR
						guard let second = byte(1), let third = byte(2) else {
							return atEOF ? .notATerminator : .needsMoreBytes
						}
						return second == 0x80 && (third == 0xA8 || third == 0xA9)
							? .terminator(length: 3) : .notATerminator
					default:
						return .notATerminator
					}
				}

				/// Appends another chunk of the file to `buffer`, or sets `atEOF`.
				///
				/// The read is synchronous. That blocks the calling task rather than suspending it,
				/// which is fine for the local files the CLI runs and keeps this free of any
				/// dependency on a particular async file API.
				private func readMore() throws {
					let handle = try openIfNeeded()

					// Drop what the caller has already consumed, so the buffer stays around the size
					// of one chunk plus the line being assembled.
					if start > 0 {
						buffer.removeFirst(start)
						start = 0
					}

					guard let chunk = try handle.read(upToCount: Self.chunkSize), !chunk.isEmpty
					else {
						atEOF = true
						try handle.close()
						self.handle = nil
						return
					}
					buffer.append(contentsOf: chunk)
				}

				private func openIfNeeded() throws -> FileHandle {
					if let handle { return handle }
					// Only on the first read, so that a second call after EOF doesn't reopen the file.
					guard !started else { throw CocoaError(.fileReadUnknown) }
					started = true
					let handle = try FileHandle(forReadingFrom: url)
					self.handle = handle
					return handle
				}
			}
		}
	}

#endif
