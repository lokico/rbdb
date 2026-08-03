import Foundation

/// What this parser refuses, as opposed to what the parsing library refuses on its way through the input.
enum ParsingError: LocalizedError {
	case conversionError
	case expected(String)
	case cannotPrintUnparseable(String)

	// FIXME: Localize these strings
	var errorDescription: String? {
		switch self {
		case .conversionError: "Could not build a formula from what was parsed"
		case .expected(let what): "Expected \(what)"
		case .cannotPrintUnparseable(let text):
			"Cannot print '\(text)': it would not parse back as itself"
		}
	}
}
