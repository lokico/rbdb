import Foundation

struct ParsedCreateTable {
	let ifNotExists: Bool
	let tableName: String
	let columnNames: [String]
	let comment: String?

	init?(sql: String) throws {
		// Handle CREATE TABLE [IF NOT EXISTS] tableName (columns...)
		let pattern = #/^CREATE\s+TABLE(\s+IF\s+NOT\s+EXISTS)?\s*([^(]+?)\(/#
			.ignoresCase()
		guard let match = sql.firstMatch(of: pattern) else {
			return nil
		}

		// Check if IF NOT EXISTS was captured
		self.ifNotExists = match.1 != nil

		let rawTableName = String(match.2).trimmingCharacters(
			in: .whitespacesAndNewlines
		)
		self.tableName = rawTableName.trimmingCharacters(
			in: CharacterSet(charactersIn: "\"'`[]")
		)

		// Find the opening parenthesis after the table name
		guard let openParenIndex = sql.firstIndex(of: "(") else { return nil }

		// Find the matching closing parenthesis, handling nested parentheses
		var parenDepth = 0

		// Also look for --! comments and /* ... */ block comments
		enum CommentState {
			case none
			case firstHyphen
			case secondHyphen
			case ignoredComment
			case tableComment(String)
			case blockCommentStart
			case blockComment
			case blockCommentEnd
		}
		var commentState: CommentState = .none
		var tableCommentLines: [String] = []
		var columnsDef: String = ""

		for char in sql[openParenIndex...] {
			switch commentState {
			case .none:
				if char == "-" {
					commentState = .firstHyphen
				} else if char == "/" {
					commentState = .blockCommentStart
				}
			case .firstHyphen:
				if char == "-" {
					// if it's a comment, remove the first hyphen from columnsDef
					columnsDef.removeLast()
					commentState = .secondHyphen
					continue
				} else {
					commentState = .none
				}
			case .secondHyphen:
				commentState = char == "!" ? .tableComment("") : .ignoredComment
				continue
			case .ignoredComment:
				if char == "\n" {
					commentState = .none
				} else {
					continue
				}
			case .tableComment(let str):
				if char == "\n" {
					commentState = .none
					tableCommentLines.append(str.trimmingCharacters(in: .whitespacesAndNewlines))
				} else {
					commentState = .tableComment(str + String(char))
					continue
				}
			case .blockCommentStart:
				if char == "*" {
					// remove the "/" from columnsDef
					columnsDef.removeLast()
					commentState = .blockComment
					continue
				} else {
					commentState = .none
				}
			case .blockComment:
				if char == "*" { commentState = .blockCommentEnd }
				continue
			case .blockCommentEnd:
				commentState = char == "/" ? .none : .blockComment
				continue
			}

			if char == "(" {
				parenDepth += 1
				if parenDepth == 1 { continue }
			} else if char == ")" {
				parenDepth -= 1
				if parenDepth == 0 { break }
			}
			columnsDef += String(char)
		}

		guard parenDepth == 0 else { return nil }

		// Extract the column definitions between the parentheses
		self.columnNames = try parseColumnNames(from: columnsDef)
		self.comment = tableCommentLines.isEmpty ? nil : tableCommentLines.joined(separator: "\n")
	}

}

private func parseColumnNames(from columnsDef: String) throws -> [String] {
	var columnNames: [String] = []

	let columnDefs = StringParsing.split(columnsDef, by: ",")
	for columnDef in columnDefs {
		try processColumn(columnDef, into: &columnNames)
	}

	return columnNames
}

private func processColumn(
	_ columnDef: String,
	into columnNames: inout [String]
) throws {
	let trimmed = columnDef.trimmingCharacters(in: .whitespacesAndNewlines)

	// Table constraints like UNIQUE (name), FOREIGN KEY, CHECK, and PRIMARY KEY aren't
	//  enforced by the generated view, so reject them rather than silently ignoring them.
	let upperTrimmed = trimmed.uppercased()
	if upperTrimmed.hasPrefix("UNIQUE(") || upperTrimmed.hasPrefix("UNIQUE ")
		|| upperTrimmed.hasPrefix("PRIMARY KEY")
		|| upperTrimmed.hasPrefix("FOREIGN KEY")
		|| upperTrimmed.hasPrefix("CHECK(") || upperTrimmed.hasPrefix("CHECK ")
		|| upperTrimmed.hasPrefix("CONSTRAINT")
	{
		throw SQLiteError.queryError(
			"Table constraints are not supported: \(trimmed)"
		)
	}

	// Extract column name (first word before space or type)
	if let columnName = trimmed.components(separatedBy: .whitespacesAndNewlines)
		.first
	{
		// Check for quoted column names and reject them
		if columnName.hasPrefix("\"") || columnName.hasPrefix("'")
			|| columnName.hasPrefix("`") || columnName.hasPrefix("[")
		{
			throw SQLiteError.queryError(
				"Quoted column names are not supported: \(columnName)"
			)
		}

		if !columnName.isEmpty {
			columnNames.append(columnName)
		}
	}
}
