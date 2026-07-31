import Foundation
import SQLite3

public typealias Row = [String: Any?]

/// A cursor for lazily iterating over SQL query results.
///
/// `SQLiteCursor`  supports multi-statement SQL.
/// All statements, except for the last, are completely exhausted in
/// the initializer, and the first row of the last statement is retrieved.
/// This is to try and throw any errors, as the rest of the iteration cannot
/// throw and any errors will be fatal.
public class SQLiteCursor: Sequence, IteratorProtocol {

	struct PreparedStatement {
		var ptr: OpaquePointer
		var argumentCount: Int

		/// Index in the original SQL where this statement appeared
		var sqlIndex: SQL.Index
	}

	// We must hold this to keep the DB connection alive while our statements are alive
	private var db: SQLiteDatabase
	private var statements: [PreparedStatement] = []
	private var nextRow: Row?

	public var hasMoreRows: Bool { nextRow != nil }
	public var underestimatedCount: Int { nextRow != nil ? 1 : 0 }

	internal init(_ db: SQLiteDatabase, sql: SQL) throws {
		self.db = db
		try prepareStatements(sql)
	}

	deinit {
		for statement in statements {
			sqlite3_finalize(statement.ptr)
		}
	}

	private func prepareStatements(_ sql: SQL) throws {
		try sql.queryText.withCString { sqlCString in
			var remainingSQL: UnsafePointer<CChar>? = sqlCString.advanced(
				by: sql.startIndex.queryOffset)

			var argumentIndex = sql.startIndex.argumentIndex
			while let currentSQL = remainingSQL, currentSQL.pointee != 0 {
				var statement: OpaquePointer?
				let index = SQL.Index(
					queryOffset: currentSQL - sqlCString, argumentIndex: argumentIndex)

				guard
					sqlite3_prepare_v2(db.db, currentSQL, -1, &statement, &remainingSQL)
						== SQLITE_OK
				else {
					throw SQLiteError.queryError(db.lastErrorMessage, index: index)
				}

				// statement will be nil here (but sqlite3_prepare_v2 succeeded) if currentSQL is whitespace
				if let statement = statement {
					let argumentCount = Int(sqlite3_bind_parameter_count(statement))
					argumentIndex += argumentCount

					let preparedStatement = PreparedStatement(
						ptr: statement,
						argumentCount: argumentCount,
						sqlIndex: index)
					statements.append(preparedStatement)

					try bind(arguments: sql.arguments, in: preparedStatement)

					// We need to fully run any statements that aren't the last, because
					//  subsequent statements might depend on them.
					if let remainingSQL = remainingSQL, remainingSQL.pointee != 0 {
						try exhaust(statement: preparedStatement)
					} else {
						nextRow = try readRow(in: preparedStatement)
					}
				}
			}

			// Validate that all arguments have been consumed
			if argumentIndex != sql.arguments.count {
				throw SQLiteError.queryParameterCount(
					expected: argumentIndex,
					got: sql.arguments.count
				)
			}
		}
	}

	private func bind(arguments: [Any?], in statement: PreparedStatement) throws {
		let argumentCount = arguments.count
		var argumentIndex = statement.sqlIndex.argumentIndex

		let availableParams = argumentCount - argumentIndex
		guard availableParams >= statement.argumentCount else {
			throw SQLiteError.queryParameterCount(
				expected: statements.reduce(0, { $0 + $1.argumentCount }),
				got: argumentCount
			)
		}

		sqlite3_clear_bindings(statement.ptr)

		for i in 0..<statement.argumentCount {
			let parameter = arguments[argumentIndex]
			let paramIndex = Int32(i + 1)  // SQLite parameters are 1-indexed

			switch parameter {
			case let stringValue as any StringProtocol:
				guard
					stringValue.withCString({ bindCStr in
						sqlite3_bind_text(
							statement.ptr,
							paramIndex,
							bindCStr,
							-1,
							unsafeBitCast(
								-1,
								to: sqlite3_destructor_type.self
							)  // SQLITE_TRANSIENT
						)
					}) == SQLITE_OK
				else {
					throw SQLiteError.queryError(
						"Failed to bind string parameter at index \(argumentIndex): \(db.lastErrorMessage)"
					)
				}
			case let intValue as Int:
				guard
					sqlite3_bind_int64(
						statement.ptr,
						paramIndex,
						Int64(intValue)
					) == SQLITE_OK
				else {
					throw SQLiteError.queryError(
						"Failed to bind int parameter at index \(argumentIndex): \(db.lastErrorMessage)"
					)
				}
			case let int64Value as Int64:
				guard
					sqlite3_bind_int64(statement.ptr, paramIndex, int64Value)
						== SQLITE_OK
				else {
					throw SQLiteError.queryError(
						"Failed to bind int64 parameter at index \(argumentIndex): \(db.lastErrorMessage)"
					)
				}
			case let doubleValue as Double:
				guard
					sqlite3_bind_double(statement.ptr, paramIndex, doubleValue)
						== SQLITE_OK
				else {
					throw SQLiteError.queryError(
						"Failed to bind double parameter at index \(argumentIndex): \(db.lastErrorMessage)"
					)
				}
			case let dataValue as Data:
				let result = dataValue.withUnsafeBytes { bytes in
					sqlite3_bind_blob(
						statement.ptr,
						paramIndex,
						bytes.baseAddress,
						Int32(dataValue.count),
						// SQLITE_TRANSIENT
						unsafeBitCast(-1, to: sqlite3_destructor_type.self)
					)
				}
				guard result == SQLITE_OK else {
					throw SQLiteError.queryError(
						"Failed to bind data parameter at index \(argumentIndex): \(db.lastErrorMessage)"
					)
				}
			case let uuidValue as UUIDv7:
				let result = uuidValue.withUnsafeBytes { bytes in
					sqlite3_bind_blob(
						statement.ptr,
						paramIndex,
						bytes.baseAddress,
						Int32(bytes.count),
						unsafeBitCast(-1, to: sqlite3_destructor_type.self)
					)
				}
				guard result == SQLITE_OK else {
					throw SQLiteError.queryError(
						"Failed to bind UUIDv7 parameter at index \(argumentIndex): \(db.lastErrorMessage)"
					)
				}
			case nil, is NSNull:
				guard sqlite3_bind_null(statement.ptr, paramIndex) == SQLITE_OK
				else {
					throw SQLiteError.queryError(
						"Failed to bind null parameter at index \(argumentIndex): \(db.lastErrorMessage)"
					)
				}
			default:
				throw SQLiteError.queryError(
					"Unsupported parameter type at index \(argumentIndex): \(type(of: parameter))"
				)
			}
			argumentIndex += 1
		}
	}

	private func exhaust(statement: PreparedStatement) throws {
		while try step(statement: statement) {
			// loop
		}
	}

	/// Resets the cursor with new arguments, allowing reuse of the prepared statements, optionally
	/// with different parameter values.
	///
	/// - Parameter newArguments: The new arguments to bind to the prepared statements. If not provided, the previous arguments are used.
	/// - Throws: `SQLiteError` if parameter binding or execution of the query fails
	@discardableResult
	func rerun(withArguments newArguments: [Any?]? = nil) throws -> Self {
		for statement in statements {
			sqlite3_reset(statement.ptr)
		}

		if let args = newArguments {
			// Validate parameter count first
			let expectedParams = statements.reduce(0) { $0 + $1.argumentCount }
			if args.count != expectedParams {
				throw SQLiteError.queryParameterCount(expected: expectedParams, got: args.count)
			}

			for statement in statements {
				try bind(arguments: args, in: statement)
			}
		}

		// Re-execute all the statements, up to the last (which the user will iterate)
		for statement in statements.dropLast() {
			try exhaust(statement: statement)
		}
		if let finalStatement = statements.last {
			nextRow = try readRow(in: finalStatement)
		}

		return self
	}

	public func next() -> Row? {
		if let currentRow = nextRow, let stmt = statements.last {
			nextRow = try! readRow(in: stmt)
			return currentRow
		}
		return nil
	}

	func step(statement: PreparedStatement) throws -> Bool {
		switch sqlite3_step(statement.ptr) {
		case SQLITE_ROW:
			return true
		case SQLITE_DONE:
			return false
		default:
			throw SQLiteError.queryError(
				"sqlite3_step failed: \(db.lastErrorMessage)", index: statement.sqlIndex)
		}
	}

	/// Executes a prepared SQLite statement and returns the next row.
	///
	/// This method  calls `sqlite3_step` and converts SQLite values to Swift types.
	///
	/// - Parameter statement: A prepared SQLite statement
	/// - Returns: A  dictionary that represents a row with column names as keys, or `nil` if the query is exhausted.
	/// - Throws: `SQLiteError.queryError` if statement execution fails
	private func readRow(in statement: PreparedStatement) throws -> Row? {
		if try step(statement: statement) {
			var row: Row = [:]
			let columnCount = sqlite3_column_count(statement.ptr)
			for i in 0..<columnCount {
				let columnName = String(
					cString: sqlite3_column_name(statement.ptr, i)
				)
				let columnType = sqlite3_column_type(statement.ptr, i)
				switch columnType {
				case SQLITE_TEXT:
					let columnValue = String(
						cString: sqlite3_column_text(statement.ptr, i)
					)
					row[columnName] = columnValue
				case SQLITE_BLOB:
					let blobPointer = sqlite3_column_blob(statement.ptr, i)
					let blobSize = sqlite3_column_bytes(statement.ptr, i)
					let blobData = Data(
						bytes: blobPointer!,
						count: Int(blobSize)
					)
					row[columnName] = blobData
				case SQLITE_INTEGER:
					let intValue = sqlite3_column_int64(statement.ptr, i)
					row[columnName] = intValue
				case SQLITE_FLOAT:
					let doubleValue = sqlite3_column_double(statement.ptr, i)
					row[columnName] = doubleValue
				case SQLITE_NULL:
					row[columnName] = NSNull()
				default:
					throw SQLiteError.queryError(
						"Unexpected column type in result set: \(columnType)"
					)
				}
			}
			return row
		}
		return nil
	}
}
