import Foundation
import SQLite3

/// Errors that can occur when working with SQLite databases.
public enum SQLiteError: Error {
	/// Failed to open the database at the specified path.
	case couldNotOpenDatabase(String)

	/// Failed to register a custom SQL function.
	case couldNotRegisterFunction(name: String)

	/// Parameter count mismatch between SQL placeholders and provided arguments.
	/// - Parameters:
	///   - expected: Number of parameters consumed by all statements
	///   - got: Number of arguments provided
	case queryParameterCount(expected: Int, got: Int)

	/// Error running a query. Gives an index in the SQL of the failing statement, if possible.
	/// - Parameters:
	///   - String: Error message from SQLite
	///   - index: Position in the SQL where the error occurred, if available
	case queryError(String, index: SQL.Index? = nil)
}

/// A Swift wrapper around SQLite with support for typed SQL queries and parameter binding.
///
/// This class provides a type-safe interface to SQLite databases using the `SQL` struct
/// for parameterized queries. It supports multi-statement SQL execution with proper
/// parameter binding and error recovery.
public class SQLiteDatabase {
	var db: OpaquePointer?

	var lastErrorMessage: String {
		String(cString: sqlite3_errmsg(db!))
	}

	/// Creates a new SQLite database connection.
	///
	/// Opens or creates a SQLite database at the specified path and registers
	/// custom functions like `uuidv7()`.
	///
	/// - Parameter path: The file system path to the database file, or ":memory:" for an in-memory database
	/// - Throws: ``SQLiteError/couldNotOpenDatabase(_:)`` if the database cannot be opened
	/// - Throws: ``SQLiteError/couldNotRegisterFunction(name:)`` if custom functions cannot be registered
	public init(path: String) throws {
		if sqlite3_open_v2(
			path,
			&db,
			SQLITE_OPEN_CREATE | SQLITE_OPEN_READWRITE,
			nil
		) != SQLITE_OK {
			let errmsg = lastErrorMessage
			sqlite3_close(db)
			throw SQLiteError.couldNotOpenDatabase(errmsg)
		}

		// Register the custom uuidv7() function
		let uuidResult = sqlite3_create_function(
			db,  // Database connection
			"uuidv7",  // Function name
			0,  // Number of arguments (0 for no arguments)
			SQLITE_UTF8 | SQLITE_INNOCUOUS,
			nil,  // User data pointer (not needed)
			uuidv7SQLiteFunction,  // Function implementation
			nil,  // Step function (for aggregates)
			nil  // Final function (for aggregates)
		)
		if uuidResult != SQLITE_OK {
			sqlite3_close(db)
			throw SQLiteError.couldNotRegisterFunction(name: "uuidv7")
		}

		// Register the custom sql_exec() function
		let sqlExecResult = sqlite3_create_function(
			db,  // Database connection
			"sql_exec",  // Function name
			1,  // Number of arguments (1 for SQL string)
			SQLITE_UTF8,
			UnsafeMutableRawPointer(db),  // Pass db handle as user data
			sqlExecSQLiteFunction,  // Function implementation
			nil,  // Step function (for aggregates)
			nil  // Final function (for aggregates)
		)
		if sqlExecResult != SQLITE_OK {
			sqlite3_close(db)
			throw SQLiteError.couldNotRegisterFunction(name: "sql_exec")
		}
	}

	deinit {
		sqlite3_close(db)
	}

	/// Executes one or more SQL statements with parameter binding.
	///
	/// This method supports multi-statement SQL execution with automatic parameter binding.
	/// Parameters are bound to `?` placeholders in the order they appear in the SQL text,
	/// distributed across all statements in the query.
	///
	/// - Parameter sql: The SQL query with embedded parameters
	/// - Returns: An array of dictionaries representing the result set from the last statement that produced results
	/// - Throws: ``SQLiteError/queryParameterCount(expected:got:)`` if parameter count doesn't match placeholders
	/// - Throws: ``SQLiteError/queryError(_:index:)`` if SQL execution fails
	///
	/// ## Example
	/// ```swift
	/// let results = try db.query(sql: """
	///     INSERT INTO users (name, age) VALUES (\(name), \(age));
	///     SELECT * FROM users WHERE age > \(minAge)
	///     """)
	/// ```
	@discardableResult
	public func query(sql: SQL) throws -> SQLiteCursor {
		return try SQLiteCursor(self, sql: sql)
	}

	/// Registers a custom scalar SQL function on this database connection.
	///
	/// This lets other modules (which can't see the private `db` handle) add their own
	/// SQLite functions, the same way `uuidv7()` and `sql_exec()` are registered above.
	public func registerFunction(
		name: String,
		argumentCount: Int32,
		flags: Int32 = SQLITE_UTF8,
		_ function:
			@escaping @convention(c) (
				OpaquePointer?, Int32, UnsafeMutablePointer<OpaquePointer?>?
			) -> Void
	) throws {
		guard
			sqlite3_create_function(
				db, name, argumentCount, flags, nil, function, nil, nil
			) == SQLITE_OK
		else {
			throw SQLiteError.couldNotRegisterFunction(name: name)
		}
	}
}

/// Custom SQLite function to execute SQL statements from within triggers
func sqlExecSQLiteFunction(
	context: OpaquePointer?,
	argc: Int32,
	argv: UnsafeMutablePointer<OpaquePointer?>?
) {
	guard let context = context,
		let argv = argv,
		argc == 1,
		let sqlPointer = argv[0]
	else {
		sqlite3_result_error(context, "sql_exec requires exactly one argument", -1)
		return
	}

	// Get the database handle from user data
	guard let userData = sqlite3_user_data(context) else {
		sqlite3_result_error(context, "sql_exec: no database handle", -1)
		return
	}
	let db = OpaquePointer(userData)

	// Get the SQL string
	guard let sqlCString = sqlite3_value_text(sqlPointer) else {
		sqlite3_result_error(context, "sql_exec: invalid SQL string", -1)
		return
	}

	let sqlString = String(cString: sqlCString)

	// Execute the SQL
	let result = sqlite3_exec(db, sqlString, nil, nil, nil)

	if result == SQLITE_OK {
		sqlite3_result_int(context, 1)  // Return 1 for success
	} else {
		let errorMsg = String(cString: sqlite3_errmsg(db))
		sqlite3_result_error(context, "sql_exec: \(errorMsg)", -1)
	}
}
