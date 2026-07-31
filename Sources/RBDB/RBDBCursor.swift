import Foundation
import SQLite3

class RBDBCursor: SQLiteCursor {
	private let rbdb: RBDB

	/// The writing statement whose coherence savepoint is currently open, if any. Held per statement
	/// rather than per cursor because multi-statement SQL is checked a statement at a time — each one
	/// is a complete write as far as the relations are concerned.
	private var savepointOwner: OpaquePointer?

	init(_ rbdb: RBDB, sql: SQL) throws {
		self.rbdb = rbdb
		try super.init(rbdb, sql: sql)
	}

	deinit {
		// Belt and braces: a write that *returns rows* (`INSERT … RETURNING`) only reaches
		//  `SQLITE_DONE` if the caller iterates it to the end, and nothing obliges them to. The
		//  savepoint must not outlive the cursor either way.
		if savepointOwner != nil {
			try? rbdb.endCoherenceSavepoint(rollingBack: false)
		}
	}

	override func step(statement: SQLiteCursor.PreparedStatement) throws -> Bool {
		if !rbdb.isInitializing {
			if let sqlText = sqlite3_sql(statement.ptr) {
				let sqlString = String(cString: sqlText).trimmingCharacters(
					in: .whitespacesAndNewlines)
				if sqlString.range(
					of: "CREATE TABLE", options: [.caseInsensitive, .anchored]
				) != nil {
					try rbdb.interceptCreateTable(sqlString)

					// Return empty result set instead of letting SQLite execute
					//  the CREATE TABLE
					return false
				}

				// A statement that writes gets a savepoint before its first step, so that a
				//  contradiction it turns out to complete can be undone once we can see it — which is
				//  only after the write lands. See `checkCoherence`.
				if savepointOwner == nil && rbdb.writes(statement.ptr, sql: sqlString) {
					try rbdb.beginCoherenceSavepoint()
					savepointOwner = statement.ptr
				}
			}
		}

		let hasRow: Bool
		do {
			hasRow = try super.step(statement: statement)
		} catch {
			// `try?`: whatever went wrong with the statement is the error worth reporting, and an
			//  unwinding savepoint must not replace it with its own.
			try? endSavepoint(for: statement.ptr, rollingBack: true)
			throw error
		}

		// The statement is done only once it stops producing rows; until then the write isn't
		//  finished and there is nothing settled to check.
		guard !hasRow, savepointOwner == statement.ptr else { return hasRow }
		do {
			try rbdb.checkCoherence()
		} catch {
			try? endSavepoint(for: statement.ptr, rollingBack: true)
			throw error
		}
		try endSavepoint(for: statement.ptr, rollingBack: false)
		return false
	}

	private func endSavepoint(for statement: OpaquePointer, rollingBack: Bool) throws {
		guard savepointOwner == statement else { return }
		savepointOwner = nil
		try rbdb.endCoherenceSavepoint(rollingBack: rollingBack)
	}
}
