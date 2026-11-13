/*
 * Copyright 2015-2022 Transmogrify LLC, 2022-2026 Revetware LLC.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at https://www.apache.org/licenses/LICENSE-2.0
 */

package com.pyranid;

import org.jspecify.annotations.Nullable;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/** Internal completion checks. Never reenters statement admission or application extension points. */
final class TransactionCompletionValidation {
	private TransactionCompletionValidation() {}

	static void validate(Connection connection, DatabaseType type, @Nullable Throwable suspect,
			boolean validationRequired) {
		if (type != DatabaseType.POSTGRESQL && type != DatabaseType.DUCK_DB) return;
		PostgresDriverSupport.TransactionStateRead state = type == DatabaseType.POSTGRESQL
				? PostgresDriverSupport.transactionState(connection) : null;
		if (state != null) {
			if (state.state() == PostgresDriverSupport.TransactionState.USABLE) return;
			if (state.state() == PostgresDriverSupport.TransactionState.FAILED)
				throw refusal("An earlier statement failure aborted the transaction before commit", suspect,
						type.dialect(), List.of());
		}
		if (suspect == null && !validationRequired) return;

		List<Throwable> failures = validityQuery(connection);
		if (failures.isEmpty()) return;
		Throwable first = failures.get(0);
		if (first instanceof Error error) {
			for (int index = 1; index < failures.size(); ++index)
				Database.appendCleanupFailure(error, failures.get(index));
			if (state != null && state.failure() != null)
				Database.appendCleanupFailure(error, state.failure());
			throw error;
		}
		if (state != null && state.failure() != null) failures.add(state.failure());
		throw refusal("Pre-commit validation could not establish transaction usability", suspect,
				type.dialect(), failures);
	}

	private static List<Throwable> validityQuery(Connection connection) {
		List<Throwable> failures = new ArrayList<>();
		Statement statement = null;
		ResultSet result = null;
		try {
			statement = connection.createStatement();
			result = statement.executeQuery("SELECT 1");
			if (!result.next() || result.getInt(1) != 1 || result.wasNull() || result.next())
				throw new SQLException("Transaction validity query returned an unexpected result");
		} catch (Throwable failure) {
			failures.add(failure);
		} finally {
			if (result != null) {
				try { result.close(); }
				catch (Throwable failure) { failures.add(failure); }
			}
			if (statement != null) {
				try { statement.close(); }
				catch (Throwable failure) { failures.add(failure); }
			}
		}
		return failures;
	}

	private static DatabaseException refusal(String message, @Nullable Throwable suspect,
			DatabaseDialect dialect, List<Throwable> validationFailures) {
		DatabaseException refusal;
		if (suspect instanceof DatabaseException databaseFailure) {
			// Copies already-scrubbed metadata and flags, without rereading foreign diagnostic accessors.
			refusal = DatabaseException.wrapSafely(message, databaseFailure, dialect, null);
		} else {
			try {
				refusal = new DatabaseException(message, suspect, dialect);
			} catch (Throwable diagnosticFailure) {
				refusal = new DatabaseException(message, suspect, GenericDialect.INSTANCE, null, false);
				Database.appendCleanupFailure(refusal, diagnosticFailure);
			}
		}
		// Do not add query or cleanup failures to a recorded exception the application may still hold.
		for (Throwable failure : validationFailures) Database.appendCleanupFailure(refusal, failure);
		return refusal;
	}
}
