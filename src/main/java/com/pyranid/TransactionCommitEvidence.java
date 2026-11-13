/*
 * Copyright 2015-2022 Transmogrify LLC, 2022-2026 Revetware LLC.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at https://www.apache.org/licenses/LICENSE-2.0
 */

package com.pyranid;

import java.io.IOException;
import java.sql.SQLException;
import java.sql.SQLNonTransientConnectionException;
import java.sql.SQLRecoverableException;
import java.sql.SQLTransientConnectionException;

/** Evidence from the original physical commit failure, before wrapping or completion changes its graph. */
final class TransactionCommitEvidence {
	private TransactionCommitEvidence() {}

	static boolean provesSerializationAbort(SQLException failure, DatabaseDialect dialect) {
		String primaryState;
		DatabaseExceptionMetadata metadata;
		try {
			primaryState = failure.getSQLState();
			if (!"40001".equals(primaryState)) return false;
			metadata = DatabaseExceptionMetadata.builder(null).sqlState(primaryState)
					.errorCode(failure.getErrorCode()).build();
		} catch (Throwable diagnosticFailure) {
			ThrowableGraph.retainInspectionFailure(failure, diagnosticFailure);
			return false;
		}
		// A positive primary state never hides conflicting, missing or unreadable subordinate evidence.
		int conflicting = ThrowableGraph.inspectRetainingFailures(failure, current -> {
			if (current instanceof IOException || current instanceof InterruptedException || current instanceof Error
					|| current instanceof SQLRecoverableException || current instanceof SQLTransientConnectionException
					|| current instanceof SQLNonTransientConnectionException)
				return 1;
			if (current instanceof SQLException sqlFailure) {
				String state = current == failure ? primaryState : sqlFailure.getSQLState();
				return "40001".equals(state) ? 0 : 1;
			}
			return 0;
		}, 1, current -> true, 1);
		if (conflicting != 0) return false;
		try {
			// Existing dialect restrictions remain necessary; SQL Server, for example, excludes deadlock victims.
			return Boolean.TRUE.equals(dialect.isSerializationFailure(metadata, failure));
		} catch (Throwable diagnosticFailure) {
			ThrowableGraph.retainInspectionFailure(failure, diagnosticFailure);
			return false;
		}
	}
}
