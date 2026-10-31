/*
 * Copyright 2015-2022 Transmogrify LLC, 2022-2026 Revetware LLC.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.pyranid;

import java.sql.SQLException;
import java.sql.SQLNonTransientConnectionException;
import java.sql.SQLRecoverableException;
import java.sql.SQLTransientException;

import static java.util.Objects.requireNonNull;

/** Rejects terminal evidence before an optional JDBC operation can fall back. */
final class JdbcFallbacks {
	private JdbcFallbacks() {}

	/** Inspects failures without publishing interruption or changing their identity. */
	static boolean isTerminal(Throwable failure) {
		requireNonNull(failure);
		return Thread.currentThread().isInterrupted() || ThrowableGraph.inspectRetainingFailures(failure, current -> {
			// A root AbstractMethodError is the legacy JDBC capability signal. Errors carried by that
			// signal, or by another failure, are still terminal.
			if (current instanceof Error && (current != failure || !(current instanceof AbstractMethodError))
					|| current instanceof InterruptedException
					|| current instanceof java.io.InterruptedIOException
					|| current instanceof java.nio.channels.ClosedByInterruptException)
				return 1;
			if (current instanceof DatabaseException databaseException
					&& (databaseException.isTransactionOutcomeCommitted() || databaseException.isTransactionOutcomeIndeterminate()))
				return 1;
			if (current instanceof PostTransactionOperationException postTransactionFailure
					&& postTransactionFailure.getTransactionResult() != TransactionResult.ROLLED_BACK)
				return 1;
			if (!(current instanceof SQLException sqlFailure))
				return 0;
			if (sqlFailure instanceof SQLTransientException || sqlFailure instanceof SQLRecoverableException
					|| sqlFailure instanceof SQLNonTransientConnectionException)
				return 1;
			String sqlState = sqlFailure.getSQLState();
			return sqlState != null && (sqlState.startsWith("40") || sqlState.startsWith("08")
					|| "57014".equals(sqlState) || "HY008".equals(sqlState)
					|| "HYT00".equals(sqlState) || "HYT01".equals(sqlState)) ? 1 : 0;
		}, 1) != 0;
	}
}
