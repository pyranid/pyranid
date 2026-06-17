/*
 * Copyright 2015-2022 Transmogrify LLC, 2022-2026 Revetware LLC.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.pyranid;

import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import static java.util.Objects.requireNonNull;

/**
 * Shared behavior for MySQL-family drivers.
 */
abstract class MySqlFamilyDialect extends UuidStringDialect {
	@NonNull
	@Override
	public PreparedStatement prepareStreamingStatement(@NonNull Connection connection,
																										 @NonNull StatementContext<?> statementContext) throws SQLException {
		requireNonNull(connection);
		requireNonNull(statementContext);

		return connection.prepareStatement(statementContext.getStatement().getSql(),
				ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
	}

	@Override
	public void configureStreamingPreparedStatement(@NonNull PreparedStatement preparedStatement,
																										@NonNull DatabaseStreamState databaseStreamState,
																										boolean transactionPresent,
																									boolean queryFetchSizeConfigured) throws SQLException {
		requireNonNull(preparedStatement);
		requireNonNull(databaseStreamState);

		if (queryFetchSizeConfigured)
			return;

		preparedStatement.setFetchSize(Integer.MIN_VALUE);
	}

	@Override
	public boolean isUniqueConstraintViolation(@NonNull DatabaseExceptionMetadata metadata,
																						 @Nullable Throwable cause) {
		requireNonNull(metadata);

		return hasErrorCode(metadata, cause, 1062);
	}

	@Override
	public boolean isForeignKeyViolation(@NonNull DatabaseExceptionMetadata metadata,
																			 @Nullable Throwable cause) {
		requireNonNull(metadata);

		return hasErrorCode(metadata, cause, 1451, 1452);
	}

	@Override
	public boolean isDeadlock(@NonNull DatabaseExceptionMetadata metadata,
														@Nullable Throwable cause) {
		requireNonNull(metadata);

		return hasErrorCode(metadata, cause, 1213);
	}

	@Override
	public boolean isTransient(@NonNull DatabaseExceptionMetadata metadata,
														 @Nullable Throwable cause) {
		requireNonNull(metadata);

		return super.isTransient(metadata, cause) || hasErrorCode(metadata, cause, 1213, 1205);
	}
}
