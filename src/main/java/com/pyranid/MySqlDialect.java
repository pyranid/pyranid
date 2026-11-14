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

import java.sql.PreparedStatement;
import java.sql.SQLException;

import static java.util.Objects.requireNonNull;

/**
 * MySQL-specific behavior.
 */
final class MySqlDialect extends MySqlFamilyDialect {
	@NonNull
	static final MySqlDialect INSTANCE = new MySqlDialect();

	private MySqlDialect() {}

	@Override
	public void configureStreamingPreparedStatement(@NonNull PreparedStatement preparedStatement,
			@NonNull DatabaseStreamState databaseStreamState, @NonNull Boolean transactionPresent,
			@NonNull Boolean queryFetchSizeConfigured) throws SQLException {
		requireNonNull(preparedStatement);
		requireNonNull(databaseStreamState);
		requireNonNull(transactionPresent);
		requireNonNull(queryFetchSizeConfigured);
		int fetchSize = preparedStatement.getFetchSize();
		if (fetchSize == 0 && queryFetchSizeConfigured) return;
		if (fetchSize > 0 && MySqlDriverSupport.cursorFetchEnabled(preparedStatement)) return;
		preparedStatement.setFetchSize(Integer.MIN_VALUE);
	}
}
