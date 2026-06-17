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
import org.junit.jupiter.api.io.TempDir;

import javax.sql.DataSource;
import java.nio.file.Path;

/**
 * @author <a href="https://www.revetkn.com">Mark Allen</a>
 * @since 4.2.0
 */
public class SQLiteIntegrationIT extends AbstractPortableJdbcIntegrationTests {
	@TempDir
	private Path tempDir;

	@NonNull
	@Override
	protected DataSource dataSource() {
		return new DriverManagerDataSource("jdbc:sqlite:" + this.tempDir.resolve("pyranid.db"), null, null);
	}

	@NonNull
	@Override
	protected String generatedKeyTableSql(@NonNull String tableName) {
		return "CREATE TABLE " + tableName + " (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL)";
	}

	@NonNull
	@Override
	protected DatabaseType expectedDatabaseType() {
		return DatabaseType.SQLITE;
	}

	@Override
	protected boolean supportsReadOnlyTransactions() {
		return false;
	}

	@Override
	protected boolean supportsTransactionIsolationOptions() {
		return false;
	}
}
