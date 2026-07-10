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

import org.hsqldb.jdbc.JDBCDataSource;
import org.jspecify.annotations.NonNull;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import javax.annotation.concurrent.ThreadSafe;
import javax.sql.DataSource;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * Exercises cache growth patterns under heavy/varied usage to catch unbounded retention.
 *
 * @author <a href="https://www.revetkn.com">Mark Allen</a>
 * @since 4.0.0
 */
@ThreadSafe
public class MemoryLeakTests {
	private static final int PLAN_CACHE_CAPACITY = 8;

	@NonNull
	protected DataSource createInMemoryDataSource(@NonNull String databaseName) {
		requireNonNull(databaseName);

		JDBCDataSource dataSource = new JDBCDataSource();
		dataSource.setUrl(format("jdbc:hsqldb:mem:%s", databaseName));
		dataSource.setUser("sa");
		dataSource.setPassword("");

		return dataSource;
	}

	public record IdRow(Integer id) {}

	@Test
	public void testRowPlanningCacheBoundedUnderVariedSchemas() {
		DataSource ds = createInMemoryDataSource("plan_cache_bounded");
		ResultSetMapper mapper = ResultSetMapper.withPlanCachingEnabled(true)
				.planCacheCapacity(PLAN_CACHE_CAPACITY)
				.build();

		Database db = Database.withDataSource(ds)
				.resultSetMapper(mapper)
				.build();

				db.query("CREATE TABLE t (id INT, value INT)").execute();
				db.query("INSERT INTO t (id, value) VALUES (1, 10)").execute();

		for (int i = 0; i < 50; i++) {
			String sql = "SELECT id, value AS extra_" + i + " FROM t";
			db.query(sql)
					.fetchObject(IdRow.class)
					.orElseThrow();
		}

		DefaultResultSetMapper defaultMapper = (DefaultResultSetMapper) db.getResultSetMapper();
		int size = defaultMapper.getRowPlanningCacheForResultClass(IdRow.class).size();

		Assertions.assertTrue(size <= PLAN_CACHE_CAPACITY,
				"Row planning cache should remain bounded after varied schemas");
	}

}
