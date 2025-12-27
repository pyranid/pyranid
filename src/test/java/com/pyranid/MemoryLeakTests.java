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
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import javax.annotation.concurrent.ThreadSafe;
import javax.sql.DataSource;
import java.sql.ResultSet;
import java.util.List;

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
	private static final int PREFERRED_MAPPER_CACHE_CAPACITY = 3;

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

		TestQueries.execute(db, "CREATE TABLE t (id INT, value INT)");
		TestQueries.execute(db, "INSERT INTO t (id, value) VALUES (1, 10)");

		for (int i = 0; i < 50; i++) {
			String sql = "SELECT id, value AS extra_" + i + " FROM t";
			db.query(sql)
					.fetchObject(IdRow.class)
					.orElseThrow();
		}

		DefaultResultSetMapper defaultMapper = (DefaultResultSetMapper) db.getResultSetMapper();
		int size = defaultMapper.getRowPlanningCache().size();

		Assertions.assertTrue(size <= PLAN_CACHE_CAPACITY,
				"Row planning cache should remain bounded after varied schemas");
	}

	public static final class SpecialType {
		private final String value;

		SpecialType(@NonNull String value) {
			this.value = requireNonNull(value);
		}

		@NonNull
		public String getValue() {
			return this.value;
		}
	}

	@Test
	public void testPreferredColumnMapperCacheBoundedUnderVariedSourceTypes() {
		DataSource ds = createInMemoryDataSource("preferred_mapper_bounded");

		CustomColumnMapper specialTypeMapper = new CustomColumnMapper() {
			@NonNull
			@Override
			public Boolean appliesTo(@NonNull TargetType targetType) {
				return targetType.matchesClass(SpecialType.class);
			}

			@NonNull
			@Override
			public MappingResult map(@NonNull StatementContext<?> statementContext,
															 @NonNull ResultSet resultSet,
															 @NonNull Object resultSetValue,
															 @NonNull TargetType targetType,
															 @NonNull Integer columnIndex,
															 @Nullable String columnLabel,
															 @NonNull InstanceProvider instanceProvider) {
				return MappingResult.of(new SpecialType(resultSetValue.toString()));
			}
		};

		ResultSetMapper mapper = ResultSetMapper.withCustomColumnMappers(List.of(specialTypeMapper))
				.preferredColumnMapperCacheCapacity(PREFERRED_MAPPER_CACHE_CAPACITY)
				.build();

		Database db = Database.withDataSource(ds)
				.resultSetMapper(mapper)
				.build();

		TestQueries.execute(db, "CREATE TABLE t (s VARCHAR(50), i INT, l BIGINT, d DECIMAL(12,2), dt DATE)");
		TestQueries.execute(db, "INSERT INTO t VALUES ('s', 1, 2, 3.14, DATE '2023-01-01')");

		List<String> columns = List.of("s", "i", "l", "d", "dt");

		for (String column : columns) {
			db.query("SELECT " + column + " FROM t")
					.fetchObject(SpecialType.class)
					.orElseThrow();
		}

		DefaultResultSetMapper defaultMapper = (DefaultResultSetMapper) db.getResultSetMapper();
		int size = defaultMapper.getPreferredColumnMapperBySourceTargetKey().size();

		Assertions.assertTrue(size <= PREFERRED_MAPPER_CACHE_CAPACITY,
				"Preferred custom mapper cache should remain bounded across varied source types");
	}
}
