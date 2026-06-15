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

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(1)
public class RowPlanCacheBenchmark {
	@State(Scope.Benchmark)
	public static class RowPlanState {
		private static final String[] COLUMN_LABELS = {
				"id",
				"account_id",
				"email",
				"display_name",
				"active",
				"login_count"
		};
		private static final int[] COLUMN_TYPES = {
				Types.INTEGER,
				Types.BIGINT,
				Types.VARCHAR,
				Types.VARCHAR,
				Types.BOOLEAN,
				Types.INTEGER
		};
		private static final String[] COLUMN_TYPE_NAMES = {
				"INTEGER",
				"BIGINT",
				"VARCHAR",
				"VARCHAR",
				"BOOLEAN",
				"INTEGER"
		};
		private static final Object[] VALUES = {
				123,
				456L,
				"pat@example.com",
				"Pat Doe",
				true,
				17
		};

		ResultSetMapper mapper;
		StatementContext<RowProjection> statementContext;
		ResultSet resultSet;
		InstanceProvider instanceProvider;

		@Setup(Level.Trial)
		public void setup() throws SQLException {
			Database database = Database.withDataSource(BenchmarkSupport.throwingDataSource())
					.databaseType(DatabaseType.GENERIC)
					.build();

			mapper = ResultSetMapper.withPlanCachingEnabled(true)
					.planCacheCapacity(256)
					.build();
			statementContext = StatementContext.<RowProjection>with(
							Statement.of("row-plan-cache-benchmark", "SELECT id, account_id, email, display_name, active, login_count"),
							database)
					.resultSetRowType(RowProjection.class)
					.build();
			resultSet = BenchmarkSupport.resultSet(COLUMN_LABELS, COLUMN_TYPES, COLUMN_TYPE_NAMES, VALUES);
			instanceProvider = new InstanceProvider() {};

			mapper.map(statementContext, resultSet, RowProjection.class, instanceProvider);
		}
	}

	@Benchmark
	public Optional<RowProjection> cacheHit(RowPlanState rowPlanState) throws SQLException {
		return rowPlanState.mapper.map(
				rowPlanState.statementContext,
				rowPlanState.resultSet,
				RowProjection.class,
				rowPlanState.instanceProvider);
	}

	public static class RowProjection {
		private Integer id;
		private Long accountId;
		private String email;
		private String displayName;
		private Boolean active;
		private Integer loginCount;

		public Integer getId() {
			return id;
		}

		public void setId(Integer id) {
			this.id = id;
		}

		public Long getAccountId() {
			return accountId;
		}

		public void setAccountId(Long accountId) {
			this.accountId = accountId;
		}

		public String getEmail() {
			return email;
		}

		public void setEmail(String email) {
			this.email = email;
		}

		public String getDisplayName() {
			return displayName;
		}

		public void setDisplayName(String displayName) {
			this.displayName = displayName;
		}

		public Boolean getActive() {
			return active;
		}

		public void setActive(Boolean active) {
			this.active = active;
		}

		public Integer getLoginCount() {
			return loginCount;
		}

		public void setLoginCount(Integer loginCount) {
			this.loginCount = loginCount;
		}
	}
}
