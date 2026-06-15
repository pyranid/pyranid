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

import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(1)
public class ParsedSqlCacheBenchmark {
	private static final String SQL = """
			SELECT u.id, u.email, u.status, u.created_at
			FROM app_user u
			WHERE u.tenant_id = :tenantId
			  AND u.status IN (:statuses)
			  AND (:email IS NULL OR u.email = :email)
			  AND u.created_at >= :createdAfter
			  AND (u.metadata ? :metadataKey OR :metadataKey IS NULL)
			ORDER BY u.created_at DESC
			LIMIT :limit
			""";

	@State(Scope.Benchmark)
	public static class DatabaseState {
		Database cachedDatabase;
		Database uncachedDatabase;

		@Setup(Level.Trial)
		public void setup() {
			cachedDatabase = Database.withDataSource(BenchmarkSupport.throwingDataSource())
					.databaseType(DatabaseType.POSTGRESQL)
					.parsedSqlCacheCapacity(256)
					.build();
			uncachedDatabase = Database.withDataSource(BenchmarkSupport.throwingDataSource())
					.databaseType(DatabaseType.POSTGRESQL)
					.parsedSqlCacheCapacity(0)
					.build();

			cachedDatabase.query(SQL);
		}
	}

	@Benchmark
	public Query cacheHit(DatabaseState databaseState) {
		return databaseState.cachedDatabase.query(SQL);
	}

	@Benchmark
	public Query noCache(DatabaseState databaseState) {
		return databaseState.uncachedDatabase.query(SQL);
	}
}
