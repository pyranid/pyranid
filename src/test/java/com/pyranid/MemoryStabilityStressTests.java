/*
 * Copyright 2015-2022 Transmogrify LLC, 2022-2025 Revetware LLC.
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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import org.jspecify.annotations.NonNull;
import javax.annotation.concurrent.ThreadSafe;
import javax.sql.DataSource;
import java.math.BigDecimal;
import java.time.Duration;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * Long-running heap stability tests intended to detect unbounded retention.
 * <p>
 * To run:
 * 1) Remove or comment out {@link Disabled}.
 * 2) Execute: mvn -Dtest=MemoryStabilityStressTests test
 *
 * @author <a href="https://www.revetkn.com">Mark Allen</a>
 * @since 4.0.0
 */
@ThreadSafe
public class MemoryStabilityStressTests {
	private static final Duration WARMUP_DURATION = Duration.ofSeconds(30);
	private static final Duration TEST_DURATION = Duration.ofMinutes(4);
	private static final Duration COOL_DOWN_DURATION = Duration.ofSeconds(10);
	private static final int PLAN_CACHE_CAPACITY = 64;
	private static final int PREFERRED_MAPPER_CACHE_CAPACITY = 32;
	private static final long BASELINE_SLACK_BYTES = 64L * 1024L * 1024L;
	private static final double BASELINE_SLACK_RATIO = 0.35d;

	@NonNull
	protected DataSource createInMemoryDataSource(@NonNull String databaseName) {
		requireNonNull(databaseName);

		JDBCDataSource dataSource = new JDBCDataSource();
		dataSource.setUrl(format("jdbc:hsqldb:mem:%s", databaseName));
		dataSource.setUser("sa");
		dataSource.setPassword("");

		return dataSource;
	}

	public record StressRow(Integer id, String name, BigDecimal amount, LocalDate createdAt) {}

	@Test
	@Tag("stress")
	@Disabled("Long-running heap stability test; enable manually.")
	public void testHeapStabilityUnderVariedWorkload() {
		DataSource ds = createInMemoryDataSource("heap_stability");
		ResultSetMapper mapper = ResultSetMapper.withPlanCachingEnabled(true)
				.planCacheCapacity(PLAN_CACHE_CAPACITY)
				.preferredColumnMapperCacheCapacity(PREFERRED_MAPPER_CACHE_CAPACITY)
				.build();

		Database db = Database.withDataSource(ds)
				.resultSetMapper(mapper)
				.build();

		TestQueries.execute(db, """
				CREATE TABLE t (
				  id INT PRIMARY KEY,
				  name VARCHAR(64),
				  amount DECIMAL(12,2),
				  created_at DATE
				)
				""");

		for (int i = 1; i <= 500; i++) {
			TestQueries.execute(db,
					"INSERT INTO t (id, name, amount, created_at) VALUES (?, ?, ?, ?)",
					i, "name-" + i, new BigDecimal("123.45"), java.sql.Date.valueOf(LocalDate.now()));
		}

		runWorkload(db, WARMUP_DURATION);
		long baseline = stabilizedUsedMemory();

		runWorkload(db, TEST_DURATION);
		runWorkload(db, COOL_DOWN_DURATION);

		long finalUsed = stabilizedUsedMemory();
		long allowedGrowth = Math.max(BASELINE_SLACK_BYTES, (long) (baseline * BASELINE_SLACK_RATIO));

		Assertions.assertTrue(finalUsed <= baseline + allowedGrowth,
				format("Heap growth exceeded threshold. baseline=%d final=%d allowed=%d",
						baseline, finalUsed, allowedGrowth));
	}

	private void runWorkload(@NonNull Database db, @NonNull Duration duration) {
		requireNonNull(db);
		requireNonNull(duration);

		long deadline = System.nanoTime() + duration.toNanos();
		Random rng = new Random(17);
		List<Integer> ids = new ArrayList<>(10);

		String[] sqls = new String[]{
				"SELECT id, name, amount, created_at FROM t WHERE id = :id",
				"SELECT id, name, amount, created_at FROM t WHERE id IN (:ids)",
				"SELECT id, name, amount, created_at FROM t ORDER BY id FETCH FIRST 5 ROWS ONLY"
		};

		while (System.nanoTime() < deadline) {
			int id = 1 + rng.nextInt(500);
			db.query("UPDATE t SET amount = amount + 1 WHERE id = :id")
					.bind("id", id)
					.execute();

			StressRow row = db.query(sqls[0])
					.bind("id", id)
					.fetchObject(StressRow.class)
					.orElseThrow();

			ids.clear();
			int count = 1 + rng.nextInt(8);
			for (int i = 0; i < count; i++) ids.add(1 + rng.nextInt(500));

			db.query(sqls[1])
					.bind("ids", Parameters.inList(ids))
					.fetchList(StressRow.class);

			db.query(sqls[2])
					.fetchList(StressRow.class);

			if (row.id() % 11 == 0) {
				db.transaction(() -> {
					db.query("UPDATE t SET name = :name WHERE id = :id")
							.bind("name", "name-" + row.id())
							.bind("id", row.id())
							.execute();

					db.query("SELECT id, name, amount, created_at FROM t WHERE id = :id")
							.bind("id", row.id())
							.fetchObject(StressRow.class)
							.orElseThrow();
				});
			}
		}
	}

	private static long stabilizedUsedMemory() {
		for (int i = 0; i < 3; i++) {
			System.gc();
			System.runFinalization();
			try {
				Thread.sleep(50L);
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				break;
			}
		}

		Runtime runtime = Runtime.getRuntime();
		return runtime.totalMemory() - runtime.freeMemory();
	}
}
