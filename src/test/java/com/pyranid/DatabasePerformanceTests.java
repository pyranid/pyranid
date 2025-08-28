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
import org.junit.Test;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;
import javax.annotation.concurrent.ThreadSafe;
import javax.sql.DataSource;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Currency;
import java.util.List;
import java.util.Locale;
import java.util.UUID;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * @author <a href="https://www.revetkn.com">Mark Allen</a>
 * @since 2.1.0
 */
@ThreadSafe
public class DatabasePerformanceTests {
	// A wide JavaBean target (42 columns).
	// Include a few @DatabaseColumn aliases so one DB column can hydrate multiple properties
	@NotThreadSafe
	public static class WideRow {
		private Long id;
		private String c_varchar_1;
		private String c_varchar_2;
		private String c_varchar_3;
		private String c_varchar_4;
		private String c_varchar_5;

		private Integer c_int_1;
		private Integer c_int_2;
		private Integer c_int_3;
		private Integer c_int_4;
		private Integer c_int_5;

		private Long c_bigint_1;
		private Long c_bigint_2;

		private BigDecimal c_decimal_1;
		private BigDecimal c_decimal_2;

		private LocalDate c_date_1;
		private LocalDate c_date_2;

		private LocalDateTime c_ts_1;
		private LocalDateTime c_ts_2;

		private Boolean c_bool_1;
		private Boolean c_bool_2;

		private UUID c_uuid_1;
		private UUID c_uuid_2;

		private Locale c_locale;
		@DatabaseColumn("c_locale")
		private String c_locale_raw; // fan-out same column

		private Currency c_currency;
		@DatabaseColumn("c_currency")
		private String c_currency_raw; // fan-out

		private String c_text_1;
		private String c_text_2;
		private String c_text_3;
		private String c_text_4;
		private String c_text_5;

		private Integer c_int_6;
		private Integer c_int_7;
		private Integer c_int_8;
		private Integer c_int_9;
		private Integer c_int_10;

		public Long getId() {
			return this.id;
		}

		public void setId(Long id) {
			this.id = id;
		}

		public String getC_varchar_1() {
			return this.c_varchar_1;
		}

		public void setC_varchar_1(String c_varchar_1) {
			this.c_varchar_1 = c_varchar_1;
		}

		public String getC_varchar_2() {
			return this.c_varchar_2;
		}

		public void setC_varchar_2(String c_varchar_2) {
			this.c_varchar_2 = c_varchar_2;
		}

		public String getC_varchar_3() {
			return this.c_varchar_3;
		}

		public void setC_varchar_3(String c_varchar_3) {
			this.c_varchar_3 = c_varchar_3;
		}

		public String getC_varchar_4() {
			return this.c_varchar_4;
		}

		public void setC_varchar_4(String c_varchar_4) {
			this.c_varchar_4 = c_varchar_4;
		}

		public String getC_varchar_5() {
			return this.c_varchar_5;
		}

		public void setC_varchar_5(String c_varchar_5) {
			this.c_varchar_5 = c_varchar_5;
		}

		public Integer getC_int_1() {
			return this.c_int_1;
		}

		public void setC_int_1(Integer c_int_1) {
			this.c_int_1 = c_int_1;
		}

		public Integer getC_int_2() {
			return this.c_int_2;
		}

		public void setC_int_2(Integer c_int_2) {
			this.c_int_2 = c_int_2;
		}

		public Integer getC_int_3() {
			return this.c_int_3;
		}

		public void setC_int_3(Integer c_int_3) {
			this.c_int_3 = c_int_3;
		}

		public Integer getC_int_4() {
			return this.c_int_4;
		}

		public void setC_int_4(Integer c_int_4) {
			this.c_int_4 = c_int_4;
		}

		public Integer getC_int_5() {
			return this.c_int_5;
		}

		public void setC_int_5(Integer c_int_5) {
			this.c_int_5 = c_int_5;
		}

		public Long getC_bigint_1() {
			return this.c_bigint_1;
		}

		public void setC_bigint_1(Long c_bigint_1) {
			this.c_bigint_1 = c_bigint_1;
		}

		public Long getC_bigint_2() {
			return this.c_bigint_2;
		}

		public void setC_bigint_2(Long c_bigint_2) {
			this.c_bigint_2 = c_bigint_2;
		}

		public BigDecimal getC_decimal_1() {
			return this.c_decimal_1;
		}

		public void setC_decimal_1(BigDecimal c_decimal_1) {
			this.c_decimal_1 = c_decimal_1;
		}

		public BigDecimal getC_decimal_2() {
			return this.c_decimal_2;
		}

		public void setC_decimal_2(BigDecimal c_decimal_2) {
			this.c_decimal_2 = c_decimal_2;
		}

		public LocalDate getC_date_1() {
			return this.c_date_1;
		}

		public void setC_date_1(LocalDate c_date_1) {
			this.c_date_1 = c_date_1;
		}

		public LocalDate getC_date_2() {
			return this.c_date_2;
		}

		public void setC_date_2(LocalDate c_date_2) {
			this.c_date_2 = c_date_2;
		}

		public LocalDateTime getC_ts_1() {
			return this.c_ts_1;
		}

		public void setC_ts_1(LocalDateTime c_ts_1) {
			this.c_ts_1 = c_ts_1;
		}

		public LocalDateTime getC_ts_2() {
			return this.c_ts_2;
		}

		public void setC_ts_2(LocalDateTime c_ts_2) {
			this.c_ts_2 = c_ts_2;
		}

		public Boolean getC_bool_1() {
			return this.c_bool_1;
		}

		public void setC_bool_1(Boolean c_bool_1) {
			this.c_bool_1 = c_bool_1;
		}

		public Boolean getC_bool_2() {
			return this.c_bool_2;
		}

		public void setC_bool_2(Boolean c_bool_2) {
			this.c_bool_2 = c_bool_2;
		}

		public UUID getC_uuid_1() {
			return this.c_uuid_1;
		}

		public void setC_uuid_1(UUID c_uuid_1) {
			this.c_uuid_1 = c_uuid_1;
		}

		public UUID getC_uuid_2() {
			return this.c_uuid_2;
		}

		public void setC_uuid_2(UUID c_uuid_2) {
			this.c_uuid_2 = c_uuid_2;
		}

		public Locale getC_locale() {
			return this.c_locale;
		}

		public void setC_locale(Locale c_locale) {
			this.c_locale = c_locale;
		}

		public String getC_locale_raw() {
			return this.c_locale_raw;
		}

		public void setC_locale_raw(String c_locale_raw) {
			this.c_locale_raw = c_locale_raw;
		}

		public Currency getC_currency() {
			return this.c_currency;
		}

		public void setC_currency(Currency c_currency) {
			this.c_currency = c_currency;
		}

		public String getC_currency_raw() {
			return this.c_currency_raw;
		}

		public void setC_currency_raw(String c_currency_raw) {
			this.c_currency_raw = c_currency_raw;
		}

		public String getC_text_1() {
			return this.c_text_1;
		}

		public void setC_text_1(String c_text_1) {
			this.c_text_1 = c_text_1;
		}

		public String getC_text_2() {
			return this.c_text_2;
		}

		public void setC_text_2(String c_text_2) {
			this.c_text_2 = c_text_2;
		}

		public String getC_text_3() {
			return this.c_text_3;
		}

		public void setC_text_3(String c_text_3) {
			this.c_text_3 = c_text_3;
		}

		public String getC_text_4() {
			return this.c_text_4;
		}

		public void setC_text_4(String c_text_4) {
			this.c_text_4 = c_text_4;
		}

		public String getC_text_5() {
			return this.c_text_5;
		}

		public void setC_text_5(String c_text_5) {
			this.c_text_5 = c_text_5;
		}

		public Integer getC_int_6() {
			return this.c_int_6;
		}

		public void setC_int_6(Integer c_int_6) {
			this.c_int_6 = c_int_6;
		}

		public Integer getC_int_7() {
			return this.c_int_7;
		}

		public void setC_int_7(Integer c_int_7) {
			this.c_int_7 = c_int_7;
		}

		public Integer getC_int_8() {
			return this.c_int_8;
		}

		public void setC_int_8(Integer c_int_8) {
			this.c_int_8 = c_int_8;
		}

		public Integer getC_int_9() {
			return this.c_int_9;
		}

		public void setC_int_9(Integer c_int_9) {
			this.c_int_9 = c_int_9;
		}

		public Integer getC_int_10() {
			return this.c_int_10;
		}

		public void setC_int_10(Integer c_int_10) {
			this.c_int_10 = c_int_10;
		}
	}

	@Test
	public void wideMappingManyRows_plannedMapper_onlyTimings() {
		int rows = 20_000; // adjust if needed
		PerfRunResult r = runOnceWithMapper("wide_planned_only", rows, createPlannedMapper());
		System.out.println(format("[PLANNED] rows=%d warmup(ms)=%d run1(ms)=%d run2(ms)=%d run3(ms)=%d median(ms)=%d",
				rows, r.warmupMs, r.measuredMs[0], r.measuredMs[1], r.measuredMs[2], median(r.measuredMs)));
	}

	@Test
	public void wideMappingManyRows_standardMapper_onlyTimings() {
		int rows = 20_000; // adjust if needed
		PerfRunResult r = runOnceWithMapper("wide_standard_only", rows, createStandardMapper());
		System.out.println(format("[STANDARD] rows=%d warmup(ms)=%d run1(ms)=%d run2(ms)=%d run3(ms)=%d median(ms)=%d",
				rows, r.warmupMs, r.measuredMs[0], r.measuredMs[1], r.measuredMs[2], median(r.measuredMs)));
	}

	@Test
	public void wideMappingManyRows_comparePlannedVsBaseline_ifAvailable() {
		int rows = 20_000;

		ResultSetMapper planned = createPlannedMapper();
		ResultSetMapper baseline = createStandardMapper();

		PerfRunResult rPlan = runOnceWithMapper("wide_plan_cmp", rows, planned);
		PerfRunResult rBase = runOnceWithMapper("wide_base_cmp", rows, baseline);

		long medPlan = median(rPlan.measuredMs);
		long medBase = median(rBase.measuredMs);
		double speedup = (double) medBase / (double) medPlan;

		System.out.println(format("[COMPARE] rows=%d baselineMed(ms)=%d plannedMed(ms)=%d speedup=%.2fx",
				rows, medBase, medPlan, speedup));
	}

	private static final class PerfRunResult {
		final long warmupMs;
		final long[] measuredMs;

		PerfRunResult(long warmupMs, long[] measuredMs) {
			this.warmupMs = warmupMs;
			this.measuredMs = measuredMs;
		}
	}

	@Nonnull
	private PerfRunResult runOnceWithMapper(@Nonnull String dbName,
																					int rowCount,
																					@Nonnull ResultSetMapper mapper) {
		requireNonNull(mapper);

		DataSource ds = createInMemoryDataSource(dbName);
		Database db = Database.forDataSource(ds)
				.timeZone(ZoneId.of("America/New_York"))
				.resultSetMapper(mapper)
				.build();

		createWideSchema(db);
		insertWideRows(db, rowCount);

		// Warm up caches and JIT once with a smaller read
		long t0 = System.nanoTime();
		db.queryForList("SELECT * FROM wide_table ORDER BY id LIMIT 1000", WideRow.class);
		long warmupMs = (System.nanoTime() - t0) / 1_000_000L;

		// Three measured full scans (ORDER BY to stabilize plan path)
		long[] ms = new long[3];
		for (int i = 0; i < 3; i++) {
			long s = System.nanoTime();
			List<WideRow> rows = db.queryForList("SELECT * FROM wide_table ORDER BY id", WideRow.class);
			// sanity check so the optimizer can't elide the work
			if (rows.size() != rowCount) {
				throw new AssertionError("Expected " + rowCount + " rows, got " + rows.size());
			}
			ms[i] = (System.nanoTime() - s) / 1_000_000L;
		}
		return new PerfRunResult(warmupMs, ms);
	}

	private static long median(long[] a) {
		long x = Math.min(a[0], Math.min(a[1], a[2]));
		long y = Math.max(a[0], Math.max(a[1], a[2]));
		return (long) (a[0] + a[1] + a[2] - x - y);
	}

	@Nonnull
	private ResultSetMapper createPlannedMapper() {
		return ResultSetMapper.withPlanCachingEnabled(true).build();
	}

	@Nullable
	private ResultSetMapper createStandardMapper() {
		return ResultSetMapper.withPlanCachingEnabled(false).build();
	}

	private void createWideSchema(@Nonnull Database db) {
		db.execute("""
				CREATE TABLE wide_table (
				  id BIGINT PRIMARY KEY,
				  c_varchar_1 VARCHAR(128),
				  c_varchar_2 VARCHAR(128),
				  c_varchar_3 VARCHAR(128),
				  c_varchar_4 VARCHAR(128),
				  c_varchar_5 VARCHAR(128),
				
				  c_int_1 INT,
				  c_int_2 INT,
				  c_int_3 INT,
				  c_int_4 INT,
				  c_int_5 INT,
				
				  c_bigint_1 BIGINT,
				  c_bigint_2 BIGINT,
				
				  c_decimal_1 DECIMAL(18,4),
				  c_decimal_2 DECIMAL(18,4),
				
				  c_date_1 DATE,
				  c_date_2 DATE,
				
				  c_ts_1 TIMESTAMP,
				  c_ts_2 TIMESTAMP,
				
				  c_bool_1 BOOLEAN,
				  c_bool_2 BOOLEAN,
				
				  c_uuid_1 VARCHAR(36),
				  c_uuid_2 VARCHAR(36),
				
				  c_locale VARCHAR(16),
				  c_currency VARCHAR(3),
				
				  c_text_1 VARCHAR(4000),
				  c_text_2 VARCHAR(4000),
				  c_text_3 VARCHAR(4000),
				  c_text_4 VARCHAR(4000),
				  c_text_5 VARCHAR(4000),
				
				  c_int_6 INT,
				  c_int_7 INT,
				  c_int_8 INT,
				  c_int_9 INT,
				  c_int_10 INT
				)
				""");
	}

	private void insertWideRows(@Nonnull Database db, int n) {
		db.transaction(() -> {
			for (int i = 1; i <= n; i++) {
				long id = i;
				String v = "name_" + i;
				String v2 = "tag_" + (i % 10);
				String v3 = "group_" + (i % 5);
				String v4 = "city_" + (i % 100);
				String v5 = "misc_" + i;

				int i1 = i, i2 = i * 2, i3 = i % 1000, i4 = 42, i5 = -i;
				long b1 = i * 1000L, b2 = i * 2000L;

				BigDecimal d1 = BigDecimal.valueOf(i * 0.1234);
				BigDecimal d2 = BigDecimal.valueOf(i * 0.5678);

				LocalDate ld1 = LocalDate.of(2020, 1, 1).plusDays(i % 1000);
				LocalDate ld2 = LocalDate.of(2021, 6, 1).plusDays(i % 500);

				LocalDateTime ts1 = LocalDateTime.of(2022, 1, 1, 12, 0).plusSeconds(i);
				LocalDateTime ts2 = LocalDateTime.of(2023, 1, 1, 8, 0).plusMinutes(i);

				boolean bo1 = (i % 2 == 0), bo2 = (i % 3 == 0);

				UUID u1 = UUID.nameUUIDFromBytes(("u1" + i).getBytes());
				UUID u2 = UUID.nameUUIDFromBytes(("u2" + i).getBytes());

				String locale = (i % 3 == 0) ? "en-US" : (i % 3 == 1) ? "ja-JP" : "fr-FR";
				String currency = (i % 2 == 0) ? "USD" : "EUR";

				String t1 = "lorem ipsum " + i;
				String t2 = "dolor sit amet " + (i % 7);
				String t3 = "consectetur " + (i % 11);
				String t4 = "adipiscing elit " + (i % 13);
				String t5 = "sed do " + (i % 17);

				int i6 = i + 1, i7 = i + 2, i8 = i + 3, i9 = i + 4, i10 = i + 5;

				db.execute("""
								INSERT INTO wide_table (
								  id,
								  c_varchar_1, c_varchar_2, c_varchar_3, c_varchar_4, c_varchar_5,
								  c_int_1, c_int_2, c_int_3, c_int_4, c_int_5,
								  c_bigint_1, c_bigint_2,
								  c_decimal_1, c_decimal_2,
								  c_date_1, c_date_2,
								  c_ts_1, c_ts_2,
								  c_bool_1, c_bool_2,
								  c_uuid_1, c_uuid_2,
								  c_locale, c_currency,
								  c_text_1, c_text_2, c_text_3, c_text_4, c_text_5,
								  c_int_6, c_int_7, c_int_8, c_int_9, c_int_10
								) VALUES (?,
								          ?,?,?,?,?,
								          ?,?,?,?,?,
								          ?,?,
								          ?,?,
								          ?,?,
								          ?,?,
								          ?,?,
								          ?,?,
								          ?,?,
								          ?,?,?,?,?,
								          ?,?,?,?,?)
								""",
						id,
						v, v2, v3, v4, v5,
						i1, i2, i3, i4, i5,
						b1, b2,
						d1, d2,
						ld1, ld2,
						ts1, ts2,
						bo1, bo2,
						u1.toString(), u2.toString(),
						locale, currency,
						t1, t2, t3, t4, t5,
						i6, i7, i8, i9, i10
				);
			}
		});
	}

	@Nonnull
	protected DataSource createInMemoryDataSource(@Nonnull String databaseName) {
		requireNonNull(databaseName);
		JDBCDataSource dataSource = new JDBCDataSource();
		dataSource.setUrl(format("jdbc:hsqldb:mem:%s;sql.syntax_pgs=true", databaseName));
		dataSource.setUser("sa");
		dataSource.setPassword("");
		return dataSource;
	}
}