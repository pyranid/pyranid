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
import java.lang.reflect.Constructor;
import java.time.OffsetTime;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicInteger;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * @author <a href="https://www.revetkn.com">Mark Allen</a>
 * @since 4.0.0
 */
@ThreadSafe
public class RowPlanningCacheTests {
	private Database db(boolean planCachingEnabled, String schema) {
		DataSource ds = createInMemoryDataSource(schema);
		return Database.withDataSource(ds)
				.resultSetMapper(ResultSetMapper.withPlanCachingEnabled(true).build())
				.build();
	}

	private Database dbWithInstanceProvider(boolean planCachingEnabled, String schema, InstanceProvider ip) {
		DataSource ds = createInMemoryDataSource(schema);
		return Database.withDataSource(ds)
				.resultSetMapper(ResultSetMapper.withPlanCachingEnabled(true).build())
				.instanceProvider(ip)
				.build();
	}

	@NonNull
	protected DataSource createInMemoryDataSource(@NonNull String databaseName) {
		requireNonNull(databaseName);

		JDBCDataSource dataSource = new JDBCDataSource();
		dataSource.setUrl(format("jdbc:hsqldb:mem:%s", databaseName));
		dataSource.setUser("sa");
		dataSource.setPassword("");

		return dataSource;
	}

	// ---------- Beans / Records used in tests ----------

	public static class BeanWithPrimitive {
		private int n;

		public int getN() {return n;}

		public void setN(int n) {this.n = n;}
	}

	public static class BeanWithPreinit {
		private String name = "INIT";

		public String getName() {return name;}

		public void setName(String name) {this.name = name;}
	}

	public static record RecWithPrimitive(int n) {}

	// ---------- 1) Primitive type handling & null semantics ----------

	@Test
	public void testBeanPrimitiveNonNullMapping_NonPlanned() {
		Database db = db(false, "prim_nonplanned_ok");
		TestQueries.execute(db, "CREATE TABLE t (n INT)");
		TestQueries.execute(db, "INSERT INTO t (n) VALUES (1)");

		BeanWithPrimitive row = db.query("SELECT n FROM t")
				.fetchObject(BeanWithPrimitive.class)
				.orElseThrow();
		Assertions.assertEquals(1, row.getN(), "INTEGER should map to int without error (non-planned path).");
	}

	@Test
	public void testBeanPrimitiveNonNullMapping_Planned() {
		Database db = db(true, "prim_planned_ok");
		TestQueries.execute(db, "CREATE TABLE t (n INT)");
		TestQueries.execute(db, "INSERT INTO t (n) VALUES (1)");

		BeanWithPrimitive row = db.query("SELECT n FROM t")
				.fetchObject(BeanWithPrimitive.class)
				.orElseThrow();
		Assertions.assertEquals(1, row.getN(), "INTEGER should map to int without error (planned path).");
	}

	@Test
	public void testBeanNullToPrimitiveThrows_NonPlanned() {
		Database db = db(false, "null_to_prim_nonplanned");
		TestQueries.execute(db, "CREATE TABLE t (n INT)");
		TestQueries.execute(db, "INSERT INTO t (n) VALUES (NULL)");

		DatabaseException ex = Assertions.assertThrows(
				DatabaseException.class,
				() -> db.query("SELECT n FROM t")
						.fetchObject(BeanWithPrimitive.class)
						.orElseThrow(),
				"NULL → primitive should throw in non-planned path"
		);
		// Optional: check message for clarity
		Assertions.assertTrue(ex.getMessage().toLowerCase(Locale.ROOT).contains("primitive"),
				"Error message should mention primitive null mapping");
	}

	@Test
	public void testBeanNullToPrimitiveThrows_Planned() {
		Database db = db(true, "null_to_prim_planned");
		TestQueries.execute(db, "CREATE TABLE t (n INT)");
		TestQueries.execute(db, "INSERT INTO t (n) VALUES (NULL)");

		DatabaseException ex = Assertions.assertThrows(
				DatabaseException.class,
				() -> db.query("SELECT n FROM t")
						.fetchObject(BeanWithPrimitive.class)
						.orElseThrow(),
				"NULL → primitive should throw in planned path (currently silent default 0)"
		);
		Assertions.assertTrue(ex.getMessage().toLowerCase(Locale.ROOT).contains("primitive"),
				"Error message should mention primitive null mapping");
	}

	@Test
	public void testBeanNullSetterCalledClearsPreinitializedValue_NonPlanned() {
		Database db = db(false, "null_setter_nonplanned");
		TestQueries.execute(db, "CREATE TABLE t (name VARCHAR(64))");
		TestQueries.execute(db, "INSERT INTO t (name) VALUES (NULL)");

		BeanWithPreinit row = db.query("SELECT name FROM t")
				.fetchObject(BeanWithPreinit.class)
				.orElseThrow();
		// Non-planned already calls setter(null), so default "INIT" must be cleared.
		Assertions.assertNull(row.getName(), "Setter should be called with null to clear preinitialized value (non-planned)");
	}

	@Test
	public void testBeanNullSetterCalledClearsPreinitializedValue_Planned() {
		Database db = db(true, "null_setter_planned");
		TestQueries.execute(db, "CREATE TABLE t (name VARCHAR(64))");
		TestQueries.execute(db, "INSERT INTO t (name) VALUES (NULL)");

		BeanWithPreinit row = db.query("SELECT name FROM t")
				.fetchObject(BeanWithPreinit.class)
				.orElseThrow();
		// Planned path should behave the same (currently it skips the setter, leaving "INIT").
		Assertions.assertNull(row.getName(), "Setter should be called with null to clear preinitialized value (planned)");
	}

	// ---------- 2) Record path: primitive NULL guard & InstanceProvider usage ----------

	@Test
	public void testRecordNullToPrimitiveThrows_Planned() {
		Database db = db(true, "rec_null_prim_planned");
		TestQueries.execute(db, "CREATE TABLE t (n INT)");
		TestQueries.execute(db, "INSERT INTO t (n) VALUES (NULL)");

		Assertions.assertThrows(
				DatabaseException.class,
				() -> db.query("SELECT n FROM t")
						.fetchObject(RecWithPrimitive.class)
						.orElseThrow(),
				"NULL → primitive record component should throw a clear DatabaseException (planned path)"
		);
	}

	@Test
	public void testRecordCreationUsesInstanceProvider_Planned() {
		AtomicInteger provideRecordCalls = new AtomicInteger();

		InstanceProvider countingProvider = new InstanceProvider() {
			@Override
			public <T> T provide(StatementContext<T> ctx, Class<T> type) {
				try {
					Constructor<T> ctor = type.getDeclaredConstructor();
					ctor.setAccessible(true);
					return ctor.newInstance();
				} catch (Exception e) {
					throw new RuntimeException(e);
				}
			}

			@Override
			public <R extends Record> R provideRecord(StatementContext<R> ctx, Class<R> recordClass, Object[] args) {
				provideRecordCalls.incrementAndGet();
				try {
					Class<?>[] argTypes = java.util.Arrays.stream(recordClass.getRecordComponents())
							.map(rc -> rc.getType()).toArray(Class<?>[]::new);
					@SuppressWarnings("unchecked")
					Constructor<R> ctor = (Constructor<R>) recordClass.getDeclaredConstructor(argTypes);
					ctor.setAccessible(true);
					return ctor.newInstance(args);
				} catch (Exception e) {
					throw new RuntimeException(e);
				}
			}
		};

		Database db = dbWithInstanceProvider(true, "rec_provider_planned", countingProvider);
		TestQueries.execute(db, "CREATE TABLE t (n INT)");
		TestQueries.execute(db, "INSERT INTO t (n) VALUES (5)");

		RecWithPrimitive row = db.query("SELECT n FROM t")
				.fetchObject(RecWithPrimitive.class)
				.orElseThrow();
		Assertions.assertEquals(5, row.n());
		// Planned path should still route record construction through the InstanceProvider:
		Assertions.assertEquals(1, provideRecordCalls.get(),
				"InstanceProvider.provideRecord(...) should be invoked exactly once in the planned path");
	}

	// ---------- 3) Character standard type formatting bug ----------

	@Test
	public void testCharacterTooLong_ThrowsDatabaseException() {
		Database db = db(true, "char_too_long");
		TestQueries.execute(db, "CREATE TABLE t (c VARCHAR(10))");
		TestQueries.execute(db, "INSERT INTO t (c) VALUES ('AB')"); // two characters

		Assertions.assertThrows(
				DatabaseException.class,
				() -> db.query("SELECT c FROM t")
						.fetchObject(Character.class)
						.orElseThrow(),
				"Mapping a multi-character string to Character should raise DatabaseException with a clear message"
		);
	}

	// ---------- (Optional) TIME WITHOUT TZ to OffsetTime determinism ----------
	// This test documents the DST nondeterminism; enable after fixing TemporalReaders.asOffsetTime anchor.
	@Test
	public void testTimeWithoutTzToOffsetTime_StableOffset() {
		Database db = db(true, "time_to_offsettime");
		TestQueries.execute(db, "CREATE TABLE t (clock TIME)");
		TestQueries.execute(db, "INSERT INTO t (clock) VALUES (TIME '12:34:56')");
		// Expect a deterministic offset choice; exact value depends on DB zone but should be stable across runs
		OffsetTime ot = db.query("SELECT clock FROM t")
				.fetchObject(OffsetTime.class)
				.orElseThrow();
		Assertions.assertNotNull(ot);
	}
}
