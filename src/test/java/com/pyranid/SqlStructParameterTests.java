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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import javax.annotation.concurrent.ThreadSafe;
import javax.sql.DataSource;
import java.lang.reflect.Proxy;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Array;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Struct;
import java.sql.Types;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Verifies SQL STRUCT parameter construction, recursive JDBC materialization, and cleanup.
 *
 * @author <a href="https://www.revetkn.com">Mark Allen</a>
 * @since 4.7.0
 */
@ThreadSafe
public class SqlStructParameterTests {
	@Test
	public void testFactoriesDefensivelyCopyAttributesAndRepresentNullStruct() {
		Object[] attributes = new Object[]{"Ada", 37};
		SqlStructParameter arrayParameter = Parameters.sqlStructOf("STRUCT(name VARCHAR, age INTEGER)", attributes);
		attributes[0] = "mutated";

		Object[] firstRead = arrayParameter.getAttributes().orElseThrow();
		Assertions.assertArrayEquals(new Object[]{"Ada", 37}, firstRead);
		firstRead[1] = 99;
		Assertions.assertArrayEquals(new Object[]{"Ada", 37}, arrayParameter.getAttributes().orElseThrow());

		List<Object> list = new ArrayList<>(List.of("Grace", 41));
		SqlStructParameter listParameter = Parameters.sqlStructOf("STRUCT(name VARCHAR, age INTEGER)", list);
		list.set(0, "mutated");
		Assertions.assertArrayEquals(new Object[]{"Grace", 41}, listParameter.getAttributes().orElseThrow());

		SqlStructParameter nullParameter = Parameters.sqlStructOf(
				"STRUCT(name VARCHAR, age INTEGER)", (Object[]) null);
		Assertions.assertEquals("STRUCT(name VARCHAR, age INTEGER)", nullParameter.getTypeName());
		Assertions.assertTrue(nullParameter.getAttributes().isEmpty());
		Assertions.assertThrows(NullPointerException.class,
				() -> Parameters.sqlStructOf(null, List.of("Ada")));
	}

	@Test
	public void testBinderCreatesStructAndUsesUntypedSetObject() throws Exception {
		JdbcCapture capture = new JdbcCapture();
		PreparedStatement preparedStatement = capture.preparedStatement(false);
		StatementContext<?> statementContext = statementContext(DatabaseType.DUCK_DB, ZoneId.of("UTC"));

		new DefaultPreparedStatementBinder().bindParameter(statementContext, preparedStatement, 1,
				Parameters.sqlStructOf("STRUCT(name VARCHAR, age INTEGER)", List.of("Ada", 37)));

		Assertions.assertEquals(1, capture.structCalls.size());
		StructCall call = capture.structCalls.get(0);
		Assertions.assertEquals("STRUCT(name VARCHAR, age INTEGER)", call.typeName());
		Assertions.assertArrayEquals(new Object[]{"Ada", 37}, call.attributes());
		Assertions.assertEquals(1, capture.setObjectArgumentCounts.size());
		Assertions.assertEquals(2, capture.setObjectArgumentCounts.get(0),
				"DuckDB STRUCTs must bind with untyped setObject(index, value)");
		Assertions.assertSame(call.struct(), capture.boundObject.get());
	}

	@Test
	public void testNullStructBindsTypedNull() throws Exception {
		JdbcCapture capture = new JdbcCapture();
		PreparedStatement preparedStatement = capture.preparedStatement(false);
		StatementContext<?> statementContext = statementContext(DatabaseType.DUCK_DB, ZoneId.of("UTC"));

		new DefaultPreparedStatementBinder().bindParameter(statementContext, preparedStatement, 3,
				Parameters.sqlStructOf("STRUCT(name VARCHAR)", (List<?>) null));

		Assertions.assertEquals(List.of(new NullCall(3, Types.STRUCT, "STRUCT(name VARCHAR)")), capture.nullCalls);
		Assertions.assertTrue(capture.structCalls.isEmpty());
	}

	@Test
	public void testNestedStructAndArrayParametersMaterializeRecursivelyAndCleanUp() throws Exception {
		JdbcCapture capture = new JdbcCapture();
		PreparedStatement preparedStatement = capture.preparedStatement(false);
		StatementContext<?> statementContext = statementContext(DatabaseType.DUCK_DB, ZoneId.of("UTC"));
		SqlStructParameter person = Parameters.sqlStructOf("STRUCT(name VARCHAR)", List.of("Ada"));
		SqlStructParameter payload = Parameters.sqlStructOf(
				"STRUCT(person STRUCT(name VARCHAR), tags VARCHAR[], people STRUCT(name VARCHAR)[])",
				List.of(
						person,
						Parameters.sqlArrayOf("VARCHAR", List.of("engineer", "mathematician")),
						Parameters.sqlArrayOf("STRUCT(name VARCHAR)", List.of(person))));

		new DefaultPreparedStatementBinder().bindParameter(statementContext, preparedStatement, 1, payload);

		Assertions.assertEquals(3, capture.structCalls.size(),
				"Two nested STRUCT instances and the outer STRUCT should be materialized");
		Assertions.assertEquals(List.of("VARCHAR", "STRUCT(name VARCHAR)"), capture.arrayBaseTypeNames);
		Assertions.assertEquals(2, statementContext.getCleanupOperations().size());

		while (!statementContext.getCleanupOperations().isEmpty())
			statementContext.getCleanupOperations().remove().close();

		Assertions.assertEquals(1, capture.arrays.get(0).freeCalls.get());
		Assertions.assertEquals(1, capture.arrays.get(1).freeCalls.get());
	}

	@Test
	public void testSuccessfulCleanupFreesOuterArrayBeforeNestedArray() throws Exception {
		JdbcCapture capture = new JdbcCapture();
		PreparedStatement preparedStatement = capture.preparedStatement(false);
		StatementContext<?> statementContext = statementContext(DatabaseType.DUCK_DB, ZoneId.of("UTC"));
		SqlStructParameter payload = Parameters.sqlStructOf("STRUCT(matrix VARCHAR[][])", List.of(
				Parameters.sqlArrayOf("VARCHAR[]", List.of(
						Parameters.sqlArrayOf("VARCHAR", List.of("secret"))))));

		new DefaultPreparedStatementBinder().bindParameter(statementContext, preparedStatement, 1, payload);

		Assertions.assertEquals(List.of("VARCHAR", "VARCHAR[]"), capture.arrayBaseTypeNames,
				"Nested dependencies should be created before their containing array");
		while (!statementContext.getCleanupOperations().isEmpty())
			statementContext.getCleanupOperations().remove().close();

		Assertions.assertEquals(List.of(capture.arrays.get(1), capture.arrays.get(0)), capture.freedArrays,
				"Successful cleanup should free the outer array before its nested dependency");
	}

	@Test
	public void testNestedArraysAreFreedAndCleanupFailuresSuppressedWhenStructBindingFails() {
		JdbcCapture capture = new JdbcCapture();
		capture.arrayFreeFailure = new SQLException("free failed");
		PreparedStatement preparedStatement = capture.preparedStatement(true);
		StatementContext<?> statementContext = statementContext(DatabaseType.DUCK_DB, ZoneId.of("UTC"));
		SqlStructParameter payload = Parameters.sqlStructOf("STRUCT(tags VARCHAR[])",
				List.of(Parameters.sqlArrayOf("VARCHAR", List.of("secret"))));

		SQLException exception = Assertions.assertThrows(SQLException.class, () ->
				new DefaultPreparedStatementBinder().bindParameter(statementContext, preparedStatement, 1, payload));

		Assertions.assertEquals("bind failed", exception.getMessage());
		Assertions.assertEquals(1, capture.arrays.get(0).freeCalls.get());
		Assertions.assertEquals(1, exception.getSuppressed().length);
		Assertions.assertEquals("free failed", exception.getSuppressed()[0].getMessage());
		Assertions.assertTrue(statementContext.getCleanupOperations().isEmpty());
	}

	@Test
	public void testRuntimeArrayCleanupFailureIsSuppressedWhenStructBindingFails() {
		JdbcCapture capture = new JdbcCapture();
		capture.arrayFreeFailure = new IllegalStateException("runtime free failed");
		PreparedStatement preparedStatement = capture.preparedStatement(true);
		StatementContext<?> statementContext = statementContext(DatabaseType.DUCK_DB, ZoneId.of("UTC"));
		SqlStructParameter payload = Parameters.sqlStructOf("STRUCT(tags VARCHAR[])",
				List.of(Parameters.sqlArrayOf("VARCHAR", List.of("secret"))));

		SQLException exception = Assertions.assertThrows(SQLException.class, () ->
				new DefaultPreparedStatementBinder().bindParameter(statementContext, preparedStatement, 1, payload));

		Assertions.assertEquals(1, capture.arrays.get(0).freeCalls.get());
		Assertions.assertEquals(1, exception.getSuppressed().length);
		Assertions.assertInstanceOf(IllegalStateException.class, exception.getSuppressed()[0]);
		Assertions.assertEquals("runtime free failed", exception.getSuppressed()[0].getMessage());
		Assertions.assertTrue(statementContext.getCleanupOperations().isEmpty());
	}

	@Test
	public void testArrayCleanupDoesNotSelfSuppressPrimaryFailure() {
		JdbcCapture capture = new JdbcCapture();
		SQLException primaryFailure = new SQLException("same bind and cleanup failure");
		capture.setObjectFailure = primaryFailure;
		capture.arrayFreeFailure = primaryFailure;
		PreparedStatement preparedStatement = capture.preparedStatement(true);
		StatementContext<?> statementContext = statementContext(DatabaseType.DUCK_DB, ZoneId.of("UTC"));
		SqlStructParameter payload = Parameters.sqlStructOf("STRUCT(tags VARCHAR[])",
				List.of(Parameters.sqlArrayOf("VARCHAR", List.of("secret"))));

		SQLException exception = Assertions.assertThrows(SQLException.class, () ->
				new DefaultPreparedStatementBinder().bindParameter(statementContext, preparedStatement, 1, payload));

		Assertions.assertSame(primaryFailure, exception);
		Assertions.assertEquals(1, capture.arrays.get(0).freeCalls.get());
		Assertions.assertEquals(0, exception.getSuppressed().length);
		Assertions.assertTrue(statementContext.getCleanupOperations().isEmpty());
	}

	@Test
	public void testCycleHitsNestingBoundaryAfterArrayCreationAndFreesArray() {
		JdbcCapture capture = new JdbcCapture();
		PreparedStatement preparedStatement = capture.preparedStatement(false);
		StatementContext<?> statementContext = statementContext(DatabaseType.DUCK_DB, ZoneId.of("UTC"));
		SqlStructParameter cyclicStruct = new SqlStructParameter() {
			@Override
			public @NonNull String getTypeName() {
				return "STRUCT(next STRUCT(next INTEGER))";
			}

			@Override
			public @NonNull Optional<Object[]> getAttributes() {
				return Optional.of(new Object[]{this});
			}
		};
		SqlStructParameter payload = Parameters.sqlStructOf("STRUCT(tags VARCHAR[], recursive STRUCT(next INTEGER))",
				List.of(Parameters.sqlArrayOf("VARCHAR", List.of("created-first")), cyclicStruct));

		IllegalArgumentException exception = Assertions.assertThrows(IllegalArgumentException.class, () ->
				new DefaultPreparedStatementBinder().bindParameter(statementContext, preparedStatement, 1, payload));

		Assertions.assertTrue(exception.getMessage().contains("maximum depth of 64"));
		Assertions.assertEquals(1, capture.arrays.size(), "Expected the first attribute's array to be created");
		Assertions.assertEquals(1, capture.arrays.get(0).freeCalls.get());
		Assertions.assertEquals(List.of(capture.arrays.get(0)), capture.freedArrays);
		Assertions.assertTrue(statementContext.getCleanupOperations().isEmpty());
	}

	@Test
	public void testNestedTypedNullStructsAndArraysMaterializeAsNullAttributesAndElements() throws Exception {
		JdbcCapture capture = new JdbcCapture();
		PreparedStatement preparedStatement = capture.preparedStatement(false);
		StatementContext<?> statementContext = statementContext(DatabaseType.DUCK_DB, ZoneId.of("UTC"));
		SqlStructParameter nullStruct = Parameters.sqlStructOf("STRUCT(name VARCHAR)", (Object[]) null);
		SqlArrayParameter<Object> nullArray = Parameters.sqlArrayOf("VARCHAR", (Object[]) null);
		SqlStructParameter payload = Parameters.sqlStructOf(
				"STRUCT(person STRUCT(name VARCHAR), tags VARCHAR[], people STRUCT(name VARCHAR)[], matrix VARCHAR[][])",
				List.of(
						nullStruct,
						nullArray,
						Parameters.sqlArrayOf("STRUCT(name VARCHAR)", List.of(nullStruct)),
						Parameters.sqlArrayOf("VARCHAR[]", List.of(nullArray))));

		new DefaultPreparedStatementBinder().bindParameter(statementContext, preparedStatement, 1, payload);

		Assertions.assertEquals(1, capture.structCalls.size(), "Typed-null nested STRUCTs must not be materialized");
		Assertions.assertEquals(List.of("STRUCT(name VARCHAR)", "VARCHAR[]"), capture.arrayBaseTypeNames,
				"Only the non-null containing arrays should be materialized");
		Object[] outerAttributes = capture.structCalls.get(0).attributes();
		Assertions.assertNull(outerAttributes[0]);
		Assertions.assertNull(outerAttributes[1]);
		Assertions.assertSame(capture.arrays.get(0).proxy, outerAttributes[2]);
		Assertions.assertSame(capture.arrays.get(1).proxy, outerAttributes[3]);
		Assertions.assertArrayEquals(new Object[]{null}, (Object[]) capture.arrays.get(0).proxy.getArray());
		Assertions.assertArrayEquals(new Object[]{null}, (Object[]) capture.arrays.get(1).proxy.getArray());
		Assertions.assertEquals(2, statementContext.getCleanupOperations().size());
	}

	@Test
	public void testDuckDbStructAttributesNormalizeDriverUnsupportedValuesByAttributeType() throws Exception {
		JdbcCapture capture = new JdbcCapture();
		PreparedStatement preparedStatement = capture.preparedStatement(false);
		ZoneId timeZone = ZoneId.of("America/New_York");
		StatementContext<?> statementContext = statementContext(DatabaseType.DUCK_DB, timeZone);
		Instant instant = Instant.parse("2026-08-29T14:30:15.123456Z");
		OffsetDateTime offsetDateTime = instant.atOffset(ZoneOffset.ofHours(2));
		ZonedDateTime zonedDateTime = instant.atZone(ZoneId.of("Pacific/Auckland"));
		BigInteger hugeint = new BigInteger("170141183460469231731687303715884105727");
		OffsetTime offsetTime = OffsetTime.of(10, 30, 15, 0, ZoneOffset.ofHours(-4));

		new DefaultPreparedStatementBinder().bindParameter(statementContext, preparedStatement, 1,
				Parameters.sqlStructOf(
						"STRUCT(instant_local TIMESTAMP, offset_local TIMESTAMP WITHOUT TIME ZONE, zoned_local TIMESTAMP, "
								+ "instant_zoned TIMESTAMPTZ, offset_zoned TIMESTAMP WITH TIME ZONE, "
								+ "zoned_zoned TIMESTAMPTZ, amount DECIMAL(10,2), huge HUGEINT, clock TIMETZ)",
						List.of(instant, offsetDateTime, zonedDateTime, instant, offsetDateTime, zonedDateTime,
								new BigDecimal("12.34"), hugeint, offsetTime)));

		Object[] normalized = capture.structCalls.get(0).attributes();
		Assertions.assertEquals(LocalDateTime.ofInstant(instant, timeZone), normalized[0]);
		Assertions.assertEquals(LocalDateTime.ofInstant(instant, timeZone), normalized[1]);
		Assertions.assertEquals(LocalDateTime.ofInstant(instant, timeZone), normalized[2]);
		Assertions.assertEquals(OffsetDateTime.ofInstant(instant, ZoneOffset.UTC), normalized[3]);
		Assertions.assertEquals(OffsetDateTime.ofInstant(instant, ZoneOffset.UTC), normalized[4]);
		Assertions.assertEquals(OffsetDateTime.ofInstant(instant, ZoneOffset.UTC), normalized[5]);
		Assertions.assertEquals(new BigDecimal("12.34"), normalized[6]);
		Assertions.assertEquals(hugeint.toString(), normalized[7]);
		Assertions.assertEquals(offsetTime.toString(), normalized[8]);
	}

	@Test
	public void testDuckDbArrayElementsNormalizeInstantBearingValuesByTargetType() throws Exception {
		ZoneId timeZone = ZoneId.of("America/New_York");
		StatementContext<?> statementContext = statementContext(DatabaseType.DUCK_DB, ZoneId.of("America/New_York"));
		Instant instant = Instant.parse("2026-08-29T14:30:15.123456Z");
		OffsetDateTime offsetDateTime = instant.atOffset(ZoneOffset.ofHours(2));
		ZonedDateTime zonedDateTime = instant.atZone(ZoneId.of("Pacific/Auckland"));

		JdbcCapture localCapture = new JdbcCapture();
		new DefaultPreparedStatementBinder().bindParameter(statementContext, localCapture.preparedStatement(false), 1,
				Parameters.sqlArrayOf("TIMESTAMP", List.of(instant, offsetDateTime, zonedDateTime)));
		Assertions.assertArrayEquals(new Object[]{
				LocalDateTime.ofInstant(instant, timeZone),
				LocalDateTime.ofInstant(instant, timeZone),
				LocalDateTime.ofInstant(instant, timeZone)},
				(Object[]) localCapture.arrays.get(0).proxy.getArray());

		JdbcCapture zonedCapture = new JdbcCapture();
		new DefaultPreparedStatementBinder().bindParameter(statementContext, zonedCapture.preparedStatement(false), 1,
				Parameters.sqlArrayOf("TIMESTAMPTZ", List.of(instant, offsetDateTime, zonedDateTime)));
		Assertions.assertArrayEquals(new Object[]{
				OffsetDateTime.ofInstant(instant, ZoneOffset.UTC),
				OffsetDateTime.ofInstant(instant, ZoneOffset.UTC),
				OffsetDateTime.ofInstant(instant, ZoneOffset.UTC)},
				(Object[]) zonedCapture.arrays.get(0).proxy.getArray());
	}

	@Test
	public void testNamedStructAliasFailsClosedForInstantBearingAttributes() {
		StatementContext<?> statementContext = statementContext(DatabaseType.DUCK_DB, ZoneId.of("America/New_York"));
		Instant instant = Instant.parse("2026-08-29T14:30:15.123456Z");

		for (Object value : List.of(instant, instant.atOffset(ZoneOffset.ofHours(2)),
				instant.atZone(ZoneId.of("Pacific/Auckland")))) {
			JdbcCapture capture = new JdbcCapture();
			PreparedStatement preparedStatement = capture.preparedStatement(false);
			IllegalArgumentException exception = Assertions.assertThrows(IllegalArgumentException.class, () ->
					new DefaultPreparedStatementBinder().bindParameter(statementContext, preparedStatement, 1,
							Parameters.sqlStructOf("event_timestamp_payload", List.of(value))));

			Assertions.assertTrue(exception.getMessage().contains("timestamp semantics are unknown"));
			Assertions.assertTrue(exception.getMessage().contains("event_timestamp_payload"));
			Assertions.assertTrue(capture.structCalls.isEmpty());
		}
	}

	@Test
	public void testNamedArrayElementAliasFailsClosedForInstantBearingElements() {
		JdbcCapture capture = new JdbcCapture();
		StatementContext<?> statementContext = statementContext(DatabaseType.DUCK_DB, ZoneId.of("America/New_York"));
		Instant instant = Instant.parse("2026-08-29T14:30:15.123456Z");

		IllegalArgumentException exception = Assertions.assertThrows(IllegalArgumentException.class, () ->
				new DefaultPreparedStatementBinder().bindParameter(statementContext, capture.preparedStatement(false), 1,
						Parameters.sqlArrayOf("event_timestamp", List.of(instant))));

		Assertions.assertTrue(exception.getMessage().contains("timestamp semantics are unknown"));
		Assertions.assertTrue(exception.getMessage().contains("event_timestamp"));
		Assertions.assertTrue(capture.arrays.isEmpty());
	}

	@Test
	public void testTimestampLookingAliasFailsClosedForInstantBearingElements() {
		StatementContext<?> statementContext = statementContext(DatabaseType.DUCK_DB, ZoneId.of("America/New_York"));
		Instant instant = Instant.parse("2026-08-29T14:30:15.123456Z");

		JdbcCapture structCapture = new JdbcCapture();
		IllegalArgumentException structException = Assertions.assertThrows(IllegalArgumentException.class, () ->
				new DefaultPreparedStatementBinder().bindParameter(statementContext,
						structCapture.preparedStatement(false), 1,
						Parameters.sqlStructOf("STRUCT(ts TIMESTAMP_M)", List.of(instant))));
		Assertions.assertTrue(structException.getMessage().contains("timestamp semantics are unknown"));
		Assertions.assertTrue(structCapture.structCalls.isEmpty());

		JdbcCapture arrayCapture = new JdbcCapture();
		IllegalArgumentException arrayException = Assertions.assertThrows(IllegalArgumentException.class, () ->
				new DefaultPreparedStatementBinder().bindParameter(statementContext,
						arrayCapture.preparedStatement(false), 1,
						Parameters.sqlArrayOf("TIMESTAMP_M", List.of(instant))));
		Assertions.assertTrue(arrayException.getMessage().contains("timestamp semantics are unknown"));
		Assertions.assertTrue(arrayCapture.arrays.isEmpty());
	}

	@Test
	public void testUnsupportedDialectRejectsStructBeforeRequestingConnection() {
		JdbcCapture capture = new JdbcCapture();
		PreparedStatement preparedStatement = capture.preparedStatement(false);
		StatementContext<?> statementContext = statementContext(DatabaseType.POSTGRESQL, ZoneId.of("UTC"));

		IllegalArgumentException exception = Assertions.assertThrows(IllegalArgumentException.class, () ->
				new DefaultPreparedStatementBinder().bindParameter(statementContext, preparedStatement, 1,
						Parameters.sqlStructOf("person_type", List.of("Ada"))));

		Assertions.assertTrue(exception.getMessage().contains("SqlStructParameter is not supported"));
		Assertions.assertTrue(exception.getMessage().contains("DatabaseType.POSTGRESQL"));
		Assertions.assertEquals(0, capture.getConnectionCalls.get());
	}

	@Test
	public void testSecureStructScrubbingTraversesNestedStructsAndArrays() {
		SqlStructParameter nested = Parameters.sqlStructOf("STRUCT(token VARCHAR)",
				List.of(Parameters.secure("inner-secret", "<inner>")));
		SqlStructParameter payload = Parameters.sqlStructOf(
				"STRUCT(token VARCHAR, nested STRUCT(token VARCHAR), tokens VARCHAR[], typed_value VARCHAR)",
				List.of(
						Parameters.secure("outer-secret", "<attribute>"),
						nested,
						Parameters.sqlArrayOf("VARCHAR", List.of(
								Parameters.secure("array-secret", "<array-element>"))),
						Parameters.listOf(Object.class, List.of(List.of(
								Parameters.secure("typed-secret", "<typed-inner>"))))));
		SecureParameterSupport.DiagnosticScrub scrub = SecureParameterSupport.diagnosticScrubForParameters(
				List.of(Parameters.secure(payload, "<struct>")));

		Assertions.assertNotNull(scrub.redactor());
		String scrubbed = scrub.redactor().apply(
				"outer-secret inner-secret array-secret typed-secret remains-visible");
		Assertions.assertEquals("<struct> <struct> <struct> <struct> remains-visible", scrubbed);
	}

	@NonNull
	private StatementContext<?> statementContext(@NonNull DatabaseType databaseType,
																							 @NonNull ZoneId timeZone) {
		DataSource dataSource = (DataSource) Proxy.newProxyInstance(DataSource.class.getClassLoader(),
				new Class<?>[]{DataSource.class}, (proxy, method, args) -> defaultValue(method.getReturnType()));
		Database database = Database.withDataSource(dataSource)
				.databaseType(databaseType)
				.timeZone(timeZone)
				.build();
		return StatementContext.with(Statement.of("sql-struct-parameter-test", "SELECT ?"), database).build();
	}

	private static Object defaultValue(@NonNull Class<?> type) {
		if (!type.isPrimitive())
			return null;
		if (type == boolean.class)
			return false;
		if (type == byte.class)
			return (byte) 0;
		if (type == short.class)
			return (short) 0;
		if (type == int.class)
			return 0;
		if (type == long.class)
			return 0L;
		if (type == float.class)
			return 0.0f;
		if (type == double.class)
			return 0.0d;
		if (type == char.class)
			return '\0';
		return null;
	}

	private record NullCall(int parameterIndex,
													int sqlType,
													@NonNull String typeName) {}

	private record StructCall(@NonNull String typeName,
														@NonNull Object[] attributes,
														@NonNull Struct struct) {}

	private static final class TestStruct implements Struct {
		@NonNull
		private final String typeName;
		@NonNull
		private final Object[] attributes;

		private TestStruct(@NonNull String typeName,
													 Object @NonNull [] attributes) {
			this.typeName = typeName;
			this.attributes = attributes.clone();
		}

		@Override
		public String getSQLTypeName() {
			return this.typeName;
		}

		@Override
		public Object[] getAttributes() {
			return this.attributes.clone();
		}

		@Override
		public Object[] getAttributes(Map<String, Class<?>> map) {
			return getAttributes();
		}
	}

	private static final class TrackedArray {
		@NonNull
		private final Array proxy;
		@NonNull
		private final AtomicInteger freeCalls;

		private TrackedArray(@NonNull String baseTypeName,
										 Object @NonNull [] elements,
										 @Nullable Throwable freeFailure,
										 @NonNull List<TrackedArray> freedArrays) {
			this.freeCalls = new AtomicInteger();
			Object[] defensiveElements = elements.clone();
			this.proxy = (Array) Proxy.newProxyInstance(Array.class.getClassLoader(), new Class<?>[]{Array.class},
					(proxy, method, args) -> switch (method.getName()) {
						case "getBaseTypeName" -> baseTypeName;
						case "getArray" -> defensiveElements.clone();
						case "free" -> {
							this.freeCalls.incrementAndGet();
							freedArrays.add(this);
							if (freeFailure != null)
								throw freeFailure;
							yield null;
						}
						case "toString" -> "TrackedArray[" + baseTypeName + "]";
						default -> defaultValue(method.getReturnType());
					});
		}
	}

	private static final class JdbcCapture {
		@NonNull
		private final List<StructCall> structCalls = new ArrayList<>();
		@NonNull
		private final List<String> arrayBaseTypeNames = new ArrayList<>();
		@NonNull
		private final List<TrackedArray> arrays = new ArrayList<>();
		@NonNull
		private final List<TrackedArray> freedArrays = new ArrayList<>();
		@NonNull
		private final List<NullCall> nullCalls = new ArrayList<>();
		@NonNull
		private final List<Integer> setObjectArgumentCounts = new ArrayList<>();
		@NonNull
		private final AtomicReference<Object> boundObject = new AtomicReference<>();
		@NonNull
		private final AtomicInteger getConnectionCalls = new AtomicInteger();
		@Nullable
		private Throwable arrayFreeFailure;
		@Nullable
		private Throwable setObjectFailure;

		@NonNull
		private PreparedStatement preparedStatement(boolean failSetObject) {
			if (failSetObject && this.setObjectFailure == null)
				this.setObjectFailure = new SQLException("bind failed");

			Connection connection = (Connection) Proxy.newProxyInstance(Connection.class.getClassLoader(),
					new Class<?>[]{Connection.class}, (proxy, method, args) -> {
						switch (method.getName()) {
							case "createStruct" -> {
								String typeName = (String) args[0];
								Object[] attributes = ((Object[]) args[1]).clone();
								Struct struct = new TestStruct(typeName, attributes);
								this.structCalls.add(new StructCall(typeName, attributes, struct));
								return struct;
							}
							case "createArrayOf" -> {
								String baseTypeName = (String) args[0];
								Object[] elements = ((Object[]) args[1]).clone();
								TrackedArray trackedArray = new TrackedArray(baseTypeName, elements, this.arrayFreeFailure,
										this.freedArrays);
								this.arrayBaseTypeNames.add(baseTypeName);
								this.arrays.add(trackedArray);
								return trackedArray.proxy;
							}
							default -> {
								return defaultValue(method.getReturnType());
							}
						}
					});

			return (PreparedStatement) Proxy.newProxyInstance(PreparedStatement.class.getClassLoader(),
					new Class<?>[]{PreparedStatement.class}, (proxy, method, args) -> {
						switch (method.getName()) {
							case "getConnection" -> {
								this.getConnectionCalls.incrementAndGet();
								return connection;
							}
							case "getParameterMetaData" -> {
								return null;
							}
							case "setObject" -> {
								this.setObjectArgumentCounts.add(args.length);
								if (failSetObject)
									throw this.setObjectFailure;
								this.boundObject.set(args[1]);
								return null;
							}
							case "setNull" -> {
								this.nullCalls.add(new NullCall((Integer) args[0], (Integer) args[1],
										args.length == 3 ? (String) args[2] : ""));
								return null;
							}
							default -> {
								return defaultValue(method.getReturnType());
							}
						}
					});
		}
	}
}
