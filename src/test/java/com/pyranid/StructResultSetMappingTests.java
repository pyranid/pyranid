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

import javax.sql.DataSource;
import java.lang.reflect.Proxy;
import java.math.BigInteger;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * Verifies SQL STRUCT mapping: values surfaced by a dialect as attribute-name to value maps are converted
 * to record targets, recursively. Uses the {@link DatabaseType#GENERIC} dialect (which passes maps through
 * unchanged), so coverage here is independent of any specific database driver.
 *
 * @author <a href="https://www.revetkn.com">Mark Allen</a>
 * @since 4.7.0
 */
public class StructResultSetMappingTests {
	public record Person(String name, Integer age) {}

	public record PersonRow(Long id, Person person) {}

	public record Inner(Integer x, Integer y) {}

	public record NestedRow(Inner innerVal, String label) {}

	public record TeamRow(Long id, List<Person> people) {}

	public record TeamArrayRow(Long id, Person[] people) {}

	public record EmailPerson(String emailAddress) {}

	public record AliasedPerson(@DatabaseColumn("full_name") String name) {}

	public record PrimitivePerson(String name, int age) {}

	public record WideAge(Long age) {}

	public record BigNumber(BigInteger value) {}

	public record MapPayloadRow(Map<String, Object> payload) {}

	public static class PersonBean {
		private Person person;

		public Person getPerson() {
			return this.person;
		}

		public void setPerson(Person person) {
			this.person = person;
		}
	}

	private record Column(@NonNull String label,
												int sqlType,
												@NonNull String typeName,
												@Nullable Object value) {}

	@Test
	public void testStructColumnMapsToRecordComponent() throws Exception {
		Map<String, Object> struct = new LinkedHashMap<>();
		struct.put("name", "Ada");
		struct.put("age", 36);

		forBothPlanningModes(mapper -> {
			PersonRow row = map(mapper, PersonRow.class,
					new Column("id", Types.BIGINT, "BIGINT", 7L),
					new Column("person", Types.STRUCT, "STRUCT(\"name\" VARCHAR, age INTEGER)", struct))
					.orElseThrow();

			Assertions.assertEquals(7L, row.id());
			Assertions.assertEquals(new Person("Ada", 36), row.person());
		});
	}

	@Test
	public void testNestedStructMapsToNestedRecord() throws Exception {
		Map<String, Object> inner = new LinkedHashMap<>();
		inner.put("x", 1);
		inner.put("y", 2);
		Map<String, Object> struct = new LinkedHashMap<>();
		struct.put("inner_val", inner);
		struct.put("label", "pt");

		forBothPlanningModes(mapper -> {
			NestedRow row = map(mapper, NestedRow.class,
					new Column("nested", Types.STRUCT, "STRUCT(inner_val STRUCT(x INTEGER, y INTEGER), \"label\" VARCHAR)", struct))
					.orElseThrow();

			Assertions.assertEquals(new Inner(1, 2), row.innerVal());
			Assertions.assertEquals("pt", row.label());
		});
	}

	@Test
	public void testListOfStructsMapsToRecordList() throws Exception {
		Map<String, Object> first = new LinkedHashMap<>();
		first.put("name", "Ada");
		first.put("age", 36);
		Map<String, Object> second = new LinkedHashMap<>();
		second.put("name", "Grace");
		second.put("age", 45);

		forBothPlanningModes(mapper -> {
			TeamRow row = map(mapper, TeamRow.class,
					new Column("id", Types.BIGINT, "BIGINT", 1L),
					new Column("people", Types.ARRAY, "STRUCT(\"name\" VARCHAR, age INTEGER)[]", new Object[]{first, second}))
					.orElseThrow();

			Assertions.assertEquals(List.of(new Person("Ada", 36), new Person("Grace", 45)), row.people());
		});
	}

	@Test
	public void testListOfStructsMapsToRecordArrayComponent() throws Exception {
		Map<String, Object> first = new LinkedHashMap<>();
		first.put("name", "Ada");
		first.put("age", 36);
		Map<String, Object> second = new LinkedHashMap<>();
		second.put("name", "Grace");
		second.put("age", 45);

		forBothPlanningModes(mapper -> {
			TeamArrayRow row = map(mapper, TeamArrayRow.class,
					new Column("id", Types.BIGINT, "BIGINT", 1L),
					new Column("people", Types.ARRAY, "STRUCT(\"name\" VARCHAR, age INTEGER)[]", new Object[]{first, second}))
					.orElseThrow();

			Assertions.assertArrayEquals(new Person[]{new Person("Ada", 36), new Person("Grace", 45)}, row.people());
		});
	}

	@Test
	public void testSingleStructColumnFallbackHonorsConfiguredInstanceProviderForTopLevelRecord() throws Exception {
		Map<String, Object> struct = new LinkedHashMap<>();
		struct.put("name", "Ada");
		struct.put("age", 36);

		forBothPlanningModes(mapper -> {
			List<Class<?>> providedRecordTypes = new java.util.ArrayList<>();
			InstanceProvider countingInstanceProvider = new InstanceProvider() {
				@Override
				public <R extends Record> R provideRecord(StatementContext<R> statementContext,
																									Class<R> recordType,
																									Object... initargs) {
					providedRecordTypes.add(recordType);
					return InstanceProvider.super.provideRecord(statementContext, recordType, initargs);
				}
			};

			StatementContext<Person> statementContext = StatementContext.<Person>with(
							Statement.of("struct-instance-provider-test", "SELECT 1"), database())
					.resultSetRowType(Person.class)
					.build();

			Person person = mapper.map(statementContext,
							resultSet(new Column("person", Types.STRUCT, "STRUCT(\"name\" VARCHAR, age INTEGER)", struct)),
							Person.class, countingInstanceProvider)
					.orElseThrow();

			Assertions.assertEquals(new Person("Ada", 36), person);
			Assertions.assertEquals(List.of(Person.class), providedRecordTypes,
					"The single-STRUCT-column fallback should construct the top-level row record via the configured InstanceProvider");
		});
	}

	@Test
	public void testStructAttributeNameNormalizationMatchesColumnConventions() throws Exception {
		Map<String, Object> struct = new LinkedHashMap<>();
		struct.put("email_address", "ada@example.com");

		forBothPlanningModes(mapper -> {
			EmailPerson person = map(mapper, EmailPerson.class,
					new Column("person", Types.STRUCT, "STRUCT(email_address VARCHAR)", struct))
					.orElseThrow();

			Assertions.assertEquals("ada@example.com", person.emailAddress());
		});
	}

	@Test
	public void testStructAttributeRespectsDatabaseColumnAlias() throws Exception {
		Map<String, Object> struct = new LinkedHashMap<>();
		struct.put("full_name", "Ada Lovelace");

		forBothPlanningModes(mapper -> {
			AliasedPerson person = map(mapper, AliasedPerson.class,
					new Column("person", Types.STRUCT, "STRUCT(full_name VARCHAR)", struct))
					.orElseThrow();

			Assertions.assertEquals("Ada Lovelace", person.name());
		});
	}

	@Test
	public void testStructAttributeValuesConvertToComponentTypes() throws Exception {
		Map<String, Object> wideStruct = new LinkedHashMap<>();
		wideStruct.put("age", 36); // Integer attribute, Long component

		Map<String, Object> bigStruct = new LinkedHashMap<>();
		bigStruct.put("value", new BigInteger("170141183460469231731687303715884105727"));

		forBothPlanningModes(mapper -> {
			WideAge wideAge = map(mapper, WideAge.class,
					new Column("w", Types.STRUCT, "STRUCT(age INTEGER)", wideStruct))
					.orElseThrow();
			Assertions.assertEquals(36L, wideAge.age());

			BigNumber bigNumber = map(mapper, BigNumber.class,
					new Column("b", Types.STRUCT, "STRUCT(\"value\" HUGEINT)", bigStruct))
					.orElseThrow();
			Assertions.assertEquals(new BigInteger("170141183460469231731687303715884105727"), bigNumber.value());
		});
	}

	@Test
	public void testSingleStructColumnMapsDirectlyToRecord() throws Exception {
		Map<String, Object> struct = new LinkedHashMap<>();
		struct.put("name", "Ada");
		struct.put("age", 36);

		forBothPlanningModes(mapper -> {
			Person person = map(mapper, Person.class,
					new Column("person", Types.STRUCT, "STRUCT(\"name\" VARCHAR, age INTEGER)", struct))
					.orElseThrow();

			Assertions.assertEquals(new Person("Ada", 36), person);
		});
	}

	@Test
	public void testSingleNullStructColumnMapsToEmpty() throws Exception {
		forBothPlanningModes(mapper -> {
			Optional<Person> person = map(mapper, Person.class,
					new Column("person", Types.STRUCT, "STRUCT(\"name\" VARCHAR, age INTEGER)", null));

			Assertions.assertTrue(person.isEmpty());
		});
	}

	@Test
	public void testRowMappingTakesPrecedenceOverSingleStructColumnFallback() throws Exception {
		Map<String, Object> struct = new LinkedHashMap<>();
		struct.put("name", "Ada");

		forBothPlanningModes(mapper -> {
			// The single STRUCT column's label matches a record component, so ordinary row mapping applies
			// and the component receives the raw attribute map rather than triggering struct-to-record fallback
			MapPayloadRow row = map(mapper, MapPayloadRow.class,
					new Column("payload", Types.STRUCT, "STRUCT(\"name\" VARCHAR)", struct))
					.orElseThrow();

			Assertions.assertEquals(struct, row.payload());
		});
	}

	@Test
	public void testStructColumnMapsToBeanPropertyOfRecordType() throws Exception {
		Map<String, Object> struct = new LinkedHashMap<>();
		struct.put("name", "Ada");
		struct.put("age", 36);

		forBothPlanningModes(mapper -> {
			PersonBean bean = map(mapper, PersonBean.class,
					new Column("person", Types.STRUCT, "STRUCT(\"name\" VARCHAR, age INTEGER)", struct))
					.orElseThrow();

			Assertions.assertEquals(new Person("Ada", 36), bean.getPerson());
		});
	}

	@Test
	public void testNullAndMissingStructAttributesMapToNullComponents() throws Exception {
		Map<String, Object> struct = new LinkedHashMap<>();
		struct.put("name", null); // present but NULL; "age" entirely absent

		forBothPlanningModes(mapper -> {
			Person person = map(mapper, Person.class,
					new Column("person", Types.STRUCT, "STRUCT(\"name\" VARCHAR)", struct))
					.orElseThrow();

			Assertions.assertNull(person.name());
			Assertions.assertNull(person.age());
		});
	}

	@Test
	public void testNullStructAttributeForPrimitiveComponentThrows() throws Exception {
		Map<String, Object> struct = new LinkedHashMap<>();
		struct.put("name", "Ada");
		struct.put("age", null);

		forBothPlanningModes(mapper -> {
			DatabaseException exception = Assertions.assertThrows(DatabaseException.class, () ->
					map(mapper, PrimitivePerson.class,
							new Column("person", Types.STRUCT, "STRUCT(\"name\" VARCHAR, age INTEGER)", struct)));

			Assertions.assertTrue(exception.getMessage().contains("STRUCT attribute 'age' is NULL"),
					"Unexpected message: " + exception.getMessage());
			Assertions.assertTrue(exception.getMessage().contains("primitive"));
		});
	}

	@Test
	public void testMissingStructAttributeForPrimitiveComponentThrows() throws Exception {
		Map<String, Object> struct = new LinkedHashMap<>();
		struct.put("name", "Ada");

		forBothPlanningModes(mapper -> {
			DatabaseException exception = Assertions.assertThrows(DatabaseException.class, () ->
					map(mapper, PrimitivePerson.class,
							new Column("person", Types.STRUCT, "STRUCT(\"name\" VARCHAR)", struct)));

			Assertions.assertTrue(exception.getMessage().contains("No STRUCT attribute matches record component 'age'"),
					"Unexpected message: " + exception.getMessage());
		});
	}

	@Test
	public void testStructWithNoMatchingAttributesThrows() throws Exception {
		Map<String, Object> struct = new LinkedHashMap<>();
		struct.put("unrelated", "value");

		forBothPlanningModes(mapper -> {
			DatabaseException exception = Assertions.assertThrows(DatabaseException.class, () ->
					map(mapper, Person.class,
							new Column("person", Types.STRUCT, "STRUCT(unrelated VARCHAR)", struct)));

			Assertions.assertTrue(exception.getMessage().contains("No STRUCT attributes map to record components"),
					"Unexpected message: " + exception.getMessage());
		});
	}

	@Test
	public void testDuplicateNormalizedStructAttributeNamesThrow() throws Exception {
		Map<String, Object> struct = new LinkedHashMap<>();
		struct.put("Name", "Ada");
		struct.put("name", "Grace");

		forBothPlanningModes(mapper -> {
			DatabaseException exception = Assertions.assertThrows(DatabaseException.class, () ->
					map(mapper, Person.class,
							new Column("person", Types.STRUCT, "STRUCT(\"Name\" VARCHAR, \"name\" VARCHAR)", struct)));

			Assertions.assertTrue(exception.getMessage().contains("Duplicate STRUCT attribute name"),
					"Unexpected message: " + exception.getMessage());
		});
	}

	@Test
	public void testMismatchedStructAttributeTypeThrows() throws Exception {
		Map<String, Object> struct = new LinkedHashMap<>();
		struct.put("name", new Object());
		struct.put("age", 36);

		forBothPlanningModes(mapper -> {
			DatabaseException exception = Assertions.assertThrows(DatabaseException.class, () ->
					map(mapper, Person.class,
							new Column("person", Types.STRUCT, "STRUCT(\"name\" VARCHAR, age INTEGER)", struct)));

			Assertions.assertTrue(exception.getMessage().contains("record component 'name'"),
					"Unexpected message: " + exception.getMessage());
		});
	}

	@Test
	public void testSingleNonStructColumnWithNoMappingsStillThrowsOriginalException() throws Exception {
		forBothPlanningModes(mapper -> {
			DatabaseException exception = Assertions.assertThrows(DatabaseException.class, () ->
					map(mapper, Person.class,
							new Column("unrelated", Types.INTEGER, "INTEGER", 1)));

			Assertions.assertTrue(exception.getMessage().contains("No result columns map to record components"),
					"Unexpected message: " + exception.getMessage());
		});
	}

	// ---------- Test harness ----------

	@FunctionalInterface
	private interface MappingProbe {
		void run(@NonNull DefaultResultSetMapper mapper) throws Exception;
	}

	private void forBothPlanningModes(@NonNull MappingProbe probe) throws Exception {
		requireNonNull(probe);

		for (boolean planCachingEnabled : new boolean[]{true, false}) {
			DefaultResultSetMapper mapper =
					(DefaultResultSetMapper) ResultSetMapper.withPlanCachingEnabled(planCachingEnabled).build();
			probe.run(mapper);
		}
	}

	@NonNull
	private <T> Optional<T> map(@NonNull DefaultResultSetMapper mapper,
															@NonNull Class<T> rowType,
															@NonNull Column... columns) throws SQLException {
		requireNonNull(mapper);
		requireNonNull(rowType);
		requireNonNull(columns);

		StatementContext<T> statementContext = StatementContext.<T>with(
						Statement.of("struct-mapping-test", "SELECT 1"), database())
				.resultSetRowType(rowType)
				.build();

		return mapper.map(statementContext, resultSet(columns), rowType, new InstanceProvider() {});
	}

	@NonNull
	private Database database() {
		DataSource dataSource = (DataSource) Proxy.newProxyInstance(DataSource.class.getClassLoader(),
				new Class<?>[]{DataSource.class},
				(proxy, method, args) -> {
					throw new UnsupportedOperationException("This test should not touch the DataSource");
				});

		return Database.withDataSource(dataSource)
				.databaseType(DatabaseType.GENERIC)
				.build();
	}

	@NonNull
	private ResultSet resultSet(@NonNull Column... columns) {
		requireNonNull(columns);

		ResultSetMetaData resultSetMetaData = resultSetMetaData(columns);

		return (ResultSet) Proxy.newProxyInstance(ResultSet.class.getClassLoader(), new Class<?>[]{ResultSet.class},
				(proxy, method, args) -> {
					return switch (method.getName()) {
						case "getMetaData" -> resultSetMetaData;
						case "getObject" -> args[0] instanceof Integer columnIndex ? columns[columnIndex - 1].value() : null;
						case "wasNull" -> false;
						case "toString" -> "ResultSet<struct-mapping-test>";
						default -> defaultValue(method.getReturnType());
					};
				});
	}

	@NonNull
	private ResultSetMetaData resultSetMetaData(@NonNull Column... columns) {
		requireNonNull(columns);

		return (ResultSetMetaData) Proxy.newProxyInstance(ResultSetMetaData.class.getClassLoader(),
				new Class<?>[]{ResultSetMetaData.class},
				(proxy, method, args) -> {
					return switch (method.getName()) {
						case "getColumnCount" -> columns.length;
						case "getColumnLabel", "getColumnName" -> columns[(Integer) args[0] - 1].label();
						case "getColumnType" -> columns[(Integer) args[0] - 1].sqlType();
						case "getColumnTypeName" -> columns[(Integer) args[0] - 1].typeName();
						case "toString" -> "ResultSetMetaData<struct-mapping-test>";
						default -> defaultValue(method.getReturnType());
					};
				});
	}

	@Nullable
	private Object defaultValue(@NonNull Class<?> returnType) {
		requireNonNull(returnType);

		if (!returnType.isPrimitive())
			return null;
		if (returnType == boolean.class)
			return false;
		if (returnType == byte.class)
			return (byte) 0;
		if (returnType == short.class)
			return (short) 0;
		if (returnType == int.class)
			return 0;
		if (returnType == long.class)
			return 0L;
		if (returnType == float.class)
			return 0.0f;
		if (returnType == double.class)
			return 0.0d;
		if (returnType == char.class)
			return '\0';
		return null;
	}
}
