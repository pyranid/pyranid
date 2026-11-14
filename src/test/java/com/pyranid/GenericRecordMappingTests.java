/*
 * Copyright 2015-2022 Transmogrify LLC, 2022-2026 Revetware LLC.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may obtain a copy at https://www.apache.org/licenses/LICENSE-2.0
 */
package com.pyranid;

import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
import java.io.StringReader;
import java.lang.reflect.Proxy;
import java.sql.Clob;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class GenericRecordMappingTests {
	public record Box<T>(T value) {}
	public record Direct(Box<UUID> value) {}
	public record ListRow(List<Box<UUID>> value) {}
	public record MapRow(Map<String, Box<UUID>> value) {}
	public record Nested(Box<Map<UUID, List<Box<Long>>>> value) {}
	public record Bounded<T extends Number>(T value) {}
	public record BoundedRow(Bounded<Long> value) {}
	public record Text(Box<String> value) {}
	public record Native(Box<Object> value) {}
	public record InstantMap(Map<String, Box<Instant>> value) {}
	public record LocalMap(Map<String, Box<LocalDateTime>> value) {}
	public record OffsetMap(Map<String, Box<OffsetDateTime>> value) {}
	public static class GenericBean<T> {
		private Box<T> value;
		public void setValue(Box<T> value) { this.value = value; }
		public Box<T> getValue() { return value; }
	}
	public static class UuidBean extends GenericBean<UUID> {}

	@Test
	void directListAndMapRecordsRetainTheirTypeArgumentsWithEitherPlanMode() throws Exception {
		UUID uuid = UUID.randomUUID();
		Map<String, String> box = Map.of("value", uuid.toString());
		for (boolean cached : new boolean[]{false, true}) {
			ResultSetMapper mapper = ResultSetMapper.withPlanCachingEnabled(cached).build();
			for (int pass = 0; pass < 2; pass++) {
				assertEquals(uuid, map(mapper, box, Direct.class).value().value());
				assertEquals(Arrays.asList(new Box<>(uuid), null),
						map(mapper, Arrays.asList(box, null), ListRow.class).value());
				Map<String, Object> entries = new LinkedHashMap<>();
				entries.put("first", box); entries.put("missing", null);
				Map<String, Box<UUID>> actual = map(mapper, entries, MapRow.class).value();
				assertEquals(new Box<>(uuid), actual.get("first"));
				assertNull(actual.get("missing"));
				assertEquals(List.of("first", "missing"), List.copyOf(actual.keySet()));
				assertNull(map(mapper, null, Direct.class).value());
				assertNull(map(mapper, java.util.Collections.singletonMap("value", null), Direct.class).value().value());
			}
		}
	}

	@Test
	void nestedAndInheritedBindingsResolveWhileCanonicalConstructorsKeepErasedSignatures() throws Exception {
		UUID uuid = UUID.randomUUID();
		for (boolean cached : new boolean[]{false, true}) {
			ResultSetMapper mapper = ResultSetMapper.withPlanCachingEnabled(cached).build();
			Map<String, Object> source = Map.of("value", Map.of(uuid.toString(), List.of(Map.of("value", 7))));
			assertEquals(7L, map(mapper, source, Nested.class).value().value().get(uuid).get(0).value());
			assertEquals(uuid, map(mapper, Map.of("value", uuid.toString()), UuidBean.class).getValue().value());
			// Bounded<T>'s canonical parameter is Number, rather than its resolved Long argument.
			assertInstanceOf(Long.class, map(mapper, Map.of("value", 7), BoundedRow.class).value().value());
		}
	}

	@Test
	void genericLobTargetsRetainTypedLimitsAndNativeOwnership() throws Exception {
		for (boolean cached : new boolean[]{false, true}) {
			AtomicInteger reads = new AtomicInteger(), frees = new AtomicInteger();
			Clob clob = clob(reads, frees);
			ResultSetMapper mapper = ResultSetMapper.withPlanCachingEnabled(cached).maxLobCharacters(4L).build();
			assertEquals("text", map(mapper, Map.of("value", clob), Text.class).value().value());
			assertEquals(1, reads.get()); assertEquals(1, frees.get());
			reads.set(0); frees.set(0);
			assertSame(clob, map(mapper, Map.of("value", clob), Native.class).value().value());
			assertEquals(0, reads.get()); assertEquals(0, frees.get());
			ResultSetMapper detached = ResultSetMapper.withPlanCachingEnabled(cached).detachLobValues(true).maxLobCharacters(4L).build();
			assertEquals("text", map(detached, Map.of("value", clob), Native.class).value().value());
			assertEquals(1, reads.get()); assertEquals(1, frees.get());
			reads.set(0); frees.set(0);
			ResultSetMapper limited = ResultSetMapper.withPlanCachingEnabled(cached).maxLobCharacters(3L).build();
			assertThrows(DatabaseException.class, () -> map(limited, Map.of("value", clob), Text.class));
			assertEquals(1, reads.get()); assertEquals(1, frees.get());
		}
	}

	@Test
	void genericRecordTemporalComponentsUseResolvedTargetsAndSqlDeclarations() throws Exception {
		ZoneId zone = ZoneId.of("Asia/Tokyo");
		LocalDateTime wall = LocalDateTime.of(2024, 1, 2, 3, 4, 5);
		Instant instant = Instant.parse("2024-01-02T03:04:05Z");
		for (boolean cached : new boolean[]{false, true}) {
			ResultSetMapper mapper = ResultSetMapper.withPlanCachingEnabled(cached).build();
			for (boolean zoned : new boolean[]{false, true}) {
				Timestamp timestamp = zoned ? Timestamp.from(instant) : Timestamp.valueOf(wall);
				String declaration = "MAP(VARCHAR, STRUCT(value " + (zoned ? "TIMESTAMPTZ" : "TIMESTAMP") + "))";
				Map<String, Object> source = Map.of("item", Map.of("value", timestamp));
				assertEquals(zoned ? instant : wall.atZone(zone).toInstant(),
						map(mapper, source, InstantMap.class, DatabaseType.DUCK_DB, declaration, zone).value().get("item").value());
				assertEquals(zoned ? instant.atZone(zone).toLocalDateTime() : wall,
						map(mapper, source, LocalMap.class, DatabaseType.DUCK_DB, declaration, zone).value().get("item").value());
				assertEquals(zoned ? instant.atZone(zone).toOffsetDateTime() : wall.atZone(zone).toOffsetDateTime(),
						map(mapper, source, OffsetMap.class, DatabaseType.DUCK_DB, declaration, zone).value().get("item").value());
			}
		}
	}

	private static Clob clob(AtomicInteger reads, AtomicInteger frees) {
		return (Clob) Proxy.newProxyInstance(Clob.class.getClassLoader(), new Class<?>[]{Clob.class}, (proxy, method, args) -> switch (method.getName()) {
			case "getCharacterStream" -> { reads.incrementAndGet(); yield new StringReader("text"); }
			case "free" -> { frees.incrementAndGet(); yield null; }
			default -> throw new UnsupportedOperationException(method.getName());
		});
	}

	private static <T> T map(ResultSetMapper mapper, Object source, Class<T> rowType) throws Exception {
		return map(mapper, source, rowType, DatabaseType.GENERIC, "STRUCT", ZoneId.systemDefault());
	}

	private static <T> T map(ResultSetMapper mapper, Object source, Class<T> rowType, DatabaseType dialect, String typeName, ZoneId zone) throws Exception {
		DataSource dataSource = (DataSource) Proxy.newProxyInstance(DataSource.class.getClassLoader(), new Class<?>[]{DataSource.class},
				(proxy, method, args) -> { throw new UnsupportedOperationException(method.getName()); });
		Database database = Database.with(dataSource, dialect).timeZone(zone).build();
		StatementContext<T> context = StatementContext.<T>with(Statement.of("generic-record", "SELECT value"), database).resultSetRowType(rowType).build();
		ResultSetMetaData metadata = (ResultSetMetaData) Proxy.newProxyInstance(ResultSetMetaData.class.getClassLoader(), new Class<?>[]{ResultSetMetaData.class},
				(proxy, method, args) -> switch (method.getName()) {
					case "getColumnCount" -> 1;
					case "getColumnLabel", "getColumnName" -> "value";
					case "getColumnType" -> Types.JAVA_OBJECT;
					case "getColumnTypeName" -> typeName;
					default -> defaultValue(method.getReturnType());
				});
		ResultSet result = (ResultSet) Proxy.newProxyInstance(ResultSet.class.getClassLoader(), new Class<?>[]{ResultSet.class},
				(proxy, method, args) -> switch (method.getName()) {
					case "getMetaData" -> metadata;
					case "getObject" -> source;
					case "wasNull" -> source == null;
					default -> defaultValue(method.getReturnType());
				});
		return mapper.map(context, result, rowType, new InstanceProvider() {}).orElseThrow();
	}

	private static Object defaultValue(Class<?> type) {
		if (!type.isPrimitive()) return null;
		if (type == boolean.class) return false;
		if (type == int.class) return 0;
		if (type == long.class) return 0L;
		return null;
	}
}
