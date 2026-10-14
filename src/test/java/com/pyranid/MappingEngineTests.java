/*
 * Copyright 2015-2022 Transmogrify LLC, 2022-2026 Revetware LLC.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy at http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.pyranid;

import org.hsqldb.jdbc.JDBCDataSource;
import org.junit.jupiter.api.Test;
import java.lang.reflect.Proxy;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Types;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import static org.junit.jupiter.api.Assertions.*;

class MappingEngineTests {
	public record InstantRow(Instant value) {}
	public record OrderedRow(String first, String second) {}

	@Test
	void cacheSettingsChangeRetentionOnlyAndPlansUseTheCurrentTimezone() {
		LocalDateTime wallClock = LocalDateTime.parse("2024-03-10T02:30:00");
		for (boolean cached : new boolean[]{false, true}) {
			DefaultResultSetMapper mapper = (DefaultResultSetMapper) ResultSetMapper.withPlanCachingEnabled(cached).build();
			for (ZoneId zone : List.of(ZoneId.of("UTC"), ZoneId.of("America/New_York"), ZoneId.of("Asia/Kathmandu"), ZoneId.of("UTC"))) {
				Database database = database(mapper, zone);
				Instant actual = database.query("SELECT CAST(:value AS TIMESTAMP) AS value FROM (VALUES (0))")
						.bind("value", wallClock).fetchObject(InstantRow.class).orElseThrow().value();
				assertEquals(wallClock.atZone(zone).toInstant(), actual);
			}
			assertEquals(cached ? 1 : 0, mapper.getRowPlanningCacheForResultClass(InstantRow.class).size());
		}
	}

	@Test
	void customHookOrderAndFallbackDoNotDependOnPlanRetention() {
		for (boolean cached : new boolean[]{false, true}) {
			List<String> calls = new ArrayList<>();
			CustomColumnMapper fallback = new CustomColumnMapper() {
				@Override public Boolean appliesTo(TargetType type) { return type.getRawClass() == String.class; }
				@Override public MappingResult map(StatementContext<?> context, ResultSet resultSet, Object value,
						TargetType target, Integer index, String label, InstanceProvider provider) {
					calls.add("fallback:" + label);
					return MappingResult.fallback();
				}
			};
			CustomColumnMapper handled = new CustomColumnMapper() {
				@Override public Boolean appliesTo(TargetType type) { return type.getRawClass() == String.class; }
				@Override public MappingResult map(StatementContext<?> context, ResultSet resultSet, Object value,
						TargetType target, Integer index, String label, InstanceProvider provider) {
					calls.add("handled:" + label);
					return MappingResult.of(label.equals("first") ? null : "mapped");
				}
			};
			Database database = database(ResultSetMapper.withPlanCachingEnabled(cached).customColumnMappers(List.of(fallback, handled)).build(), ZoneId.of("UTC"));
			for (int execution = 0; execution < 2; execution++) {
				calls.clear();
				OrderedRow row = database.query("SELECT 'b' AS second, 'a' AS first FROM (VALUES (0))").fetchObject(OrderedRow.class).orElseThrow();
				assertEquals(new OrderedRow(null, "mapped"), row);
				assertEquals(List.of("fallback:second", "handled:second", "fallback:first", "handled:first"), calls);
			}
		}
	}

	@Test
	void schemaKeysDoNotCollideWhenLabelsAndTypeNamesContainDelimiters() throws Exception {
		DefaultResultSetMapper mapper = (DefaultResultSetMapper) ResultSetMapper.withDefaultConfiguration();
		StatementContext<Object> context = StatementContext.with(Statement.of("schema", "SELECT columns"), database(mapper, ZoneId.of("UTC"))).build();
		ResultSetMetaData first = metadata(new String[]{"a:4:t:z:1|b", "c"}, new String[]{"2", "3"});
		ResultSetMetaData second = metadata(new String[]{"a", "b:4:t:z:2|c"}, new String[]{"1", "3"});
		assertNotEquals(mapper.schemaSignatureFor(context, first), mapper.schemaSignatureFor(context, second));
	}

	@Test
	void scalarIdentityPathMatchesEveryBuiltInDialect() {
		DefaultResultSetMapper mapper = (DefaultResultSetMapper) ResultSetMapper.withDefaultConfiguration();
		Object[] values = {"value", 1000, 1000L, (short) 1000, (byte) 100, true, 'x',
				new java.math.BigInteger("1000"), new java.math.BigDecimal("1000.25"), new byte[]{1, 2}};
		for (DatabaseType databaseType : DatabaseType.values()) {
			JDBCDataSource source = new JDBCDataSource();
			Database database = Database.withDataSource(source).databaseType(databaseType).build();
			StatementContext<Object> context = StatementContext.with(Statement.of("identity", "SELECT value"), database).build();
			for (Object value : values) {
				assertSame(value, context.getDatabaseDialect().unwrapResultSetValue(value), databaseType.toString());
				assertSame(value, mapper.convertResultSetValueToTargetType(context, value, TargetType.of(value.getClass())).orElseThrow());
			}
			assertThrows(DatabaseException.class, () -> mapper.convertResultSetValueToTargetType(context, Double.NaN, TargetType.of(Double.class)));
			assertThrows(DatabaseException.class, () -> mapper.convertResultSetValueToTargetType(context, Double.POSITIVE_INFINITY, TargetType.of(Float.class)));
		}
	}

	private ResultSetMetaData metadata(String[] labels, String[] types) {
		return (ResultSetMetaData) Proxy.newProxyInstance(ResultSetMetaData.class.getClassLoader(), new Class<?>[]{ResultSetMetaData.class},
				(proxy, method, arguments) -> switch (method.getName()) {
					case "getColumnCount" -> labels.length;
					case "getColumnLabel" -> labels[(int) arguments[0] - 1];
					case "getColumnTypeName" -> types[(int) arguments[0] - 1];
					case "getColumnType" -> Types.INTEGER;
					case "hashCode" -> System.identityHashCode(proxy);
					case "equals" -> proxy == arguments[0];
					default -> throw new AssertionError(method.getName());
				});
	}

	private Database database(ResultSetMapper mapper, ZoneId zone) {
		JDBCDataSource source = new JDBCDataSource();
		source.setUrl("jdbc:hsqldb:mem:mapping_engine");
		source.setUser("sa");
		return Database.withDataSource(source).databaseType(DatabaseType.GENERIC).timeZone(zone).resultSetMapper(mapper).build();
	}
}
