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

import java.io.ByteArrayInputStream;
import java.io.StringReader;
import java.lang.reflect.Proxy;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.NClob;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.LinkedHashMap;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Set;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

class MappingOwnershipRegressionTests {
	public record Returned(byte[] body, Object returned) {}
	public record BinaryOuter(byte[] body, Integer trigger) {}
	public record TextOuter(String body, Integer trigger) {}
	public record OneShotRow(OneShot value) {}
	public static final class OneShot implements Iterable<Integer> {
		private final Iterator<Integer> values = List.of(1, 2, 3).iterator();
		int traversals;
		@Override public Iterator<Integer> iterator() { this.traversals++; return this.values; }
	}

	@Test
	void customScalarAndRecordOutputsDoNotConsumeOneShotIterables() throws Exception {
		for (boolean cached : new boolean[]{false, true}) {
			for (boolean scalar : new boolean[]{false, true}) {
				OneShot output = new OneShot();
				ResultSetMapper mapper = ResultSetMapper.withPlanCachingEnabled(cached).customColumnMappers(List.of(new CustomColumnMapper() {
					@Override public Boolean appliesTo(TargetType target) { return target.getRawClass() == OneShot.class; }
					@Override public MappingResult map(StatementContext<?> context, ResultSet rs, Object value,
							TargetType target, Integer column, String label, InstanceProvider provider) { return MappingResult.of(output); }
				})).build();
				try (ResultSet rs = row(new String[]{"value"}, new int[]{Types.INTEGER}, new Object[]{1})) {
					Object result = scalar ? map(mapper, OneShot.class, rs) : map(mapper, OneShotRow.class, rs).value();
					assertSame(output, result);
					assertEquals(0, output.traversals);
					List<Integer> values = new ArrayList<>();
					output.forEach(values::add);
					assertEquals(List.of(1, 2, 3), values);
				}
			}
		}
	}

	@Test
	void opaqueCustomContainersInArraysBorrowAncestorHandlesWithoutTraversal() throws Exception {
		for (boolean cached : new boolean[]{false, true}) {
			for (boolean mapOutput : new boolean[]{false, true}) {
				Lob lob = new Lob(true);
				Object output = mapOutput ? new AbstractMap<String, Object>() {
					@Override public Set<Entry<String, Object>> entrySet() { throw new AssertionError("must not traverse custom map"); }
					@Override public Object get(Object key) { return lob.handle; }
				} : (Iterable<Object>) () -> { throw new AssertionError("must not traverse custom iterable"); };
				ResultSetMapper[] mapper = new ResultSetMapper[1];
				mapper[0] = ResultSetMapper.withPlanCachingEnabled(cached).customColumnMappers(List.of(new CustomColumnMapper() {
					@Override public Boolean appliesTo(TargetType target) { return target.getRawClass() == Integer.class || target.getRawClass() == Object[].class; }
					@Override public MappingResult map(StatementContext<?> context, ResultSet rs, Object value,
							TargetType target, Integer column, String label, InstanceProvider provider) throws SQLException {
						if (target.getRawClass() == Object[].class) return MappingResult.of(new Object[]{output});
						try (ResultSet inner = row(new String[]{"value"}, new int[]{Types.VARCHAR}, new Object[]{"trigger"})) {
							assertSame(output, MappingOwnershipRegressionTests.map(mapper[0], Object[].class, inner)[0]);
						}
						return MappingResult.of(42);
					}
				})).build();
				try (ResultSet rs = row(new String[]{"body", "trigger"}, new int[]{Types.BLOB, Types.VARCHAR}, new Object[]{lob.handle, "trigger"})) {
					assertEquals(42, map(mapper[0], BinaryOuter.class, rs).trigger());
					lob.assertReadableAndBorrowed();
				}
				// Opaque borrowing must not leak into the mapper's next row.
				Lob detached = new Lob(true);
				try (ResultSet rs = row(new String[]{"body"}, new int[]{Types.BLOB}, new Object[]{detached.handle})) {
					map(mapper[0], Returned.class, rs);
					assertEquals(1, detached.frees);
				}
			}
		}
	}

	@Test
	void customOutputsBorrowVisibleAliasesAlreadyMaterializedInTheRow() throws Exception {
		for (boolean cached : new boolean[]{false, true}) {
			for (int shape = 0; shape < 4; shape++) {
				Lob lob = new Lob(true);
				int outputShape = shape;
				ResultSetMapper mapper = ResultSetMapper.withPlanCachingEnabled(cached).customColumnMappers(List.of(new CustomColumnMapper() {
					@Override public Boolean appliesTo(TargetType target) { return target.getRawClass() == Object.class; }
					@Override public MappingResult map(StatementContext<?> context, ResultSet rs, Object value,
							TargetType target, Integer column, String label, InstanceProvider provider) throws SQLException {
						assertEquals("trigger", value); // The input is not the borrowed LOB; the output is.
						Blob handle = rs.getBlob(1);
						return MappingResult.of(switch (outputShape) {
							case 0 -> handle;
							case 1 -> new Object[]{handle};
							case 2 -> List.of(handle);
							default -> Map.of("handle", handle);
						});
					}
				})).build();
				try (ResultSet rs = row(new String[]{"body", "returned"}, new int[]{Types.BLOB, Types.VARCHAR},
						new Object[]{lob.handle, "trigger"})) {
					Returned result = map(mapper, Returned.class, rs);
					assertArrayEquals(new byte[]{1, 2}, result.body());
					assertNotNull(result.returned());
					assertFalse(rs.isClosed());
					lob.assertReadableAndBorrowed();
				}
			}
		}
	}

	@Test
	void reentrantRawScalarAndMapOutputsCancelAncestorOwnership() throws Exception {
		for (boolean cached : new boolean[]{false, true}) {
			for (boolean binary : new boolean[]{false, true}) {
				List<Class<?>> targets = binary ? List.of(Object.class, Blob.class, Map.class, LinkedHashMap.class)
						: List.of(Object.class, Clob.class, NClob.class, Map.class, LinkedHashMap.class);
				for (Class<?> rawTarget : targets) {
					Lob lob = new Lob(binary);
					AtomicReference<Object> retained = new AtomicReference<>();
					ResultSetMapper[] mapper = new ResultSetMapper[1];
					mapper[0] = ResultSetMapper.withPlanCachingEnabled(cached).customColumnMappers(List.of(new CustomColumnMapper() {
						@Override public Boolean appliesTo(TargetType target) { return target.getRawClass() == Integer.class; }
						@Override public MappingResult map(StatementContext<?> context, ResultSet rs, Object value,
								TargetType target, Integer column, String label, InstanceProvider provider) throws SQLException {
							assertEquals("trigger", value);
							try (ResultSet inner = row(new String[]{"value"}, new int[]{binary ? Types.BLOB : Types.CLOB}, new Object[]{lob.handle})) {
								Object result = MappingOwnershipRegressionTests.map(mapper[0], rawTarget, inner);
								retained.set(result instanceof Map<?, ?> values ? values.get("value") : result);
								assertFalse(inner.isClosed());
								lob.assertReadableAndBorrowed();
							}
							return MappingResult.of(42);
						}
					})).build();
					try (ResultSet rs = row(new String[]{"body", "trigger"}, new int[]{binary ? Types.BLOB : Types.CLOB, Types.VARCHAR},
							new Object[]{lob.handle, "trigger"})) {
						Class<?> outerType = binary ? BinaryOuter.class : TextOuter.class;
						Object result = map(mapper[0], outerType, rs);
						assertNotNull(result);
						assertSame(lob.handle, retained.get());
						assertFalse(rs.isClosed());
						lob.assertReadableAndBorrowed();
					}
				}
			}
		}
	}

	@Test
	void reentrantScalarCustomOutputBorrowsAnAncestorHandleNotPresentInItsInput() throws Exception {
		for (boolean cached : new boolean[]{false, true}) {
			Lob lob = new Lob(true);
			ResultSetMapper[] mapper = new ResultSetMapper[1];
			mapper[0] = ResultSetMapper.withPlanCachingEnabled(cached).customColumnMappers(List.of(new CustomColumnMapper() {
				@Override public Boolean appliesTo(TargetType target) {
					return target.getRawClass() == Integer.class || target.getRawClass() == Blob.class;
				}
				@Override public MappingResult map(StatementContext<?> context, ResultSet rs, Object value,
						TargetType target, Integer column, String label, InstanceProvider provider) throws SQLException {
					assertEquals("trigger", value);
					if (target.getRawClass() == Blob.class) return MappingResult.of(lob.handle);
					try (ResultSet inner = row(new String[]{"value"}, new int[]{Types.VARCHAR}, new Object[]{"trigger"})) {
						assertSame(lob.handle, MappingOwnershipRegressionTests.map(mapper[0], Blob.class, inner));
					}
					return MappingResult.of(42);
				}
			})).build();
			try (ResultSet rs = row(new String[]{"body", "trigger"}, new int[]{Types.BLOB, Types.VARCHAR}, new Object[]{lob.handle, "trigger"})) {
				assertEquals(42, map(mapper[0], BinaryOuter.class, rs).trigger());
				assertFalse(rs.isClosed());
				lob.assertReadableAndBorrowed();
			}
		}
	}

	private static <T> T map(ResultSetMapper mapper, Class<T> target, ResultSet rs) throws SQLException {
		Database database = Database.withDataSource(new JDBCDataSource()).databaseType(DatabaseType.GENERIC).resultSetMapper(mapper).build();
		StatementContext<T> context = StatementContext.<T>with(Statement.of("ownership", "SELECT values"), database).resultSetRowType(target).build();
		return mapper.map(context, rs, target, new InstanceProvider() {}).orElseThrow();
	}

	private static ResultSet row(String[] names, int[] types, Object[] values) {
		ResultSetMetaData metadata = (ResultSetMetaData) Proxy.newProxyInstance(ResultSetMetaData.class.getClassLoader(),
				new Class<?>[]{ResultSetMetaData.class}, (proxy, method, args) -> switch (method.getName()) {
					case "getColumnCount" -> names.length;
					case "getColumnLabel", "getColumnName" -> names[(int) args[0] - 1];
					case "getColumnType" -> types[(int) args[0] - 1];
					case "getColumnTypeName" -> "VALUE";
					case "hashCode" -> System.identityHashCode(proxy);
					case "equals" -> proxy == args[0];
					default -> throw new AssertionError(method.getName());
				});
		boolean[] closed = {false};
		return (ResultSet) Proxy.newProxyInstance(ResultSet.class.getClassLoader(), new Class<?>[]{ResultSet.class},
				(proxy, method, args) -> switch (method.getName()) {
					case "getMetaData" -> metadata;
					case "getObject", "getBlob" -> { assertFalse(closed[0]); yield values[(int) args[0] - 1]; }
					case "wasNull" -> false;
					case "close" -> { closed[0] = true; yield null; }
					case "isClosed" -> closed[0];
					default -> throw new AssertionError(method.getName());
				});
	}

	private static class Lob {
		int frees;
		final Object handle;
		Lob(boolean binary) {
			this.handle = Proxy.newProxyInstance(Blob.class.getClassLoader(), new Class<?>[]{binary ? Blob.class : NClob.class},
					(proxy, method, args) -> switch (method.getName()) {
						case "getBinaryStream" -> { checkOpen(); yield new ByteArrayInputStream(new byte[]{1, 2}); }
						case "getCharacterStream" -> { checkOpen(); yield new StringReader("text"); }
						case "free" -> { this.frees++; yield null; }
						case "toString" -> "LOB";
						default -> throw new AssertionError(method.getName());
					});
		}
		private void checkOpen() throws SQLException { if (this.frees > 0) throw new SQLException("freed handle"); }
		void assertReadableAndBorrowed() {
			assertEquals(0, this.frees);
			assertDoesNotThrow(() -> {
				if (this.handle instanceof Blob blob) {
					try (var stream = blob.getBinaryStream()) { assertArrayEquals(new byte[]{1, 2}, stream.readAllBytes()); }
				} else {
					try (var reader = ((Clob) this.handle).getCharacterStream()) { assertEquals('t', reader.read()); }
				}
			});
		}
	}
}
