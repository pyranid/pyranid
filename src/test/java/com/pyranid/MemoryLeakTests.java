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
import java.io.IOException;
import java.io.InputStream;
import java.lang.ref.Reference;
import java.lang.ref.WeakReference;
import java.lang.reflect.Proxy;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Types;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * Exercises cache growth patterns under heavy/varied usage to catch unbounded retention.
 *
 * @author <a href="https://www.revetkn.com">Mark Allen</a>
 * @since 4.0.0
 */
@ThreadSafe
public class MemoryLeakTests {
	private static final int PLAN_CACHE_CAPACITY = 8;
	private static final String RELOADABLE_TYPES = "com.pyranid.fixtures.ReloadableMappingTypes";

	@NonNull
	protected DataSource createInMemoryDataSource(@NonNull String databaseName) {
		requireNonNull(databaseName);

		JDBCDataSource dataSource = new JDBCDataSource();
		dataSource.setUrl(format("jdbc:hsqldb:mem:%s", databaseName));
		dataSource.setUser("sa");
		dataSource.setPassword("");

		return dataSource;
	}

	public record IdRow(Integer id) {}

	@Test
	public void testRowPlanningCacheBoundedUnderVariedSchemas() {
		DataSource ds = createInMemoryDataSource("plan_cache_bounded");
		ResultSetMapper mapper = ResultSetMapper.withPlanCachingEnabled(true)
				.planCacheCapacity(PLAN_CACHE_CAPACITY)
				.build();

		Database db = Database.withDataSource(ds)
				.resultSetMapper(mapper)
				.build();

				db.query("CREATE TABLE t (id INT, value INT)").execute();
				db.query("INSERT INTO t (id, value) VALUES (1, 10)").execute();

		for (int i = 0; i < 50; i++) {
			String sql = "SELECT id, value AS extra_" + i + " FROM t";
			db.query(sql)
					.fetchObject(IdRow.class)
					.orElseThrow();
		}

		DefaultResultSetMapper defaultMapper = (DefaultResultSetMapper) db.getResultSetMapper();
		int size = defaultMapper.getRowPlanningCacheForResultClass(IdRow.class).size();

		Assertions.assertTrue(size <= PLAN_CACHE_CAPACITY,
				"Row planning cache should remain bounded after varied schemas");
	}

	@Test
	public void testMapperCachesDoNotPinDisposableClassesOrTheirLoader() throws Exception {
		for (boolean cached : new boolean[]{true, false}) {
			AtomicInteger collectionMappings = new AtomicInteger();
			CustomColumnMapper customMapper = new CustomColumnMapper() {
				@Override
				public Boolean appliesTo(TargetType targetType) {
					return targetType.isList();
				}

				@Override
				public MappingResult map(StatementContext<?> context, ResultSet resultSet, Object value,
						TargetType targetType, Integer columnIndex, String columnLabel, InstanceProvider instances) {
					// Exercise parameterized mapper selection without retaining the disposable element type ourselves.
					Assertions.assertEquals(1, targetType.getTypeArguments().size());
					collectionMappings.incrementAndGet();
					return MappingResult.fallback();
				}
			};
			ResultSetMapper mapper = ResultSetMapper.withCustomColumnMappers(List.of(customMapper))
					.planCachingEnabled(cached).planCacheCapacity(PLAN_CACHE_CAPACITY).build();
			Database database = Database.withDataSource(createInMemoryDataSource("loader_reclamation_" + cached))
					.databaseType(DatabaseType.GENERIC).resultSetMapper(mapper).build();

			try {
				// A separate method ensures class tokens, reflection objects, rows, contexts and cursors leave the stack
				// before requesting collection. Only weak references cross this boundary.
				ReclamationReferences references = populateDisposableClassCaches(database, mapper);
				Assertions.assertTrue(collectionMappings.get() > 0, "Parameterized custom mapping must be exercised");
				awaitReclamation(references, cached);
				Assertions.assertSame(mapper, database.getResultSetMapper());
				Assertions.assertEquals(1, database.query("VALUES (1)").fetchObject(Integer.class).orElseThrow());
			} finally {
				// Reclamation must occur while the same Database, mapper and their ClassValue objects remain live.
				Reference.reachabilityFence(database);
				Reference.reachabilityFence(mapper);
			}
		}
	}

	@SuppressWarnings({"rawtypes", "unchecked"})
	private ReclamationReferences populateDisposableClassCaches(Database database, ResultSetMapper mapper) throws Exception {
		ClassLoader loader = new DisposableMappingClassLoader();
		Class<?> recordType = loader.loadClass(RELOADABLE_TYPES + "$RecordRow");
		Class<?> beanType = loader.loadClass(RELOADABLE_TYPES + "$BeanRow");
		Class<?> collectionType = loader.loadClass(RELOADABLE_TYPES + "$CollectionRow");
		Class<?> elementType = loader.loadClass(RELOADABLE_TYPES + "$Element");
		Assertions.assertSame(loader, recordType.getClassLoader());
		Assertions.assertSame(loader, beanType.getClassLoader());
		Assertions.assertSame(loader, collectionType.getClassLoader());
		Assertions.assertSame(loader, elementType.getClassLoader());

		// Two rows exercise retained plans when caching is enabled, and the same class metadata when disabled.
		for (int iteration = 0; iteration < 2; ++iteration) {
			Object record = database.query("SELECT 1 AS id FROM (VALUES (0))").fetchObject(recordType).orElseThrow();
			Assertions.assertEquals(1, recordType.getMethod("id").invoke(record));
			Object bean = database.query("SELECT 2 AS identifier FROM (VALUES (0))").fetchObject(beanType).orElseThrow();
			Assertions.assertEquals(2, beanType.getMethod("getId").invoke(bean));

			StatementContext context = StatementContext.with(Statement.of("loader-collection", "SELECT elements"), database)
					.resultSetRowType((Class) collectionType).build();
			try (ResultSet cursor = collectionCursor()) {
				Object collectionRow = mapper.map(context, cursor, collectionType, database.getInstanceProvider()).orElseThrow();
				List<?> elements = (List<?>) collectionType.getMethod("elements").invoke(collectionRow);
				Assertions.assertEquals(1, elements.size());
				Assertions.assertSame(elementType, elements.get(0).getClass());
				Assertions.assertEquals(3, elementType.getMethod("id").invoke(elements.get(0)));
			}
		}

		return new ReclamationReferences(new WeakReference<>(loader), List.of(
				new WeakReference<>(recordType), new WeakReference<>(beanType),
				new WeakReference<>(collectionType), new WeakReference<>(elementType)));
	}

	private static ResultSet collectionCursor() {
		ResultSetMetaData metadata = (ResultSetMetaData) Proxy.newProxyInstance(MemoryLeakTests.class.getClassLoader(),
				new Class<?>[]{ResultSetMetaData.class}, (proxy, method, args) -> switch (method.getName()) {
					case "getColumnCount" -> 1;
					case "getColumnLabel", "getColumnName" -> "elements";
					case "getColumnType" -> Types.ARRAY;
					case "getColumnTypeName" -> "STRUCT ARRAY";
					case "hashCode" -> System.identityHashCode(proxy);
					case "equals" -> proxy == args[0];
					case "toString" -> "Disposable mapping metadata";
					default -> throw new AssertionError("Unexpected metadata call: " + method);
				});
		return (ResultSet) Proxy.newProxyInstance(MemoryLeakTests.class.getClassLoader(), new Class<?>[]{ResultSet.class},
				(proxy, method, args) -> switch (method.getName()) {
					case "getMetaData" -> metadata;
					case "getObject" -> new Object[]{Map.of("id", 3)};
					case "close" -> null;
					case "wasNull" -> false;
					case "hashCode" -> System.identityHashCode(proxy);
					case "equals" -> proxy == args[0];
					case "toString" -> "Disposable mapping cursor";
					default -> throw new AssertionError("Unexpected cursor call: " + method);
				});
	}

	private static void awaitReclamation(ReclamationReferences references, boolean cached) throws InterruptedException {
		long startedAt = System.nanoTime();
		long budgetNanos = Duration.ofSeconds(5).toNanos();
		for (int attempt = 0; attempt < 100 && !references.cleared() && System.nanoTime() - startedAt < budgetNanos; ++attempt) {
			System.gc();
			Thread.sleep(25L);
		}
		Assertions.assertTrue(references.loader().refersTo(null),
				"Long-lived mapper pinned the disposable class loader; planCachingEnabled=" + cached);
		for (int index = 0; index < references.types().size(); ++index)
			Assertions.assertTrue(references.types().get(index).refersTo(null),
					"Long-lived mapper pinned disposable class " + index + "; planCachingEnabled=" + cached);
	}

	private record ReclamationReferences(WeakReference<ClassLoader> loader, List<WeakReference<Class<?>>> types) {
		private boolean cleared() {
			if (!this.loader.refersTo(null)) return false;
			for (WeakReference<Class<?>> type : this.types)
				if (!type.refersTo(null)) return false;
			return true;
		}
	}

	/** Loads only the fixture family itself; JDK and Pyranid classes retain their ordinary parent identity. */
	private static final class DisposableMappingClassLoader extends ClassLoader {
		private DisposableMappingClassLoader() {
			super(MemoryLeakTests.class.getClassLoader());
		}

		@Override
		protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
			if (!name.equals(RELOADABLE_TYPES) && !name.startsWith(RELOADABLE_TYPES + "$"))
				return super.loadClass(name, resolve);
			Class<?> type = findLoadedClass(name);
			if (type == null) {
				String resource = name.replace('.', '/') + ".class";
				try (InputStream input = getParent().getResourceAsStream(resource)) {
					if (input == null) throw new ClassNotFoundException(name);
					byte[] bytes = input.readAllBytes();
					type = defineClass(name, bytes, 0, bytes.length);
				} catch (IOException failure) {
					throw new ClassNotFoundException(name, failure);
				}
			}
			if (resolve) resolveClass(type);
			return type;
		}
	}
}
