/*
 * Copyright 2015-2022 Transmogrify LLC, 2022-2026 Revetware LLC.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy at https://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */
package com.pyranid;

import org.hsqldb.jdbc.JDBCDataSource;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Types;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MapperRetentionTests {
	public record ParameterizedRow(List<Long> ids, List<UUID> keys) {}

	@Test
	void plansRetainFullTypeSelectionWithoutRetainingCallbacksOrReselectingEachRow() throws Exception {
		UUID key = UUID.fromString("176bd707-203c-4eb3-bf84-6a03543907df");
		for (boolean cached : new boolean[]{false, true}) {
			Map<Class<?>, AtomicInteger> selections = Map.of(Long.class, new AtomicInteger(), UUID.class, new AtomicInteger());
			CustomColumnMapper callback = new CustomColumnMapper() {
				@Override public Boolean appliesTo(TargetType target) {
					if (!target.isList()) return false;
					selections.get(target.getTypeArguments().get(0).getRawClass()).incrementAndGet();
					return true;
				}
				@Override public MappingResult map(StatementContext<?> context, ResultSet result, Object value,
						TargetType target, Integer index, String label, InstanceProvider instances) {
					return MappingResult.of(target.getTypeArguments().get(0).matchesClass(Long.class) ? List.of(42L) : List.of(key));
				}
			};
			ResultSetMapper mapper = ResultSetMapper.withCustomColumnMappers(List.of(callback)).planCachingEnabled(cached).build();
			Database database = Database.with(new JDBCDataSource(), DatabaseType.GENERIC).resultSetMapper(mapper).build();
			StatementContext<ParameterizedRow> context = StatementContext.<ParameterizedRow>with(
					Statement.of("parameterized-retention", "SELECT ids, keys"), database).resultSetRowType(ParameterizedRow.class).build();
			ResultSetMetaData metadata = (ResultSetMetaData) Proxy.newProxyInstance(ResultSetMetaData.class.getClassLoader(),
					new Class<?>[]{ResultSetMetaData.class}, (proxy, method, arguments) -> switch (method.getName()) {
						case "getColumnCount" -> 2;
						case "getColumnLabel", "getColumnName" -> (int) arguments[0] == 1 ? "ids" : "keys";
						case "getColumnType" -> Types.OTHER;
						case "getColumnTypeName" -> "JAVA_OBJECT";
						case "hashCode" -> System.identityHashCode(proxy);
						case "equals" -> proxy == arguments[0];
						default -> throw new AssertionError(method);
					});
			try (ResultSet result = (ResultSet) Proxy.newProxyInstance(ResultSet.class.getClassLoader(), new Class<?>[]{ResultSet.class},
					(proxy, method, arguments) -> switch (method.getName()) {
						case "getMetaData" -> metadata;
						case "getObject" -> List.of("raw");
						case "wasNull" -> false;
						case "close" -> null;
						default -> throw new AssertionError(method);
					})) {
				for (int row = 0; row < 2; row++)
					assertEquals(new ParameterizedRow(List.of(42L), List.of(key)),
							mapper.map(context, result, ParameterizedRow.class, database.getInstanceProvider()).orElseThrow());
			}
			assertEquals(cached ? 1 : 2, selections.get(Long.class).get());
			assertEquals(cached ? 1 : 2, selections.get(UUID.class).get());
		}
	}

	@Test
	void discardedMappersAndApplicationLoadersAreCollectibleWithLiveWorkerThreads() throws Exception {
		Path java = Path.of(System.getProperty("java.home"), "bin",
				System.getProperty("os.name").toLowerCase().contains("win") ? "java.exe" : "java");
		Process process = new ProcessBuilder(java.toString(), "-Xmx96m", "-XX:+UseSerialGC", "-XX:+ClassUnloading",
				"-cp", System.getProperty("java.class.path"), MapperRetentionProbe.class.getName())
				.redirectErrorStream(true).start();
		boolean completed;
		try {
			completed = process.waitFor(60, TimeUnit.SECONDS);
		} finally {
			if (process.isAlive()) process.destroyForcibly();
		}
		String output = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
		assertTrue(completed, () -> "Retention probe exceeded its bounded child-JVM budget:\n" + output);
		assertEquals(0, process.exitValue(), () -> "Retention probe failed:\n" + output);
		assertTrue(output.contains("PASS: mapper, cursor, driver-factory and worker-loader reclamation"), output);
	}
}
