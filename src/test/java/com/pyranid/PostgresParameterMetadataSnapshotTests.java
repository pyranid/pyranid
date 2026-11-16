/*
 * Copyright 2015-2022 Transmogrify LLC, 2022-2026 Revetware LLC.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may obtain a copy at https://www.apache.org/licenses/LICENSE-2.0
 */
package com.pyranid;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.postgresql.core.BaseConnection;
import org.postgresql.core.TypeInfo;
import org.postgresql.jdbc.PgParameterMetaData;

import javax.sql.DataSource;
import javax.tools.ToolProvider;
import java.io.ByteArrayOutputStream;
import java.lang.reflect.Field;
import java.lang.reflect.Proxy;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class PostgresParameterMetadataSnapshotTests {
	@AfterEach void clearInterruption() { Thread.interrupted(); }

	@Test
	void nativeMetadataDetachesTheLiveOidsWithoutCallingCustomConnectionCode() throws SQLException {
		NativeProbe probe = new NativeProbe(1184, 0, 999999);
		var snapshot = PostgresParameterMetadataSnapshot.freeze(probe.metadata);
		assertNotNull(snapshot); assertNotSame(probe.metadata, snapshot.metadata());
		assertArrayEquals(new int[0], snapshot.builtinIndices(), "A custom BaseConnection cannot authorize speculative getters");
		assertEquals(0, probe.connectionCalls.get());
		probe.oids[0] = 1114; probe.oids[1] = 23; probe.oids[2] = 25;
		assertEquals(Types.TIMESTAMP, probe.metadata.getParameterType(1));
		assertEquals(Types.TIMESTAMP_WITH_TIMEZONE, snapshot.metadata().getParameterType(1));
		assertEquals("oid-0", snapshot.metadata().getParameterTypeName(2));
		assertEquals("oid-999999", snapshot.metadata().getParameterTypeName(3));
	}

	@Test
	void aResolverKeepsUnboundFrozenTargetsButRejectsAnEarlierInputOid() throws SQLException {
		NativeProbe probe = new NativeProbe(23, 1184, 999999);
		DefaultPreparedStatementBinder.ParameterSqlTypeResolver resolver = probe.resolver();
		resolver.beginParameterGroup(); resolver.noteParameterBound(1);
		assertEquals(Types.TIMESTAMP_WITH_TIMEZONE, resolver.determineParameterSqlType(2).orElseThrow().getSqlType());
		assertEquals(List.of(1184, 1184), probe.oidReads, "An application BaseConnection must not receive speculative getters");
		probe.oids[2] = 1114; resolver.beginParameterGroup();
		assertEquals(Types.OTHER, resolver.determineParameterSqlType(3).orElseThrow().getSqlType());
		assertEquals(List.of(1184, 1184, 999999, 999999), probe.oidReads);
		SQLException unsafe = assertThrows(SQLException.class, () -> resolver.determineParameterSqlType(1));
		assertEquals("0A000", unsafe.getSQLState());
		assertSame(unsafe, assertThrows(SQLException.class, () -> resolver.determineParameterSqlType(2)));
	}

	@Test
	void registeredPreviouslyBoundSlotsCannotTurnAnUnsafeSnapshotIntoOptionalAbsence() {
		NativeProbe probe = new NativeProbe(23, 1184, 999999);
		DefaultPreparedStatementBinder.ParameterSqlTypeResolver resolver = probe.resolver();
		resolver.beginParameterGroup(); resolver.noteParameterBound(1); resolver.registerParameterIndices(List.of(1));
		SQLException unsafe = assertThrows(SQLException.class, () -> resolver.determineParameterSqlType(2));
		assertEquals("0A000", unsafe.getSQLState()); assertArrayEquals(new Throwable[0], unsafe.getSuppressed());
		assertSame(unsafe, assertThrows(SQLException.class, () -> resolver.determineParameterSqlType(2)));
		assertEquals(List.of(1184, 1184), probe.oidReads);
	}

	@Test
	void wrappersAndSubclassesAreNeverUnwrappedOrInspected() throws SQLException {
		ParameterMetaData wrapper = (ParameterMetaData) Proxy.newProxyInstance(ParameterMetaData.class.getClassLoader(),
				new Class<?>[]{ParameterMetaData.class}, (proxy, method, args) -> { throw new AssertionError(method); });
		assertNull(PostgresParameterMetadataSnapshot.freeze(wrapper));
		NativeProbe probe = new NativeProbe(1184);
		ParameterMetaData subclass = new PgParameterMetaData(probe.connection, probe.oids) {
			@Override public int getParameterType(int index) { throw new AssertionError("Subclass metadata belongs to application code"); }
		};
		assertNull(PostgresParameterMetadataSnapshot.freeze(subclass)); assertEquals(0, probe.connectionCalls.get());
	}

	@Test
	void onlyLocallyInstalledScalarOidsAreEagerForAnExactNativeConnection() throws Exception {
		int[] oids = {1184, 1114, 1082, 23, 25, 114, 3802, 0, 999999, 1186, 2950, 1007};
		for (String root : List.of("org.postgresql", "com.yugabyte")) {
			try (Fixture fixture = Fixture.open(root, false)) {
				ParameterMetaData metadata = fixture.metadata(oids);
				var snapshot = PostgresParameterMetadataSnapshot.freeze(metadata);
				assertNotNull(snapshot);
				assertArrayEquals(root.equals("org.postgresql") ? new int[]{1, 2, 3, 4, 5, 6, 7} : new int[]{1, 2, 3, 4, 5, 6}, snapshot.builtinIndices());
				assertEquals(0, fixture.type.getField("getterCalls").getInt(null));
				oids[0] = 25;
				assertEquals(1184, snapshot.metadata().getParameterType(1));
				oids[0] = 1184;
			}
		}
	}

	@Test
	void incompatibleOptionalDriverLayoutsFallBackWithoutCallingMetadata() throws Exception {
		for (String root : List.of("org.postgresql", "com.yugabyte")) {
			try (Fixture fixture = Fixture.open(root, true)) {
				ParameterMetaData metadata = (ParameterMetaData) fixture.type.getConstructor().newInstance();
				assertNull(PostgresParameterMetadataSnapshot.freeze(metadata));
				assertEquals(0, fixture.type.getField("getterCalls").getInt(null));
			}
		}
	}

	@Test
	void nativeConstructorFailuresPreserveOriginalSqlRuntimeFatalAndInterruptedEvidence() throws Exception {
		try (Fixture fixture = Fixture.open("org.postgresql", false)) {
			ParameterMetaData metadata = fixture.metadata(new int[]{1184});
			Field failure = fixture.type.getField("failure");
			for (Throwable original : List.of(new SQLException("cancelled", "57014"), new IllegalStateException("constructor"),
					new AssertionError("fatal"), new SQLException("interrupted", new InterruptedException("cancel")))) {
				failure.set(null, original);
				assertSame(original, assertThrows(Throwable.class, () -> PostgresParameterMetadataSnapshot.freeze(metadata)));
				assertFalse(Thread.currentThread().isInterrupted(), "This internal helper does not publish retained interruption");
			}
			failure.set(null, null); Thread.currentThread().interrupt();
			assertNotNull(PostgresParameterMetadataSnapshot.freeze(metadata)); assertTrue(Thread.currentThread().isInterrupted());
		}
	}

	private static final class NativeProbe {
		final int[] oids;
		final AtomicInteger connectionCalls = new AtomicInteger();
		final List<Integer> oidReads = new ArrayList<>();
		final BaseConnection connection;
		final ParameterMetaData metadata;
		NativeProbe(int... oids) {
			this.oids = oids;
			TypeInfo info = (TypeInfo) Proxy.newProxyInstance(TypeInfo.class.getClassLoader(), new Class<?>[]{TypeInfo.class},
					(proxy, method, args) -> {
						int oid = (Integer) args[0]; oidReads.add(oid);
						return switch (method.getName()) {
							case "getSQLType" -> oid == 1184 ? Types.TIMESTAMP_WITH_TIMEZONE : oid == 1114 ? Types.TIMESTAMP : Types.OTHER;
							case "getPGType" -> "oid-" + oid;
							default -> throw new AssertionError(method);
						};
					});
			connection = (BaseConnection) Proxy.newProxyInstance(BaseConnection.class.getClassLoader(), new Class<?>[]{BaseConnection.class},
					(proxy, method, args) -> switch (method.getName()) {
						case "getTypeInfo" -> { connectionCalls.incrementAndGet(); yield info; }
						case "getAutoCommit" -> true;
						default -> throw new AssertionError(method);
					});
			metadata = new PgParameterMetaData(connection, oids);
		}
		DefaultPreparedStatementBinder.ParameterSqlTypeResolver resolver() {
			DataSource unused = (DataSource) Proxy.newProxyInstance(DataSource.class.getClassLoader(), new Class<?>[]{DataSource.class},
					(proxy, method, args) -> { throw new AssertionError(method); });
			StatementContext<?> context = StatementContext.with(Statement.of("frozen-metadata", "SELECT :a, :b, :c"),
					Database.with(unused, DatabaseType.POSTGRESQL).build()).build();
			PreparedStatement statement = (PreparedStatement) Proxy.newProxyInstance(PreparedStatement.class.getClassLoader(),
					new Class<?>[]{PreparedStatement.class}, (proxy, method, args) -> switch (method.getName()) {
						case "getConnection" -> connection;
						case "getParameterMetaData" -> metadata;
						default -> throw new AssertionError(method);
					});
			return new DefaultPreparedStatementBinder.ParameterSqlTypeResolver(context, statement);
		}
	}

	private static final class Fixture implements AutoCloseable {
		final URLClassLoader loader;
		final Class<?> type, api;
		final Object connection;
		Fixture(URLClassLoader loader, Class<?> type, Class<?> api, Object connection) {
			this.loader = loader; this.type = type; this.api = api; this.connection = connection;
		}
		ParameterMetaData metadata(int[] oids) throws Exception {
			return (ParameterMetaData) type.getConstructor(api, int[].class).newInstance(connection, oids);
		}
		static Fixture open(String root, boolean missingFields) throws Exception {
			Path directory = Files.createTempDirectory("pyranid-native-metadata-");
			Path api = directory.resolve("sources/" + root.replace('.', '/') + "/core/BaseConnection.java");
			Path connection = directory.resolve("sources/" + root.replace('.', '/') + "/jdbc/PgConnection.java");
			Path metadata = connection.resolveSibling("PgParameterMetaData.java");
			Files.createDirectories(api.getParent()); Files.createDirectories(connection.getParent());
			Files.writeString(api, "package " + root + ".core; public interface BaseConnection {}\n");
			Files.writeString(connection, "package " + root + ".jdbc; public final class PgConnection implements " + root + ".core.BaseConnection {}\n");
			String fields = missingFields ? "public PgParameterMetaData() {}" : """
				private final %s.core.BaseConnection connection;
				private final int[] oids;
				public static Throwable failure;
				public PgParameterMetaData(%s.core.BaseConnection connection, int[] oids) throws java.sql.SQLException {
				  if (failure instanceof java.sql.SQLException sql) throw sql;
				  if (failure instanceof RuntimeException runtime) throw runtime;
				  if (failure instanceof Error error) throw error;
				  this.connection = connection; this.oids = oids;
				}
				""".formatted(root, root);
			String value = missingFields ? "throw new AssertionError();" : "getterCalls++; return oids[index - 1];";
			Files.writeString(metadata, """
				package %s.jdbc;
				public class PgParameterMetaData implements java.sql.ParameterMetaData {
				  public static int getterCalls;
				  %s
				  public int getParameterType(int index) { %s }
				  public String getParameterTypeName(int index) { getterCalls++; return "native"; }
				  public int getParameterCount() { throw new AssertionError(); }
				  public int getParameterMode(int i) { throw new AssertionError(); }
				  public String getParameterClassName(int i) { throw new AssertionError(); }
				  public int getPrecision(int i) { throw new AssertionError(); }
				  public int getScale(int i) { throw new AssertionError(); }
				  public int isNullable(int i) { throw new AssertionError(); }
				  public boolean isSigned(int i) { throw new AssertionError(); }
				  public <T> T unwrap(Class<T> type) { throw new AssertionError(); }
				  public boolean isWrapperFor(Class<?> type) { throw new AssertionError(); }
				}
				""".formatted(root, fields, value));
			Path classes = directory.resolve("classes"); Files.createDirectory(classes);
			ByteArrayOutputStream diagnostics = new ByteArrayOutputStream();
			int result = ToolProvider.getSystemJavaCompiler().run(null, diagnostics, diagnostics, "--release", "17", "-d", classes.toString(),
					api.toString(), connection.toString(), metadata.toString());
			if (result != 0) throw new AssertionError(diagnostics.toString());
			URLClassLoader loader = new URLClassLoader(new URL[]{classes.toUri().toURL()}, ClassLoader.getPlatformClassLoader());
			return new Fixture(loader, loader.loadClass(root + ".jdbc.PgParameterMetaData"), loader.loadClass(root + ".core.BaseConnection"),
					loader.loadClass(root + ".jdbc.PgConnection").getConstructor().newInstance());
		}
		@Override public void close() throws Exception { loader.close(); }
	}
}
