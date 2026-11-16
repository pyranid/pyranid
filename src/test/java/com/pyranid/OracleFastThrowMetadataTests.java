/*
 * Copyright 2015-2022 Transmogrify LLC, 2022-2026 Revetware LLC.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may obtain a copy at https://www.apache.org/licenses/LICENSE-2.0
 */
package com.pyranid;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
import javax.tools.ToolProvider;
import java.io.ByteArrayOutputStream;
import java.lang.reflect.Proxy;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

class OracleFastThrowMetadataTests {
	private static Fixture fixture;

	@BeforeAll static void openFixture() throws Exception { fixture = Fixture.open(); }
	@AfterAll static void closeFixture() throws Exception { fixture.close(); }
	@AfterEach void clearInterruption() { Thread.interrupted(); }

	@Test
	void nativeStacklessTypeAndNameFailuresKeepNullFallbackCachedAcrossPositions() throws Exception {
		for (String phase : List.of("type", "name")) {
			Probe probe = new Probe(stackless(), phase, Types.INTEGER);
			var resolver = probe.resolver();
			DefaultPreparedStatementBinder binder = new DefaultPreparedStatementBinder();
			binder.bindParameter(probe.context, probe.statement, 1, Optional.empty(), resolver);
			binder.bindParameter(probe.context, probe.statement, 2, Optional.empty(), resolver);
			assertEquals(2, probe.setters.size());
			assertEquals(phase.equals("type") ? Types.NULL : Types.INTEGER, probe.setters.get(0)[1]);
			assertEquals(Types.NULL, probe.setters.get(1)[1]);
			assertEquals(1, probe.descriptions);
			assertEquals(1, fixture.reads(probe.metadata, "typeReads"));
			assertEquals(phase.equals("name") ? 1 : 0, fixture.reads(probe.metadata, "nameReads"));
		}
	}

	@Test
	void nativeFastThrowKeepsZeroFamilySelectionFailureInsteadOfLeakingTheDriverNpe() throws Exception {
		Probe probe = new Probe(stackless(), "type", -104);
		var resolver = probe.resolver();
		for (int attempt = 0; attempt < 2; attempt++) {
			DatabaseException failure = assertThrows(DatabaseException.class,
					() -> new DefaultPreparedStatementBinder().bindParameter(probe.context, probe.statement, 1, SqlInterval.ZERO, resolver));
			assertTrue(failure.getMessage().contains("Cannot choose the SQL interval family for SqlInterval.ZERO"));
			assertNull(failure.getCause());
		}
		assertEquals(1, probe.descriptions);
		assertEquals(1, fixture.reads(probe.metadata, "typeReads"));
		assertTrue(probe.setters.isEmpty());
	}

	@Test
	void cachedNativeFastThrowCannotAuthorizeTemporalBindingsAfterANull() throws Exception {
		List<Object> values = List.of(Instant.parse("2026-01-01T05:00:00Z"),
				OffsetDateTime.parse("2026-01-01T10:00:00+05:00"),
				LocalDateTime.of(2026, 1, 1, 5, 0), LocalDate.of(2026, 1, 1));
		for (boolean nullFirst : new boolean[]{false, true}) {
			for (Object value : values) {
				NullPointerException original = stackless();
				Probe probe = new Probe(original, "type", Types.TIMESTAMP);
				var resolver = probe.resolver();
				DefaultPreparedStatementBinder binder = new DefaultPreparedStatementBinder();
				if (nullFirst) binder.bindParameter(probe.context, probe.statement, 1, Optional.empty(), resolver);
				probe.setters.clear();
				DatabaseException refusal = assertThrows(DatabaseException.class,
						() -> binder.bindParameter(probe.context, probe.statement, 2, value, resolver));
				assertTrue(refusal.getMessage().contains("Cannot determine the Oracle temporal target"));
				assertSame(original, refusal.getCause());
				assertTrue(probe.setters.isEmpty(), "Refuse before dispatching a temporal setter");
				assertEquals(1, probe.descriptions);
				assertEquals(1, fixture.reads(probe.metadata, "typeReads"));
			}
		}
	}

	@Test
	void nativeFastThrowNameFailureRetainsIdentifyingTimestampCodesAndLocalDateEvidence() throws Exception {
		Instant instant = Instant.parse("2026-01-01T05:00:00Z");
		for (int sqlType : new int[]{Types.TIMESTAMP, Types.TIMESTAMP_WITH_TIMEZONE, -101, -102}) {
			Probe probe = new Probe(stackless(), "name", sqlType);
			new DefaultPreparedStatementBinder().bindParameter(probe.context, probe.statement, 1, instant, probe.resolver());
			assertEquals(1, probe.setters.size());
			Object bound = probe.setters.get(0)[1];
			if (sqlType == Types.TIMESTAMP) assertEquals(LocalDateTime.ofInstant(instant, ZoneOffset.UTC), bound);
			else assertEquals(instant, ((OffsetDateTime) bound).toInstant());
			assertEquals(1, fixture.reads(probe.metadata, "typeReads"));
			assertEquals(1, fixture.reads(probe.metadata, "nameReads"));
		}
		Probe date = new Probe(stackless(), "name", Types.DATE);
		LocalDate value = LocalDate.of(2026, 1, 1);
		new DefaultPreparedStatementBinder().bindParameter(date.context, date.statement, 1, value, date.resolver());
		assertEquals(value, date.setters.get(0)[1]);
	}

	@Test
	void foreignMetadataSubclassesOtherDialectsAndContextFreeResolversCannotClaimFastThrow() throws Exception {
		NullPointerException original = stackless();
		ParameterMetaData proxy = (ParameterMetaData) Proxy.newProxyInstance(ParameterMetaData.class.getClassLoader(),
				new Class<?>[]{ParameterMetaData.class}, (handle, method, args) -> {
					if (method.getName().equals("getParameterType") || method.getName().equals("getParameterTypeName")) throw original;
					throw new AssertionError("Foreign metadata must not be unwrapped: " + method);
				});
		Probe foreign = new Probe(original, "type", Types.TIMESTAMP, DatabaseType.ORACLE, proxy);
		assertRetained(foreign, foreign.resolver());
		Probe subclass = new Probe(original, "type", Types.TIMESTAMP, DatabaseType.ORACLE,
				fixture.metadata(original, "type", Types.TIMESTAMP, true));
		assertRetained(subclass, subclass.resolver());
		for (DatabaseType type : List.of(DatabaseType.GENERIC, DatabaseType.POSTGRESQL, DatabaseType.DUCK_DB)) {
			Probe probe = new Probe(original, "type", Types.TIMESTAMP, type, null);
			assertRetained(probe, probe.resolver());
		}
		Probe contextFree = new Probe(original, "type", Types.TIMESTAMP);
		assertRetained(contextFree, new DefaultPreparedStatementBinder.ParameterSqlTypeResolver(contextFree.statement));
	}

	@Test
	void stacklessAcquisitionFailuresAreNeverAnOptionalMetadataFallback() throws Exception {
		Probe probe = new Probe(stackless(), "metadata", Types.TIMESTAMP);
		assertRetained(probe, probe.resolver());
		assertEquals(0, fixture.reads(probe.metadata, "typeReads"));
	}

	@Test
	void nativeFastThrowCannotHideMessagesCausesSuppressedFailuresOrInterruption() throws Exception {
		List<NullPointerException> failures = new ArrayList<>();
		NullPointerException message = new NullPointerException("application or other driver failure");
		message.setStackTrace(new StackTraceElement[0]); failures.add(message);
		NullPointerException subclass = new NullPointerException() {};
		subclass.setStackTrace(new StackTraceElement[0]); failures.add(subclass);
		for (Throwable evidence : List.of(new SQLException("cancelled", "57014"),
				new InterruptedException("interrupted"), new AssertionError("fatal"))) {
			NullPointerException cause = stackless(); cause.initCause(evidence); failures.add(cause);
			NullPointerException suppressed = stackless(); suppressed.addSuppressed(evidence); failures.add(suppressed);
		}
		for (NullPointerException failure : failures) {
			Probe probe = new Probe(failure, "type", Types.TIMESTAMP);
			assertRetained(probe, probe.resolver());
		}
		Probe interrupted = new Probe(stackless(), "type", Types.TIMESTAMP);
		Thread.currentThread().interrupt();
		assertRetained(interrupted, interrupted.resolver());
		assertTrue(Thread.currentThread().isInterrupted());
	}

	private static void assertRetained(Probe probe, DefaultPreparedStatementBinder.ParameterSqlTypeResolver resolver) {
		assertSame(probe.failure, assertThrows(Throwable.class, () -> resolver.determineParameterSqlType(1)));
		assertSame(probe.failure, assertThrows(Throwable.class, () -> resolver.determineParameterSqlType(2)));
		assertEquals(1, probe.descriptions);
		assertTrue(probe.setters.isEmpty());
	}

	private static NullPointerException stackless() {
		NullPointerException failure = new NullPointerException();
		failure.setStackTrace(new StackTraceElement[0]); return failure;
	}

	private static final class Probe {
		final Throwable failure;
		final ParameterMetaData metadata;
		final StatementContext<?> context;
		final PreparedStatement statement;
		final List<Object[]> setters = new ArrayList<>();
		int descriptions;
		Probe(Throwable failure, String phase, int sqlType) throws Exception {
			this(failure, phase, sqlType, DatabaseType.ORACLE, null);
		}
		Probe(Throwable failure, String phase, int sqlType, DatabaseType type, ParameterMetaData override) throws Exception {
			this.failure = failure;
			metadata = override == null ? fixture.metadata(failure, phase, sqlType, false) : override;
			DataSource unused = (DataSource) Proxy.newProxyInstance(DataSource.class.getClassLoader(), new Class<?>[]{DataSource.class},
					(handle, method, args) -> { throw new AssertionError(method); });
			context = StatementContext.with(Statement.of("oracle-fast-throw", "INSERT INTO work SELECT :value FROM dual"),
					Database.with(unused, type).timeZone(ZoneOffset.UTC).build()).build();
			Connection connection = (Connection) Proxy.newProxyInstance(Connection.class.getClassLoader(), new Class<?>[]{Connection.class},
					(handle, method, args) -> { if (method.getName().equals("getAutoCommit")) return true; throw new AssertionError(method); });
			statement = (PreparedStatement) Proxy.newProxyInstance(PreparedStatement.class.getClassLoader(), new Class<?>[]{PreparedStatement.class},
					(handle, method, args) -> switch (method.getName()) {
						case "getParameterMetaData" -> { descriptions++; if (phase.equals("metadata")) throw failure; yield metadata; }
						case "getConnection" -> connection;
						case "setNull", "setObject", "setDate", "setTimestamp" -> { setters.add(args.clone()); yield null; }
						default -> throw new AssertionError(method);
					});
		}
		DefaultPreparedStatementBinder.ParameterSqlTypeResolver resolver() {
			return new DefaultPreparedStatementBinder.ParameterSqlTypeResolver(context, statement);
		}
	}

	private static final class Fixture implements AutoCloseable {
		final Path directory;
		final URLClassLoader loader;
		final Class<?> nativeType, subclassType;
		Fixture(Path directory, URLClassLoader loader, Class<?> nativeType, Class<?> subclassType) {
			this.directory = directory; this.loader = loader; this.nativeType = nativeType; this.subclassType = subclassType;
		}
		ParameterMetaData metadata(Throwable failure, String phase, int sqlType, boolean subclass) throws Exception {
			return (ParameterMetaData) (subclass ? subclassType : nativeType)
					.getConstructor(Throwable.class, String.class, int.class).newInstance(failure, phase, sqlType);
		}
		int reads(ParameterMetaData metadata, String field) throws Exception { return nativeType.getField(field).getInt(metadata); }
		static Fixture open() throws Exception {
			Path directory = Files.createTempDirectory("pyranid-oracle-fast-throw-");
			Path source = directory.resolve("src/oracle/jdbc/driver/OracleParameterMetaData.java");
			Path subclass = directory.resolve("src/application/ForeignMetadata.java");
			Files.createDirectories(source.getParent()); Files.createDirectories(subclass.getParent());
			Files.writeString(source, """
				package oracle.jdbc.driver;
				public class OracleParameterMetaData implements java.sql.ParameterMetaData {
				  private final Throwable failure; private final String phase; private final int sqlType;
				  public int typeReads, nameReads;
				  public OracleParameterMetaData(Throwable failure, String phase, int sqlType) {
				    this.failure=failure; this.phase=phase; this.sqlType=sqlType;
				  }
				  private void fail() throws java.sql.SQLException {
				    if(failure instanceof RuntimeException runtime) throw runtime;
				    if(failure instanceof Error error) throw error;
				    if(failure instanceof java.sql.SQLException sql) throw sql;
				    throw new java.sql.SQLException(failure);
				  }
				  public int getParameterType(int index) throws java.sql.SQLException {
				    typeReads++; if(phase.equals("type")) fail(); return sqlType;
				  }
				  public String getParameterTypeName(int index) throws java.sql.SQLException {
				    nameReads++; if(phase.equals("name")) fail(); return "TIMESTAMP";
				  }
				  public int getParameterCount() { throw new AssertionError(); }
				  public int getParameterMode(int i) { throw new AssertionError(); }
				  public String getParameterClassName(int i) { throw new AssertionError(); }
				  public int getPrecision(int i) { throw new AssertionError(); }
				  public int getScale(int i) { throw new AssertionError(); }
				  public int isNullable(int i) { throw new AssertionError(); }
				  public boolean isSigned(int i) { throw new AssertionError(); }
				  public <T>T unwrap(Class<T> type) { throw new AssertionError(); }
				  public boolean isWrapperFor(Class<?> type) { throw new AssertionError(); }
				}
				""");
			Files.writeString(subclass, """
				package application;
				public final class ForeignMetadata extends oracle.jdbc.driver.OracleParameterMetaData {
				  public ForeignMetadata(Throwable failure, String phase, int sqlType) { super(failure,phase,sqlType); }
				}
				""");
			Path classes = directory.resolve("classes"); Files.createDirectories(classes);
			ByteArrayOutputStream diagnostics = new ByteArrayOutputStream();
			int result = ToolProvider.getSystemJavaCompiler().run(null, diagnostics, diagnostics, "--release", "17", "-d", classes.toString(),
					source.toString(), subclass.toString());
			if (result != 0) throw new AssertionError(diagnostics.toString());
			URLClassLoader loader = new URLClassLoader(new URL[]{classes.toUri().toURL()}, ClassLoader.getPlatformClassLoader());
			return new Fixture(directory, loader, loader.loadClass("oracle.jdbc.driver.OracleParameterMetaData"), loader.loadClass("application.ForeignMetadata"));
		}
		@Override public void close() throws Exception {
			loader.close();
			try (var paths = Files.walk(directory)) {
				for (Path path : paths.sorted(Comparator.reverseOrder()).toList()) Files.delete(path);
			}
		}
	}
}
