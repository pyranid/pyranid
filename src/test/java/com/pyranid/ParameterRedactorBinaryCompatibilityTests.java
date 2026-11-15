/*
 * Copyright 2015-2022 Transmogrify LLC, 2022-2026 Revetware LLC.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may obtain a copy of the License at
 * https://www.apache.org/licenses/LICENSE-2.0
 */
package com.pyranid;

import org.hsqldb.jdbc.JDBCDataSource;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import javax.tools.ToolProvider;
import java.io.ByteArrayOutputStream;
import java.lang.reflect.InvocationTargetException;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class ParameterRedactorBinaryCompatibilityTests {
	@TempDir Path directory;

	@Test
	void oldCompiledImplementationsFailAtUseWhileSqlFailuresRetainTheirDiagnostics() throws Exception {
		try (URLClassLoader loader = compileLegacyFixture()) {
			Class<?> fixture = loader.loadClass("legacy.OldRedactorFixture");
			for (String factory : List.of("lambda", "anonymous")) {
				ParameterRedactor redactor = (ParameterRedactor) fixture.getMethod(factory).invoke(null);
				assertTrue(ParameterRedactor.class.isInstance(redactor), "The runtime interface must be the current one");
				Database database = database(redactor);
				StatementContext<?> context = StatementContext.with(Statement.of("legacy-redactor", "VALUES(?)"), database)
						.parameters(List.of(1)).build();
				assertThrows(AbstractMethodError.class, context::getRedactedParameters);
				database.query("CREATE TABLE item (id INTEGER PRIMARY KEY)").execute();
				database.query("INSERT INTO item VALUES (1)").execute();
				DatabaseException failure = assertThrows(DatabaseException.class,
						() -> database.query("INSERT INTO item VALUES (:id)").bind("id", 1).execute());
				assertTrue(failure.isUniqueConstraintViolation());
				assertEquals(Optional.of("23505"), failure.getSqlState());
				assertInstanceOf(SQLException.class, failure.getCause());
				assertTrue(Arrays.stream(failure.getSuppressed()).anyMatch(AbstractMethodError.class::isInstance));
				assertTrue(failure.getMessage().contains("<unavailable>"));
			}
		}
	}

	@Test
	void oldCompiledDirectCallTargetsTheRemovedObjectReturnDescriptor() throws Exception {
		try (URLClassLoader loader = compileLegacyFixture()) {
			Class<?> fixture = loader.loadClass("legacy.OldRedactorFixture");
			StatementContext<?> context = StatementContext.with(Statement.of("old-caller", "VALUES(?)"), database(ParameterRedactor.none()))
					.parameters(List.of(1)).build();
			InvocationTargetException failure = assertThrows(InvocationTargetException.class,
					() -> fixture.getMethod("call", ParameterRedactor.class, StatementContext.class)
							.invoke(null, ParameterRedactor.none(), context));
			assertInstanceOf(NoSuchMethodError.class, failure.getCause());
		}
	}

	@Test
	void recompiledObjectAssignmentsReceiveTheOptionalWithoutASourceError() {
		StatementContext<?> context = StatementContext.with(Statement.of("new-caller", "VALUES(?)"), database(ParameterRedactor.none()))
				.parameters(List.of("value")).build();
		Object shown = ParameterRedactor.none().redactParameter(context, 0, "value");
		assertEquals(Optional.of("value"), shown);
		assertEquals("value", ParameterRedactor.none().redactParameter(context, 0, "value").orElseThrow());
	}

	private URLClassLoader compileLegacyFixture() throws Exception {
		Path sources = Files.createTempDirectory(directory, "legacy-source-");
		Path classes = Files.createTempDirectory(directory, "legacy-classes-");
		Path spi = sources.resolve("com/pyranid/ParameterRedactor.java");
		Path fixture = sources.resolve("legacy/OldRedactorFixture.java");
		Files.createDirectories(spi.getParent());
		Files.createDirectories(fixture.getParent());
		// Reproduce the released 4.8 erased descriptor, independent of a pre-populated Maven cache.
		Files.writeString(spi, """
				package com.pyranid;
				@FunctionalInterface
				public interface ParameterRedactor {
				  Object redactParameter(StatementContext<?> context, Integer index, Object value);
				}
				""");
		Files.writeString(fixture, """
				package legacy;
				import com.pyranid.*;
				public final class OldRedactorFixture {
				  public static ParameterRedactor lambda() {
				    return (context, index, value) -> "legacy-mask";
				  }
				  public static ParameterRedactor anonymous() {
				    return new ParameterRedactor() {
				      public Object redactParameter(StatementContext<?> context, Integer index, Object value) {
				        return "legacy-mask";
				      }
				    };
				  }
				  public static Object call(ParameterRedactor redactor, StatementContext<?> context) {
				    return redactor.redactParameter(context, 0, "value");
				  }
				}
				""");
		ByteArrayOutputStream diagnostics = new ByteArrayOutputStream();
		assertEquals(0, ToolProvider.getSystemJavaCompiler().run(null, diagnostics, diagnostics,
				"--release", "17", "-classpath", System.getProperty("java.class.path"), "-d", classes.toString(),
				spi.toString(), fixture.toString()), diagnostics::toString);
		// Parent-first loading intentionally ignores the compile-time SPI stub at runtime.
		return new URLClassLoader(new java.net.URL[]{classes.toUri().toURL()}, ParameterRedactor.class.getClassLoader());
	}

	private static Database database(ParameterRedactor redactor) {
		JDBCDataSource source = new JDBCDataSource();
		source.setUrl("jdbc:hsqldb:mem:redactor_linkage_" + UUID.randomUUID());
		source.setUser("sa");
		return Database.with(source, DatabaseType.GENERIC).parameterRedactor(redactor).build();
	}
}
