/*
 * Copyright 2015-2022 Transmogrify LLC, 2022-2026 Revetware LLC.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may obtain a copy at https://www.apache.org/licenses/LICENSE-2.0
 */
package com.pyranid;

import javax.tools.ToolProvider;
import java.io.ByteArrayOutputStream;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;

/** Driver-shaped classes live only in disposable loaders, never on the real integration driver's class path. */
final class OracleIntervalTestDriver {
	private OracleIntervalTestDriver() {}
	static Loader sharedLoader() { return Holder.SHARED; }
	static Loader newLoader() throws Exception { return new Loader(Holder.CLASSES); }
	static Loader loaderWithoutDaySecond() throws Exception { return new Loader(Holder.CLASSES, false); }
	static Loader loaderWithoutDaySecondConstructor() throws Exception { return new Loader(Holder.NO_CONSTRUCTOR_CLASSES); }

	private static final class Holder {
		static final Path CLASSES = compile(true);
		static final Path NO_CONSTRUCTOR_CLASSES = compile(false);
		static final Loader SHARED;
		static {
			try { SHARED = new Loader(CLASSES); }
			catch (Exception failure) { throw new ExceptionInInitializerError(failure); }
		}
		private static Path compile(boolean stringConstructor) {
			try {
				Path directory = Files.createTempDirectory("pyranid-oracle-interval-fixture-");
				Path api = directory.resolve("sources/oracle/jdbc/OracleConnection.java");
				Path value = directory.resolve("sources/oracle/sql/INTERVALDS.java");
				Files.createDirectories(api.getParent()); Files.createDirectories(value.getParent());
				Path classes = directory.resolve("classes"); Files.createDirectory(classes);
				Files.writeString(api, "package oracle.jdbc; public interface OracleConnection {}\n");
				Files.writeString(value, """
						package oracle.sql;
						public final class INTERVALDS {
						  public static Throwable failure;
						  private final String value;
						  public INTERVALDS(%s) throws java.sql.SQLException {
						    if (failure instanceof java.sql.SQLException sql) throw sql;
						    if (failure instanceof RuntimeException runtime) throw runtime;
						    if (failure instanceof Error error) throw error;
						    this.value = %s;
						  }
						  public String toString() { return value; }
						}
						""".formatted(stringConstructor ? "String value" : "", stringConstructor ? "value" : "\"no String constructor\""));
				ByteArrayOutputStream diagnostics = new ByteArrayOutputStream();
				int result = ToolProvider.getSystemJavaCompiler().run(null, diagnostics, diagnostics, "--release", "17", "-d",
						classes.toString(), api.toString(), value.toString());
				if (result != 0) throw new AssertionError(diagnostics.toString());
				return classes;
			} catch (Exception failure) { throw new ExceptionInInitializerError(failure); }
		}
	}

	static final class Loader extends URLClassLoader {
		private final boolean daySecondVisible;
		Loader(Path classes) throws Exception {
			this(classes, true);
		}
		Loader(Path classes, boolean daySecondVisible) throws Exception {
			super(new URL[]{classes.toUri().toURL()}, OracleIntervalTestDriver.class.getClassLoader());
			this.daySecondVisible = daySecondVisible;
		}
		@Override protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
			if (!this.daySecondVisible && name.equals("oracle.sql.INTERVALDS")) throw new ClassNotFoundException(name);
			if (!name.equals("oracle.jdbc.OracleConnection") && !name.equals("oracle.sql.INTERVALDS"))
				return super.loadClass(name, resolve);
			synchronized (getClassLoadingLock(name)) {
				Class<?> result = findLoadedClass(name);
				if (result == null) result = findClass(name);
				if (resolve) resolveClass(result);
				return result;
			}
		}
	}
}
