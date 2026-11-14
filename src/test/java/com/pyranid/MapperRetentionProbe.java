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

import javax.sql.DataSource;
import java.io.IOException;
import java.lang.ref.Reference;
import java.lang.ref.WeakReference;
import java.net.URL;
import java.net.URLClassLoader;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/** Runs without the test runner's own reflection caches and with a bounded heap and collection controls. */
public final class MapperRetentionProbe {
	public record Row(Long id) {}

	public static void main(String[] arguments) throws Exception {
		JDBCDataSource source = new JDBCDataSource();
		for (boolean cached : new boolean[]{false, true}) {
			for (boolean callbackBackreference : new boolean[]{false, true}) {
				List<WeakReference<?>> discarded = discardMappers(source, cached, callbackBackreference);
				awaitCollection(discarded, "discarded mappers; cached=" + cached + ", callback=" + callbackBackreference);
			}
		}
		awaitCollection(discardBinders(source, false), "unused custom binders and their Database owners");
		awaitCollection(discardBinders(source, true), "cached custom binders and their Database owners");
		Database database = Database.with(source, DatabaseType.GENERIC).build();
		awaitCollection(discardCursors(database), "cursors of a retained mapper");
		Reference.reachabilityFence(database);

		ExecutorService worker = Executors.newSingleThreadExecutor(runnable -> {
			Thread thread = new Thread(runnable, "retention-probe-worker");
			thread.setDaemon(true);
			return thread;
		});
		try (ChildFirstLoader retained = applicationLoader()) {
			WeakReference<ClassLoader> retainedControl = new WeakReference<>(retained);
			for (String mode : List.of("construct", "long", "string", "record", "failure", "pgobject", "binder")) {
				List<WeakReference<?>> loaders = new ArrayList<>();
				for (int deployment = 0; deployment < 3; deployment++)
					loaders.add(deploy(worker, source, mode));
				// A parent-owned task ensures the worker is idle and no previous invocation remains on its stack.
				worker.submit(() -> {}).get(5, TimeUnit.SECONDS);
				awaitCollection(loaders, "application loaders on a live pooled worker; mode=" + mode);
				check(!retainedControl.refersTo(null), "Strongly retained loader control was unexpectedly collected");
				System.out.println("Collected application loaders: " + mode);
			}
			Reference.reachabilityFence(retained);
		} finally {
			worker.shutdownNow();
			check(worker.awaitTermination(5, TimeUnit.SECONDS), "Worker did not terminate");
		}
		System.out.println("PASS: mapper, cursor, driver-factory and worker-loader reclamation");
	}

	private static List<WeakReference<?>> discardBinders(DataSource source, boolean populateCache) throws Exception {
		List<WeakReference<?>> references = new ArrayList<>();
		for (int index = 0; index < 20; index++) {
			Database[] owner = new Database[1];
			CustomParameterBinder callback = new CustomParameterBinder() {
				@Override public Boolean appliesTo(TargetType type) { return type.matchesClass(String.class); }
				@Override public BindingResult bind(StatementContext<?> context, java.sql.PreparedStatement statement,
						Integer parameterIndex, Object value) throws java.sql.SQLException {
					check(owner[0] != null, "Custom binder lost its configured Database");
					statement.setString(parameterIndex, value.toString());
					return BindingResult.handled();
				}
			};
			PreparedStatementBinder binder = PreparedStatementBinder.withCustomParameterBinders(List.of(callback));
			Database database = Database.with(source, DatabaseType.GENERIC).preparedStatementBinder(binder).build();
			owner[0] = database;
			if (populateCache) MappingRetentionApplication.bind(binder, database);
			references.add(new WeakReference<>(binder));
			references.add(new WeakReference<>(database));
			references.add(new WeakReference<>(callback));
		}
		return references;
	}

	private static List<WeakReference<?>> discardMappers(DataSource source, boolean cached, boolean callbackBackreference) throws Exception {
		List<WeakReference<?>> references = new ArrayList<>();
		for (int index = 0; index < 40; index++) {
			Database[] callbackOwner = new Database[1];
			CustomColumnMapper callback = new CustomColumnMapper() {
				@Override public Boolean appliesTo(TargetType type) { return type.matchesClass(Long.class); }
				@Override public MappingResult map(StatementContext<?> context, java.sql.ResultSet result, Object value,
						TargetType type, Integer column, String label, InstanceProvider instances) {
					check(callbackOwner[0] != null, "Custom mapper lost its configured Database");
					return MappingResult.fallback();
				}
			};
			ResultSetMapper mapper = ResultSetMapper.withPlanCachingEnabled(cached)
					.customColumnMappers(callbackBackreference ? List.of(callback) : List.of()).build();
			Database database = Database.with(source, DatabaseType.GENERIC).resultSetMapper(mapper).build();
			callbackOwner[0] = database;
			StatementContext<Row> context = StatementContext.<Row>with(Statement.of("retention", "SELECT id"), database)
					.resultSetRowType(Row.class).build();
			try (var cursor = MappingRetentionApplication.cursor(1, "id")) {
				check(new Row(1L).equals(mapper.map(context, cursor, Row.class, database.getInstanceProvider()).orElseThrow()),
						"Record mapping control returned the wrong row");
			}
			references.add(new WeakReference<>(mapper));
			references.add(new WeakReference<>(database));
		}
		return references;
	}

	private static List<WeakReference<?>> discardCursors(Database database) throws Exception {
		List<WeakReference<?>> references = new ArrayList<>();
		StatementContext<Row> context = StatementContext.<Row>with(Statement.of("retention", "SELECT id"), database)
				.resultSetRowType(Row.class).build();
		for (int index = 0; index < 30; index++) {
			try (var cursor = MappingRetentionApplication.cursor(1, "id")) {
				database.getResultSetMapper().map(context, cursor, Row.class, database.getInstanceProvider()).orElseThrow();
				references.add(new WeakReference<>(cursor));
			}
		}
		return references;
	}

	private static WeakReference<ClassLoader> deploy(ExecutorService worker, DataSource source, String mode) throws Exception {
		ChildFirstLoader loader = applicationLoader();
		WeakReference<ClassLoader> reference = new WeakReference<>(loader);
		try {
			String output = worker.submit(() -> {
				Class<?> application = loader.loadClass(MappingRetentionApplication.class.getName());
				check(application.getClassLoader() == loader, "Application was not child-loaded");
				check(loader.loadClass(Database.class.getName()).getClassLoader() == loader, "Pyranid was not child-loaded");
				return (String) application.getMethod("exercise", String.class, DataSource.class).invoke(null, mode, source);
			}).get(10, TimeUnit.SECONDS);
			check(output.equals(mode), "Child-loaded application control failed");
		} finally {
			loader.close();
		}
		return reference;
	}

	private static ChildFirstLoader applicationLoader() throws IOException {
		return new ChildFirstLoader(new URL[]{Database.class.getProtectionDomain().getCodeSource().getLocation(),
				MappingRetentionApplication.class.getProtectionDomain().getCodeSource().getLocation()});
	}

	private static void awaitCollection(List<WeakReference<?>> references, String description) throws InterruptedException {
		WeakReference<Object> ordinaryControl = new WeakReference<>(new byte[1024]);
		Object retainedControl = new Object();
		WeakReference<Object> retainedReference = new WeakReference<>(retainedControl);
		long end = System.nanoTime() + Duration.ofSeconds(5).toNanos();
		while (System.nanoTime() < end) {
			System.gc();
			if (ordinaryControl.refersTo(null) && references.stream().allMatch(reference -> reference.refersTo(null))) break;
			Thread.sleep(25);
		}
		check(ordinaryControl.refersTo(null), "Collection control did not run for " + description);
		check(!retainedReference.refersTo(null), "Retained collection control disappeared for " + description);
		long retained = references.stream().filter(reference -> !reference.refersTo(null)).count();
		check(retained == 0, "Retained " + retained + "/" + references.size() + ": " + description);
		Reference.reachabilityFence(retainedControl);
	}

	private static void check(boolean condition, String message) {
		if (!condition) throw new AssertionError(message);
	}

	private static final class ChildFirstLoader extends URLClassLoader {
		ChildFirstLoader(URL[] urls) { super(urls, MapperRetentionProbe.class.getClassLoader()); }

		@Override protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
			if (!name.startsWith("com.pyranid.")) return super.loadClass(name, resolve);
			synchronized (getClassLoadingLock(name)) {
				Class<?> type = findLoadedClass(name);
				if (type == null) type = findClass(name);
				if (resolve) resolveClass(type);
				return type;
			}
		}
	}
}
