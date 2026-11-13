/*
 * Copyright 2015-2022 Transmogrify LLC, 2022-2026 Revetware LLC.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may obtain a copy at https://www.apache.org/licenses/LICENSE-2.0
 */
package com.pyranid;

import org.hsqldb.jdbc.JDBCDataSource;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

class LockContentionClassificationTests {
	@Test
	void lockTimeoutCodesClassifyAcrossPrimaryCauseAndNextExceptionEdges() {
		for (DatabaseType type : List.of(DatabaseType.POSTGRESQL, DatabaseType.ORACLE)) {
			for (int code : type == DatabaseType.POSTGRESQL ? new int[]{0} : new int[]{54, 30006}) {
				for (String edge : List.of("primary", "cause", "next", "cycle")) {
					SQLException detail = new SQLException("lock unavailable", type == DatabaseType.POSTGRESQL ? "55P03" : "72000", code);
					DatabaseException failure = new DatabaseException("operation failed", graph(detail, edge), type.dialect());
					assertTrue(failure.isTimeout(), type + " " + code + " " + edge);
					assertTrue(failure.isTransient(), type + " " + code + " " + edge);
					assertFalse(failure.isSerializationFailure());
					assertFalse(failure.isDeadlock());
					assertTrue(RetryPolicy.Condition.timeout().shouldRetry(failure));
				}
			}
		}
	}

	@Test
	void snapshotUpdateConflictsAreTransientSerializationFailuresRatherThanTimeouts() {
		for (String edge : List.of("primary", "cause", "next", "cycle")) {
			DatabaseException failure = new DatabaseException("snapshot conflict", graph(new SQLException("snapshot conflict", "42000", 3960), edge), SqlServerDialect.INSTANCE);
			assertTrue(failure.isTransient(), edge);
			assertTrue(failure.isSerializationFailure(), edge);
			assertFalse(failure.isTimeout(), edge);
			assertFalse(failure.isDeadlock(), edge);
		}
	}

	@Test
	void vendorSpecificContentionDoesNotBroadenOtherDialectsOrCommitAbortProof() {
		for (SQLException failure : List.of(new SQLException("Oracle NOWAIT", "72000", 54),
				new SQLException("Oracle WAIT", "72000", 30006), new SQLException("snapshot conflict", "42000", 3960),
				new SQLException("PostgreSQL lock unavailable", "55P03", 0))) {
			DatabaseException generic = new DatabaseException("generic", failure, GenericDialect.INSTANCE);
			assertFalse(generic.isTransient());
			assertFalse(generic.isTimeout());
			for (DatabaseType type : List.of(DatabaseType.POSTGRESQL, DatabaseType.ORACLE, DatabaseType.SQL_SERVER))
				assertFalse(TransactionCommitEvidence.provesSerializationAbort(failure, type.dialect()));
		}
		for (DatabaseType type : List.of(DatabaseType.POSTGRESQL, DatabaseType.ORACLE, DatabaseType.SQL_SERVER)) {
			DatabaseException syntax = new DatabaseException("syntax", new SQLException("syntax", "42000", 942), type.dialect());
			assertFalse(syntax.isTransient());
			assertFalse(syntax.isTimeout());
		}
		assertTrue(new DatabaseException("existing cancellation", new SQLException("cancelled", "57014"), PostgresDialect.INSTANCE).isTimeout());
		assertTrue(new DatabaseException("existing lock timeout", new SQLException("lock", "HY000", 1222), SqlServerDialect.INSTANCE).isTimeout());
	}

	@Test
	void timeoutClassificationCannotReplayAnUncertainCommitThatAlreadyPersisted() {
		SQLException acknowledgment = new SQLException("commit acknowledgment lost while lock unavailable", "55P03");
		Fixture fixture = new Fixture(acknowledgment, null);
		AtomicInteger attempts = new AtomicInteger(), policies = new AtomicInteger();
		AtomicReference<TransactionCompletion> completion = new AtomicReference<>();
		DatabaseException failure = assertThrows(DatabaseException.class, () -> fixture.database.transactionWithRetry(
				policy(policies), TransactionOptions.withReadOnly(null).completionObserver(completion::set).build(), () -> {
					attempts.incrementAndGet(); fixture.insert();
				}));
		assertSame(acknowledgment, failure.getCause());
		assertTrue(failure.isTimeout());
		assertTrue(failure.isTransient());
		assertEquals(TransactionResult.IN_DOUBT, completion.get().getResult());
		assertEquals(1, attempts.get());
		assertEquals(0, policies.get());
		assertEquals(1, fixture.commits.get());
		assertEquals(List.of(1), fixture.ids());
	}

	@Test
	void timeoutClassificationCannotReplayCommittedWorkAfterCleanupFailure() {
		SQLException cleanup = new SQLException("connection cleanup lock unavailable", "55P03");
		Fixture fixture = new Fixture(null, cleanup);
		AtomicInteger attempts = new AtomicInteger(), policies = new AtomicInteger();
		AtomicReference<TransactionCompletion> completion = new AtomicReference<>();
		DatabaseException failure = assertThrows(DatabaseException.class, () -> fixture.database.transactionWithRetry(
				policy(policies), TransactionOptions.withReadOnly(null).completionObserver(completion::set).build(), () -> {
					attempts.incrementAndGet(); fixture.insert();
				}));
		assertTrue(failure.isTimeout());
		assertEquals(TransactionResult.COMMITTED, completion.get().getResult());
		assertEquals(1, attempts.get());
		assertEquals(0, policies.get());
		assertEquals(1, fixture.commits.get());
		assertEquals(List.of(1), fixture.ids());
		assertTrue(ThrowableGraph.inspect(failure, current -> current == cleanup ? 1 : 0, 0) != 0);
	}

	private static RetryPolicy policy(AtomicInteger policies) {
		return RetryPolicy.ofMaxAttempts(2, RetryPolicy.Backoff.fixed(Duration.ZERO), failure -> {
			policies.incrementAndGet();
			return RetryPolicy.Condition.timeout().shouldRetry(failure);
		});
	}
	private static Throwable graph(SQLException detail, String edge) {
		if (edge.equals("primary")) return detail;
		if (edge.equals("cause")) return new IllegalStateException("wrapper", detail);
		SQLException wrapper = new SQLException("wrapper", "HY000");
		wrapper.setNextException(detail);
		if (edge.equals("cycle")) detail.setNextException(wrapper);
		return wrapper;
	}

	private static final class Fixture {
		final Database database, inspection;
		final AtomicInteger commits = new AtomicInteger();
		Fixture(SQLException commitFailure, SQLException closeFailure) {
			JDBCDataSource source = new JDBCDataSource();
			source.setUrl("jdbc:hsqldb:mem:lock_classification_" + UUID.randomUUID());
			source.setUser("sa"); source.setPassword("");
			this.inspection = Database.with(source, DatabaseType.GENERIC).build();
			this.inspection.query("CREATE TABLE work (id INT PRIMARY KEY)").execute();
			DataSource wrapped = proxy(DataSource.class, (method, args) -> {
				Object value = invoke(source, method, args);
				if (!(value instanceof Connection connection)) return value;
				return proxy(Connection.class, (jdbc, parameters) -> {
					Object result = invoke(connection, jdbc, parameters);
					if (jdbc.getName().equals("commit")) {
						commits.incrementAndGet();
						if (commitFailure != null) throw commitFailure;
					}
					if (jdbc.getName().equals("close") && closeFailure != null) throw closeFailure;
					return result;
				});
			});
			this.database = Database.with(wrapped, DatabaseType.POSTGRESQL).build();
		}
		void insert() { this.database.query("INSERT INTO work VALUES (1)").execute(); }
		List<Integer> ids() { return this.inspection.query("SELECT id FROM work").fetchList(Integer.class); }
	}
	@FunctionalInterface private interface Call { Object run(Method method, Object[] args) throws Throwable; }
	private static <T> T proxy(Class<T> type, Call call) {
		return type.cast(Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[]{type}, (proxy, method, args) -> call.run(method, args)));
	}
	private static Object invoke(Object target, Method method, Object[] args) throws Throwable {
		try { return method.invoke(target, args); }
		catch (InvocationTargetException failure) { throw failure.getCause(); }
	}
}
