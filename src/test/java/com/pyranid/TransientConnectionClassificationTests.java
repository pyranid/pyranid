/*
 * Copyright 2015-2022 Transmogrify LLC, 2022-2026 Revetware LLC.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may obtain a copy at https://www.apache.org/licenses/LICENSE-2.0
 */
package com.pyranid;

import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.sql.SQLRecoverableException;
import java.sql.SQLTimeoutException;
import java.sql.SQLTransactionRollbackException;
import java.sql.SQLTransientConnectionException;
import java.sql.SQLTransientException;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class TransientConnectionClassificationTests {
	@Test
	void connectionSubtypeRequiresConnectionStateOrNoStateWhileRollbackStatesStillCount() {
		for (DatabaseType type : List.of(DatabaseType.GENERIC, DatabaseType.MYSQL, DatabaseType.MARIA_DB)) {
			for (String state : new String[]{null, "08001", "08006", "08S01", "08", "40001", "40P01"})
				assertTrue(classify(type, new SQLTransientConnectionException("driver report", state)).isTransient(),
						type + " " + state);
			for (String state : List.of("", "45000", "HY000", "HYT00", "42000", "23505"))
				assertFalse(classify(type, new SQLTransientConnectionException("business error", state)).isTransient(),
						type + " " + state);
		}
	}

	@Test
	void causeAndNextGraphsPreserveBusinessErrorsAndIndependentOperationalEvidence() {
		for (String edge : List.of("cause", "next")) {
			SQLException business = new SQLTransientConnectionException("business signal", "45000", 1644);
			SQLException wrapper = new SQLException("driver wrapper", "HY000");
			attach(wrapper, business, edge);
			assertFalse(classify(DatabaseType.MARIA_DB, wrapper).isTransient(), edge);

			SQLException transport = new SQLException("connection failure", "08006");
			attach(business, transport, edge);
			assertTrue(classify(DatabaseType.MARIA_DB, wrapper).isTransient(), edge);
			// A cycle/shared node cannot erase operational evidence or cause recursion.
			transport.setNextException(wrapper);
			assertTrue(classify(DatabaseType.MARIA_DB, wrapper).isTransient(), edge);
		}
		SQLException business = new SQLTransientConnectionException("business signal", "45000", 1644);
		business.addSuppressed(new SQLException("cleanup connection failure", "08006"));
		assertFalse(classify(DatabaseType.MARIA_DB, business).isTransient(),
				"Suppressed cleanup is not primary/cause/next classification evidence");
	}

	@Test
	void otherExplicitTransientAndRecoverableClassesRetainTheirMeaning() {
		for (SQLException exception : List.of(new SQLTransientException("explicit transient", "45000"),
				new SQLTimeoutException("timeout", "HYT00"),
				new SQLTransactionRollbackException("rollback", "45000"),
				new SQLRecoverableException("recoverable", "HY000"))) {
			assertTrue(classify(DatabaseType.GENERIC, exception).isTransient(), exception.getClass().getName());
		}
		assertTrue(classify(DatabaseType.GENERIC, new SQLException("connection", "08006")).isTransient());
		assertTrue(classify(DatabaseType.GENERIC, new SQLException("rollback", "40001")).isTransient());
		assertFalse(classify(DatabaseType.GENERIC, new SQLException("business", "45000")).isTransient());
	}

	@Test
	void unreadableSubtypeDiagnosticsRetainTheOperationFailureWithoutAuthorizingRetry() {
		AtomicInteger reads = new AtomicInteger();
		IllegalStateException diagnostic = new IllegalStateException("SQLState accessor failed");
		SQLException failure = new SQLTransientConnectionException("operation failed") {
			@Override public String getSQLState() {
				if (reads.incrementAndGet() == 1) return null;
				throw diagnostic;
			}
		};
		DatabaseException wrapped = DatabaseException.wrapSafely("Database operation failed", failure, GenericDialect.INSTANCE);
		assertSame(failure, wrapped.getCause());
		assertFalse(wrapped.isTransient());
		assertTrue(List.of(failure.getSuppressed()).contains(diagnostic));
	}

	private static DatabaseException classify(DatabaseType type, SQLException exception) {
		return new DatabaseException("operation failed", exception, type.dialect());
	}

	private static void attach(SQLException primary, SQLException detail, String edge) {
		if (edge.equals("cause")) primary.initCause(detail);
		else primary.setNextException(detail);
	}
}
