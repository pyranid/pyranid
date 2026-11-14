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
import java.lang.reflect.Proxy;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/** Public-API helpers, intentionally local recipes rather than new nesting modes. */
class TransactionRecipeTests {
	@Test
	void joinOrBeginStartsAClosureOutsideATransactionAndJoinsTheCurrentOwnerInside() {
		Fixture fixture = new Fixture();
		assertTrue(fixture.database.currentTransaction().isEmpty());
		assertEquals(Optional.of("outside"), joinOrBegin(fixture.database, () -> {
			assertTrue(fixture.database.currentTransaction().isPresent());
			fixture.insert(1);
			return Optional.of("outside");
		}));
		int acquisitions = fixture.acquisitions.get();
		fixture.database.transaction(() -> {
			Transaction owner = fixture.database.currentTransaction().orElseThrow();
			fixture.insert(2);
			assertEquals(Optional.of("joined"), joinOrBegin(fixture.database, () -> {
				assertSame(owner, fixture.database.currentTransaction().orElseThrow());
				fixture.insert(3);
				return Optional.of("joined");
			}));
			assertEquals(acquisitions + 1, fixture.acquisitions.get(), "Joining must reuse the physical transaction connection");
		});
		assertEquals(List.of(1, 2, 3), fixture.ids());
		assertTrue(fixture.database.currentTransaction().isEmpty());
	}

	@Test
	void caughtJoinedFailureStillRollsBackTheOwnerEvenWhenItsCallbackReturnsNormally() {
		Fixture fixture = new Fixture();
		IllegalStateException rejection = new IllegalStateException("joined operation failed");
		TransactionOutcome<String> outcome = fixture.database.transactionWithOutcome(() -> {
			fixture.insert(1);
			assertSame(rejection, assertThrows(IllegalStateException.class, () -> joinOrBegin(fixture.database, () -> {
				fixture.insert(2);
				throw rejection;
			})));
			assertTrue(fixture.database.currentTransaction().orElseThrow().isRollbackOnly());
			fixture.insert(3);
			return Optional.of("handled");
		});
		assertEquals(TransactionResult.ROLLED_BACK, outcome.getResult());
		assertEquals(Optional.of("handled"), outcome.getAttemptedValue());
		assertTrue(outcome.getFailure().isEmpty(), "A normally returned value is not evidence of commit");
		assertEquals(List.of(), fixture.ids());
	}

	@Test
	void savepointOrBeginStartsAClosureOutsideATransactionAndReusesTheOwnerInside() {
		Fixture fixture = new Fixture();
		assertEquals(Optional.of(1), savepointOrBegin(fixture.database, () -> {
			fixture.insert(1);
			return Optional.of(1);
		}));
		fixture.database.transaction(() -> {
			Transaction owner = fixture.database.currentTransaction().orElseThrow();
			fixture.insert(2);
			int acquisitions = fixture.acquisitions.get();
			assertEquals(Optional.of(3), savepointOrBegin(fixture.database, () -> {
				assertSame(owner, fixture.database.currentTransaction().orElseThrow());
				fixture.insert(3);
				return Optional.of(3);
			}));
			assertEquals(acquisitions, fixture.acquisitions.get());
		});
		assertEquals(List.of(1, 2, 3), fixture.ids());
	}

	@Test
	void caughtSavepointFailureDiscardsOnlyItsWorkAndTheOuterTransactionCanCommit() {
		Fixture fixture = new Fixture();
		IllegalStateException rejection = new IllegalStateException("savepoint operation failed");
		TransactionOutcome<String> outcome = fixture.database.transactionWithOutcome(() -> {
			fixture.insert(1);
			assertSame(rejection, assertThrows(IllegalStateException.class, () -> savepointOrBegin(fixture.database, () -> {
				fixture.insert(2);
				throw rejection;
			})));
			assertFalse(fixture.database.currentTransaction().orElseThrow().isRollbackOnly());
			fixture.insert(3);
			return Optional.of("continued");
		});
		assertEquals(TransactionResult.COMMITTED, outcome.getResult());
		assertEquals(Optional.of("continued"), outcome.getAttemptedValue());
		assertTrue(outcome.getFailure().isEmpty());
		assertEquals(List.of(1, 3), fixture.ids());
	}

	@Test
	void helperReturnValuesRetainThePublicNullOptionalConvention() {
		Fixture fixture = new Fixture();
		assertEquals(Optional.empty(), joinOrBegin(fixture.database, () -> null));
		assertEquals(Optional.empty(), savepointOrBegin(fixture.database, () -> null));
		fixture.database.transaction(() -> {
			assertEquals(Optional.empty(), joinOrBegin(fixture.database, () -> null));
			assertEquals(Optional.empty(), savepointOrBegin(fixture.database, () -> null));
		});
	}

	static <T> Optional<T> joinOrBegin(Database database, ReturningTransactionalOperation<T> operation) {
		Transaction current = database.currentTransaction().orElse(null);
		return current == null ? database.transaction(operation) : database.participate(current, operation);
	}

	static <T> Optional<T> savepointOrBegin(Database database, ReturningTransactionalOperation<T> operation) {
		Transaction current = database.currentTransaction().orElse(null);
		return current == null ? database.transaction(operation) : current.withSavepoint(operation);
	}

	private static final class Fixture {
		private final AtomicInteger acquisitions = new AtomicInteger();
		private final Database database;

		private Fixture() {
			JDBCDataSource nativeSource = new JDBCDataSource();
			nativeSource.setUrl("jdbc:hsqldb:mem:transaction_recipe_" + UUID.randomUUID());
			nativeSource.setUser("sa"); nativeSource.setPassword("");
			DataSource source = (DataSource) Proxy.newProxyInstance(getClass().getClassLoader(), new Class<?>[]{DataSource.class},
					(proxy, method, args) -> {
						if (method.getName().equals("getConnection")) this.acquisitions.incrementAndGet();
						try { return method.invoke(nativeSource, args); }
						catch (InvocationTargetException failure) { throw failure.getCause(); }
					});
			this.database = Database.with(source, DatabaseType.GENERIC).build();
			this.database.query("CREATE TABLE work (id INTEGER PRIMARY KEY)").execute();
		}

		private void insert(int id) { this.database.query("INSERT INTO work VALUES (:id)").bind("id", id).execute(); }
		private List<Integer> ids() { return this.database.query("SELECT id FROM work ORDER BY id").fetchList(Integer.class); }
	}
}
