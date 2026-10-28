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
import org.junit.jupiter.api.Test;

import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Predicate;

import static org.junit.jupiter.api.Assertions.*;

public class TransactionHookRemovalTests {

	@Test
	public void matchingEqualityCanAppendUnrelatedHooksWithoutRestarting() {
		Database database = database();
		AtomicInteger comparisons = new AtomicInteger(), originalCalls = new AtomicInteger(), addedCalls = new AtomicInteger();
		database.transaction(() -> {
			Transaction transaction = database.currentTransaction().orElseThrow();
			class KeyedHook implements Consumer<TransactionResult> {
				private final int key;
				private final boolean appendDuringEquality;
				private KeyedHook(int key, boolean appendDuringEquality) {
					this.key = key;
					this.appendDuringEquality = appendDuringEquality;
				}
				@Override public void accept(TransactionResult result) { originalCalls.incrementAndGet(); }
				@Override public boolean equals(Object other) {
					if (this.appendDuringEquality) {
						assertTrue(comparisons.incrementAndGet() < 8, "Hook removal must make bounded progress");
						transaction.addPostTransactionOperation(result -> addedCalls.incrementAndGet());
					}
					return other instanceof KeyedHook hook && hook.key == this.key;
				}
				@Override public int hashCode() { return this.key; }
			}
			Consumer<TransactionResult> registered = new KeyedHook(1, false);
			Consumer<TransactionResult> equalRegistration = new KeyedHook(1, false);
			transaction.addPostTransactionOperation(registered);
			transaction.addPostTransactionOperation(equalRegistration);
			assertTrue(transaction.removePostTransactionOperation(new KeyedHook(1, true)));
			assertSame(equalRegistration, transaction.getPostTransactionOperations().get(0));
		});
		assertEquals(1, comparisons.get());
		assertEquals(1, originalCalls.get());
		assertEquals(1, addedCalls.get());
	}

	@Test
	public void absentEqualityDoesNotChaseHooksAppendedDuringTheSearch() {
		Database database = database();
		AtomicInteger comparisons = new AtomicInteger(), calls = new AtomicInteger();
		database.transaction(() -> {
			Transaction transaction = database.currentTransaction().orElseThrow();
			Consumer<TransactionResult> original = result -> calls.incrementAndGet();
			Consumer<TransactionResult> appendedMatch = result -> calls.addAndGet(10);
			transaction.addPostTransactionOperation(original);
			assertFalse(transaction.removePostTransactionOperation(matcher(other -> {
				assertTrue(comparisons.incrementAndGet() < 8, "An absent hook must not restart the search");
				transaction.addPostTransactionOperation(appendedMatch);
				return other == appendedMatch;
			})));
			assertEquals(2, transaction.getPostTransactionOperations().size());
			assertSame(original, transaction.getPostTransactionOperations().get(0));
			assertSame(appendedMatch, transaction.getPostTransactionOperations().get(1));
		});
		assertEquals(1, comparisons.get());
		assertEquals(11, calls.get());
	}

	@Test
	public void removalContinuesPastAnEqualHookRemovedReentrantly() {
		Database database = database();
		AtomicInteger comparisons = new AtomicInteger(), calls = new AtomicInteger();
		database.transaction(() -> {
			Transaction transaction = database.currentTransaction().orElseThrow();
			Consumer<TransactionResult> first = result -> calls.incrementAndGet();
			Consumer<TransactionResult> second = result -> calls.addAndGet(10);
			transaction.addPostTransactionOperation(first);
			transaction.addPostTransactionOperation(second);
			assertTrue(transaction.removePostTransactionOperation(matcher(other -> {
				assertTrue(comparisons.incrementAndGet() < 8, "Reentrant removal must not restart the search");
				if (other == first) assertTrue(transaction.removePostTransactionOperation(first));
				return other == first || other == second;
			})));
			assertTrue(transaction.getPostTransactionOperations().isEmpty());
		});
		assertEquals(2, comparisons.get());
		assertEquals(0, calls.get());
	}

	@Test
	public void onlyOneDuplicateRegistrationIsRemovedWhenEqualityAppendsAHook() {
		Database database = database();
		AtomicInteger comparisons = new AtomicInteger(), duplicateCalls = new AtomicInteger(), addedCalls = new AtomicInteger();
		database.transaction(() -> {
			Transaction transaction = database.currentTransaction().orElseThrow();
			Consumer<TransactionResult> duplicate = result -> duplicateCalls.incrementAndGet();
			transaction.addPostTransactionOperation(duplicate);
			transaction.addPostTransactionOperation(duplicate);
			assertTrue(transaction.removePostTransactionOperation(matcher(other -> {
				assertTrue(comparisons.incrementAndGet() < 8, "Duplicate removal must not restart the search");
				transaction.addPostTransactionOperation(result -> addedCalls.incrementAndGet());
				return other == duplicate;
			})));
			assertEquals(2, transaction.getPostTransactionOperations().size());
			assertSame(duplicate, transaction.getPostTransactionOperations().get(0));
		});
		assertEquals(1, comparisons.get());
		assertEquals(1, duplicateCalls.get());
		assertEquals(1, addedCalls.get());
	}

	@Test
	public void concurrentRemovalCanShiftTheMatchedCandidateWithoutChangingItsIdentity() {
		Database database = database();
		AtomicInteger firstCalls = new AtomicInteger(), secondCalls = new AtomicInteger();
		database.transaction(() -> {
			Transaction transaction = database.currentTransaction().orElseThrow();
			Consumer<TransactionResult> first = result -> firstCalls.incrementAndGet();
			Consumer<TransactionResult> second = result -> secondCalls.incrementAndGet();
			transaction.addPostTransactionOperation(first);
			transaction.addPostTransactionOperation(second);
			assertTrue(transaction.removePostTransactionOperation(matcher(other -> {
				if (other != second) return false;
				AtomicReference<Throwable> failure = new AtomicReference<>();
				Thread worker = worker(() -> assertTrue(transaction.removePostTransactionOperation(first)), failure);
				join(worker);
				assertNull(failure.get());
				return true;
			})));
			assertTrue(transaction.getPostTransactionOperations().isEmpty());
		});
		assertEquals(0, firstCalls.get());
		assertEquals(0, secondCalls.get());
	}

	@Test
	public void completionWinsOverAnInFlightEqualityComparisonEvenWhenNoMatchExists() {
		for (boolean matches : new boolean[]{true, false}) {
			Database database = database();
			CountDownLatch registered = new CountDownLatch(1), finishBody = new CountDownLatch(1);
			CountDownLatch equalityEntered = new CountDownLatch(1), finishEquality = new CountDownLatch(1);
			AtomicReference<Transaction> transaction = new AtomicReference<>();
			AtomicReference<Throwable> ownerFailure = new AtomicReference<>(), removalFailure = new AtomicReference<>();
			AtomicInteger hookCalls = new AtomicInteger();
			Consumer<TransactionResult> stored = result -> hookCalls.incrementAndGet();
			Thread owner = worker(() -> database.transaction(() -> {
				transaction.set(database.currentTransaction().orElseThrow());
				transaction.get().addPostTransactionOperation(stored);
				registered.countDown();
				await(finishBody);
			}), ownerFailure);
			await(registered);
			Thread remover = worker(() -> transaction.get().removePostTransactionOperation(matcher(other -> {
				equalityEntered.countDown();
				await(finishEquality);
				return matches;
			})), removalFailure);
			try {
				await(equalityEntered);
				finishBody.countDown();
				join(owner);
				assertNull(ownerFailure.get());
				assertEquals(1, hookCalls.get());
			} finally {
				finishBody.countDown();
				finishEquality.countDown();
				join(owner);
				join(remover);
			}
			assertInstanceOf(IllegalStateException.class, removalFailure.get());
			assertEquals(1, transaction.get().getPostTransactionOperations().size());
			assertSame(stored, transaction.get().getPostTransactionOperations().get(0));
		}
	}

	private static Consumer<TransactionResult> matcher(Predicate<Object> equals) {
		return new Consumer<>() {
			@Override public void accept(TransactionResult result) { fail("The search operation is never registered"); }
			@Override public boolean equals(Object other) { return equals.test(other); }
			@Override public int hashCode() { return 1; }
		};
	}

	private static Database database() {
		JDBCDataSource source = new JDBCDataSource();
		source.setUrl("jdbc:hsqldb:mem:hook_removal_" + UUID.randomUUID());
		return Database.withDataSource(source).databaseType(DatabaseType.GENERIC).build();
	}

	private static Thread worker(Runnable operation, AtomicReference<Throwable> failure) {
		Thread thread = new Thread(() -> {
			try { operation.run(); } catch (Throwable thrown) { failure.set(thrown); }
		});
		thread.setDaemon(true);
		thread.start();
		return thread;
	}

	private static void await(CountDownLatch latch) {
		try { assertTrue(latch.await(5, TimeUnit.SECONDS), "Timed out waiting for test boundary"); }
		catch (InterruptedException failure) { Thread.currentThread().interrupt(); throw new AssertionError(failure); }
	}

	private static void join(Thread thread) {
		try { thread.join(5_000); }
		catch (InterruptedException failure) { Thread.currentThread().interrupt(); throw new AssertionError(failure); }
		assertFalse(thread.isAlive(), "Worker did not complete");
	}
}
