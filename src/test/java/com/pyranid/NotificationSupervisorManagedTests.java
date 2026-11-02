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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import javax.sql.DataSource;
import java.lang.reflect.Proxy;
import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BooleanSupplier;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.*;

/** Exercises the managed supervisor through its public lifecycle and fake session boundary. */
@Timeout(15)
class NotificationSupervisorManagedTests {
	private static final Duration POLL_INTERVAL = Duration.ofMillis(10);
	private static final Database DATABASE = Database.with((DataSource) Proxy.newProxyInstance(
			NotificationSupervisorManagedTests.class.getClassLoader(), new Class<?>[]{DataSource.class},
			(proxy, method, args) -> { throw new AssertionError("Unexpected data source call: " + method); }), DatabaseType.POSTGRESQL).build();

	@Test
	void managedSettingsAreValidatedAndFactoriesAreLazy() throws Exception {
		assertThrows(NullPointerException.class, () -> builder().threadFactory(null));
		assertThrows(NullPointerException.class, () -> builder().pollEvery(null));
		assertThrows(IllegalArgumentException.class, () -> builder().pollEvery(Duration.ZERO));
		assertThrows(IllegalArgumentException.class, () -> builder().pollEvery(Duration.ofNanos(-1)));
		assertThrows(NullPointerException.class, () -> builder().onReconciliationFailure(null));
		AtomicInteger resolved = new AtomicInteger();
		NotificationSupervisor supervisor = build(builder().threadFactory(role -> {
			resolved.incrementAndGet();
			return Thread::new;
		}), operation -> fail("Stopped supervisor opened a session"));
		assertEquals(0, resolved.get());
		supervisor.requestShutdown();
		assertTrue(result(supervisor).isShutdownRequested());
		assertInstanceOf(CancellationException.class, readinessFailure(supervisor));
		assertThrows(IllegalStateException.class, supervisor::start);
		assertEquals(0, resolved.get());
	}

	@Test
	void defaultWorkersAreDaemonThreadsAndReconciliationHasItsOwnWorker() throws Exception {
		AtomicReference<Thread> listener = new AtomicReference<>();
		AtomicReference<Thread> reconciliation = new AtomicReference<>();
		CountDownLatch receiving = new CountDownLatch(1);
		NotificationSupervisor supervisor = build(builder().reconcile(() ->
				reconciliation.set(Thread.currentThread())), operation -> {
			listener.set(Thread.currentThread());
			operation.perform(wait -> {
				receiving.countDown();
				new CountDownLatch(1).await();
				return List.of();
			});
		});
		try {
			supervisor.start();
			await(receiving);
			assertTrue(listener.get().isDaemon());
			assertTrue(reconciliation.get().isDaemon());
			assertNotSame(listener.get(), reconciliation.get());
			assertNotEquals(listener.get().getName(), reconciliation.get().getName());
			assertTrue(supervisor.snapshot().getLastSuccessfulReconciliation().isPresent());
			assertThrows(IllegalStateException.class, supervisor::start);
		} finally {
			stop(supervisor);
		}
		join(listener.get());
		join(reconciliation.get());
	}

	@Test
	void roleFactoriesAreResolvedBeforeWorkersAreCreatedAndOwnTheirThreadPolicy() throws Exception {
		AtomicInteger resolved = new AtomicInteger();
		ConcurrentHashMap<NotificationSupervisor.ThreadRole, Thread> workers = new ConcurrentHashMap<>();
		CountDownLatch receiving = new CountDownLatch(1);
		NotificationSupervisor supervisor = build(builder().threadFactory(role -> {
			resolved.incrementAndGet();
			return task -> {
				assertEquals(NotificationSupervisor.ThreadRole.values().length, resolved.get());
				Thread worker = new Thread(task, "custom-supervisor-" + role);
				worker.setDaemon(false);
				workers.put(role, worker);
				return worker;
			};
		}).reconcile(() -> assertSame(workers.get(NotificationSupervisor.ThreadRole.RECONCILIATION),
				Thread.currentThread())), operation -> {
			assertSame(workers.get(NotificationSupervisor.ThreadRole.LISTENER), Thread.currentThread());
			operation.perform(wait -> {
				receiving.countDown();
				new CountDownLatch(1).await();
				return List.of();
			});
		});
		try {
			supervisor.start();
			await(receiving);
			assertEquals(NotificationSupervisor.ThreadRole.values().length, resolved.get());
			assertEquals("custom-supervisor-LISTENER", workers.get(NotificationSupervisor.ThreadRole.LISTENER).getName());
			assertFalse(workers.get(NotificationSupervisor.ThreadRole.LISTENER).isDaemon());
			assertFalse(workers.get(NotificationSupervisor.ThreadRole.RECONCILIATION).isDaemon());
		} finally {
			stop(supervisor);
		}
		for (Thread worker : workers.values())
			join(worker);
	}

	@Test
	void nullOrThrowingRoleResolverFailsBeforeAnyWorkerCreationOrSessionWork() throws Exception {
		for (boolean returnNull : new boolean[]{true, false}) {
			AtomicInteger created = new AtomicInteger();
			IllegalStateException failure = new IllegalStateException("Factory resolution failed");
			NotificationSupervisor supervisor = build(builder().threadFactory(role -> {
				if (role == NotificationSupervisor.ThreadRole.RECONCILIATION) {
					if (returnNull)
						return null;
					throw failure;
				}
				return task -> {
					created.incrementAndGet();
					return new Thread(task);
				};
			}), operation -> fail("Factory failure opened a session"));
			RuntimeException thrown = assertThrows(RuntimeException.class, supervisor::start);
			if (!returnNull)
				assertSame(failure, thrown);
			assertSame(thrown, result(supervisor).getFailure().orElseThrow());
			assertSame(thrown, readinessFailure(supervisor));
			assertEquals(0, created.get());
		}
	}

	@Test
	void nullOrFailingThreadFactoryDoesNotStartPreviouslyCreatedWorkers() throws Exception {
		for (boolean returnNull : new boolean[]{true, false}) {
			ConcurrentLinkedQueue<Thread> created = new ConcurrentLinkedQueue<>();
			IllegalStateException failure = new IllegalStateException("Thread creation failed");
			NotificationSupervisor supervisor = build(builder().threadFactory(role -> task -> {
				if (role == NotificationSupervisor.ThreadRole.RECONCILIATION) {
					if (returnNull)
						return null;
					throw failure;
				}
				Thread worker = new Thread(task);
				worker.setDaemon(true);
				created.add(worker);
				return worker;
			}), operation -> fail("Incomplete startup opened a session"));
			RuntimeException thrown = assertThrows(RuntimeException.class, supervisor::start);
			if (!returnNull)
				assertSame(failure, thrown);
			assertSame(thrown, result(supervisor).getFailure().orElseThrow());
			for (Thread worker : created)
				assertEquals(Thread.State.NEW, worker.getState());
		}
	}

	@Test
	void startedOrReusedThreadsAreRejectedWithoutOpeningSessions() throws Exception {
		Thread completed = new Thread(() -> {});
		completed.start();
		join(completed);
		for (Thread invalid : List.of(completed, new Thread(() -> {}))) {
			NotificationSupervisor supervisor = build(builder().threadFactory(role -> task -> invalid),
					operation -> fail("Invalid worker opened a session"));
			RuntimeException failure = assertThrows(RuntimeException.class, supervisor::start);
			assertSame(failure, result(supervisor).getFailure().orElseThrow());
			assertFalse(invalid.isAlive());
		}
	}

	@Test
	void stoppingDuringFactoryResolutionWaitsForStartupToUnwind() throws Exception {
		CountDownLatch resolving = new CountDownLatch(1);
		CountDownLatch release = new CountDownLatch(1);
		AtomicReference<Throwable> startFailure = new AtomicReference<>();
		Threads workers = new Threads();
		NotificationSupervisor supervisor = build(builder().threadFactory(role -> {
			resolving.countDown();
			awaitUninterruptibly(release);
			return workers.factory().apply(role);
		}), operation -> fail("Stopped startup opened a session"));
		Thread starter = new Thread(() -> {
			try {
				supervisor.start();
			} catch (Throwable failure) {
				startFailure.set(failure);
			}
		});
		starter.setDaemon(true);
		starter.start();
		try {
			await(resolving);
			supervisor.requestShutdown();
			assertFalse(supervisor.awaitShutdownAsync().toCompletableFuture().isDone());
			assertInstanceOf(CancellationException.class, readinessFailure(supervisor));
		} finally {
			release.countDown();
			join(starter);
			stop(supervisor);
			workers.joinAll();
		}
		assertNull(startFailure.get());
	}

	@Test
	void failureStartingListenerCleansUpReconciliationWorker() throws Exception {
		IllegalStateException failure = new IllegalStateException("Thread start failed");
		Threads workers = new Threads();
		NotificationSupervisor supervisor = build(builder().pollEvery(POLL_INTERVAL).threadFactory(role -> {
			if (role == NotificationSupervisor.ThreadRole.LISTENER)
				return task -> new Thread(task) {
					@Override public synchronized void start() { throw failure; }
				};
			return workers.factory().apply(role);
		}), operation -> fail("Failed listener start opened a session"));
		assertSame(failure, assertThrows(IllegalStateException.class, supervisor::start));
		assertSame(failure, result(supervisor).getFailure().orElseThrow());
		workers.joinAll();
	}

	@Test
	void pollingContinuesWhileListenerReceiveIsBlocked() throws Exception {
		CountDownLatch receiving = new CountDownLatch(1);
		CountDownLatch polled = new CountDownLatch(1);
		AtomicInteger active = new AtomicInteger();
		AtomicInteger maximumActive = new AtomicInteger();
		Threads workers = new Threads();
		NotificationSupervisor supervisor = build(builder().threadFactory(workers.factory())
				.pollEvery(POLL_INTERVAL).reconcile(() -> {
			int concurrent = active.incrementAndGet();
			maximumActive.accumulateAndGet(concurrent, Math::max);
			try {
				if (receiving.getCount() == 0)
					polled.countDown();
			} finally {
				active.decrementAndGet();
			}
		}), operation -> operation.perform(wait -> {
			receiving.countDown();
			new CountDownLatch(1).await();
			return List.of();
		}));
		try {
			supervisor.start();
			await(receiving);
			await(polled);
			assertEquals(1, maximumActive.get());
			assertEquals(NotificationSupervisor.State.ACTIVE, supervisor.snapshot().getState());
			assertTrue(supervisor.snapshot().getLastSuccessfulReconciliation().isPresent());
		} finally {
			stop(supervisor);
			workers.joinAll();
		}
	}

	@Test
	void pollingContinuesBeforeListenerCanOpenItsSession() throws Exception {
		CountDownLatch opening = new CountDownLatch(1);
		CountDownLatch polledTwice = new CountDownLatch(2);
		NotificationSupervisor supervisor = build(builder().pollEvery(POLL_INTERVAL).reconcile(polledTwice::countDown),
				operation -> {
			opening.countDown();
			new CountDownLatch(1).await();
		});
		try {
			supervisor.start();
			await(opening);
			await(polledTwice);
			assertFalse(supervisor.firstReady().toCompletableFuture().isDone());
			assertEquals(NotificationSupervisor.State.STARTING, supervisor.snapshot().getState());
		} finally {
			stop(supervisor);
		}
	}

	@Test
	void pollingContinuesThroughListenerReconnectBackoff() throws Exception {
		CountDownLatch backingOff = new CountDownLatch(1);
		CountDownLatch polledDuringBackoff = new CountDownLatch(1);
		DatabaseException failure = new DatabaseException("Listener disconnected");
		NotificationSupervisor supervisor = builder().pollEvery(POLL_INTERVAL).reconcile(() -> {
			if (backingOff.getCount() == 0)
				polledDuringBackoff.countDown();
		}).retry(ignored -> true, RetryPolicy.Backoff.fixed(Duration.ofSeconds(1)), Duration.ofMinutes(1))
				.build(operation -> { throw failure; }, System::nanoTime, delay -> {
			backingOff.countDown();
			new CountDownLatch(1).await();
		});
		try {
			supervisor.start();
			await(backingOff);
			await(polledDuringBackoff);
			assertEquals(NotificationSupervisor.State.BACKING_OFF, supervisor.snapshot().getState());
		} finally {
			stop(supervisor);
		}
	}

	@Test
	void readinessRequiresFreshReconciliationAfterRegistration() throws Exception {
		CountDownLatch oldPass = new CountDownLatch(1);
		CountDownLatch releaseOldPass = new CountDownLatch(1);
		CountDownLatch freshPass = new CountDownLatch(1);
		CountDownLatch releaseFreshPass = new CountDownLatch(1);
		AtomicInteger passes = new AtomicInteger();
		NotificationSupervisor supervisor = build(builder().pollEvery(Duration.ofDays(1)).reconcile(() -> {
			if (passes.incrementAndGet() == 1) {
				oldPass.countDown();
				awaitUninterruptibly(releaseOldPass);
			} else {
				freshPass.countDown();
				awaitUninterruptibly(releaseFreshPass);
			}
		}), operation -> {
			await(oldPass);
			operation.perform(wait -> {
				new CountDownLatch(1).await();
				return List.of();
			});
		});
		try {
			supervisor.start();
			await(oldPass);
			awaitCondition(() -> supervisor.snapshot().isReconciliationPending());
			assertFalse(supervisor.firstReady().toCompletableFuture().isDone());
			releaseOldPass.countDown();
			await(freshPass);
			assertFalse(supervisor.firstReady().toCompletableFuture().isDone(),
					"The pre-registration poll satisfied readiness");
			releaseFreshPass.countDown();
			supervisor.firstReady().toCompletableFuture().get(5, TimeUnit.SECONDS);
			assertEquals(2, passes.get());
		} finally {
			releaseOldPass.countDown();
			releaseFreshPass.countDown();
			stop(supervisor);
		}
	}

	@Test
	void recoverableStartupRefreshFailuresAreReportedAndRetriedWithoutReconnecting() throws Exception {
		DatabaseException failure = new DatabaseException("Cache query failed");
		AtomicInteger passes = new AtomicInteger();
		AtomicInteger reports = new AtomicInteger();
		AtomicInteger opens = new AtomicInteger();
		NotificationSupervisor supervisor = build(builder().pollEvery(POLL_INTERVAL).reconcile(() -> {
			if (passes.incrementAndGet() <= 3)
				throw failure;
		}).onReconciliationFailure(observed -> {
			assertSame(failure, observed);
			reports.incrementAndGet();
		}), operation -> {
			opens.incrementAndGet();
			operation.perform(wait -> {
				new CountDownLatch(1).await();
				return List.of();
			});
		});
		try {
			supervisor.start();
			supervisor.firstReady().toCompletableFuture().get(5, TimeUnit.SECONDS);
			assertEquals(1, opens.get());
			assertEquals(3, reports.get());
			assertTrue(passes.get() >= 4);
			assertTrue(supervisor.snapshot().getLastSuccessfulReconciliation().isPresent());
		} finally {
			stop(supervisor);
		}
	}

	@Test
	void pollingFailureAfterReadinessDoesNotReconnectHealthyListenerOrCancelPolling() throws Exception {
		AtomicBoolean failNext = new AtomicBoolean();
		AtomicInteger opens = new AtomicInteger();
		CountDownLatch reported = new CountDownLatch(1);
		CountDownLatch recovered = new CountDownLatch(1);
		DatabaseException failure = new DatabaseException("Temporary cache query failure");
		NotificationSupervisor supervisor = build(builder().pollEvery(POLL_INTERVAL).reconcile(() -> {
			if (failNext.compareAndSet(true, false))
				throw failure;
			if (reported.getCount() == 0)
				recovered.countDown();
		}).onReconciliationFailure(observed -> {
			assertSame(failure, observed);
			reported.countDown();
		}), operation -> {
			opens.incrementAndGet();
			operation.perform(wait -> {
				new CountDownLatch(1).await();
				return List.of();
			});
		});
		try {
			supervisor.start();
			supervisor.firstReady().toCompletableFuture().get(5, TimeUnit.SECONDS);
			failNext.set(true);
			await(reported);
			await(recovered);
			assertEquals(1, opens.get());
			assertEquals(NotificationSupervisor.State.ACTIVE, supervisor.snapshot().getState());
			assertTrue(supervisor.firstReady().toCompletableFuture().isDone());
		} finally {
			stop(supervisor);
		}
	}

	@Test
	void failureObserverFailureTerminatesBothWorkersAndRetainsOriginalFailure() throws Exception {
		DatabaseException reconciliationFailure = new DatabaseException("Cache query failed");
		IllegalStateException observerFailure = new IllegalStateException("Failure observer failed");
		Threads workers = new Threads();
		NotificationSupervisor supervisor = build(builder().threadFactory(workers.factory()).pollEvery(POLL_INTERVAL)
				.reconcile(() -> { throw reconciliationFailure; })
				.onReconciliationFailure(failure -> { throw observerFailure; }), operation -> operation.perform(wait -> {
			new CountDownLatch(1).await();
			return List.of();
		}));
		try {
			supervisor.start();
			assertSame(observerFailure, result(supervisor).getFailure().orElseThrow());
			assertTrue(List.of(observerFailure.getSuppressed()).contains(reconciliationFailure));
		} finally {
			stop(supervisor);
			workers.joinAll();
		}
	}

	@Test
	void terminationWaitsForInFlightReconciliationAndDoesNotInterruptIt() throws Exception {
		CountDownLatch entered = new CountDownLatch(1);
		CountDownLatch release = new CountDownLatch(1);
		AtomicBoolean interrupted = new AtomicBoolean();
		Threads workers = new Threads();
		NotificationSupervisor supervisor = build(builder().threadFactory(workers.factory()).reconcile(() -> {
			entered.countDown();
			awaitUninterruptibly(release);
			interrupted.set(Thread.currentThread().isInterrupted());
		}), operation -> operation.perform(wait -> List.of()));
		try {
			supervisor.start();
			await(entered);
			assertTrue(supervisor.snapshot().isReconciliationRunning());
			supervisor.requestShutdown();
			assertFalse(supervisor.awaitShutdownAsync().toCompletableFuture().isDone());
		} finally {
			release.countDown();
			stop(supervisor);
			workers.joinAll();
		}
		assertFalse(interrupted.get());
	}

	@Test
	void lateDecodedBatchReceivesFinalReconciliationBeforeManagedShutdown() throws Exception {
		CountDownLatch receiving = new CountDownLatch(1);
		CountDownLatch deliverBatch = new CountDownLatch(1);
		CountDownLatch finalPass = new CountDownLatch(1);
		AtomicBoolean stopRequested = new AtomicBoolean();
		AtomicInteger passes = new AtomicInteger();
		NotificationSupervisor supervisor = build(builder().pollEvery(Duration.ofDays(1)).reconcile(() -> {
			passes.incrementAndGet();
			if (stopRequested.get())
				finalPass.countDown();
		}), operation -> operation.perform(wait -> {
			receiving.countDown();
			awaitUninterruptibly(deliverBatch);
			return List.of(Notification.of("job_ready", ""));
		}));
		int beforeStop;
		try {
			supervisor.start();
			await(receiving);
			beforeStop = passes.get();
			stopRequested.set(true);
			supervisor.requestShutdown();
			assertFalse(supervisor.awaitShutdownAsync().toCompletableFuture().isDone());
			deliverBatch.countDown();
			await(finalPass);
			result(supervisor);
			assertEquals(beforeStop + 1, passes.get());
		} finally {
			deliverBatch.countDown();
			stop(supervisor);
		}
	}

	@Test
	void terminationWaitsForListenerCleanupAfterPollingStops() throws Exception {
		CountDownLatch receiving = new CountDownLatch(1);
		CountDownLatch cleanup = new CountDownLatch(1);
		CountDownLatch releaseCleanup = new CountDownLatch(1);
		NotificationSupervisor supervisor = build(builder().pollEvery(POLL_INTERVAL), operation -> {
			try {
				operation.perform(wait -> {
					receiving.countDown();
					new CountDownLatch(1).await();
					return List.of();
				});
			} finally {
				cleanup.countDown();
				awaitUninterruptibly(releaseCleanup);
			}
		});
		try {
			supervisor.start();
			await(receiving);
			supervisor.requestShutdown();
			await(cleanup);
			assertEquals(NotificationSupervisor.State.STOPPING, supervisor.snapshot().getState());
			assertFalse(supervisor.awaitShutdownAsync().toCompletableFuture().isDone());
		} finally {
			releaseCleanup.countDown();
			stop(supervisor);
		}
	}

	@Test
	void blockingAwaitHonorsInterruptionWithoutRequestingShutdown() throws Exception {
		CountDownLatch receiving = new CountDownLatch(1);
		CountDownLatch awaiting = new CountDownLatch(1);
		CountDownLatch interrupted = new CountDownLatch(1);
		AtomicReference<Throwable> unexpected = new AtomicReference<>();
		NotificationSupervisor supervisor = build(builder(), operation -> operation.perform(wait -> {
			receiving.countDown();
			new CountDownLatch(1).await();
			return List.of();
		}));
		Thread waiter = new Thread(() -> {
			awaiting.countDown();
			try {
				supervisor.awaitShutdown();
				unexpected.set(new AssertionError("Await returned before shutdown"));
			} catch (InterruptedException expected) {
				interrupted.countDown();
			} catch (Throwable failure) {
				unexpected.set(failure);
			}
		});
		waiter.setDaemon(true);
		try {
			supervisor.start();
			await(receiving);
			waiter.start();
			await(awaiting);
			waiter.interrupt();
			await(interrupted);
			assertNull(unexpected.get());
			assertFalse(supervisor.awaitShutdownAsync().toCompletableFuture().isDone());
			assertEquals(NotificationSupervisor.State.ACTIVE, supervisor.snapshot().getState());
		} finally {
			stop(supervisor);
			join(waiter);
		}
	}

	@Test
	void blockingAwaitWaitsUntilListenerCleanupFinishes() throws Exception {
		CountDownLatch receiving = new CountDownLatch(1);
		CountDownLatch cleanup = new CountDownLatch(1);
		CountDownLatch releaseCleanup = new CountDownLatch(1);
		CountDownLatch waiting = new CountDownLatch(1);
		CountDownLatch returned = new CountDownLatch(1);
		AtomicReference<Throwable> failure = new AtomicReference<>();
		NotificationSupervisor supervisor = build(builder(), operation -> {
			try {
				operation.perform(wait -> {
					receiving.countDown();
					new CountDownLatch(1).await();
					return List.of();
				});
			} finally {
				cleanup.countDown();
				awaitUninterruptibly(releaseCleanup);
			}
		});
		Thread waiter = new Thread(() -> {
			waiting.countDown();
			try {
				supervisor.awaitShutdown();
				returned.countDown();
			} catch (Throwable unexpected) {
				failure.set(unexpected);
			}
		});
		waiter.setDaemon(true);
		try {
			supervisor.start();
			await(receiving);
			waiter.start();
			await(waiting);
			supervisor.requestShutdown();
			await(cleanup);
			assertEquals(1, returned.getCount());
		} finally {
			releaseCleanup.countDown();
			stop(supervisor);
			join(waiter);
		}
		assertEquals(0, returned.getCount());
		assertNull(failure.get());
	}

	@Test
	void shutdownCanBeRequestedFromReconciliationWithoutDeadlock() throws Exception {
		AtomicReference<NotificationSupervisor> reference = new AtomicReference<>();
		Threads workers = new Threads();
		NotificationSupervisor supervisor = build(builder().threadFactory(workers.factory()).reconcile(() ->
				reference.get().requestShutdown()), operation -> operation.perform(wait -> List.of()));
		reference.set(supervisor);
		try {
			supervisor.start();
			assertTrue(result(supervisor).isShutdownRequested());
			assertInstanceOf(CancellationException.class, readinessFailure(supervisor));
		} finally {
			stop(supervisor);
			workers.joinAll();
		}
	}

	@Test
	void unexpectedPollingFailuresTerminateTheManagedLifecycle() throws Exception {
		for (Throwable fatal : List.of(new IllegalStateException("Broken cache refresh"),
				new AssertionError("Fatal cache refresh"))) {
			CountDownLatch receiving = new CountDownLatch(1);
			Threads workers = new Threads();
			NotificationSupervisor supervisor = build(builder().threadFactory(workers.factory()).pollEvery(POLL_INTERVAL)
					.reconcile(() -> {
				if (receiving.getCount() == 0) {
					if (fatal instanceof Error error)
						throw error;
					throw (RuntimeException) fatal;
				}
			}), operation -> operation.perform(wait -> {
				receiving.countDown();
				new CountDownLatch(1).await();
				return List.of();
			}));
			try {
				supervisor.start();
				await(receiving);
				assertSame(fatal, result(supervisor).getFailure().orElseThrow());
			} finally {
				stop(supervisor);
				workers.joinAll();
			}
		}
	}

	@Test
	void blockingAwaitFailsFastOnEitherManagedWorker() throws Exception {
		AtomicReference<NotificationSupervisor> reference = new AtomicReference<>();
		AtomicInteger guarded = new AtomicInteger();
		NotificationSupervisor supervisor = build(builder().reconcile(() -> {
			assertThrows(IllegalStateException.class, () -> reference.get().awaitShutdown());
			guarded.incrementAndGet();
		}), operation -> {
			assertThrows(IllegalStateException.class, () -> reference.get().awaitShutdown());
			guarded.incrementAndGet();
			operation.perform(wait -> {
				new CountDownLatch(1).await();
				return List.of();
			});
		});
		reference.set(supervisor);
		try {
			supervisor.start();
			supervisor.firstReady().toCompletableFuture().get(5, TimeUnit.SECONDS);
			assertEquals(2, guarded.get());
		} finally {
			stop(supervisor);
		}
	}

	@Test
	void managedOnlySettingsRejectLegacyStartWithoutConsumingTheSupervisor() throws Exception {
		for (int setting : new int[]{0, 1, 2}) {
			NotificationSupervisor.Builder builder = builder();
			if (setting == 0)
				builder.pollEvery(POLL_INTERVAL);
			else if (setting == 1)
				builder.threadFactory(role -> task -> {
					Thread worker = new Thread(task);
					worker.setDaemon(true);
					return worker;
				});
			else
				builder.onReconciliationFailure(failure -> {});
			NotificationSupervisor supervisor = build(builder, operation -> { throw new InterruptedException(); });
			assertThrows(IllegalStateException.class, () -> supervisor.start(Runnable::run));
			assertEquals(NotificationSupervisor.State.NEW, supervisor.snapshot().getState());
			try {
				supervisor.start();
				assertTrue(result(supervisor).getFailure().isEmpty());
			} finally {
				stop(supervisor);
			}
		}
	}

	@Test
	void managedReconciliationWithoutPollingStillHonorsExplicitListenerRetryPolicy() throws Exception {
		AtomicInteger passes = new AtomicInteger();
		AtomicInteger opens = new AtomicInteger();
		AtomicInteger retries = new AtomicInteger();
		DatabaseException failure = new DatabaseException("Initial refresh failed");
		NotificationSupervisor supervisor = build(builder().reconcile(() -> {
			if (passes.incrementAndGet() == 1)
				throw failure;
		}).retry(observed -> {
			assertSame(failure, observed.getException());
			assertEquals(NotificationSupervisor.FailurePhase.RECONCILE, observed.getPhase());
			retries.incrementAndGet();
			return true;
		}, RetryPolicy.Backoff.fixed(Duration.ZERO), Duration.ofMinutes(1)), operation -> {
			opens.incrementAndGet();
			operation.perform(wait -> {
				new CountDownLatch(1).await();
				return List.of();
			});
		});
		try {
			supervisor.start();
			supervisor.firstReady().toCompletableFuture().get(5, TimeUnit.SECONDS);
			assertEquals(2, opens.get());
			assertEquals(2, passes.get());
			assertEquals(1, retries.get());
		} finally {
			stop(supervisor);
		}
	}

	@Test
	void factoryResolverCannotBlockAwaitingItsOwnStartup() throws Exception {
		AtomicReference<NotificationSupervisor> reference = new AtomicReference<>();
		AtomicInteger guarded = new AtomicInteger();
		NotificationSupervisor supervisor = build(builder().threadFactory(role -> {
			assertThrows(IllegalStateException.class, () -> reference.get().awaitShutdown());
			guarded.incrementAndGet();
			return task -> {
				assertThrows(IllegalStateException.class, () -> reference.get().awaitShutdown());
				guarded.incrementAndGet();
				Thread worker = new Thread(task);
				worker.setDaemon(true);
				return worker;
			};
		}), operation -> operation.perform(wait -> {
			new CountDownLatch(1).await();
			return List.of();
		}));
		reference.set(supervisor);
		try {
			supervisor.start();
			supervisor.firstReady().toCompletableFuture().get(5, TimeUnit.SECONDS);
			assertEquals(4, guarded.get());
		} finally {
			stop(supervisor);
		}
	}

	@Test
	void fatalPollingFailureBeforeListenerEntryPreventsOpeningASession() throws Exception {
		IllegalStateException failure = new IllegalStateException("Fatal initial poll");
		CountDownLatch releaseListener = new CountDownLatch(1);
		AtomicInteger opens = new AtomicInteger();
		Threads workers = new Threads();
		NotificationSupervisor supervisor = build(builder().pollEvery(POLL_INTERVAL).reconcile(() -> { throw failure; })
				.threadFactory(role -> {
			if (role == NotificationSupervisor.ThreadRole.LISTENER)
				return task -> {
					Thread worker = new Thread(() -> {
						awaitUninterruptibly(releaseListener);
						task.run();
					});
					worker.setDaemon(true);
					workers.workers.add(worker);
					return worker;
				};
			return workers.factory().apply(role);
		}), operation -> {
			opens.incrementAndGet();
			operation.perform(wait -> List.of());
		});
		try {
			supervisor.start();
			awaitCondition(() -> supervisor.snapshot().getLastFailure().orElse(null) == failure);
			releaseListener.countDown();
			assertSame(failure, result(supervisor).getFailure().orElseThrow());
			assertEquals(0, opens.get());
		} finally {
			releaseListener.countDown();
			stop(supervisor);
			workers.joinAll();
		}
	}

	@Test
	void prematurelyStartedFactoryWorkerIsDrainedBeforeShutdownCompletes() throws Exception {
		CountDownLatch entered = new CountDownLatch(1);
		CountDownLatch release = new CountDownLatch(1);
		AtomicReference<Thread> startedWorker = new AtomicReference<>();
		NotificationSupervisor supervisor = build(builder().threadFactory(role -> task -> {
			Thread worker = new Thread(() -> {
				entered.countDown();
				awaitUninterruptibly(release);
				task.run();
			});
			worker.setDaemon(true);
			if (role == NotificationSupervisor.ThreadRole.LISTENER) {
				startedWorker.set(worker);
				worker.start();
				awaitUninterruptibly(entered);
			}
			return worker;
		}), operation -> fail("Prematurely started factory worker opened a session"));
		try {
			RuntimeException failure = assertThrows(RuntimeException.class, supervisor::start);
			assertFalse(supervisor.awaitShutdownAsync().toCompletableFuture().isDone(),
					"Startup failure completed shutdown while a supplied task was still running");
			release.countDown();
			assertSame(failure, result(supervisor).getFailure().orElseThrow());
		} finally {
			release.countDown();
			stop(supervisor);
			join(startedWorker.get());
		}
	}

	@Test
	void customStartThatDoesNotStartAWorkerFailsAndCleansUpOtherWorkers() throws Exception {
		for (NotificationSupervisor.ThreadRole brokenRole : NotificationSupervisor.ThreadRole.values()) {
			AtomicInteger reconciliations = new AtomicInteger();
			Threads workers = new Threads();
			NotificationSupervisor supervisor = build(builder().pollEvery(POLL_INTERVAL)
					.reconcile(reconciliations::incrementAndGet).threadFactory(role -> task -> {
				Thread worker = role == brokenRole ? new Thread(task) {
					@Override public synchronized void start() {}
				} : new Thread(task);
				worker.setDaemon(true);
				workers.workers.add(worker);
				return worker;
			}), operation -> fail("No-op worker start allowed session work"));
			try {
				IllegalStateException failure = assertThrows(IllegalStateException.class, supervisor::start);
				assertSame(failure, result(supervisor).getFailure().orElseThrow());
				assertEquals(0, reconciliations.get());
			} finally {
				stop(supervisor);
				workers.joinAll();
			}
		}
	}

	@Test
	void unreadableListenerCleanupCannotReplaceTheOriginalFatalPollingFailure() throws Exception {
		CountDownLatch receiving = new CountDownLatch(1);
		AssertionError fatal = new AssertionError("Fatal periodic reconciliation");
		AssertionError diagnosticFailure = new AssertionError("Unreadable cleanup cause");
		DatabaseException cleanupFailure = new DatabaseException("Listener cleanup failed") {
			@Override public synchronized Throwable getCause() { throw diagnosticFailure; }
		};
		Threads workers = new Threads();
		NotificationSupervisor supervisor = build(builder().threadFactory(workers.factory()).pollEvery(POLL_INTERVAL)
				.reconcile(() -> {
			if (receiving.getCount() == 0)
				throw fatal;
		}), operation -> {
			try {
				operation.perform(wait -> {
					receiving.countDown();
					new CountDownLatch(1).await();
					return List.of();
				});
			} finally {
				throw cleanupFailure;
			}
		});
		try {
			supervisor.start();
			await(receiving);
			assertSame(fatal, result(supervisor).getFailure().orElseThrow());
			assertSame(fatal, supervisor.snapshot().getLastFailure().orElseThrow());
			assertTrue(List.of(fatal.getSuppressed()).contains(cleanupFailure),
					"The secondary listener cleanup failure was lost");
		} finally {
			stop(supervisor);
			workers.joinAll();
		}
	}

	private static NotificationSupervisor.Builder builder() {
		return NotificationSupervisor.withDatabase(DATABASE).channels(Set.of("job_ready")).reconcile(() -> {});
	}

	private static NotificationSupervisor build(NotificationSupervisor.Builder builder,
			NotificationSupervisor.SessionAttempt attempt) {
		return builder.build(attempt, System::nanoTime, delay -> {});
	}

	private static NotificationSupervisor.ShutdownResult result(NotificationSupervisor supervisor) throws Exception {
		return supervisor.awaitShutdownAsync().toCompletableFuture().get(5, TimeUnit.SECONDS);
	}

	private static Throwable readinessFailure(NotificationSupervisor supervisor) {
		return assertThrows(ExecutionException.class,
				() -> supervisor.firstReady().toCompletableFuture().get(5, TimeUnit.SECONDS)).getCause();
	}

	private static void stop(NotificationSupervisor supervisor) throws Exception {
		supervisor.requestShutdown();
		result(supervisor);
	}

	private static void await(CountDownLatch latch) throws InterruptedException {
		assertTrue(latch.await(5, TimeUnit.SECONDS), "Timed out waiting for test phase");
	}

	private static void awaitUninterruptibly(CountDownLatch latch) {
		boolean interrupted = false;
		try {
			while (true) {
				try {
					await(latch);
					return;
				} catch (InterruptedException signal) {
					interrupted = true;
				}
			}
		} finally {
			if (interrupted)
				Thread.currentThread().interrupt();
		}
	}

	private static void awaitCondition(BooleanSupplier condition) {
		long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
		while (!condition.getAsBoolean()) {
			assertTrue(System.nanoTime() - deadline < 0, "Timed out waiting for managed state");
			LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
		}
	}

	private static void join(Thread worker) throws InterruptedException {
		worker.join(5_000);
		assertFalse(worker.isAlive(), "Managed worker did not exit: " + worker.getName());
	}

	private static final class Threads {
		private final ConcurrentLinkedQueue<Thread> workers = new ConcurrentLinkedQueue<>();

		private Function<NotificationSupervisor.ThreadRole, ThreadFactory> factory() {
			return role -> task -> {
				Thread worker = new Thread(task, "managed-supervisor-test-" + role);
				worker.setDaemon(true);
				worker.setUncaughtExceptionHandler((thread, failure) -> {});
				this.workers.add(worker);
				return worker;
			};
		}

		private void joinAll() throws InterruptedException {
			for (Thread worker : this.workers)
				join(worker);
		}
	}
}
