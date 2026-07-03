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

import jdk.jfr.Recording;
import jdk.jfr.consumer.RecordedEvent;
import jdk.jfr.consumer.RecordedFrame;
import jdk.jfr.consumer.RecordingFile;
import org.hsqldb.jdbc.JDBCDataSource;
import org.jspecify.annotations.NonNull;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static java.lang.String.format;

/**
 * Defends Pyranid's zero-carrier-thread-pinning property under virtual threads.
 * <p>
 * The main source contains no {@code synchronized} (see {@link SourcePolicyTests}); this test verifies the
 * runtime consequence: a concurrent virtual-thread workload through {@link Database} — queries, transactions,
 * batches, streams — produces no {@code jdk.VirtualThreadPinned} JFR events attributable to Pyranid code.
 * <p>
 * Compiled against the Java 17 baseline, so the virtual-thread executor is obtained reflectively; the test is
 * assumption-skipped on JDKs without virtual threads.
 *
 * @author <a href="https://www.revetkn.com">Mark Allen</a>
 */
public class VirtualThreadPinningTests {
	@Test
	public void testVirtualThreadWorkloadDoesNotPinCarrierThreads() throws Exception {
		ExecutorService executorService = virtualThreadExecutorIfAvailable();
		Assumptions.assumeTrue(executorService != null, "Virtual threads require JDK 21+");

		Path recordingFile = Files.createTempFile("pyranid-vt-pinning", ".jfr");

		try (Recording recording = new Recording()) {
			// Warm up before recording: concurrent first-time class loading contends on JVM classloader
			// monitors and emits VirtualThreadPinned events that have nothing to do with Pyranid's design
			runWorkload(executorService, "vt_pinning_warmup");

			// Explicit zero threshold: the default JFR configuration thresholds this event at 20ms, and the
			// programmatic-enable default varies by JDK — "zero pinning events" must mean zero, however brief
			recording.enable("jdk.VirtualThreadPinned").withThreshold(Duration.ZERO).withStackTrace();
			recording.start();

			runWorkload(executorService, "vt_pinning");

			recording.stop();
			recording.dump(recordingFile);
		} finally {
			executorService.shutdown();
			Assertions.assertTrue(executorService.awaitTermination(30, TimeUnit.SECONDS));
		}

		List<String> pyranidPinningEvents = new ArrayList<>();

		try (RecordingFile events = new RecordingFile(recordingFile)) {
			while (events.hasMoreEvents()) {
				RecordedEvent event = events.readEvent();

				if (!"jdk.VirtualThreadPinned".equals(event.getEventType().getName()))
					continue;

				if (isAttributableToPyranid(event))
					pyranidPinningEvents.add(String.valueOf(event));
			}
		} finally {
			Files.deleteIfExists(recordingFile);
		}

		Assertions.assertTrue(pyranidPinningEvents.isEmpty(), () -> format(
				"Virtual threads were pinned to carrier threads with Pyranid frames on the stack — "
						+ "a synchronized block (or Object.wait) was likely introduced in a blocking path. Events: %s",
				pyranidPinningEvents));
	}

	/**
	 * A pinning event is Pyranid's fault only when a Pyranid frame is on the stack AND no frame ABOVE it
	 * (nearer the park site) belongs to a subsystem that owns monitors of its own — JVM class loading /
	 * class initialization, or the in-process HSQLDB engine (whose internals are legitimately synchronized).
	 * In those cases Pyranid is merely the caller that triggered someone else's monitor.
	 * <p>
	 * This attribution is a heuristic: the {@code jdk.VirtualThreadPinned} event does not identify the
	 * monitor owner. The airtight guard against reintroduced {@code synchronized} in Pyranid is
	 * {@link SourcePolicyTests#testMainSourceContainsNoSynchronized()}; this test is the runtime backstop.
	 */
	private static boolean isAttributableToPyranid(@NonNull RecordedEvent event) {
		if (event.getStackTrace() == null)
			return false;

		List<RecordedFrame> frames = event.getStackTrace().getFrames();

		for (int i = 0; i < frames.size(); ++i) {
			String type = frames.get(i).getMethod().getType().getName();

			if (type.startsWith("com.pyranid.") && !type.startsWith("com.pyranid.VirtualThreadPinningTests")) {
				// Found the topmost Pyranid frame; check whether an external monitor owner sits above it
				for (int j = 0; j < i; ++j) {
					String above = frames.get(j).getMethod().getType().getName();
					if (above.startsWith("jdk.internal.loader.") || above.startsWith("java.lang.ClassLoader")
							|| above.startsWith("java.lang.Class") || above.startsWith("org.hsqldb."))
						return false;
				}

				return true;
			}
		}

		return false;
	}

	private void runWorkload(@NonNull ExecutorService executorService,
													 @NonNull String databaseName) throws Exception {
		JDBCDataSource dataSource = new JDBCDataSource();
		dataSource.setUrl(format("jdbc:hsqldb:mem:%s", databaseName));
		dataSource.setUser("sa");
		dataSource.setPassword("");

		Database database = Database.withDataSource(dataSource).build();
		database.query("CREATE TABLE vt_rows (id INTEGER, label VARCHAR(32))").execute();

		int virtualThreads = 16;
		int operationsPerThread = 25;
		CountDownLatch startGate = new CountDownLatch(1);
		List<Future<?>> futures = new ArrayList<>(virtualThreads);

		for (int threadIndex = 0; threadIndex < virtualThreads; ++threadIndex) {
			int base = threadIndex * operationsPerThread;
			futures.add(executorService.submit(() -> {
				try {
					startGate.await();

					for (int i = 0; i < operationsPerThread; ++i) {
						int id = base + i;

						// Exercise the shared hot paths: parsed-SQL cache, row-plan cache, binder, mapper,
						// transactions (ThreadLocal carrier + ReentrantLock), batch, stream
						database.transaction(() -> {
							database.query("INSERT INTO vt_rows (id, label) VALUES (:id, :label)")
									.bind("id", id)
									.bind("label", "label-" + id)
									.execute();
						});

						database.query("SELECT label FROM vt_rows WHERE id = :id")
								.bind("id", id)
								.fetchObject(String.class);

						database.query("SELECT id, label FROM vt_rows WHERE id IN (:ids)")
								.bind("ids", Parameters.inList(new int[]{id, id + 1}))
								.fetchList(Query.mapRowType());

						if (i % 5 == 0)
							database.query("SELECT id FROM vt_rows WHERE id <= :max ORDER BY id")
									.bind("max", id)
									.fetchStream(Integer.class, stream -> stream.limit(10).count());
					}

					return null;
				} catch (InterruptedException e) {
					Thread.currentThread().interrupt();
					throw new RuntimeException(e);
				}
			}));
		}

		startGate.countDown();

		for (Future<?> future : futures)
			future.get(60, TimeUnit.SECONDS);
	}

	/**
	 * Obtains {@code Executors.newVirtualThreadPerTaskExecutor()} reflectively (the source baseline is Java 17;
	 * virtual threads arrived in 21), or {@code null} when unavailable.
	 */
	private static ExecutorService virtualThreadExecutorIfAvailable() {
		try {
			Method factory = java.util.concurrent.Executors.class.getMethod("newVirtualThreadPerTaskExecutor");
			return (ExecutorService) factory.invoke(null);
		} catch (ReflectiveOperationException e) {
			return null;
		}
	}
}
