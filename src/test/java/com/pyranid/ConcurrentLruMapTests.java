/*
 * Copyright 2022-2026 Revetware LLC.
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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author <a href="https://www.revetkn.com">Mark Allen</a>
 * @since 4.1.0
 */
public class ConcurrentLruMapTests {
	@Test
	public void testPutOverCapacityEvictsBeforeReturning() {
		ConcurrentLruMap<Integer, Integer> map = new ConcurrentLruMap<>(2);

		map.put(1, 1);
		map.put(2, 2);
		map.drain();

		map.put(3, 3);

		Assertions.assertTrue(map.size() <= map.capacity(), "Map should be bounded after over-capacity write returns");
		Assertions.assertEquals(List.of(3, 2), map.keysInAccessOrder());
	}

	@Test
	public void testBurstyUniqueWritesRemainBoundedAndConverge() throws Exception {
		int capacity = 1_024;
		int threadCount = 8;
		int writesPerThread = 10_000;
		int maxExpectedPeakSize = capacity * 4;
		ConcurrentLruMap<Integer, Integer> map = new ConcurrentLruMap<>(capacity);
		ExecutorService executorService = Executors.newFixedThreadPool(threadCount + 1);
		CountDownLatch start = new CountDownLatch(1);
		AtomicInteger maxObservedSize = new AtomicInteger(0);
		AtomicBoolean running = new AtomicBoolean(true);
		List<Future<?>> futures = new ArrayList<>();

		Future<?> sampler = executorService.submit(() -> {
			try {
				while (running.get()) {
					recordMax(maxObservedSize, map.size());
					TimeUnit.MILLISECONDS.sleep(10);
				}

				recordMax(maxObservedSize, map.size());
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				throw new RuntimeException(e);
			}
		});

		for (int threadIndex = 0; threadIndex < threadCount; ++threadIndex) {
			int offset = threadIndex * writesPerThread;
			futures.add(executorService.submit(() -> {
				try {
					start.await();

					for (int i = 0; i < writesPerThread; ++i) {
						int key = offset + i;
						map.put(key, key);
						recordMax(maxObservedSize, map.size());
					}
				} catch (InterruptedException e) {
					Thread.currentThread().interrupt();
					throw new RuntimeException(e);
				}
			}));
		}

		try {
			start.countDown();

			for (Future<?> future : futures)
				Assertions.assertTimeoutPreemptively(Duration.ofSeconds(30), () -> future.get());
		} finally {
			running.set(false);
			sampler.get(5, TimeUnit.SECONDS);
			executorService.shutdownNow();
			Assertions.assertTrue(executorService.awaitTermination(5, TimeUnit.SECONDS));
		}

		Assertions.assertTrue(maxObservedSize.get() <= maxExpectedPeakSize,
				() -> "Expected peak size <= " + maxExpectedPeakSize + " but observed " + maxObservedSize.get());

		map.drain();
		Assertions.assertTrue(map.size() <= capacity, "Map should converge to configured capacity after draining maintenance");
	}

	private static void recordMax(AtomicInteger maxObservedSize,
																int size) {
		maxObservedSize.accumulateAndGet(size, Math::max);
	}
}
