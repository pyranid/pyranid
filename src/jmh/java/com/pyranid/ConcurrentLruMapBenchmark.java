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

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(1)
public class ConcurrentLruMapBenchmark {
	@State(Scope.Benchmark)
	public static class MapState {
		@Param({"256", "4096"})
		public int capacity;

		ConcurrentLruMap<Integer, Integer> map;
		ConcurrentHashMap<Integer, Integer> concurrentHashMap;

		@Setup(Level.Trial)
		public void setup() {
			map = new ConcurrentLruMap<>(capacity);
			concurrentHashMap = new ConcurrentHashMap<>(capacity);

			for (int i = 0; i < capacity; ++i) {
				map.put(i, i);
				concurrentHashMap.put(i, i);
			}
		}
	}

	@State(Scope.Thread)
	public static class Cursor {
		private int value;

		int next(int capacity) {
			return ++value & (capacity - 1);
		}
	}

	@Benchmark
	public Integer concurrentHashMapGetHit(MapState mapState,
																				 Cursor cursor) {
		return mapState.concurrentHashMap.get(cursor.next(mapState.capacity));
	}

	@Benchmark
	public Integer getHit(MapState mapState,
												Cursor cursor) {
		return mapState.map.get(cursor.next(mapState.capacity));
	}

	@Benchmark
	public Integer getMixedHitMiss(MapState mapState,
																 Cursor cursor) {
		return mapState.map.get(cursor.next(mapState.capacity * 2));
	}

	@Benchmark
	public Integer computeIfAbsentHit(MapState mapState,
																		Cursor cursor) {
		return mapState.map.computeIfAbsent(cursor.next(mapState.capacity), Integer::valueOf);
	}

	@Benchmark
	public Integer computeIfAbsentMixedHitMiss(MapState mapState,
																						 Cursor cursor) {
		return mapState.map.computeIfAbsent(cursor.next(mapState.capacity * 2), Integer::valueOf);
	}

	@Benchmark
	public Integer putEvicting(MapState mapState,
														 Cursor cursor) {
		Integer key = mapState.capacity + cursor.next(mapState.capacity * 4);
		mapState.map.put(key, key);
		return key;
	}
}
