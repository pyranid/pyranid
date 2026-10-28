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

import java.sql.SQLException;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Set;
import java.util.function.Predicate;
import java.util.function.ToIntFunction;

/** Inspects failure evidence without mutating failures, recursing, or trusting custom accessors. */
final class ThrowableGraph {
	private ThrowableGraph() {}

	/** Combines evidence bits; unreadable nodes or edges contribute the caller's conservative fallback. */
	static int inspect(Throwable failure, ToIntFunction<Throwable> evidence, int unreadableEvidence) {
		return inspect(failure, evidence, unreadableEvidence, current -> true);
	}

	/** Inspects each node but follows its outgoing edges only when {@code descend} permits it. */
	static int inspect(Throwable failure, ToIntFunction<Throwable> evidence, int unreadableEvidence,
			Predicate<Throwable> descend) {
		if (failure == null)
			return 0;
		int result = 0;
		Set<Throwable> visited = Collections.newSetFromMap(new IdentityHashMap<>());
		ArrayDeque<Throwable> pending = new ArrayDeque<>();
		pending.add(failure);
		while (!pending.isEmpty()) {
			Throwable current = pending.removeLast();
			if (!visited.add(current))
				continue;
			try {
				result |= evidence.applyAsInt(current);
			} catch (Throwable inspectionFailure) {
				result |= unreadableEvidence;
			}
			try {
				if (!descend.test(current))
					continue;
			} catch (Throwable inspectionFailure) {
				result |= unreadableEvidence;
				continue;
			}
			try {
				Throwable cause = current.getCause();
				if (cause != null)
					pending.add(cause);
			} catch (Throwable inspectionFailure) {
				result |= unreadableEvidence;
			}
			// Inspect each edge independently: an unreadable cause must not hide suppressed interrupt evidence.
			Collections.addAll(pending, current.getSuppressed());
			if (current instanceof SQLException sqlException) {
				try {
					SQLException next = sqlException.getNextException();
					if (next != null)
						pending.add(next);
				} catch (Throwable inspectionFailure) {
					result |= unreadableEvidence;
				}
			}
		}
		return result;
	}
}
