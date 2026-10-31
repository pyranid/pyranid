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
import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;
import java.util.function.ToIntFunction;

/** Iterative failure inspection that does not trust custom accessors. */
final class ThrowableGraph {
	private ThrowableGraph() {}

	/** Combines evidence bits; unreadable nodes or edges contribute the caller's conservative fallback. */
	static int inspect(Throwable failure, ToIntFunction<Throwable> evidence, int unreadableEvidence) {
		return inspect(failure, evidence, unreadableEvidence, current -> true);
	}

	/** Inspects each node but follows its outgoing edges only when {@code descend} permits it. */
	static int inspect(Throwable failure, ToIntFunction<Throwable> evidence, int unreadableEvidence,
			Predicate<Throwable> descend) {
		return inspect(failure, evidence, unreadableEvidence, descend, false);
	}

	/** Operation guards retain accessor failures so later readable scans cannot lose newly observed evidence. */
	static int inspectRetainingFailures(Throwable failure, ToIntFunction<Throwable> evidence, int unreadableEvidence) {
		return inspect(failure, evidence, unreadableEvidence, current -> true, true);
	}

	private static int inspect(Throwable failure, ToIntFunction<Throwable> evidence, int unreadableEvidence,
			Predicate<Throwable> descend, boolean retainFailures) {
		if (failure == null)
			return 0;
		int result = 0;
		List<Throwable> inspectionFailures = null;
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
				if (retainFailures) inspectionFailures = collect(inspectionFailures, inspectionFailure);
			}
			try {
				if (!descend.test(current))
					continue;
			} catch (Throwable inspectionFailure) {
				result |= unreadableEvidence;
				if (retainFailures) inspectionFailures = collect(inspectionFailures, inspectionFailure);
				continue;
			}
			try {
				Throwable cause = current.getCause();
				if (cause != null)
					pending.add(cause);
			} catch (Throwable inspectionFailure) {
				result |= unreadableEvidence;
				if (retainFailures) inspectionFailures = collect(inspectionFailures, inspectionFailure);
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
					if (retainFailures) inspectionFailures = collect(inspectionFailures, inspectionFailure);
				}
			}
		}
		// Append only after traversal. A diagnostic may itself have failing accessors; the current scan must
		// not start following newly attached failures or repeatedly revisit a driver's changing diagnostics.
		if (inspectionFailures != null) {
			Set<Throwable> retained = Collections.newSetFromMap(new IdentityHashMap<>());
			retained.add(failure);
			Collections.addAll(retained, failure.getSuppressed());
			for (Throwable inspectionFailure : inspectionFailures)
				if (retained.add(inspectionFailure)) failure.addSuppressed(inspectionFailure);
		}
		return result;
	}

	private static List<Throwable> collect(List<Throwable> failures, Throwable failure) {
		if (failures == null) failures = new ArrayList<>();
		failures.add(failure);
		return failures;
	}

	/** Keep diagnostic identity without self-suppression or duplicate retention across repeated scans. */
	static void retainInspectionFailure(Throwable primary, Throwable diagnostic) {
		if (primary == diagnostic) return;
		for (Throwable existing : primary.getSuppressed())
			if (existing == diagnostic) return;
		primary.addSuppressed(diagnostic);
	}
}
