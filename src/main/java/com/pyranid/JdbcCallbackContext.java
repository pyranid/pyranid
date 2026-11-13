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

import org.jspecify.annotations.Nullable;

import java.util.function.Supplier;

/** Carries a statement's captured connection owner through nested JDBC callback dispatch. */
final class JdbcCallbackContext {
	private static final ThreadLocal<StatementExecution> CURRENT = new ThreadLocal<>();

	private JdbcCallbackContext() {}

	static Scope activate(StatementExecution execution) {
		StatementExecution previous = CURRENT.get();
		// A standalone operation must also shadow an enclosing transaction's callback context.
		CURRENT.set(execution);
		return new Scope(previous);
	}

	static void requireValidation() {
		StatementExecution execution = CURRENT.get();
		if (execution != null) execution.requireCompletionValidation();
	}

	@Nullable
	static DatabaseException prepareCapabilityFailure(Supplier<DatabaseException> failure) {
		StatementExecution execution = CURRENT.get();
		return execution == null ? null : execution.prepareCapabilityFailure(failure);
	}

	static void recordFailure(Throwable failure) {
		StatementExecution execution = CURRENT.get();
		if (execution != null) execution.recordFailure(failure);
	}

	static final class Scope implements AutoCloseable {
		private final StatementExecution previous;
		private Scope(StatementExecution previous) { this.previous = previous; }
		@Override public void close() {
			if (this.previous == null) CURRENT.remove();
			else CURRENT.set(this.previous);
		}
	}
}
