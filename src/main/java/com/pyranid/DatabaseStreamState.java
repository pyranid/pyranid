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

import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

/**
 * Dialect-owned state for a streaming {@link java.sql.ResultSet}.
 */
final class DatabaseStreamState {
	@NonNull
	private static final DatabaseStreamState NONE = new DatabaseStreamState(false, null);

	private final boolean managedTransactionStarted;
	@Nullable
	private final Boolean initialAutoCommit;

	private DatabaseStreamState(boolean managedTransactionStarted,
															@Nullable Boolean initialAutoCommit) {
		this.managedTransactionStarted = managedTransactionStarted;
		this.initialAutoCommit = initialAutoCommit;
	}

	@NonNull
	static DatabaseStreamState none() {
		return NONE;
	}

	@NonNull
	static DatabaseStreamState managedTransaction(@Nullable Boolean initialAutoCommit) {
		return new DatabaseStreamState(true, initialAutoCommit);
	}

	boolean isManagedTransactionStarted() {
		return this.managedTransactionStarted;
	}

	@Nullable
	Boolean getInitialAutoCommit() {
		return this.initialAutoCommit;
	}
}
