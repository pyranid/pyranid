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

import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.LongSupplier;

/** Per-operation connection lease. Owns a standalone connection or a transaction lock, never both. */
final class StatementExecution {
	private final StatementContext<?> context;
	private final StatementConnections connections;
	private final MetricsCollectorDispatcher metrics;
	private final LongSupplier clock;
	private final long started;
	private final Optional<Transaction> transaction;
	private final ReentrantLock lock;
	private boolean locked;
	private boolean closed;
	private Connection connection;
	private long heldSince;
	private Duration acquisitionDuration;
	private Duration elapsedDuration;

	StatementExecution(StatementContext<?> context, StatementConnections connections, MetricsCollectorDispatcher metrics) {
		this(context, connections, metrics, System::nanoTime);
	}

	StatementExecution(StatementContext<?> context, StatementConnections connections, MetricsCollectorDispatcher metrics,
			LongSupplier clock) {
		this.clock = clock;
		this.started = clock.getAsLong();
		this.context = context;
		this.connections = connections;
		this.metrics = metrics;
		this.transaction = connections.currentTransaction();
		this.lock = this.transaction.map(Transaction::getConnectionLock).orElse(null);
	}

	Connection acquire(String operation) {
		if (this.lock != null) {
			Database.lockInterruptibly(this.lock, operation);
			this.locked = true;
		}
		boolean alreadyAcquired = this.transaction.isPresent() && this.transaction.get().hasConnection();
		if (this.transaction.isPresent()) {
			this.connection = this.transaction.get().getConnection();
		} else {
			this.metrics.willAcquireStatementConnection(this.context);
			try {
				this.connection = this.connections.acquire();
			} catch (SQLException e) {
				DatabaseException wrapped = DatabaseException.wrapSafely("Unable to acquire database connection", e,
						DatabaseDialect.forExceptionCause(e));
				acquisitionFailed(wrapped);
				throw wrapped;
			} catch (RuntimeException e) {
				acquisitionFailed(e);
				throw e;
			}
		}
		this.acquisitionDuration = alreadyAcquired ? null : since(this.started);
		if (this.transaction.isEmpty()) {
			this.heldSince = this.clock.getAsLong();
			if (this.metrics.isEnabled()) {
				try (var ignored = this.connections.metadataScope(this.connection)) {
					this.metrics.didAcquireStatementConnection(this.context, this.acquisitionDuration);
				}
			}
		}
		return this.connection;
	}

	private void acquisitionFailed(RuntimeException failure) {
		this.acquisitionDuration = since(this.started);
        // No connection was acquired, so ownership has ended before terminal acquisition metrics.
        this.elapsedDuration = this.acquisitionDuration;
		this.metrics.didFailToAcquireStatementConnection(this.context, this.connections.cachedType(),
				this.acquisitionDuration, failure);
	}

	boolean transactional() { return this.transaction.isPresent(); }
	Duration acquisitionDuration() { return this.acquisitionDuration; }
	Duration elapsedDuration() { return this.elapsedDuration; }
	StatementConnections.MetadataScope metadataScope() { return this.connections.metadataScope(this.connection); }

	void assertCloseThread() {
		if (this.locked && !this.lock.isHeldByCurrentThread())
			throw new DatabaseException("Transactional streams must be closed by the thread that opened them");
	}

	Throwable close(Throwable failure) {
		if (this.closed) return failure;
		assertCloseThread();
		this.closed = true;
		try {
			if (this.connection != null && this.transaction.isEmpty()) {
				Duration held = since(this.heldSince);
				Throwable releaseFailure = null;
				try {
					this.connections.release(this.connection);
				} catch (Throwable cleanup) {
					releaseFailure = cleanup;
					if (failure == null) failure = cleanup;
					else if (failure != cleanup) failure.addSuppressed(cleanup);
				}
				// Resource ownership ends before terminal release metrics; collectors can run arbitrary code.
				this.elapsedDuration = since(this.started);
				if (releaseFailure == null) this.metrics.didReleaseStatementConnection(this.context, held);
				else this.metrics.didFailToReleaseStatementConnection(this.context, held, releaseFailure);
			}
		} finally {
			if (this.locked) {
				this.lock.unlock();
				this.locked = false;
			}
			if (this.elapsedDuration == null) this.elapsedDuration = since(this.started);
		}
		return failure;
	}

	private Duration since(long start) { return Duration.ofNanos(this.clock.getAsLong() - start); }
}
