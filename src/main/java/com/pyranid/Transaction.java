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

import javax.annotation.concurrent.ThreadSafe;
import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Savepoint;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.logging.Logger;

import static java.lang.String.format;
import static java.lang.System.nanoTime;
import static java.util.Objects.requireNonNull;

/**
 * Represents a database transaction.
 * <p>
 * Note that commit and rollback operations are controlled internally by {@link Database}.
 *
 * @author <a href="https://www.revetkn.com">Mark Allen</a>
 * @since 1.0.0
 */
@ThreadSafe
public final class Transaction {
	@NonNull
	private static final AtomicLong ID_GENERATOR;

	static {
		ID_GENERATOR = new AtomicLong(0);
	}

	@NonNull
	private final Long id;
	@NonNull
	private final DataSource dataSource;
	@NonNull
	private final TransactionIsolation transactionIsolation;
	@NonNull
	private final MetricsCollectorDispatcher metricsCollectorDispatcher;
	@NonNull
	private final DatabaseType databaseType;
	@NonNull
	private final List<@NonNull Consumer<TransactionResult>> postTransactionOperations;
	@NonNull
	private final ReentrantLock connectionLock;
	@NonNull
	private final Logger logger;
	@NonNull
	private final Long ownerThreadId;

	@NonNull
	private final AtomicBoolean rollbackOnly;
	@NonNull
	private final AtomicBoolean completed;
	@Nullable
	private volatile Connection connection;
	@Nullable
	private volatile Boolean initialAutoCommit;
	@Nullable
	private volatile Integer initialTransactionIsolationJdbcLevel;
	@Nullable
	private volatile Long connectionAcquiredAtNanos;
	@NonNull
	private final AtomicBoolean transactionIsolationWasChanged;

	Transaction(@NonNull DataSource dataSource,
							@NonNull TransactionIsolation transactionIsolation,
							@NonNull MetricsCollectorDispatcher metricsCollectorDispatcher,
							@NonNull DatabaseType databaseType) {
		requireNonNull(dataSource);
		requireNonNull(transactionIsolation);
		requireNonNull(metricsCollectorDispatcher);
		requireNonNull(databaseType);

		this.id = generateId();
		this.dataSource = dataSource;
		this.transactionIsolation = transactionIsolation;
		this.metricsCollectorDispatcher = metricsCollectorDispatcher;
		this.databaseType = databaseType;
		this.connection = null;
		this.rollbackOnly = new AtomicBoolean(false);
		this.completed = new AtomicBoolean(false);
		this.initialAutoCommit = null;
		this.connectionAcquiredAtNanos = null;
		this.transactionIsolationWasChanged = new AtomicBoolean(false);
		this.postTransactionOperations = new CopyOnWriteArrayList();
		this.connectionLock = new ReentrantLock();
		this.logger = Logger.getLogger(Transaction.class.getName());
		this.ownerThreadId = Thread.currentThread().getId();
	}

	@Override
	@NonNull
	public String toString() {
		return format("%s{id=%s, transactionIsolation=%s, hasConnection=%s, isRollbackOnly=%s}",
				getClass().getSimpleName(), id(), getTransactionIsolation(), hasConnection(), isRollbackOnly());
	}

	/**
	 * Creates a transaction savepoint that can be rolled back to via {@link #rollback(Savepoint)}.
	 * <p>
	 * For most application code, prefer {@link #withSavepoint(TransactionalOperation)} or
	 * {@link #withSavepoint(ReturningTransactionalOperation)} so rollback and release cleanup are handled automatically.
	 *
	 * @return a transaction savepoint
	 * @throws IllegalStateException if this transaction has already completed
	 */
	@NonNull
	public Savepoint createSavepoint() {
		assertNotCompleted("create a savepoint");

		try {
			Savepoint savepoint = getConnection().setSavepoint();
			getMetricsCollectorDispatcher().didCreateSavepoint(this, getDatabaseType());
			return savepoint;
		} catch (SQLException e) {
			throw new DatabaseException("Unable to create savepoint", e);
		}
	}

	/**
	 * Rolls back to the provided transaction savepoint.
	 *
	 * @param savepoint the savepoint to roll back to
	 * @throws IllegalStateException if this transaction has already completed
	 */
	public void rollback(@NonNull Savepoint savepoint) {
		requireNonNull(savepoint);
		assertNotCompleted("roll back to a savepoint");

		try {
			getConnection().rollback(savepoint);
			getMetricsCollectorDispatcher().didRollbackToSavepoint(this, getDatabaseType());
		} catch (SQLException e) {
			throw new DatabaseException("Unable to roll back to savepoint", e);
		}
	}

	/**
	 * Releases the provided transaction savepoint.
	 * <p>
	 * For most application code, prefer {@link #withSavepoint(TransactionalOperation)} or
	 * {@link #withSavepoint(ReturningTransactionalOperation)} so rollback and release cleanup are handled automatically.
	 *
	 * @param savepoint the savepoint to release
	 * @throws IllegalStateException if this transaction has already completed
	 * @since 4.1.0
	 */
	public void releaseSavepoint(@NonNull Savepoint savepoint) {
		requireNonNull(savepoint);
		assertNotCompleted("release a savepoint");
		releaseSavepointJdbc(savepoint);
	}

	/**
	 * Performs an operation inside a transaction savepoint.
	 * <p>
	 * If {@code transactionalOperation} completes successfully, the savepoint is released when the driver supports release.
	 * If an exception bubbles out, Pyranid rolls back to the savepoint, attempts to release it, and preserves cleanup failures
	 * as suppressed exceptions on the thrown exception.
	 * <p>
	 * Nested savepoint usage should be stack-like: finish inner savepoints before manually releasing or rolling back outer
	 * savepoints.
	 *
	 * @param transactionalOperation the operation to perform inside a savepoint
	 * @throws IllegalStateException if this transaction has already completed
	 * @since 4.1.0
	 */
	public void withSavepoint(@NonNull TransactionalOperation transactionalOperation) {
		requireNonNull(transactionalOperation);

		withSavepoint(() -> {
			transactionalOperation.perform();
			return Optional.empty();
		});
	}

	/**
	 * Performs an operation inside a transaction savepoint and optionally returns a value.
	 * <p>
	 * If {@code transactionalOperation} completes successfully, the savepoint is released when the driver supports release.
	 * If an exception bubbles out, Pyranid rolls back to the savepoint, attempts to release it, and preserves cleanup failures
	 * as suppressed exceptions on the thrown exception.
	 * <p>
	 * Nested savepoint usage should be stack-like: finish inner savepoints before manually releasing or rolling back outer
	 * savepoints.
	 *
	 * @param transactionalOperation the operation to perform inside a savepoint
	 * @param <T>                    the type to be returned
	 * @return the result of the operation
	 * @throws IllegalStateException if this transaction has already completed
	 * @since 4.1.0
	 */
	@NonNull
	public <T> Optional<T> withSavepoint(@NonNull ReturningTransactionalOperation<T> transactionalOperation) {
		requireNonNull(transactionalOperation);
		assertNotCompleted("run a savepoint operation");

		Savepoint savepoint = createSavepoint();

		try {
			Optional<T> returnValue = transactionalOperation.perform();

			if (returnValue == null)
				returnValue = Optional.empty();

			releaseSavepointAfterSuccess(savepoint);
			return returnValue;
		} catch (RuntimeException e) {
			cleanupSavepointAfterFailure(savepoint, e);
			throw e;
		} catch (Error e) {
			cleanupSavepointAfterFailure(savepoint, e);
			throw e;
		} catch (Throwable t) {
			RuntimeException wrapped = new RuntimeException(t);
			cleanupSavepointAfterFailure(savepoint, wrapped);
			throw wrapped;
		}
	}

	/**
	 * Should this transaction be rolled back upon completion?
	 * <p>
	 * Default value is {@code false}.
	 *
	 * @return {@code true} if this transaction should be rolled back, {@code false} otherwise
	 */
	@NonNull
	public Boolean isRollbackOnly() {
		return this.rollbackOnly.get();
	}

	/**
	 * Sets whether this transaction should be rolled back upon completion.
	 *
	 * @param rollbackOnly whether to set this transaction to be rollback-only
	 */
	public void setRollbackOnly(@NonNull Boolean rollbackOnly) {
		requireNonNull(rollbackOnly);
		assertNotCompleted("set rollback-only state");
		this.rollbackOnly.set(rollbackOnly);
	}

	/**
	 * Adds an operation to the list of operations to be executed when the transaction completes.
	 *
	 * @param postTransactionOperation the post-transaction operation to add
	 */
	public void addPostTransactionOperation(@NonNull Consumer<TransactionResult> postTransactionOperation) {
		requireNonNull(postTransactionOperation);
		assertNotCompleted("add a post-transaction operation");
		this.postTransactionOperations.add(postTransactionOperation);
	}

	/**
	 * Removes an operation from the list of operations to be executed when the transaction completes.
	 *
	 * @param postTransactionOperation the post-transaction operation to remove
	 * @return {@code true} if the post-transaction operation was removed, {@code false} otherwise
	 */
	@NonNull
	public Boolean removePostTransactionOperation(@NonNull Consumer<TransactionResult> postTransactionOperation) {
		requireNonNull(postTransactionOperation);
		assertNotCompleted("remove a post-transaction operation");
		return this.postTransactionOperations.remove(postTransactionOperation);
	}

	/**
	 * Gets an unmodifiable list of post-transaction operations.
	 * <p>
	 * To manipulate the list, use {@link #addPostTransactionOperation(Consumer)} and
	 * {@link #removePostTransactionOperation(Consumer)}.
	 *
	 * @return the list of post-transaction operations
	 */
	@NonNull
	public List<@NonNull Consumer<TransactionResult>> getPostTransactionOperations() {
		return Collections.unmodifiableList(this.postTransactionOperations);
	}

	/**
	 * Get the isolation level for this transaction.
	 *
	 * @return the isolation level
	 */
	@NonNull
	public TransactionIsolation getTransactionIsolation() {
		return this.transactionIsolation;
	}

	@NonNull
	Long id() {
		return this.id;
	}

	@NonNull
	Boolean hasConnection() {
		getConnectionLock().lock();

		try {
			return this.connection != null;
		} finally {
			getConnectionLock().unlock();
		}
	}

	@NonNull
	Boolean isOwnedByCurrentThread() {
		return Thread.currentThread().getId() == this.ownerThreadId;
	}

	@NonNull
	Boolean isOwnedBy(@NonNull DataSource dataSource) {
		requireNonNull(dataSource);
		return this.dataSource == dataSource;
	}

	void commit() {
		getConnectionLock().lock();

		try {
			if (!hasConnection()) {
				logger.finer("Transaction has no connection, so nothing to commit");
				return;
			}

			logger.finer("Committing transaction...");

			long startTime = nanoTime();

			try {
				getConnection().commit();
				getMetricsCollectorDispatcher().didCommitPhysicalTransaction(this, getDatabaseType(), Duration.ofNanos(nanoTime() - startTime));
				logger.finer("Transaction committed.");
			} catch (SQLException e) {
				DatabaseException wrapped = new DatabaseException("Unable to commit transaction", e);
				getMetricsCollectorDispatcher().didFailToCommitPhysicalTransaction(this, getDatabaseType(), Duration.ofNanos(nanoTime() - startTime), wrapped);
				throw wrapped;
			}
		} finally {
			getConnectionLock().unlock();
		}
	}

	void rollback() {
		getConnectionLock().lock();

		try {
			if (!hasConnection()) {
				logger.finer("Transaction has no connection, so nothing to roll back");
				return;
			}

			logger.finer("Rolling back transaction...");

			long startTime = nanoTime();

			try {
				getConnection().rollback();
				getMetricsCollectorDispatcher().didRollbackPhysicalTransaction(this, getDatabaseType(), Duration.ofNanos(nanoTime() - startTime));
				logger.finer("Transaction rolled back.");
			} catch (SQLException e) {
				DatabaseException wrapped = new DatabaseException("Unable to roll back transaction", e);
				getMetricsCollectorDispatcher().didFailToRollbackPhysicalTransaction(this, getDatabaseType(), Duration.ofNanos(nanoTime() - startTime), wrapped);
				throw wrapped;
			}
		} finally {
			getConnectionLock().unlock();
		}
	}

	/**
	 * The connection associated with this transaction.
	 * <p>
	 * If no connection is associated yet, we ask the {@link DataSource} for one.
	 *
	 * @return The connection associated with this transaction.
	 * @throws DatabaseException if unable to acquire a connection.
	 */
	@NonNull
	Connection getConnection() {
		getConnectionLock().lock();

		try {
			assertNotCompleted("get the transaction connection");

			if (hasConnection())
				return this.connection;

			long startTime = nanoTime();
			getMetricsCollectorDispatcher().willAcquireTransactionConnection(this, getDatabaseType());

			try {
				this.connection = getDataSource().getConnection();
			} catch (SQLException e) {
				DatabaseException wrapped = new DatabaseException("Unable to acquire database connection", e);
				Duration acquisitionDuration = Duration.ofNanos(nanoTime() - startTime);
				getMetricsCollectorDispatcher().didFailToAcquireTransactionConnection(this, getDatabaseType(), acquisitionDuration, wrapped);
				getMetricsCollectorDispatcher().didFailToBeginPhysicalTransaction(this, getTransactionIsolation(),
						MetricsCollector.PhysicalTransactionBeginFailurePhase.ACQUIRE_CONNECTION, getDatabaseType(), wrapped);
				throw wrapped;
			}

			this.connectionAcquiredAtNanos = nanoTime();
			getMetricsCollectorDispatcher().didAcquireTransactionConnection(this, getDatabaseType(), Duration.ofNanos(this.connectionAcquiredAtNanos - startTime));

			// Keep track of the initial setting for autocommit since it might need to get changed from "true" to "false" for
			// the duration of the transaction and then back to "true" post-transaction.
			try {
				this.initialAutoCommit = this.connection.getAutoCommit();
			} catch (SQLException e) {
				DatabaseException wrapped = new DatabaseException("Unable to determine database connection autocommit setting", e);
				getMetricsCollectorDispatcher().didFailToBeginPhysicalTransaction(this, getTransactionIsolation(),
						MetricsCollector.PhysicalTransactionBeginFailurePhase.READ_INITIAL_AUTOCOMMIT, getDatabaseType(), wrapped);
				throw wrapped;
			}

			// Track initial isolation
			try {
				this.initialTransactionIsolationJdbcLevel = this.connection.getTransactionIsolation();
			} catch (SQLException e) {
				DatabaseException wrapped = new DatabaseException("Unable to determine database connection transaction isolation", e);
				getMetricsCollectorDispatcher().didFailToBeginPhysicalTransaction(this, getTransactionIsolation(),
						MetricsCollector.PhysicalTransactionBeginFailurePhase.READ_INITIAL_ISOLATION, getDatabaseType(), wrapped);
				throw wrapped;
			}

			// Immediately flip autocommit to false if needed...if initially true, it will get set back to true by Database at
			// the end of the transaction
			if (this.initialAutoCommit) {
				try {
					setAutoCommit(false);
				} catch (DatabaseException e) {
					getMetricsCollectorDispatcher().didFailToBeginPhysicalTransaction(this, getTransactionIsolation(),
							MetricsCollector.PhysicalTransactionBeginFailurePhase.SET_AUTOCOMMIT_FALSE, getDatabaseType(), e);
					throw e;
				}
			}

			// Apply requested isolation if not DEFAULT and different from current
			TransactionIsolation desiredTransactionIsolation = getTransactionIsolation();

			if (desiredTransactionIsolation != TransactionIsolation.DEFAULT) {
				// Safe; only DEFAULT has a null value
				int desiredJdbcLevel = desiredTransactionIsolation.getJdbcLevel().get();
				// Apply only if different from current (or current unknown)
				if (this.initialTransactionIsolationJdbcLevel == null || this.initialTransactionIsolationJdbcLevel.intValue() != desiredJdbcLevel) {
					try {
						// In the future, we might check supportsTransactionIsolationLevel via DatabaseMetaData first.
						// Probably want to calculate that at Database init time and cache it off
						this.connection.setTransactionIsolation(desiredJdbcLevel);
						this.transactionIsolationWasChanged.set(true);
					} catch (SQLException e) {
						DatabaseException wrapped = new DatabaseException(format("Unable to set transaction isolation to %s", desiredTransactionIsolation.name()), e);
						getMetricsCollectorDispatcher().didFailToBeginPhysicalTransaction(this, getTransactionIsolation(),
								MetricsCollector.PhysicalTransactionBeginFailurePhase.SET_ISOLATION, getDatabaseType(), wrapped);
						throw wrapped;
					}
				}
			}

			getMetricsCollectorDispatcher().didBeginPhysicalTransaction(this, getTransactionIsolation(), getDatabaseType());
			return this.connection;
		} finally {
			getConnectionLock().unlock();
		}
	}

	void setAutoCommit(@NonNull Boolean autoCommit) {
		requireNonNull(autoCommit);

		getConnectionLock().lock();

		try {
			Connection connection = this.connection;

			if (connection == null)
				throw new DatabaseException("Transaction has no connection");

			try {
				connection.setAutoCommit(autoCommit);
			} catch (SQLException e) {
				throw new DatabaseException(format("Unable to set database connection autocommit value to '%s'", autoCommit), e);
			}
		} finally {
			getConnectionLock().unlock();
		}
	}

	void restoreTransactionIsolationIfNeeded() {
		getConnectionLock().lock();

		try {
			if (this.connection == null)
				return;

			Integer initialTransactionIsolationJdbcLevel = getInitialTransactionIsolationJdbcLevel().orElse(null);

			if (getTransactionIsolationWasChanged() && initialTransactionIsolationJdbcLevel != null) {
				try {
					this.connection.setTransactionIsolation(initialTransactionIsolationJdbcLevel.intValue());
				} catch (SQLException e) {
					throw new DatabaseException("Unable to restore original transaction isolation", e);
				} finally {
					this.transactionIsolationWasChanged.set(false);
				}
			}
		} finally {
			getConnectionLock().unlock();
		}
	}

	@NonNull
	Long generateId() {
		return ID_GENERATOR.incrementAndGet();
	}

	@NonNull
	Optional<Boolean> getInitialAutoCommit() {
		return Optional.ofNullable(this.initialAutoCommit);
	}

	@NonNull
	DataSource getDataSource() {
		return this.dataSource;
	}

	@NonNull
	protected Optional<Integer> getInitialTransactionIsolationJdbcLevel() {
		return Optional.ofNullable(this.initialTransactionIsolationJdbcLevel);
	}

	@NonNull
	protected Boolean getTransactionIsolationWasChanged() {
		return this.transactionIsolationWasChanged.get();
	}

	@NonNull
	protected ReentrantLock getConnectionLock() {
		return this.connectionLock;
	}

	@NonNull
	Optional<Connection> getExistingConnection() {
		getConnectionLock().lock();

		try {
			return Optional.ofNullable(this.connection);
		} finally {
			getConnectionLock().unlock();
		}
	}

	void clearConnection() {
		getConnectionLock().lock();

		try {
			this.connection = null;
		} finally {
			getConnectionLock().unlock();
		}
	}

	void markCompleted() {
		this.completed.set(true);
	}

	@NonNull
	Boolean isCompleted() {
		return this.completed.get();
	}

	private void releaseSavepointAfterSuccess(@NonNull Savepoint savepoint) {
		requireNonNull(savepoint);

		try {
			getConnection().releaseSavepoint(savepoint);
			getMetricsCollectorDispatcher().didReleaseSavepoint(this, getDatabaseType());
		} catch (SQLFeatureNotSupportedException e) {
			// Some drivers support rollback-to-savepoint but not release; successful closures should still succeed.
		} catch (SQLException e) {
			throw new DatabaseException("Unable to release savepoint", e);
		}
	}

	private void cleanupSavepointAfterFailure(@NonNull Savepoint savepoint,
																						@NonNull Throwable primary) {
		requireNonNull(savepoint);
		requireNonNull(primary);

		try {
			getConnection().rollback(savepoint);
			getMetricsCollectorDispatcher().didRollbackToSavepoint(this, getDatabaseType());
		} catch (Throwable rollbackException) {
			primary.addSuppressed(new DatabaseException("Unable to roll back to savepoint", rollbackException));
		}

		try {
			getConnection().releaseSavepoint(savepoint);
			getMetricsCollectorDispatcher().didReleaseSavepoint(this, getDatabaseType());
		} catch (SQLFeatureNotSupportedException e) {
			// Some drivers support rollback-to-savepoint but not release.
		} catch (Throwable releaseException) {
			primary.addSuppressed(new DatabaseException("Unable to release savepoint", releaseException));
		}
	}

	private void releaseSavepointJdbc(@NonNull Savepoint savepoint) {
		requireNonNull(savepoint);

		try {
			getConnection().releaseSavepoint(savepoint);
			getMetricsCollectorDispatcher().didReleaseSavepoint(this, getDatabaseType());
		} catch (SQLException e) {
			throw new DatabaseException("Unable to release savepoint", e);
		}
	}

	@NonNull
	MetricsCollectorDispatcher getMetricsCollectorDispatcher() {
		return this.metricsCollectorDispatcher;
	}

	@NonNull
	DatabaseType getDatabaseType() {
		return this.databaseType;
	}

	@NonNull
	Optional<Long> getConnectionAcquiredAtNanos() {
		return Optional.ofNullable(this.connectionAcquiredAtNanos);
	}

	private void assertNotCompleted(@NonNull String operation) {
		requireNonNull(operation);

		if (isCompleted())
			throw new IllegalStateException(format("Transaction %s has already completed and cannot %s", id(), operation));
	}
}
