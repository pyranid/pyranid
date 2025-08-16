/*
 * Copyright 2015-2022 Transmogrify LLC, 2022-2025 Revetware LLC.
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

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.logging.Logger;

import static java.lang.String.format;
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
public class Transaction {
	@Nonnull
	private static final AtomicLong ID_GENERATOR;

	static {
		ID_GENERATOR = new AtomicLong(0);
	}

	@Nonnull
	private final Long id;
	@Nonnull
	private final DataSource dataSource;
	@Nonnull
	private final TransactionIsolation transactionIsolation;
	@Nonnull
	private final List<Consumer<TransactionResult>> postTransactionOperations;
	@Nonnull
	private final ReentrantLock connectionLock;
	@Nonnull
	private final Logger logger;

	@Nullable
	private Connection connection;
	@Nonnull
	private Boolean rollbackOnly;
	@Nullable
	private Boolean initialAutoCommit;

	Transaction(@Nonnull DataSource dataSource,
							@Nonnull TransactionIsolation transactionIsolation) {
		requireNonNull(dataSource);
		requireNonNull(transactionIsolation);

		this.id = generateId();
		this.dataSource = dataSource;
		this.transactionIsolation = transactionIsolation;
		this.connection = null;
		this.rollbackOnly = false;
		this.initialAutoCommit = null;
		this.postTransactionOperations = new CopyOnWriteArrayList();
		this.connectionLock = new ReentrantLock();
		this.logger = Logger.getLogger(Transaction.class.getName());
	}

	@Override
	@Nonnull
	public String toString() {
		return format("%s{id=%s, transactionIsolation=%s, hasConnection=%s, isRollbackOnly=%s}",
				getClass().getSimpleName(), id(), getTransactionIsolation(), hasConnection(), isRollbackOnly());
	}

	/**
	 * Creates a transaction savepoint that can be rolled back to via {@link #rollback(Savepoint)}.
	 *
	 * @return a transaction savepoint
	 */
	@Nonnull
	public Savepoint createSavepoint() {
		try {
			return getConnection().setSavepoint();
		} catch (SQLException e) {
			throw new DatabaseException("Unable to create savepoint", e);
		}
	}

	/**
	 * Rolls back to the provided transaction savepoint.
	 *
	 * @param savepoint the savepoint to roll back to
	 */
	public void rollback(@Nonnull Savepoint savepoint) {
		requireNonNull(savepoint);

		try {
			getConnection().rollback(savepoint);
		} catch (SQLException e) {
			throw new DatabaseException("Unable to roll back to savepoint", e);
		}
	}

	/**
	 * Should this transaction be rolled back upon completion?
	 * <p>
	 * Default value is {@code false}.
	 *
	 * @return {@code true} if this transaction should be rolled back, {@code false} otherwise
	 */
	@Nonnull
	public Boolean isRollbackOnly() {
		return this.rollbackOnly;
	}

	/**
	 * Sets whether this transaction should be rolled back upon completion.
	 *
	 * @param rollbackOnly whether to set this transaction to be rollback-only
	 */
	public void setRollbackOnly(@Nonnull Boolean rollbackOnly) {
		requireNonNull(rollbackOnly);
		this.rollbackOnly = rollbackOnly;
	}

	/**
	 * Adds an operation to the list of operations to be executed when the transaction completes.
	 *
	 * @param postTransactionOperation the post-transaction operation to add
	 */
	public void addPostTransactionOperation(@Nonnull Consumer<TransactionResult> postTransactionOperation) {
		requireNonNull(postTransactionOperation);
		this.postTransactionOperations.add(postTransactionOperation);
	}

	/**
	 * Removes an operation from the list of operations to be executed when the transaction completes.
	 *
	 * @param postTransactionOperation the post-transaction operation to remove
	 * @return {@code true} if the post-transaction operation was removed, {@code false} otherwise
	 */
	@Nonnull
	public Boolean removePostTransactionOperation(@Nonnull Consumer<TransactionResult> postTransactionOperation) {
		requireNonNull(postTransactionOperation);
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
	@Nonnull
	public List<Consumer<TransactionResult>> getPostTransactionOperations() {
		return Collections.unmodifiableList(this.postTransactionOperations);
	}

	/**
	 * Get the isolation level for this transaction.
	 *
	 * @return the isolation level
	 */
	@Nonnull
	public TransactionIsolation getTransactionIsolation() {
		return this.transactionIsolation;
	}

	@Nonnull
	Long id() {
		return this.id;
	}

	@Nonnull
	Boolean hasConnection() {
		getConnectionLock().lock();

		try {
			return this.connection != null;
		} finally {
			getConnectionLock().unlock();
		}
	}

	void commit() {
		getConnectionLock().lock();

		try {
			if (!hasConnection()) {
				logger.finer("Transaction has no connection, so nothing to commit");
				return;
			}

			logger.finer("Committing transaction...");

			try {
				getConnection().commit();
				logger.finer("Transaction committed.");
			} catch (SQLException e) {
				throw new DatabaseException("Unable to commit transaction", e);
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

			try {
				getConnection().rollback();
				logger.finer("Transaction rolled back.");
			} catch (SQLException e) {
				throw new DatabaseException("Unable to roll back transaction", e);
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
	@Nonnull
	Connection getConnection() {
		getConnectionLock().lock();

		try {
			if (hasConnection())
				return this.connection;

			try {
				this.connection = getDataSource().getConnection();
			} catch (SQLException e) {
				throw new DatabaseException("Unable to acquire database connection", e);
			}

			// Keep track of the initial setting for autocommit since it might need to get changed from "true" to "false" for
			// the duration of the transaction and then back to "true" post-transaction.
			try {
				this.initialAutoCommit = this.connection.getAutoCommit();
			} catch (SQLException e) {
				throw new DatabaseException("Unable to determine database connection autocommit setting", e);
			}

			// Immediately flip autocommit to false if needed...if initially true, it will get set back to true by Database at
			// the end of the transaction
			if (this.initialAutoCommit)
				setAutoCommit(false);

			return this.connection;
		} finally {
			getConnectionLock().unlock();
		}
	}

	void setAutoCommit(@Nonnull Boolean autoCommit) {
		requireNonNull(autoCommit);

		try {
			getConnection().setAutoCommit(autoCommit);
		} catch (SQLException e) {
			throw new DatabaseException(format("Unable to set database connection autocommit value to '%s'", autoCommit), e);
		}
	}

	@Nonnull
	Long generateId() {
		return ID_GENERATOR.incrementAndGet();
	}

	@Nonnull
	Optional<Boolean> getInitialAutoCommit() {
		return Optional.ofNullable(this.initialAutoCommit);
	}

	@Nonnull
	DataSource getDataSource() {
		return this.dataSource;
	}

	@Nonnull
	protected ReentrantLock getConnectionLock() {
		return this.connectionLock;
	}
}