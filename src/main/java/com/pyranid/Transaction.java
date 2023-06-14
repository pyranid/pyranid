/*
 * Copyright 2015-2022 Transmogrify LLC, 2022-2023 Revetware LLC.
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

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.logging.Logger;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * Represents a database transaction.
 *
 * @author <a href="https://www.revetware.com">Mark Allen</a>
 * @since 1.0.0
 */
public class Transaction {
	private static final AtomicLong ID_GENERATOR = new AtomicLong(0);

	private final long id = ID_GENERATOR.incrementAndGet();
	private final Optional<DataSource> dataSource;
	private final TransactionIsolation transactionIsolation;
	private final List<Consumer<TransactionResult>> postTransactionOperations;
	private final Logger logger = Logger.getLogger(Transaction.class.getName());
	private Optional<Connection> connection;
	private boolean rollbackOnly;
	private Optional<Boolean> initialAutoCommit;

	Transaction(DataSource dataSource, TransactionIsolation transactionIsolation) {
		this.dataSource = Optional.of(requireNonNull(dataSource));
		this.transactionIsolation = transactionIsolation;
		this.connection = Optional.empty();
		this.rollbackOnly = false;
		this.initialAutoCommit = Optional.empty();
		this.postTransactionOperations = new ArrayList<>();
	}

	@Override
	public String toString() {
		return format("%s{id=%s, transactionIsolation=%s, hasConnection=%s, isRollbackOnly=%s}",
				getClass().getSimpleName(), id(), transactionIsolation(), hasConnection(), isRollbackOnly());
	}

	public Savepoint createSavepoint() {
		try {
			return connection().setSavepoint();
		} catch (SQLException e) {
			throw new DatabaseException("Unable to create savepoint", e);
		}
	}

	public void rollback(Savepoint savepoint) {
		requireNonNull(savepoint);

		try {
			connection().rollback(savepoint);
		} catch (SQLException e) {
			throw new DatabaseException("Unable to roll back to savepoint", e);
		}
	}

	/**
	 * Should this transaction be rolled back upon completion?
	 * <p>
	 * Default value is {@code false}.
	 *
	 * @return {@code true} if this transaction should be rolled back, {@code false} otherwise.
	 */
	public boolean isRollbackOnly() {
		return this.rollbackOnly;
	}

	public void setRollbackOnly(boolean rollbackOnly) {
		this.rollbackOnly = rollbackOnly;
	}

	public void addPostTransactionOperation(Consumer<TransactionResult> postTransactionOperation) {
		requireNonNull(postTransactionOperation);
		postTransactionOperations.add(postTransactionOperation);
	}

	public boolean removePostTransactionOperation(Consumer<TransactionResult> postTransactionOperation) {
		requireNonNull(postTransactionOperation);
		return postTransactionOperations.remove(postTransactionOperation);
	}

	long id() {
		return this.id;
	}

	boolean hasConnection() {
		return this.connection.isPresent();
	}

	void commit() {
		if (!hasConnection()) {
			logger.finer("Transaction has no connection, so nothing to commit");
			return;
		}

		logger.finer("Committing transaction...");

		try {
			connection().commit();
			logger.finer("Transaction committed.");
		} catch (SQLException e) {
			throw new DatabaseException("Unable to commit transaction", e);
		}
	}

	void rollback() {
		if (!hasConnection()) {
			logger.finer("Transaction has no connection, so nothing to roll back");
			return;
		}

		logger.finer("Rolling back transaction...");

		try {
			connection().rollback();
			logger.finer("Transaction rolled back.");
		} catch (SQLException e) {
			throw new DatabaseException("Unable to roll back transaction", e);
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
	Connection connection() {
		if (hasConnection())
			return this.connection.get();

		try {
			this.connection = Optional.of(dataSource.get().getConnection());
		} catch (SQLException e) {
			throw new DatabaseException("Unable to acquire database connection", e);
		}

		// Keep track of the initial setting for autocommit since it might need to get changed from "true" to "false" for
		// the duration of the transaction and then back to "true" post-transaction.
		try {
			this.initialAutoCommit = Optional.of(this.connection.get().getAutoCommit());
		} catch (SQLException e) {
			throw new DatabaseException("Unable to determine database connection autocommit setting", e);
		}

		// Immediately flip autocommit to false if needed...if initially true, it will get set back to true by Database at
		// the end of the transaction
		if (this.initialAutoCommit.get())
			setAutoCommit(false);

		return this.connection.get();
	}

	void setAutoCommit(boolean autoCommit) {
		try {
			connection().setAutoCommit(autoCommit);
		} catch (SQLException e) {
			throw new DatabaseException(format("Unable to set database connection autocommit value to '%s'", autoCommit), e);
		}
	}

	Optional<Boolean> initialAutoCommit() {
		return initialAutoCommit;
	}

	public TransactionIsolation transactionIsolation() {
		return transactionIsolation;
	}

	public List<Consumer<TransactionResult>> postTransactionOperations() {
		return Collections.unmodifiableList(postTransactionOperations);
	}
}