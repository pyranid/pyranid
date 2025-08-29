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
import javax.annotation.concurrent.NotThreadSafe;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static java.lang.String.format;

/**
 * Thrown when an error occurs when interacting with a {@link Database}.
 * <p>
 * If the {@code cause} of this exception is a {@link SQLException}, the {@link #getErrorCode()} and {@link #getSqlState()}
 * accessors are shorthand for retrieving the corresponding {@link SQLException} values.
 *
 * @author <a href="https://www.revetkn.com">Mark Allen</a>
 * @since 1.0.0
 */
@NotThreadSafe
public class DatabaseException extends RuntimeException {
	@Nullable
	private final Integer errorCode;
	@Nullable
	private final String sqlState;
	@Nullable
	private final String column;
	@Nullable
	private final String constraint;
	@Nullable
	private final String datatype;
	@Nullable
	private final String detail;
	@Nullable
	private final String file;
	@Nullable
	private final String hint;
	@Nullable
	private final Integer internalPosition;
	@Nullable
	private final String internalQuery;
	@Nullable
	private final Integer line;
	@Nullable
	private final String dbmsMessage;
	@Nullable
	private final Integer position;
	@Nullable
	private final String routine;
	@Nullable
	private final String schema;
	@Nullable
	private final String severity;
	@Nullable
	private final String table;
	@Nullable
	private final String where;

	/**
	 * Creates a {@code DatabaseException} with the given {@code message}.
	 *
	 * @param message a message describing this exception
	 */
	public DatabaseException(@Nullable String message) {
		this(message, null);
	}

	/**
	 * Creates a {@code DatabaseException} which wraps the given {@code cause}.
	 *
	 * @param cause the cause of this exception
	 */
	public DatabaseException(@Nullable Throwable cause) {
		this(cause == null ? null : cause.getMessage(), cause);
	}

	/**
	 * Creates a {@code DatabaseException} which wraps the given {@code cause}.
	 *
	 * @param message a message describing this exception
	 * @param cause   the cause of this exception
	 */
	public DatabaseException(@Nullable String message,
													 @Nullable Throwable cause) {
		super(message, cause);

		Integer errorCode = null;
		String sqlState = null;
		String column = null;
		String constraint = null;
		String datatype = null;
		String detail = null;
		String file = null;
		String hint = null;
		Integer internalPosition = null;
		String internalQuery = null;
		Integer line = null;
		String dbmsMessage = null;
		Integer position = null;
		String routine = null;
		String schema = null;
		String severity = null;
		String table = null;
		String where = null;

		if (cause != null) {
			// Special handling for Postgres
			if ("org.postgresql.util.PSQLException".equals(cause.getClass().getName())) {
				org.postgresql.util.PSQLException psqlException = (org.postgresql.util.PSQLException) cause;
				org.postgresql.util.ServerErrorMessage serverErrorMessage = psqlException.getServerErrorMessage();

				if (serverErrorMessage != null) {
					errorCode = psqlException.getErrorCode();
					column = serverErrorMessage.getColumn();
					constraint = serverErrorMessage.getConstraint();
					datatype = serverErrorMessage.getDatatype();
					detail = serverErrorMessage.getDetail();
					file = serverErrorMessage.getFile();
					hint = serverErrorMessage.getHint();
					internalQuery = serverErrorMessage.getInternalQuery();
					dbmsMessage = serverErrorMessage.getMessage();
					routine = serverErrorMessage.getRoutine();
					schema = serverErrorMessage.getSchema();
					severity = serverErrorMessage.getSeverity();
					sqlState = serverErrorMessage.getSQLState();
					table = serverErrorMessage.getTable();
					where = serverErrorMessage.getWhere();
					internalPosition = serverErrorMessage.getInternalPosition();
					line = serverErrorMessage.getLine();
					position = serverErrorMessage.getPosition();
				}
			} else if (cause instanceof SQLException) {
				SQLException sqlException = (SQLException) cause;
				errorCode = sqlException.getErrorCode();
				sqlState = sqlException.getSQLState();
			}
		}

		this.errorCode = errorCode;
		this.sqlState = sqlState;
		this.column = column;
		this.constraint = constraint;
		this.datatype = datatype;
		this.detail = detail;
		this.file = file;
		this.hint = hint;
		this.internalPosition = internalPosition;
		this.internalQuery = internalQuery;
		this.line = line;
		this.dbmsMessage = dbmsMessage;
		this.position = position;
		this.routine = routine;
		this.schema = schema;
		this.severity = severity;
		this.table = table;
		this.where = where;
	}

	@Override
	public String toString() {
		List<String> components = new ArrayList<>(20);

		if (getMessage() != null && getMessage().trim().length() > 0)
			components.add(format("message=%s", getMessage()));

		if (getErrorCode().isPresent())
			components.add(format("errorCode=%s", getErrorCode().get()));
		if (getSqlState().isPresent())
			components.add(format("sqlState=%s", getSqlState().get()));
		if (getColumn().isPresent())
			components.add(format("column=%s", getColumn().get()));
		if (getConstraint().isPresent())
			components.add(format("constraint=%s", getConstraint().get()));
		if (getDatatype().isPresent())
			components.add(format("datatype=%s", getDatatype().get()));
		if (getDetail().isPresent())
			components.add(format("detail=%s", getDetail().get()));
		if (getFile().isPresent())
			components.add(format("file=%s", getFile().get()));
		if (getHint().isPresent())
			components.add(format("hint=%s", getHint().get()));
		if (getInternalPosition().isPresent())
			components.add(format("internalPosition=%s", getInternalPosition().get()));
		if (getInternalQuery().isPresent())
			components.add(format("internalQuery=%s", getInternalQuery().get()));
		if (getLine().isPresent())
			components.add(format("line=%s", getLine().get()));
		if (getDbmsMessage().isPresent())
			components.add(format("dbmsMessage=%s", getDbmsMessage().get()));
		if (getPosition().isPresent())
			components.add(format("position=%s", getPosition().get()));
		if (getRoutine().isPresent())
			components.add(format("routine=%s", getRoutine().get()));
		if (getSchema().isPresent())
			components.add(format("schema=%s", getSchema().get()));
		if (getSeverity().isPresent())
			components.add(format("severity=%s", getSeverity().get()));
		if (getTable().isPresent())
			components.add(format("table=%s", getTable().get()));
		if (getWhere().isPresent())
			components.add(format("where=%s", getWhere().get()));

		return format("%s: %s", getClass().getName(), components.stream().collect(Collectors.joining(", ")));
	}

	/**
	 * Shorthand for {@link SQLException#getErrorCode()} if this exception was caused by a {@link SQLException}.
	 *
	 * @return the value of {@link SQLException#getErrorCode()}, or empty if not available
	 */
	@Nonnull
	public Optional<Integer> getErrorCode() {
		return Optional.ofNullable(this.errorCode);
	}

	/**
	 * Shorthand for {@link SQLException#getSQLState()} if this exception was caused by a {@link SQLException}.
	 *
	 * @return the value of {@link SQLException#getSQLState()}, or empty if not available
	 */
	@Nonnull
	public Optional<String> getSqlState() {
		return Optional.ofNullable(this.sqlState);
	}

	/**
	 * @return the value of the offending {@code column}, or empty if not available
	 * @since 1.0.12
	 */
	@Nonnull
	public Optional<String> getColumn() {
		return Optional.ofNullable(this.column);
	}

	/**
	 * @return the value of the offending {@code constraint}, or empty if not available
	 * @since 1.0.12
	 */
	@Nonnull
	public Optional<String> getConstraint() {
		return Optional.ofNullable(this.constraint);
	}

	/**
	 * @return the value of the offending {@code datatype}, or empty if not available
	 * @since 1.0.12
	 */
	@Nonnull
	public Optional<String> getDatatype() {
		return Optional.ofNullable(this.datatype);
	}

	/**
	 * @return the value of the offending {@code detail}, or empty if not available
	 * @since 1.0.12
	 */
	@Nonnull
	public Optional<String> getDetail() {
		return Optional.ofNullable(this.detail);
	}

	/**
	 * @return the value of the offending {@code file}, or empty if not available
	 * @since 1.0.12
	 */
	@Nonnull
	public Optional<String> getFile() {
		return Optional.ofNullable(this.file);
	}

	/**
	 * @return the value of the error {@code hint}, or empty if not available
	 * @since 1.0.12
	 */
	@Nonnull
	public Optional<String> getHint() {
		return Optional.ofNullable(this.hint);
	}

	/**
	 * @return the value of the offending {@code internalPosition}, or empty if not available
	 * @since 1.0.12
	 */
	@Nonnull
	public Optional<Integer> getInternalPosition() {
		return Optional.ofNullable(this.internalPosition);
	}

	/**
	 * @return the value of the offending {@code internalQuery}, or empty if not available
	 * @since 1.0.12
	 */
	@Nonnull
	public Optional<String> getInternalQuery() {
		return Optional.ofNullable(this.internalQuery);
	}

	/**
	 * @return the value of the offending {@code line}, or empty if not available
	 * @since 1.0.12
	 */
	@Nonnull
	public Optional<Integer> getLine() {
		return Optional.ofNullable(this.line);
	}

	/**
	 * @return the value of the error {@code dbmsMessage}, or empty if not available
	 * @since 1.0.12
	 */
	@Nonnull
	public Optional<String> getDbmsMessage() {
		return Optional.ofNullable(this.dbmsMessage);
	}

	/**
	 * @return the value of the offending {@code position}, or empty if not available
	 * @since 1.0.12
	 */
	@Nonnull
	public Optional<Integer> getPosition() {
		return Optional.ofNullable(this.position);
	}

	/**
	 * @return the value of the offending {@code routine}, or empty if not available
	 * @since 1.0.12
	 */
	@Nonnull
	public Optional<String> getRoutine() {
		return Optional.ofNullable(this.routine);
	}

	/**
	 * @return the value of the offending {@code schema}, or empty if not available
	 * @since 1.0.12
	 */
	@Nonnull
	public Optional<String> getSchema() {
		return Optional.ofNullable(this.schema);
	}

	/**
	 * @return the error {@code severity}, or empty if not available
	 * @since 1.0.12
	 */
	@Nonnull
	public Optional<String> getSeverity() {
		return Optional.ofNullable(this.severity);
	}

	/**
	 * @return the value of the offending {@code table}, or empty if not available
	 * @since 1.0.12
	 */
	@Nonnull
	public Optional<String> getTable() {
		return Optional.ofNullable(this.table);
	}

	/**
	 * @return the value of the offending {@code where}, or empty if not available
	 * @since 1.0.12
	 */
	@Nonnull
	public Optional<String> getWhere() {
		return Optional.ofNullable(this.where);
	}
}