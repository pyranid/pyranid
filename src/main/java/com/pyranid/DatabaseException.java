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

import javax.annotation.concurrent.NotThreadSafe;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.UnaryOperator;
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
	@NonNull
	private final Boolean uniqueConstraintViolation;
	@NonNull
	private final Boolean foreignKeyViolation;
	@NonNull
	private final Boolean deadlock;
	@NonNull
	private final Boolean transientException;
	@NonNull
	private final Boolean serializationFailure;
	@NonNull
	private final Boolean timeout;

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
		this(message, cause, DatabaseDialect.forExceptionCause(cause));
	}

	DatabaseException(@Nullable String message,
										@Nullable Throwable cause,
										@NonNull DatabaseDialect databaseDialect) {
		this(message, cause, databaseDialect, null);
	}

	/**
	 * Package-private constructor which applies a best-effort diagnostic redactor to every String metadata field
	 * extracted from the cause - used by {@link Database} to scrub verbatim occurrences of {@link SecureParameter}
	 * values that the database driver may have echoed into its error text.
	 * <p>
	 * The {@code message} is deliberately NOT re-scrubbed here: the caller is expected to have scrubbed the raw
	 * driver text before composing the message (scrubbing must run before whitespace-collapsing/truncation), and
	 * the composed remainder is Pyranid-rendered and already redaction-aware. Re-applying the redactor would
	 * re-scan mask text from the first pass, violating the documented one-pass "masks are never re-masked"
	 * semantics. Each metadata field, by contrast, is raw dialect-extracted text seen exactly once.
	 * <p>
	 * The {@code cause} chain is deliberately left untouched: sinks that render the stack trace or walk
	 * {@link #getCause()} can still observe the raw driver text.
	 */
	DatabaseException(@Nullable String message,
										@Nullable Throwable cause,
										@NonNull DatabaseDialect databaseDialect,
										@Nullable UnaryOperator<String> diagnosticRedactor) {
		super(message, cause);

		DatabaseExceptionMetadata metadata = databaseDialect.databaseExceptionMetadata(cause);

		this.errorCode = metadata.errorCode;
		this.sqlState = applyDiagnosticRedactor(diagnosticRedactor, metadata.sqlState);
		this.column = applyDiagnosticRedactor(diagnosticRedactor, metadata.column);
		this.constraint = applyDiagnosticRedactor(diagnosticRedactor, metadata.constraint);
		this.datatype = applyDiagnosticRedactor(diagnosticRedactor, metadata.datatype);
		this.detail = applyDiagnosticRedactor(diagnosticRedactor, metadata.detail);
		this.file = applyDiagnosticRedactor(diagnosticRedactor, metadata.file);
		this.hint = applyDiagnosticRedactor(diagnosticRedactor, metadata.hint);
		this.internalPosition = metadata.internalPosition;
		this.internalQuery = applyDiagnosticRedactor(diagnosticRedactor, metadata.internalQuery);
		this.line = metadata.line;
		this.dbmsMessage = applyDiagnosticRedactor(diagnosticRedactor, metadata.dbmsMessage);
		this.position = metadata.position;
		this.routine = applyDiagnosticRedactor(diagnosticRedactor, metadata.routine);
		this.schema = applyDiagnosticRedactor(diagnosticRedactor, metadata.schema);
		this.severity = applyDiagnosticRedactor(diagnosticRedactor, metadata.severity);
		this.table = applyDiagnosticRedactor(diagnosticRedactor, metadata.table);
		this.where = applyDiagnosticRedactor(diagnosticRedactor, metadata.where);
		this.uniqueConstraintViolation = databaseDialect.isUniqueConstraintViolation(metadata, cause);
		this.foreignKeyViolation = databaseDialect.isForeignKeyViolation(metadata, cause);
		this.deadlock = databaseDialect.isDeadlock(metadata, cause);
		this.transientException = databaseDialect.isTransient(metadata, cause);
		this.serializationFailure = databaseDialect.isSerializationFailure(metadata, cause);
		this.timeout = databaseDialect.isTimeout(metadata, cause);
	}

	@Nullable
	private static String applyDiagnosticRedactor(@Nullable UnaryOperator<String> diagnosticRedactor,
																								@Nullable String value) {
		return diagnosticRedactor == null || value == null ? value : diagnosticRedactor.apply(value);
	}

	@Override
	public String toString() {
		List<String> components = new ArrayList<>(20);

		if (getMessage() != null && getMessage().trim().length() > 0)
			components.add(format("message=%s", getMessage().trim()));

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
	@NonNull
	public Optional<Integer> getErrorCode() {
		return Optional.ofNullable(this.errorCode);
	}

	/**
	 * Shorthand for {@link SQLException#getSQLState()} if this exception was caused by a {@link SQLException}.
	 *
	 * @return the value of {@link SQLException#getSQLState()}, or empty if not available
	 */
	@NonNull
	public Optional<String> getSqlState() {
		return Optional.ofNullable(this.sqlState);
	}

	/**
	 * Determines if this exception is recognized as a unique constraint violation.
	 * <p>
	 * This method is intentionally conservative: it returns {@code true} only when Pyranid recognizes a database-specific
	 * error code or SQLState as a unique or primary-key uniqueness violation.
	 *
	 * @return {@code true} if this exception is recognized as a unique constraint violation
	 * @since 4.3.0
	 */
	@NonNull
	public Boolean isUniqueConstraintViolation() {
		return this.uniqueConstraintViolation;
	}

	/**
	 * Determines if this exception is recognized as a foreign-key violation.
	 * <p>
	 * This method is intentionally conservative: it returns {@code true} only when Pyranid recognizes a database-specific
	 * error code or SQLState as a foreign-key violation.
	 * For example, SQL Server error {@code 547} is not classified as a foreign-key violation by code alone because the
	 * same error code also covers other constraint failures, such as {@code CHECK} constraints.
	 *
	 * @return {@code true} if this exception is recognized as a foreign-key violation
	 * @since 4.3.0
	 */
	@NonNull
	public Boolean isForeignKeyViolation() {
		return this.foreignKeyViolation;
	}

	/**
	 * Determines if this exception is recognized as a deadlock.
	 * <p>
	 * This method is intentionally conservative: it returns {@code true} only when Pyranid recognizes a database-specific
	 * error code or SQLState as a deadlock.
	 *
	 * @return {@code true} if this exception is recognized as a deadlock
	 * @since 4.3.0
	 */
	@NonNull
	public Boolean isDeadlock() {
		return this.deadlock;
	}

	/**
	 * Determines if this exception is recognized as transient.
	 * <p>
	 * This method is intentionally conservative: it returns {@code true} for JDBC transient/recoverable exception classes,
	 * SQLState classes for connection exceptions and transaction rollbacks, or database-specific deadlock/lock-timeout
	 * signals recognized by Pyranid. A {@code true} result does not guarantee that retrying the operation will succeed.
	 *
	 * @return {@code true} if this exception is recognized as transient
	 * @since 4.3.0
	 */
	@NonNull
	public Boolean isTransient() {
		return this.transientException;
	}

	/**
	 * Determines if this exception is recognized as a serialization failure.
	 * <p>
	 * This method is intentionally conservative: it returns {@code true} only when Pyranid recognizes a database-specific
	 * error code or SQLState as a serialization failure.
	 *
	 * @return {@code true} if this exception is recognized as a serialization failure
	 * @since 4.4.0
	 */
	@NonNull
	public Boolean isSerializationFailure() {
		return this.serializationFailure;
	}

	/**
	 * Determines if this exception is recognized as a timeout or cancellation.
	 * <p>
	 * This method is intentionally conservative: it returns {@code true} only when Pyranid recognizes a JDBC timeout,
	 * database-specific timeout, or database-specific cancellation signal. A {@code true} result does not mean the operation
	 * is automatically safe to retry.
	 *
	 * @return {@code true} if this exception is recognized as a timeout or cancellation
	 * @since 4.4.0
	 */
	@NonNull
	public Boolean isTimeout() {
		return this.timeout;
	}

	/**
	 * @return the value of the offending {@code column}, or empty if not available
	 * @since 1.0.12
	 */
	@NonNull
	public Optional<String> getColumn() {
		return Optional.ofNullable(this.column);
	}

	/**
	 * @return the value of the offending {@code constraint}, or empty if not available
	 * @since 1.0.12
	 */
	@NonNull
	public Optional<String> getConstraint() {
		return Optional.ofNullable(this.constraint);
	}

	/**
	 * @return the value of the offending {@code datatype}, or empty if not available
	 * @since 1.0.12
	 */
	@NonNull
	public Optional<String> getDatatype() {
		return Optional.ofNullable(this.datatype);
	}

	/**
	 * @return the value of the offending {@code detail}, or empty if not available
	 * @since 1.0.12
	 */
	@NonNull
	public Optional<String> getDetail() {
		return Optional.ofNullable(this.detail);
	}

	/**
	 * @return the value of the offending {@code file}, or empty if not available
	 * @since 1.0.12
	 */
	@NonNull
	public Optional<String> getFile() {
		return Optional.ofNullable(this.file);
	}

	/**
	 * @return the value of the error {@code hint}, or empty if not available
	 * @since 1.0.12
	 */
	@NonNull
	public Optional<String> getHint() {
		return Optional.ofNullable(this.hint);
	}

	/**
	 * @return the value of the offending {@code internalPosition}, or empty if not available
	 * @since 1.0.12
	 */
	@NonNull
	public Optional<Integer> getInternalPosition() {
		return Optional.ofNullable(this.internalPosition);
	}

	/**
	 * @return the value of the offending {@code internalQuery}, or empty if not available
	 * @since 1.0.12
	 */
	@NonNull
	public Optional<String> getInternalQuery() {
		return Optional.ofNullable(this.internalQuery);
	}

	/**
	 * @return the value of the offending {@code line}, or empty if not available
	 * @since 1.0.12
	 */
	@NonNull
	public Optional<Integer> getLine() {
		return Optional.ofNullable(this.line);
	}

	/**
	 * @return the value of the error {@code dbmsMessage}, or empty if not available
	 * @since 1.0.12
	 */
	@NonNull
	public Optional<String> getDbmsMessage() {
		return Optional.ofNullable(this.dbmsMessage);
	}

	/**
	 * @return the value of the offending {@code position}, or empty if not available
	 * @since 1.0.12
	 */
	@NonNull
	public Optional<Integer> getPosition() {
		return Optional.ofNullable(this.position);
	}

	/**
	 * @return the value of the offending {@code routine}, or empty if not available
	 * @since 1.0.12
	 */
	@NonNull
	public Optional<String> getRoutine() {
		return Optional.ofNullable(this.routine);
	}

	/**
	 * @return the value of the offending {@code schema}, or empty if not available
	 * @since 1.0.12
	 */
	@NonNull
	public Optional<String> getSchema() {
		return Optional.ofNullable(this.schema);
	}

	/**
	 * @return the error {@code severity}, or empty if not available
	 * @since 1.0.12
	 */
	@NonNull
	public Optional<String> getSeverity() {
		return Optional.ofNullable(this.severity);
	}

	/**
	 * @return the value of the offending {@code table}, or empty if not available
	 * @since 1.0.12
	 */
	@NonNull
	public Optional<String> getTable() {
		return Optional.ofNullable(this.table);
	}

	/**
	 * @return the value of the offending {@code where}, or empty if not available
	 * @since 1.0.12
	 */
	@NonNull
	public Optional<String> getWhere() {
		return Optional.ofNullable(this.where);
	}
}
