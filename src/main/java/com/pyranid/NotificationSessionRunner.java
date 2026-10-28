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

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.time.Duration;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import static java.lang.String.format;
import static java.lang.System.nanoTime;
import static java.util.Objects.requireNonNull;

/** Owns one callback-scoped listener's setup, failure precedence and connection cleanup. */
final class NotificationSessionRunner {
	private NotificationSessionRunner() {}

	static void run(@NonNull Database database, @NonNull Set<@NonNull String> channels,
			@NonNull NotificationSessionOperation operation) throws InterruptedException {
		requireNonNull(database);
		requireNonNull(channels);
		requireNonNull(operation);

		if (channels.isEmpty())
			throw new IllegalArgumentException("channels must not be empty");

		Set<String> validatedChannels = new LinkedHashSet<>(channels.size());

		for (String channel : channels) {
			Notification.validateChannel(channel);
			validatedChannels.add(channel);
		}

		if (Database.hasAmbientTransaction())
			throw new IllegalStateException("Notification sessions are not permitted inside a Pyranid transaction");

		if (Thread.interrupted())
			throw new InterruptedException();

		DatabaseType databaseType = database.getDatabaseType();
		DatabaseNotificationSupport notificationSupport = database.getDatabaseDialect().notificationSupport();

		for (String channel : validatedChannels)
			notificationSupport.validateChannel(channel);

		if (Thread.interrupted())
			throw new InterruptedException();

		performNotificationSession(database, Set.copyOf(validatedChannels), operation, databaseType, notificationSupport);
	}

	private static void performNotificationSession(@NonNull Database database,
			@NonNull Set<@NonNull String> channels,
																					@NonNull NotificationSessionOperation operation,
																					@NonNull DatabaseType databaseType,
																					@NonNull DatabaseNotificationSupport notificationSupport)
			throws InterruptedException {
		requireNonNull(channels);
		requireNonNull(operation);
		requireNonNull(databaseType);
		requireNonNull(notificationSupport);

		MetricsCollectorDispatcher metricsCollectorDispatcher = database.getMetricsCollectorDispatcher();
		UUID notificationSessionId = metricsCollectorDispatcher.isEnabled() ? UUID.randomUUID() : null;
		long lifecycleStartTime = notificationSessionId == null ? 0L : nanoTime();

		if (notificationSessionId != null)
			metricsCollectorDispatcher.willOpenNotificationSession(databaseType, notificationSessionId);

		Connection connection = null;
		NotificationTransport transport = null;
		NotificationSession session = null;
		Boolean initialAutoCommit = null;
		NotificationSetupPhase setupPhase = NotificationSetupPhase.CAPABILITY;
		UnsupportedOperationException frameworkUnsupportedFailure = null;
		AtomicReference<SQLException> registrationStatementFailure = new AtomicReference<>();
		boolean initialSanitationComplete = false;

		try {
			if (!notificationSupport.isReceiveRuntimeAvailable()) {
				frameworkUnsupportedFailure = new UnsupportedOperationException(format(
						"Database type %s does not support notification listening with the current runtime", databaseType));
				throw frameworkUnsupportedFailure;
			}

			setupPhase = NotificationSetupPhase.ACQUIRE_CONNECTION;
			connection = requireNonNull(database.getDataSource().getConnection(), "DataSource returned a null connection");

			setupPhase = NotificationSetupPhase.CONFIGURE_CONNECTION;
			initialAutoCommit = connection.getAutoCommit();

			if (!initialAutoCommit) {
				// A pooled connection can be returned with an unfinished transaction. Discard inherited work before
				// enabling autocommit; LISTEN registration must not inherit that transaction.
				connection.rollback();
				connection.setAutoCommit(true);
			}

			setupPhase = NotificationSetupPhase.OPEN_TRANSPORT;
			transport = notificationSupport.open(connection);

			setupPhase = NotificationSetupPhase.SANITIZE_UNLISTEN;
			transport.unlistenAll();

			setupPhase = NotificationSetupPhase.SANITIZE_DRAIN;
			requireNonNull(transport.drain(), "Notification transport returned a null batch");

			if (transport.isConnectionUncertain())
				throw new SQLException("Notification transport became uncertain during initial sanitation");

			initialSanitationComplete = true;
			setupPhase = NotificationSetupPhase.REGISTER;
			transport.listen(channels, registrationStatementFailure::set);

			if (transport.isConnectionUncertain())
				throw new SQLException("Notification transport became uncertain during registration");

			session = new NotificationSession(
					database, requireNonNull(transport), databaseType, notificationSessionId);
		} catch (Throwable setupCause) {
			boolean connectionFailure = isNotificationConnectionFailure(connection, setupCause);
			Throwable setupFailure = normalizeNotificationSetupFailure(
					setupCause, frameworkUnsupportedFailure, registrationStatementFailure.get(),
					setupPhase, connectionFailure, databaseType);

			if (notificationSessionId != null) {
				metricsCollectorDispatcher.didFailToOpenNotificationSession(
						databaseType, notificationSessionId,
						Duration.ofNanos(nanoTime() - lifecycleStartTime), setupFailure);
			}

			boolean interruptionObserved = Thread.interrupted();
			// Synthetic uncertainty failures need not carry a connection-class SQLSTATE. Cleanup still fails closed:
			// this independent transport-state check prevents reuse even if generic exception classification does not.
			boolean canAttemptHealthyCleanup = initialSanitationComplete
					&& connection != null
					&& transport != null
					&& initialAutoCommit != null
					&& setupPhase == NotificationSetupPhase.REGISTER
					&& !connectionFailure
					&& !transport.isConnectionUncertain();
			Throwable cleanupFailure = canAttemptHealthyCleanup
					? cleanupOpenedNotificationSession(connection, transport, initialAutoCommit, false, databaseType)
					: cleanupFailedNotificationCandidate(connection, databaseType);
			setupFailure = appendSuppressedIfPresent(setupFailure, cleanupFailure);

			if (Thread.interrupted())
				interruptionObserved = true;

			if (interruptionObserved)
				Thread.currentThread().interrupt();

			throwNotificationFailure(setupFailure);
			return;
		}

		if (notificationSessionId != null) {
			metricsCollectorDispatcher.didOpenNotificationSession(
					databaseType, notificationSessionId,
					Duration.ofNanos(nanoTime() - lifecycleStartTime));
		}

		session = requireNonNull(session);
		Throwable callbackFailure = null;
		boolean callbackFailureIsError = false;
		InterruptedException concreteInterruptedException = null;
		boolean interruptionObserved = false;

		try {
			if (Thread.interrupted()) {
				interruptionObserved = true;
			} else {
				operation.perform(session);
			}
		} catch (InterruptedException interruptedException) {
			concreteInterruptedException = interruptedException;
			interruptionObserved = true;
			// Retain the exact exception while clearing any simultaneously pending status before cleanup.
			Thread.interrupted();
		} catch (DatabaseException databaseException) {
			callbackFailure = databaseException;
		} catch (Error error) {
			callbackFailure = error;
			callbackFailureIsError = true;
		} catch (Throwable throwable) {
			callbackFailure = normalizeNotificationFailure(
					"Notification session operation failed", throwable, databaseType);
		} finally {
			session.expire();
		}

		Throwable transportFailure = session.terminalFailure();
		Throwable primaryFailure;

		if (callbackFailureIsError) {
			primaryFailure = appendSuppressedIfPresent(callbackFailure, transportFailure);
		} else if (transportFailure != null) {
			primaryFailure = appendSuppressedIfPresent(transportFailure, callbackFailure);
		} else {
			primaryFailure = callbackFailure;
		}

		// Cleanup begins flag-clear with respect to status already pending at this boundary.
		if (Thread.interrupted())
			interruptionObserved = true;

		Throwable cleanupFailure = cleanupOpenedNotificationSession(
				requireNonNull(connection), requireNonNull(transport), requireNonNull(initialAutoCommit),
				session.isConnectionUncertain(), databaseType);
		primaryFailure = appendSuppressedIfPresent(primaryFailure, cleanupFailure);

		if (Thread.interrupted())
			interruptionObserved = true;

		MetricsCollector.NotificationSessionOutcome outcome;

		if (primaryFailure != null)
			outcome = MetricsCollector.NotificationSessionOutcome.FAILED;
		else if (interruptionObserved)
			outcome = MetricsCollector.NotificationSessionOutcome.INTERRUPTED;
		else
			outcome = MetricsCollector.NotificationSessionOutcome.CALLBACK_RETURNED;

		if (notificationSessionId != null) {
			metricsCollectorDispatcher.didCloseNotificationSession(
					databaseType, notificationSessionId, outcome,
					Duration.ofNanos(nanoTime() - lifecycleStartTime),
					outcome == MetricsCollector.NotificationSessionOutcome.FAILED ? primaryFailure : null);
		}

		if (primaryFailure != null) {
			if (interruptionObserved) {
				primaryFailure = appendSuppressedIfPresent(primaryFailure, concreteInterruptedException);
				Thread.currentThread().interrupt();
			}

			throwNotificationFailure(primaryFailure);
			return;
		}

		if (interruptionObserved)
			throw concreteInterruptedException == null ? new InterruptedException() : concreteInterruptedException;
	}

	@NonNull
	private static Throwable normalizeNotificationSetupFailure(@NonNull Throwable setupCause,
																					 @Nullable UnsupportedOperationException frameworkUnsupportedFailure,
																					 @Nullable SQLException registrationStatementFailure,
																					 @NonNull NotificationSetupPhase setupPhase,
																											 boolean connectionFailure,
																											 @NonNull DatabaseType databaseType) {
		requireNonNull(setupCause);
		requireNonNull(setupPhase);
		requireNonNull(databaseType);

		if (setupCause == frameworkUnsupportedFailure)
			return setupCause;

		if (setupCause instanceof NotificationReceiveUnsupportedException && !connectionFailure) {
			return new UnsupportedOperationException(
					"Notification listening is not supported by the current database driver connection", setupCause);
		}

		if (setupPhase == NotificationSetupPhase.OPEN_TRANSPORT
				&& !connectionFailure
				&& (setupCause instanceof SQLFeatureNotSupportedException
				|| setupCause instanceof UnsupportedOperationException
				|| setupCause instanceof AbstractMethodError)) {
			return new UnsupportedOperationException(
					"Notification listening is not supported by the current database driver connection", setupCause);
		}

		// Pyranid generates the registration SQL itself and validates every channel first. Classify only the
		// exact SQLException raised by Statement.execute(); statement creation and close failures remain JDBC errors.
		if (setupPhase == NotificationSetupPhase.REGISTER
				&& setupCause == registrationStatementFailure
				&& !isNotificationStatementConnectionFailure(setupCause)
				&& isNotificationStatementUnsupported(setupCause)) {
			return new UnsupportedOperationException(
					notificationServerCapabilityUnavailableMessage(databaseType), setupCause);
		}

		return normalizeNotificationFailure("Unable to open notification session", setupCause, databaseType);
	}

	/**
	 * Whether {@code throwable} carries a SQLState indicating the server cannot run a notification
	 * statement Pyranid generated. Observed across PostgreSQL-compatible engines that omit the feature:
	 * {@code 0A000} (feature not supported - YugabyteDB reports "LISTEN/NOTIFY is disabled"),
	 * {@code 42601} (syntax error - CockroachDB does not parse LISTEN or NOTIFY at all), and
	 * {@code 42883} (undefined function, which is how a missing {@code pg_notify} surfaces).
	 */
	static boolean isNotificationStatementUnsupported(@Nullable Throwable throwable) {
		// A generated statement can carry server details through either causes or JDBC next exceptions.
		// Suppressed exceptions belong to statement cleanup and must not reclassify the execution failure.
		return GenericDialect.INSTANCE.hasSqlException(throwable, sqlException -> {
			try {
				String sqlState = sqlException.getSQLState();
				return "0A000".equals(sqlState) || "42601".equals(sqlState) || "42883".equals(sqlState);
			} catch (Throwable inspectionFailure) {
				// Diagnostics must never replace the statement failure or skip connection cleanup.
				return false;
			}
		});
	}

	@NonNull
	static String notificationServerCapabilityUnavailableMessage(@NonNull DatabaseType databaseType) {
		requireNonNull(databaseType);
		return format("Database type %s is configured or detected, but LISTEN/NOTIFY is unavailable. "
				+ "It may be unsupported by the current server version or disabled in its current configuration.",
				databaseType);
	}

	private static boolean isNotificationStatementConnectionFailure(Throwable failure) {
		// Cleanup may make disposal unsafe without changing what the LISTEN execution reported.
		// In particular, a suppressed close failure must not turn an unsupported LISTEN into a retry.
		return GenericDialect.INSTANCE.hasSqlException(failure, sqlException -> {
			try {
				String sqlState = sqlException.getSQLState();
				return sqlState != null && sqlState.startsWith("08");
			} catch (Throwable inspectionFailure) {
				return true;
			}
		});
	}

	private static boolean isNotificationConnectionFailure(@Nullable Connection connection,
			@NonNull Throwable throwable) {
		requireNonNull(throwable);

		// If failure details cannot be inspected, fail closed instead of returning a possibly broken handle.
		if (ThrowableGraph.inspect(throwable, current -> {
			if (!(current instanceof SQLException sqlException))
				return 0;
			String sqlState = sqlException.getSQLState();
			return sqlState != null && sqlState.startsWith("08") ? 1 : 0;
		}, 1) != 0)
			return true;

		if (connection != null) {
			try {
				return connection.isClosed();
			} catch (Throwable diagnosticFailure) {
				if (diagnosticFailure != throwable)
					throwable.addSuppressed(diagnosticFailure);

				// If connection health cannot be inspected, never return the candidate to a pool as healthy.
				return true;
			}
		}

		return false;
	}

	@Nullable
	private static Throwable cleanupFailedNotificationCandidate(@Nullable Connection connection,
																											 @NonNull DatabaseType databaseType) {
		requireNonNull(databaseType);

		if (connection == null)
			return null;

		Throwable cleanupFailure = null;

		try {
			connection.abort(Runnable::run);
		} catch (Throwable throwable) {
			cleanupFailure = appendSuppressed(cleanupFailure,
					normalizeConnectionCleanupFailure("Unable to abort failed notification-session connection",
							throwable, databaseType));
		}

		try {
			connection.close();
		} catch (Throwable throwable) {
			cleanupFailure = appendSuppressed(cleanupFailure,
					normalizeConnectionCleanupFailure("Unable to close failed notification-session connection",
							throwable, databaseType));
		}

		return cleanupFailure;
	}

	@Nullable
	private static Throwable cleanupOpenedNotificationSession(@NonNull Connection connection,
																										 @NonNull NotificationTransport transport,
																										 @NonNull Boolean initialAutoCommit,
																										 boolean connectionUncertain,
																										 @NonNull DatabaseType databaseType) {
		requireNonNull(connection);
		requireNonNull(transport);
		requireNonNull(initialAutoCommit);
		requireNonNull(databaseType);

		Throwable cleanupFailure = null;
		boolean abortRequired = connectionUncertain;

		if (!abortRequired) {
			try {
				transport.unlistenAllForCleanup();
			} catch (Throwable throwable) {
				cleanupFailure = appendSuppressed(cleanupFailure,
						normalizeNotificationFailure("Unable to unregister notification channels", throwable, databaseType));
				abortRequired = true;
			}
		}

		if (!abortRequired) {
			try {
				requireNonNull(transport.drain(), "Notification transport returned a null cleanup batch");

				if (transport.isConnectionUncertain())
					throw new SQLException("Notification transport became uncertain during cleanup");
			} catch (Throwable throwable) {
				cleanupFailure = appendSuppressed(cleanupFailure,
						normalizeNotificationFailure("Unable to drain notification connection during cleanup",
								throwable, databaseType));
				abortRequired = true;
			}
		}

		if (!abortRequired && !initialAutoCommit) {
			try {
				connection.setAutoCommit(false);
			} catch (Throwable throwable) {
				cleanupFailure = appendSuppressed(cleanupFailure,
						normalizeNotificationFailure("Unable to restore notification connection autocommit",
								throwable, databaseType));
				abortRequired = true;
			}
		}

		if (!abortRequired) {
			try {
				connection.close();
			} catch (Throwable throwable) {
				cleanupFailure = appendSuppressed(cleanupFailure,
						normalizeConnectionCleanupFailure("Unable to close notification-session connection",
								throwable, databaseType));
				// Once close has been invoked, the connection handle might already have been returned to a pool.
				// Do not attempt abort or a second close through a handle whose ownership is now unknown.
			}
		}

		if (abortRequired) {
			try {
				connection.abort(Runnable::run);
			} catch (Throwable throwable) {
				cleanupFailure = appendSuppressed(cleanupFailure,
						normalizeConnectionCleanupFailure("Unable to abort uncertain notification-session connection",
								throwable, databaseType));
			}

			try {
				connection.close();
			} catch (Throwable throwable) {
				cleanupFailure = appendSuppressed(cleanupFailure,
						normalizeConnectionCleanupFailure("Unable to close uncertain notification-session connection",
								throwable, databaseType));
			}
		}

		return cleanupFailure;
	}

	@NonNull
	private static Throwable normalizeConnectionCleanupFailure(@NonNull String message,
			@NonNull Throwable throwable, @NonNull DatabaseType databaseType) {
		// Preserve fatal identity; the supervisor always scans errors, regardless of their origin.
		if (throwable instanceof Error)
			return throwable;
		try {
			return new NotificationConnectionCleanupException(message, throwable, databaseType.dialect());
		} catch (Throwable inspectionFailure) {
			appendSuppressed(throwable, inspectionFailure);
			return new NotificationConnectionCleanupException(message, throwable, databaseType.dialect(), false);
		}
	}

	@NonNull
	private static Throwable normalizeNotificationFailure(@NonNull String message,
																							 @NonNull Throwable throwable,
																							 @NonNull DatabaseType databaseType) {
		requireNonNull(message);
		requireNonNull(throwable);
		requireNonNull(databaseType);

		if (throwable instanceof DatabaseException || throwable instanceof Error)
			return throwable;

		try {
			return new DatabaseException(message, throwable, databaseType.dialect());
		} catch (Throwable inspectionFailure) {
			appendSuppressed(throwable, inspectionFailure);
			return new DatabaseException(message, throwable, databaseType.dialect(), null, false);
		}
	}

	private static void throwNotificationFailure(@NonNull Throwable throwable) {
		requireNonNull(throwable);

		if (throwable instanceof RuntimeException runtimeException)
			throw runtimeException;

		if (throwable instanceof Error error)
			throw error;

		throw new DatabaseException("Notification session failed", throwable);
	}

	@Nullable
	private static Throwable appendSuppressedIfPresent(@Nullable Throwable existing,
																										@Nullable Throwable additional) {
		if (additional == null)
			return existing;

		return appendSuppressed(existing, additional);
	}

	private enum NotificationSetupPhase {
		CAPABILITY,
		ACQUIRE_CONNECTION,
		CONFIGURE_CONNECTION,
		OPEN_TRANSPORT,
		SANITIZE_UNLISTEN,
		SANITIZE_DRAIN,
		REGISTER
	}

	@NonNull
	private static Throwable appendSuppressed(@Nullable Throwable existing,
																						@NonNull Throwable additional) {
		requireNonNull(additional);

		if (existing == null)
			return additional;

		if (existing != additional)
			existing.addSuppressed(additional);

		return existing;
	}

}
