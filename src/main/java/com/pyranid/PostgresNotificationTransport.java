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
import org.postgresql.PGConnection;
import org.postgresql.PGNotification;

import java.sql.Connection;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static java.util.Objects.requireNonNull;

/**
 * pgjdbc-facing transport for one physical PostgreSQL notification session.
 */
final class PostgresNotificationTransport implements NotificationTransport {
	private static final long NANOS_PER_MILLISECOND = 1_000_000L;
	@NonNull
	private static final Duration MAX_WAIT_SLICE = Duration.ofMillis(250);
	@NonNull
	private final Connection connection;
	@NonNull
	private final PGConnection pgConnection;
	private boolean connectionUncertain;

	private PostgresNotificationTransport(@NonNull Connection connection,
																				@NonNull PGConnection pgConnection) {
		this.connection = requireNonNull(connection);
		this.pgConnection = requireNonNull(pgConnection);
	}

	@NonNull
	static NotificationTransport open(@NonNull Connection connection)
			throws SQLException, NotificationReceiveUnsupportedException {
		requireNonNull(connection);

		try {
			if (!connection.isWrapperFor(PGConnection.class))
				throw unsupported("JDBC connection does not expose PostgreSQL notification receive support", null);

			PGConnection pgConnection = connection.unwrap(PGConnection.class);

			if (pgConnection == null)
				throw unsupported("JDBC connection returned a null PostgreSQL notification receive adapter", null);

			return new PostgresNotificationTransport(connection, pgConnection);
		} catch (SQLFeatureNotSupportedException exception) {
			throw unsupported("JDBC connection does not support PostgreSQL notification receive unwrapping", exception);
		} catch (SQLException exception) {
			if (isConnectionFailure(connection, exception))
				throw exception;

			throw unsupported("JDBC connection cannot expose PostgreSQL notification receive support", exception);
		} catch (UnsupportedOperationException exception) {
			throw unsupported("JDBC connection does not support PostgreSQL notification receive unwrapping", exception);
		}
	}

	@Override
	public void listen(@NonNull Set<@NonNull String> channels) throws SQLException {
		requireNonNull(channels);

		if (channels.isEmpty())
			throw new IllegalArgumentException("channels must not be empty");

		for (String channel : channels)
			PostgresNotificationSupport.INSTANCE.validateChannel(requireNonNull(channel));

		try (Statement statement = this.connection.createStatement()) {
			for (String channel : channels)
				statement.execute("LISTEN " + quotedIdentifier(channel));
		}
	}

	@Override
	public void unlistenAll() throws SQLException {
		try (Statement statement = this.connection.createStatement()) {
			statement.execute("UNLISTEN *");
		}
	}

	@NonNull
	@Override
	public List<@NonNull Notification> receive(@NonNull Duration waitSlice) throws SQLException {
		requireNonNull(waitSlice);

		if (waitSlice.isZero() || waitSlice.isNegative())
			throw new IllegalArgumentException("waitSlice must be positive");

		if (waitSlice.compareTo(MAX_WAIT_SLICE) > 0)
			throw new IllegalArgumentException("waitSlice must not exceed 250 milliseconds");

		long waitNanos = waitSlice.toNanos();
		int waitMilliseconds = (int) Math.max(1L,
				(waitNanos + NANOS_PER_MILLISECOND - 1L) / NANOS_PER_MILLISECOND);

		return notifications(guardedReceive(() -> this.pgConnection.getNotifications(waitMilliseconds)));
	}

	@NonNull
	@Override
	public List<@NonNull Notification> drain() throws SQLException {
		return notifications(guardedReceive(this.pgConnection::getNotifications));
	}

	@Override
	public boolean isConnectionUncertain() {
		return this.connectionUncertain;
	}

	@NonNull
	private PGNotification[] guardedReceive(@NonNull DriverReceive driverReceive) throws SQLException {
		requireNonNull(driverReceive);

		Integer originalTimeout = null;
		Throwable receiveFailure = null;

		try {
			try {
				originalTimeout = this.connection.getNetworkTimeout();
			} catch (SQLFeatureNotSupportedException | UnsupportedOperationException | AbstractMethodError failure) {
				throw unsupported("JDBC connection does not support notification receive timeout guarding", failure);
			} catch (SQLException failure) {
				if (isConnectionFailure(this.connection, failure))
					throw failure;

				throw unsupported("JDBC connection cannot inspect notification receive timeout guarding", failure);
			}

			if (originalTimeout > 0) {
				try {
					this.connection.setNetworkTimeout(Runnable::run, 0);
				} catch (SQLFeatureNotSupportedException | UnsupportedOperationException | AbstractMethodError failure) {
					throw unsupported("JDBC connection does not support notification receive timeout guarding", failure);
				} catch (SQLException failure) {
					if (isConnectionFailure(this.connection, failure))
						throw failure;

					throw unsupported("JDBC connection cannot neutralize notification receive timeout guarding", failure);
				}
			}

			try {
				return driverReceive.receive();
			} catch (SQLFeatureNotSupportedException | UnsupportedOperationException | AbstractMethodError failure) {
				throw unsupported("PostgreSQL notification receive adapter is incompatible", failure);
			}
		} catch (SQLException | RuntimeException | Error failure) {
			this.connectionUncertain = true;
			receiveFailure = failure;
			throw failure;
		} finally {
			if (originalTimeout != null && originalTimeout > 0) {
				try {
					this.connection.setNetworkTimeout(Runnable::run, originalTimeout);
				} catch (SQLFeatureNotSupportedException | UnsupportedOperationException | AbstractMethodError failure) {
					NotificationReceiveUnsupportedException restoreFailure = unsupported(
							"JDBC connection does not support notification receive timeout restoration", failure);
					this.connectionUncertain = true;

					if (receiveFailure == null)
						throw restoreFailure;

					if (restoreFailure != receiveFailure)
						receiveFailure.addSuppressed(restoreFailure);
				} catch (SQLException restoreFailure) {
					this.connectionUncertain = true;

					if (restoreFailure != receiveFailure) {
						Throwable classifiedRestoreFailure = isConnectionFailure(this.connection, restoreFailure)
								? restoreFailure
								: unsupported(
										"JDBC connection cannot restore notification receive timeout guarding",
										restoreFailure);

						if (receiveFailure == null) {
							if (classifiedRestoreFailure instanceof SQLException)
								throw (SQLException) classifiedRestoreFailure;

							throw (NotificationReceiveUnsupportedException) classifiedRestoreFailure;
						}

						receiveFailure.addSuppressed(classifiedRestoreFailure);
					}
				} catch (RuntimeException | Error restoreFailure) {
					this.connectionUncertain = true;

					if (receiveFailure == null)
						throw restoreFailure;

					if (restoreFailure != receiveFailure)
						receiveFailure.addSuppressed(restoreFailure);
				}
			}
		}
	}

	@NonNull
	private static List<@NonNull Notification> notifications(PGNotification[] pgNotifications) {
		if (pgNotifications == null || pgNotifications.length == 0)
			return List.of();

		List<Notification> notifications = new ArrayList<>(pgNotifications.length);

		for (PGNotification pgNotification : pgNotifications) {
			requireNonNull(pgNotification);
			String payload = pgNotification.getParameter();
			notifications.add(Notification.of(pgNotification.getName(), payload == null ? "" : payload));
		}

		return List.copyOf(notifications);
	}

	@NonNull
	private static String quotedIdentifier(@NonNull String identifier) {
		requireNonNull(identifier);
		return '"' + identifier.replace("\"", "\"\"") + '"';
	}

	private static boolean isConnectionFailure(@NonNull Connection connection,
																						 @NonNull SQLException exception) {
		requireNonNull(connection);
		requireNonNull(exception);

		if (hasConnectionSqlState(exception))
			return true;

		try {
			return connection.isClosed();
		} catch (SQLException closedCheckFailure) {
			if (closedCheckFailure != exception)
				exception.addSuppressed(closedCheckFailure);

			return hasConnectionSqlState(closedCheckFailure);
		}
	}

	private static boolean hasConnectionSqlState(@NonNull SQLException exception) {
		requireNonNull(exception);
		String sqlState = exception.getSQLState();
		return sqlState != null && sqlState.startsWith("08");
	}

	@NonNull
	private static NotificationReceiveUnsupportedException unsupported(@NonNull String message,
																																		 @Nullable Throwable cause) {
		return new NotificationReceiveUnsupportedException(message, cause);
	}

	@FunctionalInterface
	private interface DriverReceive {
		PGNotification[] receive() throws SQLException;
	}
}
