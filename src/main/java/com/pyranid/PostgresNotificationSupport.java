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

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.SQLException;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * PostgreSQL notification behavior whose send path does not require pgjdbc notification types.
 */
final class PostgresNotificationSupport implements DatabaseNotificationSupport {
	@NonNull
	static final PostgresNotificationSupport INSTANCE = new PostgresNotificationSupport();
	private static final int MAX_CHANNEL_BYTES = 63;
	private static final int MAX_PAYLOAD_BYTES = 7_999;
	@NonNull
	private static final String SEND_STATEMENT_SQL = "SELECT pg_notify(?, ?)";

	private PostgresNotificationSupport() {}

	@Override
	public boolean isSendSupported() {
		return true;
	}

	@Override
	public boolean isReceiveRuntimeAvailable() {
		return PostgresNotificationAdapterLoader.isAvailable();
	}

	@Override
	public void validateChannel(@NonNull String channel) {
		Notification.validateChannel(channel);

		int byteLength = channel.getBytes(StandardCharsets.UTF_8).length;

		if (byteLength > MAX_CHANNEL_BYTES)
			throw new IllegalArgumentException(format(
					"PostgreSQL notification channel must be at most %s UTF-8 bytes, but was %s",
					MAX_CHANNEL_BYTES, byteLength));
	}

	@Override
	public void validatePayload(@NonNull String payload) {
		requireNonNull(payload);

		if (payload.indexOf('\0') >= 0)
			throw new IllegalArgumentException("PostgreSQL notification payload must not contain a NUL character");

		int byteLength = payload.getBytes(StandardCharsets.UTF_8).length;

		if (byteLength > MAX_PAYLOAD_BYTES)
			throw new IllegalArgumentException(format(
					"PostgreSQL notification payload must be at most %s UTF-8 bytes, but was %s",
					MAX_PAYLOAD_BYTES, byteLength));
	}

	@NonNull
	@Override
	public String sendStatementSql() {
		return SEND_STATEMENT_SQL;
	}

	@NonNull
	@Override
	public NotificationTransport open(@NonNull Connection connection)
			throws SQLException, NotificationReceiveUnsupportedException {
		return PostgresNotificationAdapterLoader.open(requireNonNull(connection));
	}
}
