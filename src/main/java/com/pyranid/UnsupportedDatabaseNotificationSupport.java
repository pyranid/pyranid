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

import static java.util.Objects.requireNonNull;

/**
 * Notification behavior for dialects which do not support notifications.
 */
final class UnsupportedDatabaseNotificationSupport implements DatabaseNotificationSupport {
	@NonNull
	static final UnsupportedDatabaseNotificationSupport INSTANCE = new UnsupportedDatabaseNotificationSupport();

	private UnsupportedDatabaseNotificationSupport() {}

	@Override
	public boolean isSendSupported() {
		return false;
	}

	@Override
	public boolean isReceiveRuntimeAvailable() {
		return false;
	}

	@Override
	public void validateChannel(@NonNull String channel) {
		Notification.validateChannel(channel);
	}

	@Override
	public void validatePayload(@Nullable String payload) {}

	@NonNull
	@Override
	public String sendStatementSql() {
		throw new UnsupportedOperationException("Database notifications are not supported by this dialect");
	}

	@NonNull
	@Override
	public NotificationTransport open(@NonNull Connection connection)
			throws NotificationReceiveUnsupportedException {
		requireNonNull(connection);
		throw new NotificationReceiveUnsupportedException(
				"Database notification listening is not supported by this dialect");
	}
}
