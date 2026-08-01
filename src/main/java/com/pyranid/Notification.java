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
import java.util.Objects;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * A named database notification received by a {@link NotificationSession}.
 * <p>
 * Notifications are transient, lossy hints rather than durable events. An implementation may coalesce notifications,
 * and the number of returned {@code Notification} instances does not represent an underlying event count.
 * <p>
 * This value deliberately omits any backend-specific sender identifier. Payload nullability and null/empty-string
 * handling are database-specific; Pyranid performs no generic normalization.
 *
 * @author <a href="https://www.revetkn.com">Mark Allen</a>
 * @since 4.6.0
 */
@ThreadSafe
public final class Notification {
	@NonNull
	private final String channel;
	@Nullable
	private final String payload;

	private Notification(@NonNull String channel,
											 @Nullable String payload) {
		this.channel = requireNonNull(channel);
		this.payload = payload;
	}

	/**
	 * Creates a notification value.
	 * <p>
	 * This factory enforces the common channel contract but does not apply backend-specific byte limits.
	 *
	 * @param channel nonblank notification channel, which must not contain a NUL character
	 * @param payload notification payload, which may be null or empty
	 * @return a notification value
	 * @throws NullPointerException if {@code channel} is null
	 * @throws IllegalArgumentException if {@code channel} is blank or contains a NUL character
	 * @since 4.6.0
	 */
	@NonNull
	public static Notification of(@NonNull String channel,
																	@Nullable String payload) {
		validateChannel(channel);

		return new Notification(channel, payload);
	}

	@NonNull
	static String validateChannel(@NonNull String channel) {
		requireNonNull(channel);

		if (channel.isBlank())
			throw new IllegalArgumentException("channel must not be blank");

		if (channel.indexOf('\0') >= 0)
			throw new IllegalArgumentException("channel must not contain a NUL character");

		return channel;
	}

	/**
	 * Gets the notification channel.
	 *
	 * @return notification channel
	 * @since 4.6.0
	 */
	@NonNull
	public String getChannel() {
		return this.channel;
	}

	/**
	 * Gets the notification payload.
	 *
	 * @return notification payload, which may be null or empty according to backend behavior
	 * @since 4.6.0
	 */
	@Nullable
	public String getPayload() {
		return this.payload;
	}

	@Override
	public boolean equals(@Nullable Object object) {
		if (this == object)
			return true;

		if (!(object instanceof Notification notification))
			return false;

		return Objects.equals(getChannel(), notification.getChannel())
				&& Objects.equals(getPayload(), notification.getPayload());
	}

	@Override
	public int hashCode() {
		return Objects.hash(getChannel(), getPayload());
	}

	/**
	 * Returns a diagnostic representation containing the channel and payload length, but never the payload contents.
	 *
	 * @return diagnostic representation of this notification
	 * @since 4.6.0
	 */
	@Override
	@NonNull
	public String toString() {
		String payload = getPayload();
		String payloadLength = payload == null ? "null" : String.valueOf(payload.length());

		return format("%s{channel=%s, payloadLength=%s}",
				getClass().getSimpleName(), getChannel(), payloadLength);
	}
}
