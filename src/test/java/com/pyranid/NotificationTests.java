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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import javax.annotation.concurrent.ThreadSafe;

/**
 * @author <a href="https://www.revetkn.com">Mark Allen</a>
 * @since 4.6.0
 */
@ThreadSafe
public class NotificationTests {
	@Test
	public void factoryValidatesCommonContract() {
		Assertions.assertThrows(NullPointerException.class, () -> Notification.of(null, ""));
		Assertions.assertThrows(NullPointerException.class, () -> Notification.of("car_changed", null));
		Assertions.assertThrows(IllegalArgumentException.class, () -> Notification.of("", ""));
		Assertions.assertThrows(IllegalArgumentException.class, () -> Notification.of(" \t\n", ""));
		Assertions.assertThrows(IllegalArgumentException.class, () -> Notification.of("car\0changed", ""));

		Notification notification = Notification.of("car_changed", "");

		Assertions.assertEquals("car_changed", notification.getChannel());
		Assertions.assertEquals("", notification.getPayload());
	}

	@Test
	public void hasValueEqualityAndHashCode() {
		Notification notification = Notification.of("car_changed", "42");
		Notification equivalent = Notification.of("car_changed", "42");

		Assertions.assertNotSame(notification, equivalent);
		Assertions.assertEquals(notification, equivalent);
		Assertions.assertEquals(notification.hashCode(), equivalent.hashCode());
		Assertions.assertNotEquals(notification, Notification.of("other_channel", "42"));
		Assertions.assertNotEquals(notification, Notification.of("car_changed", "43"));
		Assertions.assertNotEquals(notification, null);
		Assertions.assertNotEquals(notification, "car_changed");
	}

	@Test
	public void toStringNeverIncludesPayloadContents() {
		String payload = "customer-secret-token";
		Notification notification = Notification.of("car_changed", payload);
		String rendered = notification.toString();

		Assertions.assertEquals("Notification{channel=car_changed, payloadLength=" + payload.length() + "}", rendered);
		Assertions.assertFalse(rendered.contains(payload));
	}
}
