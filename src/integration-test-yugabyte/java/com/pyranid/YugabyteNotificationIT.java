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

import com.yugabyte.ds.PGSimpleDataSource;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.YugabyteDBYSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import javax.sql.DataSource;
import java.sql.Connection;
import java.time.Duration;
import java.util.List;
import java.util.UUID;

/**
 * Advisory compatibility coverage for YugabyteDB's smart JDBC driver and Early Access notification support.
 *
 * @author <a href="https://www.revetkn.com">Mark Allen</a>
 * @since 4.7.0
 */
@Testcontainers
public class YugabyteNotificationIT {
	private static final String YUGABYTE_IMAGE_NAME =
			System.getProperty("yugabyte.integration.image", "yugabytedb/yugabyte:2026.1.1.1-b2");
	private static final String YUGABYTE_COMMAND = "bin/yugabyted start --background=false "
			+ "--master_flags=ysql_yb_enable_listen_notify=true "
			+ "--tserver_flags=ysql_yb_enable_listen_notify=true";

	@Container
	private static final YugabyteDBYSQLContainer YUGABYTE =
			new YugabyteDBYSQLContainer(YUGABYTE_IMAGE_NAME)
					.withCommand(YUGABYTE_COMMAND)
					.withStartupTimeout(Duration.ofMinutes(3));

	@Test
	public void testSmartDriverJsonAndNotificationsEndToEnd() throws Exception {
		DataSource dataSource = dataSource();

		try (Connection connection = dataSource.getConnection()) {
			Assertions.assertTrue(connection.getClass().getName().startsWith("com.yugabyte."),
					"The advisory leg must exercise YugabyteDB's smart driver, not stock pgjdbc");
		}

		Database database = Database.withDataSource(dataSource)
				.databaseType(DatabaseType.POSTGRESQL)
				.build();
		String suffix = UUID.randomUUID().toString().replace("-", "");
		String table = "pyranid_yb_json_" + suffix;
		String channel = "pyranid_yb_notify_" + suffix;
		String json = "{\"kind\":\"yugabyte\",\"count\":3}";

		awaitNotificationInfrastructure(database, channel + "_ready");

		try {
			database.query("CREATE TABLE " + table + " (id INTEGER PRIMARY KEY, payload JSONB NOT NULL)")
					.execute();
			database.query("INSERT INTO " + table + " (id, payload) VALUES (1, :payload)")
					.bind("payload", Parameters.json(json))
					.execute();

			String returnedJson = database.query("SELECT payload FROM " + table + " WHERE id = 1")
					.fetchObject(String.class)
					.orElseThrow();
			Assertions.assertTrue(returnedJson.contains("\"kind\": \"yugabyte\"")
						|| returnedJson.contains("\"kind\":\"yugabyte\""), returnedJson);

			database.withNotificationSession(channel, session -> {
				database.sendNotification(channel, "smart-driver");
				Assertions.assertEquals(
						Notification.of(channel, "smart-driver"),
						awaitNotification(session, channel, "smart-driver", Duration.ofSeconds(20)));
			});
		} finally {
			database.query("DROP TABLE IF EXISTS " + table).execute();
		}
	}

	private static DataSource dataSource() {
		PGSimpleDataSource dataSource = new PGSimpleDataSource();
		dataSource.setURL(YUGABYTE.getJdbcUrl());
		dataSource.setUser(YUGABYTE.getUsername());
		dataSource.setPassword(YUGABYTE.getPassword());
		return dataSource;
	}

	private static void awaitNotificationInfrastructure(Database database,
																								 String channel) throws InterruptedException {
		long deadline = System.nanoTime() + Duration.ofSeconds(45).toNanos();
		Throwable lastFailure = null;

		while (System.nanoTime() < deadline) {
			try {
				database.withNotificationSession(channel, session -> {});
				return;
			} catch (DatabaseException | UnsupportedOperationException exception) {
				lastFailure = exception;
				Thread.sleep(250L);
			}
		}

		AssertionError error = new AssertionError("YugabyteDB notification infrastructure did not become ready");
		if (lastFailure != null)
			error.initCause(lastFailure);
		throw error;
	}

	private static Notification awaitNotification(NotificationSession session,
																					 String channel,
																					 String payload,
																					 Duration timeout) throws InterruptedException {
		long deadline = System.nanoTime() + timeout.toNanos();

		while (System.nanoTime() < deadline) {
			List<Notification> notifications = session.awaitNotifications(Duration.ofMillis(250));

			for (Notification notification : notifications)
				if (channel.equals(notification.getChannel()) && payload.equals(notification.getPayload()))
					return notification;
		}

		return Assertions.fail("Timed out waiting for YugabyteDB notification on channel " + channel);
	}
}
