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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

import javax.sql.DataSource;
import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.UUID;

/**
 * Notification deployment-topology integration tests.
 *
 * <p>PgBouncer transaction and statement pooling are intentionally not exercised as listener sources:
 * PostgreSQL documents {@code LISTEN} as unsupported in those modes, and Pyranid does not promise to diagnose
 * or deterministically reject that deployment error.</p>
 *
 * @author <a href="https://www.revetkn.com">Mark Allen</a>
 * @since 4.6.0
 */
@Testcontainers
public class PostgreSqlNotificationTopologyIT {
	private static final String POSTGRES_IMAGE_NAME =
			System.getProperty("postgres.integration.image", "pgvector/pgvector:pg17");
	private static final String PGBOUNCER_IMAGE_NAME =
			System.getProperty("pgbouncer.notification.topology.image", "edoburu/pgbouncer:v1.25.2-p0");
	private static final DockerImageName POSTGRES_IMAGE = DockerImageName.parse(POSTGRES_IMAGE_NAME)
			.asCompatibleSubstituteFor("postgres");
	private static final DockerImageName PGBOUNCER_IMAGE = DockerImageName.parse(PGBOUNCER_IMAGE_NAME);

	@Container
	private static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer(POSTGRES_IMAGE)
			.withNetwork(Network.SHARED)
			.withNetworkAliases("notification-primary")
			.withDatabaseName("pyranid")
			.withUsername("pyranid")
			.withPassword("pyranid");

	@Container
	private static final GenericContainer<?> SESSION_POOL = pgbouncer(
			"pgbouncer/session-pooling.ini",
			"notification-session-pool");

	@Container
	private static final GenericContainer<?> TRANSACTION_POOL = pgbouncer(
			"pgbouncer/transaction-pooling.ini",
			"notification-transaction-pool");

	@Test
	public void testSessionPooledListenerReceivesNotificationFromSamePrimary() throws InterruptedException {
		Assertions.assertEquals("session", poolMode(SESSION_POOL));

		Database listenerDatabase = database(dataSource(SESSION_POOL, "pyranid"));
		Database publisherDatabase = database(directDataSource());
		String channel = uniqueName("session_listener");
		String payload = "session-pooled-delivery";

		listenerDatabase.withNotificationSession(channel, session -> {
			publisherDatabase.sendNotification(channel, payload);
			Assertions.assertEquals(
					Notification.of(channel, payload),
					awaitNotification(session, channel, payload, Duration.ofSeconds(5)));
		});
	}

	@Test
	public void testTransactionPooledApplicationWorksWithDistinctSessionPooledListener()
			throws InterruptedException {
		Assertions.assertEquals("transaction", poolMode(TRANSACTION_POOL));
		Assertions.assertEquals("session", poolMode(SESSION_POOL));

		Database applicationDatabase = database(dataSource(TRANSACTION_POOL, "pyranid"));
		Database listenerDatabase = database(dataSource(SESSION_POOL, "pyranid"));
		String table = uniqueName("notification_state");
		String channel = uniqueName("state_changed");
		String payload = "committed-through-transaction-pool";

		applicationDatabase.query("CREATE TABLE " + table + " ("
				+ "id BIGINT PRIMARY KEY, "
				+ "value TEXT NOT NULL"
				+ ")").execute();

		Assertions.assertEquals(Boolean.FALSE, applicationDatabase.query("SELECT pg_is_in_recovery()")
				.fetchObject(Boolean.class)
				.orElseThrow());
		Assertions.assertEquals(Boolean.FALSE, listenerDatabase.query("SELECT pg_is_in_recovery()")
				.fetchObject(Boolean.class)
				.orElseThrow());

		listenerDatabase.withNotificationSession(channel, session -> {
			Assertions.assertEquals(
					Long.valueOf(0L),
					applicationDatabase.query("SELECT COUNT(*) FROM " + table)
							.fetchObject(Long.class)
							.orElseThrow());

			applicationDatabase.transaction(() -> {
				applicationDatabase.query("INSERT INTO " + table + " (id, value) VALUES (:id, :value)")
						.bind("id", 1L)
						.bind("value", payload)
						.execute();
				applicationDatabase.sendNotification(channel, payload);
			});

			Assertions.assertEquals(
					Notification.of(channel, payload),
					awaitNotification(session, channel, payload, Duration.ofSeconds(5)));
			Assertions.assertEquals(
					payload,
					applicationDatabase.query("SELECT value FROM " + table + " WHERE id = :id")
							.bind("id", 1L)
							.fetchObject(String.class)
							.orElseThrow());
		});
	}

	@NonNull
	private static GenericContainer<?> pgbouncer(@NonNull String configurationResource,
												  @NonNull String networkAlias) {
		return new GenericContainer<>(PGBOUNCER_IMAGE)
				.withNetwork(Network.SHARED)
				.withNetworkAliases(networkAlias)
				.withExposedPorts(5432)
				.withCopyFileToContainer(
						MountableFile.forClasspathResource(configurationResource),
						"/etc/pgbouncer/pgbouncer.ini")
				.withCopyFileToContainer(
						MountableFile.forClasspathResource("pgbouncer/userlist.txt"),
						"/etc/pgbouncer/userlist.txt")
				.dependsOn(POSTGRES)
				.waitingFor(Wait.forListeningPort().withStartupTimeout(Duration.ofSeconds(60)));
	}

	@NonNull
	private static Database database(@NonNull DataSource dataSource) {
		return Database.withDataSource(dataSource)
				.databaseType(DatabaseType.POSTGRESQL)
				.build();
	}

	@NonNull
	private static DataSource directDataSource() {
		PGSimpleDataSource dataSource = new PGSimpleDataSource();
		dataSource.setURL(POSTGRES.getJdbcUrl());
		dataSource.setUser(POSTGRES.getUsername());
		dataSource.setPassword(POSTGRES.getPassword());
		return dataSource;
	}

	@NonNull
	private static DataSource dataSource(@NonNull GenericContainer<?> pgbouncer, @NonNull String databaseName) {
		PGSimpleDataSource dataSource = new PGSimpleDataSource();
		dataSource.setURL("jdbc:postgresql://%s:%d/%s?sslmode=disable".formatted(
				pgbouncer.getHost(),
				pgbouncer.getMappedPort(5432),
				databaseName));
		dataSource.setUser("pyranid");
		dataSource.setPassword("pyranid");
		return dataSource;
	}

	@NonNull
	private static String poolMode(@NonNull GenericContainer<?> pgbouncer) throws InterruptedException {
		org.testcontainers.containers.Container.ExecResult result;

		try {
			result = pgbouncer.execInContainer(
					"env",
					"PGPASSWORD=pyranid",
					"psql",
					"-h", "127.0.0.1",
					"-p", "5432",
					"-U", "pyranid",
					"-d", "pgbouncer",
					"-At",
					"-c", "SHOW CONFIG");
		} catch (IOException exception) {
			throw new AssertionError("Unable to inspect PgBouncer pool_mode", exception);
		}

		Assertions.assertEquals(0L, result.getExitCode(), result.getStderr());

		for (String row : result.getStdout().split("\\R")) {
			String[] fields = row.split("\\|", -1);

			if (fields.length >= 2 && "pool_mode".equals(fields[0]))
				return fields[1];
		}

		throw new AssertionError("PgBouncer SHOW CONFIG did not include pool_mode");
	}

	@NonNull
	private static Notification awaitNotification(@NonNull NotificationSession session,
												   @NonNull String channel,
												   @NonNull String payload,
												   @NonNull Duration timeout)
			throws InterruptedException {
		Notification expected = Notification.of(channel, payload);
		long deadline = System.nanoTime() + timeout.toNanos();

		while (System.nanoTime() < deadline) {
			List<Notification> notifications = session.awaitNotifications(Duration.ofMillis(250));

			if (notifications.contains(expected))
				return expected;
		}

		Assertions.fail("Timed out waiting for notification " + expected);
		throw new AssertionError("Unreachable");
	}

	@NonNull
	private static String uniqueName(@NonNull String prefix) {
		return prefix + "_" + UUID.randomUUID().toString().replace("-", "");
	}
}
