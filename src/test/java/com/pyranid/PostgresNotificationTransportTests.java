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
import org.postgresql.PGConnection;
import org.postgresql.PGNotification;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Tests for the package-private PostgreSQL notification support and transport.
 */
public class PostgresNotificationTransportTests {
	@Test
	public void testReflectionLoaderFindsCompatibleRuntimeAndOpensTransport() throws Exception {
		TransportFixture fixture = new TransportFixture();

		Assertions.assertTrue(PostgresNotificationAdapterLoader.isAvailable());
		Assertions.assertNotNull(PostgresNotificationSupport.INSTANCE.open(fixture.connection));
	}

	@Test
	public void testPostgresValidationAndSendSql() {
		PostgresNotificationSupport support = PostgresNotificationSupport.INSTANCE;
		String sixtyThreeAsciiBytes = "a".repeat(63);
		String sixtyThreeMultibyteBytes = "é".repeat(31) + "a";
		String sevenThousandNineHundredNinetyNineAsciiBytes = "a".repeat(7_999);
		String sevenThousandNineHundredNinetyNineMultibyteBytes = "é".repeat(3_999) + "a";

		Assertions.assertDoesNotThrow(() -> support.validateChannel(sixtyThreeAsciiBytes));
		Assertions.assertDoesNotThrow(() -> support.validateChannel(sixtyThreeMultibyteBytes));
		Assertions.assertThrows(IllegalArgumentException.class,
				() -> support.validateChannel("a".repeat(64)));
		Assertions.assertThrows(IllegalArgumentException.class,
				() -> support.validateChannel("é".repeat(32)));
		Assertions.assertThrows(IllegalArgumentException.class,
				() -> support.validateChannel(" \t"));
		Assertions.assertThrows(IllegalArgumentException.class,
				() -> support.validateChannel("car\0changed"));

		Assertions.assertDoesNotThrow(() -> support.validatePayload(null));
		Assertions.assertDoesNotThrow(() -> support.validatePayload(sevenThousandNineHundredNinetyNineAsciiBytes));
		Assertions.assertDoesNotThrow(() -> support.validatePayload(sevenThousandNineHundredNinetyNineMultibyteBytes));
		Assertions.assertThrows(IllegalArgumentException.class,
				() -> support.validatePayload("a".repeat(8_000)));
		Assertions.assertThrows(IllegalArgumentException.class,
				() -> support.validatePayload("é".repeat(4_000)));
		Assertions.assertThrows(IllegalArgumentException.class,
				() -> support.validatePayload("car\0changed"));

		Assertions.assertTrue(support.isSendSupported());
		Assertions.assertEquals("SELECT pg_notify(?, ?)", support.sendStatementSql());
	}

	@Test
	public void testListenIdentifierQuotesEveryChannel() throws Exception {
		TransportFixture fixture = new TransportFixture();
		NotificationTransport transport = fixture.openTransport();
		Set<String> channels = new LinkedHashSet<>();
		channels.add("Mixed Case");
		channels.add("car\"; NOTIFY hacked; --");

		transport.listen(channels);

		Assertions.assertEquals(List.of(
				"LISTEN \"Mixed Case\"",
				"LISTEN \"car\"\"; NOTIFY hacked; --\""), fixture.executedSql);
		Assertions.assertTrue(fixture.statementClosed);
	}

	@Test
	public void testZeroNetworkTimeoutDrainUsesNoArgumentReceiveAndTimedReceiveRoundsAndBounds() throws Exception {
		TransportFixture fixture = new TransportFixture();
		fixture.networkTimeout = 0;
		fixture.drainNotifications = new PGNotification[]{new TestPgNotification("drained", "")};
		fixture.timedNotifications = new PGNotification[]{new TestPgNotification("timed", "payload")};
		NotificationTransport transport = fixture.openTransport();

		Assertions.assertEquals(List.of(Notification.of("drained", "")), transport.drain());
		Assertions.assertEquals(1, fixture.noArgumentReceiveCount);
		Assertions.assertTrue(fixture.timedWaitMilliseconds.isEmpty());

		Assertions.assertEquals(List.of(Notification.of("timed", "payload")),
				transport.receive(Duration.ofNanos(1)));
		transport.receive(Duration.ofNanos(1_000_001));
		transport.receive(Duration.ofMillis(250));

		Assertions.assertEquals(List.of(1, 2, 250), fixture.timedWaitMilliseconds);
		Assertions.assertThrows(IllegalArgumentException.class,
				() -> transport.receive(Duration.ZERO));
		Assertions.assertThrows(IllegalArgumentException.class,
				() -> transport.receive(Duration.ofNanos(-1)));
		Assertions.assertThrows(IllegalArgumentException.class,
				() -> transport.receive(Duration.ofMillis(250).plusNanos(1)));
		Assertions.assertThrows(IllegalArgumentException.class,
				() -> transport.receive(Duration.ofSeconds(Long.MAX_VALUE)));
		Assertions.assertEquals(List.of(
				"getNetworkTimeout",
				"getNotifications",
				"getNetworkTimeout",
				"getNotifications:1",
				"getNetworkTimeout",
				"getNotifications:2",
				"getNetworkTimeout",
				"getNotifications:250"), fixture.receiveEvents);
		Assertions.assertEquals(0, fixture.networkTimeout);
	}

	@Test
	public void testNullArraysAndPayloadsNormalizeWithoutChangingEncounterOrder() throws Exception {
		TransportFixture emptyFixture = new TransportFixture();
		emptyFixture.drainNotifications = null;
		NotificationTransport emptyTransport = emptyFixture.openTransport();

		Assertions.assertEquals(List.of(), emptyTransport.drain());

		TransportFixture orderedFixture = new TransportFixture();
		orderedFixture.timedNotifications = new PGNotification[]{
				new TestPgNotification("first", null),
				new TestPgNotification("second", "payload")
		};
		NotificationTransport orderedTransport = orderedFixture.openTransport();

		Assertions.assertEquals(
				List.of(
						Notification.of("first", ""),
						Notification.of("second", "payload")),
				orderedTransport.receive(Duration.ofMillis(1)));
	}

	@Test
	public void testReceiveSnapshotsZerosAndRestoresNetworkTimeout() throws Exception {
		TransportFixture fixture = new TransportFixture();
		fixture.networkTimeout = 731;
		fixture.timedNotifications = new PGNotification[]{new TestPgNotification("car_changed", "17")};
		NotificationTransport transport = fixture.openTransport();

		List<Notification> notifications = transport.receive(Duration.ofMillis(25));

		Assertions.assertEquals(List.of(Notification.of("car_changed", "17")), notifications);
		Assertions.assertEquals(List.of(
				"getNetworkTimeout",
				"setNetworkTimeout:0",
				"getNotifications:25",
				"setNetworkTimeout:731"), fixture.receiveEvents);
		Assertions.assertEquals(731, fixture.networkTimeout);
		Assertions.assertFalse(transport.isConnectionUncertain());
	}

	@Test
	public void testNonConnectionNetworkTimeoutInspectionFailureIsUnsupportedAndLatchesUncertainty()
			throws Exception {
		TransportFixture fixture = new TransportFixture();
		SQLException inspectionFailure = new SQLException("inspection");
		fixture.networkTimeoutInspectionFailure = inspectionFailure;
		NotificationTransport transport = fixture.openTransport();

		NotificationReceiveUnsupportedException thrown = Assertions.assertThrows(
				NotificationReceiveUnsupportedException.class, transport::drain);

		Assertions.assertSame(inspectionFailure, thrown.getCause());
		Assertions.assertEquals(List.of("getNetworkTimeout"), fixture.receiveEvents);
		Assertions.assertEquals(0, fixture.noArgumentReceiveCount);
		Assertions.assertTrue(transport.isConnectionUncertain());
	}

	@Test
	public void testUnsupportedNetworkTimeoutGuardUsesDedicatedCapabilityFailure() throws Exception {
		TransportFixture fixture = new TransportFixture();
		SQLFeatureNotSupportedException capabilityFailure =
				new SQLFeatureNotSupportedException("network timeout unsupported");
		fixture.networkTimeoutInspectionFailure = capabilityFailure;
		NotificationTransport transport = fixture.openTransport();

		NotificationReceiveUnsupportedException thrown = Assertions.assertThrows(
				NotificationReceiveUnsupportedException.class, transport::drain);

		Assertions.assertSame(capabilityFailure, thrown.getCause());
		Assertions.assertEquals(0, fixture.noArgumentReceiveCount);
		Assertions.assertTrue(transport.isConnectionUncertain());
	}

	@Test
	public void testNonConnectionNetworkTimeoutZeroingFailureIsUnsupportedAndRestores() throws Exception {
		TransportFixture fixture = new TransportFixture();
		fixture.networkTimeout = 419;
		SQLException zeroingFailure = new SQLException("zeroing");
		fixture.networkTimeoutZeroingFailure = zeroingFailure;
		NotificationTransport transport = fixture.openTransport();

		NotificationReceiveUnsupportedException thrown = Assertions.assertThrows(
				NotificationReceiveUnsupportedException.class, transport::drain);

		Assertions.assertSame(zeroingFailure, thrown.getCause());
		Assertions.assertEquals(List.of(
				"getNetworkTimeout",
				"setNetworkTimeout:0",
				"setNetworkTimeout:419"), fixture.receiveEvents);
		Assertions.assertEquals(0, fixture.noArgumentReceiveCount);
		Assertions.assertEquals(419, fixture.networkTimeout);
		Assertions.assertTrue(transport.isConnectionUncertain());
	}

	@Test
	public void testNetworkTimeoutGuardPreservesSqlConnectionFailures() throws Exception {
		TransportFixture inspectionFixture = new TransportFixture();
		SQLException inspectionFailure = new SQLException("inspection connection loss", "08006");
		inspectionFixture.networkTimeoutInspectionFailure = inspectionFailure;
		NotificationTransport inspectionTransport = inspectionFixture.openTransport();

		SQLException inspectionThrown = Assertions.assertThrows(SQLException.class, inspectionTransport::drain);

		Assertions.assertSame(inspectionFailure, inspectionThrown);
		Assertions.assertTrue(inspectionTransport.isConnectionUncertain());

		TransportFixture zeroingFixture = new TransportFixture();
		zeroingFixture.networkTimeout = 521;
		zeroingFixture.closed = true;
		SQLException zeroingFailure = new SQLException("zeroing on closed connection", "HY000");
		zeroingFixture.networkTimeoutZeroingFailure = zeroingFailure;
		NotificationTransport zeroingTransport = zeroingFixture.openTransport();

		SQLException zeroingThrown = Assertions.assertThrows(SQLException.class, zeroingTransport::drain);

		Assertions.assertSame(zeroingFailure, zeroingThrown);
		Assertions.assertTrue(zeroingTransport.isConnectionUncertain());

		TransportFixture restorationFixture = new TransportFixture();
		restorationFixture.networkTimeout = 619;
		SQLException restorationFailure = new SQLException("restoration connection loss", "08003");
		restorationFixture.networkTimeoutRestorationFailure = restorationFailure;
		NotificationTransport restorationTransport = restorationFixture.openTransport();

		SQLException restorationThrown = Assertions.assertThrows(
				SQLException.class, restorationTransport::drain);

		Assertions.assertSame(restorationFailure, restorationThrown);
		Assertions.assertTrue(restorationTransport.isConnectionUncertain());
	}

	@Test
	public void testDriverFailureRemainsPrimaryAndNetworkTimeoutIsRestored() throws Exception {
		TransportFixture fixture = new TransportFixture();
		fixture.networkTimeout = 509;
		SQLException driverFailure = new SQLException("driver");
		fixture.driverReceiveFailure = driverFailure;
		NotificationTransport transport = fixture.openTransport();

		SQLException thrown = Assertions.assertThrows(SQLException.class,
				() -> transport.receive(Duration.ofMillis(9)));

		Assertions.assertSame(driverFailure, thrown);
		Assertions.assertEquals(List.of(
				"getNetworkTimeout",
				"setNetworkTimeout:0",
				"getNotifications:9",
				"setNetworkTimeout:509"), fixture.receiveEvents);
		Assertions.assertEquals(509, fixture.networkTimeout);
		Assertions.assertTrue(transport.isConnectionUncertain());
	}

	@Test
	public void testRestorationFailureWithholdsReturnedBatchAndLatchesUncertainty() throws Exception {
		TransportFixture fixture = new TransportFixture();
		fixture.networkTimeout = 811;
		TestPgNotification pgNotification = new TestPgNotification("car_changed", "23");
		fixture.timedNotifications = new PGNotification[]{pgNotification};
		SQLException restorationFailure = new SQLException("restoration");
		fixture.networkTimeoutRestorationFailure = restorationFailure;
		NotificationTransport transport = fixture.openTransport();

		NotificationReceiveUnsupportedException thrown = Assertions.assertThrows(
				NotificationReceiveUnsupportedException.class,
				() -> transport.receive(Duration.ofMillis(12)));

		Assertions.assertSame(restorationFailure, thrown.getCause());
		Assertions.assertEquals(0, pgNotification.accessCount);
		Assertions.assertEquals(List.of(
				"getNetworkTimeout",
				"setNetworkTimeout:0",
				"getNotifications:12",
				"setNetworkTimeout:811"), fixture.receiveEvents);
		Assertions.assertTrue(transport.isConnectionUncertain());
	}

	@Test
	public void testDriverAndRestorationFailuresPreservePrimaryAndSuppression() throws Exception {
		TransportFixture fixture = new TransportFixture();
		fixture.networkTimeout = 613;
		SQLException driverFailure = new SQLException("driver");
		SQLException restorationFailure = new SQLException("restoration");
		fixture.driverReceiveFailure = driverFailure;
		fixture.networkTimeoutRestorationFailure = restorationFailure;
		NotificationTransport transport = fixture.openTransport();

		SQLException thrown = Assertions.assertThrows(SQLException.class, transport::drain);

		Assertions.assertSame(driverFailure, thrown);
		Assertions.assertEquals(1, thrown.getSuppressed().length);
		Assertions.assertInstanceOf(
				NotificationReceiveUnsupportedException.class, thrown.getSuppressed()[0]);
		Assertions.assertSame(restorationFailure, thrown.getSuppressed()[0].getCause());
		Assertions.assertTrue(transport.isConnectionUncertain());
	}

	@Test
	public void testFailureSuppressionIsIdentitySafe() throws Exception {
		TransportFixture fixture = new TransportFixture();
		fixture.networkTimeout = 347;
		SQLException sharedFailure = new SQLException("shared");
		fixture.driverReceiveFailure = sharedFailure;
		fixture.networkTimeoutRestorationFailure = sharedFailure;
		NotificationTransport transport = fixture.openTransport();

		SQLException thrown = Assertions.assertThrows(SQLException.class, transport::drain);

		Assertions.assertSame(sharedFailure, thrown);
		Assertions.assertEquals(0, thrown.getSuppressed().length);
		Assertions.assertTrue(transport.isConnectionUncertain());
	}

	@Test
	public void testRawDriverErrorMarksTransportUncertainButPostGuardMappingErrorDoesNot() throws Exception {
		TransportFixture driverFixture = new TransportFixture();
		AssertionError driverFailure = new AssertionError("driver linkage failed");
		driverFixture.driverReceiveFailure = driverFailure;
		NotificationTransport driverTransport = driverFixture.openTransport();

		AssertionError driverThrown = Assertions.assertThrows(AssertionError.class, driverTransport::drain);

		Assertions.assertSame(driverFailure, driverThrown);
		Assertions.assertTrue(driverTransport.isConnectionUncertain());

		TransportFixture mappingFixture = new TransportFixture();
		AssertionError mappingFailure = new AssertionError("notification mapping failed");
		mappingFixture.drainNotifications = new PGNotification[]{new PGNotification() {
			@Override
			public String getName() {
				throw mappingFailure;
			}

			@Override
			public int getPID() {
				return 1;
			}

			@Override
			public String getParameter() {
				return "";
			}
		}};
		NotificationTransport mappingTransport = mappingFixture.openTransport();

		AssertionError mappingThrown = Assertions.assertThrows(AssertionError.class, mappingTransport::drain);

		Assertions.assertSame(mappingFailure, mappingThrown);
		Assertions.assertFalse(mappingTransport.isConnectionUncertain(),
				"Mapping after a successfully restored guard is not physical transport uncertainty");
	}

	@Test
	public void testUnwrapDistinguishesUnsupportedCapabilityFromConnectionFailure() throws Exception {
		TransportFixture notWrapper = new TransportFixture();
		notWrapper.wrapperAvailable = false;
		Assertions.assertThrows(NotificationReceiveUnsupportedException.class, notWrapper::openTransport);

		TransportFixture nullUnwrap = new TransportFixture();
		nullUnwrap.nullUnwrapResult = true;
		Assertions.assertThrows(NotificationReceiveUnsupportedException.class, nullUnwrap::openTransport);

		TransportFixture unsupportedFeature = new TransportFixture();
		SQLFeatureNotSupportedException featureFailure = new SQLFeatureNotSupportedException("unsupported");
		unsupportedFeature.isWrapperForFailure = featureFailure;
		NotificationReceiveUnsupportedException featureThrown = Assertions.assertThrows(
				NotificationReceiveUnsupportedException.class, unsupportedFeature::openTransport);
		Assertions.assertSame(featureFailure, featureThrown.getCause());

		TransportFixture nonConnectionFailure = new TransportFixture();
		SQLException unwrapFailure = new SQLException("proxy cannot unwrap", "HY000");
		nonConnectionFailure.unwrapFailure = unwrapFailure;
		NotificationReceiveUnsupportedException unsupportedThrown = Assertions.assertThrows(
				NotificationReceiveUnsupportedException.class, nonConnectionFailure::openTransport);
		Assertions.assertSame(unwrapFailure, unsupportedThrown.getCause());

		TransportFixture lostConnection = new TransportFixture();
		SQLException connectionFailure = new SQLException("connection lost", "08006");
		lostConnection.unwrapFailure = connectionFailure;
		SQLException connectionThrown = Assertions.assertThrows(SQLException.class, lostConnection::openTransport);
		Assertions.assertSame(connectionFailure, connectionThrown);

		TransportFixture confirmedClosed = new TransportFixture();
		SQLException closedFailure = new SQLException("unwrap failed", "HY000");
		confirmedClosed.unwrapFailure = closedFailure;
		confirmedClosed.closed = true;
		SQLException closedThrown = Assertions.assertThrows(SQLException.class, confirmedClosed::openTransport);
		Assertions.assertSame(closedFailure, closedThrown);

		TransportFixture diagnosticConnectionFailure = new TransportFixture();
		SQLException originalFailure = new SQLException("unwrap failed", "HY000");
		SQLException closedCheckFailure = new SQLException("connection lost", "08006");
		diagnosticConnectionFailure.unwrapFailure = originalFailure;
		diagnosticConnectionFailure.closedCheckFailure = closedCheckFailure;
		SQLException diagnosticThrown = Assertions.assertThrows(
				SQLException.class, diagnosticConnectionFailure::openTransport);
		Assertions.assertSame(originalFailure, diagnosticThrown);
		Assertions.assertArrayEquals(new Throwable[]{closedCheckFailure}, diagnosticThrown.getSuppressed());
	}

	private static final class TransportFixture implements InvocationHandler {
		private final Connection connection;
		private final List<String> executedSql;
		private final List<String> receiveEvents;
		private final List<Integer> timedWaitMilliseconds;
		private boolean wrapperAvailable;
		private boolean nullUnwrapResult;
		private boolean closed;
		private boolean statementClosed;
		private int networkTimeout;
		private int noArgumentReceiveCount;
		private Throwable isWrapperForFailure;
		private Throwable unwrapFailure;
		private Throwable closedCheckFailure;
		private Throwable networkTimeoutInspectionFailure;
		private Throwable networkTimeoutZeroingFailure;
		private Throwable networkTimeoutRestorationFailure;
		private Throwable driverReceiveFailure;
		private PGNotification[] drainNotifications;
		private PGNotification[] timedNotifications;

		private TransportFixture() {
			this.executedSql = new ArrayList<>();
			this.receiveEvents = new ArrayList<>();
			this.timedWaitMilliseconds = new ArrayList<>();
			this.wrapperAvailable = true;
			this.drainNotifications = new PGNotification[0];
			this.timedNotifications = new PGNotification[0];
			this.connection = (Connection) Proxy.newProxyInstance(
					PostgresNotificationTransportTests.class.getClassLoader(),
					new Class<?>[]{Connection.class, PGConnection.class}, this);
		}

		private NotificationTransport openTransport()
				throws SQLException, NotificationReceiveUnsupportedException {
			return PostgresNotificationTransport.open(this.connection);
		}

		@Override
		public Object invoke(Object proxy,
											 Method method,
											 Object[] arguments) throws Throwable {
			if (method.getDeclaringClass() == Object.class)
				return objectMethod(proxy, method, arguments);

			switch (method.getName()) {
				case "isWrapperFor":
					if (this.isWrapperForFailure != null)
						throw this.isWrapperForFailure;

					return this.wrapperAvailable && arguments[0] == PGConnection.class;
				case "unwrap":
					if (this.unwrapFailure != null)
						throw this.unwrapFailure;

					return this.nullUnwrapResult ? null : this.connection;
				case "isClosed":
					if (this.closedCheckFailure != null)
						throw this.closedCheckFailure;

					return this.closed;
				case "createStatement":
					return statement();
				case "getNetworkTimeout":
					this.receiveEvents.add("getNetworkTimeout");

					if (this.networkTimeoutInspectionFailure != null)
						throw this.networkTimeoutInspectionFailure;

					return this.networkTimeout;
				case "setNetworkTimeout":
					int requestedTimeout = (Integer) arguments[1];
					this.receiveEvents.add("setNetworkTimeout:" + requestedTimeout);

					if (requestedTimeout == 0 && this.networkTimeoutZeroingFailure != null)
						throw this.networkTimeoutZeroingFailure;

					if (requestedTimeout != 0 && this.networkTimeoutRestorationFailure != null)
						throw this.networkTimeoutRestorationFailure;

					this.networkTimeout = requestedTimeout;
					return null;
				case "getNotifications":
					if (arguments == null || arguments.length == 0) {
						++this.noArgumentReceiveCount;
						this.receiveEvents.add("getNotifications");

						if (this.driverReceiveFailure != null)
							throw this.driverReceiveFailure;

						return this.drainNotifications;
					}

					int waitMilliseconds = (Integer) arguments[0];
					this.timedWaitMilliseconds.add(waitMilliseconds);
					this.receiveEvents.add("getNotifications:" + waitMilliseconds);

					if (this.driverReceiveFailure != null)
						throw this.driverReceiveFailure;

					return this.timedNotifications;
				default:
					throw new AssertionError("Unexpected connection method: " + method);
			}
		}

		private java.sql.Statement statement() {
			return (java.sql.Statement) Proxy.newProxyInstance(
					PostgresNotificationTransportTests.class.getClassLoader(),
					new Class<?>[]{java.sql.Statement.class}, (proxy, method, arguments) -> {
						if (method.getDeclaringClass() == Object.class)
							return objectMethod(proxy, method, arguments);

						if ("execute".equals(method.getName())) {
							this.executedSql.add((String) arguments[0]);
							return false;
						}

						if ("close".equals(method.getName())) {
							this.statementClosed = true;
							return null;
						}

						throw new AssertionError("Unexpected statement method: " + method);
					});
		}
	}

	private static final class TestPgNotification implements PGNotification {
		private final String name;
		private final String parameter;
		private int accessCount;

		private TestPgNotification(String name,
															 String parameter) {
			this.name = name;
			this.parameter = parameter;
		}

		@Override
		public String getName() {
			++this.accessCount;
			return this.name;
		}

		@Override
		public int getPID() {
			return 1;
		}

		@Override
		public String getParameter() {
			++this.accessCount;
			return this.parameter;
		}
	}

	private static Object objectMethod(Object proxy,
																		 Method method,
																		 Object[] arguments) {
		switch (method.getName()) {
			case "equals":
				return proxy == arguments[0];
			case "hashCode":
				return System.identityHashCode(proxy);
			case "toString":
				return proxy.getClass().getName();
			default:
				throw new AssertionError("Unexpected Object method: " + method);
		}
	}
}
