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
import org.junit.jupiter.api.Timeout;
import org.postgresql.ds.PGSimpleDataSource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import javax.net.SocketFactory;
import javax.sql.DataSource;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Logger;

import static java.util.Objects.requireNonNull;

/**
 * Real-pgjdbc regressions for asynchronous-notification frames fragmented beyond the configured socket timeout.
 *
 * @author <a href="https://www.revetkn.com">Mark Allen</a>
 * @since 4.6.0
 */
@Testcontainers
public class PostgreSqlNotificationFragmentedFrameIT {
	private static final int POSTGRES_PORT = 5432;
	private static final int SOCKET_TIMEOUT_MILLISECONDS = 1_000;
	private static final int FRAGMENT_PREFIX_BYTES = 10;
	private static final int MAX_SERVER_FRAME_BYTES = 16 * 1024 * 1024;
	private static final Duration COORDINATION_TIMEOUT = Duration.ofSeconds(10);
	@NonNull
	private static final ConcurrentMap<String, ClientSocketCapture> CLIENT_SOCKET_CAPTURES =
			new ConcurrentHashMap<>();
	private static final String POSTGRES_IMAGE_NAME =
			System.getProperty("postgres.integration.image", "pgvector/pgvector:pg17");
	private static final DockerImageName POSTGRES_IMAGE = DockerImageName.parse(POSTGRES_IMAGE_NAME)
			.asCompatibleSubstituteFor("postgres");

	@Container
	private static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>(POSTGRES_IMAGE)
			.withDatabaseName("pyranid")
			.withUsername("pyranid")
			.withPassword("pyranid");

	@Test
	@Timeout(value = 30, unit = TimeUnit.SECONDS)
	public void publicDrainSurvivesFragmentedNotificationFrameAndRetainsConnection() throws Exception {
		try (ScriptedPostgreSqlProxy proxy = proxy()) {
			CapturingDataSource listenerDataSource = listenerDataSource(proxy);
			Database listenerDatabase = Database.withDataSource(listenerDataSource)
					.databaseType(DatabaseType.POSTGRESQL)
					.build();
			Database applicationDatabase = Database.withDataSource(applicationDataSource())
					.databaseType(DatabaseType.POSTGRESQL)
					.build();
			String channel = channel("drain");
			Notification expected = Notification.of(channel, "fragmented-drain");
			FragmentPlan fragmentPlan = new FragmentPlan();
			ScenarioControl control = new ScenarioControl();
			ExecutorService executor = newScenarioExecutor();
			Future<ScenarioResult> future = executor.submit(() -> {
				AtomicReference<ScenarioResult> result = new AtomicReference<>();

				listenerDatabase.withNotificationSession(channel, session -> {
					Connection listenerConnection = listenerDataSource.capturedConnection();
					int originalNetworkTimeout = listenerConnection.getNetworkTimeout();
					Assertions.assertEquals(SOCKET_TIMEOUT_MILLISECONDS, originalNetworkTimeout);

					proxy.fragmentNextNotification(fragmentPlan);
					applicationDatabase.sendNotification(channel, expected.getPayload());
					Assertions.assertTrue(fragmentPlan.awaitPrefix(COORDINATION_TIMEOUT),
							"The proxy did not forward the fragmented notification prefix");
					Assertions.assertTrue(
							listenerDataSource.awaitClientReadableBytes(
									FRAGMENT_PREFIX_BYTES, COORDINATION_TIMEOUT),
							"The pgjdbc client socket did not expose the complete fragmented prefix");

					control.receiveStarting.countDown();
					long receiveStartedAt = System.nanoTime();
					List<Notification> notifications = session.drainNotifications();
					long receiveElapsedNanos = System.nanoTime() - receiveStartedAt;
					control.receiveReturned.countDown();
					int restoredNetworkTimeout = listenerConnection.getNetworkTimeout();
					int selectResult = selectOne(listenerConnection);

					result.set(new ScenarioResult(
							notifications,
							originalNetworkTimeout,
							restoredNetworkTimeout,
							selectResult,
							receiveElapsedNanos));
				});

				return requireNonNull(result.get());
			});

			try {
				assertReceiveRemainsBlockedBeyondConfiguredTimeout(future, control);
				fragmentPlan.releaseRemainder();
				ScenarioResult result = awaitResult(future, COORDINATION_TIMEOUT);

				Assertions.assertEquals(List.of(expected), result.notifications());
				assertRestoredAndUsable(result);
				Assertions.assertTrue(
						result.receiveElapsedNanos() >= TimeUnit.MILLISECONDS.toNanos(SOCKET_TIMEOUT_MILLISECONDS),
						"The public drain returned before the configured socket timeout elapsed");
				Assertions.assertTrue(fragmentPlan.awaitRemainder(COORDINATION_TIMEOUT));
				proxy.assertHealthy();
			} finally {
				stopScenario(future, fragmentPlan, listenerDataSource, proxy, executor);
			}
		}
	}

	@Test
	@Timeout(value = 30, unit = TimeUnit.SECONDS)
	public void timedAwaitSurvivesFragmentAfterAccumulatedNotificationAndRetainsConnection() throws Exception {
		try (ScriptedPostgreSqlProxy proxy = proxy()) {
			CapturingDataSource listenerDataSource = listenerDataSource(proxy);
			Database listenerDatabase = Database.withDataSource(listenerDataSource)
					.databaseType(DatabaseType.POSTGRESQL)
					.build();
			Database applicationDatabase = Database.withDataSource(applicationDataSource())
					.databaseType(DatabaseType.POSTGRESQL)
					.build();
			String channel = channel("timed");
			Notification accumulated = Notification.of(channel, "accumulated");
			Notification fragmented = Notification.of(channel, "fragmented-timed");
			FragmentPlan fragmentPlan = new FragmentPlan();
			ScenarioControl control = new ScenarioControl();
			ExecutorService executor = newScenarioExecutor();
			Future<ScenarioResult> future = executor.submit(() -> {
				AtomicReference<ScenarioResult> result = new AtomicReference<>();

				listenerDatabase.withNotificationSession(channel, session -> {
					Connection listenerConnection = listenerDataSource.capturedConnection();
					int originalNetworkTimeout = listenerConnection.getNetworkTimeout();
					Assertions.assertEquals(SOCKET_TIMEOUT_MILLISECONDS, originalNetworkTimeout);

					CountDownLatch accumulatedFrameForwarded = proxy.observeNextNotification();
					applicationDatabase.sendNotification(channel, accumulated.getPayload());
					Assertions.assertTrue(
							accumulatedFrameForwarded.await(
									COORDINATION_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS),
							"The proxy did not forward the complete notification frame");

					// Reading a complete statement response makes pgjdbc consume the already-forwarded
					// NotificationResponse into its accumulated-notification list without draining that list.
					Assertions.assertEquals(1, selectOne(listenerConnection));

					proxy.fragmentNextNotification(fragmentPlan);
					applicationDatabase.sendNotification(channel, fragmented.getPayload());
					Assertions.assertTrue(fragmentPlan.awaitPrefix(COORDINATION_TIMEOUT),
							"The proxy did not forward the fragmented notification prefix");
					Assertions.assertTrue(
							listenerDataSource.awaitClientReadableBytes(
									FRAGMENT_PREFIX_BYTES, COORDINATION_TIMEOUT),
							"The pgjdbc client socket did not expose the complete fragmented prefix");

					control.receiveStarting.countDown();
					long receiveStartedAt = System.nanoTime();
					List<Notification> notifications =
							session.awaitNotifications(Duration.ofSeconds(5));
					long receiveElapsedNanos = System.nanoTime() - receiveStartedAt;
					control.receiveReturned.countDown();
					int restoredNetworkTimeout = listenerConnection.getNetworkTimeout();
					int selectResult = selectOne(listenerConnection);

					result.set(new ScenarioResult(
							notifications,
							originalNetworkTimeout,
							restoredNetworkTimeout,
							selectResult,
							receiveElapsedNanos));
				});

				return requireNonNull(result.get());
			});

			try {
				assertReceiveRemainsBlockedBeyondConfiguredTimeout(future, control);
				fragmentPlan.releaseRemainder();
				ScenarioResult result = awaitResult(future, COORDINATION_TIMEOUT);

				Assertions.assertEquals(2, result.notifications().size());
				Assertions.assertEquals(
						Set.of(accumulated, fragmented),
						Set.copyOf(result.notifications()));
				assertRestoredAndUsable(result);
				Assertions.assertTrue(
						result.receiveElapsedNanos() >= TimeUnit.MILLISECONDS.toNanos(SOCKET_TIMEOUT_MILLISECONDS),
						"The timed public receive returned before the configured socket timeout elapsed");
				Assertions.assertTrue(fragmentPlan.awaitRemainder(COORDINATION_TIMEOUT));
				proxy.assertHealthy();
			} finally {
				stopScenario(future, fragmentPlan, listenerDataSource, proxy, executor);
			}
		}
	}

	private static void assertReceiveRemainsBlockedBeyondConfiguredTimeout(
			@NonNull Future<ScenarioResult> future,
			@NonNull ScenarioControl control) throws Exception {
		requireNonNull(future);
		requireNonNull(control);

		if (!control.receiveStarting.await(
				COORDINATION_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)) {
			if (future.isDone())
				awaitResult(future, Duration.ZERO);

			Assertions.fail("The notification receive did not start within the bounded harness timeout");
		}

		Assertions.assertThrows(
				TimeoutException.class,
				() -> future.get(
						SOCKET_TIMEOUT_MILLISECONDS + 500L,
						TimeUnit.MILLISECONDS),
				"The notification receive returned at the old configured socket timeout");
		Assertions.assertEquals(
				1L,
				control.receiveReturned.getCount(),
				"The public receive returned before the proxy released the frame remainder");
	}

	private static void assertRestoredAndUsable(@NonNull ScenarioResult result) {
		requireNonNull(result);
		Assertions.assertTrue(result.originalNetworkTimeout() > 0);
		Assertions.assertEquals(
				result.originalNetworkTimeout(),
				result.restoredNetworkTimeout(),
				"The receive guard did not restore the exact configured network timeout");
		Assertions.assertEquals(
				1,
				result.selectResult(),
				"The retained physical listener connection was not protocol-usable after receive");
	}

	@NonNull
	private static ScenarioResult awaitResult(
			@NonNull Future<ScenarioResult> future,
			@NonNull Duration timeout) throws Exception {
		requireNonNull(future);
		requireNonNull(timeout);

		try {
			return future.get(timeout.toMillis(), TimeUnit.MILLISECONDS);
		} catch (ExecutionException exception) {
			Throwable cause = exception.getCause();

			if (cause instanceof Exception)
				throw (Exception) cause;

			if (cause instanceof Error)
				throw (Error) cause;

			throw new AssertionError("Unexpected direct Throwable from notification scenario", cause);
		}
	}

	private static void stopScenario(
			@NonNull Future<ScenarioResult> future,
			@NonNull FragmentPlan fragmentPlan,
			@NonNull CapturingDataSource listenerDataSource,
			@NonNull ScriptedPostgreSqlProxy proxy,
			@NonNull ExecutorService executor) throws InterruptedException {
		requireNonNull(future);
		requireNonNull(fragmentPlan);
		requireNonNull(listenerDataSource);
		requireNonNull(proxy);
		requireNonNull(executor);

		fragmentPlan.releaseRemainder();

		if (!future.isDone()) {
			listenerDataSource.abortCapturedConnection();
			proxy.close();
			future.cancel(true);
		}

		executor.shutdownNow();
		Assertions.assertTrue(
				executor.awaitTermination(COORDINATION_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS),
				"The bounded notification scenario worker did not terminate");
	}

	private static int selectOne(@NonNull Connection connection) throws SQLException {
		requireNonNull(connection);

		try (Statement statement = connection.createStatement();
				 ResultSet resultSet = statement.executeQuery("SELECT 1")) {
			Assertions.assertTrue(resultSet.next());
			return resultSet.getInt(1);
		}
	}

	@NonNull
	private static ExecutorService newScenarioExecutor() {
		return Executors.newSingleThreadExecutor(runnable -> {
			Thread thread = new Thread(runnable, "pyranid-fragmented-notification-scenario");
			thread.setDaemon(true);
			return thread;
		});
	}

	@NonNull
	private static ScriptedPostgreSqlProxy proxy() throws IOException {
		return new ScriptedPostgreSqlProxy(
				POSTGRES.getHost(),
				POSTGRES.getMappedPort(POSTGRES_PORT));
	}

	@NonNull
	private static CapturingDataSource listenerDataSource(@NonNull ScriptedPostgreSqlProxy proxy) {
		requireNonNull(proxy);
		PGSimpleDataSource dataSource = configuredDataSource(
				proxy.getListenAddress(),
				proxy.getListenPort());
		String captureId = UUID.randomUUID().toString();
		ClientSocketCapture clientSocketCapture = new ClientSocketCapture();

		if (CLIENT_SOCKET_CAPTURES.putIfAbsent(captureId, clientSocketCapture) != null)
			throw new IllegalStateException("Duplicate fragmented-frame client-socket capture identifier");

		dataSource.setSocketFactory(ClientSocketCapturingFactory.class.getName());
		dataSource.setSocketFactoryArg(captureId);
		dataSource.setSocketTimeout(
				(int) TimeUnit.MILLISECONDS.toSeconds(SOCKET_TIMEOUT_MILLISECONDS));
		return new CapturingDataSource(dataSource, captureId, clientSocketCapture);
	}

	@NonNull
	private static DataSource applicationDataSource() {
		return configuredDataSource(POSTGRES.getHost(), POSTGRES.getMappedPort(POSTGRES_PORT));
	}

	@NonNull
	private static PGSimpleDataSource configuredDataSource(
			@NonNull String host,
			int port) {
		requireNonNull(host);
		PGSimpleDataSource dataSource = new PGSimpleDataSource();
		dataSource.setServerNames(new String[]{host});
		dataSource.setPortNumbers(new int[]{port});
		dataSource.setDatabaseName(POSTGRES.getDatabaseName());
		dataSource.setUser(POSTGRES.getUsername());
		dataSource.setPassword(POSTGRES.getPassword());
		dataSource.setSslMode("disable");
		dataSource.setConnectTimeout(5);
		return dataSource;
	}

	@NonNull
	private static String channel(@NonNull String kind) {
		requireNonNull(kind);
		return "pyranid_fragment_" + kind + "_"
				+ UUID.randomUUID().toString().replace("-", "").substring(0, 12);
	}

	private record ScenarioResult(
			@NonNull List<@NonNull Notification> notifications,
			int originalNetworkTimeout,
			int restoredNetworkTimeout,
			int selectResult,
			long receiveElapsedNanos) {}

	private static final class ScenarioControl {
		@NonNull
		private final CountDownLatch receiveStarting = new CountDownLatch(1);
		@NonNull
		private final CountDownLatch receiveReturned = new CountDownLatch(1);
	}

	private static final class FragmentPlan {
		@NonNull
		private final CountDownLatch prefixForwarded = new CountDownLatch(1);
		@NonNull
		private final CountDownLatch releaseRemainder = new CountDownLatch(1);
		@NonNull
		private final CountDownLatch remainderForwarded = new CountDownLatch(1);

		private boolean awaitPrefix(@NonNull Duration timeout) throws InterruptedException {
			requireNonNull(timeout);
			return this.prefixForwarded.await(timeout.toMillis(), TimeUnit.MILLISECONDS);
		}

		private void releaseRemainder() {
			this.releaseRemainder.countDown();
		}

		private boolean awaitRemainder(@NonNull Duration timeout) throws InterruptedException {
			requireNonNull(timeout);
			return this.remainderForwarded.await(timeout.toMillis(), TimeUnit.MILLISECONDS);
		}
	}

	private static final class CapturingDataSource implements DataSource {
		@NonNull
		private final DataSource delegate;
		@NonNull
		private final String clientSocketCaptureId;
		@NonNull
		private final ClientSocketCapture clientSocketCapture;
		@NonNull
		private final AtomicReference<Connection> connection = new AtomicReference<>();

		private CapturingDataSource(@NonNull DataSource delegate,
				@NonNull String clientSocketCaptureId,
				@NonNull ClientSocketCapture clientSocketCapture) {
			this.delegate = requireNonNull(delegate);
			this.clientSocketCaptureId = requireNonNull(clientSocketCaptureId);
			this.clientSocketCapture = requireNonNull(clientSocketCapture);
		}

		@Override
		public Connection getConnection() throws SQLException {
			return capture(this.delegate.getConnection());
		}

		@Override
		public Connection getConnection(String username, String password) throws SQLException {
			return capture(this.delegate.getConnection(username, password));
		}

		@Override
		public PrintWriter getLogWriter() throws SQLException {
			return this.delegate.getLogWriter();
		}

		@Override
		public void setLogWriter(PrintWriter out) throws SQLException {
			this.delegate.setLogWriter(out);
		}

		@Override
		public void setLoginTimeout(int seconds) throws SQLException {
			this.delegate.setLoginTimeout(seconds);
		}

		@Override
		public int getLoginTimeout() throws SQLException {
			return this.delegate.getLoginTimeout();
		}

		@Override
		public Logger getParentLogger() throws SQLFeatureNotSupportedException {
			return this.delegate.getParentLogger();
		}

		@Override
		public <T> T unwrap(Class<T> iface) throws SQLException {
			if (iface.isInstance(this))
				return iface.cast(this);

			return this.delegate.unwrap(iface);
		}

		@Override
		public boolean isWrapperFor(Class<?> iface) throws SQLException {
			return iface.isInstance(this) || this.delegate.isWrapperFor(iface);
		}

		@NonNull
		private Connection capture(@NonNull Connection capturedConnection) throws SQLException {
			requireNonNull(capturedConnection);

			if (!this.connection.compareAndSet(null, capturedConnection)) {
				capturedConnection.close();
				throw new SQLException("Fragmented-frame fixture acquired more than one listener connection");
			}

			return capturedConnection;
		}

		@NonNull
		private Connection capturedConnection() {
			return requireNonNull(
					this.connection.get(),
					"The listener connection has not been acquired");
		}

		private void abortCapturedConnection() {
			CLIENT_SOCKET_CAPTURES.remove(this.clientSocketCaptureId, this.clientSocketCapture);
			Connection capturedConnection = this.connection.get();

			if (capturedConnection == null)
				return;

			try {
				capturedConnection.abort(Runnable::run);
			} catch (SQLException ignored) {
				// Best-effort harness escape hatch; the proxy socket is closed next.
			}
		}

		private boolean awaitClientReadableBytes(int byteCount, @NonNull Duration timeout)
				throws IOException, InterruptedException {
			return this.clientSocketCapture.awaitReadableBytes(byteCount, requireNonNull(timeout));
		}
	}

	private static final class ClientSocketCapture {
		@NonNull
		private final AtomicReference<Socket> socket = new AtomicReference<>();

		private void capture(@NonNull Socket capturedSocket) throws IOException {
			requireNonNull(capturedSocket);

			if (!this.socket.compareAndSet(null, capturedSocket))
				throw new IOException("Fragmented-frame fixture created more than one listener client socket");
		}

		private boolean awaitReadableBytes(int byteCount, @NonNull Duration timeout)
				throws IOException, InterruptedException {
			if (byteCount <= 0)
				throw new IllegalArgumentException("byteCount must be positive");

			requireNonNull(timeout);
			long deadlineNanos = System.nanoTime() + timeout.toNanos();

			for (;;) {
				Socket capturedSocket = this.socket.get();

				if (capturedSocket != null
						&& capturedSocket.getInputStream().available() >= byteCount)
					return true;

				long remainingNanos = deadlineNanos - System.nanoTime();

				if (remainingNanos <= 0)
					return false;

				TimeUnit.NANOSECONDS.sleep(Math.min(
						remainingNanos,
						TimeUnit.MILLISECONDS.toNanos(1)));
			}
		}
	}

	/**
	 * Test-only pgjdbc socket factory that exposes the exact client socket without consuming protocol bytes.
	 */
	public static final class ClientSocketCapturingFactory extends SocketFactory {
		@NonNull
		private final SocketFactory delegate;
		@NonNull
		private final ClientSocketCapture clientSocketCapture;

		public ClientSocketCapturingFactory(@NonNull String captureId) {
			this.delegate = SocketFactory.getDefault();
			this.clientSocketCapture = requireNonNull(
					CLIENT_SOCKET_CAPTURES.remove(requireNonNull(captureId)),
					"Unknown fragmented-frame client-socket capture identifier");
		}

		@Override
		@NonNull
		public Socket createSocket() throws IOException {
			return capture(this.delegate.createSocket());
		}

		@Override
		@NonNull
		public Socket createSocket(String host, int port) throws IOException {
			return capture(this.delegate.createSocket(host, port));
		}

		@Override
		@NonNull
		public Socket createSocket(String host, int port,
				InetAddress localHost, int localPort) throws IOException {
			return capture(this.delegate.createSocket(host, port, localHost, localPort));
		}

		@Override
		@NonNull
		public Socket createSocket(InetAddress host, int port) throws IOException {
			return capture(this.delegate.createSocket(host, port));
		}

		@Override
		@NonNull
		public Socket createSocket(InetAddress address, int port,
				InetAddress localAddress, int localPort) throws IOException {
			return capture(this.delegate.createSocket(address, port, localAddress, localPort));
		}

		@NonNull
		private Socket capture(@NonNull Socket socket) throws IOException {
			this.clientSocketCapture.capture(requireNonNull(socket));
			return socket;
		}
	}

	private static final class ScriptedPostgreSqlProxy implements AutoCloseable {
		@NonNull
		private final String upstreamHost;
		private final int upstreamPort;
		@NonNull
		private final ServerSocket serverSocket;
		@NonNull
		private final ExecutorService executor;
		@NonNull
		private final AtomicReference<FragmentPlan> fragmentPlan = new AtomicReference<>();
		@NonNull
		private final AtomicReference<CountDownLatch> notificationObserver = new AtomicReference<>();
		@NonNull
		private final AtomicReference<Throwable> failure = new AtomicReference<>();
		@NonNull
		private final AtomicBoolean closing = new AtomicBoolean(false);
		@NonNull
		private final AtomicBoolean connectionFinishing = new AtomicBoolean(false);
		@NonNull
		private final AtomicReference<Socket> clientSocket = new AtomicReference<>();
		@NonNull
		private final AtomicReference<Socket> upstreamSocket = new AtomicReference<>();

		private ScriptedPostgreSqlProxy(@NonNull String upstreamHost, int upstreamPort)
				throws IOException {
			this.upstreamHost = requireNonNull(upstreamHost);
			this.upstreamPort = upstreamPort;
			this.serverSocket = new ServerSocket(
					0,
					1,
					InetAddress.getLoopbackAddress());
			this.executor = Executors.newFixedThreadPool(2, runnable -> {
				Thread thread = new Thread(runnable, "pyranid-postgresql-scripted-proxy");
				thread.setDaemon(true);
				return thread;
			});
			this.executor.submit(this::acceptAndProxy);
		}

		@NonNull
		private String getListenAddress() {
			return this.serverSocket.getInetAddress().getHostAddress();
		}

		private int getListenPort() {
			return this.serverSocket.getLocalPort();
		}

		private void fragmentNextNotification(@NonNull FragmentPlan plan) {
			requireNonNull(plan);

			if (!this.fragmentPlan.compareAndSet(null, plan))
				throw new IllegalStateException("A notification fragmentation plan is already armed");
		}

		@NonNull
		private CountDownLatch observeNextNotification() {
			CountDownLatch observer = new CountDownLatch(1);

			if (!this.notificationObserver.compareAndSet(null, observer))
				throw new IllegalStateException("A notification observer is already armed");

			return observer;
		}

		private void assertHealthy() {
			Throwable proxyFailure = this.failure.get();

			if (proxyFailure != null)
				throw new AssertionError("The scripted PostgreSQL proxy failed", proxyFailure);

			Assertions.assertNull(
					this.fragmentPlan.get(),
					"The scripted proxy never consumed the armed fragmentation plan");
		}

		private void acceptAndProxy() {
			Future<?> clientToServer = null;

			try {
				Socket acceptedClient = this.serverSocket.accept();
				acceptedClient.setTcpNoDelay(true);
				this.clientSocket.set(acceptedClient);

				Socket connectedUpstream = new Socket();
				connectedUpstream.connect(
						new InetSocketAddress(this.upstreamHost, this.upstreamPort),
						(int) COORDINATION_TIMEOUT.toMillis());
				connectedUpstream.setTcpNoDelay(true);
				this.upstreamSocket.set(connectedUpstream);

				clientToServer = this.executor.submit(() ->
						copyClientToServer(acceptedClient, connectedUpstream));
				forwardServerFrames(
						connectedUpstream.getInputStream(),
						acceptedClient.getOutputStream());
			} catch (Throwable throwable) {
				recordFailure(throwable);
			} finally {
				this.connectionFinishing.set(true);
				closeSocket(this.clientSocket.getAndSet(null));
				closeSocket(this.upstreamSocket.getAndSet(null));

				if (clientToServer != null)
					clientToServer.cancel(true);
			}
		}

		private void copyClientToServer(
				@NonNull Socket acceptedClient,
				@NonNull Socket connectedUpstream) {
			requireNonNull(acceptedClient);
			requireNonNull(connectedUpstream);

			try {
				InputStream input = acceptedClient.getInputStream();
				OutputStream output = connectedUpstream.getOutputStream();
				byte[] buffer = new byte[8_192];

				for (;;) {
					int count = input.read(buffer);

					if (count < 0)
						return;

					output.write(buffer, 0, count);
					output.flush();
				}
			} catch (Throwable throwable) {
				recordFailure(throwable);
			}
		}

		private void forwardServerFrames(
				@NonNull InputStream input,
				@NonNull OutputStream output) throws IOException {
			requireNonNull(input);
			requireNonNull(output);

			for (;;) {
				int type = input.read();

				if (type < 0)
					return;

				byte[] lengthBytes = readExactly(input, Integer.BYTES);
				int length = decodeInt32(lengthBytes);

				if (length < Integer.BYTES || length > MAX_SERVER_FRAME_BYTES)
					throw new IOException("Invalid PostgreSQL backend frame length: " + length);

				byte[] body = readExactly(input, length - Integer.BYTES);
				byte[] frame = new byte[1 + lengthBytes.length + body.length];
				frame[0] = (byte) type;
				System.arraycopy(lengthBytes, 0, frame, 1, lengthBytes.length);
				System.arraycopy(body, 0, frame, 1 + lengthBytes.length, body.length);

				if (type == 'A') {
					FragmentPlan plan = this.fragmentPlan.getAndSet(null);

					if (plan != null) {
						forwardFragmentedFrame(frame, output, plan);
						continue;
					}
				}

				output.write(frame);
				output.flush();

				if (type == 'A') {
					CountDownLatch observer = this.notificationObserver.getAndSet(null);

					if (observer != null)
						observer.countDown();
				}
			}
		}

		private void forwardFragmentedFrame(
				byte[] frame,
				@NonNull OutputStream output,
				@NonNull FragmentPlan plan) throws IOException {
			requireNonNull(frame);
			requireNonNull(output);
			requireNonNull(plan);

			if (frame.length < 11)
				throw new IOException("PostgreSQL NotificationResponse frame was unexpectedly short");

			int splitAt = FRAGMENT_PREFIX_BYTES;
			output.write(frame, 0, splitAt);
			output.flush();
			plan.prefixForwarded.countDown();

			try {
				if (!plan.releaseRemainder.await(
						COORDINATION_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS))
					throw new IOException("Timed out waiting to release fragmented frame remainder");
			} catch (InterruptedException exception) {
				Thread.currentThread().interrupt();
				throw new IOException("Interrupted while holding fragmented frame remainder", exception);
			}

			output.write(frame, splitAt, frame.length - splitAt);
			output.flush();
			plan.remainderForwarded.countDown();
		}

		private void recordFailure(@NonNull Throwable throwable) {
			requireNonNull(throwable);

			if (this.closing.get() || this.connectionFinishing.get())
				return;

			if (throwable instanceof SocketException && this.connectionFinishing.get())
				return;

			this.failure.compareAndSet(null, throwable);
		}

		@Override
		public void close() {
			if (!this.closing.compareAndSet(false, true))
				return;

			FragmentPlan armedPlan = this.fragmentPlan.getAndSet(null);

			if (armedPlan != null)
				armedPlan.releaseRemainder();

			closeSocket(this.clientSocket.getAndSet(null));
			closeSocket(this.upstreamSocket.getAndSet(null));

			try {
				this.serverSocket.close();
			} catch (IOException ignored) {
				// Best-effort bounded harness cleanup.
			}

			this.executor.shutdownNow();

			try {
				this.executor.awaitTermination(
						COORDINATION_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
			} catch (InterruptedException exception) {
				Thread.currentThread().interrupt();
			}
		}

		private static void closeSocket(Socket socket) {
			if (socket == null)
				return;

			try {
				socket.close();
			} catch (IOException ignored) {
				// Best-effort bounded harness cleanup.
			}
		}

		@NonNull
		private static byte[] readExactly(@NonNull InputStream input, int byteCount)
				throws IOException {
			requireNonNull(input);
			byte[] bytes = input.readNBytes(byteCount);

			if (bytes.length != byteCount)
				throw new EOFException("PostgreSQL backend frame ended prematurely");

			return bytes;
		}

		private static int decodeInt32(byte[] bytes) {
			requireNonNull(bytes);

			if (bytes.length != Integer.BYTES)
				throw new IllegalArgumentException("A PostgreSQL frame length must contain four bytes");

			return (bytes[0] & 0xFF) << 24
					| (bytes[1] & 0xFF) << 16
					| (bytes[2] & 0xFF) << 8
					| bytes[3] & 0xFF;
		}
	}
}
