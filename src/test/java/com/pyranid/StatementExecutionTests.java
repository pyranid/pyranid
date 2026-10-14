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

import org.junit.jupiter.api.Test;
import javax.sql.DataSource;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import static org.junit.jupiter.api.Assertions.*;

class StatementExecutionTests {
    @Test
    void elapsedClockCoversAcquisitionAndCleanupAndFreezesAfterRelease() {
        AtomicLong clock = new AtomicLong();
        AtomicInteger closes = new AtomicInteger();
        Connection connection = connection();
        DataSource source = source(connection);
        Database database = Database.withDataSource(source).databaseType(DatabaseType.GENERIC).build();
        StatementConnections connections = new StatementConnections(source, Optional::empty, value -> {
            assertSame(connection, value);
            closes.incrementAndGet();
            clock.addAndGet(20);
        }, new ThreadLocal<>(), value -> GenericDialect.INSTANCE, () -> DatabaseType.GENERIC);
        StatementContext<Void> context = StatementContext.with(Statement.of("lease", "SELECT 1"), database).build();
        StatementExecution lease = new StatementExecution(context, connections, new MetricsCollectorDispatcher(new MetricsCollector() {
            @Override public void didReleaseStatementConnection(StatementContext<?> context, Duration held) {
                clock.addAndGet(500);
            }
        }), clock::get);
        clock.set(30);
        assertSame(connection, lease.acquire("test"));
        assertEquals(Duration.ofNanos(30), lease.acquisitionDuration());
        clock.set(80);
        assertNull(lease.close(null));
        assertEquals(Duration.ofNanos(100), lease.elapsedDuration());
        assertEquals(600, clock.get(), "Terminal release metrics run after the elapsed window closes");
        clock.set(700);
        assertNull(lease.close(null));
        assertEquals(Duration.ofNanos(100), lease.elapsedDuration());
        assertEquals(1, closes.get());
    }

    @Test
    void failedAcquisitionFreezesElapsedBeforeMetricsAndPreservesFailure() {
        for (Exception acquisitionFailure : new Exception[]{new SQLException("acquisition"),
                new IllegalStateException("acquisition")}) {
            AtomicLong clock = new AtomicLong();
            AtomicInteger closes = new AtomicInteger();
            AtomicInteger reportedFailures = new AtomicInteger();
            AtomicReference<Throwable> reportedFailure = new AtomicReference<>();
            DataSource source = (DataSource) Proxy.newProxyInstance(DataSource.class.getClassLoader(),
                    new Class<?>[]{DataSource.class}, (proxy, method, args) -> {
                        if (method.getName().equals("getConnection")) {
                            clock.set(100);
                            throw acquisitionFailure;
                        }
                        return null;
                    });
            Database database = Database.withDataSource(source).databaseType(DatabaseType.GENERIC).build();
            StatementConnections connections = new StatementConnections(source, Optional::empty,
                    value -> closes.incrementAndGet(), new ThreadLocal<>(), value -> GenericDialect.INSTANCE,
                    () -> DatabaseType.GENERIC);
            StatementContext<Void> context = StatementContext.with(Statement.of("lease", "SELECT 1"), database).build();
            StatementExecution lease = new StatementExecution(context, connections,
                    new MetricsCollectorDispatcher(new MetricsCollector() {
                        @Override public void didFailToAcquireStatementConnection(StatementContext<?> context,
                                DatabaseType databaseType, Duration acquisitionDuration, Throwable failure) {
                            reportedFailures.incrementAndGet();
                            reportedFailure.set(failure);
                            clock.addAndGet(500);
                        }
                    }), clock::get);

            RuntimeException failure = assertThrows(RuntimeException.class, () -> lease.acquire("test"));
            if (acquisitionFailure instanceof SQLException)
                assertSame(acquisitionFailure, assertInstanceOf(DatabaseException.class, failure).getCause());
            else
                assertSame(acquisitionFailure, failure);
            assertSame(failure, reportedFailure.get());
            assertEquals(Duration.ofNanos(100), lease.acquisitionDuration());
            assertEquals(Duration.ofNanos(100), lease.elapsedDuration());
            assertEquals(600, clock.get(), "Terminal acquisition metrics run after the elapsed window closes");
            assertSame(failure, lease.close(failure));
            clock.set(700);
            assertSame(failure, lease.close(failure));
            assertEquals(Duration.ofNanos(100), lease.elapsedDuration());
            assertEquals(1, reportedFailures.get());
            assertEquals(0, closes.get());
        }
    }

    @Test
    void cleanupPreservesPrimaryFailureAndMetadataScopesRestoreOuterConnection() {
        Connection outer = connection();
        Connection inner = connection();
        DataSource source = source(inner);
        Database database = Database.withDataSource(source).databaseType(DatabaseType.GENERIC).build();
        ThreadLocal<Connection> metadata = new ThreadLocal<>();
        metadata.set(outer);
        RuntimeException cleanup = new RuntimeException("cleanup");
        StatementConnections connections = new StatementConnections(source, Optional::empty, value -> { throw cleanup; },
                metadata, value -> GenericDialect.INSTANCE, () -> DatabaseType.GENERIC);
        StatementContext<Void> context = StatementContext.with(Statement.of("lease", "SELECT 1"), database).build();
        StatementExecution lease = new StatementExecution(context, connections, new MetricsCollectorDispatcher(null));
        lease.acquire("test");
        try (var scope = lease.metadataScope()) {
            assertSame(inner, metadata.get());
            try (var nested = connections.metadataScope(outer)) { assertSame(outer, metadata.get()); }
            assertSame(inner, metadata.get());
        }
        assertSame(outer, metadata.get());
        AssertionError primary = new AssertionError("operation");
        assertSame(primary, lease.close(primary));
        assertArrayEquals(new Throwable[]{cleanup}, primary.getSuppressed());
        assertSame(primary, lease.close(primary));
        assertEquals(1, primary.getSuppressed().length);
        metadata.remove();
        try (var scope = connections.metadataScope(inner)) { assertSame(inner, metadata.get()); }
        assertNull(metadata.get());
    }

    private static Connection connection() {
        return (Connection) Proxy.newProxyInstance(Connection.class.getClassLoader(), new Class<?>[]{Connection.class},
                (proxy, method, args) -> null);
    }
    private static DataSource source(Connection connection) {
        return (DataSource) Proxy.newProxyInstance(DataSource.class.getClassLoader(), new Class<?>[]{DataSource.class},
                (proxy, method, args) -> method.getName().equals("getConnection") ? connection : null);
    }
}
