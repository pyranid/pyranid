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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class SqliteDateFormatTests {
	@AfterEach
	void clearInterruptFlag() { Thread.interrupted(); }

	@Test
	void unavailableBorrowedStatementOrConnectionReturnsUnknownWithoutClosingAnything() throws Exception {
		for (boolean connectionStep : new boolean[]{false, true}) {
			AtomicInteger closes = new AtomicInteger();
			assertNull(SqliteDialect.configuredDateStringFormat(resultSet(connectionStep, null, closes)));
			assertEquals(0, closes.get());
			for (Throwable unavailable : new Throwable[]{new SQLFeatureNotSupportedException("unavailable"),
					new UnsupportedOperationException("unavailable"), new AbstractMethodError("unavailable")}) {
				assertNull(SqliteDialect.configuredDateStringFormat(resultSet(connectionStep, unavailable, closes)));
				assertEquals(0, closes.get());
			}
		}
	}

	@Test
	void accessorFailuresPreserveSqlRollbackCancellationAndFatalEvidence() {
		for (boolean connectionStep : new boolean[]{false, true}) {
			for (Throwable failure : new Throwable[]{new SQLException("rollback", "40001"),
					new SQLException("connection failed", "08006"), new IllegalStateException("application failure"),
					new AssertionError("fatal"),
					new SQLFeatureNotSupportedException("unsupported with cancellation", new InterruptedException("cancelled"))}) {
				AtomicInteger closes = new AtomicInteger();
				assertSame(failure, assertThrows(Throwable.class,
						() -> SqliteDialect.configuredDateStringFormat(resultSet(connectionStep, failure, closes))));
				assertEquals(0, closes.get());
			}
		}
	}

	@Test
	void unsupportedCapabilityCannotHideRollbackOrUnreadableDiagnostics() {
		for (boolean connectionStep : new boolean[]{false, true}) {
			SQLException rollback = new SQLException("rollback", "40001");
			SQLFeatureNotSupportedException unsupported = new SQLFeatureNotSupportedException("unsupported");
			unsupported.addSuppressed(rollback);
			assertSame(unsupported, assertThrows(SQLException.class,
					() -> SqliteDialect.configuredDateStringFormat(resultSet(connectionStep, unsupported, new AtomicInteger()))));
			RuntimeException diagnostic = new IllegalStateException("unreadable SQLState");
			SQLFeatureNotSupportedException unreadable = new SQLFeatureNotSupportedException("unavailable") {
				private boolean first = true;
				@Override public String getSQLState() {
					if (this.first) { this.first = false; throw diagnostic; }
					return null;
				}
			};
			assertSame(unreadable, assertThrows(SQLException.class,
					() -> SqliteDialect.configuredDateStringFormat(resultSet(connectionStep, unreadable, new AtomicInteger()))));
			assertArrayEquals(new Throwable[]{diagnostic}, unreadable.getSuppressed());
		}
	}

	@Test
	void nonNativeBorrowedConnectionReturnsUnknownWithoutClosingHandles() throws Exception {
		AtomicInteger closes = new AtomicInteger();
		java.sql.Connection connection = (java.sql.Connection) Proxy.newProxyInstance(
				java.sql.Connection.class.getClassLoader(), new Class<?>[]{java.sql.Connection.class},
				(proxy, method, args) -> {
					if (method.getName().equals("close")) { closes.incrementAndGet(); return null; }
					if (method.getName().equals("isWrapperFor")) return false;
					throw new AssertionError(method);
				});
		Statement statement = (Statement) Proxy.newProxyInstance(Statement.class.getClassLoader(),
				new Class<?>[]{Statement.class}, (proxy, method, args) -> {
					if (method.getName().equals("close")) { closes.incrementAndGet(); return null; }
					if (method.getName().equals("getConnection")) return connection;
					throw new AssertionError(method);
				});
		ResultSet resultSet = (ResultSet) Proxy.newProxyInstance(ResultSet.class.getClassLoader(),
				new Class<?>[]{ResultSet.class}, (proxy, method, args) -> {
					if (method.getName().equals("close")) { closes.incrementAndGet(); return null; }
					if (method.getName().equals("getStatement")) return statement;
					throw new AssertionError(method);
				});
		assertNull(SqliteDialect.configuredDateStringFormat(resultSet));
		assertEquals(0, closes.get());
	}

	private static ResultSet resultSet(boolean connectionStep, Throwable failure, AtomicInteger closes) {
		Statement statement = (Statement) Proxy.newProxyInstance(Statement.class.getClassLoader(), new Class<?>[]{Statement.class},
				(proxy, method, args) -> {
					if (method.getName().equals("close")) { closes.incrementAndGet(); return null; }
					if (method.getName().equals("getConnection")) {
						if (failure != null) throw failure;
						return null;
					}
					throw new AssertionError(method);
				});
		return (ResultSet) Proxy.newProxyInstance(ResultSet.class.getClassLoader(), new Class<?>[]{ResultSet.class},
				(proxy, method, args) -> {
					if (method.getName().equals("close")) { closes.incrementAndGet(); return null; }
					if (method.getName().equals("getStatement")) {
						if (connectionStep) return statement;
						if (failure != null) throw failure;
						return null;
					}
					throw new AssertionError(method);
				});
	}
}
