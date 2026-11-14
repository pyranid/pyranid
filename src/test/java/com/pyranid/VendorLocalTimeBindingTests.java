/*
 * Copyright 2015-2022 Transmogrify LLC, 2022-2026 Revetware LLC.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may obtain a copy at https://www.apache.org/licenses/LICENSE-2.0
 */
package com.pyranid;

import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Types;
import java.time.LocalTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class VendorLocalTimeBindingTests {
	@Test
	void oracleUsesNativeLocalTimeWithoutAnExplicitTimeTarget() {
		for (LocalTime value : values()) {
			Capture capture = new Capture();
			database(DatabaseType.ORACLE, capture).query("UPDATE work SET time_value = :value").bind("value", value).execute();
			assertEquals(List.of("setObject"), capture.binds);
			assertEquals(value, capture.value);
			assertEquals(2, capture.argumentCount, "An explicit JDBC TIME target selects Oracle's legacy Time conversion");
			assertEquals(1, capture.executions);
		}
	}

	@Test
	void sqlServerUsesIsoTimeStringsWithoutTheSendTimeAsDatetimePath() {
		for (LocalTime value : values()) {
			Capture capture = new Capture();
			database(DatabaseType.SQL_SERVER, capture).query("UPDATE work SET time_value = :value").bind("value", value).execute();
			assertEquals(List.of("setString"), capture.binds);
			assertEquals(value.toString(), capture.value);
			assertEquals(1, capture.executions);
		}
	}

	@Test
	void genericLocalTimesRetainTheirExistingTypedJdbcPath() {
		Capture capture = new Capture();
		database(DatabaseType.GENERIC, capture).query("UPDATE work SET time_value = :value")
				.bind("value", LocalTime.of(10, 11, 12, 123456789)).execute();
		assertEquals(List.of("setObject"), capture.binds);
		assertEquals(3, capture.argumentCount);
		assertEquals(Types.TIME, capture.sqlType);
	}

	@Test
	void vendorBindingFailuresRetainTheOriginalCauseWithoutFallbackOrExecution() {
		for (DatabaseType type : List.of(DatabaseType.ORACLE, DatabaseType.SQL_SERVER)) {
			Capture capture = new Capture();
			capture.failure = new SQLException("transaction aborted", "40001", type == DatabaseType.ORACLE ? 8177 : 3960);
			DatabaseException failure = assertThrows(DatabaseException.class, () -> database(type, capture)
					.query("UPDATE work SET time_value = :value").bind("value", LocalTime.NOON).execute());
			assertSame(capture.failure, failure.getCause());
			assertTrue(failure.isSerializationFailure());
			assertEquals(1, capture.binds.size());
			assertEquals(0, capture.executions);
		}
	}

	private static List<LocalTime> values() {
		return List.of(LocalTime.MIDNIGHT, LocalTime.of(10, 11, 12, 123456789),
				LocalTime.of(23, 59, 59, 998500000), LocalTime.MAX);
	}

	private static Database database(DatabaseType type, Capture capture) {
		PreparedStatement statement = proxy(PreparedStatement.class, (method, args) -> {
			if (method.equals("getParameterMetaData")) throw new SQLFeatureNotSupportedException("unavailable");
			if (method.equals("setObject") || method.equals("setString") || method.equals("setTime")) {
				capture.binds.add(method);
				capture.value = args[1]; capture.argumentCount = args.length;
				capture.sqlType = args.length > 2 ? (Integer) args[2] : null;
				if (capture.failure != null) throw capture.failure;
			}
			if (method.equals("executeLargeUpdate")) { capture.executions++; return 1L; }
			if (method.equals("executeUpdate")) { capture.executions++; return 1; }
			return null;
		});
		Connection connection = proxy(Connection.class, (method, args) -> method.equals("prepareStatement") ? statement : null);
		DataSource source = proxy(DataSource.class, (method, args) -> method.equals("getConnection") ? connection : null);
		return Database.with(source, type).timeZone(ZoneId.of("Asia/Tokyo")).build();
	}

	private static <T> T proxy(Class<T> type, Call call) {
		return type.cast(Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[]{type}, (proxy, method, args) -> {
			Object result = call.invoke(method.getName(), args);
			if (result != null || !method.getReturnType().isPrimitive()) return result;
			if (method.getReturnType() == boolean.class) return false;
			if (method.getReturnType() == int.class) return 0;
			if (method.getReturnType() == long.class) return 0L;
			return null;
		}));
	}
	@FunctionalInterface private interface Call { Object invoke(String method, Object[] args) throws Throwable; }
	private static final class Capture {
		final List<String> binds = new ArrayList<>();
		Object value;
		Integer sqlType;
		int argumentCount, executions;
		SQLException failure;
	}
}
