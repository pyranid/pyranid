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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
import java.io.PrintWriter;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Types;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.logging.Logger;

import static com.pyranid.AmbiguousTimestampBindingStrategy.TIMESTAMP_WITHOUT_TIME_ZONE;
import static java.util.Objects.requireNonNull;

/**
 * @author <a href="https://www.revetkn.com">Mark Allen</a>
 * @since 4.2.0
 */
public class TemporalParameterBindingTests {
	@Test
	public void metadataUnavailableInstantBindsAsTimestampWithTimeZoneByDefault() {
		BindingCapture capture = new BindingCapture();
		Instant instant = Instant.parse("2020-01-02T03:04:05Z");

		database(capture, ZoneId.of("America/New_York"), null)
				.query("UPDATE t SET created_at=:createdAt")
				.bind("createdAt", instant)
				.execute();

		Assertions.assertEquals("setObject", capture.methodName);
		Assertions.assertEquals(1, capture.parameterIndex);
		Assertions.assertEquals(OffsetDateTime.ofInstant(instant, ZoneOffset.UTC), capture.value);
		Assertions.assertEquals(Types.TIMESTAMP_WITH_TIMEZONE, capture.sqlType);
	}

	@Test
	public void metadataUnavailableOffsetDateTimeBindsAsTimestampWithTimeZoneByDefault() {
		BindingCapture capture = new BindingCapture();
		OffsetDateTime offsetDateTime = OffsetDateTime.parse("2020-01-02T03:04:05-03:00");

		database(capture, ZoneId.of("America/New_York"), null)
				.query("UPDATE t SET created_at=:createdAt")
				.bind("createdAt", offsetDateTime)
				.execute();

		Assertions.assertEquals("setObject", capture.methodName);
		Assertions.assertEquals(1, capture.parameterIndex);
		Assertions.assertEquals(offsetDateTime, capture.value);
		Assertions.assertEquals(Types.TIMESTAMP_WITH_TIMEZONE, capture.sqlType);
	}

	@Test
	public void metadataUnavailableInstantCanBindAsTimestampWithoutTimeZone() {
		BindingCapture capture = new BindingCapture();
		ZoneId zone = ZoneId.of("America/New_York");
		Instant instant = Instant.parse("2020-01-02T03:04:05Z");

		database(capture, zone, TIMESTAMP_WITHOUT_TIME_ZONE)
				.query("UPDATE t SET created_at=:createdAt")
				.bind("createdAt", instant)
				.execute();

		Assertions.assertEquals("setObject", capture.methodName);
		Assertions.assertEquals(1, capture.parameterIndex);
		Assertions.assertEquals(LocalDateTime.ofInstant(instant, zone), capture.value);
		Assertions.assertEquals(Types.TIMESTAMP, capture.sqlType);
	}

	@Test
	public void genericTimestampMetadataCanBindAsTimestampWithoutTimeZone() {
		BindingCapture capture = new BindingCapture();
		capture.parameterSqlType = Types.OTHER;
		ZoneId zone = ZoneId.of("America/New_York");
		Instant instant = Instant.parse("2020-01-02T03:04:05Z");

		database(capture, zone, TIMESTAMP_WITHOUT_TIME_ZONE)
				.query("UPDATE t SET created_at=:createdAt")
				.bind("createdAt", instant)
				.execute();

		Assertions.assertEquals("setObject", capture.methodName);
		Assertions.assertEquals(1, capture.parameterIndex);
		Assertions.assertEquals(LocalDateTime.ofInstant(instant, zone), capture.value);
		Assertions.assertEquals(Types.TIMESTAMP, capture.sqlType);
	}

	@Test
	public void metadataUnavailableOffsetDateTimeCanBindAsTimestampWithoutTimeZone() {
		BindingCapture capture = new BindingCapture();
		ZoneId zone = ZoneId.of("America/New_York");
		OffsetDateTime offsetDateTime = OffsetDateTime.parse("2020-01-02T03:04:05-03:00");

		database(capture, zone, TIMESTAMP_WITHOUT_TIME_ZONE)
				.query("UPDATE t SET created_at=:createdAt")
				.bind("createdAt", offsetDateTime)
				.execute();

		Assertions.assertEquals("setObject", capture.methodName);
		Assertions.assertEquals(1, capture.parameterIndex);
		Assertions.assertEquals(offsetDateTime.atZoneSameInstant(zone).toLocalDateTime(), capture.value);
		Assertions.assertEquals(Types.TIMESTAMP, capture.sqlType);
	}

	@Test
	public void metadataAvailableTimestampWithTimeZoneOverridesAmbiguousBindingStrategy() {
		BindingCapture capture = new BindingCapture();
		capture.parameterSqlType = Types.TIMESTAMP_WITH_TIMEZONE;
		capture.parameterTypeName = "TIMESTAMP WITH TIME ZONE";
		Instant instant = Instant.parse("2020-01-02T03:04:05Z");

		database(capture, ZoneId.of("America/New_York"), TIMESTAMP_WITHOUT_TIME_ZONE)
				.query("UPDATE t SET created_at=:createdAt")
				.bind("createdAt", instant)
				.execute();

		Assertions.assertEquals("setObject", capture.methodName);
		Assertions.assertEquals(1, capture.parameterIndex);
		Assertions.assertEquals(OffsetDateTime.ofInstant(instant, ZoneOffset.UTC), capture.value);
		Assertions.assertEquals(Types.TIMESTAMP_WITH_TIMEZONE, capture.sqlType);
	}

	@NonNull
	private Database database(@NonNull BindingCapture capture,
														@NonNull ZoneId zone,
														@Nullable AmbiguousTimestampBindingStrategy ambiguousTimestampBindingStrategy) {
		requireNonNull(capture);
		requireNonNull(zone);

		return Database.withDataSource(dataSource(capture))
				.databaseType(DatabaseType.GENERIC)
				.timeZone(zone)
				.ambiguousTimestampBindingStrategy(ambiguousTimestampBindingStrategy)
				.build();
	}

	@NonNull
	private DataSource dataSource(@NonNull BindingCapture capture) {
		requireNonNull(capture);

		return new DataSource() {
			@Override
			public Connection getConnection() {
				return connection(capture);
			}

			@Override
			public Connection getConnection(String username,
																			String password) {
				return connection(capture);
			}

			@Override
			public PrintWriter getLogWriter() {
				return null;
			}

			@Override
			public void setLogWriter(PrintWriter out) {
			}

			@Override
			public void setLoginTimeout(int seconds) {
			}

			@Override
			public int getLoginTimeout() {
				return 0;
			}

			@Override
			public Logger getParentLogger() throws SQLFeatureNotSupportedException {
				throw new SQLFeatureNotSupportedException();
			}

			@Override
			public <T> T unwrap(Class<T> iface) throws SQLException {
				throw new SQLException("unwrap");
			}

			@Override
			public boolean isWrapperFor(Class<?> iface) {
				return false;
			}
		};
	}

	@NonNull
	private Connection connection(@NonNull BindingCapture capture) {
		requireNonNull(capture);

		return (Connection) Proxy.newProxyInstance(
				Connection.class.getClassLoader(),
				new Class<?>[]{Connection.class},
				(proxy, method, args) -> {
					String name = method.getName();
					if ("prepareStatement".equals(name))
						return preparedStatement(capture);
					if ("close".equals(name))
						return null;
					if ("toString".equals(name))
						return "Connection<temporal-parameter-binding>";
					return defaultValue(method.getReturnType());
				});
	}

	@NonNull
	private PreparedStatement preparedStatement(@NonNull BindingCapture capture) {
		requireNonNull(capture);

		return (PreparedStatement) Proxy.newProxyInstance(
				PreparedStatement.class.getClassLoader(),
				new Class<?>[]{PreparedStatement.class},
				(proxy, method, args) -> {
					String name = method.getName();
					if ("getParameterMetaData".equals(name))
						return parameterMetaData(capture);
					if ("setObject".equals(name)) {
						capture.methodName = "setObject";
						capture.parameterIndex = (Integer) args[0];
						capture.value = args[1];
						capture.sqlType = args.length >= 3 && args[2] instanceof Integer ? (Integer) args[2] : null;
						return null;
					}
					if ("setTimestamp".equals(name)) {
						capture.methodName = "setTimestamp";
						capture.parameterIndex = (Integer) args[0];
						capture.value = args[1];
						capture.sqlType = Types.TIMESTAMP;
						return null;
					}
					if ("executeLargeUpdate".equals(name))
						return 1L;
					if ("executeUpdate".equals(name))
						return 1;
					if ("close".equals(name))
						return null;
					if ("toString".equals(name))
						return "PreparedStatement<temporal-parameter-binding>";
					return defaultValue(method.getReturnType());
				});
	}

	@NonNull
	private ParameterMetaData parameterMetaData(@NonNull BindingCapture capture) throws SQLFeatureNotSupportedException {
		requireNonNull(capture);

		if (capture.parameterSqlType == null)
			throw new SQLFeatureNotSupportedException("parameter metadata unavailable");

		return (ParameterMetaData) Proxy.newProxyInstance(
				ParameterMetaData.class.getClassLoader(),
				new Class<?>[]{ParameterMetaData.class},
				(proxy, method, args) -> {
					String name = method.getName();
					if ("getParameterCount".equals(name))
						return 1;
					if ("getParameterType".equals(name))
						return capture.parameterSqlType;
					if ("getParameterTypeName".equals(name))
						return capture.parameterTypeName;
					if ("toString".equals(name))
						return "ParameterMetaData<temporal-parameter-binding>";
					return defaultValue(method.getReturnType());
				});
	}

	@Nullable
	private static Object defaultValue(@NonNull Class<?> type) {
		requireNonNull(type);

		if (type == boolean.class)
			return false;
		if (type == byte.class)
			return (byte) 0;
		if (type == short.class)
			return (short) 0;
		if (type == int.class)
			return 0;
		if (type == long.class)
			return 0L;
		if (type == float.class)
			return 0F;
		if (type == double.class)
			return 0D;
		if (type == char.class)
			return '\0';

		return null;
	}

	private static final class BindingCapture {
		@Nullable
		private Integer parameterSqlType;
		@Nullable
		private String parameterTypeName;
		@Nullable
		private String methodName;
		private int parameterIndex;
		@Nullable
		private Object value;
		@Nullable
		private Integer sqlType;
	}
}
