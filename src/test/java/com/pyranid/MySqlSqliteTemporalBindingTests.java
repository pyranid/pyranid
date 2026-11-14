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
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Date;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class MySqlSqliteTemporalBindingTests {
	private static final Instant VALUE = Instant.parse("2024-07-01T12:34:56.123Z");
	private static final ZoneId ZONE = ZoneId.of("Asia/Tokyo");

	@Test
	void mysqlFamilyUnzonedAndMissingMetadataUseTheDatabaseZoneForAllInstantLikeValues() {
		for (DatabaseType type : List.of(DatabaseType.MYSQL, DatabaseType.MARIA_DB)) {
			for (Integer metadata : new Integer[]{null, Types.OTHER, Types.VARCHAR, Types.DATE, Types.TIMESTAMP}) {
				for (AmbiguousTimestampBindingStrategy strategy : AmbiguousTimestampBindingStrategy.values()) {
					for (Object value : values()) {
						Capture capture = new Capture(metadata, null);
						capture.database(type, strategy).query("UPDATE work SET value=:v").bind("v", value).execute();
						assertEquals("setObject", capture.method);
						assertEquals(LocalDateTime.ofInstant(VALUE, ZONE), capture.value, type + " " + metadata + " " + value.getClass());
						assertEquals(Types.TIMESTAMP, capture.sqlType);
					}
				}
			}
		}
	}

	@Test
	void mysqlFamilyPositiveZonedMetadataRetainsPrecedenceOverDialectAndConfiguredPreferences() {
		for (DatabaseType type : List.of(DatabaseType.MYSQL, DatabaseType.MARIA_DB)) {
			for (Integer metadata : List.of(Types.TIMESTAMP, Types.TIMESTAMP_WITH_TIMEZONE)) {
				Capture capture = new Capture(metadata, "TIMESTAMP WITH TIME ZONE");
				capture.database(type, AmbiguousTimestampBindingStrategy.TIMESTAMP_WITHOUT_TIME_ZONE)
						.query("UPDATE work SET value=:v").bind("v", VALUE).execute();
				assertEquals(VALUE, ((OffsetDateTime) capture.value).toInstant());
				assertEquals(Types.TIMESTAMP_WITH_TIMEZONE, capture.sqlType);
			}
		}
	}

	@Test
	void sqliteDefaultInstantTextHasOneUtcRepresentationAcrossOffsetsAndLegacyTypes() {
		for (Object value : values()) {
			Capture capture = new Capture(null, null);
			capture.database(DatabaseType.SQLITE, AmbiguousTimestampBindingStrategy.TIMESTAMP_WITH_TIME_ZONE)
					.query("UPDATE work SET value=:v").bind("v", value).execute();
			assertEquals("setString", capture.method);
			assertEquals("2024-07-01T12:34:56.123000000Z", capture.value);
		}
		Capture capture = new Capture(Types.VARCHAR, "VARCHAR");
		capture.database(DatabaseType.SQLITE, AmbiguousTimestampBindingStrategy.TIMESTAMP_WITH_TIME_ZONE)
				.query("UPDATE work SET value=:v").bind("v", Instant.parse("2024-07-01T12:34:00Z")).execute();
		assertEquals("2024-07-01T12:34:00.000000000Z", capture.value);
	}

	@Test
	void sqliteExplicitUnzonedStrategyUsesDatabaseWallTimeEvenWithNonTimestampMetadata() {
		for (Integer metadata : new Integer[]{null, Types.VARCHAR, Types.DATE, Types.OTHER}) {
			Capture capture = new Capture(metadata, metadata == null ? null : "VARCHAR");
			capture.database(DatabaseType.SQLITE, AmbiguousTimestampBindingStrategy.TIMESTAMP_WITHOUT_TIME_ZONE)
					.query("UPDATE work SET value=:v").bind("v", VALUE).execute();
			assertEquals("setString", capture.method);
			assertEquals("2024-07-01T21:34:56.123000000", capture.value);
			assertNull(capture.sqlType);
		}
	}

	@Test
	void dialectFormattingDoesNotReplaceCustomBindersOrOrdinaryLocalValues() {
		Capture capture = new Capture(null, null);
		PreparedStatementBinder binder = PreparedStatementBinder.withCustomParameterBinders(List.of(new CustomParameterBinder() {
			@Override public Boolean appliesTo(TargetType type) { return type.matchesClass(Instant.class); }
			@Override public BindingResult bind(StatementContext<?> context, PreparedStatement statement, Integer index, Object value) throws SQLException {
				assertSame(VALUE, value);
				statement.setString(index, "custom instant"); return BindingResult.handled();
			}
		}));
		capture.database(DatabaseType.SQLITE, AmbiguousTimestampBindingStrategy.TIMESTAMP_WITH_TIME_ZONE, binder)
				.query("UPDATE work SET value=:v").bind("v", VALUE).execute();
		assertEquals("custom instant", capture.value);
		LocalDateTime local = LocalDateTime.of(2024, 7, 1, 12, 34);
		capture.database(DatabaseType.SQLITE, AmbiguousTimestampBindingStrategy.TIMESTAMP_WITH_TIME_ZONE)
				.query("UPDATE work SET value=:v").bind("v", local).execute();
		assertEquals("2024-07-01T12:34:00.000000000", capture.value);
	}

	private static List<Object> values() {
		return List.of(VALUE, VALUE.atOffset(ZoneOffset.ofHours(-4)), VALUE.atZone(ZONE), Date.from(VALUE), Timestamp.from(VALUE));
	}

	private static final class Capture {
		final Integer metadata; final String typeName;
		String method; Object value; Integer sqlType;
		Capture(Integer metadata, String typeName) { this.metadata = metadata; this.typeName = typeName; }
		Database database(DatabaseType type, AmbiguousTimestampBindingStrategy strategy) {
			return database(type, strategy, PreparedStatementBinder.withDefaultConfiguration());
		}
		Database database(DatabaseType type, AmbiguousTimestampBindingStrategy strategy, PreparedStatementBinder binder) {
			ParameterMetaData description = proxy(ParameterMetaData.class, (name, args) -> switch (name) {
				case "getParameterType" -> this.metadata;
				case "getParameterTypeName" -> this.typeName;
				case "getParameterCount" -> 1;
				default -> null;
			});
			PreparedStatement statement = proxy(PreparedStatement.class, (name, args) -> {
				if (name.equals("getParameterMetaData")) {
					if (this.metadata == null) throw new SQLFeatureNotSupportedException("no metadata");
					return description;
				}
				if (name.startsWith("set") && args != null && args.length >= 2 && args[0] instanceof Integer) {
					this.method = name; this.value = args[1];
					this.sqlType = args.length > 2 && args[2] instanceof Integer index ? index : null;
				}
				return name.equals("executeLargeUpdate") ? 1L : null;
			});
			Connection connection = proxy(Connection.class, (name, args) -> name.equals("prepareStatement") ? statement : null);
			DataSource source = proxy(DataSource.class, (name, args) -> name.equals("getConnection") ? connection : null);
			return Database.with(source, type).timeZone(ZONE).ambiguousTimestampBindingStrategy(strategy).preparedStatementBinder(binder).build();
		}
	}
	@FunctionalInterface private interface Call { Object invoke(String method, Object[] args) throws Throwable; }
	private static <T> T proxy(Class<T> type, Call call) {
		return type.cast(Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[]{type}, (proxy, method, args) -> {
			if (method.getName().equals("toString")) return type.getSimpleName() + "<temporal-regression>";
			Object result = call.invoke(method.getName(), args);
			if (result != null || !method.getReturnType().isPrimitive()) return result;
			if (method.getReturnType() == boolean.class) return false;
			if (method.getReturnType() == long.class) return 0L;
			if (method.getReturnType() == int.class) return 0;
			return null;
		}));
	}
}
