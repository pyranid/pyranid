/*
 * Copyright 2015-2022 Transmogrify LLC, 2022-2026 Revetware LLC.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may obtain a copy at https://www.apache.org/licenses/LICENSE-2.0
 */
package com.pyranid;

import org.jspecify.annotations.Nullable;

import java.nio.ByteBuffer;
import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.time.DateTimeException;
import java.time.Duration;
import java.time.Period;
import java.util.Locale;
import java.util.Optional;
import java.util.regex.Pattern;

/** Internal interval adapters. No driver classes, mutable driver state or JDBC resources are cached. */
final class SqlIntervalSupport {
	private static final Pattern WHITESPACE = Pattern.compile("\\s+");
	private static final String LEADING_PRECISION = "(?:\\s*\\(\\s*\\d+\\s*\\))?";
	private static final String SECOND_PRECISION = "(?:\\s*\\(\\s*\\d+(?:\\s*,\\s*\\d+)?\\s*\\))?";
	private static final Pattern YEAR_MONTH_NAME = Pattern.compile("INTERVAL (?:YEAR" + LEADING_PRECISION
			+ "(?: TO MONTH)?|MONTH" + LEADING_PRECISION + ")");
	private static final Pattern DAY_SECOND_NAME = Pattern.compile("INTERVAL (?:DAY" + LEADING_PRECISION
			+ "(?: TO (?:HOUR|MINUTE|SECOND" + SECOND_PRECISION + "))?|HOUR" + LEADING_PRECISION
			+ "(?: TO (?:MINUTE|SECOND" + SECOND_PRECISION + "))?|MINUTE" + LEADING_PRECISION
			+ "(?: TO SECOND" + SECOND_PRECISION + ")?|SECOND" + SECOND_PRECISION + ")");

	private SqlIntervalSupport() {}

	static boolean needsZeroFamilyMetadata(DatabaseType databaseType, SqlInterval value) {
		return (databaseType == DatabaseType.ORACLE || databaseType == DatabaseType.GENERIC)
				&& value.equals(SqlInterval.ZERO);
	}

	static void bind(PreparedStatement statement, int index, StatementContext<?> context, SqlInterval value,
			@Nullable Integer sqlType, @Nullable String typeName) throws SQLException {
		try {
			switch (context.getDatabaseType()) {
				case POSTGRESQL -> statement.setObject(index, PostgresDriverSupport.newPgObject(
						statement.getConnection(), "interval", SqlIntervalText.formatPostgres(value)));
				case DUCK_DB -> statement.setString(index, SqlIntervalText.formatDuckDb(value));
				case ORACLE, GENERIC -> bindNative(statement, index, context, value, sqlType, typeName);
				default -> throw new DatabaseException(context.getDatabaseType() + " has no supported SQL interval type. "
						+ "Bind an explicit representation or provide a CustomParameterBinder for SqlInterval.");
			}
		} catch (DateTimeException failure) {
			throw new DatabaseException("SqlInterval cannot be represented exactly by " + context.getDatabaseType()
					+ ". Check interval range and fractional precision; round or truncate explicitly if intended.", failure);
		}
	}

	private static void bindNative(PreparedStatement statement, int index, StatementContext<?> context,
			SqlInterval value, @Nullable Integer sqlType, @Nullable String typeName) throws SQLException {
		if (value.getDays() != 0)
			throw new DatabaseException(context.getDatabaseType() + " interval binding does not infer an elapsed length "
					+ "for calendar days. Use SqlInterval.of(Duration.ofDays(...)) if fixed 24-hour days are intended.");
		if (value.getMonths() != 0 && !value.getTime().isZero())
			throw new DatabaseException(context.getDatabaseType() + " cannot bind one interval containing both months and elapsed time.");

		Family target = family(context.getDatabaseType(), sqlType, typeName);
		Family intended = value.getMonths() != 0 ? Family.YEAR_MONTH : Family.DAY_SECOND;
		if (value.equals(SqlInterval.ZERO)) {
			if (target != Family.YEAR_MONTH && target != Family.DAY_SECOND)
				throw new DatabaseException("Cannot choose the SQL interval family for SqlInterval.ZERO. "
						+ "Provide recognizable YEAR TO MONTH or DAY TO SECOND target metadata, or use a custom binder "
						+ "selecting native Period.ZERO or Duration.ZERO.");
			intended = target;
		} else if ((target == Family.YEAR_MONTH || target == Family.DAY_SECOND) && target != intended) {
			throw new DatabaseException("SqlInterval fields are incompatible with the identified SQL interval target family.");
		}
		// Nonzero fields already choose the native representation. Cached parameter metadata can
		// describe an enclosing expression (for example Oracle's SYSTIMESTAMP + ?), so only a
		// positively identified opposite interval family can veto it. The database validates the bind.
		Object nativeValue = intended == Family.YEAR_MONTH ? value.toPeriod()
				: context.getDatabaseType() == DatabaseType.ORACLE
						? OracleDriverSupport.newIntervalDaySecond(statement.getConnection(), value.getTime()) : value.getTime();
		statement.setObject(index, nativeValue);
	}

	static Optional<SqlInterval> read(ResultSet rows, int index, StatementContext<?> context,
			ResultSetMetaData metadata) throws SQLException {
		return sourceColumn(metadata, index).read(rows, index, context);
	}

	static SourceColumn sourceColumn(ResultSetMetaData metadata, int index) throws SQLException {
		return sourceColumn(metadata.getColumnType(index), metadata.getColumnTypeName(index));
	}

	static SourceColumn sourceColumn(int sqlType, @Nullable String typeName) {
		Family name = nameFamily(typeName);
		return new SourceColumn(familyFromName(DatabaseType.GENERIC, sqlType, name),
				familyFromName(DatabaseType.ORACLE, sqlType, name));
	}

	/** A plan may retain these enum facts, never the JDBC metadata or database that supplied them. */
	static final class SourceColumn {
		private final Family defaultFamily;
		private final Family oracleFamily;

		private SourceColumn(Family defaultFamily, Family oracleFamily) {
			this.defaultFamily = defaultFamily;
			this.oracleFamily = oracleFamily;
		}

		Optional<SqlInterval> read(ResultSet rows, int index, StatementContext<?> context) throws SQLException {
			DatabaseType databaseType = context.getDatabaseType();
			Family family = databaseType == DatabaseType.ORACLE ? this.oracleFamily : this.defaultFamily;
			try {
				return switch (databaseType) {
					case POSTGRESQL -> {
						if (!family.isInterval()) throw unsupportedColumn();
						yield readPostgres(rows, index);
					}
					case DUCK_DB -> {
						if (family != Family.UNIFIED) throw unsupportedColumn();
						String text = rows.getString(index);
						if (isSqlNull(text, rows.wasNull())) yield Optional.empty();
						yield Optional.of(SqlIntervalText.parseDuckDb(text));
					}
					case ORACLE, GENERIC -> readNative(rows, index, family);
					default -> throw unsupportedColumn();
				};
			} catch (DateTimeException failure) {
				throw new DatabaseException("SQL interval cannot be represented as SqlInterval without losing fields or precision.", failure);
			}
		}
	}

	private static Optional<SqlInterval> readPostgres(ResultSet rows, int index) throws SQLException {
		String text = rows.getString(index);
		boolean wasNull = rows.wasNull();
		if (text != null) {
			isSqlNull(text, wasNull);
			return Optional.of(SqlIntervalText.parsePostgres(text));
		}
		if (wasNull) return Optional.empty();
		// pgjdbc does not implement textual conversion for its forced binary interval result. The
		// positive interval metadata gate above makes this exact wire-format fallback unambiguous.
		byte[] bytes = rows.getBytes(index);
		if (rows.wasNull() || bytes == null || bytes.length != 16)
			throw new DatabaseException("Non-NULL PostgreSQL interval has no valid 16-byte binary representation.");
		ByteBuffer input = ByteBuffer.wrap(bytes);
		long micros = input.getLong();
		int days = input.getInt(), months = input.getInt();
		SqlInterval value = SqlInterval.of(months, days, SqlIntervalText.fromMicroseconds(micros));
		SqlIntervalText.rejectPostgresInfinity(value);
		return Optional.of(value);
	}

	private static Optional<SqlInterval> readNative(ResultSet rows, int index, Family family) throws SQLException {
		if (family == Family.YEAR_MONTH) {
			Object raw = rows.getObject(index, Period.class);
			if (isSqlNull(raw, rows.wasNull())) return Optional.empty();
			if (!(raw instanceof Period period) || period.getDays() != 0)
				throw new DatabaseException("The driver did not return an exact YEAR TO MONTH Period.");
			return Optional.of(SqlInterval.of(period));
		}
		if (family == Family.DAY_SECOND) {
			Object raw = rows.getObject(index, Duration.class);
			if (isSqlNull(raw, rows.wasNull())) return Optional.empty();
			if (!(raw instanceof Duration duration))
				throw new DatabaseException("The driver did not return an exact DAY TO SECOND Duration.");
			return Optional.of(SqlInterval.of(duration));
		}
		throw unsupportedColumn();
	}

	private static boolean isSqlNull(@Nullable Object value, boolean wasNull) {
		if ((value == null) != wasNull)
			throw new DatabaseException("SQL interval getter returned inconsistent SQL NULL evidence.");
		return wasNull;
	}

	private static DatabaseException unsupportedColumn() {
		return new DatabaseException("SqlInterval requires a positively identified supported interval column and family. "
				+ "Text/numeric storage and unknown driver families require a CustomColumnMapper.");
	}

	private static Family family(DatabaseType databaseType, @Nullable Integer sqlType, @Nullable String typeName) {
		return familyFromName(databaseType, sqlType, nameFamily(typeName));
	}

	private static Family familyFromName(DatabaseType databaseType, @Nullable Integer sqlType, Family name) {
		if (sqlType != null && (sqlType == Types.ARRAY || sqlType == Types.STRUCT)) return Family.NON_INTERVAL;
		Family code = databaseType == DatabaseType.ORACLE && sqlType != null
				? switch (sqlType) {
					case -103 -> Family.YEAR_MONTH;
					case -104 -> Family.DAY_SECOND;
					case -100, -101, -102 -> Family.NON_INTERVAL; // Oracle timestamp variants
					default -> Family.UNKNOWN;
				}
				: Family.UNKNOWN;
		if (code == Family.UNKNOWN && contradictsInterval(sqlType)) code = Family.NON_INTERVAL;
		if (code == Family.UNKNOWN) return name;
		if (code == Family.NON_INTERVAL) return name.isInterval() ? Family.CONFLICT : Family.NON_INTERVAL;
		if (name == Family.UNKNOWN || name == Family.UNIFIED || name == code) return code;
		return Family.CONFLICT;
	}

	private static boolean contradictsInterval(@Nullable Integer sqlType) {
		if (sqlType == null) return false;
		switch (sqlType) {
			// HSQLDB reports genuine native intervals as VARCHAR. Other opaque, domain and text
			// codes do not establish a conflicting family; the positive interval name is still required.
			case Types.OTHER, Types.JAVA_OBJECT, Types.DISTINCT, Types.NULL,
					Types.CHAR, Types.VARCHAR, Types.LONGVARCHAR, Types.NCHAR, Types.NVARCHAR, Types.LONGNVARCHAR,
					// Oracle-specific evidence is resolved above only for an Oracle context. Other
					// contexts treat these as unknown vendor codes without allocating lookup failures.
					-100, -101, -102, -103, -104:
				return false;
			default:
				try {
					JDBCType.valueOf(sqlType);
					return true;
				} catch (IllegalArgumentException vendorType) {
					// Unrecognized vendor codes carry no portable type evidence. A recognized native
					// interval name can still identify a driver-specific representation.
					return false;
				}
		}
	}

	private static Family nameFamily(@Nullable String typeName) {
		if (typeName == null || typeName.isBlank()) return Family.UNKNOWN;
		String name = WHITESPACE.matcher(typeName.trim().toUpperCase(Locale.ROOT)).replaceAll(" ");
		if (name.equals("INTERVAL")) return Family.UNIFIED;
		if (name.equals("INTERVALYM") || YEAR_MONTH_NAME.matcher(name).matches()) return Family.YEAR_MONTH;
		if (name.equals("INTERVALDS") || DAY_SECOND_NAME.matcher(name).matches()) return Family.DAY_SECOND;
		return Family.NON_INTERVAL;
	}

	private enum Family {
		UNKNOWN, NON_INTERVAL, CONFLICT, UNIFIED, YEAR_MONTH, DAY_SECOND;
		boolean isInterval() { return this == UNIFIED || this == YEAR_MONTH || this == DAY_SECOND; }
	}
}
