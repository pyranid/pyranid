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

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.Locale;

import static java.util.Objects.requireNonNull;

/**
 * Identifies different types of databases, which allows for special platform-specific handling.
 *
 * @author <a href="https://www.revetkn.com">Mark Allen</a>
 * @since 1.0.0
 */
public enum DatabaseType {
	/**
	 * A database which requires no special handling.
	 */
	GENERIC,
	/**
	 * A PostgreSQL database.
	 */
	POSTGRESQL,
	/**
	 * An Oracle database.
	 */
	ORACLE,
	/**
	 * A MySQL database.
	 *
	 * @since 4.3.0
	 */
	MYSQL,
	/**
	 * A MariaDB database.
	 *
	 * @since 4.3.0
	 */
	MARIADB,
	/**
	 * A SQLite database.
	 *
	 * @since 4.3.0
	 */
	SQLITE,
	/**
	 * A Microsoft SQL Server database.
	 *
	 * @since 4.3.0
	 */
	SQL_SERVER;

	/**
	 * Determines the type of database to which the given {@code dataSource} connects.
	 * <p>
	 * Note: this will establish a {@link Connection} to the database.
	 *
	 * @param dataSource the database connection factory
	 * @return the type of database
	 * @throws DatabaseException if an exception occurs while attempting to read database metadata
	 */
	@NonNull
	public static DatabaseType fromDataSource(@NonNull DataSource dataSource) {
		requireNonNull(dataSource);
		
		try (Connection connection = dataSource.getConnection()) {
			return fromConnection(connection);
		} catch (SQLException e) {
			throw new DatabaseException("Unable to connect to database to determine its type", e);
		}
	}

	/**
	 * Determines the type of database represented by the given {@code connection}.
	 *
	 * @param connection an active database connection
	 * @return the type of database
	 * @throws DatabaseException if an exception occurs while attempting to read database metadata
	 */
	@NonNull
	public static DatabaseType fromConnection(@NonNull Connection connection) {
		requireNonNull(connection);

		try {
			DatabaseMetaData databaseMetaData = connection.getMetaData();
			String databaseProductName = databaseMetaData.getDatabaseProductName();
			String databaseProductVersion = databaseProductVersion(databaseMetaData);
			String url = databaseMetaData.getURL();
			String driverName = databaseMetaData.getDriverName();

			// All of our checks are against databases with English names
			String databaseProductNameLowercase = databaseProductName == null ? "" : databaseProductName.toLowerCase(Locale.ENGLISH);
			String databaseProductVersionLowercase = databaseProductVersion == null ? "" : databaseProductVersion.toLowerCase(Locale.ENGLISH);
			String urlLowercase = url == null ? "" : url.toLowerCase(Locale.ENGLISH);
			String driverNameLowercase = driverName == null ? "" : driverName.toLowerCase(Locale.ENGLISH);

			// Prefer product name
			if (databaseProductNameLowercase.startsWith("oracle"))
				return DatabaseType.ORACLE;

			// Strict match for PostgreSQL
			if (databaseProductNameLowercase.contains("postgresql") || databaseProductNameLowercase.equals("postgres"))  // some proxies shorten it
				return DatabaseType.POSTGRESQL;

			if (databaseProductNameLowercase.contains("mariadb"))
				return DatabaseType.MARIADB;

			if (databaseProductNameLowercase.contains("mysql"))
				return mysqlFamilyDatabaseType(databaseProductVersionLowercase, driverNameLowercase);

			if (databaseProductNameLowercase.contains("sqlite"))
				return DatabaseType.SQLITE;

			if (isSqlServerProductName(databaseProductNameLowercase))
				return DatabaseType.SQL_SERVER;

			// Fallbacks if product name is absent/weird but we're clearly using a vendor driver/URL
			if (urlLowercase.startsWith("jdbc:postgresql:") || driverNameLowercase.contains("postgresql"))
				return DatabaseType.POSTGRESQL;

			if (urlLowercase.startsWith("jdbc:oracle:") || driverNameLowercase.contains("oracle jdbc"))
				return DatabaseType.ORACLE;

			if (urlLowercase.startsWith("jdbc:mariadb:") || driverNameLowercase.contains("mariadb"))
				return DatabaseType.MARIADB;

			if (isMysqlUrl(urlLowercase) || driverNameLowercase.contains("mysql"))
				return mysqlFamilyDatabaseType(databaseProductVersionLowercase, driverNameLowercase);

			if (urlLowercase.startsWith("jdbc:sqlite:") || driverNameLowercase.contains("sqlite"))
				return DatabaseType.SQLITE;

			if (isSqlServerUrl(urlLowercase) || isSqlServerDriverName(driverNameLowercase))
				return DatabaseType.SQL_SERVER;

			return DatabaseType.GENERIC;
		} catch (SQLException e) {
			throw new DatabaseException("Unable to inspect database metadata to determine its type", e);
		}
	}

	@Nullable
	private static String databaseProductVersion(@NonNull DatabaseMetaData databaseMetaData) {
		requireNonNull(databaseMetaData);

		try {
			return databaseMetaData.getDatabaseProductVersion();
		} catch (SQLException e) {
			return null;
		}
	}

	@NonNull
	DatabaseDialect dialect() {
		return switch (this) {
			case POSTGRESQL -> PostgresDialect.INSTANCE;
			case ORACLE -> OracleDialect.INSTANCE;
			case MYSQL -> MySqlDialect.INSTANCE;
			case MARIADB -> MariaDbDialect.INSTANCE;
			case SQLITE -> SqliteDialect.INSTANCE;
			case SQL_SERVER -> SqlServerDialect.INSTANCE;
			case GENERIC -> GenericDialect.INSTANCE;
		};
	}

	@NonNull
	private static DatabaseType mysqlFamilyDatabaseType(@NonNull String databaseProductVersionLowercase,
																										 @NonNull String driverNameLowercase) {
		requireNonNull(databaseProductVersionLowercase);
		requireNonNull(driverNameLowercase);

		if (databaseProductVersionLowercase.contains("mariadb") || driverNameLowercase.contains("mariadb"))
			return DatabaseType.MARIADB;

		return DatabaseType.MYSQL;
	}

	private static boolean isMysqlUrl(@NonNull String urlLowercase) {
		requireNonNull(urlLowercase);

		return urlLowercase.startsWith("jdbc:mysql:") || urlLowercase.startsWith("jdbc:mysql+srv:");
	}

	private static boolean isSqlServerProductName(@NonNull String databaseProductNameLowercase) {
		requireNonNull(databaseProductNameLowercase);

		return databaseProductNameLowercase.contains("microsoft sql server")
				|| databaseProductNameLowercase.equals("sql server")
				|| databaseProductNameLowercase.equals("sqlserver");
	}

	private static boolean isSqlServerUrl(@NonNull String urlLowercase) {
		requireNonNull(urlLowercase);

		return urlLowercase.startsWith("jdbc:sqlserver:") || urlLowercase.startsWith("jdbc:jtds:sqlserver:");
	}

	private static boolean isSqlServerDriverName(@NonNull String driverNameLowercase) {
		requireNonNull(driverNameLowercase);

		return driverNameLowercase.contains("microsoft jdbc driver")
				|| driverNameLowercase.contains("sql server")
				|| driverNameLowercase.contains("jtds");
	}
}
