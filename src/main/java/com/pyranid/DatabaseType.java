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
	ORACLE;

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
			DatabaseMetaData databaseMetaData = connection.getMetaData();
			String databaseProductName = databaseMetaData.getDatabaseProductName();
			String url = databaseMetaData.getURL();
			String driverName = databaseMetaData.getDriverName();

			// All of our checks are against databases with English names
			String databaseProductNameLowercase = databaseProductName == null ? "" : databaseProductName.toLowerCase(Locale.ENGLISH);
			String urlLowercase = url == null ? "" : url.toLowerCase(Locale.ENGLISH);
			String driverNameLowercase = driverName == null ? "" : driverName.toLowerCase(Locale.ENGLISH);

			// Prefer product name
			if (databaseProductNameLowercase.startsWith("oracle"))
				return DatabaseType.ORACLE;

			// Strict match for PostgreSQL
			if (databaseProductNameLowercase.contains("postgresql") || databaseProductNameLowercase.equals("postgres"))  // some proxies shorten it
				return DatabaseType.POSTGRESQL;

			// Fallbacks if product name is absent/weird but we're clearly using the PG driver/URL
			if (urlLowercase.startsWith("jdbc:postgresql:") || driverNameLowercase.contains("postgresql"))
				return DatabaseType.POSTGRESQL;

			return DatabaseType.GENERIC;
		} catch (SQLException e) {
			throw new DatabaseException("Unable to connect to database to determine its type", e);
		}
	}
}