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

import org.jspecify.annotations.Nullable;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/** JDBC cleanup that preserves the original failure when drivers reuse a throwable instance. */
final class JdbcResources {
	private JdbcResources() {}

	static void close(@Nullable Statement resource, @Nullable Throwable primary) throws SQLException {
		if (resource == null) return;
		try {
			resource.close();
		} catch (SQLException | RuntimeException | Error failure) {
			if (primary == null) throw failure;
			if (primary != failure) primary.addSuppressed(failure);
		}
	}

	static void close(@Nullable ResultSet resource, @Nullable Throwable primary) throws SQLException {
		if (resource == null) return;
		try {
			resource.close();
		} catch (SQLException | RuntimeException | Error failure) {
			if (primary == null) throw failure;
			if (primary != failure) primary.addSuppressed(failure);
		}
	}
}
