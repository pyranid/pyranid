/*
 * Copyright 2015-2022 Transmogrify LLC, 2022-2025 Revetware LLC.
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

import java.sql.DatabaseMetaData;
import java.sql.SQLException;

/**
 * Functional interface used by {@link Database#readDatabaseMetaData(DatabaseMetaDataReader)}, which permits callers to examine a transient {@link DatabaseMetaData} instance.
 *
 * @author <a href="https://www.revetkn.com">Mark Allen</a>
 * @since 3.0.0
 */
@FunctionalInterface
public interface DatabaseMetaDataReader {
	/**
	 * Examines a JDBC {@link DatabaseMetaData}, which provides comprehensive vendor-specific information about this database as a whole.
	 *
	 * @param databaseMetaData JBDC metadata for this database
	 * @throws SQLException if an error occurs while examining metadata
	 */
	void read(@NonNull DatabaseMetaData databaseMetaData) throws SQLException;
}