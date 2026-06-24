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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.sql.SQLRecoverableException;
import java.sql.SQLTimeoutException;
import java.sql.SQLTransientConnectionException;
import java.util.List;

/**
 * @author <a href="https://www.revetkn.com">Mark Allen</a>
 * @since 4.3.0
 */
public class DatabaseExceptionClassificationTests {
	@Test
	public void testGenericSqlStateClassification() {
		DatabaseException unique = databaseException(DatabaseType.GENERIC, new SQLException("duplicate", "23505", 0));
		Assertions.assertTrue(unique.isUniqueConstraintViolation());
		Assertions.assertFalse(unique.isForeignKeyViolation());
		Assertions.assertFalse(unique.isDeadlock());
		Assertions.assertFalse(unique.isTransient());

		DatabaseException foreignKey = databaseException(DatabaseType.GENERIC, new SQLException("foreign key", "23503", 0));
		Assertions.assertFalse(foreignKey.isUniqueConstraintViolation());
		Assertions.assertTrue(foreignKey.isForeignKeyViolation());
		Assertions.assertFalse(foreignKey.isDeadlock());
		Assertions.assertFalse(foreignKey.isTransient());

		DatabaseException connection = databaseException(DatabaseType.GENERIC, new SQLException("connection", "08006", 0));
		Assertions.assertTrue(connection.isTransient());

		DatabaseException serialization = databaseException(DatabaseType.GENERIC, new SQLException("serialization", "40001", 0));
		Assertions.assertFalse(serialization.isDeadlock());
		Assertions.assertTrue(serialization.isSerializationFailure());
		Assertions.assertTrue(serialization.isTransient());

		DatabaseException timeout = databaseException(DatabaseType.GENERIC, new SQLTimeoutException("timeout"));
		Assertions.assertTrue(timeout.isTimeout());

		DatabaseException syntax = databaseException(DatabaseType.GENERIC, new SQLException("syntax", "42000", 0));
		assertUnclassified(syntax);
	}

	@Test
	public void testJdbcTransientAndRecoverableExceptionsAreTransient() {
		Assertions.assertTrue(new DatabaseException("connection", new SQLTransientConnectionException("connection"),
				DatabaseType.GENERIC.dialect()).isTransient());
		Assertions.assertTrue(new DatabaseException("recoverable", new SQLRecoverableException("recoverable"),
				DatabaseType.GENERIC.dialect()).isTransient());
	}

	@Test
	public void testPostgreSqlClassification() {
		DatabaseException deadlock = databaseException(DatabaseType.POSTGRESQL,
				new SQLException("deadlock", "40P01", 0));

		Assertions.assertTrue(deadlock.isDeadlock());
		Assertions.assertFalse(deadlock.isSerializationFailure());
		Assertions.assertTrue(deadlock.isTransient());

		DatabaseException serialization = databaseException(DatabaseType.POSTGRESQL,
				new SQLException("serialization", "40001", 0));
		Assertions.assertTrue(serialization.isSerializationFailure());

		DatabaseException timeout = databaseException(DatabaseType.POSTGRESQL,
				new SQLException("timeout", "57014", 0));
		Assertions.assertTrue(timeout.isTimeout());
		Assertions.assertTrue(databaseException(DatabaseType.POSTGRESQL, new SQLTimeoutException("driver timeout"))
				.isTimeout());
	}

	@Test
	public void testMySqlFamilyClassification() {
		for (DatabaseType databaseType : List.of(DatabaseType.MYSQL, DatabaseType.MARIA_DB)) {
			DatabaseException unique = databaseException(databaseType, new SQLException("duplicate", "23000", 1062));
			Assertions.assertTrue(unique.isUniqueConstraintViolation());
			Assertions.assertFalse(unique.isForeignKeyViolation());

			Assertions.assertTrue(databaseException(databaseType, new SQLException("parent", "23000", 1451))
					.isForeignKeyViolation());
			Assertions.assertTrue(databaseException(databaseType, new SQLException("child", "23000", 1452))
					.isForeignKeyViolation());

			DatabaseException deadlock = databaseException(databaseType, new SQLException("deadlock", "40001", 1213));
			Assertions.assertTrue(deadlock.isDeadlock());
			Assertions.assertFalse(deadlock.isSerializationFailure());
			Assertions.assertTrue(deadlock.isTransient());

			DatabaseException serialization = databaseException(databaseType, new SQLException("serialization", "40001", 0));
			Assertions.assertTrue(serialization.isSerializationFailure());

			DatabaseException lockTimeout = databaseException(databaseType, new SQLException("lock timeout", "HY000", 1205));
			Assertions.assertFalse(lockTimeout.isDeadlock());
			Assertions.assertTrue(lockTimeout.isTimeout());
			Assertions.assertTrue(lockTimeout.isTransient());

			Assertions.assertTrue(databaseException(databaseType, new SQLException("execution timeout", "HY000", 3024))
					.isTimeout());

			DatabaseException broadIntegrity = databaseException(databaseType, new SQLException("constraint", "23000", 0));
			Assertions.assertFalse(broadIntegrity.isUniqueConstraintViolation());
			Assertions.assertFalse(broadIntegrity.isForeignKeyViolation());
		}
	}

	@Test
	public void testSqlServerClassificationDoesNotOverclaimForeignKeyViolations() {
		Assertions.assertTrue(databaseException(DatabaseType.SQL_SERVER, new SQLException("unique", null, 2627))
				.isUniqueConstraintViolation());
		Assertions.assertTrue(databaseException(DatabaseType.SQL_SERVER, new SQLException("unique index", null, 2601))
				.isUniqueConstraintViolation());

		DatabaseException deadlock = databaseException(DatabaseType.SQL_SERVER, new SQLException("deadlock", null, 1205));
		Assertions.assertTrue(deadlock.isDeadlock());
		Assertions.assertFalse(deadlock.isSerializationFailure());
		Assertions.assertTrue(deadlock.isTransient());

		Assertions.assertTrue(databaseException(DatabaseType.SQL_SERVER, new SQLException("snapshot", null, 3960))
				.isSerializationFailure());

		DatabaseException lockTimeout = databaseException(DatabaseType.SQL_SERVER, new SQLException("lock timeout", null, 1222));
		Assertions.assertFalse(lockTimeout.isDeadlock());
		Assertions.assertTrue(lockTimeout.isTimeout());
		Assertions.assertTrue(lockTimeout.isTransient());

		Assertions.assertTrue(databaseException(DatabaseType.SQL_SERVER, new SQLException("query timeout", null, -2))
				.isTimeout());

		DatabaseException broadConstraint = databaseException(DatabaseType.SQL_SERVER, new SQLException("constraint", null, 547));
		Assertions.assertFalse(broadConstraint.isForeignKeyViolation());
	}

	@Test
	public void testOracleClassification() {
		Assertions.assertTrue(databaseException(DatabaseType.ORACLE, new SQLException("unique", null, 1))
				.isUniqueConstraintViolation());
		Assertions.assertTrue(databaseException(DatabaseType.ORACLE, new SQLException("parent", null, 2291))
				.isForeignKeyViolation());
		Assertions.assertTrue(databaseException(DatabaseType.ORACLE, new SQLException("child", null, 2292))
				.isForeignKeyViolation());

		DatabaseException deadlock = databaseException(DatabaseType.ORACLE, new SQLException("deadlock", null, 60));
		Assertions.assertTrue(deadlock.isDeadlock());
		Assertions.assertTrue(deadlock.isTransient());

		DatabaseException serialization = databaseException(DatabaseType.ORACLE, new SQLException("serialization", null, 8177));
		Assertions.assertFalse(serialization.isDeadlock());
		Assertions.assertTrue(serialization.isSerializationFailure());
		Assertions.assertTrue(serialization.isTransient());

		Assertions.assertTrue(databaseException(DatabaseType.ORACLE, new SQLException("cancel", null, 1013)).isTimeout());
		Assertions.assertTrue(databaseException(DatabaseType.ORACLE, new SQLException("timeout", null, 51)).isTimeout());
	}

	@Test
	public void testSqliteClassificationDoesNotOverclaimBaseConstraintCode() {
		DatabaseException baseConstraint = databaseException(DatabaseType.SQLITE, new SQLException("constraint", null, 19));
		Assertions.assertFalse(baseConstraint.isUniqueConstraintViolation());
		Assertions.assertFalse(baseConstraint.isForeignKeyViolation());

		Assertions.assertTrue(databaseException(DatabaseType.SQLITE, new SQLException("busy", null, 5)).isTransient());
		Assertions.assertTrue(databaseException(DatabaseType.SQLITE, new SQLException("locked", null, 6)).isTransient());
		Assertions.assertTrue(databaseException(DatabaseType.SQLITE, new SQLException("busy", null, 5)).isTimeout());
		Assertions.assertTrue(databaseException(DatabaseType.SQLITE, new SQLException("locked", null, 6)).isTimeout());
		Assertions.assertFalse(databaseException(DatabaseType.SQLITE, new SQLException("busy", null, 5)).isSerializationFailure());
	}

	@Test
	public void testClassificationInspectsNextSqlException() {
		SQLException batch = new SQLException("batch failed");
		batch.setNextException(new SQLException("duplicate", "23000", 1062));

		Assertions.assertTrue(databaseException(DatabaseType.MYSQL, batch).isUniqueConstraintViolation());
	}

	@NonNull
	private DatabaseException databaseException(@NonNull DatabaseType databaseType,
																						 @NonNull SQLException cause) {
		return new DatabaseException(cause.getMessage(), cause, databaseType.dialect());
	}

	private void assertUnclassified(@NonNull DatabaseException exception) {
		Assertions.assertEquals(Boolean.FALSE, exception.isUniqueConstraintViolation());
		Assertions.assertEquals(Boolean.FALSE, exception.isForeignKeyViolation());
		Assertions.assertEquals(Boolean.FALSE, exception.isDeadlock());
		Assertions.assertEquals(Boolean.FALSE, exception.isTransient());
		Assertions.assertEquals(Boolean.FALSE, exception.isSerializationFailure());
		Assertions.assertEquals(Boolean.FALSE, exception.isTimeout());
	}
}
