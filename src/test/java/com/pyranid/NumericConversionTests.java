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

import org.hsqldb.jdbc.JDBCDataSource;
import org.jspecify.annotations.NonNull;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
import java.math.BigInteger;
import java.util.Optional;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * @author <a href="https://www.revetkn.com">Mark Allen</a>
 * @since 4.1.0
 */
public class NumericConversionTests {
	@Test
	public void testBigintToIntegerOverflowThrows() {
		Database db = Database.withDataSource(createInMemoryDataSource("numeric_int_overflow")).build();

		Assertions.assertThrows(DatabaseException.class, () ->
				db.query("SELECT CAST(1099511627776 AS BIGINT) FROM (VALUES (0)) AS t(x)").fetchObject(Integer.class));
	}

	@Test
	public void testFractionalNumericToBigIntegerThrows() {
		Database db = Database.withDataSource(createInMemoryDataSource("numeric_bigint_fractional")).build();

		Assertions.assertThrows(DatabaseException.class, () ->
				db.query("SELECT CAST(100.50 AS DECIMAL(10, 2)) FROM (VALUES (0)) AS t(x)").fetchObject(BigInteger.class));
	}

	@Test
	public void testFractionalNumericBooleanIsTrueWhenNonZero() {
		Database db = Database.withDataSource(createInMemoryDataSource("numeric_boolean_fractional")).build();

		Optional<Boolean> value = db.query("SELECT CAST(0.5 AS DECIMAL(10, 1)) FROM (VALUES (0)) AS t(x)")
				.fetchObject(Boolean.class);

		Assertions.assertEquals(Optional.of(Boolean.TRUE), value);
	}

	@Test
	public void testFractionalNumericToCharacterThrows() {
		Database db = Database.withDataSource(createInMemoryDataSource("numeric_char_fractional")).build();

		Assertions.assertThrows(DatabaseException.class, () ->
				db.query("SELECT CAST(65.5 AS DECIMAL(10, 1)) FROM (VALUES (0)) AS t(x)").fetchObject(Character.class));
	}

	@Test
	public void testOutOfRangeNumericToCharacterThrows() {
		Database db = Database.withDataSource(createInMemoryDataSource("numeric_char_out_of_range")).build();

		Assertions.assertThrows(DatabaseException.class, () ->
				db.query("SELECT 70000 FROM (VALUES (0)) AS t(x)").fetchObject(Character.class));
	}

	@Test
	public void testNonFiniteDoubleToDoubleThrows() {
		Database db = Database.withDataSource(createInMemoryDataSource("numeric_double_non_finite")).build();

		Assertions.assertThrows(DatabaseException.class, () ->
				db.query("SELECT :value FROM (VALUES (0)) AS t(x)")
						.bind("value", Double.POSITIVE_INFINITY)
						.fetchObject(Double.class));
	}

	@Test
	public void testNonFiniteDoubleToFloatThrows() {
		Database db = Database.withDataSource(createInMemoryDataSource("numeric_float_non_finite")).build();

		Assertions.assertThrows(DatabaseException.class, () ->
				db.query("SELECT :value FROM (VALUES (0)) AS t(x)")
						.bind("value", Double.POSITIVE_INFINITY)
						.fetchObject(Float.class));
	}

	@NonNull
	private DataSource createInMemoryDataSource(@NonNull String databaseName) {
		requireNonNull(databaseName);

		JDBCDataSource dataSource = new JDBCDataSource();
		dataSource.setUrl(format("jdbc:hsqldb:mem:%s", databaseName));
		dataSource.setUser("sa");
		dataSource.setPassword("");

		return dataSource;
	}
}
