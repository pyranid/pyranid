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

import java.time.Year;
import java.time.YearMonth;
import java.util.List;
import java.util.Optional;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * Verifies the 4.5.0 {@link Year} (INT) and {@link YearMonth} (ISO string) scalar support.
 *
 * @author <a href="https://www.revetkn.com">Mark Allen</a>
 */
public class YearAndYearMonthMappingTests {
	public record FiscalRow(Year fiscalYear, YearMonth expiryMonth) {}

	@Test
	public void testYearBindsAsIntAndRoundTrips() {
		Database db = createDatabase("year_roundtrip");
		db.query("CREATE TABLE fiscal (fiscal_year INTEGER)").execute();

		db.query("INSERT INTO fiscal (fiscal_year) VALUES (:year)")
				.bind("year", Year.of(2024))
				.execute();

		// Bound as a plain INT - readable by non-Year consumers
		Assertions.assertEquals(2024, db.query("SELECT fiscal_year FROM fiscal").fetchObject(Integer.class).orElseThrow());

		// And round-trips as Year
		Assertions.assertEquals(Year.of(2024),
				db.query("SELECT fiscal_year FROM fiscal").fetchObject(Year.class).orElseThrow());
	}

	@Test
	public void testYearMonthBindsAsIsoStringAndRoundTrips() {
		Database db = createDatabase("yearmonth_roundtrip");
		db.query("CREATE TABLE expiry (expiry_month VARCHAR(16))").execute();

		db.query("INSERT INTO expiry (expiry_month) VALUES (:month)")
				.bind("month", YearMonth.of(2027, 12))
				.execute();

		// Bound as the ISO-8601 string form - readable and sortable by non-YearMonth consumers
		Assertions.assertEquals("2027-12",
				db.query("SELECT expiry_month FROM expiry").fetchObject(String.class).orElseThrow());

		Assertions.assertEquals(YearMonth.of(2027, 12),
				db.query("SELECT expiry_month FROM expiry").fetchObject(YearMonth.class).orElseThrow());
	}

	@Test
	public void testYearFromStringColumn() {
		Database db = createDatabase("year_from_string");

		Assertions.assertEquals(Year.of(1999),
				db.query("SELECT '1999' FROM (VALUES (0)) AS t(x)").fetchObject(Year.class).orElseThrow());
	}

	@Test
	public void testRecordComponentsMapped() {
		Database db = createDatabase("year_record");
		db.query("CREATE TABLE card (fiscal_year INTEGER, expiry_month VARCHAR(16))").execute();
		db.query("INSERT INTO card VALUES (:y, :m)")
				.bind("y", Year.of(2025))
				.bind("m", YearMonth.of(2028, 3))
				.execute();

		FiscalRow row = db.query("SELECT fiscal_year, expiry_month FROM card")
				.fetchObject(FiscalRow.class)
				.orElseThrow();

		Assertions.assertEquals(Year.of(2025), row.fiscalYear());
		Assertions.assertEquals(YearMonth.of(2028, 3), row.expiryMonth());
	}

	@Test
	public void testYearFromDateSurfacedColumn() {
		// Some drivers surface year columns as DATE - e.g. MySQL Connector/J maps YEAR to DATE by default
		Database db = createDatabase("year_from_date");
		db.query("CREATE TABLE vintages (v DATE)").execute();
		db.query("INSERT INTO vintages VALUES (:v)").bind("v", java.time.LocalDate.of(2024, 1, 1)).execute();

		Assertions.assertEquals(Year.of(2024),
				db.query("SELECT v FROM vintages").fetchObject(Year.class).orElseThrow());

		// Property path receives LocalDate via dialect-aware extraction
		record VintageRow(Year v) {}
		Assertions.assertEquals(Year.of(2024),
				db.query("SELECT v FROM vintages").fetchObject(VintageRow.class).orElseThrow().v());
	}

	@Test
	public void testNullHandling() {
		Database db = createDatabase("year_nulls");
		db.query("CREATE TABLE nullable_years (y INTEGER, m VARCHAR(16))").execute();
		db.query("INSERT INTO nullable_years VALUES (:y, :m)").bind("y", null).bind("m", null).execute();

		Optional<Year> year = db.query("SELECT y FROM nullable_years").fetchObject(Year.class);
		Optional<YearMonth> yearMonth = db.query("SELECT m FROM nullable_years").fetchObject(YearMonth.class);

		Assertions.assertTrue(year.isEmpty(), "Expected NULL to map to empty Optional for Year");
		Assertions.assertTrue(yearMonth.isEmpty(), "Expected NULL to map to empty Optional for YearMonth");
	}

	@Test
	public void testUnparseableValuesFailClearly() {
		Database db = createDatabase("year_unparseable");

		DatabaseException yearFailure = Assertions.assertThrows(DatabaseException.class, () ->
				db.query("SELECT 'not-a-year' FROM (VALUES (0)) AS t(x)").fetchObject(Year.class));
		Assertions.assertTrue(yearFailure.getMessage().contains("Year"),
				format("Expected a Year conversion diagnostic but got: %s", yearFailure.getMessage()));

		DatabaseException yearMonthFailure = Assertions.assertThrows(DatabaseException.class, () ->
				db.query("SELECT 'not-a-month' FROM (VALUES (0)) AS t(x)").fetchObject(YearMonth.class));
		Assertions.assertTrue(yearMonthFailure.getMessage().contains("YearMonth"),
				format("Expected a YearMonth conversion diagnostic but got: %s", yearMonthFailure.getMessage()));
	}

	@Test
	public void testYearInListExpansion() {
		Database db = createDatabase("year_inlist");
		db.query("CREATE TABLE reports (fiscal_year INTEGER)").execute();
		for (int y = 2020; y <= 2024; ++y)
			db.query("INSERT INTO reports VALUES (:y)").bind("y", Year.of(y)).execute();

		List<Year> matched = db.query("SELECT fiscal_year FROM reports WHERE fiscal_year IN (:years) ORDER BY fiscal_year")
				.bind("years", Parameters.inList(List.of(Year.of(2021), Year.of(2023))))
				.fetchList(Year.class);

		Assertions.assertEquals(List.of(Year.of(2021), Year.of(2023)), matched);
	}

	@NonNull
	private Database createDatabase(@NonNull String databaseName) {
		requireNonNull(databaseName);

		JDBCDataSource dataSource = new JDBCDataSource();
		dataSource.setUrl(format("jdbc:hsqldb:mem:%s", databaseName));
		dataSource.setUser("sa");
		dataSource.setPassword("");

		return Database.withDataSource(dataSource).build();
	}
}
