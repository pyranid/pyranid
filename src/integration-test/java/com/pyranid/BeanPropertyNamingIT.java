/*
 * Copyright 2015-2022 Transmogrify LLC, 2022-2026 Revetware LLC.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may obtain a copy at https://www.apache.org/licenses/LICENSE-2.0
 */
package com.pyranid;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class BeanPropertyNamingIT {
	@TempDir Path directory;
	public record Names(String sha256Hash, String s3Key, String ipv4Address, String oauth2Token,
			String employeeId, String URLValue) {}
	public record Payload(Names payload) {}
	public record LiteralPayload(Names employeeId) {}
	public record Employee(String employeeId) {}
	public static class NamesBean {
		private String sha256Hash, s3Key, ipv4Address, oauth2Token, employeeId, URLValue;
		public void setSha256Hash(String value) { sha256Hash = value; }
		public void setS3Key(String value) { s3Key = value; }
		public void setIpv4Address(String value) { ipv4Address = value; }
		public void setOauth2Token(String value) { oauth2Token = value; }
		public void setEmployeeId(String value) { employeeId = value; }
		public void setURLValue(String value) { URLValue = value; }
		Names values() { return new Names(sha256Hash, s3Key, ipv4Address, oauth2Token, employeeId, URLValue); }
	}

	@Test
	void sqliteDefaultColumnNamesAndLiteralAliasesPopulateTheSameRecordAndBeanValues() { verify(DatabaseType.SQLITE); }

	@Test
	void duckDbDefaultColumnNamesAndLiteralAliasesPopulateTheSameRecordAndBeanValues() { verify(DatabaseType.DUCK_DB); }

	@Test
	void duckDbNestedStructRecordsUseTheSameNamingCandidatesAndNormalProjectionPrecedence() {
		for (boolean cached : new boolean[]{false, true}) {
			Database database = database(DatabaseType.DUCK_DB, "nested_" + cached, cached);
			String value = "{'sha256_hash': 'sha', 's3_key': 'bucket', 'ipv4_address': 'network', "
					+ "'oauth2_token': 'oauth', 'employeeId': 'employee', 'url_value': 'url'}";
			assertEquals(expected(), database.query("SELECT " + value + " AS payload").fetchObject(Names.class).orElseThrow());
			assertEquals(new Payload(expected()), database.query("SELECT " + value + " AS payload").fetchObject(Payload.class).orElseThrow());
			// A column named exactly like the Java component must map as an ordinary projection,
			// before considering the direct single-STRUCT record fallback.
			assertEquals(new LiteralPayload(expected()), database.query("SELECT " + value + " AS \"employeeId\"")
					.fetchObject(LiteralPayload.class).orElseThrow());
			assertEquals(new Employee("mapped"), database.query("SELECT {'employeeId': 'mapped'} AS payload")
					.fetchObject(Employee.class).orElseThrow());
		}
	}

	private void verify(DatabaseType type) {
		for (boolean cached : new boolean[]{false, true}) {
			Database database = database(type, "rows_" + cached, cached);
			database.query("CREATE TABLE naming_rows (sha256_hash VARCHAR(20), s3_key VARCHAR(20), ipv4_address VARCHAR(20), "
					+ "oauth2_token VARCHAR(20), employee_id VARCHAR(20), url_value VARCHAR(20))").execute();
			database.query("INSERT INTO naming_rows VALUES ('sha', 'bucket', 'network', 'oauth', 'employee', 'url')").execute();
			for (int pass = 0; pass < 2; pass++) {
				for (String sql : List.of("SELECT * FROM naming_rows", "SELECT sha256_hash AS \"sha256Hash\", s3_key AS \"s3Key\", "
						+ "ipv4_address AS \"ipv4Address\", oauth2_token AS \"oauth2Token\", employee_id AS \"employeeId\", url_value AS \"URLValue\" FROM naming_rows")) {
					assertEquals(expected(), database.query(sql).fetchObject(Names.class).orElseThrow());
					assertEquals(expected(), database.query(sql).fetchObject(NamesBean.class).orElseThrow().values());
				}
			}
			assertEquals(new Employee("employee"), database.query("SELECT employee_id AS \"employeeId\" FROM naming_rows")
					.fetchObject(Employee.class).orElseThrow());
			DatabaseException failure = assertThrows(DatabaseException.class, () -> database.query(
					"SELECT employee_id AS \"employeeId\", employee_id AS \"employeeid\" FROM naming_rows").fetchObject(Employee.class));
			assertTrue(failure.getMessage().contains("Duplicate column label"));
		}
	}
	private static Names expected() { return new Names("sha", "bucket", "network", "oauth", "employee", "url"); }
	private Database database(DatabaseType type, String name, boolean cached) {
		String url = type == DatabaseType.DUCK_DB ? "jdbc:duckdb:" + directory.resolve(name + ".duckdb")
				: "jdbc:sqlite:" + directory.resolve(name + ".sqlite");
		return Database.with(new AbstractPortableJdbcIntegrationTests.DriverManagerDataSource(url, null, null), type)
				.resultSetMapper(ResultSetMapper.withPlanCachingEnabled(cached).build()).build();
	}
}
