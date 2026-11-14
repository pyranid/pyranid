/*
 * Copyright 2015-2022 Transmogrify LLC, 2022-2026 Revetware LLC.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may obtain a copy at https://www.apache.org/licenses/LICENSE-2.0
 */
package com.pyranid;

import org.hsqldb.jdbc.JDBCDataSource;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class BeanPropertyNamingTests {
	public record Names(String sha256Hash, String s3Key, String ipv4Address, String oauth2Token,
			String employeeId, String URLValue, String address1) {}
	public record Employee(String employeeId) {}
	public record FanOut(String employeeId, String employeeid) {}
	public record Aliased(@DatabaseColumn("external2Code") String code) {}
	public static class NamesBean {
		private String sha256Hash, s3Key, ipv4Address, oauth2Token, employeeId, URLValue, address1;
		public void setSha256Hash(String value) { sha256Hash = value; }
		public void setS3Key(String value) { s3Key = value; }
		public void setIpv4Address(String value) { ipv4Address = value; }
		public void setOauth2Token(String value) { oauth2Token = value; }
		public void setEmployeeId(String value) { employeeId = value; }
		public void setURLValue(String value) { URLValue = value; }
		public void setAddress1(String value) { address1 = value; }
		Names values() { return new Names(sha256Hash, s3Key, ipv4Address, oauth2Token, employeeId, URLValue, address1); }
	}
	public static class AliasedBean {
		@DatabaseColumn("external2Code") private String code;
		public void setCode(String value) { code = value; }
		public String getCode() { return code; }
	}

	@Test
	void literalLegacyAndDigitWordCandidatesAreAllRetained() {
		DefaultResultSetMapper mapper = (DefaultResultSetMapper) ResultSetMapper.withDefaultConfiguration();
		assertCandidates(mapper, "employeeId", "employeeid", "employee_id");
		assertCandidates(mapper, "sha256Hash", "sha256hash", "sha_256hash", "sha256_hash", "sha_256_hash");
		assertCandidates(mapper, "s3Key", "s3key", "s_3key", "s3_key", "s_3_key");
		assertCandidates(mapper, "ipv4Address", "ipv4address", "ipv_4address", "ipv4_address", "ipv_4_address");
		assertCandidates(mapper, "oauth2Token", "oauth2token", "oauth_2token", "oauth2_token", "oauth_2_token");
		assertCandidates(mapper, "URLValue", "urlvalue", "url_value");
		assertCandidates(mapper, "OAuth2Token", "oauth2token", "oauth2_token", "o_auth2_token");
		assertCandidates(mapper, "address1", "address1", "address_1");
	}

	@Test
	void digitWordAndAcronymSnakeColumnsPopulateRecordsAndBeansWithEitherCacheMode() {
		Names expected = expected();
		for (boolean cached : new boolean[]{false, true}) {
			Database database = database(cached);
			for (int pass = 0; pass < 2; pass++) {
				String sql = projection("sha256_hash", "s3_key", "ipv4_address", "oauth2_token", "employee_id", "url_value", "address1");
				assertEquals(expected, database.query(sql).fetchObject(Names.class).orElseThrow());
				assertEquals(expected, database.query(sql).fetchObject(NamesBean.class).orElseThrow().values());
			}
		}
	}

	@Test
	void literalJavaAliasesAndHistoricalSpellingsRemainAvailableAcrossSchemas() {
		for (boolean cached : new boolean[]{false, true}) {
			Database database = database(cached);
			for (String sql : List.of(
					projection("sha256Hash", "s3Key", "ipv4Address", "oauth2Token", "employeeId", "URLValue", "address1"),
					projection("sha_256hash", "s_3key", "ipv_4address", "oauth_2token", "employee_id", "urlvalue", "address_1"),
					projection("sha_256_hash", "s_3_key", "ipv_4_address", "oauth_2_token", "employee_id", "url_value", "address_1"))) {
				assertEquals(expected(), database.query(sql).fetchObject(Names.class).orElseThrow());
				assertEquals(expected(), database.query(sql).fetchObject(NamesBean.class).orElseThrow().values());
			}
		}
	}

	@Test
	void aSingleLiteralPropertyAliasIsRecognizedAsAnOrdinaryProjection() {
		for (boolean cached : new boolean[]{false, true}) {
			Database database = database(cached);
			assertEquals(new Employee("mapped"), database.query("SELECT 'mapped' AS \"employeeId\" FROM (VALUES (0))")
					.fetchObject(Employee.class).orElseThrow());
			assertEquals("mapped", database.query("SELECT 'mapped' AS \"employeeId\" FROM (VALUES (0))")
					.fetchObject(NamesBean.class).orElseThrow().values().employeeId());
		}
	}

	@Test
	void explicitAnnotationAliasesKeepTheirLiteralAndHistoricalDigitSpellings() {
		for (boolean cached : new boolean[]{false, true}) {
			Database database = database(cached);
			for (String label : List.of("external2Code", "EXTERNAL2CODE", "external_2code")) {
				String sql = "SELECT 'mapped' AS \"" + label + "\" FROM (VALUES (0))";
				assertEquals(new Aliased("mapped"), database.query(sql).fetchObject(Aliased.class).orElseThrow());
				assertEquals("mapped", database.query(sql).fetchObject(AliasedBean.class).orElseThrow().getCode());
			}
		}
	}

	@Test
	void intentionalFanOutAndDuplicateNormalizedLabelProtectionRemainInForce() {
		for (boolean cached : new boolean[]{false, true}) {
			Database database = database(cached);
			assertEquals(new FanOut("shared", "shared"), database.query("SELECT 'shared' AS employeeid FROM (VALUES (0))")
					.fetchObject(FanOut.class).orElseThrow());
			DatabaseException failure = assertThrows(DatabaseException.class, () -> database.query(
					"SELECT 'one' AS \"employeeId\", 'two' AS \"employeeid\" FROM (VALUES (0))").fetchObject(Employee.class));
			assertTrue(failure.getMessage().contains("Duplicate column label"));
		}
	}

	private static void assertCandidates(DefaultResultSetMapper mapper, String property, String... names) {
		Set<String> actual = mapper.databaseColumnNamesForPropertyName(property);
		assertTrue(actual.containsAll(List.of(names)), () -> property + " aliases: " + actual);
	}
	private static Names expected() { return new Names("sha", "bucket", "network", "oauth", "employee", "url", "address"); }
	private static String projection(String... names) {
		String[] values = {"sha", "bucket", "network", "oauth", "employee", "url", "address"};
		StringBuilder sql = new StringBuilder("SELECT ");
		for (int index = 0; index < names.length; index++) {
			if (index > 0) sql.append(", ");
			sql.append('\'').append(values[index]).append("' AS \"").append(names[index]).append('"');
		}
		return sql.append(" FROM (VALUES (0))").toString();
	}
	private static Database database(boolean cached) {
		JDBCDataSource source = new JDBCDataSource();
		source.setUrl("jdbc:hsqldb:mem:bean_names_" + UUID.randomUUID()); source.setUser("sa"); source.setPassword("");
		return Database.with(source, DatabaseType.GENERIC).resultSetMapper(ResultSetMapper.withPlanCachingEnabled(cached).build()).build();
	}
}
