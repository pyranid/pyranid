/*
 * Copyright 2015-2022 Transmogrify LLC, 2022-2026 Revetware LLC.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may obtain a copy at https://www.apache.org/licenses/LICENSE-2.0
 */
package com.pyranid;

import org.hsqldb.jdbc.JDBCDataSource;
import org.junit.jupiter.api.Test;
import org.postgresql.util.PSQLException;
import org.postgresql.util.ServerErrorMessage;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class DiagnosticPrivacyTests {
	public record RejectedRow(String passwordHash, String socialSecurityNumber) {
		public RejectedRow { throw new IllegalArgumentException("record validation failed"); }
	}

	@Test
	void automaticRenderingDoesNotReintroduceHiddenPostgresDiagnosticValues() {
		char separator = 0;
		String fields = "SERROR" + separator + "C23505" + separator + "Mduplicate key" + separator
				+ "Ddetail-secret" + separator + "Hhint-secret" + separator
				+ "qSELECT internal-secret" + separator + "Wcontext-secret" + separator + separator;
		PSQLException driver = new PSQLException(new ServerErrorMessage(fields), false);
		DatabaseException exception = new DatabaseException("Database operation failed", driver);
		assertEquals("detail-secret", exception.getDetail().orElseThrow());
		assertEquals("hint-secret", exception.getHint().orElseThrow());
		assertEquals("SELECT internal-secret", exception.getInternalQuery().orElseThrow());
		assertEquals("context-secret", exception.getWhere().orElseThrow());
		assertTrue(exception.isUniqueConstraintViolation());
		Database database = database(new ArrayList<>());
		StatementContext<?> context = StatementContext.with(Statement.of("privacy", "SELECT 1"), database).build();
		StatementLog<?> log = StatementLog.withStatementContext(context).exception(exception).build();
		for (String secret : List.of("detail-secret", "hint-secret", "internal-secret", "context-secret")) {
			assertFalse(driver.getMessage().contains(secret));
			assertFalse(exception.toString().contains(secret));
			assertFalse(log.toString().contains(secret));
		}
		assertSame(driver, exception.getCause());
	}

	@Test
	void recordConstructionErrorsDescribeComponentsWithoutRenderingRowValues() {
		for (boolean cached : new boolean[]{false, true}) {
			List<String> logs = new ArrayList<>();
			Database database = database(logs, cached);
			DatabaseException exception = assertThrows(DatabaseException.class, () -> database.query(
					"SELECT :password_hash AS password_hash, :social_security_number AS social_security_number FROM (VALUES (0)) AS t(x)")
					.bind("password_hash", "hash-secret-123").bind("social_security_number", "078-05-1120")
					.fetchObject(RejectedRow.class));
			assertNotNull(exception.getCause());
			assertTrue(exception.getMessage().contains("passwordHash:java.lang.String"));
			assertTrue(exception.getMessage().contains("socialSecurityNumber:java.lang.String"));
			assertEquals(1, logs.size());
			for (String secret : List.of("hash-secret-123", "078-05-1120")) {
				assertFalse(exception.getMessage().contains(secret));
				assertFalse(exception.toString().contains(secret));
				assertFalse(logs.get(0).contains(secret));
			}
		}
	}

	private static Database database(List<String> logs) { return database(logs, true); }
	private static Database database(List<String> logs, boolean cached) {
		JDBCDataSource source = new JDBCDataSource();
		source.setUrl("jdbc:hsqldb:mem:diagnostic_privacy_" + UUID.randomUUID());
		source.setUser("sa"); source.setPassword("");
		return Database.with(source, DatabaseType.GENERIC).parameterRedactor(ParameterRedactor.redactAll())
				.resultSetMapper(ResultSetMapper.withPlanCachingEnabled(cached).build())
				.statementLogger(log -> logs.add(log.toString())).build();
	}
}
