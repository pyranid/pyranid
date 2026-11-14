/*
 * Copyright 2015-2022 Transmogrify LLC, 2022-2026 Revetware LLC.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may obtain a copy at https://www.apache.org/licenses/LICENSE-2.0
 */
package com.pyranid;

import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

import javax.sql.DataSource;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

@Testcontainers
class PostgreSqlLexicalIT {
	@Container private static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer(DockerImageName.parse(
			System.getProperty("postgres.integration.image", "pgvector/pgvector:pg17")).asCompatibleSubstituteFor("postgres"))
			.withDatabaseName("pyranid_lexical").withUsername("pyranid").withPassword("pyranid");
	public record Flags(Boolean rawExists, Boolean escapedExists, Boolean rawAny, Boolean escapedAny,
			Boolean rawAll, Boolean escapedAll, Boolean rawPath, Boolean escapedPath) {}

	@Test
	void nestedCommentsKeepInnerParametersAndQuestionMarksProtected() {
		SqlCompilerIntegrationSupport.verifyNestedComments(source(), DatabaseType.POSTGRESQL);
	}

	@Test
	void rawAndAlreadyEscapedOperatorsKeepJsonResultsAndNamedBindingOrder() {
		String sql = "SELECT :doc::jsonb ? :present AS raw_exists, :doc::jsonb ?? :absent AS escaped_exists, "
				+ ":doc::jsonb ?| ARRAY[:present] AS raw_any, :doc::jsonb ??| ARRAY[:absent, :second] AS escaped_any, "
				+ ":doc::jsonb ?& ARRAY[:present, :absent] AS raw_all, :doc::jsonb ??& ARRAY[:present, :second] AS escaped_all, "
				+ ":doc::jsonb @? :path::jsonpath AS raw_path, :doc::jsonb @?? :path::jsonpath AS escaped_path";
		for (int capacity : new int[]{0, 16}) {
			AtomicReference<StatementLog<?>> log = new AtomicReference<>();
			Database database = Database.with(source(), DatabaseType.POSTGRESQL).parsedSqlCacheCapacity(capacity).statementLogger(log::set).build();
			for (int pass = 0; pass < 2; pass++) {
				assertEquals(new Flags(true, false, true, true, false, true, true, true), database.query(sql)
						.bind("doc", "{\"a\":1,\"b\":2}").bind("present", "a").bind("absent", "missing").bind("second", "b")
						.bind("path", "$.a ? (@ > 0)").fetchObject(Flags.class).orElseThrow());
				String sent = log.get().getStatementContext().getStatement().getSql();
				assertFalse(sent.contains("????"), sent);
				assertTrue(sent.contains("??| ARRAY[?, ?]"), sent);
				assertEquals(19, log.get().getStatementContext().getParameters().size());
				assertEquals(true, database.query("SELECT :where ?? :key AND :select ? :key")
						.bind("where", Parameters.json("{\"a\":1}"))
						.bind("select", Parameters.json("{\"a\":1}")).bind("key", "a")
						.fetchObject(Boolean.class).orElseThrow());
				assertEquals(List.of(2, 4), database.query("SELECT v FROM (VALUES (1), (2), (3), (4)) AS numbers(v) "
						+ "WHERE '{\"a\":1}'::jsonb ?? 'a' AND v IN (:ids) ORDER BY v")
						.bind("ids", Parameters.inList(List.of(2, 4))).fetchList(Integer.class));
			}
		}
	}

	private static DataSource source() {
		PGSimpleDataSource source = new PGSimpleDataSource();
		source.setUrl(POSTGRES.getJdbcUrl()); source.setUser(POSTGRES.getUsername()); source.setPassword(POSTGRES.getPassword());
		return source;
	}
}
