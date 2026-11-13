/*
 * Copyright 2015-2022 Transmogrify LLC, 2022-2026 Revetware LLC.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may obtain a copy at https://www.apache.org/licenses/LICENSE-2.0
 */
package com.pyranid;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class DuckDbMetricsIntegrationIT {
	@TempDir Path directory;

	@Test
	void enabledMetricsPreserveDdlSetAndTransactionResults() {
		for (boolean enabled : new boolean[]{false, true}) {
			List<StatementResult> results = new ArrayList<>();
			MetricsCollector collector = enabled ? new MetricsCollector() {
				@Override public void didExecuteStatement(StatementContext<?> context, StatementLog<?> log, StatementResult result) {
					results.add(result);
				}
			} : MetricsCollector.disabledInstance();
			Database database = Database.with(new AbstractPortableJdbcIntegrationTests.DriverManagerDataSource(
					"jdbc:duckdb:" + this.directory.resolve("metrics_" + enabled + ".duckdb"), null, null), DatabaseType.DUCK_DB)
					.metricsCollector(collector).build();
			assertEquals(-1L, database.query("CREATE TABLE work (id INT PRIMARY KEY)").execute());
			assertEquals(-1L, database.query("SET threads = 1").execute());
			TransactionOutcome<Void> outcome = database.transactionWithOutcome(() -> {
				assertEquals(1L, database.query("INSERT INTO work VALUES (1)").execute());
				assertEquals(-1L, database.query("CREATE TABLE other_work (id INT)").execute());
			});
			assertEquals(TransactionResult.COMMITTED, outcome.getResult());
			assertTrue(outcome.getFailure().isEmpty());
			assertEquals(1L, database.query("CREATE TABLE copied_work AS SELECT * FROM work").execute());
			if (enabled) assertEquals(List.of(new StatementResult(null, null), new StatementResult(null, null),
					new StatementResult(null, 1L), new StatementResult(null, null), new StatementResult(null, 1L)), results);
			assertEquals(List.of(1), database.query("SELECT id FROM copied_work").fetchList(Integer.class));
			assertEquals(0L, database.query("SELECT COUNT(*) FROM other_work").fetchObject(Long.class).orElseThrow());
		}
	}
}
