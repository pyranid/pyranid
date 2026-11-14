/*
 * Copyright 2015-2022 Transmogrify LLC, 2022-2026 Revetware LLC.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may obtain a copy at https://www.apache.org/licenses/LICENSE-2.0
 */
package com.pyranid;

import java.util.Optional;

/** Isolates the regression's large result set from the integration-test JVM's heap. */
public final class MariaDbStreamingHeapProbe {
	public static void main(String[] args) throws Exception {
		Database database = Database.with(new AbstractPortableJdbcIntegrationTests.DriverManagerDataSource(args[0], args[1], args[2]),
				DatabaseType.MARIA_DB).build();
		for (boolean transaction : new boolean[]{false, true}) {
			ReturningTransactionalOperation<Long> operation = () -> Optional.of(database.query(
					"SELECT REPEAT('x', 400) FROM seq_1_to_524288").fetchStream(String.class,
					rows -> rows.mapToLong(String::length).sum()));
			long bytes = transaction ? database.transaction(operation).orElseThrow() : operation.perform().orElseThrow();
			if (bytes != 524288L * 400) throw new AssertionError("Incomplete result: " + bytes);
			if (database.query("SELECT 1").fetchObject(Integer.class).orElseThrow() != 1)
				throw new AssertionError("Connection unusable after streaming");
		}
		System.out.println("PASS: MariaDB streams 524288 rows in and out of a transaction with a 64 MB heap");
	}
}
