/*
 * Copyright 2015-2022 Transmogrify LLC, 2022-2026 Revetware LLC.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may obtain a copy at https://www.apache.org/licenses/LICENSE-2.0
 */
package com.pyranid;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.ZoneId;
import java.util.List;

class DuckDbTimestampRoundTripIT {
	@TempDir Path directory;

	@Test
	void timestampReadModifyWritePreservesInstantAcrossScalarRecordAndBeanMappings() {
		for (boolean cached : new boolean[]{false, true}) {
			for (ZoneId zone : List.of(ZoneId.of("UTC"), ZoneId.of("America/New_York"), ZoneId.of("Asia/Kathmandu"))) {
				String file = (cached ? "cached_" : "uncached_") + zone.getId().replace('/', '_') + ".duckdb";
				Database database = Database.with(new AbstractPortableJdbcIntegrationTests.DriverManagerDataSource(
						"jdbc:duckdb:" + this.directory.resolve(file), null, null), DatabaseType.DUCK_DB).timeZone(zone)
						.resultSetMapper(ResultSetMapper.withPlanCachingEnabled(cached).build()).build();
				TemporalTimestampIntegrationSupport.verify(database);
			}
		}
	}
}
