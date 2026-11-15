/*
 * Copyright 2015-2022 Transmogrify LLC, 2022-2026 Revetware LLC.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may obtain a copy at https://www.apache.org/licenses/LICENSE-2.0
 */
package com.pyranid;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class TemporalColumnTypeDetectionTests {
	@Test
	void scalarZonedTimestampsAreRecognizedIncludingPrecisionAndLocalTimeZone() throws SQLException {
		for (String name : List.of("TIMESTAMPTZ", " timestamptz ", "TIMESTAMP WITH TIME ZONE",
				"TIMESTAMP(9) WITH TIME ZONE", "TIMESTAMP ( 6 ) WITH LOCAL TIME ZONE",
				"timestamp\twith\tlocal\ttime\tzone"))
			assertTrue(GenericDialect.INSTANCE.isTimestampWithTimeZone(metadata(Types.TIMESTAMP, name), 1), name);
		assertTrue(GenericDialect.INSTANCE.isTimestampWithTimeZone(metadata(Types.TIMESTAMP_WITH_TIMEZONE, null), 1));
	}

	@Test
	void containerAndOtherTypeNamesDoNotBecomeScalarZonedTimestamps() throws SQLException {
		for (String name : List.of("INTEGER", "VARCHAR", "MAP(TIMESTAMP, TIMESTAMP WITH TIME ZONE)",
				"STRUCT(local TIMESTAMP, zoned TIMESTAMPTZ)", "TIMESTAMPTZ[]", "TIMESTAMP WITH TIME ZONE[]",
				"TIMESTAMP(6) WITH TIME ZONE[2]", "my_timestamptz", "TIMESTAMP", "TIME WITH TIME ZONE"))
			assertFalse(GenericDialect.INSTANCE.isTimestampWithTimeZone(metadata(Types.JAVA_OBJECT, name), 1), name);
		assertFalse(GenericDialect.INSTANCE.isTimestampWithTimeZone(metadata(Types.OTHER, null), 1));
	}

	private static ResultSetMetaData metadata(int jdbcType, String typeName) {
		return (ResultSetMetaData) Proxy.newProxyInstance(ResultSetMetaData.class.getClassLoader(),
				new Class<?>[]{ResultSetMetaData.class}, (proxy, method, args) -> switch (method.getName()) {
					case "getColumnType" -> jdbcType;
					case "getColumnTypeName" -> typeName;
					default -> throw new AssertionError(method.getName());
				});
	}
}
