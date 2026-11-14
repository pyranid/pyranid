/*
 * Copyright 2015-2022 Transmogrify LLC, 2022-2026 Revetware LLC.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may obtain a copy at https://www.apache.org/licenses/LICENSE-2.0
 */
package com.pyranid;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class MariaDbStreamingConfigurationTests {
	@Test
	void defaultFetchSizeEnablesStreamingInsideAndOutsideTransactions() throws Exception {
		for (boolean transaction : new boolean[]{false, true}) {
			Capture capture = new Capture(0);
			configure(capture, transaction, false);
			assertEquals(256, capture.fetchSize);
			assertEquals(List.of(256), capture.writes);
		}
	}

	@Test
	void explicitQuerySettingsAndPositiveInheritedOrCustomizerValuesRemainInForce() throws Exception {
		for (boolean transaction : new boolean[]{false, true}) {
			for (int size : new int[]{0, 1, 1_000}) {
				Capture capture = new Capture(size);
				configure(capture, transaction, true);
				assertEquals(size, capture.fetchSize);
				assertTrue(capture.writes.isEmpty());
			}
			Capture customized = new Capture(37);
			configure(customized, transaction, false);
			assertEquals(37, customized.fetchSize);
			assertTrue(customized.writes.isEmpty());
		}
	}

	@Test
	void unreadableOrRejectedConfigurationPropagatesTheOriginalFailure() {
		for (boolean onWrite : new boolean[]{false, true}) {
			Capture capture = new Capture(0);
			capture.failure = new SQLException("connection failed during fetch setup", "08006");
			capture.failOnWrite = onWrite;
			assertSame(capture.failure, assertThrows(SQLException.class, () -> configure(capture, false, false)));
			assertEquals(onWrite ? List.of(256) : List.of(), capture.writes);
		}
	}

	private static void configure(Capture capture, boolean transaction, boolean configured) throws Exception {
		PreparedStatement statement = (PreparedStatement) Proxy.newProxyInstance(PreparedStatement.class.getClassLoader(), new Class<?>[]{PreparedStatement.class},
				(proxy, method, args) -> switch (method.getName()) {
					case "getFetchSize" -> {
						if (capture.failure != null && !capture.failOnWrite) throw capture.failure;
						yield capture.fetchSize;
					}
					case "setFetchSize" -> {
						capture.writes.add((Integer) args[0]);
						if (capture.failure != null && capture.failOnWrite) throw capture.failure;
						capture.fetchSize = (Integer) args[0];
						yield null;
					}
					default -> throw new AssertionError("Unexpected statement operation: " + method.getName());
				});
		MariaDbDialect.INSTANCE.configureStreamingPreparedStatement(statement, DatabaseStreamState.none(), transaction, configured);
	}

	private static final class Capture {
		int fetchSize;
		final List<Integer> writes = new ArrayList<>();
		SQLException failure;
		boolean failOnWrite;
		Capture(int fetchSize) { this.fetchSize = fetchSize; }
	}
}
