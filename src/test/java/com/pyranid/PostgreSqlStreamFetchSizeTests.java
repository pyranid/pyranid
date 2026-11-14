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

class PostgreSqlStreamFetchSizeTests {
	@Test
	void defaultFetchSizeAppliesToBothOwnedStreamAndEnclosingTransactionConnections() throws SQLException {
		for (boolean enclosingTransaction : new boolean[]{false, true}) {
			Capture capture = new Capture(0);
			PostgresDialect.INSTANCE.configureStreamingPreparedStatement(capture.statement(),
					enclosingTransaction ? DatabaseStreamState.none() : DatabaseStreamState.managedTransaction(true),
					enclosingTransaction, false);
			assertEquals(256, capture.fetchSize);
			assertEquals(List.of("getFetchSize", "setFetchSize"), capture.calls);
		}
	}

	@Test
	void explicitZeroAndPositiveSettingsRemainUntouchedInEitherTransactionScope() throws SQLException {
		for (boolean transaction : new boolean[]{false, true}) {
			for (int size : new int[]{0, 17, 1000}) {
				Capture explicit = new Capture(size);
				PostgresDialect.INSTANCE.configureStreamingPreparedStatement(explicit.statement(),
						transaction ? DatabaseStreamState.none() : DatabaseStreamState.managedTransaction(true), transaction, true);
				assertEquals(size, explicit.fetchSize);
				assertTrue(explicit.calls.isEmpty(), "Explicit configuration must not be replaced by a driver getter/default");
			}
			Capture customized = new Capture(23);
			PostgresDialect.INSTANCE.configureStreamingPreparedStatement(customized.statement(),
					transaction ? DatabaseStreamState.none() : DatabaseStreamState.managedTransaction(true), transaction, false);
			assertEquals(23, customized.fetchSize);
			assertEquals(List.of("getFetchSize"), customized.calls);
		}
	}

	@Test
	void fetchSizeInspectionAndApplicationFailuresPropagateWithoutFallback() {
		for (boolean failOnRead : new boolean[]{false, true}) {
			SQLException original = new SQLException("stream configuration connection failure", "08006");
			Capture capture = new Capture(0);
			capture.readFailure = failOnRead ? original : null;
			capture.writeFailure = failOnRead ? null : original;
			assertSame(original, assertThrows(SQLException.class, () -> PostgresDialect.INSTANCE.configureStreamingPreparedStatement(
					capture.statement(), DatabaseStreamState.none(), true, false)));
			assertEquals(failOnRead ? List.of("getFetchSize") : List.of("getFetchSize", "setFetchSize"), capture.calls);
		}
	}

	private static final class Capture {
		int fetchSize;
		SQLException readFailure, writeFailure;
		final List<String> calls = new ArrayList<>();
		Capture(int fetchSize) { this.fetchSize = fetchSize; }
		PreparedStatement statement() {
			return (PreparedStatement) Proxy.newProxyInstance(PreparedStatement.class.getClassLoader(), new Class<?>[]{PreparedStatement.class},
					(proxy, method, args) -> switch (method.getName()) {
						case "getFetchSize" -> {
							calls.add("getFetchSize");
							if (readFailure != null) throw readFailure;
							yield fetchSize;
						}
						case "setFetchSize" -> {
							calls.add("setFetchSize");
							if (writeFailure != null) throw writeFailure;
							fetchSize = (Integer) args[0];
							yield null;
						}
						default -> throw new AssertionError(method.getName());
					});
		}
	}
}
