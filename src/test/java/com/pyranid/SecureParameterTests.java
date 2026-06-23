/*
 * Copyright 2015-2022 Transmogrify LLC, 2022-2026 Revetware LLC.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.pyranid;

import org.hsqldb.jdbc.JDBCDataSource;
import org.jspecify.annotations.NonNull;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import javax.annotation.concurrent.ThreadSafe;
import javax.sql.DataSource;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * @author <a href="https://www.revetkn.com">Mark Allen</a>
 * @since 4.4.0
 */
@ThreadSafe
public class SecureParameterTests {
	@Test
	public void testSecureParameterFactoryAndEquality() {
		SecureParameter secureParameter = Parameters.secure("secret");

		Assertions.assertEquals(Optional.of("secret"), secureParameter.getValue());
		Assertions.assertEquals("<redacted>", secureParameter.getMask());
		Assertions.assertEquals("<redacted>", secureParameter.toString());

		SecureParameter customMask = Parameters.secure("secret", "<token>");

		Assertions.assertEquals(Optional.of("secret"), customMask.getValue());
		Assertions.assertEquals("<token>", customMask.getMask());
		Assertions.assertEquals("<token>", customMask.toString());
		Assertions.assertEquals(Parameters.secure("secret", "<token>"), customMask);
		Assertions.assertEquals(Parameters.secure("secret", "<token>").hashCode(), customMask.hashCode());
		Assertions.assertNotEquals(Parameters.secure("secret", "<other>"), customMask);
		Assertions.assertNotEquals(Parameters.secure("other", "<token>"), customMask);
	}

	@Test
	public void testStatementContextMasksSecureAndSkipsRedactor() {
		AtomicInteger redactorCalls = new AtomicInteger();
		Database db = Database.withDataSource(createInMemoryDataSource("secure_context_masks"))
				.parameterRedactor((statementContext, parameterIndex, parameter) -> {
					redactorCalls.incrementAndGet();
					return format("<nonsecure:%s>", parameterIndex);
				})
				.build();
		StatementContext<?> statementContext = StatementContext.with(Statement.of("secure-context", "SELECT * FROM t WHERE token = ? AND status = ?"), db)
				.parameters(Parameters.secure("raw-token", "<token>"), "active")
				.build();

		Assertions.assertEquals(List.of("<token>", "<nonsecure:1>"), statementContext.getRedactedParameters());
		Assertions.assertEquals(1, redactorCalls.get());
		Assertions.assertTrue(statementContext.getParameters().get(0) instanceof SecureParameter);

		String renderedStatementContext = statementContext.toString();
		String renderedStatementLog = StatementLog.withStatementContext(statementContext).build().toString();

		Assertions.assertTrue(renderedStatementContext.contains("<token>"));
		Assertions.assertFalse(renderedStatementContext.contains("raw-token"));
		Assertions.assertTrue(renderedStatementLog.contains("<token>"));
		Assertions.assertFalse(renderedStatementLog.contains("raw-token"));
	}

	@Test
	public void testRedactAllMasksNonSecureParameters() {
		Database db = Database.withDataSource(createInMemoryDataSource("secure_redact_all"))
				.parameterRedactor(ParameterRedactor.redactAll())
				.build();
		StatementContext<?> statementContext = StatementContext.with(Statement.of("redact-all", "SELECT * FROM t WHERE email = ?"), db)
				.parameters("customer@example.com")
				.build();

		Assertions.assertEquals(List.of("customer@example.com"), statementContext.getParameters());
		Assertions.assertEquals(List.of("<redacted>"), statementContext.getRedactedParameters());
		Assertions.assertFalse(statementContext.toString().contains("customer@example.com"));
	}

	@Test
	public void testDefaultRedactorLeavesNonSecureParametersVerbatim() {
		Database db = Database.withDataSource(createInMemoryDataSource("secure_none_redactor")).build();
		StatementContext<?> statementContext = StatementContext.with(Statement.of("none-redactor", "SELECT ?"), db)
				.parameters("customer@example.com")
				.build();

		Assertions.assertEquals(List.of("customer@example.com"), statementContext.getParameters());
		Assertions.assertEquals(List.of("customer@example.com"), statementContext.getRedactedParameters());
	}

	@Test
	public void testOptionalSecureParameterMasksAndSkipsRedactor() {
		AtomicInteger redactorCalls = new AtomicInteger();
		Database db = Database.withDataSource(createInMemoryDataSource("secure_optional_display"))
				.parameterRedactor((statementContext, parameterIndex, parameter) -> {
					redactorCalls.incrementAndGet();
					return "<nonsecure>";
				})
				.build();
		SecureParameter secureParameter = Parameters.secure("raw-token", "<token>");
		StatementContext<?> statementContext = StatementContext.with(Statement.of("optional-secure", "SELECT ?"), db)
				.parameters(Optional.of(secureParameter))
				.build();

		Assertions.assertEquals(List.of(Optional.of(secureParameter)), statementContext.getParameters());
		Assertions.assertEquals(List.of("<token>"), statementContext.getRedactedParameters());
		Assertions.assertEquals(0, redactorCalls.get());
		Assertions.assertFalse(statementContext.toString().contains("raw-token"));
	}

	@Test
	public void testCustomSecureParameterToStringIsNotTrustedForDiagnostics() {
		SecureParameter leakySecureParameter = new SecureParameter() {
			@NonNull
			@Override
			public Optional<Object> getValue() {
				return Optional.of("raw-secret");
			}

			@NonNull
			@Override
			public String getMask() {
				return "<safe>";
			}

			@Override
			public String toString() {
				return "raw-secret";
			}
		};
		Database db = Database.withDataSource(createInMemoryDataSource("secure_custom_tostring")).build();
		StatementContext<?> statementContext = StatementContext.with(Statement.of("secure-custom", "SELECT ?"), db)
				.parameters(leakySecureParameter)
				.build();

		Assertions.assertEquals(List.of("<safe>"), statementContext.getRedactedParameters());
		Assertions.assertTrue(statementContext.toString().contains("<safe>"));
		Assertions.assertFalse(statementContext.toString().contains("raw-secret"));
	}

	@Test
	public void testRedactionFailuresAreFailFast() {
		RuntimeException redactionFailure = new RuntimeException("redaction boom");
		Database redactorDatabase = Database.withDataSource(createInMemoryDataSource("secure_redactor_failure"))
				.parameterRedactor((statementContext, parameterIndex, parameter) -> {
					throw redactionFailure;
				})
				.build();
		StatementContext<?> redactorContext = StatementContext.with(Statement.of("redactor-failure", "SELECT ?"), redactorDatabase)
				.parameters("raw")
				.build();

		Assertions.assertSame(redactionFailure, Assertions.assertThrows(RuntimeException.class, redactorContext::getRedactedParameters));

		SecureParameter nullMask = new SecureParameter() {
			@NonNull
			@Override
			public Optional<Object> getValue() {
				return Optional.of("raw");
			}

			@SuppressWarnings("DataFlowIssue")
			@NonNull
			@Override
			public String getMask() {
				return null;
			}
		};
		Database secureParameterDatabase = Database.withDataSource(createInMemoryDataSource("secure_mask_failure")).build();
		StatementContext<?> secureParameterContext = StatementContext.with(Statement.of("mask-failure", "SELECT ?"), secureParameterDatabase)
				.parameters(nullMask)
				.build();

		Assertions.assertThrows(NullPointerException.class, secureParameterContext::getRedactedParameters);
	}

	@Test
	public void testNonBatchTopLevelListIsRedactedAsScalar() {
		AtomicInteger redactorCalls = new AtomicInteger();
		Database db = Database.withDataSource(createInMemoryDataSource("secure_top_level_list"))
				.parameterRedactor((statementContext, parameterIndex, parameter) -> {
					redactorCalls.incrementAndGet();
					Assertions.assertEquals(0, parameterIndex);
					Assertions.assertTrue(parameter instanceof List<?>);
					return "<list>";
				})
				.build();
		StatementContext<?> statementContext = StatementContext.with(Statement.of("top-level-list", "SELECT ?"), db)
				.parameters(List.of((Object) List.of("raw-secret")))
				.build();

		Assertions.assertEquals(List.of("<list>"), statementContext.getRedactedParameters());
		Assertions.assertEquals(1, redactorCalls.get());
	}

	@Test
	public void testBatchDiagnosticsUseBoundedSummaryAndSkipRedactor() {
		AtomicInteger redactorCalls = new AtomicInteger();
		AtomicReference<StatementLog<?>> statementLogReference = new AtomicReference<>();
		Database db = Database.withDataSource(createInMemoryDataSource("secure_batch_summary"))
				.parameterRedactor((statementContext, parameterIndex, parameter) -> {
					redactorCalls.incrementAndGet();
					return "<nonsecure>";
				})
				.statementLogger(statementLogReference::set)
				.build();

		db.query("CREATE TABLE t (id INT, value VARCHAR(255))").execute();
		statementLogReference.set(null);
		redactorCalls.set(0);

		db.query("INSERT INTO t (id, value) VALUES (:id, :value)")
				.executeBatch(List.of(
						mapOf("id", 1, "value", Parameters.secure("raw-secret-1")),
						mapOf("id", 2, "value", "raw-secret-2")
				));

		StatementLog<?> statementLog = requireNonNull(statementLogReference.get());
		String renderedStatementLog = statementLog.toString();

		Assertions.assertEquals(List.of("<batch: 2 groups x 2 parameters>"),
				statementLog.getStatementContext().getRedactedParameters());
		Assertions.assertTrue(renderedStatementLog.contains("<batch: 2 groups x 2 parameters>"));
		Assertions.assertFalse(renderedStatementLog.contains("raw-secret-1"));
		Assertions.assertFalse(renderedStatementLog.contains("raw-secret-2"));
		Assertions.assertEquals(0, redactorCalls.get());
		Assertions.assertTrue(statementLog.getStatementContext().getParameters().get(0) instanceof List<?>);
	}

	@Test
	public void testSecureScalarValuesBindLikeRawValues() {
		Database db = Database.withDataSource(createInMemoryDataSource("secure_scalar_binding")).build();

		db.query("CREATE TABLE t (id INT, secret VARCHAR(255), maybe_null VARCHAR(255))").execute();
		db.query("INSERT INTO t (id, secret, maybe_null) VALUES (:id, :secret, :maybeNull)")
				.bind("id", 1)
				.bind("secret", Parameters.secure("raw-secret"))
				.bind("maybeNull", Parameters.secure(null))
				.execute();

		Integer count = db.query("SELECT COUNT(*) FROM t WHERE secret = :secret AND maybe_null IS NULL")
				.bind("secret", "raw-secret")
				.fetchObject(Integer.class)
				.orElseThrow();

		Assertions.assertEquals(Integer.valueOf(1), count);
	}

	@Test
	public void testSecureInListExpandsAndMasksEachElement() {
		AtomicReference<StatementLog<?>> statementLogReference = new AtomicReference<>();
		Database db = Database.withDataSource(createInMemoryDataSource("secure_inlist"))
				.statementLogger(statementLogReference::set)
				.build();

		db.query("CREATE TABLE t (id INT, value VARCHAR(255))").execute();
		db.query("INSERT INTO t VALUES (1, 'raw-secret-1')").execute();
		db.query("INSERT INTO t VALUES (2, 'raw-secret-2')").execute();
		db.query("INSERT INTO t VALUES (3, 'raw-secret-3')").execute();
		statementLogReference.set(null);

		List<Integer> ids = db.query("SELECT id FROM t WHERE value IN (:values) ORDER BY id")
				.bind("values", Parameters.secure(Parameters.inList(List.of("raw-secret-1", "raw-secret-3")), "<value>"))
				.fetchList(Integer.class);

		StatementLog<?> statementLog = requireNonNull(statementLogReference.get());
		String renderedStatementLog = statementLog.toString();

		Assertions.assertEquals(List.of(1, 3), ids);
		Assertions.assertEquals(List.of("<value>", "<value>"), statementLog.getStatementContext().getRedactedParameters());
		Assertions.assertTrue(renderedStatementLog.contains("<value>"));
		Assertions.assertFalse(renderedStatementLog.contains("raw-secret-1"));
		Assertions.assertFalse(renderedStatementLog.contains("raw-secret-3"));
	}

	@Test
	public void testSecureInListInsideBatchBindsCorrectlyAndUsesBatchSummary() {
		AtomicReference<StatementLog<?>> statementLogReference = new AtomicReference<>();
		Database db = Database.withDataSource(createInMemoryDataSource("secure_batch_inlist"))
				.statementLogger(statementLogReference::set)
				.build();

		db.query("CREATE TABLE source (value VARCHAR(255))").execute();
		db.query("INSERT INTO source VALUES ('raw-secret-1')").execute();
		db.query("INSERT INTO source VALUES ('raw-secret-2')").execute();
		db.query("INSERT INTO source VALUES ('raw-secret-3')").execute();
		db.query("CREATE TABLE target (group_id INT, value VARCHAR(255))").execute();
		statementLogReference.set(null);

		db.query("INSERT INTO target (group_id, value) SELECT :groupId, value FROM source WHERE value IN (:values)")
				.executeBatch(List.of(
						mapOf("groupId", 1, "values", Parameters.secure(Parameters.inList(List.of("raw-secret-1", "raw-secret-3")))),
						mapOf("groupId", 2, "values", Parameters.secure(Parameters.inList(List.of("raw-secret-2", "raw-secret-3"))))
				));
		StatementLog<?> statementLog = requireNonNull(statementLogReference.get());

		Integer count = db.query("SELECT COUNT(*) FROM target")
				.fetchObject(Integer.class)
				.orElseThrow();
		String renderedStatementLog = statementLog.toString();

		Assertions.assertEquals(Integer.valueOf(4), count);
		Assertions.assertEquals(List.of("<batch: 2 groups x 3 parameters>"),
				statementLog.getStatementContext().getRedactedParameters());
		Assertions.assertTrue(renderedStatementLog.contains("<batch: 2 groups x 3 parameters>"));
		Assertions.assertFalse(renderedStatementLog.contains("raw-secret-1"));
		Assertions.assertFalse(renderedStatementLog.contains("raw-secret-2"));
		Assertions.assertFalse(renderedStatementLog.contains("raw-secret-3"));
	}

	@Test
	public void testCustomPreparedStatementBinderReceivesUnwrappedSecureValue() {
		AtomicReference<Object> receivedParameter = new AtomicReference<>();
		PreparedStatementBinder preparedStatementBinder = new PreparedStatementBinder() {
			@Override
			public <T> void bindParameter(@NonNull StatementContext<T> statementContext,
																		@NonNull PreparedStatement preparedStatement,
																		@NonNull Integer parameterIndex,
																		@NonNull Object parameter) throws SQLException {
				receivedParameter.set(parameter);
				preparedStatement.setString(parameterIndex, (String) parameter);
			}
		};
		Database db = Database.withDataSource(createInMemoryDataSource("secure_custom_binder"))
				.preparedStatementBinder(preparedStatementBinder)
				.build();

		db.query("CREATE TABLE t (value VARCHAR(255))").execute();
		db.query("INSERT INTO t (value) VALUES (:value)")
				.bind("value", Parameters.secure(Optional.of(Parameters.secure("raw-secret"))))
				.execute();

		Assertions.assertEquals("raw-secret", receivedParameter.get());
		Assertions.assertFalse(receivedParameter.get() instanceof SecureParameter);
	}

	@Test
	public void testSecureParameterGetValueFailureFailsBinding() {
		SecureParameter failingSecureParameter = new SecureParameter() {
			@NonNull
			@Override
			public Optional<Object> getValue() {
				throw new IllegalStateException("value boom");
			}

			@NonNull
			@Override
			public String getMask() {
				return "<safe>";
			}
		};
		Database db = Database.withDataSource(createInMemoryDataSource("secure_value_failure")).build();

		db.query("CREATE TABLE t (value VARCHAR(255))").execute();

		IllegalStateException exception = Assertions.assertThrows(IllegalStateException.class, () ->
				db.query("INSERT INTO t (value) VALUES (:value)")
						.bind("value", failingSecureParameter)
						.execute());

		Assertions.assertEquals("value boom", exception.getMessage());
	}

	@NonNull
	private static Map<String, Object> mapOf(@NonNull String key1,
																					 @NonNull Object value1,
																					 @NonNull String key2,
																					 @NonNull Object value2) {
		requireNonNull(key1);
		requireNonNull(value1);
		requireNonNull(key2);
		requireNonNull(value2);

		Map<String, Object> map = new LinkedHashMap<>();
		map.put(key1, value1);
		map.put(key2, value2);
		return map;
	}

	@NonNull
	private static DataSource createInMemoryDataSource(@NonNull String databaseName) {
		requireNonNull(databaseName);

		JDBCDataSource dataSource = new JDBCDataSource();
		dataSource.setUrl(format("jdbc:hsqldb:mem:%s", databaseName));
		dataSource.setUser("sa");
		dataSource.setPassword("");

		return dataSource;
	}
}
