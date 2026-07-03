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

import javax.sql.DataSource;
import java.io.PrintWriter;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.UnaryOperator;
import java.util.logging.Logger;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * Verifies the 4.5.0 best-effort scrub of {@link SecureParameter} values from driver-echoed text in
 * {@link DatabaseException} messages/metadata and {@link StatementLog} diagnostics.
 *
 * @author <a href="https://www.revetkn.com">Mark Allen</a>
 */
public class SecureParameterDiagnosticScrubTests {
	// --- Pure-unit tests of the scrub machinery ---

	@Test
	public void testBasicNeedleScrubWithDefaultMask() {
		SecureParameterSupport.DiagnosticScrub scrub = SecureParameterSupport.diagnosticScrubForParameters(
				List.of(Parameters.secure("alice@secret.com")));

		Assertions.assertNotNull(scrub.redactor(), "Expected a redactor when a secure parameter is present");
		Assertions.assertEquals("Key (email)=(<redacted>) already exists.",
				scrub.redactor().apply("Key (email)=(alice@secret.com) already exists."));
	}

	@Test
	public void testCustomMaskHonored() {
		SecureParameterSupport.DiagnosticScrub scrub = SecureParameterSupport.diagnosticScrubForParameters(
				List.of(Parameters.secure("hunter2", "***")));

		Assertions.assertEquals("password *** rejected", scrub.redactor().apply("password hunter2 rejected"));
	}

	@Test
	public void testNullSecureValueDoesNotCorruptDiagnostics() {
		SecureParameterSupport.DiagnosticScrub scrubOfNull = SecureParameterSupport.diagnosticScrubForParameters(
				List.of(Parameters.secure(null)));
		SecureParameterSupport.DiagnosticScrub scrubOfEmptyOptional = SecureParameterSupport.diagnosticScrubForParameters(
				List.of(Parameters.secure(Optional.empty())));

		// A null secure value has nothing to scrub; the word "null" in driver text must be preserved
		Assertions.assertNull(scrubOfNull.redactor(), "Expected no redactor for a null secure value");
		Assertions.assertNull(scrubOfEmptyOptional.redactor(), "Expected no redactor for an empty Optional secure value");
	}

	@Test
	public void testBooleanSecureValueSkipped() {
		SecureParameterSupport.DiagnosticScrub scrub = SecureParameterSupport.diagnosticScrubForParameters(
				List.of(Parameters.secure(Boolean.TRUE)));

		Assertions.assertNull(scrub.redactor(), "Expected no redactor for a Boolean secure value");
	}

	@Test
	public void testShortNeedleSkipped() {
		SecureParameterSupport.DiagnosticScrub scrub = SecureParameterSupport.diagnosticScrubForParameters(
				List.of(Parameters.secure("ab")));

		Assertions.assertNull(scrub.redactor(), "Expected no redactor for a value shorter than the minimum needle length");
	}

	@Test
	public void testOverlappingNeedlesLeftmostLongestWins() {
		SecureParameterSupport.DiagnosticScrub scrub = SecureParameterSupport.diagnosticScrubForParameters(
				List.of(Parameters.secure("secret"), Parameters.secure("secret-extended")));

		// The longer needle wins where both match; the shorter still masks elsewhere
		Assertions.assertEquals("saw <redacted> and <redacted>",
				scrub.redactor().apply("saw secret-extended and secret"));
	}

	@Test
	public void testMaskContainingAnotherNeedleIsNeverRemasked() {
		// The first parameter's mask deliberately contains the second parameter's secret value
		SecureParameterSupport.DiagnosticScrub scrub = SecureParameterSupport.diagnosticScrubForParameters(
				List.of(Parameters.secure("valueA", "mask-with-valueB"), Parameters.secure("valueB")));

		// Appended masks are never re-scanned, so "valueB" inside the first mask survives verbatim
		Assertions.assertEquals("x mask-with-valueB y <redacted>", scrub.redactor().apply("x valueA y valueB"));
	}

	@Test
	public void testTypedParameterFamilyUnwrappedForNeedles() {
		SecureParameterSupport.DiagnosticScrub jsonScrub = SecureParameterSupport.diagnosticScrubForParameters(
				List.of(Parameters.secure(Parameters.json("{\"ssn\":\"123-45-6789\"}"))));

		Assertions.assertNotNull(jsonScrub.redactor(), "Expected a needle for a secure-wrapped JSON parameter");
		Assertions.assertEquals("json was <redacted> here",
				jsonScrub.redactor().apply("json was {\"ssn\":\"123-45-6789\"} here"));
	}

	@Test
	public void testBatchParameterGroupsWalked() {
		List<Object> group1 = List.of("plain", Parameters.secure("batch-secret-1"));
		List<Object> group2 = List.of("plain", Parameters.secure("batch-secret-2"));
		SecureParameterSupport.DiagnosticScrub scrub = SecureParameterSupport.diagnosticScrubForParameters(
				List.of(group1, group2));

		Assertions.assertEquals("<redacted> then <redacted>",
				scrub.redactor().apply("batch-secret-1 then batch-secret-2"));
	}

	@Test
	public void testToStringThrowingSecureValueSkippedWithFailureDescription() {
		Object throwingValue = new Object() {
			@Override
			public String toString() {
				throw new IllegalStateException("toString boom with secret text");
			}
		};

		SecureParameterSupport.DiagnosticScrub scrub = SecureParameterSupport.diagnosticScrubForParameters(
				List.of(Parameters.secure(throwingValue)));

		Assertions.assertNull(scrub.redactor(), "Expected no redactor when the only needle failed to render");
		Assertions.assertEquals(1, scrub.needleRenderingFailureDescriptions().size());
		Assertions.assertFalse(scrub.needleRenderingFailureDescriptions().get(0).contains("secret text"),
				"Failure description must carry the class name only, never message text");
	}

	@Test
	public void testNoSecureParametersFastPath() {
		SecureParameterSupport.DiagnosticScrub scrub = SecureParameterSupport.diagnosticScrubForParameters(
				List.of("plain", 42, List.of("nested-plain")));

		Assertions.assertNull(scrub.redactor(), "Expected the no-secure-parameters fast path to produce no redactor");
		Assertions.assertTrue(scrub.needleRenderingFailureDescriptions().isEmpty());
	}

	@Test
	public void testConstructorScrubsEveryStringMetadataField() {
		String secret = "s3cr3t-value";
		SecureParameterSupport.DiagnosticScrub scrub = SecureParameterSupport.diagnosticScrubForParameters(
				List.of(Parameters.secure(secret)));
		UnaryOperator<String> redactor = scrub.redactor();

		SQLException cause = new SQLException("driver says " + secret + " was bad", "23505");
		DatabaseExceptionMetadata metadata = DatabaseExceptionMetadata.builder(cause)
				.sqlState("state-" + secret)
				.column("column-" + secret)
				.constraint("constraint-" + secret)
				.datatype("datatype-" + secret)
				.detail("detail-" + secret)
				.file("file-" + secret)
				.hint("hint-" + secret)
				.internalQuery("internalQuery-" + secret)
				.dbmsMessage("dbmsMessage-" + secret)
				.routine("routine-" + secret)
				.schema("schema-" + secret)
				.severity("severity-" + secret)
				.table("table-" + secret)
				.where("where-" + secret)
				.errorCode(42)
				.build();
		DatabaseDialect stubDialect = new GenericDialect() {
			@Override
			public DatabaseExceptionMetadata databaseExceptionMetadata(Throwable metadataCause) {
				return metadata;
			}
		};

		// The message is NOT re-scrubbed by the constructor (the Database factory scrubs the raw driver
		// text before composing); metadata fields - raw dialect-extracted text - are each scrubbed once
		DatabaseException databaseException = new DatabaseException("already-scrubbed message", cause, stubDialect, redactor);

		Assertions.assertEquals("state-<redacted>", databaseException.getSqlState().orElseThrow());
		Assertions.assertEquals("column-<redacted>", databaseException.getColumn().orElseThrow());
		Assertions.assertEquals("constraint-<redacted>", databaseException.getConstraint().orElseThrow());
		Assertions.assertEquals("datatype-<redacted>", databaseException.getDatatype().orElseThrow());
		Assertions.assertEquals("detail-<redacted>", databaseException.getDetail().orElseThrow());
		Assertions.assertEquals("file-<redacted>", databaseException.getFile().orElseThrow());
		Assertions.assertEquals("hint-<redacted>", databaseException.getHint().orElseThrow());
		Assertions.assertEquals("internalQuery-<redacted>", databaseException.getInternalQuery().orElseThrow());
		Assertions.assertEquals("dbmsMessage-<redacted>", databaseException.getDbmsMessage().orElseThrow());
		Assertions.assertEquals("routine-<redacted>", databaseException.getRoutine().orElseThrow());
		Assertions.assertEquals("schema-<redacted>", databaseException.getSchema().orElseThrow());
		Assertions.assertEquals("severity-<redacted>", databaseException.getSeverity().orElseThrow());
		Assertions.assertEquals("table-<redacted>", databaseException.getTable().orElseThrow());
		Assertions.assertEquals("where-<redacted>", databaseException.getWhere().orElseThrow());
		Assertions.assertEquals(42, databaseException.getErrorCode().orElseThrow(), "Non-String fields untouched");
		Assertions.assertFalse(databaseException.toString().contains(secret),
				"Expected no metadata field to leak the secret through toString()");
		Assertions.assertTrue(databaseException.getCause().getMessage().contains(secret),
				"Expected the raw cause to remain untouched");
	}

	@Test
	public void testThrowingSecureToStringEndToEnd() {
		Object throwingValue = new Object() {
			@Override
			public String toString() {
				throw new IllegalStateException("boom containing sensitive text");
			}
		};
		Database db = Database.withDataSource(createThrowingDataSource("driver failure text")).build();

		DatabaseException ex = Assertions.assertThrows(DatabaseException.class, () ->
				db.query("INSERT INTO t (v) VALUES (:v)").bind("v", Parameters.secure(throwingValue)).execute());

		// The exception must still be constructed, carrying a synthetic class-name-only suppressed failure
		boolean syntheticFailurePresent = false;
		for (Throwable suppressed : ex.getSuppressed()) {
			String suppressedMessage = String.valueOf(suppressed.getMessage());
			if (suppressedMessage.contains("diagnostic scrubbing")) {
				syntheticFailurePresent = true;
				Assertions.assertTrue(suppressedMessage.contains(IllegalStateException.class.getName()),
						"Expected the failure class name in the synthetic suppressed exception");
				Assertions.assertFalse(suppressedMessage.contains("sensitive text"),
						"The synthetic suppressed exception must never carry the failure's message text");
			}
		}
		Assertions.assertTrue(syntheticFailurePresent, "Expected a synthetic needle-rendering suppressed failure");
	}

	// --- End-to-end tests through Database with a controllable failing driver ---

	@Test
	public void testDriverEchoedSecretScrubbedEndToEnd() {
		String secret = "alice@secret.com";
		Database db = Database.withDataSource(createThrowingDataSource(
				format("ERROR: duplicate key value. Detail: Key (email)=(%s) already exists.", secret))).build();

		DatabaseException ex = Assertions.assertThrows(DatabaseException.class, () ->
				db.query("INSERT INTO account (email) VALUES (:email)")
						.bind("email", Parameters.secure(secret))
						.execute());

		Assertions.assertFalse(ex.getMessage().contains(secret), "Expected the secret absent from getMessage()");
		Assertions.assertFalse(ex.toString().contains(secret), "Expected the secret absent from toString()");
		Assertions.assertTrue(ex.getMessage().contains("<redacted>"), "Expected the mask present in the message");
		Assertions.assertTrue(ex.getCause().getMessage().contains(secret),
				"Expected the raw driver exception preserved as the cause");
	}

	@Test
	public void testWhitespaceContainingSecretScrubbedBeforeMessageBounding() {
		// boundedDiagnosticMessage collapses whitespace runs; scrubbing must happen against the RAW message
		String secret = "line1\n  line2secret";
		Database db = Database.withDataSource(createThrowingDataSource(
				format("rejected value: %s (constraint x)", secret))).build();

		DatabaseException ex = Assertions.assertThrows(DatabaseException.class, () ->
				db.query("INSERT INTO t (v) VALUES (:v)").bind("v", Parameters.secure(secret)).execute());

		Assertions.assertFalse(ex.getMessage().contains("line2secret"),
				"Expected the whitespace-containing secret scrubbed despite whitespace collapsing");
		Assertions.assertTrue(ex.getMessage().contains("<redacted>"));
	}

	@Test
	public void testSecretStraddlingTruncationBoundaryLeavesNoPrefix() {
		// Craft a message where the secret spans the 1024-char diagnostic truncation boundary
		String secret = "SECRETSECRETSECRETSECRETSECRETSECRETSECRETSECRET";
		String filler = "x".repeat(1000);
		Database db = Database.withDataSource(createThrowingDataSource(filler + " " + secret + " tail")).build();

		DatabaseException ex = Assertions.assertThrows(DatabaseException.class, () ->
				db.query("INSERT INTO t (v) VALUES (:v)").bind("v", Parameters.secure(secret)).execute());

		Assertions.assertFalse(ex.getMessage().contains("SECRETSECRET"),
				"Expected no secret prefix to survive truncation because scrubbing happens first");
	}

	@Test
	public void testStatementLogCarriesScrubbedWrappedException() {
		String secret = "log-visible-secret";
		AtomicReference<StatementLog<?>> loggedStatementLog = new AtomicReference<>();
		Database db = Database.withDataSource(createThrowingDataSource(format("driver rejected %s here", secret)))
				.statementLogger(loggedStatementLog::set)
				.build();

		Assertions.assertThrows(DatabaseException.class, () ->
				db.query("INSERT INTO t (v) VALUES (:v)").bind("v", Parameters.secure(secret)).execute());

		StatementLog<?> statementLog = loggedStatementLog.get();
		Assertions.assertNotNull(statementLog, "Expected a StatementLog for the failed statement");
		Assertions.assertTrue(statementLog.getException().isPresent());
		Assertions.assertTrue(statementLog.getException().get() instanceof DatabaseException,
				"Expected the wrapped exception in the StatementLog");
		Assertions.assertFalse(statementLog.getException().get().getMessage().contains(secret),
				"Expected the secret absent from the logged exception message");
		Assertions.assertFalse(statementLog.toString().contains(secret),
				"Expected the secret absent from StatementLog.toString()");
	}

	@Test
	public void testNoSecureParametersLeavesDriverMessageIntact() {
		String driverMessage = "plain failure mentioning value-12345 with no secure params";
		Database db = Database.withDataSource(createThrowingDataSource(driverMessage)).build();

		DatabaseException ex = Assertions.assertThrows(DatabaseException.class, () ->
				db.query("INSERT INTO t (v) VALUES (:v)").bind("v", "value-12345").execute());

		Assertions.assertTrue(ex.getMessage().contains("value-12345"),
				"Expected untouched driver text on the no-secure-parameters fast path");
	}

	@Test
	public void testRedactAllAloneDoesNotScrubDriverText() {
		// ParameterRedactor governs Pyranid's parameter-list rendering only; driver-text scrubbing is
		// triggered exclusively by explicit Parameters.secure(...) marks
		String plainValue = "not-marked-secure-value";
		Database db = Database.withDataSource(createThrowingDataSource(format("driver echoed %s", plainValue)))
				.parameterRedactor(ParameterRedactor.redactAll())
				.build();

		DatabaseException ex = Assertions.assertThrows(DatabaseException.class, () ->
				db.query("INSERT INTO t (v) VALUES (:v)").bind("v", plainValue).execute());

		Assertions.assertTrue(ex.getMessage().contains(plainValue),
				"redactAll() must not trigger driver-text scrubbing for non-secure values");
	}

	@Test
	public void testSecureValueAbsentFromRealFailureDiagnostics() {
		// A1 third-sink regression: real (HSQLDB) failure with a secure param + redactAll - the secret
		// must not appear anywhere in getMessage()/toString()
		String secret = "regression-secret";
		Database db = Database.withDataSource(createInMemoryDataSource("scrub_a1_third_sink"))
				.parameterRedactor(ParameterRedactor.redactAll())
				.build();

		DatabaseException ex = Assertions.assertThrows(DatabaseException.class, () ->
				db.query("SELECT * FROM missing_table WHERE v = :v").bind("v", Parameters.secure(secret))
						.fetchList(String.class));

		Assertions.assertFalse(ex.getMessage().contains(secret), "Expected the secret absent from getMessage()");
		Assertions.assertFalse(ex.toString().contains(secret), "Expected the secret absent from toString()");
	}

	@Test
	public void testSecureInListElementsScrubbed() {
		String secret1 = "in-list-secret-1";
		String secret2 = "in-list-secret-2";
		Database db = Database.withDataSource(createThrowingDataSource(
				format("bad values %s and %s", secret1, secret2))).build();

		DatabaseException ex = Assertions.assertThrows(DatabaseException.class, () ->
				db.query("SELECT * FROM t WHERE v IN (:vs)")
						.bind("vs", Parameters.secure(Parameters.inList(List.of(secret1, secret2))))
						.fetchList(String.class));

		Assertions.assertFalse(ex.getMessage().contains(secret1), "Expected first IN-list secret scrubbed");
		Assertions.assertFalse(ex.getMessage().contains(secret2), "Expected second IN-list secret scrubbed");
	}

	// --- Test infrastructure ---

	@NonNull
	private DataSource createInMemoryDataSource(@NonNull String databaseName) {
		requireNonNull(databaseName);

		JDBCDataSource dataSource = new JDBCDataSource();
		dataSource.setUrl(format("jdbc:hsqldb:mem:%s", databaseName));
		dataSource.setUser("sa");
		dataSource.setPassword("");

		return dataSource;
	}

	/**
	 * A DataSource whose connections throw a {@link SQLException} with a controlled, driver-style message
	 * on {@code prepareStatement}, letting tests exercise the scrub against arbitrary echoed text.
	 */
	@NonNull
	private DataSource createThrowingDataSource(@NonNull String driverMessage) {
		requireNonNull(driverMessage);

		InvocationHandler connectionHandler = (proxy, method, args) -> {
			switch (method.getName()) {
				case "prepareStatement":
					throw new SQLException(driverMessage, "23505");
				case "close", "setAutoCommit", "commit", "rollback":
					return null;
				case "getAutoCommit":
					return true;
				case "isClosed":
					return false;
				case "isValid":
					return true;
				case "toString":
					return "ThrowingConnection";
				case "hashCode":
					return System.identityHashCode(proxy);
				case "equals":
					return proxy == args[0];
				default:
					throw new SQLException(driverMessage, "23505");
			}
		};

		Connection connection = (Connection) Proxy.newProxyInstance(getClass().getClassLoader(),
				new Class<?>[]{Connection.class}, connectionHandler);

		return new DataSource() {
			@Override
			public Connection getConnection() {
				return connection;
			}

			@Override
			public Connection getConnection(String username, String password) {
				return connection;
			}

			@Override
			public PrintWriter getLogWriter() {
				return null;
			}

			@Override
			public void setLogWriter(PrintWriter out) {
				// no-op
			}

			@Override
			public void setLoginTimeout(int seconds) {
				// no-op
			}

			@Override
			public int getLoginTimeout() {
				return 0;
			}

			@Override
			public Logger getParentLogger() {
				return Logger.getGlobal();
			}

			@Override
			public <T> T unwrap(Class<T> iface) throws SQLException {
				throw new SQLException("Not a wrapper");
			}

			@Override
			public boolean isWrapperFor(Class<?> iface) {
				return false;
			}
		};
	}
}
