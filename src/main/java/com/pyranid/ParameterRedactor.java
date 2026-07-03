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

import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Converts non-secure bound parameter values into safe display values for diagnostics.
 * <p>
 * This is display-only: it does not change what is bound to the database. Pyranid never invokes a
 * {@code ParameterRedactor} for {@link SecureParameter} values; {@link SecureParameter#getMask()} always wins. For batch
 * executions, Pyranid renders a bounded batch summary and does not invoke this redactor for individual batch values.
 * <p>
 * <strong>Scope.</strong> A {@code ParameterRedactor} governs Pyranid's rendering of its own {@code parameters=[...]}
 * diagnostic list only. It does <em>not</em> reach text the database driver itself produces: driver error messages can
 * echo bound values (for example, constraint-violation detail), and only values explicitly wrapped with
 * {@link Parameters#secure} trigger Pyranid's best-effort scrub of driver-echoed text - see {@link SecureParameter}
 * for that mechanism and its limits. Note that under the default {@link #none()}, non-secure values render verbatim
 * in statement logs and exception text; configure {@link #redactAll()} or a custom redactor if diagnostics may leave
 * a trusted boundary.
 * <p>
 * Redaction failures are fail-fast. If an implementation throws, Pyranid propagates the failure from the diagnostic
 * rendering call site.
 *
 * @author <a href="https://www.revetkn.com">Mark Allen</a>
 * @since 4.4.0
 */
@ThreadSafe
@FunctionalInterface
public interface ParameterRedactor {
	/**
	 * Redacts a single non-secure parameter for diagnostics.
	 *
	 * @param statementContext current SQL context
	 * @param parameterIndex   zero-based index into {@link StatementContext#getParameters()} after IN-list expansion
	 * @param parameter        parameter value to render safely
	 * @return the safe display value
	 */
	@Nullable
	Object redactParameter(@NonNull StatementContext<?> statementContext,
												 int parameterIndex,
												 @Nullable Object parameter);

	/**
	 * Acquires the default redactor, which renders non-secure values verbatim for non-batch executions.
	 *
	 * @return the default redactor
	 */
	@NonNull
	static ParameterRedactor none() {
		return (statementContext, parameterIndex, parameter) -> parameter;
	}

	/**
	 * Acquires a redactor that masks every non-secure value as {@code <redacted>}.
	 *
	 * @return a redactor that masks every non-secure value
	 */
	@NonNull
	static ParameterRedactor redactAll() {
		return (statementContext, parameterIndex, parameter) -> SecureParameterSupport.DEFAULT_MASK;
	}
}
