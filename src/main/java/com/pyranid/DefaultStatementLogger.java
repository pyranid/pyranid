/*
 * Copyright 2015-2022 Transmogrify LLC, 2022-2024 Revetware LLC.
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

import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

/**
 * Basic implementation of {@link StatementLogger} which logs via <a href="https://docs.oracle.com/en/java/javase/20/docs/api/java.logging/java/util/logging/package-summary.html">java.util.logging</a>.
 *
 * @author <a href="https://www.revetkn.com">Mark Allen</a>
 * @since 1.0.0
 */
@ThreadSafe
public class DefaultStatementLogger implements StatementLogger {
	@Nonnull
	public static final String DEFAULT_LOGGER_NAME = "com.pyranid.SQL";
	@Nonnull
	public static final Level DEFAULT_LOGGER_LEVEL = Level.FINE;

	/**
	 * The point at which we ellipsize output for parameters.
	 */
	@Nonnull
	private static final int MAXIMUM_PARAMETER_LOGGING_LENGTH = 100;

	@Nonnull
	private final Logger logger;
	@Nonnull
	private final Level loggerLevel;

	/**
	 * Creates a new statement logger with the default logger name <code>{@value #DEFAULT_LOGGER_NAME}</code> and level.
	 */
	public DefaultStatementLogger() {
		this(DEFAULT_LOGGER_NAME, DEFAULT_LOGGER_LEVEL);
	}

	/**
	 * Creates a new statement logger with the given logger name and level.
	 *
	 * @param loggerName  the logger name to use
	 * @param loggerLevel the logger level to use
	 */
	public DefaultStatementLogger(@Nonnull String loggerName,
																@Nonnull Level loggerLevel) {
		requireNonNull(loggerName);
		requireNonNull(loggerLevel);

		this.logger = Logger.getLogger(loggerName);
		this.loggerLevel = loggerLevel;
	}

	@Override
	public void log(@Nonnull StatementLog statementLog) {
		requireNonNull(statementLog);

		if (getLogger().isLoggable(getLoggerLevel()))
			getLogger().log(getLoggerLevel(), formatStatementLog(statementLog));
	}

	@Nonnull
	protected String formatStatementLog(@Nonnull StatementLog statementLog) {
		requireNonNull(statementLog);

		List<String> timingEntries = new ArrayList<>(4);

		if (statementLog.getConnectionAcquisitionDuration().isPresent())
			timingEntries.add(format("%s acquiring connection", statementLog.getConnectionAcquisitionDuration().get()));

		if (statementLog.getPreparationDuration().isPresent())
			timingEntries.add(format("%s preparing statement", statementLog.getPreparationDuration().get()));

		if (statementLog.getExecutionDuration().isPresent())
			timingEntries.add(format("%s executing statement", statementLog.getExecutionDuration().get()));

		if (statementLog.getResultSetMappingDuration().isPresent())
			timingEntries.add(format("%s processing resultset", statementLog.getResultSetMappingDuration().get()));

		String parameterLine = null;

		if (statementLog.getStatementContext().getParameters().size() > 0) {
			StringBuilder parameterLineBuilder = new StringBuilder();
			parameterLineBuilder.append("Parameters: ");
			parameterLineBuilder.append(statementLog.getStatementContext().getParameters().stream().map(parameter -> {
				if (parameter == null)
					return "null";

				if (parameter instanceof Number)
					return format("%s", parameter);

				if (parameter.getClass().isArray()) {
					// TODO: cap size of arrays

					if (parameter instanceof byte[])
						return format("[byte array of length %d]", ((byte[]) parameter).length);
				}

				return format("'%s'", ellipsize(parameter.toString(), MAXIMUM_PARAMETER_LOGGING_LENGTH));
			}).collect(joining(", ")));

			parameterLine = parameterLineBuilder.toString();
		}

		List<String> lines = new ArrayList<>(4);

		lines.add(statementLog.getStatementContext().getStatement().getSql());

		if (parameterLine != null)
			lines.add(parameterLine);

		if (timingEntries.size() > 0)
			lines.add(timingEntries.stream().collect(joining(", ")));

		Throwable exception = (Exception) statementLog.getException().orElse(null);

		if (exception != null) {
			if (exception instanceof DatabaseException && exception.getCause() != null)
				exception = exception.getCause();

			lines.add(format("Failed due to %s", exception.toString()));
		}

		return lines.stream().collect(joining("\n"));
	}

	/**
	 * Ellipsizes the given {@code string}, capping at {@code maximumLength}.
	 *
	 * @param string        the string to ellipsize
	 * @param maximumLength the maximum length of the ellipsized string, not including ellipsis
	 * @return an ellipsized version of {@code string}
	 */
	@Nonnull
	protected String ellipsize(@Nonnull String string,
														 int maximumLength) {
		requireNonNull(string);

		string = string.trim();

		if (string.length() <= maximumLength)
			return string;

		return format("%s...", string.substring(0, maximumLength));
	}

	@Nonnull
	protected Logger getLogger() {
		return this.logger;
	}

	@Nonnull
	protected Level getLoggerLevel() {
		return this.loggerLevel;
	}
}