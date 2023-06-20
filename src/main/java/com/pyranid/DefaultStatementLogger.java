/*
 * Copyright 2015-2022 Transmogrify LLC, 2022-2023 Revetware LLC.
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

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

/**
 * Basic implementation of {@link StatementLogger} which logs to <code>{@value #LOGGER_NAME}</code> at
 * {@link Level#FINE}.
 *
 * @author <a href="https://www.revetware.com">Mark Allen</a>
 * @since 1.0.0
 */
public class DefaultStatementLogger implements StatementLogger {
	/**
	 * The name of our logger.
	 */
	public static final String LOGGER_NAME = "com.pyranid.SQL";

	/**
	 * The level of our logger.
	 */
	public static final Level LOGGER_LEVEL = Level.FINE;

	/**
	 * The point at which we ellipsize output for parameters.
	 */
	private static final int MAXIMUM_PARAMETER_LOGGING_LENGTH = 100;

	private final Logger logger = Logger.getLogger(LOGGER_NAME);

	@Override
	public void log(StatementLog statementLog) {
		requireNonNull(statementLog);

		if (logger.isLoggable(LOGGER_LEVEL))
			logger.log(LOGGER_LEVEL, formatStatementLog(statementLog));
	}

	protected String formatStatementLog(StatementLog statementLog) {
		requireNonNull(statementLog);

		List<String> timingEntries = new ArrayList<>(4);

		if (statementLog.connectionAcquisitionTime().isPresent())
			timingEntries.add(format("%.2fms acquiring connection",
					(float) statementLog.connectionAcquisitionTime().get() / 1_000_000f));

		if (statementLog.preparationTime().isPresent())
			timingEntries.add(format("%.2fms preparing statement", (float) statementLog.preparationTime().get() / 1_000_000f));

		if (statementLog.executionTime().isPresent())
			timingEntries.add(format("%.2fms executing statement", (float) statementLog.executionTime().get() / 1_000_000f));

		if (statementLog.resultSetMappingTime().isPresent())
			timingEntries.add(format("%.2fms processing resultset", (float) statementLog.resultSetMappingTime().get() / 1_000_000f));

		String parameterLine = null;

		if (statementLog.statementContext().getParameters().size() > 0) {
			StringBuilder parameterLineBuilder = new StringBuilder();
			parameterLineBuilder.append("Parameters: ");
			parameterLineBuilder.append(statementLog.statementContext().getParameters().stream().map(parameter -> {
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

		lines.add(statementLog.statementContext().getSql());

		if (parameterLine != null)
			lines.add(parameterLine);

		if (timingEntries.size() > 0)
			lines.add(timingEntries.stream().collect(joining(", ")));

		if (statementLog.exception().isPresent()) {
			Throwable throwable = (Throwable) statementLog.exception().get();

			if (throwable instanceof DatabaseException && throwable.getCause() != null)
				throwable = throwable.getCause();

			lines.add(format("Failed due to %s", throwable.toString()));
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
	protected String ellipsize(String string, int maximumLength) {
		requireNonNull(string);

		string = string.trim();

		if (string.length() <= maximumLength)
			return string;

		return format("%s...", string.substring(0, maximumLength));
	}
}