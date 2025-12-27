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

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * @author <a href="https://www.revetkn.com">Mark Allen</a>
 * @since 4.0.0
 */
final class TestQueries {
	private TestQueries() {}

	@NonNull
	static Long execute(@NonNull Database db,
											@NonNull String sql,
											Object @Nullable ... parameters) {
		requireNonNull(db);
		requireNonNull(sql);

		Object[] params = parameters == null ? new Object[0] : parameters;
		String rewrittenSql = rewritePositionalParameters(sql, params.length);

		Query query = db.query(rewrittenSql);
		for (int i = 0; i < params.length; i++)
			query.bind("p" + (i + 1), params[i]);

		return query.execute();
	}

	@NonNull
	private static String rewritePositionalParameters(@NonNull String sql, int paramCount) {
		requireNonNull(sql);

		StringBuilder rewritten = new StringBuilder(sql.length() + paramCount * 3);
		boolean inSingleQuote = false;
		boolean inDoubleQuote = false;
		int paramIndex = 0;

		for (int i = 0; i < sql.length(); i++) {
			char c = sql.charAt(i);

			if (c == '\'' && !inDoubleQuote) {
				rewritten.append(c);

				if (inSingleQuote) {
					if (i + 1 < sql.length() && sql.charAt(i + 1) == '\'') {
						rewritten.append('\'');
						i++;
					} else {
						inSingleQuote = false;
					}
				} else {
					inSingleQuote = true;
				}

				continue;
			}

			if (c == '"' && !inSingleQuote) {
				inDoubleQuote = !inDoubleQuote;
				rewritten.append(c);
				continue;
			}

			if (c == '?' && !inSingleQuote && !inDoubleQuote) {
				paramIndex++;
				rewritten.append(":p").append(paramIndex);
				continue;
			}

			rewritten.append(c);
		}

		if (paramIndex != paramCount) {
			throw new IllegalArgumentException(format(
					"Expected %d parameters but found %d positional placeholders in SQL: %s",
					paramCount, paramIndex, sql));
		}

		return rewritten.toString();
	}
}
