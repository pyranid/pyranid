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

import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * PostgreSQL-specific behavior.
 */
final class PostgresDialect extends GenericDialect {
	@NonNull
	static final PostgresDialect INSTANCE = new PostgresDialect();

	private PostgresDialect() {}

	@NonNull
	@Override
	public List<String> sqlFragmentsForOperators(boolean hasQuestionMarkOperators,
																							 @NonNull List<String> sqlFragments,
																							 @NonNull List<String> postgresqlSqlFragments) {
		requireNonNull(sqlFragments);
		requireNonNull(postgresqlSqlFragments);

		return hasQuestionMarkOperators ? postgresqlSqlFragments : sqlFragments;
	}
}
